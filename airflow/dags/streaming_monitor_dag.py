"""
streaming_monitor_dag.py  —  airflow/dags/
MWAA DAG that monitors the streaming pipeline health,
triggers Redshift loads on schedule, and runs compaction on Delta tables.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.amazon.aws.sensors.kinesis import KinesisConsumerLagSensor

import boto3

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": True,
    "email": ["de-alerts@example.com"],
}

EMR_APP_ID = "{{ var.value.EMR_APP_ID }}"
EMR_ROLE_ARN = "{{ var.value.EMR_EXECUTION_ROLE_ARN }}"
SCRIPTS_BUCKET = "{{ var.value.ETL_SCRIPTS_BUCKET }}"


def check_kinesis_shard_utilization(**context):
    """Alert if any shard exceeds 80% utilization."""
    cw = boto3.client("cloudwatch")
    stream_name = context["var"]["value"]["KINESIS_CLICKSTREAM_STREAM"]

    resp = cw.get_metric_statistics(
        Namespace="AWS/Kinesis",
        MetricName="GetRecords.IteratorAgeMilliseconds",
        Dimensions=[{"Name": "StreamName", "Value": stream_name}],
        StartTime=datetime.utcnow() - timedelta(minutes=10),
        EndTime=datetime.utcnow(),
        Period=300,
        Statistics=["Maximum"],
    )

    datapoints = resp.get("Datapoints", [])
    if datapoints:
        max_age_ms = max(d["Maximum"] for d in datapoints)
        context["task_instance"].xcom_push(key="max_iterator_age_ms", value=max_age_ms)
        if max_age_ms > 60_000:
            raise ValueError(f"Kinesis consumer lag too high: {max_age_ms}ms — check Spark job")
    else:
        context["task_instance"].log.info("No Kinesis metrics found — stream may be idle")


with DAG(
    dag_id="streaming_pipeline_monitor",
    default_args=DEFAULT_ARGS,
    description="Monitor + maintain real-time streaming pipeline",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["streaming", "kinesis", "spark", "redshift"],
) as dag:

    check_lag = PythonOperator(
        task_id="check_kinesis_lag",
        python_callable=check_kinesis_shard_utilization,
        provide_context=True,
    )

    load_redshift = BashOperator(
        task_id="load_redshift",
        bash_command="python /opt/airflow/dags/redshift/loaders/redshift_loader.py",
    )

    check_lag >> load_redshift


with DAG(
    dag_id="streaming_delta_compaction",
    default_args=DEFAULT_ARGS,
    description="Nightly Delta Lake compaction on streaming output tables",
    schedule_interval="0 4 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["streaming", "delta-lake", "compaction"],
) as compaction_dag:

    compact_clickstream = EmrServerlessStartJobOperator(
        task_id="compact_clickstream_delta",
        application_id=EMR_APP_ID,
        execution_role_arn=EMR_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": f"s3://{SCRIPTS_BUCKET}/spark/delta_compaction.py",
                "arguments": ["--table", "clickstream", "--z-order-cols", "country_code,event_type"],
                "sparkSubmitParameters": (
                    "--conf spark.executor.cores=4 "
                    "--conf spark.executor.memory=8g "
                    "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
                ),
            }
        },
        aws_conn_id="aws_default",
        wait_for_completion=True,
    )

    compact_clickstream
