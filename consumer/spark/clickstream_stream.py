"""
clickstream_stream.py  —  consumer/spark/
PySpark Structured Streaming job on EMR Serverless.
Consumes from Kinesis Data Streams, applies watermarking + windowed aggregations,
enriches events, and writes to S3 (Delta Lake) and Redshift.
"""

import logging
import os
from datetime import datetime

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KINESIS_STREAM = os.environ.get("KINESIS_STREAM_NAME", "clickstream-events")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
S3_CHECKPOINT = os.environ.get("S3_CHECKPOINT_PATH", "s3://etl-processed/checkpoints/clickstream")
S3_OUTPUT = os.environ.get("S3_OUTPUT_PATH", "s3://etl-processed/delta/clickstream")
REDSHIFT_JDBC_URL = os.environ.get("REDSHIFT_JDBC_URL", "")
REDSHIFT_TEMP_DIR = os.environ.get("REDSHIFT_TEMP_DIR", "s3://etl-processed/redshift-temp/")
REDSHIFT_IAM_ROLE = os.environ.get("REDSHIFT_IAM_ROLE", "")

MICRO_BATCH_INTERVAL = "30 seconds"
WATERMARK_DELAY = "2 minutes"

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("session_id", StringType()),
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("page_url", StringType()),
    StructField("referrer", StringType()),
    StructField("device_type", StringType()),
    StructField("ip_address", StringType()),
    StructField("country_code", StringType()),
    StructField("event_ts", LongType()),
])


def get_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("clickstream-structured-streaming")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.streaming.checkpointLocation", S3_CHECKPOINT)
        .config("spark.sql.shuffle.partitions", "8")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def read_kinesis_stream(spark: SparkSession):
    return (
        spark.readStream
        .format("kinesis")
        .option("streamName", KINESIS_STREAM)
        .option("regionName", AWS_REGION)
        .option("startingPosition", "LATEST")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_events(raw_df):
    return (
        raw_df
        .select(
            F.from_json(
                F.col("data").cast("string"),
                EVENT_SCHEMA,
            ).alias("event")
        )
        .select("event.*")
        .filter(F.col("event_id").isNotNull())
        .withColumn("event_ts", (F.col("event_ts") / 1000).cast(TimestampType()))
    )


def enrich(df):
    import hashlib
    hash_udf = F.udf(lambda v: hashlib.sha256(v.encode()).hexdigest() if v else None)

    return (
        df
        .withColumn("user_id_hashed", hash_udf(F.col("user_id"))).drop("user_id")
        .withColumn("ip_address", F.lit("***MASKED***"))
        .withColumn("domain", F.regexp_extract(F.col("page_url"), r"https?://([^/]+)", 1))
        .withColumn("page_path", F.regexp_extract(F.col("page_url"), r"https?://[^/]+(/.+)?", 1))
        .withColumn("is_mobile", F.col("device_type") == "mobile")
        .withColumn("processed_ts", F.current_timestamp())
    )


def deduplicate(df):
    """Deduplicate within each micro-batch on event_id."""
    from pyspark.sql.window import Window
    w = Window.partitionBy("event_id").orderBy(F.col("event_ts").desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def compute_session_aggregates(df):
    """
    Windowed aggregation: events per session in 5-minute tumbling windows.
    Uses watermark to handle late arrivals up to 2 minutes.
    """
    return (
        df
        .withWatermark("event_ts", WATERMARK_DELAY)
        .groupBy(
            F.window("event_ts", "5 minutes"),
            F.col("session_id"),
            F.col("device_type"),
            F.col("country_code"),
        )
        .agg(
            F.count("event_id").alias("event_count"),
            F.countDistinct("page_url").alias("unique_pages"),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("cart_adds"),
            F.min("event_ts").alias("session_start"),
            F.max("event_ts").alias("session_end"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
    )


def write_to_delta(batch_df, batch_id: int):
    if batch_df.isEmpty():
        return
    logger.info(f"Batch {batch_id}: writing {batch_df.count()} rows to Delta")
    (
        batch_df.write
        .format("delta")
        .mode("append")
        .partitionBy("country_code")
        .save(S3_OUTPUT)
    )


def write_to_redshift(batch_df, batch_id: int):
    if batch_df.isEmpty():
        return
    logger.info(f"Batch {batch_id}: writing to Redshift")
    (
        batch_df.write
        .format("io.github.spark_redshift_community.spark.redshift")
        .option("url", REDSHIFT_JDBC_URL)
        .option("dbtable", "staging.clickstream_events")
        .option("tempdir", REDSHIFT_TEMP_DIR)
        .option("aws_iam_role", REDSHIFT_IAM_ROLE)
        .mode("append")
        .save()
    )


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = read_kinesis_stream(spark)
    events = parse_events(raw)
    enriched = enrich(events)
    deduped = deduplicate(enriched)

    # Stream 1: raw enriched events → Delta Lake
    delta_query = (
        deduped.writeStream
        .foreachBatch(write_to_delta)
        .trigger(processingTime=MICRO_BATCH_INTERVAL)
        .option("checkpointLocation", f"{S3_CHECKPOINT}/delta")
        .outputMode("append")
        .start()
    )

    # Stream 2: session aggregates → Redshift
    agg_query = (
        compute_session_aggregates(deduped)
        .writeStream
        .foreachBatch(write_to_redshift)
        .trigger(processingTime=MICRO_BATCH_INTERVAL)
        .option("checkpointLocation", f"{S3_CHECKPOINT}/redshift")
        .outputMode("update")
        .start()
    )

    logger.info("Streaming queries started. Awaiting termination.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
