"""
redshift_loader.py  —  redshift/loaders/
Triggered by Lambda or Airflow every 5 minutes.
Runs COPY from S3 → staging, then MERGE into target tables.
"""

import logging
import os

import boto3
import redshift_connector

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REDSHIFT_IAM_ROLE = os.environ["REDSHIFT_IAM_ROLE"]
REDSHIFT_DATABASE = os.environ.get("REDSHIFT_DATABASE", "etl_db")
S3_OUTPUT = os.environ["S3_OUTPUT_PATH"]


def get_connection() -> redshift_connector.Connection:
    secret = boto3.client("secretsmanager").get_secret_value(
        SecretId=os.environ["REDSHIFT_SECRET_ARN"]
    )
    import json
    creds = json.loads(secret["SecretString"])
    return redshift_connector.connect(
        host=creds["host"],
        database=REDSHIFT_DATABASE,
        user=creds["username"],
        password=creds["password"],
    )


def run_copy(cursor, s3_path: str, table: str):
    sql = f"""
        COPY {table}
        FROM '{s3_path}'
        IAM_ROLE '{REDSHIFT_IAM_ROLE}'
        FORMAT AS PARQUET
        SERIALIZETOJSON;
    """
    logger.info(f"COPY {s3_path} -> {table}")
    cursor.execute(sql)


def merge_clickstream(cursor):
    cursor.execute("""
        MERGE INTO public.fact_clickstream AS tgt
        USING staging.clickstream_events AS src
        ON tgt.event_id = src.event_id
        WHEN NOT MATCHED THEN INSERT (
            event_id, session_id, user_id_hashed, event_type,
            page_path, domain, device_type, is_mobile,
            country_code, event_ts, processed_ts
        ) VALUES (
            src.event_id, src.session_id, src.user_id_hashed, src.event_type,
            src.page_path, src.domain, src.device_type, src.is_mobile,
            src.country_code, src.event_ts, src.processed_ts
        );
    """)
    logger.info("MERGE clickstream complete")


def merge_sessions(cursor):
    cursor.execute("""
        MERGE INTO public.fact_session_aggregates AS tgt
        USING staging.session_aggregates AS src
        ON tgt.session_id = src.session_id AND tgt.window_start = src.window_start
        WHEN MATCHED THEN UPDATE SET
            event_count = src.event_count,
            unique_pages = src.unique_pages,
            cart_adds = src.cart_adds,
            session_duration_s = DATEDIFF('second', src.session_start, src.session_end)
        WHEN NOT MATCHED THEN INSERT (
            session_id, window_start, device_type, country_code,
            event_count, unique_pages, cart_adds, session_duration_s
        ) VALUES (
            src.session_id, src.window_start, src.device_type, src.country_code,
            src.event_count, src.unique_pages, src.cart_adds,
            DATEDIFF('second', src.session_start, src.session_end)
        );
    """)
    logger.info("MERGE sessions complete")


def refresh_materialized_view(cursor):
    cursor.execute("REFRESH MATERIALIZED VIEW public.mv_hourly_event_summary;")
    logger.info("Materialized view refreshed")


def main():
    conn = get_connection()
    conn.autocommit = False
    cursor = conn.cursor()

    try:
        cursor.execute("TRUNCATE TABLE staging.clickstream_events;")
        cursor.execute("TRUNCATE TABLE staging.session_aggregates;")

        run_copy(cursor, f"{S3_OUTPUT}/clickstream/", "staging.clickstream_events")
        run_copy(cursor, f"{S3_OUTPUT}/sessions/", "staging.session_aggregates")

        merge_clickstream(cursor)
        merge_sessions(cursor)
        refresh_materialized_view(cursor)

        conn.commit()
        logger.info("Load cycle complete")
    except Exception as e:
        conn.rollback()
        logger.error(f"Load failed, rolled back: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
