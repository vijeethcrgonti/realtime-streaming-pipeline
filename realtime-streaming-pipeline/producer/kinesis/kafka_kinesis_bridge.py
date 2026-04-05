"""
kafka_kinesis_bridge.py  —  producer/kinesis/
Lambda function that consumes from MSK (Kafka) and fans out to Kinesis Data Streams.
Triggered via MSK event source mapping in Lambda.
Handles batching, error isolation, and DLQ routing on poison pills.
"""

import base64
import json
import logging
import os
from collections import defaultdict

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

kinesis = boto3.client("kinesis")
sqs = boto3.client("sqs")

STREAM_MAP = {
    "clickstream.events": os.environ.get("KINESIS_CLICKSTREAM_STREAM", "clickstream-events"),
    "orders.created": os.environ.get("KINESIS_ORDERS_STREAM", "orders-created"),
    "sessions.started": os.environ.get("KINESIS_SESSIONS_STREAM", "sessions-started"),
}
DLQ_URL = os.environ.get("DLQ_URL", "")
MAX_KINESIS_BATCH = 500


def decode_kafka_value(encoded: str) -> dict | None:
    try:
        raw = base64.b64decode(encoded).decode("utf-8")
        return json.loads(raw)
    except Exception as e:
        logger.warning(f"Failed to decode Kafka value: {e}")
        return None


def build_kinesis_records(records: list[dict], partition_key_field: str) -> list[dict]:
    kinesis_records = []
    for rec in records:
        pk = str(rec.get(partition_key_field, "default"))
        kinesis_records.append({
            "Data": json.dumps(rec).encode("utf-8"),
            "PartitionKey": pk,
        })
    return kinesis_records


def put_to_kinesis(stream_name: str, records: list[dict]) -> tuple[int, int]:
    success, failed = 0, 0
    for i in range(0, len(records), MAX_KINESIS_BATCH):
        batch = records[i: i + MAX_KINESIS_BATCH]
        resp = kinesis.put_records(StreamName=stream_name, Records=batch)
        failed_count = resp.get("FailedRecordCount", 0)
        success += len(batch) - failed_count
        failed += failed_count

        if failed_count > 0:
            logger.warning(f"{failed_count} records failed in Kinesis put_records for {stream_name}")

    return success, failed


def send_to_dlq(records: list[dict], topic: str, reason: str):
    if not DLQ_URL:
        return
    for rec in records:
        sqs.send_message(
            QueueUrl=DLQ_URL,
            MessageBody=json.dumps({"record": rec, "topic": topic, "reason": reason}),
        )


def lambda_handler(event, context):
    logger.info(f"Processing {sum(len(v) for v in event.get('records', {}).values())} Kafka records")

    by_topic: dict[str, list[dict]] = defaultdict(list)
    decode_failures = defaultdict(list)

    for topic_partition, records in event.get("records", {}).items():
        topic = topic_partition.rsplit("-", 1)[0]
        for record in records:
            value = decode_kafka_value(record.get("value", ""))
            if value is None:
                decode_failures[topic].append(record)
            else:
                by_topic[topic].append(value)

    for topic, bad_records in decode_failures.items():
        logger.error(f"{len(bad_records)} decode failures for topic {topic}")
        send_to_dlq(bad_records, topic, "decode_failure")

    summary = {}
    for topic, records in by_topic.items():
        stream_name = STREAM_MAP.get(topic)
        if not stream_name:
            logger.warning(f"No Kinesis stream mapped for topic: {topic}")
            continue

        pk_field = "session_id" if "clickstream" in topic else "order_id"
        kinesis_records = build_kinesis_records(records, pk_field)
        success, failed = put_to_kinesis(stream_name, kinesis_records)

        summary[topic] = {"success": success, "failed": failed}
        logger.info(f"{topic} → {stream_name}: {success} ok, {failed} failed")

    return {"statusCode": 200, "summary": summary}
