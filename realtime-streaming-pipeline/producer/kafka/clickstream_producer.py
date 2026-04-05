"""
clickstream_producer.py  —  producer/kafka/
Simulates clickstream events from web/mobile clients.
Publishes to Kafka topic: clickstream.events
Supports configurable event rate and duration for load testing.
"""

import argparse
import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TOPIC = "clickstream.events"

CLICKSTREAM_SCHEMA = """
{
  "type": "record",
  "name": "ClickstreamEvent",
  "namespace": "com.pipeline.events",
  "fields": [
    {"name": "event_id",     "type": "string"},
    {"name": "session_id",   "type": "string"},
    {"name": "user_id",      "type": ["null", "string"], "default": null},
    {"name": "event_type",   "type": "string"},
    {"name": "page_url",     "type": "string"},
    {"name": "referrer",     "type": ["null", "string"], "default": null},
    {"name": "device_type",  "type": "string"},
    {"name": "ip_address",   "type": "string"},
    {"name": "country_code", "type": ["null", "string"], "default": null},
    {"name": "event_ts",     "type": "long", "logicalType": "timestamp-millis"}
  ]
}
"""

EVENT_TYPES = ["page_view", "button_click", "form_submit", "scroll", "video_play", "search", "add_to_cart"]
DEVICE_TYPES = ["desktop", "mobile", "tablet"]
PAGES = ["/home", "/products", "/cart", "/checkout", "/account", "/search", "/blog"]
COUNTRIES = ["US", "IN", "GB", "CA", "AU", "DE", "FR", "BR", "JP", "MX"]


def generate_event(session_id: str, user_id: str | None) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "session_id": session_id,
        "user_id": user_id,
        "event_type": random.choice(EVENT_TYPES),
        "page_url": f"https://example.com{random.choice(PAGES)}",
        "referrer": random.choice([None, "https://google.com", "https://facebook.com"]),
        "device_type": random.choice(DEVICE_TYPES),
        "ip_address": f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}",
        "country_code": random.choice(COUNTRIES),
        "event_ts": int(datetime.now(timezone.utc).timestamp() * 1000),
    }


def delivery_report(err, msg):
    if err:
        logger.error(f"Delivery failed for {msg.key()}: {err}")
    else:
        logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")


def run_producer(bootstrap_servers: str, schema_registry_url: str, rate: int, duration: int):
    schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})
    avro_serializer = AvroSerializer(schema_registry_client, CLICKSTREAM_SCHEMA)

    producer = Producer({"bootstrap.servers": bootstrap_servers, "acks": "all"})

    sessions = [str(uuid.uuid4()) for _ in range(50)]
    users = [str(uuid.uuid4()) for _ in range(200)] + [None] * 50

    interval = 1.0 / rate
    start = time.time()
    total = 0

    logger.info(f"Starting producer: {rate} events/sec for {duration}s")

    while time.time() - start < duration:
        session_id = random.choice(sessions)
        user_id = random.choice(users)
        event = generate_event(session_id, user_id)

        producer.produce(
            topic=TOPIC,
            key=session_id,
            value=avro_serializer(
                event,
                SerializationContext(TOPIC, MessageField.VALUE),
            ),
            on_delivery=delivery_report,
        )
        producer.poll(0)
        total += 1

        elapsed = time.time() - start
        expected = total * interval
        if expected > elapsed:
            time.sleep(expected - elapsed)

    producer.flush()
    logger.info(f"Done. Produced {total} events in {time.time() - start:.1f}s")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--schema-registry", default="http://localhost:8081")
    parser.add_argument("--rate", type=int, default=50, help="Events per second")
    parser.add_argument("--duration", type=int, default=60, help="Duration in seconds")
    args = parser.parse_args()

    run_producer(args.bootstrap_servers, args.schema_registry, args.rate, args.duration)


if __name__ == "__main__":
    main()
