# Real-Time Streaming Pipeline

Event-driven streaming pipeline processing clickstream and order events in real time.
Kafka producers publish events → Kinesis Data Streams buffers → PySpark Structured Streaming transforms → Redshift for analytics.
Infrastructure provisioned via AWS CDK. Deployed on EMR Serverless + Kinesis Data Firehose.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                          EVENT PRODUCERS                                      │
│   Web App (clicks)  │  Mobile App (sessions)  │  Order Service (transactions) │
└────────┬────────────┴──────────┬──────────────┴───────────┬───────────────────┘
         │                       │                           │
         ▼                       ▼                           ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                       KAFKA (MSK)  →  KINESIS                                 │
│   Kafka topics: clickstream, orders, sessions                                 │
│   Kafka → Kinesis bridge (Lambda consumer)                                    │
│   Kinesis Data Streams: 4 shards, 24hr retention                             │
└────────────────────────────┬─────────────────────────────────────────────────┘
                             │
              ┌──────────────┴──────────────┐
              ▼                             ▼
┌─────────────────────────┐   ┌─────────────────────────────────────────────┐
│  KINESIS DATA FIREHOSE  │   │     PYSPARK STRUCTURED STREAMING             │
│  S3 delivery (archive)  │   │     EMR Serverless                           │
│  Parquet, 5min buffer   │   │     Micro-batch: 30s trigger interval        │
└─────────────────────────┘   │     Watermark: 2 minutes                     │
                              │     Transformations: dedup, enrich, aggregate │
                              └──────────────┬──────────────────────────────┘
                                             │
                              ┌──────────────┴──────────────┐
                              ▼                             ▼
                 ┌─────────────────────┐      ┌─────────────────────────┐
                 │    S3 (processed)   │      │  REDSHIFT SERVERLESS    │
                 │    Delta Lake fmt   │      │  COPY every 5 minutes   │
                 │    Athena queryable │      │  Real-time dashboards   │
                 └─────────────────────┘      └─────────────────────────┘
                                             │
                                             ▼
                              ┌─────────────────────────────┐
                              │   MONITORING                 │
                              │   CloudWatch + SNS + Grafana │
                              │   Kinesis shard utilization  │
                              │   PySpark lag metrics        │
                              └─────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Message Broker | Apache Kafka (Amazon MSK) |
| Stream | Amazon Kinesis Data Streams |
| Delivery | Amazon Kinesis Data Firehose |
| Processing | PySpark Structured Streaming (EMR Serverless) |
| Storage | Amazon S3 + Delta Lake |
| Warehouse | Amazon Redshift Serverless |
| Infra | AWS CDK (Python) |
| Monitoring | CloudWatch + Grafana |
| Orchestration | Apache Airflow (MWAA) |

---

## Project Structure

```
realtime-streaming-pipeline/
├── producer/
│   ├── kafka/             # Kafka producers (clickstream, orders)
│   └── kinesis/           # Kinesis bridge + direct producers
├── consumer/
│   ├── spark/             # PySpark Structured Streaming jobs
│   └── lambda/            # Kinesis → S3 Lambda consumer
├── redshift/
│   ├── schemas/           # DDL for staging + target tables
│   └── loaders/           # COPY + MERGE scripts
├── cdk/lib/               # CDK stack definitions
├── airflow/dags/          # MWAA orchestration
├── monitoring/            # CloudWatch dashboards + alerts
├── tests/                 # Unit + integration tests
└── config/                # Schema registry configs
```

---

## Setup

### Prerequisites
- AWS CLI configured
- Python 3.11+
- Docker (for local Kafka)
- AWS CDK CLI: `npm install -g aws-cdk`

### 1. Local Development (Kafka via Docker)
```bash
docker-compose up -d
# Starts: Zookeeper, Kafka broker, Schema Registry, Kafka UI
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Deploy AWS Infrastructure
```bash
cd cdk
cdk bootstrap
cdk deploy --all --context stage=dev
```

### 4. Start Producer (local test)
```bash
python producer/kafka/clickstream_producer.py --rate 100 --duration 60
```

### 5. Submit Spark Streaming Job (EMR Serverless)
```bash
aws emr-serverless start-job-run \
  --application-id $EMR_APP_ID \
  --execution-role-arn $EMR_ROLE_ARN \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://SCRIPTS_BUCKET/spark/clickstream_stream.py",
      "sparkSubmitParameters": "--conf spark.executor.cores=2 --conf spark.executor.memory=4g"
    }
  }'
```

---

## Kafka Topics

| Topic | Partitions | Retention | Schema |
|---|---|---|---|
| `clickstream.events` | 6 | 7 days | Avro |
| `orders.created` | 4 | 7 days | Avro |
| `sessions.started` | 4 | 3 days | Avro |
| `orders.dlq` | 2 | 14 days | JSON |

---

## PySpark Streaming Features

- **Watermark** — 2-minute late-data tolerance
- **Stateful aggregations** — session windows, sliding windows
- **Exactly-once semantics** — Kinesis checkpoint in S3
- **Schema evolution** — Delta Lake handles column additions
- **Micro-batch interval** — 30 seconds

---

## Redshift Schema

```sql
-- Clickstream fact table
CREATE TABLE fact_clickstream (
    event_id        VARCHAR(64)     NOT NULL,
    session_id      VARCHAR(64),
    user_id_hashed  VARCHAR(64),
    event_type      VARCHAR(32),
    page_url        VARCHAR(512),
    device_type     VARCHAR(16),
    event_ts        TIMESTAMP,
    processed_ts    TIMESTAMP DEFAULT GETDATE(),
    PRIMARY KEY (event_id)
) DISTKEY(user_id_hashed) SORTKEY(event_ts);
```

---

## Key Design Decisions

**Kafka → Kinesis bridge (not direct Kinesis producers)** — Kafka decouples producers from AWS. Internal services publish to Kafka topics; a bridge Lambda fans out to Kinesis. This avoids vendor lock-in at the source.

**Structured Streaming over Spark DStream** — DStream is deprecated in Spark 3.x. Structured Streaming gives DataFrame API, better fault tolerance, and exactly-once guarantees with checkpointing.

**EMR Serverless over Databricks** — No cluster management. Auto-scales to zero. Cost-effective for variable streaming workloads. Full Spark API compatibility.

**Delta Lake on S3 for processed output** — ACID guarantees, time travel, and Athena compatibility without a separate metastore. Compaction jobs run nightly to merge small files.

---

## License

MIT
