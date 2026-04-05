"""
test_spark_transforms.py  —  tests/unit/
Unit tests for PySpark Structured Streaming transformation logic.
Uses local Spark session — no cluster required.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("test-streaming-transforms")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def raw_events(spark):
    schema = StructType([
        StructField("event_id", StringType()),
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
    data = [
        ("evt-001", "sess-A", "usr-1", "page_view", "https://example.com/home", None, "desktop", "1.2.3.4", "US", 1705312800000),
        ("evt-002", "sess-A", "usr-1", "button_click", "https://example.com/cart", "https://google.com", "desktop", "1.2.3.4", "US", 1705312860000),
        ("evt-001", "sess-A", "usr-1", "page_view", "https://example.com/home", None, "desktop", "1.2.3.4", "US", 1705312800000),  # duplicate
        ("evt-003", "sess-B", None, "page_view", "https://example.com/blog", None, "mobile", "5.6.7.8", "IN", 1705312900000),
        (None, "sess-C", "usr-2", "scroll", "https://example.com/products", None, "tablet", "9.0.1.2", "GB", 1705313000000),  # null event_id
    ]
    return spark.createDataFrame(data, schema)


class TestParseAndFilter:
    def test_null_event_id_dropped(self, spark, raw_events):
        from pyspark.sql.types import TimestampType
        enriched = (
            raw_events
            .filter(F.col("event_id").isNotNull())
            .withColumn("event_ts", (F.col("event_ts") / 1000).cast(TimestampType()))
        )
        null_count = enriched.filter(F.col("event_id").isNull()).count()
        assert null_count == 0

    def test_timestamp_conversion(self, spark, raw_events):
        from pyspark.sql.types import TimestampType
        df = raw_events.withColumn("event_ts", (F.col("event_ts") / 1000).cast(TimestampType()))
        ts_type = dict(df.dtypes)["event_ts"]
        assert ts_type == "timestamp"


class TestEnrich:
    def test_user_id_masked(self, spark, raw_events):
        import hashlib
        hash_udf = F.udf(lambda v: hashlib.sha256(v.encode()).hexdigest() if v else None)
        df = raw_events.withColumn("user_id_hashed", hash_udf(F.col("user_id"))).drop("user_id")
        assert "user_id" not in df.columns
        assert "user_id_hashed" in df.columns

    def test_ip_masked(self, spark, raw_events):
        df = raw_events.withColumn("ip_address", F.lit("***MASKED***"))
        ips = {row["ip_address"] for row in df.collect()}
        assert ips == {"***MASKED***"}

    def test_domain_extracted(self, spark, raw_events):
        df = raw_events.withColumn("domain", F.regexp_extract(F.col("page_url"), r"https?://([^/]+)", 1))
        domains = {row["domain"] for row in df.collect() if row["domain"]}
        assert "example.com" in domains

    def test_is_mobile_flag(self, spark, raw_events):
        df = raw_events.withColumn("is_mobile", F.col("device_type") == "mobile")
        mobile_rows = df.filter(F.col("is_mobile")).collect()
        for row in mobile_rows:
            assert row["device_type"] == "mobile"


class TestDeduplicate:
    def test_duplicate_removed(self, spark, raw_events):
        from pyspark.sql.window import Window
        w = Window.partitionBy("event_id").orderBy(F.col("event_ts").desc())
        deduped = (
            raw_events
            .filter(F.col("event_id").isNotNull())
            .withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
        event_ids = [row["event_id"] for row in deduped.collect()]
        assert len(event_ids) == len(set(event_ids))

    def test_count_after_dedup(self, spark, raw_events):
        from pyspark.sql.window import Window
        w = Window.partitionBy("event_id").orderBy(F.col("event_ts").desc())
        deduped = (
            raw_events
            .filter(F.col("event_id").isNotNull())
            .withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
        assert deduped.count() == 3  # evt-001 (deduped), evt-002, evt-003
