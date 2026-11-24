"""SILVER — parse + flatten JSON, validate with dynamic rules, dedupe, route bad rows to quarantine.

Flow:  bronze.events_raw
          └─> orders_parsed (temp view: parse, flatten, compute dq_failed_reasons, per-rule metrics)
                 ├─> silver.orders             (valid rows, deduplicated)
                 └─> silver.orders_quarantine  (invalid rows + reason codes)
"""
import os
import sys

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType)

REPO_ROOT = spark.conf.get("pipeline.repo_root", "/Workspace/Shared/databricks_ns_lakeflow_event_hub")
ENV = spark.conf.get("pipeline.env", "dev")
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

from common.config_loader import load_config  # noqa: E402
from common.dq import build_rules, expectations, failed_reasons  # noqa: E402

cfg = load_config(os.path.join(REPO_ROOT, "config"), env=ENV)
CATALOG = cfg["catalog"]
BRONZE, SILVER = cfg["schemas"]["bronze"], cfg["schemas"]["silver"]

RULES = build_rules(cfg["dq_rules"]["silver_orders"])

# Expected message contract (Confluent Datagen "orders" quickstart).
# Verify against a real message:  SELECT raw_payload FROM <catalog>.bronze.events_raw LIMIT 5
ORDER_SCHEMA = StructType([
    StructField("ordertime", LongType()),    # epoch millis
    StructField("orderid", IntegerType()),
    StructField("itemid", StringType()),
    StructField("orderunits", DoubleType()),
    StructField("address", StructType([
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("zipcode", LongType()),
    ])),
])


# ---------------------------------------------------------------------------
# 1) Parse + flatten + validate once (temporary view, not persisted)
# ---------------------------------------------------------------------------
@dp.temporary_view(name="orders_parsed")
@dp.expect_all(expectations(RULES, "quarantine"))  # warn-mode on the view = per-rule pass/fail metrics in the UI
def orders_parsed():
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE}.events_raw")   # incremental: only new Bronze rows
        .withColumn("o", F.from_json("raw_payload", ORDER_SCHEMA))
        .select(
            F.col("o.orderid").alias("order_id"),
            F.col("o.itemid").alias("item_id"),
            F.col("o.orderunits").alias("order_units"),
            F.timestamp_millis("o.ordertime").alias("order_ts"),
            F.col("o.address.city").alias("city"),
            F.col("o.address.state").alias("state"),
            F.col("o.address.zipcode").alias("zipcode"),
            "raw_payload", "topic", "partition", "offset", "kafka_ts", "ingest_ts",
        )
        .withColumn("order_date", F.to_date("order_ts"))
        .withColumn("dq_failed_reasons", failed_reasons(RULES))
    )


# ---------------------------------------------------------------------------
# 2) Valid rows -> Silver (deduplicated, partitioned by event date)
# ---------------------------------------------------------------------------
@dp.table(
    name=f"{CATALOG}.{SILVER}.orders",
    comment="Validated, flattened, de-duplicated orders.",
    table_properties={"quality": "silver", "delta.autoOptimize.optimizeWrite": "true"},
    partition_cols=["order_date"],
)
@dp.expect_all(expectations(RULES, "warn"))                    # soft rules: keep row, record metric
@dp.expect_or_fail("no_quarantined_rows_leak", "order_id IS NOT NULL")  # safety net
def orders():
    return (
        spark.readStream.table("orders_parsed")
        .filter(F.size("dq_failed_reasons") == 0)
        # Kafka is at-least-once -> redelivered messages arrive close together in ingestion time,
        # so watermark on kafka_ts (not order_ts, which can be far in the past) and dedupe on business key.
        .withWatermark("kafka_ts", "2 hours")
        .dropDuplicatesWithinWatermark(["order_id"])
        .select(
            "order_id", "item_id", "order_units", "order_ts", "order_date",
            "city", "state", "zipcode",
            "topic", "partition", "offset", "kafka_ts", "ingest_ts",
        )
    )


# ---------------------------------------------------------------------------
# 3) Invalid rows -> Quarantine (raw payload kept for replay/debug)
# ---------------------------------------------------------------------------
@dp.table(
    name=f"{CATALOG}.{SILVER}.orders_quarantine",
    comment="Rows that failed validation, with reason codes.",
    table_properties={"quality": "quarantine"},
    partition_cols=["quarantine_date"],
)
def orders_quarantine():
    return (
        spark.readStream.table("orders_parsed")
        .filter(F.size("dq_failed_reasons") > 0)
        .select(
            F.col("dq_failed_reasons")[0].alias("primary_reason_code"),
            "dq_failed_reasons",
            "raw_payload", "topic", "partition", "offset", "kafka_ts", "ingest_ts",
            F.current_timestamp().alias("quarantined_at"),
        )
        .withColumn("quarantine_date", F.to_date("quarantined_at"))
    )
