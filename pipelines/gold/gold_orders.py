"""GOLD — curated aggregates for analytics (materialized views, refreshed on a schedule).

Runs in a separate TRIGGERED pipeline (hourly via a Job). Materialized views on serverless
refresh incrementally where possible, so only changed Silver data is reprocessed.
"""
import os
import sys

from pyspark import pipelines as dp
from pyspark.sql import functions as F

REPO_ROOT = spark.conf.get("pipeline.repo_root", "/Workspace/Shared/databricks_ns_lakeflow_event_hub")
ENV = spark.conf.get("pipeline.env", "dev")
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

from common.config_loader import load_config  # noqa: E402

cfg = load_config(os.path.join(REPO_ROOT, "config"), env=ENV)
CATALOG = cfg["catalog"]
SILVER, GOLD = cfg["schemas"]["silver"], cfg["schemas"]["gold"]

SILVER_ORDERS = f"{CATALOG}.{SILVER}.orders"
SILVER_QUARANTINE = f"{CATALOG}.{SILVER}.orders_quarantine"


@dp.materialized_view(
    name=f"{CATALOG}.{GOLD}.daily_orders_by_state",
    comment="Daily order KPIs per state.",
    table_properties={"quality": "gold"},
    cluster_by=["order_date", "state"],
)
def daily_orders_by_state():
    return (
        spark.read.table(SILVER_ORDERS)
        .groupBy("order_date", "state")
        .agg(
            F.count("order_id").alias("order_count"),
            F.round(F.sum("order_units"), 2).alias("total_units"),
            F.round(F.avg("order_units"), 2).alias("avg_units_per_order"),
            F.max("order_ts").alias("last_order_ts"),
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.{GOLD}.hourly_order_volume",
    comment="Order volume per event hour (time-series dashboards).",
    table_properties={"quality": "gold"},
    cluster_by=["order_hour"],
)
def hourly_order_volume():
    return (
        spark.read.table(SILVER_ORDERS)
        .withColumn("order_hour", F.date_trunc("hour", "order_ts"))
        .groupBy("order_hour")
        .agg(
            F.count("order_id").alias("order_count"),
            F.round(F.sum("order_units"), 2).alias("total_units"),
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.{GOLD}.daily_item_sales",
    comment="Daily units and orders per item.",
    table_properties={"quality": "gold"},
    cluster_by=["order_date"],
)
def daily_item_sales():
    return (
        spark.read.table(SILVER_ORDERS)
        .groupBy("order_date", "item_id")
        .agg(
            F.count("order_id").alias("order_count"),
            F.round(F.sum("order_units"), 2).alias("total_units"),
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.{GOLD}.dq_daily_summary",
    comment="Quarantined records per day and reason code (data-quality monitoring).",
    table_properties={"quality": "gold"},
)
def dq_daily_summary():
    return (
        spark.read.table(SILVER_QUARANTINE)
        .withColumn("reason_code", F.explode("dq_failed_reasons"))
        .groupBy("quarantine_date", "reason_code")
        .agg(F.count("*").alias("failed_records"))
    )
