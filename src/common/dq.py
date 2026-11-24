from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import *

CATALOG = spark.conf.get("pipeline.catalog", "dev")

# 1. Expected message contract (nested JSON)
ORDER_SCHEMA = StructType([
    StructField("ordertime",  LongType()),
    StructField("orderid",    IntegerType()),
    StructField("itemid",     StringType()),
    StructField("orderunits", DoubleType()),
    StructField("address", StructType([
        StructField("city",    StringType()),
        StructField("state",   StringType()),
        StructField("zipcode", LongType()),
    ])),
])

# 2. Validation rules: name -> (SQL rule, reason code)
RULES = {
    "valid_json":       ("json_object_keys(raw_payload) IS NOT NULL", "E001_INVALID_JSON"),
    "order_id_present": ("order_id IS NOT NULL",                      "E003_NULL_ORDER_ID"),
    "valid_order_ts":   ("order_ts <= current_timestamp()",           "E004_BAD_ORDER_TS"),
    "positive_units":   ("order_units > 0",                           "E005_INVALID_UNITS"),
}

def failed_reasons():
    checks = [F.when(~F.coalesce(F.expr(rule), F.lit(False)), F.lit(code))
              for rule, code in RULES.values()]
    return F.filter(F.array(*checks), lambda c: c.isNotNull())

# 3. Parse + flatten + validate once
@dp.temporary_view(name="orders_parsed")
@dp.expect_all({name: rule for name, (rule, _) in RULES.items()})   # per-rule metrics in the UI
def orders_parsed():
    return (spark.readStream.table(f"{CATALOG}.bronze.events_raw")   # incremental: new rows only
            .withColumn("o", F.from_json("raw_payload", ORDER_SCHEMA))
            .select(
                F.col("o.orderid").alias("order_id"),
                F.col("o.itemid").alias("item_id"),
                F.col("o.orderunits").alias("order_units"),
                F.timestamp_millis("o.ordertime").alias("order_ts"),
                F.col("o.address.city").alias("city"),
                F.col("o.address.state").alias("state"),
                F.col("o.address.zipcode").alias("zipcode"),
                "raw_payload", "topic", "partition", "offset", "kafka_ts")
            .withColumn("order_date", F.to_date("order_ts"))
            .withColumn("dq_failed_reasons", failed_reasons()))

# 4. Valid rows -> Silver (deduplicated, partitioned by event date)
@dp.table(name=f"{CATALOG}.silver.orders",
          partition_cols=["order_date"],
          table_properties={"quality": "silver"})
def orders():
    return (spark.readStream.table("orders_parsed")
            .filter(F.size("dq_failed_reasons") == 0)
            .withWatermark("kafka_ts", "2 hours")
            .dropDuplicatesWithinWatermark(["order_id"])
            .drop("raw_payload", "dq_failed_reasons"))

# 5. Invalid rows -> Quarantine with reason codes
@dp.table(name=f"{CATALOG}.silver.orders_quarantine",
          table_properties={"quality": "quarantine"})
def orders_quarantine():
    return (spark.readStream.table("orders_parsed")
            .filter(F.size("dq_failed_reasons") > 0)
            .select(F.col("dq_failed_reasons")[0].alias("primary_reason_code"),
                    "dq_failed_reasons", "raw_payload",
                    "topic", "partition", "offset", "kafka_ts",
                    F.current_timestamp().alias("quarantined_at")))