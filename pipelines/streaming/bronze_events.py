import sys, os
import dlt
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql import functions as F

repo_root = spark.conf.get("pipeline.repo_root")
sys.path.insert(0, os.path.join(repo_root, "src"))
from common.config_loader import load_config, build_kafka_options

cfg        = load_config(os.path.join(repo_root, "config"), env=spark.conf.get("pipeline.env", "dev"))
kafka_opts = build_kafka_options(cfg, dbutils)
kafka_opts["startingOffsets"] = "earliest"     # first run only

@dlt.table(
    name="events_raw",
    comment="Raw Kafka events from Confluent",
    table_properties={"quality": "bronze"},
)
def events_raw():
    return (spark.readStream.format("kafka").options(**kafka_opts).load()
            .select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").alias("raw_value"),                       # original bytes, untouched
        # Schema Registry wire format = 0x00 + 4-byte schema id + payload
        F.expr("""CASE WHEN substring(value, 1, 1) = X'00'
                           THEN CAST(substring(value, 6) AS STRING)
                           ELSE CAST(value AS STRING) END""").alias("raw_payload"),
        F.expr("""CASE WHEN substring(value, 1, 1) = X'00'
                           THEN CAST(conv(hex(substring(value, 2, 4)), 16, 10) AS INT)
                      END""").alias("schema_id"),                    # which schema version produced it
        "topic", "partition", "offset",
        F.col("timestamp").alias("kafka_ts"),
        F.current_timestamp().alias("ingest_ts"),
    )
            .withColumn("ingest_date", F.to_date("ingest_ts")))