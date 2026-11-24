import sys, os
import dlt
from pyspark.sql.functions import col, current_timestamp

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
            .select(col("key").cast("string").alias("kafka_key"),
                    col("value").cast("string").alias("raw_payload"),
                    "topic", "partition", "offset",
                    col("timestamp").alias("kafka_ts"),
                    current_timestamp().alias("ingest_ts")))