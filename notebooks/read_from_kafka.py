import sys, os, json

repo_root = "/Workspace/Shared/databricks_ns_lakeflow_event_hub"
sys.path.insert(0, os.path.join(repo_root, "src"))

from common.config_loader import load_config, build_kafka_options
from pyspark.sql.functions import col, current_timestamp

# ---------- Config ----------
cfg        = load_config(os.path.join(repo_root, "config"), env="dev")
kafka_opts = build_kafka_options(cfg, dbutils)
kafka_opts["startingOffsets"] = "earliest"   # used ONLY when checkpoint is empty
kafka_opts["failOnDataLoss"]  = "false"      # dev: don't fail if retention deleted old offsets

catalog = cfg["catalog"]
bronze  = cfg["schemas"]["bronze"]
volume  = cfg["checkpoint"]["volume"]

bronze_table    = f"{catalog}.{bronze}.events_raw"
checkpoint_path = f"/Volumes/{catalog}/{bronze}/{volume}/{cfg['checkpoint']['bronze_events']}"

# ---------- One-time setup: schema + volume for checkpoints ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA  IF NOT EXISTS {catalog}.{bronze}")
spark.sql(f"CREATE VOLUME  IF NOT EXISTS {catalog}.{bronze}.{volume}")

# ---------- Is this a first run? ----------
def checkpoint_exists(path: str) -> bool:
    try:
        return len(dbutils.fs.ls(f"{path}/offsets")) > 0
    except Exception:
        return False

def show_latest_offsets(path: str):
    files = sorted(dbutils.fs.ls(f"{path}/offsets"),
                     key=lambda f: int(f.name) if f.name.isdigit() else -1)
    latest = files[-1]
    content = dbutils.fs.head(latest.path).splitlines()
    print(f"Batch {latest.name} offsets:")
    print(json.dumps(json.loads(content[-1]), indent=2))   # last line = {"topic": {"partition": offset}}

    
if checkpoint_exists(checkpoint_path):
    print(f"Checkpoint found → resuming from saved offsets\n  {checkpoint_path}")
    show_latest_offsets(checkpoint_path)
else:
    print(f"No checkpoint → starting from EARLIEST\n  {checkpoint_path}")

# ---------- Read stream from Kafka ----------
raw = (spark.readStream
         .format("kafka")
         .options(**kafka_opts)
         .load())

bronze_df = raw.select(
    col("key").cast("string").alias("kafka_key"),
    col("value").cast("string").alias("raw_payload"),
    "topic", "partition", "offset",
    col("timestamp").alias("kafka_ts"),
    current_timestamp().alias("ingest_ts"),
)

# ---------- Write to Bronze with checkpointing ----------
query = (bronze_df.writeStream
           .format("delta")
           .outputMode("append")
           .option("checkpointLocation", checkpoint_path)
           .trigger(availableNow=True)      # process everything new, then stop
           .toTable(bronze_table))

query.awaitTermination()
print(f"Done. Rows in {bronze_table}: {spark.table(bronze_table).count()}")