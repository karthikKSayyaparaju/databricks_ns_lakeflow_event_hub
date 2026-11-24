import sys, os

repo_root = "/Workspace/Shared/databricks_ns_lakeflow_event_hub"
sys.path.insert(0, os.path.join(repo_root, "src"))

from common.config_loader import load_config, build_kafka_options

config_dir = os.path.join(repo_root, "config")
print(os.listdir(config_dir))      # should show base.yaml, dev.yaml, ...

cfg = load_config(config_dir, env="dev")
kafka_opts = build_kafka_options(cfg, dbutils)

df = spark.read.format("kafka").options(**kafka_opts).option("endingOffsets", "latest").load()
display(df.selectExpr("CAST(value AS STRING) AS raw_payload", "offset", "timestamp"))

bronze_table = f'{cfg["catalog"]}.{cfg["schemas"]["bronze"]}.events_raw'
print(bronze_table)   # alchemy_dev.bronze.events_raw