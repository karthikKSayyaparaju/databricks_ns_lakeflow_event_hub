"""DQ gate — runs before the Gold refresh.

Checks the last N hours of Silver vs quarantine against thresholds in config/dq_rules.yaml.
If breached and on_breach=fail, the task fails -> Gold is NOT refreshed and the job alerts.
"""
import argparse
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--repo-root", required=True)
parser.add_argument("--env", default="dev")
parser.add_argument("--dataset", default="silver_orders")
args = parser.parse_args()

sys.path.insert(0, os.path.join(args.repo_root, "src"))
from common.config_loader import load_config  # noqa: E402

spark = SparkSession.builder.getOrCreate()
cfg = load_config(os.path.join(args.repo_root, "config"), env=args.env)
th = cfg["dq_rules"][args.dataset]["thresholds"]

catalog, silver = cfg["catalog"], cfg["schemas"]["silver"]
since = F.expr(f"current_timestamp() - INTERVAL {int(th['lookback_hours'])} HOURS")

valid = spark.table(f"{catalog}.{silver}.orders").filter(F.col("ingest_ts") >= since).count()
quarantine_df = spark.table(f"{catalog}.{silver}.orders_quarantine").filter(F.col("quarantined_at") >= since)
quarantined = quarantine_df.count()

total = valid + quarantined
rate = (quarantined / total * 100) if total else 0.0

print(f"[DQ gate] env={args.env} window={th['lookback_hours']}h "
      f"valid={valid} quarantined={quarantined} rate={rate:.2f}%")
if quarantined:
    print("[DQ gate] quarantine by reason code:")
    (quarantine_df.select(F.explode("dq_failed_reasons").alias("reason_code"))
        .groupBy("reason_code").count().orderBy(F.desc("count")).show(truncate=False))

breaches = []
if rate > float(th["max_quarantine_rate_pct"]):
    breaches.append(f"quarantine rate {rate:.2f}% > {th['max_quarantine_rate_pct']}%")
if valid < int(th["min_valid_rows"]):
    breaches.append(f"only {valid} valid rows in last {th['lookback_hours']}h (min {th['min_valid_rows']})")

if breaches:
    msg = "DQ gate breached: " + "; ".join(breaches)
    if th.get("on_breach", "fail") == "fail":
        raise RuntimeError(msg)   # fails the task -> Gold refresh skipped, failure alert sent
    print("[DQ gate] WARNING:", msg)
else:
    print("[DQ gate] PASSED")
