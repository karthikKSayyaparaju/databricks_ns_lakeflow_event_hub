import os
import yaml

def _deep_merge(base: dict, override: dict) -> dict:
    out = dict(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out

def _read_yaml(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        return yaml.safe_load(f) or {}

def load_config(config_dir: str, env: str = "dev") -> dict:
    """Merge base.yaml + <env>.yaml and attach dq_rules.yaml."""
    cfg = _read_yaml(os.path.join(config_dir, "base.yaml"))
    cfg = _deep_merge(cfg, _read_yaml(os.path.join(config_dir, f"{env}.yaml")))
    cfg["dq_rules"] = _read_yaml(os.path.join(config_dir, "dq_rules.yaml"))
    cfg["env"] = env
    return cfg

def build_kafka_options(cfg: dict, dbutils) -> dict:
    k = cfg["kafka"]
    s = k["secrets"]
    api_key    = dbutils.secrets.get(s["scope"], s["key_name"])
    api_secret = dbutils.secrets.get(s["scope"], s["secret_name"])
    return {
        "kafka.bootstrap.servers": k["bootstrap_servers"],
        "kafka.security.protocol": k["security_protocol"],
        "kafka.sasl.mechanism":    k["sasl_mechanism"],
        "kafka.sasl.jaas.config": (
            "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{api_key}" password="{api_secret}";'
        ),
        "subscribe":             k["topic"],
        "startingOffsets":       k["starting_offsets"],
        "maxOffsetsPerTrigger":  str(k["max_offsets_per_trigger"]),
    }