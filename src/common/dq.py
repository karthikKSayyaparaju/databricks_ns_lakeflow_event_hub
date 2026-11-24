"""DQ helpers (plain Python module, imported by pipeline source files).

No `spark` / dataset definitions here: this module is imported via sys.path, so pipeline
globals like `spark` are not available, and datasets must only be defined in pipeline sources.
"""
from pyspark.sql import functions as F


def build_rules(rule_cfg: dict) -> list:
    """Turn the dq_rules.yaml section into a list of {name, rule, reason_code, action}."""
    rules = []
    required = rule_cfg.get("required_fields") or []
    if required:
        keys = ", ".join(f"'{f}'" for f in required)
        rules.append({
            "name": "required_fields_present",
            "rule": (f"size(array_except(array({keys}), "
                     f"coalesce(json_object_keys(raw_payload), CAST(array() AS ARRAY<STRING>)))) = 0"),
            "reason_code": rule_cfg.get("missing_fields_reason_code", "E002_MISSING_FIELDS"),
            "action": "quarantine",
        })
    for r in rule_cfg.get("rules") or []:
        rules.append({
            "name": r["name"],
            "rule": r["rule"],
            "reason_code": r["reason_code"],
            "action": r.get("action", "quarantine"),
        })
    return rules


def expectations(rules: list, action: str) -> dict:
    """{name: rule} for the rules with the given action (for @dp.expect_all)."""
    return {r["name"]: r["rule"] for r in rules if r["action"] == action}


def failed_reasons(rules: list):
    """Array of reason codes for failed quarantine rules (NULL result counts as failed)."""
    checks = [F.when(~F.coalesce(F.expr(r["rule"]), F.lit(False)), F.lit(r["reason_code"]))
              for r in rules if r["action"] == "quarantine"]
    return F.filter(F.array(*checks), lambda c: c.isNotNull())
