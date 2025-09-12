# MKM_Data_Profiling/profilers/run_all_profiles.py

import os, sys, yaml
from _base_profile import profile_once

# Default config (used if YAML missing keys)
DEFAULTS = {
    "value_frequencies": {
        "enabled": True,
        "sample_fraction": None,
        "only_types": ["string","boolean","tinyint","smallint","int","bigint"],
        "columns_include": None,
        "columns_exclude": [],
    }
}

def _merge(a, b):
    if not isinstance(b, dict): return a
    out = dict(a)
    for k, v in b.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _merge(out[k], v)
        else:
            out[k] = v
    return out

def main():
    # Load YAML config if present
    cfg_path = os.path.join(os.path.dirname(__file__), "profiling_config.yaml")
    cfg = {"tables": {}}
    if os.path.exists(cfg_path):
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or cfg

    tables = list((cfg.get("tables") or {}).keys())
    if not tables:
        # fallback to your 8 tables if YAML empty
        tables = [
            "orders","order_items","payments","order_status_history",
            "users","products","category","address"
        ]

    results = {}
    for t in tables:
        per_table_opts = _merge(DEFAULTS, (cfg.get("tables", {}).get(t) or {}))
        try:
            path = profile_once(t, logger_name=f"profilers.{t}", options=per_table_opts)
            results[t] = ("OK", path)
        except Exception as e:
            results[t] = ("ERROR", str(e))

    print("\n=== Profiling Summary ===")
    for t, (status, detail) in results.items():
        print(f"{t:22s} -> {status:6s} {detail}")

if __name__ == "__main__":
    main()
