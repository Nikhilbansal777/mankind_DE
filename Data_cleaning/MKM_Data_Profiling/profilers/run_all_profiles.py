# MKM_Data_Profiling/profilers/run_all_profiles.py

import os, yaml
from _base_profile import profile_once
from src.utils.lineage import get_run_id, write_run_manifest, write_event

RUN_ID = get_run_id()

# Create manifest once per process
write_run_manifest("profiling", {
    "job": "profiling/run_all_profiles.py",
    "db_url": os.getenv("DB_URL"),
}, run_id=RUN_ID)

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

    # ---- batch start event
    write_event("profiling", {
        "event": "profiling_batch_start",
        "tables": tables,
    }, run_id=RUN_ID)

    results = {}
    for t in tables:
        per_table_opts = _merge(DEFAULTS, (cfg.get("tables", {}).get(t) or {}))
        try:
            path = profile_once(t, logger_name=f"profilers.{t}", options=per_table_opts)
            results[t] = ("OK", path)
            write_event("profiling", {
                "event": "profile_complete",
                "table": t,
                "report_path": path,
            }, run_id=RUN_ID)
        except Exception as e:
            results[t] = ("ERROR", str(e))
            write_event("profiling", {
                "event": "profile_error",
                "table": t,
                "error": str(e),
            }, run_id=RUN_ID)

    # ---- batch finish event
    ok = sum(1 for s, _ in results.values() if s == "OK")
    err = sum(1 for s, _ in results.values() if s == "ERROR")
    write_event("profiling", {
        "event": "profiling_batch_finish",
        "ok": ok,
        "errors": err,
    }, run_id=RUN_ID)

    print("\n=== Profiling Summary ===")
    for t, (status, detail) in results.items():
        print(f"{t:22s} -> {status:6s} {detail}")

if __name__ == "__main__":
    main()
