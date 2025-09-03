import os, sys, json
from datetime import datetime, timezone

# --- project bootstrap (works no matter where you run it) ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# ------------------------------------------------------------

from src.utils.config_loader import load_env_and_get
from src.utils.path_utils import get_validation_report_path, get_project_root
from MKM_Data_Validation_and_cleaning.validators.pre_validations.validators_common.validation_checks import (
    check_not_null, check_unique
)

# Optional schema validator (DF-based as you requested earlier)
try:
    from MKM_Data_Validation_and_cleaning.metadata.schema_validator import validate_schema
    HAS_SCHEMA_VALIDATOR = True
except Exception:
    HAS_SCHEMA_VALIDATOR = False

# YAML loader for master cleaning rules (rename_columns)
try:
    import yaml
except Exception:
    yaml = None  # we'll handle gracefully if missing


# ------------------------- helpers ------------------------- #
def _load_master_rules():
    """
    Loads src/config/master_schema_cleaning_rules.yaml from repo root.
    Returns a dict (or {} if not found / YAML missing).
    """
    try:
        root = get_project_root()
        path = os.path.join(root, "src", "config", "master_schema_cleaning_rules.yaml")
        if not os.path.exists(path) or yaml is None:
            return {}
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def _resolve_requested_columns(df, table_name, requested_cols, master_rules):
    """
    Map logical/requested column names to actual *pre-clean* DF columns.

    Strategy (no UDFs, Spark-native):
      - If requested is a *target* in rename_columns (e.g., users_id), map back to its *source* first.
      - Try the requested name as-is.
      - If requested appears as a *source* in rename_columns, also try the *target*.
      - Try a safe heuristic candidate: <table>_id.
      - Keep order & dedupe; pick the first that exists in df.columns.
    """
    requested_cols = requested_cols or []
    df_cols = set(df.columns)

    # rename map for this table (source -> target)
    rename_map = {}
    try:
        rename_map = (master_rules.get("tables", {}) or {}).get(table_name, {}).get("rename_columns", {}) or {}
    except Exception:
        rename_map = {}

    # reverse (target -> source)
    reverse_map = {v: k for k, v in rename_map.items()} if rename_map else {}

    resolved = []
    unresolved = []
    mapping_detail = {}  # requested -> chosen_actual (or None)

    for req in requested_cols:
        candidates = []

        # if they passed the *target* name (post-clean), translate to source first
        if req in reverse_map:
            candidates.append(reverse_map[req])

        # original requested name
        candidates.append(req)

        # if req is a known source in rules, also try its target
        if req in rename_map:
            candidates.append(rename_map[req])

        # heuristic fallback
        candidates.append(f"{table_name}_id")

        # dedupe while preserving order
        seen = set()
        cand_final = []
        for c in candidates:
            if c and c not in seen:
                seen.add(c)
                cand_final.append(c)

        chosen = next((c for c in cand_final if c in df_cols), None)
        if chosen:
            resolved.append(chosen)
            mapping_detail[req] = chosen
        else:
            unresolved.append(req)
            mapping_detail[req] = None

    return resolved, unresolved, mapping_detail
# ----------------------------------------------------------- #


def run_prevalidate_for_df(df, table_name: str, not_null_cols=None, unique_cols=None):
    """
    Reusable DF-first validator (no UDFs).
    - Uses optional validate_schema(df, table_name) if available
    - Auto-resolves requested columns using master_schema_cleaning_rules.yaml and safe fallbacks
    - Writes JSON to pre_cleaning_validation_reports/<table>_pre_validation.json
    """
    not_null_cols = not_null_cols or []
    unique_cols   = unique_cols or []

    # 0) Load master rules (for rename_columns resolution)
    master_rules = _load_master_rules()

    # 1) Optional schema check (does not crash if YAML missing)
    if HAS_SCHEMA_VALIDATOR:
        try:
            validate_schema(df, table_name)
        except FileNotFoundError:
            print(f"[SCHEMA NOTICE] Expected schema YAML not found for {table_name}; skipping schema check.")

    # 2) Resolve requested column names against actual DF columns
    nn_resolved, nn_unresolved, nn_map = _resolve_requested_columns(df, table_name, not_null_cols, master_rules)
    uq_resolved, uq_unresolved, uq_map = _resolve_requested_columns(df, table_name, unique_cols,   master_rules)

    # 3) Basic data checks (Spark-native, no UDFs)
    issues = {
        "not_null": check_not_null(df, nn_resolved) if nn_resolved else {},
        "unique":   check_unique(df,   uq_resolved) if uq_resolved else {},
    }

    # 4) Write report
    report = {
        "table": table_name,
        "phase": "pre_cleaning_validation_reports",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "row_count": df.count(),
        "column_resolution": {
            "not_null": {
                "requested": not_null_cols,
                "resolved": nn_resolved,
                "unresolved": nn_unresolved,
                "mapping": nn_map,
            },
            "unique": {
                "requested": unique_cols,
                "resolved": uq_resolved,
                "unresolved": uq_unresolved,
                "mapping": uq_map,
            },
        },
        "issues": issues,
    }
    out = get_validation_report_path("pre_cleaning_validation_reports", f"{table_name}_pre_validation.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    print(f"[PRE-VALIDATION] ✅ saved: {out}")

    if nn_unresolved or uq_unresolved:
        print(f"[PRE-VALIDATION][{table_name}] ⚠️ Unresolved columns -> NOT NULL: {nn_unresolved} | UNIQUE: {uq_unresolved}")

    return issues


def run_prevalidate_via_jdbc(spark, table_name: str, not_null_cols=None, unique_cols=None):
    """
    Same as DF-first, but reads the DF via JDBC using an existing Spark session.
    Ideal for running many tables with a single Spark session.
    """
    load_env_and_get()
    jdbc_url = os.getenv("DB_URL")
    props = {
        "user": os.getenv("DB_USERNAME"),
        "password": os.getenv("DB_PASSWORD"),
        "driver": "com.mysql.cj.jdbc.Driver",
    }
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=props)
    return run_prevalidate_for_df(df, table_name, not_null_cols, unique_cols)


# Convenience wrapper to match your existing one-liner launchers
from src.connections.db_connections import spark_session_for_JDBC
def run_prevalidate(TABLE: str, not_null_cols=None, unique_cols=None):
    """
    So you can keep:
        from .run_prevalidate_common import run_prevalidate
        if __name__ == "__main__":
            run_prevalidate(TABLE="users", not_null_cols=[...], unique_cols=[...])
    """
    load_env_and_get()
    spark = spark_session_for_JDBC(app_name=f"prevalidate_{TABLE}")
    try:
        return run_prevalidate_via_jdbc(
            spark,
            table_name=TABLE,
            not_null_cols=not_null_cols,
            unique_cols=unique_cols,
        )
    finally:
        spark.stop()
