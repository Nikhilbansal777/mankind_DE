import os, sys, json
from datetime import datetime, timezone
from src.utils.lineage import get_run_id, write_run_manifest, write_event, schema_fingerprint
RUN_ID = get_run_id()


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
# --- ADD: drift ledger + run id ---
from .._drift_recorder import record_schema_drift, record_unresolved_checks
import os
from datetime import datetime, timezone
RUN_ID = os.environ.get("RUN_ID") or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
# --- END ADD ---

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

# --- ADD: minimal drift detector (columns + optional types) ---
def _diff_vs_expected_yaml(df, table_name: str):
    """
    Look for <repo>/MKM_Data_Validation_and_cleaning/metadata/expected_schemas/latest/<table>_schema.yaml.
    Return (new_columns:list, removed_columns:list, type_changes:dict) — type_changes empty unless YAML carries types.
    """
    try:
        root = get_project_root()
        ypath = os.path.join(
            root,
            "MKM_Data_Validation_and_cleaning", "metadata", "expected_schemas",
            "latest", f"{table_name}_schema.yaml"
        )
        if not os.path.exists(ypath):
            return [], [], {}

        if yaml is None:
            return [], [], {}

        with open(ypath, "r", encoding="utf-8") as f:
            y = yaml.safe_load(f) or {}

        # tolerate shapes:
        #   {columns: {name: {type: ...}}} or {name: type} or [{name:..., type:...}]
        expected_cols = set()
        expected_types = {}

        if isinstance(y, dict) and "columns" in y:
            cols_obj = y["columns"]
            if isinstance(cols_obj, dict):
                for k, v in cols_obj.items():
                    expected_cols.add(k)
                    if isinstance(v, dict) and "type" in v:
                        expected_types[k] = str(v["type"]).lower()
            elif isinstance(cols_obj, list):
                for item in cols_obj:
                    if isinstance(item, dict) and item.get("name"):
                        expected_cols.add(item["name"])
                        if "type" in item:
                            expected_types[item["name"]] = str(item["type"]).lower()
        elif isinstance(y, dict):
            for k, v in y.items():
                expected_cols.add(k)
                if not isinstance(v, dict):
                    expected_types[k] = str(v).lower()
        elif isinstance(y, list):
            for item in y:
                if isinstance(item, dict) and item.get("name"):
                    expected_cols.add(item["name"])
                    if "type" in item:
                        expected_types[item["name"]] = str(item["type"]).lower()

        actual_cols = set(df.columns)
        new_cols = [c for c in df.columns if c not in expected_cols]
        removed = [c for c in expected_cols if c not in actual_cols]

        type_changes = {}
        if expected_types:
            actual_types = {name: str(t).lower() for name, t in df.dtypes}
            for c in (actual_cols & expected_cols):
                et = expected_types.get(c)
                at = actual_types.get(c)
                if et and at and et != at:
                    type_changes[c] = f"{et}->{at}"

        return new_cols, removed, type_changes
    except Exception:
        return [], [], {}
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

    # 1.1) --- ADD: drift diff vs expected YAML, then persist to ledger if any
    new_columns, removed_columns, type_changes = _diff_vs_expected_yaml(df, table_name)
    if new_columns:
        print(f"[SCHEMA NOTICE] New/unexpected columns detected: {new_columns}")
    if removed_columns:
        print(f"[SCHEMA NOTICE] Missing/removed columns: {removed_columns}")
    if type_changes:
        print(f"[SCHEMA NOTICE] Type changes: {type_changes}")
    if new_columns or removed_columns or type_changes:
        record_schema_drift(
            table=table_name,
            new_columns=new_columns or None,
            removed_columns=removed_columns or None,
            type_changes=type_changes or None,
            run_id=RUN_ID,
        )

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

    # 4.1) --- ADD: persist unresolved column requests
    if nn_unresolved or uq_unresolved:
        print(f"[PRE-VALIDATION][{table_name}] ⚠️ Unresolved columns -> NOT NULL: {nn_unresolved} | UNIQUE: {uq_unresolved}")
        record_unresolved_checks(
            table=table_name,
            not_null=nn_unresolved or None,
            unique=uq_unresolved or None,
            run_id=RUN_ID,
        )

    # add RUN_ID into the JSON you write
    report["run_id"] = RUN_ID
    with open(out, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    # lineage: ensure run manifest (idempotent)
    write_run_manifest("validation/pre", {
        "job": "pre_validation",
        "db_url": os.getenv("DB_URL")
    }, run_id=RUN_ID)

    # lineage: one event per table
    write_event("validation/pre", {
        "event": "pre_validation_complete",
        "table": table_name,
        "dataset_in": f"{os.getenv('DB_URL')}#{table_name}",
        "report_path": out,
        "row_count": report["row_count"],
        "schema_fp": schema_fingerprint(df),
        "unresolved_not_null": nn_unresolved,
        "unresolved_unique": uq_unresolved
    }, run_id=RUN_ID)


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
