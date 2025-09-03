# MKM_Data_Validation_and_cleaning/utilities/generate_master_cleaning.py

import os
import sys
import json
import yaml

# --- Path bootstrap (so imports work no matter where you run from) ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# --------------------------------------------------------------------

# Constants ---->  Inputs / Outputs (canonicalized)
SCHEMA_JSON_PATH = os.path.join(
    "Data_cleaning", "MKM_Data_Validation_and_cleaning", "reports", "profiling_schema_summary", "latest_mysql_schema_summary.json"
)
PROFILING_DIR = os.path.join("Data_cleaning", "MKM_Data_Profiling", "profiling_reports", "profiling")
OUTPUT_YAML_PATH = os.path.join("Data_cleaning", "src", "config", "master_schema_cleaning_rules.yaml")


def _load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _safe_load_profile(table_name: str):
    """
    Load a table's profiling JSON if it exists; else return None.
    """
    path = os.path.join(PROFILING_DIR, f"{table_name}_profile.json")
    if os.path.exists(path):
        return _load_json(path)
    return None


def _ensure_rule_shape(rules: dict | None) -> dict:
    """
    Make sure the rules dict has all expected top-level keys.
    """
    base = dict(rules) if rules else {}
    base.setdefault("rename_columns", {})
    base.setdefault("type_conversions", {})
    base.setdefault("null_replacements", {})
    base.setdefault("case_formatting", {})
    base.setdefault("standardize_units", {})
    # Optional: base.setdefault("drop_columns", [])
    # Optional: base.setdefault("filters", [])
    return base


def _infer_boolean_from_freq(freq_list):
    """
    freq_list may be:
      - list of [value, count]
      - list of {"value": <v>, "count": <n>}
    Return True if values look boolean-like.
    """
    if not isinstance(freq_list, list):
        return False

    observed = set()
    for item in freq_list:
        if isinstance(item, dict) and "value" in item:
            v = item["value"]
        elif isinstance(item, (list, tuple)) and len(item) >= 1:
            v = item[0]
        else:
            continue
        observed.add(str(v).strip().lower())

    return bool(observed) and observed.issubset({"0", "1", "true", "false", "yes", "no"})


def enrich_with_profiling(table_name: str, schema_rules: dict | None) -> dict:
    """
    Merge the schema exploration rules with profiling-derived hints.
    We keep this conservative to avoid false positives.
    """
    prof = _safe_load_profile(table_name)
    rules = _ensure_rule_shape(schema_rules)

    if not prof:
        # No profiling file; return the exploration-only rules.
        return rules

    # --- Heuristic 1: rename plain 'id' to '<table>_id' if present in prof datatypes and not already renamed
    data_types = prof.get("data_types", {})
    if "id" in data_types and "id" not in rules["rename_columns"]:
        rules["rename_columns"]["id"] = f"{table_name}_id"

    # --- Heuristic 2: boolean inference (only if no explicit type already set)
    # value_frequencies: { col: [[value, count], ...] } OR { col: [{"value":..,"count":..}, ...] }
    freq_map = prof.get("value_frequencies", {})
    if isinstance(freq_map, dict):
        for col, freqs in freq_map.items():
            # skip if the user already set a cast
            if col in rules["type_conversions"]:
                continue
            if _infer_boolean_from_freq(freqs):
                rules["type_conversions"][col] = "BooleanType"

    # (Optional & risky) Null replacement heuristics need row_count which we don't persist in profiling.
    # If you later add 'row_count' into profiling output, you can revive a guarded heuristic here.

    return rules


def main():
    if not os.path.exists(SCHEMA_JSON_PATH):
        raise FileNotFoundError(f"❌ Schema summary not found: {SCHEMA_JSON_PATH}")

    schema_summary = _load_json(SCHEMA_JSON_PATH)

    # Expect shape: {"version": "v1", "generated_at": "...", "tables": {<table>: {rules...}}}
    tables_dict = schema_summary.get("tables", {})
    if not isinstance(tables_dict, dict):
        raise ValueError("❌ 'tables' key missing or invalid in schema summary JSON.")

    final_rules = {}
    for table_name, base in tables_dict.items():
        # base is the exploration-enriched skeleton for this table
        final_rules[table_name] = enrich_with_profiling(table_name, base)

    yaml_obj = {
        "version": schema_summary.get("version", "v1"),
        "generated_at": schema_summary.get("generated_at"),
        "tables": final_rules,
    }

    os.makedirs(os.path.dirname(OUTPUT_YAML_PATH), exist_ok=True)
    with open(OUTPUT_YAML_PATH, "w", encoding="utf-8") as f:
        yaml.dump(yaml_obj, f, sort_keys=False)

    print(f"✅ Cleaning YAML created: {OUTPUT_YAML_PATH}")


if __name__ == "__main__":
    main()








# import os
# import sys
# import json
# import yaml

# # --- Path injection ---
# project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
# if project_root not in sys.path:
#     sys.path.insert(0, project_root)
# from project_bootstrap import bootstrap_project_paths
# bootstrap_project_paths(__file__)

# # # --- Imports ---
# # from src.utils.path_utils import get_local_output_path

# # --- Constants ---
# SCHEMA_JSON_PATH = os.path.join("MKM_Data_Validation_and_cleaning", "reports", "latest_mysql_schema_summary.json")
# PROFILING_DIR = os.path.join("MKM_Data_Profiling", "profiling_reports", "profiling")
# OUTPUT_YAML_PATH = os.path.join("Data_cleaning", "src", "config", "master_schema_cleaning_rules.yaml")


# def enrich_with_profiling(table_name, schema_rules):
#     profile_path = os.path.join(PROFILING_DIR, f"{table_name}_profile.json")
#     if not os.path.exists(profile_path):
#         print(f"⚠️ Skipping profiling for: {table_name}")
#         return schema_rules

#     with open(profile_path, "r") as f:
#         profile = json.load(f)

#     enriched = schema_rules.copy()

#     # --- Preserve existing type_conversions (e.g., TimestampType)
#     enriched.setdefault("type_conversions", {})
#     type_conversions = enriched["type_conversions"]

#     # --- Rename "id" to table-specific name if not already done
#     enriched.setdefault("rename_columns", {})
#     if "id" in profile.get("data_types", {}) and "id" not in enriched["rename_columns"]:
#         enriched["rename_columns"]["id"] = f"{table_name}_id"

#     # --- Null Replacements ---
#     enriched["null_replacements"] = {}
#     null_counts = profile.get("null_counts", {})
#     row_count = profile.get("row_count", 1)

#     for col, val in null_counts.items():
#         nulls = val.get("nulls", val) if isinstance(val, dict) else val
#         try:
#             null_pct = (nulls / row_count) * 100
#             if null_pct > 50.0:
#                 enriched["null_replacements"][col] = "UNKNOWN"
#         except Exception as e:
#             print(f"[WARN] Failed null_pct calc for {col}: {e}")

#     # --- Boolean Detection (ONLY if not already set)
#     freq_data = profile.get("value_frequencies", {})
#     for col, freqs in freq_data.items():
#         if col in type_conversions:
#             continue
#         if isinstance(freqs, list):
#             values = [item.get("value") for item in freqs if isinstance(item, dict) and "value" in item]
#             str_vals = {str(v).strip().lower() for v in values}
#             if str_vals and str_vals.issubset({"0", "1", "true", "false", "yes", "no"}):
#                 type_conversions[col] = "BooleanType"

#     # --- Default other fields ---
#     enriched.setdefault("standardize_units", {})
#     enriched.setdefault("case_formatting", {})

#     return enriched


# # --- Main function ---
# def main():
#     print("📄 Generating YAML cleaning rules from JSON summary and profiling...")

#     if not os.path.exists(SCHEMA_JSON_PATH):
#         raise FileNotFoundError(f"❌ Schema summary not found: {SCHEMA_JSON_PATH}")

#     with open(SCHEMA_JSON_PATH, "r") as f:
#         schema_summary = json.load(f)

#     enriched_rules = {}

#     for table_name, rules in schema_summary.get("tables", {}).items():
#         print(f"🔍 Processing table: {table_name}")
#         enriched_rules[table_name] = enrich_with_profiling(table_name, rules)

#     yaml_obj = {
#         "version": schema_summary.get("version", "v1"),
#         "generated_at": schema_summary.get("generated_at"),
#         "tables": enriched_rules
#     }

#     os.makedirs(os.path.dirname(OUTPUT_YAML_PATH), exist_ok=True)
#     with open(OUTPUT_YAML_PATH, "w") as f:
#         yaml.dump(yaml_obj, f, sort_keys=False)

#     print(f"\n✅ Cleaning YAML created: {OUTPUT_YAML_PATH}")


# if __name__ == "__main__":
#     main()
