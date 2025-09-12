# Data_cleaning/MKM_Data_Validation_and_cleaning/validators/post_validations/run_postvalidate_common.py

# --- bootstrap (keep at very top) ---
import sys, json
from pathlib import Path
HERE = Path(__file__).resolve()
REPO_ROOT = next(p for p in [HERE.parent] + list(HERE.parents) if (p / "project_bootstrap.py").exists())
sys.path.insert(0, str(REPO_ROOT))
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# ------------------------------------

from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from src.connections.db_connections import spark_session_for_JDBC
from src.utils.config_loader import load_env_and_get
from src.utils.path_utils import cleaning_output_paths, get_validation_report_path, get_lineage_path

# import checks from your toolbox package (relative import)
from .post_validator_common.post_validation_checks import (
    check_not_null, check_unique, check_columns_present, check_allowed_values,
    check_value_range, check_regex, check_cast_success, check_foreign_key,
    check_rowcount_delta, check_renamed_id
)

def _read_df(spark: SparkSession, path: str, file_format: str):
    if file_format == "json":
        return spark.read.json(path)
    elif file_format == "parquet":
        return spark.read.parquet(path)
    elif file_format == "csv":
        return spark.read.options(header=True, inferSchema=True).csv(path)
    else:
        raise ValueError(f"Unsupported format: {file_format}")

def _split_existing(seq: Optional[List[str]], cols_set: set) -> Tuple[List[str], List[str]]:
    seq = seq or []
    exist = [c for c in seq if c in cols_set]
    miss  = [c for c in seq if c not in cols_set]
    return exist, miss

def _filter_rule_map(rule_map: Optional[Dict[str, Any]], cols_set: set) -> Tuple[Dict[str, Any], List[str]]:
    filtered, missing = {}, []
    for col, spec in (rule_map or {}).items():
        if col in cols_set:
            filtered[col] = spec
        else:
            missing.append(col)
    return filtered, missing

def _filter_rule_list(rule_list: Optional[List[Dict[str, Any]]], cols_set: set) -> Tuple[List[Dict[str, Any]], List[str]]:
    filtered, missing = [], []
    for spec in (rule_list or []):
        col = spec.get("col")
        if col in cols_set:
            filtered.append(spec)
        else:
            missing.append(col)
    return filtered, missing

def _write_json(obj: Dict[str, Any], phase_dir: str, filename: str):
    """
    Pure-Python safe writer for reports/lineage. Handles datetime via default=str.
    """
    out = get_validation_report_path(phase_dir, filename)
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, default=str)
    return out

def run_postvalidate(
    TABLE: str,
    file_format: str = "json",
    not_null_cols: Optional[List[str]] = None,
    unique_cols: Optional[List[str]] = None,
    require_renamed_id: bool = True,
    required_cols: Optional[List[str]] = None,             # Columns that must exist
    forbidden_cols: Optional[List[str]] = None,            # Columns that must NOT exist
    expected_rowcount: Optional[int] = None,               # Expected row count for validation
    min_retain_ratio: float = 0.8,                        # Minimum ratio of expected rows to retain
    enum_rules: Optional[Dict[str, List[Any]]] = None,     # {col: [allowed,...]}
    range_rules: Optional[List[Dict[str, Any]]] = None,    # [{"col":"price","min":0}]
    regex_rules: Optional[List[Dict[str, str]]] = None,    # [{"col":"email","pattern":".+@.+"}]
    cast_rules: Optional[Dict[str, str]] = None,           # {col: "int"/"double"/"timestamp"/...}
    fk_rules: Optional[List[Dict[str, str]]] = None,       # [{"fk_col":"user_id","ref_table":"users","ref_key":"users_id"}]
) -> Dict[str, Any]:
    """
    Loads cleaned <TABLE> from cleaned_outputs/, runs checks, writes post_validation_reports JSON.
    Also drops a *lineage* JSON alongside (samples only) — no Spark writes here.
    Defensive: skips checks for columns that don't exist and records them under issues["_unresolved"].
    """
    load_env_and_get()
    spark = spark_session_for_JDBC(app_name=f"postvalidate_{TABLE}")

    try:
        cleaned_path = cleaning_output_paths(TABLE, file_format=file_format)
        df = _read_df(spark, cleaned_path, file_format)
        cols_set = set(df.columns)

        issues: Dict[str, Any] = {}
        unresolved: Dict[str, Any] = {}

        # ----- defensively filter rules by actual DF columns -----
        not_null_cols, miss_nn = _split_existing(not_null_cols, cols_set)
        unique_cols,   miss_uq = _split_existing(unique_cols, cols_set)

        enum_rules,  miss_enum  = _filter_rule_map(enum_rules,  cols_set)
        cast_rules,  miss_cast  = _filter_rule_map(cast_rules,  cols_set)
        range_rules, miss_range = _filter_rule_list(range_rules, cols_set)
        regex_rules, miss_regex = _filter_rule_list(regex_rules, cols_set)

        # FK rules: keep only those where fk_col exists in this DF
        fk_rules = fk_rules or []
        fk_rules_filtered, fk_missing = [], []
        for spec in fk_rules:
            fk_col = spec.get("fk_col")
            if fk_col in cols_set:
                fk_rules_filtered.append(spec)
            else:
                fk_missing.append(fk_col)
        fk_rules = fk_rules_filtered

        if miss_nn:    unresolved["not_null_missing_cols"] = miss_nn
        if miss_uq:    unresolved["unique_missing_cols"]   = miss_uq
        if miss_enum:  unresolved["enum_missing_cols"]     = miss_enum
        if miss_cast:  unresolved["cast_missing_cols"]     = miss_cast
        if miss_range: unresolved["range_missing_cols"]    = miss_range
        if miss_regex: unresolved["regex_missing_cols"]    = miss_regex
        if fk_missing: unresolved["fk_missing_cols"]       = fk_missing

        row_count = df.count()

        # ----- row count validation -----
        if expected_rowcount is not None:
            issues["rowcount_delta"] = check_rowcount_delta(expected_rowcount, row_count, min_retain_ratio)

        # ----- required ID convention check -----
        if require_renamed_id:
            issues["renamed_id"] = check_renamed_id(df, TABLE)

        # ----- column presence check -----
        if required_cols or forbidden_cols:
            issues["columns_present"] = check_columns_present(df, required_cols or [], forbidden_cols)

        # ----- basic checks -----
        if not_null_cols:
            issues["not_null"] = check_not_null(df, not_null_cols)

        if unique_cols:
            issues["unique"] = check_unique(df, unique_cols)

        # ----- enums/ranges/regex/casts -----
        if enum_rules:
            issues["enums"] = {col: check_allowed_values(df, col, allowed)
                               for col, allowed in enum_rules.items()}

        if range_rules:
            issues["ranges"] = [check_value_range(df, r["col"], r.get("min"), r.get("max"), r.get("inclusive", True))
                                for r in range_rules]

        if regex_rules:
            issues["regex"] = [check_regex(df, r["col"], r["pattern"]) for r in regex_rules]

        if cast_rules:
            issues["casts"] = {col: check_cast_success(df, col, tgt) for col, tgt in cast_rules.items()}

        # ----- foreign keys -----
        if fk_rules:
            fk_results, fk_ref_errors = [], []
            for spec in fk_rules:
                fk_col = spec["fk_col"]
                ref_table = spec["ref_table"]
                ref_key = spec.get("ref_key", f"{ref_table}_id")
                try:
                    ref_path = cleaning_output_paths(ref_table, file_format=file_format)
                    ref_df = _read_df(spark, ref_path, file_format)
                    fk_results.append(check_foreign_key(df, fk_col, ref_df, ref_key))
                except Exception as e:
                    fk_ref_errors.append({"fk_col": fk_col, "ref_table": ref_table, "ref_key": ref_key, "error": str(e)})
            if fk_results:
                issues["foreign_keys"] = fk_results
            if fk_ref_errors:
                unresolved["fk_ref_read_errors"] = fk_ref_errors

        if unresolved:
            issues["_unresolved"] = unresolved

        report = {
            "table": TABLE,
            "phase": "post_validation_reports",
            "row_count": row_count,
            "file_format": file_format,
            "output_dir": cleaned_path,
            "columns": sorted(df.columns),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "issues": issues,
        }

        # Write the canonical validation report (JSON)
        out = _write_json(report, "post_validation_reports", f"{TABLE}_post_validation.json")
        print(f"[POST-VALIDATION] ✅ saved: {out}")

        # Also write a lineage JSON (same payload; downstream can read samples from here)
        lineage_out = get_lineage_path(f"{TABLE}_lineage.json")
        with open(lineage_out, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, default=str)
        print(f"[POST-VALIDATION] 🧭 lineage saved: {lineage_out}")


        return report
    finally:
        spark.stop()















# # Data_cleaning/MKM_Data_Validation_and_cleaning/validators/post_validations/run_postvalidate_common.py

# # --- bootstrap (keep at very top) ---
# import sys, json
# from pathlib import Path
# HERE = Path(__file__).resolve()
# REPO_ROOT = next(p for p in [HERE.parent] + list(HERE.parents) if (p / "project_bootstrap.py").exists())
# sys.path.insert(0, str(REPO_ROOT))
# from project_bootstrap import bootstrap_project_paths
# bootstrap_project_paths(__file__)
# # ------------------------------------

# from typing import Dict, List, Any, Optional, Tuple
# from pyspark.sql import SparkSession
# from src.connections.db_connections import spark_session_for_JDBC
# from src.utils.config_loader import load_env_and_get
# from src.utils.path_utils import cleaning_output_paths, get_validation_report_path

# # import checks from your toolbox package (relative import)
# from .post_validator_common.post_validation_checks import (
#     check_not_null, check_unique, check_columns_present, check_allowed_values,
#     check_value_range, check_regex, check_cast_success, check_foreign_key,
#     check_rowcount_delta, check_renamed_id
# )

# def _read_df(spark: SparkSession, path: str, file_format: str):
#     if file_format == "json":
#         return spark.read.json(path)
#     elif file_format == "parquet":
#         return spark.read.parquet(path)
#     elif file_format == "csv":
#         return spark.read.options(header=True, inferSchema=True).csv(path)
#     else:
#         raise ValueError(f"Unsupported format: {file_format}")

# def _split_existing(seq: Optional[List[str]], cols_set: set) -> Tuple[List[str], List[str]]:
#     """
#     Returns (existing, missing) given a list of column names and the dataframe's column set.
#     """
#     seq = seq or []
#     exist = [c for c in seq if c in cols_set]
#     miss  = [c for c in seq if c not in cols_set]
#     return exist, miss

# def _filter_rule_map(rule_map: Optional[Dict[str, Any]], cols_set: set) -> Tuple[Dict[str, Any], List[str]]:
#     """
#     For maps like {col: rule}, keep only entries where col exists. Return (filtered_map, missing_cols).
#     """
#     filtered, missing = {}, []
#     for col, spec in (rule_map or {}).items():
#         if col in cols_set:
#             filtered[col] = spec
#         else:
#             missing.append(col)
#     return filtered, missing

# def _filter_rule_list(rule_list: Optional[List[Dict[str, Any]]], cols_set: set) -> Tuple[List[Dict[str, Any]], List[str]]:
#     """
#     For lists of dicts like [{"col": "x", ...}], keep only entries whose col exists.
#     Return (filtered_list, missing_cols).
#     """
#     filtered, missing = [], []
#     for spec in (rule_list or []):
#         col = spec.get("col")
#         if col in cols_set:
#             filtered.append(spec)
#         else:
#             missing.append(col)
#     return filtered, missing

# def run_postvalidate(
#     TABLE: str,
#     file_format: str = "json",
#     not_null_cols: Optional[List[str]] = None,
#     unique_cols: Optional[List[str]] = None,
#     require_renamed_id: bool = True,
#     enum_rules: Optional[Dict[str, List[Any]]] = None,     # {col: [allowed,...]}
#     range_rules: Optional[List[Dict[str, Any]]] = None,    # [{"col":"price","min":0}]
#     regex_rules: Optional[List[Dict[str, str]]] = None,    # [{"col":"email","pattern":".+@.+"}]
#     cast_rules: Optional[Dict[str, str]] = None,           # {col: "int"/"double"/"timestamp"/...}
#     fk_rules: Optional[List[Dict[str, str]]] = None,       # [{"fk_col":"user_id","ref_table":"users","ref_key":"users_id"}]
# ) -> Dict[str, Any]:
#     """
#     Loads cleaned <TABLE> from cleaned_outputs/, runs checks, writes post_validation_reports JSON.
#     Defensive: skips checks for columns that don't exist and records them under issues["_unresolved"].
#     """
#     load_env_and_get()
#     spark = spark_session_for_JDBC(app_name=f"postvalidate_{TABLE}")

#     try:
#         cleaned_path = cleaning_output_paths(TABLE, file_format=file_format)
#         df = _read_df(spark, cleaned_path, file_format)
#         cols_set = set(df.columns)

#         issues: Dict[str, Any] = {}
#         unresolved: Dict[str, Any] = {}

#         # ----- defensively filter rules by actual DF columns -----
#         not_null_cols, miss_nn = _split_existing(not_null_cols, cols_set)
#         unique_cols,   miss_uq = _split_existing(unique_cols, cols_set)

#         enum_rules,  miss_enum  = _filter_rule_map(enum_rules,  cols_set)
#         cast_rules,  miss_cast  = _filter_rule_map(cast_rules,  cols_set)
#         range_rules, miss_range = _filter_rule_list(range_rules, cols_set)
#         regex_rules, miss_regex = _filter_rule_list(regex_rules, cols_set)

#         # FK rules: keep only those where fk_col exists in this DF
#         fk_rules = fk_rules or []
#         fk_rules_filtered, fk_missing = [], []
#         for spec in fk_rules:
#             fk_col = spec.get("fk_col")
#             if fk_col in cols_set:
#                 fk_rules_filtered.append(spec)
#             else:
#                 fk_missing.append(fk_col)
#         fk_rules = fk_rules_filtered

#         if miss_nn:   unresolved["not_null_missing_cols"] = miss_nn
#         if miss_uq:   unresolved["unique_missing_cols"]   = miss_uq
#         if miss_enum: unresolved["enum_missing_cols"]     = miss_enum
#         if miss_cast: unresolved["cast_missing_cols"]     = miss_cast
#         if miss_range: unresolved["range_missing_cols"]   = miss_range
#         if miss_regex: unresolved["regex_missing_cols"]   = miss_regex
#         if fk_missing: unresolved["fk_missing_cols"]      = fk_missing

#         row_count = df.count()

#         # ----- required ID convention check -----
#         if require_renamed_id:
#             issues["renamed_id"] = check_renamed_id(df, TABLE)

#         # ----- basic checks -----
#         if not_null_cols:
#             issues["not_null"] = check_not_null(df, not_null_cols)

#         if unique_cols:
#             issues["unique"] = check_unique(df, unique_cols)

#         # ----- enums/ranges/regex/casts -----
#         if enum_rules:
#             issues["enums"] = {col: check_allowed_values(df, col, allowed)
#                                for col, allowed in enum_rules.items()}

#         if range_rules:
#             issues["ranges"] = [check_value_range(df, r["col"], r.get("min"), r.get("max"), r.get("inclusive", True))
#                                 for r in range_rules]

#         if regex_rules:
#             issues["regex"] = [check_regex(df, r["col"], r["pattern"]) for r in regex_rules]

#         if cast_rules:
#             issues["casts"] = {col: check_cast_success(df, col, tgt) for col, tgt in cast_rules.items()}

#         # ----- foreign keys -----
#         if fk_rules:
#             fk_results, fk_ref_errors = [], []
#             for spec in fk_rules:
#                 fk_col = spec["fk_col"]
#                 ref_table = spec["ref_table"]
#                 ref_key = spec.get("ref_key", f"{ref_table}_id")
#                 try:
#                     ref_path = cleaning_output_paths(ref_table, file_format=file_format)
#                     ref_df = _read_df(spark, ref_path, file_format)
#                     fk_results.append(check_foreign_key(df, fk_col, ref_df, ref_key))
#                 except Exception as e:
#                     fk_ref_errors.append({"fk_col": fk_col, "ref_table": ref_table, "ref_key": ref_key, "error": str(e)})
#             if fk_results:
#                 issues["foreign_keys"] = fk_results
#             if fk_ref_errors:
#                 unresolved["fk_ref_read_errors"] = fk_ref_errors

#         if unresolved:
#             issues["_unresolved"] = unresolved

#         report = {
#             "table": TABLE,
#             "phase": "post_validation_reports",
#             "row_count": row_count,
#             "file_format": file_format,
#             "output_dir": cleaned_path,
#             "columns": sorted(df.columns),
#             "issues": issues,
#         }

#         out = get_validation_report_path("post_validation_reports", f"{TABLE}_post_validation.json")
#         with open(out, "w", encoding="utf-8") as f:
#             json.dump(report, f, indent=2)
#         print(f"[POST-VALIDATION] ✅ saved: {out}")

#         return report
#     finally:
#         spark.stop()
