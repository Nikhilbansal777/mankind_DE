# Data_cleaning/MKM_Data_Validation_and_cleaning/validators/pre_validations/validators_common/validation_checks.py
from __future__ import annotations

from typing import Iterable, Dict, List, Tuple, Optional
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

# ✅ drift ledger writers
from .._drift_recorder import record_schema_drift, record_unresolved_checks

# (optional) for local prints if you want them here
try:
    from src.utils.log_utils import get_logger
    _LOG = get_logger("validators.pre.validation_checks")
except Exception:
    _LOG = None


# ------------------------------------------------------------------------------
# Low-level column checks (kept exactly like you had, returns issue dicts)
# ------------------------------------------------------------------------------

def check_not_null(df: DataFrame, cols: Iterable[str]) -> Dict[str, Dict[str, int]]:
    """
    For each column in cols, count nulls. Returns:
      { col_name: {"null_count": N}, ... } for columns that have nulls.
    """
    issues: Dict[str, Dict[str, int]] = {}
    df_cols = set(df.columns)
    for c in cols:
        if c in df_cols:
            cnt = df.filter(F.col(c).isNull()).count()
            if cnt > 0:
                issues[c] = {"null_count": cnt}
    return issues


def check_unique(df: DataFrame, cols: Iterable[str]) -> Dict[str, Dict[str, int]]:
    """
    For each column in cols, detect dupes by comparing total vs distinct.
    Returns:
      { col_name: {"duplicate_count": K}, ... } for columns that are not unique.
    """
    issues: Dict[str, Dict[str, int]] = {}
    df_cols = set(df.columns)
    total = df.count() if cols else 0
    for c in cols:
        if c in df_cols:
            distinct = df.select(c).distinct().count()
            if distinct < total:
                dups = total - distinct
                issues[c] = {"duplicate_count": dups}
    return issues


# ------------------------------------------------------------------------------
# Schema-diff utilities
# ------------------------------------------------------------------------------

def _to_simple_type_map(df: DataFrame) -> Dict[str, str]:
    """Spark schema -> {col: simpleType} e.g. 'int', 'string', 'timestamp'."""
    return {f.name: f.dataType.simpleString() for f in df.schema.fields}


def diff_schema(
    df: DataFrame,
    expected_cols: Optional[Iterable[str]] = None,
    expected_types: Optional[Dict[str, str]] = None,
) -> Tuple[List[str], List[str], Dict[str, str]]:
    """
    Compute schema drift:
      - new_columns: present in df but not in expected_cols
      - missing_columns: present in expected_cols but not in df
      - type_changes: if expected_types is provided (mapping col -> typeStr),
                      returns {col: "expected->actual"} for mismatches
    """
    actual_cols = list(df.columns)
    new_columns: List[str] = []
    missing_columns: List[str] = []
    type_changes: Dict[str, str] = {}

    if expected_cols is not None:
        exp_set = set(expected_cols)
        act_set = set(actual_cols)
        new_columns = sorted(list(act_set - exp_set))
        missing_columns = sorted(list(exp_set - act_set))

    if expected_types:
        actual_types = _to_simple_type_map(df)
        for c, exp_t in expected_types.items():
            if c in actual_types:
                act_t = actual_types[c]
                # compare normalized types
                if str(exp_t).lower() != str(act_t).lower():
                    type_changes[c] = f"{exp_t}->{act_t}"

    return new_columns, missing_columns, type_changes


# ------------------------------------------------------------------------------
# One-shot helper to validate & record (optional convenience)
# ------------------------------------------------------------------------------

def validate_and_record(
    *,
    table: str,
    df: DataFrame,
    not_null_cols: Optional[Iterable[str]] = None,
    unique_cols: Optional[Iterable[str]] = None,
    expected_cols: Optional[Iterable[str]] = None,
    expected_types: Optional[Dict[str, str]] = None,
    run_id: Optional[str] = None,
) -> Dict:
    """
    Convenience function you can call from each *pre_validate.py:
      - diffs the schema
      - checks not-null & uniqueness
      - prints small notices (optional)
      - writes drift + unresolved to the JSONL ledger via _drift_recorder

    Returns a small summary dict you can include in your per-table JSON report.
    """
    # 1) Schema diff
    new_cols, missing_cols, type_changes = diff_schema(
        df, expected_cols=expected_cols, expected_types=expected_types
    )

    if new_cols:
        msg = f"[SCHEMA NOTICE] New/unexpected columns detected: {new_cols}"
        print(msg)
        if _LOG: _LOG.info(msg, extra={"table": table})
    if missing_cols:
        msg = f"[SCHEMA NOTICE] Missing/removed columns detected: {missing_cols}"
        print(msg)
        if _LOG: _LOG.info(msg, extra={"table": table})
    if type_changes:
        msg = f"[SCHEMA NOTICE] Type changes detected: {type_changes}"
        print(msg)
        if _LOG: _LOG.info(msg, extra={"table": table})

    # Write to drift ledger (only non-empty parts are persisted)
    record_schema_drift(
        table=table,
        new_columns=new_cols or None,
        removed_columns=missing_cols or None,
        type_changes=type_changes or None,
        run_id=run_id,
    )

    # 2) Constraint checks
    not_null_cols = list(not_null_cols or [])
    unique_cols = list(unique_cols or [])

    nn_issues = check_not_null(df, not_null_cols)
    uq_issues = check_unique(df, unique_cols)

    unresolved_not_null = sorted(nn_issues.keys())
    unresolved_unique = sorted(uq_issues.keys())

    if unresolved_not_null or unresolved_unique:
        msg = (f"[PRE-VALIDATION][{table}] ⚠️ Unresolved columns -> "
               f"NOT NULL: {unresolved_not_null} | UNIQUE: {unresolved_unique}")
        print(msg)
        if _LOG: _LOG.warning(msg, extra={"table": table})
        record_unresolved_checks(
            table=table,
            not_null=unresolved_not_null or None,
            unique=unresolved_unique or None,
            run_id=run_id,
        )

    # 3) Return compact summary to embed in your per-table JSON
    status = "OK" if not (new_cols or missing_cols or type_changes or unresolved_not_null or unresolved_unique) else "WARN"
    return {
        "table": table,
        "status": status,
        "schema_drift": {
            "new_columns": new_cols,
            "missing_columns": missing_cols,
            "type_changes": type_changes,
        },
        "unresolved": {
            "not_null": unresolved_not_null,
            "unique": unresolved_unique,
        },
    }





















# # Data_cleaning/MKM_Data_Validation_and_cleaning/validators/pre_validations/validators_common/validation_checks.py
# from pyspark.sql import DataFrame
# import pyspark.sql.functions as F
# from .._drift_recorder import record_schema_drift, record_unresolved_checks


# def check_not_null(df: DataFrame, cols):
#     issues = {}
#     for c in cols:
#         if c in df.columns:
#             cnt = df.filter(F.col(c).isNull()).count()
#             if cnt > 0:
#                 issues[c] = {"null_count": cnt}
#     return issues

# def check_unique(df: DataFrame, cols):
#     issues = {}
#     for c in cols:
#         if c in df.columns:
#             total = df.count()
#             distinct = df.select(c).distinct().count()
#             if distinct < total:
#                 dups = total - distinct
#                 issues[c] = {"duplicate_count": dups}
#     return issues



















# from pyspark.sql import DataFrame
# from pyspark.sql.functions import col, length

# # ✅ 1. Null check
# def check_nulls(df: DataFrame, column: str) -> DataFrame:
#     return df.withColumn(f"{column}_not_null", col(column).isNotNull())

# # ✅ 2. Allowed values check (e.g., True/False, categories)
# def check_allowed_values(df: DataFrame, column: str, allowed_values: list) -> DataFrame:
#     return df.withColumn(f"{column}_allowed", col(column).isin(allowed_values))

# # ✅ 3. Numeric range check (min ≤ value ≤ max)
# def check_numeric_range(df: DataFrame, column: str, min_value: float, max_value: float) -> DataFrame:
#     return df.withColumn(f"{column}_in_range", (col(column) >= min_value) & (col(column) <= max_value))

# # ✅ 4. Alphanumeric format check (for IDs, SKUs, etc.)
# def check_format_alphanumeric(df: DataFrame, column: str) -> DataFrame:
#     return df.withColumn(f"{column}_alphanumeric", col(column).rlike("^[a-zA-Z0-9\\-]+$"))

# # ✅ 5. Timestamp validity check (if castable to timestamp)
# def check_timestamp_castable(df: DataFrame, column: str) -> DataFrame:
#     return df.withColumn(f"{column}_is_timestamp", col(column).cast("timestamp").isNotNull())

# # ✅ 6. Positive numeric check
# def check_positive(df: DataFrame, column: str) -> DataFrame:
#     return df.withColumn(f"{column}_positive", col(column) > 0)

# # ✅ 7. String non-empty check (optional: if you want to flag empty strings)
# def check_non_empty_string(df: DataFrame, column: str) -> DataFrame:
#     return df.withColumn(f"{column}_non_empty", (col(column).isNotNull()) & (length(col(column)) > 0))
