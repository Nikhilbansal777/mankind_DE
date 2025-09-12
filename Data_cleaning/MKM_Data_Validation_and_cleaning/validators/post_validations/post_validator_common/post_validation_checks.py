# Data_cleaning/MKM_Data_Validation_and_cleaning/validators/post_validations/post_validator_common/post_validation_checks.py

from __future__ import annotations
from typing import Dict, List, Any
from pyspark.sql import DataFrame, functions as F

def _count(df: DataFrame) -> int:
    try:
        return df.count()
    except Exception:
        return -1

def check_not_null(df: DataFrame, cols: List[str]) -> Dict[str, Any]:
    out = {}
    for c in cols:
        bad = df.filter(F.col(c).isNull())
        n = _count(bad)
        samples = [r.asDict(True) for r in bad.limit(20).collect()] if n else []
        out[c] = {"null_count": n, "samples": samples}
    return out

def check_unique(df: DataFrame, cols: List[str]) -> Dict[str, Any]:
    if not cols:
        return {"error": "no_cols"}
    dup = df.groupBy([F.col(c) for c in cols]).count().filter(F.col("count") > 1)
    n = _count(dup)
    samples = [r.asDict(True) for r in dup.limit(20).collect()] if n else []
    return {"dup_groups": n, "samples": samples, "columns": cols}

def check_columns_present(df: DataFrame, must_exist: List[str], must_absent: List[str] | None = None) -> Dict[str, Any]:
    cols = set(df.columns)
    return {
        "missing": [c for c in must_exist if c not in cols],
        "present_forbidden": [c for c in (must_absent or []) if c in cols],
    }

def check_allowed_values(df: DataFrame, col: str, allowed: List[Any]) -> Dict[str, Any]:
    bad = df.filter(~F.col(col).isin(allowed))
    n = _count(bad)
    samples = [r.asDict(True) for r in bad.limit(20).collect()] if n else []
    return {"col": col, "allowed": allowed, "violations": n, "samples": samples}

def check_value_range(df: DataFrame, col: str, min_value: Any = None, max_value: Any = None, inclusive: bool = True) -> Dict[str, Any]:
    cond = F.lit(True)
    if min_value is not None:
        cond = cond & (F.col(col) >= min_value if inclusive else F.col(col) > min_value)
    if max_value is not None:
        cond = cond & (F.col(col) <= max_value if inclusive else F.col(col) < max_value)
    bad = df.filter(~cond)
    n = _count(bad)
    samples = [r.asDict(True) for r in bad.limit(20).collect()] if n else []
    return {"col": col, "min": min_value, "max": max_value, "violations": n, "samples": samples}

def check_regex(df: DataFrame, col: str, pattern: str) -> Dict[str, Any]:
    bad = df.filter(~F.col(col).rlike(pattern))
    n = _count(bad)
    samples = [r.asDict(True) for r in bad.limit(20).collect()] if n else []
    return {"col": col, "pattern": pattern, "violations": n, "samples": samples}

def check_cast_success(df: DataFrame, col: str, target_spark_type: str) -> Dict[str, Any]:
    cast_col = F.col(col).cast(target_spark_type)
    bad = df.filter(F.col(col).isNotNull() & cast_col.isNull())
    n = _count(bad)
    samples = [r.asDict(True) for r in bad.select(col).limit(20).collect()] if n else []
    return {"col": col, "target": target_spark_type, "cast_failures": n, "samples": samples}

def check_foreign_key(df: DataFrame, fk_col: str, ref_df: DataFrame, ref_col: str) -> Dict[str, Any]:
    unmatched = (
        df.select(F.col(fk_col).alias("_fk"))
          .where(F.col("_fk").isNotNull())
          .join(ref_df.select(F.col(ref_col).alias("_pk")), F.col("_fk") == F.col("_pk"), "left_anti")
    )
    n = _count(unmatched)
    samples = [r["_fk"] for r in unmatched.limit(20).collect()] if n else []
    return {"fk_col": fk_col, "ref_col": ref_col, "unmatched_count": n, "sample_fk": samples}

def check_rowcount_delta(before: int, after: int, min_retain_ratio: float = 0.8) -> Dict[str, Any]:
    ok = after >= int(before * min_retain_ratio)
    return {"before": before, "after": after, "min_retain_ratio": min_retain_ratio, "ok": ok}

def check_renamed_id(df: DataFrame, table: str) -> Dict[str, Any]:
    t_id = f"{table}_id"
    cols = set(df.columns)
    return {"table": table, "required_id": t_id, "present": t_id in cols, "old_id_still_present": "id" in cols}
