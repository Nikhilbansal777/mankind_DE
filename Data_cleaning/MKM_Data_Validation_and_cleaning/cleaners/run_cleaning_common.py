# MKM_Data_Validation_and_cleaning/cleaners/run_cleaning_common.py

import os, sys, json
from datetime import datetime, timezone
from src.utils.lineage import get_run_id, write_run_manifest, write_event, schema_fingerprint
RUN_ID = get_run_id()


# --- project bootstrap ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# -------------------------

from pyspark.sql import functions as F, types as T
from src.utils.config_loader import load_env_and_get
from src.utils.path_utils import cleaning_output_paths, get_project_root
from src.utils.log_utils import get_logger 

# optional: reuse your DF-based schema validator (won’t crash if YAML missing)
try:
    from MKM_Data_Validation_and_cleaning.metadata.schema_validator import validate_schema
    HAS_SCHEMA_VALIDATOR = True
except Exception:
    HAS_SCHEMA_VALIDATOR = False
    
try:
    import yaml
except Exception:
    yaml = None  # we’ll handle gracefully


# ---------- helpers: config & resolution ----------

def _load_master_rules():
    """
    Load src/config/master_schema_cleaning_rules.yaml from repo root.
    Shape expected:
      {
        version, generated_at,
        tables: {
          <table>: {
            rename_columns, type_conversions, null_replacements,
            case_formatting, standardize_units
          }
        }
      }
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

def _rules_for_table(master_rules, table_name):
    return ((master_rules.get("tables", {}) or {}).get(table_name, {})) or {}

def _resolve_col(df, table, logical_col, rules):
    """
    Resolve a logical column name against the actual DF column (pre-clean).
    Works for both pre-rename (source) and post-rename (target) names.
    """
    df_cols = set(df.columns)
    rename_map = rules.get("rename_columns", {}) or {}
    reverse_map = {v: k for k, v in rename_map.items()} if rename_map else {}

    candidates = []
    if logical_col in reverse_map:  # target -> source
        candidates.append(reverse_map[logical_col])
    candidates.append(logical_col)
    if logical_col in rename_map:   # source -> target
        candidates.append(rename_map[logical_col])
    candidates.append(f"{table}_id")  # heuristic for id-like fields

    seen, ordered = set(), []
    for c in candidates:
        if c and c not in seen:
            seen.add(c)
            ordered.append(c)
    for c in ordered:
        if c in df_cols:
            return c
    return None

# ---------- helpers: transformations (no UDFs) ----------

_SPARK_TYPE = {
    "StringType":    T.StringType(),
    "IntegerType":   T.IntegerType(),
    "LongType":      T.LongType(),
    "DoubleType":    T.DoubleType(),
    "FloatType":     T.FloatType(),
    "BooleanType":   T.BooleanType(),
    "DateType":      T.DateType(),
    "TimestampType": T.TimestampType(),
    # add more as needed
}

def _apply_type_conversion(df, actual_col, type_name: str):
    """Safe, UDF-free casting with a few smart branches."""
    t = (type_name or "").strip()
    if not t:
        return df

    if t == "BooleanType":
        # normalize textual/num booleans -> True/False (no UDFs)
        s = F.lower(F.col(actual_col).cast("string"))
        truthy = F.when(s.isin("true", "t", "1", "yes", "y"), F.lit(True))
        falsy  = F.when(s.isin("false", "f", "0", "no", "n"), F.lit(False))
        return df.withColumn(actual_col, truthy.otherwise(falsy.otherwise(F.lit(None))).cast("boolean"))

    if t == "TimestampType":
        # let Spark parse common formats
        return df.withColumn(actual_col, F.to_timestamp(F.col(actual_col)))

    if t == "DateType":
        return df.withColumn(actual_col, F.to_date(F.col(actual_col)))

    spark_type = _SPARK_TYPE.get(t)
    if spark_type is not None:
        return df.withColumn(actual_col, F.col(actual_col).cast(spark_type))

    # fallback: pass-through
    return df

def _apply_case_formatting(df, actual_col, style: str):
    """
    style: 'lower' or 'upper'
    """
    st = (style or "").lower()
    if st == "lower":
        return df.withColumn(actual_col, F.lower(F.col(actual_col)))
    if st == "upper":
        return df.withColumn(actual_col, F.upper(F.col(actual_col)))
    return df

def _apply_unit_standardization(df, actual_col, rule):
    """
    rule examples:
      {"multiply": 0.001}      # e.g., mg -> g
      {"factor": 0.45359237}   # lb -> kg
    """
    factor = None
    if isinstance(rule, dict):
        factor = rule.get("multiply") or rule.get("factor")
    if factor is None:
        return df
    return df.withColumn(actual_col, F.col(actual_col).cast("double") * F.lit(float(factor)))

def _safe_rename(df, rename_map):
    """
    Rename columns source->target without destructive collisions.
    If target exists and differs from source, skip and record as conflict.
    Returns: (df2, applied_pairs, skipped_conflicts)
    """
    applied, conflicts = [], []
    for src, tgt in (rename_map or {}).items():
        if src == tgt:
            continue
        if src not in df.columns:
            conflicts.append({"source_missing": src, "target": tgt})
            continue
        if tgt in df.columns and tgt != src:
            # avoid overwriting; report conflict
            conflicts.append({"conflict": (src, tgt)})
            continue
        df = df.withColumnRenamed(src, tgt)
        applied.append((src, tgt))
    return df, applied, conflicts


# ---------- CDC flags (no UDFs, table-aware) ----------

def _set_flag(df, name, expr):
    """
    Create/overwrite a boolean flag column from a boolean-able expression.
    Returns (df2, created: bool)
    """
    existed = name in df.columns
    df2 = df.withColumn(name, expr.cast("boolean"))
    return df2, (not existed)

def apply_cdc_flags(table: str, df):
    """
    Add simple CDC/state flags based on available columns.
    Non-destructive: only uses Spark built-ins; creates flags when inputs exist.
    Returns (df2, added_flags: List[str])
    """
    added = []
    cols = set(df.columns)

    def add(name, expr):
        nonlocal df, added
        df, created = _set_flag(df, name, expr)
        if created:
            added.append(name)

    if table == "users":
        has_deleted_at = "deleted_at" in cols
        has_status     = "status" in cols
        if has_deleted_at and has_status:
            add("is_active", F.when(F.col("deleted_at").isNotNull(), F.lit(False))
                             .otherwise(F.col("status").isin("active", "verified")))
        elif has_deleted_at:
            add("is_active", F.col("deleted_at").isNull())
        elif has_status:
            add("is_active", F.col("status").isin("active", "verified"))

    elif table == "orders":
        if "status" in cols:
            add("is_paid",      F.col("status").isin("paid", "shipped"))
            add("is_cancelled", F.col("status") == F.lit("cancelled"))
            add("is_returned",  F.col("status") == F.lit("returned"))
            add("is_refunded",  F.col("status").isin("refunded", "returned"))

    elif table in ("payments", "payments"):
        has_status = "status" in cols
        has_refund_amount = "refund_amount" in cols
        if has_status:
            add("is_paid",     F.col("status") == F.lit("paid"))
            add("is_failed",   F.col("status") == F.lit("failed"))
            add("is_refunded", F.col("status") == F.lit("refunded"))
        if has_refund_amount:
            # override / ensure refunded is true if any refund amount > 0
            df = df.withColumn("is_refunded",
                               (F.col("refund_amount").cast("double") > F.lit(0)).cast("boolean"))
            if "is_refunded" not in added:
                added.append("is_refunded")

    elif table == "order_items":
        if {"quantity", "product_price", "subtotal"}.issubset(cols):
            add("is_discounted",
                (F.col("subtotal").cast("double") <
                 (F.col("quantity").cast("double") * F.col("product_price").cast("double"))))
        if "quantity" in cols:
            add("is_returned", F.col("quantity").cast("double") < F.lit(0.0))

    elif table == "cart_item":
        if "quantity" in cols:
            add("is_active", F.col("quantity").cast("double") > F.lit(0.0))

    elif table == "wishlist":
        add("is_active", F.lit(True))

    return df, added


# ---------- main: run cleaning for a single table ----------

def run_clean_table(table_name: str, file_format: str = "json"):
    """
    Reads table via JDBC, applies master cleaning rules (+ CDC flags), writes to cleaned_outputs.
    Returns a small audit dict.
    """
    from src.connections.db_connections import spark_session_for_JDBC

    logger = get_logger(f"cleaners.{table_name}")  # <-- NEW

    load_env_and_get()
    spark = spark_session_for_JDBC(app_name=f"clean_{table_name}")
    logger.info(f"start cleaning {table_name}")


    try:
        jdbc_url = os.getenv("DB_URL")
        props = {
            "user": os.getenv("DB_USERNAME"),
            "password": os.getenv("DB_PASSWORD"),
            "driver": "com.mysql.cj.jdbc.Driver",
        }
        df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=props)

        fp_in = schema_fingerprint(df)

        # optional schema sanity before cleaning
        if HAS_SCHEMA_VALIDATOR:
            try:
                validate_schema(df, table_name)
            except FileNotFoundError:
                print(f"[SCHEMA NOTICE] YAML not found for {table_name}; skipping schema check.")

        master = _load_master_rules()
        rules = _rules_for_table(master, table_name)

        audit = {
            "run_id": RUN_ID, 
            "table": table_name,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "input_row_count": df.count(),
            "input_columns": df.columns,  # <-- extra context
            "actions": {
                "renames_applied": [],
                "rename_conflicts": [],
                "type_conversions": [],
                "null_fills": [],
                "case_formatting": [],
                "unit_standardization": [],
                "cdc_flags_added": [],
            }
        }

        # --- 1) type conversions (resolve columns first, apply in-place) ---
        for col_name, type_name in (rules.get("type_conversions") or {}).items():
            actual = _resolve_col(df, table_name, col_name, rules)
            if actual:
                df = _apply_type_conversion(df, actual, type_name)
                audit["actions"]["type_conversions"].append({"column": actual, "target_type": type_name})
            else:
                audit["actions"]["type_conversions"].append({"column": col_name, "skipped": "unresolved"})

        # --- 2) case formatting ---
        for col_name, style in (rules.get("case_formatting") or {}).items():
            actual = _resolve_col(df, table_name, col_name, rules)
            if actual:
                df = _apply_case_formatting(df, actual, style)
                audit["actions"]["case_formatting"].append({"column": actual, "style": style})
            else:
                audit["actions"]["case_formatting"].append({"column": col_name, "skipped": "unresolved"})

        # --- 3) null replacements (bulk .na.fill for resolved cols only) ---
        fills = {}
        for col_name, val in (rules.get("null_replacements") or {}).items():
            actual = _resolve_col(df, table_name, col_name, rules)
            if actual:
                fills[actual] = val
                audit["actions"]["null_fills"].append({"column": actual, "value": val})
            else:
                audit["actions"]["null_fills"].append({"column": col_name, "skipped": "unresolved"})
        if fills:
            df = df.na.fill(fills)

        # --- 4) standardize units (simple multipliers) ---
        for col_name, unit_rule in (rules.get("standardize_units") or {}).items():
            actual = _resolve_col(df, table_name, col_name, rules)
            if actual:
                df = _apply_unit_standardization(df, actual, unit_rule)
                audit["actions"]["unit_standardization"].append({"column": actual, "rule": unit_rule})
            else:
                audit["actions"]["unit_standardization"].append({"column": col_name, "skipped": "unresolved"})

        # --- 5) renames at the end (source -> target) ---
        df, applied, conflicts = _safe_rename(df, rules.get("rename_columns") or {})
        audit["actions"]["renames_applied"] = applied
        audit["actions"]["rename_conflicts"] = conflicts

        # --- 6) CDC flags (after renames & casts) ---
        df, flags_added = apply_cdc_flags(table_name, df)
        audit["actions"]["cdc_flags_added"] = flags_added

        # --- write cleaned output ---  
        fp_out = schema_fingerprint(df)

        out_path = cleaning_output_paths(table_name, file_format=file_format)
        fmt = (file_format or "json").lower()
        if fmt == "json":
            df.coalesce(1).write.mode("overwrite").json(out_path)
        elif fmt in ("parquet", "pq"):
            df.write.mode("overwrite").parquet(out_path)
        elif fmt in ("csv",):
            df.coalesce(1).write.mode("overwrite").option("header", True).csv(out_path)
        else:
            df.coalesce(1).write.mode("overwrite").json(out_path)

        audit["output_path"] = out_path
        audit["output_row_count"] = df.count()
        audit["output_columns"] = df.columns  # helpful snapshot


        # also drop a small audit JSON next to output path (same basename + .cleaning_audit.json)
        audit_path = out_path + ".cleaning_audit.json"
        with open(audit_path, "w", encoding="utf-8") as f:
            json.dump(audit, f, indent=2)
        audit["audit_path"] = audit_path  # <-- NEW so clean_all can print it
        
        # ---- lineage artifacts ----
        # ensure (or update) a per-run manifest (idempotent)
        write_run_manifest("cleaning", {
            "job": "cleaning/run_cleaning_common.py",
            "db_url": os.getenv("DB_URL"),
        }, run_id=RUN_ID)

        # simple column lineage from renames
        col_lineage = [{"from": s, "to": t} for (s, t) in audit["actions"]["renames_applied"]]

        # append a 'clean_complete' event
        write_event("cleaning", {
            "event": "clean_complete",
            "table": table_name,
            "dataset_in": f"{os.getenv('DB_URL')}#{table_name}",
            "dataset_out": out_path,
            "rows_in": audit["input_row_count"],
            "rows_out": audit["output_row_count"],
            "schema_fp_in": fp_in,
            "schema_fp_out": fp_out,
            "column_lineage": col_lineage,  
            "audit_path": audit_path
        }, run_id=RUN_ID)

        print(f"[CLEANING] ✅ {table_name} -> {out_path}")
        print(f"[CLEANING] 🧾 audit: {audit_path}")
        logger.info(f"done cleaning {table_name}", extra={"table": table_name, "out": out_path, "rows": audit["output_row_count"]})
        return audit
    
        
    finally:
        spark.stop()


























# import os, sys, json
# from datetime import datetime, timezone

# # --- project bootstrap ---
# project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
# if project_root not in sys.path:
#     sys.path.insert(0, project_root)
# from project_bootstrap import bootstrap_project_paths
# bootstrap_project_paths(__file__)
# # -------------------------

# from pyspark.sql import functions as F, types as T
# from src.utils.config_loader import load_env_and_get
# from src.utils.path_utils import cleaning_output_paths, get_project_root

# # optional: reuse your DF-based schema validator (won’t crash if YAML missing)
# try:
#     from MKM_Data_Validation_and_cleaning.metadata.schema_validator import validate_schema
#     HAS_SCHEMA_VALIDATOR = True
# except Exception:
#     HAS_SCHEMA_VALIDATOR = False

# try:
#     import yaml
# except Exception:
#     yaml = None  # we’ll handle gracefully


# # ---------- helpers: config & resolution ----------

# def _load_master_rules():
#     """
#     Load src/config/master_schema_cleaning_rules.yaml from repo root.
#     Shape expected:
#       {
#         version, generated_at,
#         tables: {
#           <table>: {
#             rename_columns, type_conversions, null_replacements,
#             case_formatting, standardize_units
#           }
#         }
#       }
#     """
#     try:
#         root = get_project_root()
#         path = os.path.join(root, "src", "config", "master_schema_cleaning_rules.yaml")
#         if not os.path.exists(path) or yaml is None:
#             return {}
#         with open(path, "r", encoding="utf-8") as f:
#             return yaml.safe_load(f) or {}
#     except Exception:
#         return {}

# def _rules_for_table(master_rules, table_name):
#     return ((master_rules.get("tables", {}) or {}).get(table_name, {})) or {}

# def _resolve_col(df, table, logical_col, rules):
#     """
#     Resolve a logical column name against the actual DF column (pre-clean).
#     Works for both pre-rename (source) and post-rename (target) names.
#     """
#     df_cols = set(df.columns)
#     rename_map = rules.get("rename_columns", {}) or {}
#     reverse_map = {v: k for k, v in rename_map.items()} if rename_map else {}

#     candidates = []
#     if logical_col in reverse_map:  # target -> source
#         candidates.append(reverse_map[logical_col])
#     candidates.append(logical_col)
#     if logical_col in rename_map:   # source -> target
#         candidates.append(rename_map[logical_col])
#     candidates.append(f"{table}_id")  # heuristic for id-like fields

#     seen, ordered = set(), []
#     for c in candidates:
#         if c and c not in seen:
#             seen.add(c)
#             ordered.append(c)
#     for c in ordered:
#         if c in df_cols:
#             return c
#     return None

# # ---------- helpers: transformations (no UDFs) ----------

# _SPARK_TYPE = {
#     "StringType":    T.StringType(),
#     "IntegerType":   T.IntegerType(),
#     "LongType":      T.LongType(),
#     "DoubleType":    T.DoubleType(),
#     "FloatType":     T.FloatType(),
#     "BooleanType":   T.BooleanType(),
#     "DateType":      T.DateType(),
#     "TimestampType": T.TimestampType(),
#     # add more as needed
# }

# def _apply_type_conversion(df, actual_col, type_name: str):
#     """Safe, UDF-free casting with a few smart branches."""
#     t = (type_name or "").strip()
#     if not t:
#         return df

#     if t == "BooleanType":
#         # normalize textual/num booleans -> True/False (no UDFs)
#         s = F.lower(F.col(actual_col).cast("string"))
#         truthy = F.when(s.isin("true", "t", "1", "yes", "y"), F.lit(True))
#         falsy  = F.when(s.isin("false", "f", "0", "no", "n"), F.lit(False))
#         return df.withColumn(actual_col, truthy.otherwise(falsy.otherwise(F.lit(None))).cast("boolean"))

#     if t == "TimestampType":
#         # let Spark parse common formats
#         return df.withColumn(actual_col, F.to_timestamp(F.col(actual_col)))

#     if t == "DateType":
#         return df.withColumn(actual_col, F.to_date(F.col(actual_col)))

#     spark_type = _SPARK_TYPE.get(t)
#     if spark_type is not None:
#         return df.withColumn(actual_col, F.col(actual_col).cast(spark_type))

#     # fallback: pass-through
#     return df

# def _apply_case_formatting(df, actual_col, style: str):
#     """
#     style: 'lower' or 'upper'
#     """
#     st = (style or "").lower()
#     if st == "lower":
#         return df.withColumn(actual_col, F.lower(F.col(actual_col)))
#     if st == "upper":
#         return df.withColumn(actual_col, F.upper(F.col(actual_col)))
#     return df

# def _apply_unit_standardization(df, actual_col, rule):
#     """
#     rule examples:
#       {"multiply": 0.001}      # e.g., mg -> g
#       {"factor": 0.45359237}   # lb -> kg
#     """
#     factor = None
#     if isinstance(rule, dict):
#         factor = rule.get("multiply") or rule.get("factor")
#     if factor is None:
#         return df
#     return df.withColumn(actual_col, F.col(actual_col).cast("double") * F.lit(float(factor)))

# def _safe_rename(df, rename_map):
#     """
#     Rename columns source->target without destructive collisions.
#     If target exists and differs from source, skip and record as conflict.
#     Returns: (df2, applied_pairs, skipped_conflicts)
#     """
#     applied, conflicts = [], []
#     for src, tgt in (rename_map or {}).items():
#         if src == tgt:
#             continue
#         if src not in df.columns:
#             conflicts.append({"source_missing": src, "target": tgt})
#             continue
#         if tgt in df.columns and tgt != src:
#             # avoid overwriting; report conflict
#             conflicts.append({"conflict": (src, tgt)})
#             continue
#         df = df.withColumnRenamed(src, tgt)
#         applied.append((src, tgt))
#     return df, applied, conflicts


# # ---------- CDC flags (no UDFs, table-aware) ----------

# def _set_flag(df, name, expr):
#     """
#     Create/overwrite a boolean flag column from a boolean-able expression.
#     Returns (df2, created: bool)
#     """
#     existed = name in df.columns
#     df2 = df.withColumn(name, expr.cast("boolean"))
#     return df2, (not existed)

# def apply_cdc_flags(table: str, df):
#     """
#     Add simple CDC/state flags based on available columns.
#     Non-destructive: only uses Spark built-ins; creates flags when inputs exist.
#     Returns (df2, added_flags: List[str])
#     """
#     added = []
#     cols = set(df.columns)

#     def add(name, expr):
#         nonlocal df, added
#         df, created = _set_flag(df, name, expr)
#         if created:
#             added.append(name)

#     if table == "users":
#         has_deleted_at = "deleted_at" in cols
#         has_status     = "status" in cols
#         if has_deleted_at and has_status:
#             add("is_active", F.when(F.col("deleted_at").isNotNull(), F.lit(False))
#                              .otherwise(F.col("status").isin("active", "verified")))
#         elif has_deleted_at:
#             add("is_active", F.col("deleted_at").isNull())
#         elif has_status:
#             add("is_active", F.col("status").isin("active", "verified"))

#     elif table == "orders":
#         if "status" in cols:
#             add("is_paid",      F.col("status").isin("paid", "shipped"))
#             add("is_cancelled", F.col("status") == F.lit("cancelled"))
#             add("is_returned",  F.col("status") == F.lit("returned"))
#             add("is_refunded",  F.col("status").isin("refunded", "returned"))

#     elif table == "payment":
#         has_status = "status" in cols
#         has_refund_amount = "refund_amount" in cols
#         if has_status:
#             add("is_paid",     F.col("status") == F.lit("paid"))
#             add("is_failed",   F.col("status") == F.lit("failed"))
#             add("is_refunded", F.col("status") == F.lit("refunded"))
#         if has_refund_amount:
#             # override / ensure refunded is true if any refund amount > 0
#             df = df.withColumn("is_refunded",
#                                (F.col("refund_amount").cast("double") > F.lit(0)).cast("boolean"))
#             if "is_refunded" not in added:
#                 added.append("is_refunded")

#     elif table == "order_items":
#         if {"quantity", "product_price", "subtotal"}.issubset(cols):
#             add("is_discounted",
#                 (F.col("subtotal").cast("double") <
#                  (F.col("quantity").cast("double") * F.col("product_price").cast("double"))))
#         if "quantity" in cols:
#             add("is_returned", F.col("quantity").cast("double") < F.lit(0.0))

#     elif table == "cart_item":
#         if "quantity" in cols:
#             add("is_active", F.col("quantity").cast("double") > F.lit(0.0))

#     elif table == "wishlist":
#         add("is_active", F.lit(True))

#     return df, added


# # ---------- main: run cleaning for a single table ----------

# def run_clean_table(table_name: str, file_format: str = "json"):
#     """
#     Reads table via JDBC, applies master cleaning rules (+ CDC flags), writes to cleaned_outputs.
#     Returns a small audit dict.
#     """
#     from src.connections.db_connections import spark_session_for_JDBC

#     load_env_and_get()
#     spark = spark_session_for_JDBC(app_name=f"clean_{table_name}")

#     try:
#         jdbc_url = os.getenv("DB_URL")
#         props = {
#             "user": os.getenv("DB_USERNAME"),
#             "password": os.getenv("DB_PASSWORD"),
#             "driver": "com.mysql.cj.jdbc.Driver",
#         }
#         df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=props)

#         # optional schema sanity before cleaning
#         if HAS_SCHEMA_VALIDATOR:
#             try:
#                 validate_schema(df, table_name)
#             except FileNotFoundError:
#                 print(f"[SCHEMA NOTICE] YAML not found for {table_name}; skipping schema check.")

#         master = _load_master_rules()
#         rules = _rules_for_table(master, table_name)

#         audit = {
#             "table": table_name,
#             "timestamp": datetime.now(timezone.utc).isoformat(),
#             "input_row_count": df.count(),
#             "actions": {
#                 "renames_applied": [],
#                 "rename_conflicts": [],
#                 "type_conversions": [],
#                 "null_fills": [],
#                 "case_formatting": [],
#                 "unit_standardization": [],
#                 "cdc_flags_added": [],
#             }
#         }

#         # --- 1) type conversions (resolve columns first, apply in-place) ---
#         for col_name, type_name in (rules.get("type_conversions") or {}).items():
#             actual = _resolve_col(df, table_name, col_name, rules)
#             if actual:
#                 df = _apply_type_conversion(df, actual, type_name)
#                 audit["actions"]["type_conversions"].append({"column": actual, "target_type": type_name})
#             else:
#                 audit["actions"]["type_conversions"].append({"column": col_name, "skipped": "unresolved"})

#         # --- 2) case formatting ---
#         for col_name, style in (rules.get("case_formatting") or {}).items():
#             actual = _resolve_col(df, table_name, col_name, rules)
#             if actual:
#                 df = _apply_case_formatting(df, actual, style)
#                 audit["actions"]["case_formatting"].append({"column": actual, "style": style})
#             else:
#                 audit["actions"]["case_formatting"].append({"column": col_name, "skipped": "unresolved"})

#         # --- 3) null replacements (bulk .na.fill for resolved cols only) ---
#         fills = {}
#         for col_name, val in (rules.get("null_replacements") or {}).items():
#             actual = _resolve_col(df, table_name, col_name, rules)
#             if actual:
#                 fills[actual] = val
#                 audit["actions"]["null_fills"].append({"column": actual, "value": val})
#             else:
#                 audit["actions"]["null_fills"].append({"column": col_name, "skipped": "unresolved"})
#         if fills:
#             df = df.na.fill(fills)

#         # --- 4) standardize units (simple multipliers) ---
#         for col_name, unit_rule in (rules.get("standardize_units") or {}).items():
#             actual = _resolve_col(df, table_name, col_name, rules)
#             if actual:
#                 df = _apply_unit_standardization(df, actual, unit_rule)
#                 audit["actions"]["unit_standardization"].append({"column": actual, "rule": unit_rule})
#             else:
#                 audit["actions"]["unit_standardization"].append({"column": col_name, "skipped": "unresolved"})

#         # --- 5) renames at the end (source -> target) ---
#         df, applied, conflicts = _safe_rename(df, rules.get("rename_columns") or {})
#         audit["actions"]["renames_applied"] = applied
#         audit["actions"]["rename_conflicts"] = conflicts

#         # --- 6) CDC flags (after renames & casts) ---
#         df, flags_added = apply_cdc_flags(table_name, df)
#         audit["actions"]["cdc_flags_added"] = flags_added

#         # --- write cleaned output ---
#         out_path = cleaning_output_paths(table_name, file_format=file_format)
#         fmt = (file_format or "json").lower()
#         if fmt == "json":
#             df.coalesce(1).write.mode("overwrite").json(out_path)
#         elif fmt in ("parquet", "pq"):
#             df.write.mode("overwrite").parquet(out_path)
#         elif fmt in ("csv",):
#             df.coalesce(1).write.mode("overwrite").option("header", True).csv(out_path)
#         else:
#             df.coalesce(1).write.mode("overwrite").json(out_path)

#         audit["output_path"] = out_path
#         audit["output_row_count"] = df.count()

#         # also drop a small audit JSON next to output path (same basename + .cleaning_audit.json)
#         audit_path = out_path + ".cleaning_audit.json"
#         with open(audit_path, "w", encoding="utf-8") as f:
#             json.dump(audit, f, indent=2)
#         print(f"[CLEANING] ✅ {table_name} -> {out_path}")
#         print(f"[CLEANING] 🧾 audit: {audit_path}")
#         return audit

#     finally:
#         spark.stop()
#         print(f"[CLEANING] Spark session stopped for {table_name}.")
        


