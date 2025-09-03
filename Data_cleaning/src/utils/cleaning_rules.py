# Data_cleaning/src/utils/cleaning_rules.py

import os
import yaml
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StringType, IntegerType, DoubleType, LongType, BooleanType, TimestampType
)

# Support both SparkType strings and SQL-ish lowercase
TYPE_MAP = {
    "StringType": StringType(),   "string": StringType(),
    "IntegerType": IntegerType(), "int": IntegerType(), "integer": IntegerType(),
    "DoubleType": DoubleType(),   "double": DoubleType(), "float": DoubleType(),
    "LongType": LongType(),       "long": LongType(), "bigint": LongType(),
    "BooleanType": BooleanType(), "boolean": BooleanType(), "bool": BooleanType(),
    "TimestampType": TimestampType(), "timestamp": TimestampType(), "datetime": TimestampType(),
}

def clean_dataframe_with_rules(df: DataFrame, rules: dict) -> DataFrame:
    if not rules:
        return df
    cleaned_df = df

    # Rename columns (support both keys: rename_columns or renames)
    rename_map = rules.get("rename_columns") or rules.get("renames") or {}
    for old_col, new_col in rename_map.items():
        if old_col in cleaned_df.columns and new_col != old_col:
            cleaned_df = cleaned_df.withColumnRenamed(old_col, new_col)

    # Type conversions
    type_map = rules.get("type_conversions", {})
    for col, target_type in type_map.items():
        if col in cleaned_df.columns:
            spark_type = TYPE_MAP.get(str(target_type))
            if spark_type:
                cleaned_df = cleaned_df.withColumn(col, cleaned_df[col].cast(spark_type))

    # Null replacements
    null_map = rules.get("null_replacements", {})
    for col, replacement in null_map.items():
        if col in cleaned_df.columns:
            cleaned_df = cleaned_df.withColumn(
                col, F.when(F.col(col).isNull(), F.lit(replacement)).otherwise(F.col(col))
            )

    # Case formatting
    case_rules = rules.get("case_formatting", {})
    for col, fmt in case_rules.items():
        if col in cleaned_df.columns:
            if fmt == "lower":
                cleaned_df = cleaned_df.withColumn(col, F.lower(F.col(col)))
            elif fmt == "upper":
                cleaned_df = cleaned_df.withColumn(col, F.upper(F.col(col)))
            elif fmt in ("capitalize", "title"):
                cleaned_df = cleaned_df.withColumn(col, F.initcap(F.col(col)))

    # Unit standardization
    unit_rules = rules.get("standardize_units", {})
    for col, spec in unit_rules.items():
        if col in cleaned_df.columns and isinstance(spec, dict):
            # normalize whitespaces
            cleaned_df = cleaned_df.withColumn(col, F.regexp_replace(F.col(col), r"\s+", ""))
            from_u = str(spec.get("from", ""))
            to_u   = str(spec.get("to", ""))
            if from_u and to_u:
                cleaned_df = cleaned_df.withColumn(col, F.regexp_replace(F.col(col), from_u, to_u))
            factor = spec.get("factor", None)
            if factor is not None:
                cleaned_df = cleaned_df.withColumn(col, F.col(col) * F.lit(float(factor)))

    # Optional drops
    for c in rules.get("drop_columns", []):
        if c in cleaned_df.columns:
            cleaned_df = cleaned_df.drop(c)

    # Optional filters
    for expr in rules.get("filters", []):
        cleaned_df = cleaned_df.filter(expr)

    return cleaned_df

def _default_cleaning_yaml_path():
    # Prefer the Data_cleaning/config location
    # Try from CWD; if not found, walk upwards.
    candidates = [
        os.path.join(os.getcwd(), "Data_cleaning", "src", "config", "master_schema_cleaning_rules.yaml"),
        os.path.join(os.getcwd(), "src", "config", "master_schema_cleaning_rules.yaml"),
    ]
    for c in candidates:
        if os.path.exists(c):
            return c
    # last resort: project relative guess
    return "Data_cleaning/src/config/master_schema_cleaning_rules.yaml"

def load_cleaning_config(table_name: str, config_path: str = None) -> dict:
    config_path = config_path or _default_cleaning_yaml_path()
    with open(config_path, "r", encoding="utf-8") as f:
        all_rules = yaml.safe_load(f) or {}

    # Support both shapes:
    # A) {"users": {...}}
    # B) {"version": "v1", "tables": {"users": {...}}}
    if isinstance(all_rules, dict) and "tables" in all_rules and isinstance(all_rules["tables"], dict):
        return all_rules["tables"].get(table_name, {})
    return all_rules.get(table_name, {})
