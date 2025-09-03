# Data_cleaning/MKM_Data_Validation_and_cleaning/validators/pre_validations/validators_common/validation_checks.py
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def check_not_null(df: DataFrame, cols):
    issues = {}
    for c in cols:
        if c in df.columns:
            cnt = df.filter(F.col(c).isNull()).count()
            if cnt > 0:
                issues[c] = {"null_count": cnt}
    return issues

def check_unique(df: DataFrame, cols):
    issues = {}
    for c in cols:
        if c in df.columns:
            total = df.count()
            distinct = df.select(c).distinct().count()
            if distinct < total:
                dups = total - distinct
                issues[c] = {"duplicate_count": dups}
    return issues



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
