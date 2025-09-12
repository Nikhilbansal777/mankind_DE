# --- bootstrap (keep at very top) ---
import sys
from pathlib import Path
HERE = Path(__file__).resolve()
REPO_ROOT = next(p for p in [HERE.parent] + list(HERE.parents) if (p / "project_bootstrap.py").exists())
sys.path.insert(0, str(REPO_ROOT))
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# ------------------------------------

from typing import Dict, Any, List
from pyspark.sql import SparkSession, DataFrame, functions as F
from src.utils.path_utils import cleaning_output_paths

SEMANTIC_BASE = Path("Data_cleaning") / "MKM_Glue_tranformations" / "transformed_outputs" / "semantic"

def get_spark(app_name: str = "build_semantic") -> SparkSession:
    from src.connections.db_connections import spark_session_for_JDBC
    return spark_session_for_JDBC(app_name=app_name)

def silver_path(table: str) -> str:
    # Silver is your cleaned parquet folder (…/<table>_cleaned.parquet/)
    return cleaning_output_paths(table_name=table, file_format="parquet")

def read_silver(spark: SparkSession, table: str) -> DataFrame:
    return spark.read.parquet(silver_path(table))

def write_semantic(df: DataFrame, name: str, fmt: str = "parquet") -> Dict[str, Any]:
    out_dir = SEMANTIC_BASE / f"{name}.{fmt}"
    if fmt == "parquet":
        df.write.mode("overwrite").parquet(str(out_dir))
    elif fmt == "json":
        df.coalesce(1).write.mode("overwrite").json(str(out_dir))
    elif fmt == "csv":
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(out_dir))
    else:
        raise ValueError(f"Unsupported format: {fmt}")
    rc = df.count()
    print(f"[SEMANTIC] ✅ {name}: {rc} rows -> {out_dir}")
    return {"name": name, "rows": rc, "path": str(out_dir)}

def normalize_snake(df: DataFrame) -> DataFrame:
    """
    Guard for legacy/uppercase sources. Converts all column names to lower snake-ish form.
    Uses native Spark operations for better performance and lower cloud costs.
    """
    # Get columns that need renaming
    rename_map = {c: c.strip().lower() for c in df.columns if c.strip().lower() != c}
    
    # Apply all renames at once using select with aliases
    if rename_map:
        select_exprs = []
        for col in df.columns:
            if col in rename_map:
                select_exprs.append(F.col(col).alias(rename_map[col]))
            else:
                select_exprs.append(F.col(col))
        return df.select(*select_exprs)
    
    return df

def safe_select(df: DataFrame, cols: List[str]) -> DataFrame:
    keep = [c for c in cols if c in df.columns]
    return df.select(*keep)

# =============================================================================
# SPARK SESSION CLEANUP UTILITY
# =============================================================================
# Note: This function is provided for use in other scripts that import this module.
# Always call this at the end of your Spark scripts to prevent memory leaks.

def cleanup_spark_session(spark: SparkSession, app_name: str = "build_semantic"):
    """
    Simple function to clean up Spark session.
    Call this at the end of your scripts to prevent memory leaks.
    
    Usage:
        spark = get_spark("my_app")
        try:
            # Your Spark operations here
            df = spark.read.parquet("path")
        finally:
            cleanup_spark_session(spark, "my_app")
    """
    if spark is not None:
        spark.stop()
        print(f"[SPARK] ✅ Session '{app_name}' stopped and cleaned up")
