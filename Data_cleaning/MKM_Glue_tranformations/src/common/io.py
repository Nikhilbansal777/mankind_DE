# MKM_Glue_tranformations/src/common/io.py

# --- bootstrap (keep at top) ---
import sys, os
from pathlib import Path
HERE = Path(__file__).resolve()
REPO_ROOT = next(p for p in [HERE.parent] + list(HERE.parents) if (p / "project_bootstrap.py").exists())
sys.path.insert(0, str(REPO_ROOT))
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# --------------------------------

from typing import Iterable, Optional
from pyspark.sql import SparkSession, DataFrame
from src.utils.path_utils import get_project_root, cleaning_output_paths

# ----- Silver readers -----

def _exists(path_str: str) -> bool:
    return Path(path_str).exists()

def _read_by_format(spark: SparkSession, path: str, fmt: str) -> DataFrame:
    f = (fmt or "json").lower()
    if f == "json":
        return spark.read.json(path)
    if f == "parquet":
        return spark.read.parquet(path)
    if f == "csv":
        return spark.read.options(header=True, inferSchema=True).csv(path)
    raise ValueError(f"Unsupported format: {fmt}")

def read_silver(spark: SparkSession, table: str, file_format: str = "json") -> DataFrame:
    """
    Reads cleaned output for a table using the same resolver as the cleaners.
    Note: for JSON, the cleaner writes to a *directory* named '<table>_cleaned.json'.
    """
    path = cleaning_output_paths(table, file_format=file_format)
    if not _exists(path):
        raise FileNotFoundError(f"[read_silver] No cleaned output found for '{table}' under {Path(path).parent}")
    return _read_by_format(spark, path, file_format)

# ----- Semantic writers -----

def _semantic_base() -> Path:
    # Data_cleaning/MKM_Glue_tranformations/transformed_outputs/semantic
    root = Path(get_project_root())
    return root / "MKM_Glue_tranformations" / "transformed_outputs" / "semantic"

def write_semantic(
    df: DataFrame,
    name: str,
    fmt: str = "parquet",
    mode: str = "overwrite",
    partition_by: Optional[Iterable[str]] = None,
) -> str:
    """
    Writes a semantic surface to .../transformed_outputs/semantic/<name> in the chosen format.
    Supports optional partitioning for large datasets.
    Returns the output directory path (string).
    """
    out_dir = _semantic_base() / name
    out_dir.parent.mkdir(parents=True, exist_ok=True)

    writer = df.write.mode(mode)
    if partition_by:
        writer = writer.partitionBy(*list(partition_by))

    f = (fmt or "parquet").lower()
    if f == "parquet":
        writer.parquet(str(out_dir))
    elif f == "json":
        writer.json(str(out_dir))
    elif f == "csv":
        writer.option("header", True).csv(str(out_dir))
    else:
        raise ValueError(f"Unsupported format for write_semantic: {fmt}")

    return str(out_dir)




























# # MKM_Glue_tranformations/src/common/io.py

# # --- bootstrap (keep at very top) ---
# import os
# import sys
# from pathlib import Path
# HERE = Path(__file__).resolve()
# REPO_ROOT = next(p for p in [HERE.parent] + list(HERE.parents) if (p / "project_bootstrap.py").exists())
# sys.path.insert(0, str(REPO_ROOT))
# from project_bootstrap import bootstrap_project_paths
# bootstrap_project_paths(__file__)
# # ------------------------------------

# from typing import Optional, Sequence
# from pyspark.sql import SparkSession, DataFrame

# # Paths (layer roots)
# def _silver_base() -> str:
#     return os.path.join(REPO_ROOT, "Data_cleaning", "MKM_Data_Validation_and_cleaning", "reports", "cleaned_outputs")

# def _semantic_base() -> str:
#     return os.path.join(REPO_ROOT, "MKM_Glue_tranformations", "transformed_outputs", "semantic")

# def _gold_base() -> str:
#     return os.path.join(REPO_ROOT, "MKM_Glue_tranformations", "transformed_outputs", "gold")

# def _ensure_dir(path: str) -> None:
#     os.makedirs(path, exist_ok=True)

# # ---------- READ: Silver (cleaned outputs) ----------
# def _candidate_silver_paths(table: str) -> list[tuple[str, str]]:
#     """
#     Return [(format, path), ...] in preferred read order.
#     Cleaning writes as a *directory* named like users_cleaned.json / .parquet / .csv
#     """
#     base = _silver_base()
#     return [
#         ("parquet", os.path.join(base, f"{table}_cleaned.parquet")),
#         ("json",    os.path.join(base, f"{table}_cleaned.json")),
#         ("csv",     os.path.join(base, f"{table}_cleaned.csv")),
#     ]

# def read_silver(spark: SparkSession, table: str) -> DataFrame:
#     """
#     Read a cleaned table (Silver) regardless of file format.
#     Tries parquet -> json -> csv.
#     """
#     for fmt, path in _candidate_silver_paths(table):
#         if os.path.exists(path):
#             if fmt == "parquet":
#                 return spark.read.parquet(path)
#             elif fmt == "json":
#                 # cleaned JSON is folder-of-json; multiline not required here
#                 return spark.read.json(path)
#             elif fmt == "csv":
#                 return spark.read.options(header=True, inferSchema=True).csv(path)
#     raise FileNotFoundError(f"[read_silver] No cleaned output found for '{table}' under {_silver_base()}")

# # ---------- WRITE: Semantic & Gold ----------
# def _write_parquet(df: DataFrame, out_dir: str, partition_by: Optional[Sequence[str]] = None, mode: str = "overwrite") -> str:
#     _ensure_dir(out_dir)
#     writer = df.write.mode(mode)
#     if partition_by:
#         writer = writer.partitionBy(list(partition_by))
#     writer.parquet(out_dir)  # default Snappy
#     return out_dir

# def write_semantic(df: DataFrame, surface_name: str, partition_by: Optional[Sequence[str]] = None, mode: str = "overwrite") -> str:
#     """
#     Write a semantic surface as parquet under transformed_outputs/semantic/<surface_name>/
#     """
#     out_dir = os.path.join(_semantic_base(), surface_name)
#     return _write_parquet(df, out_dir, partition_by=partition_by, mode=mode)

# def write_gold(df: DataFrame, mart: str, entity_name: str, partition_by: Optional[Sequence[str]] = None, mode: str = "overwrite") -> str:
#     """
#     Write a gold (mart) entity as parquet under transformed_outputs/gold/<mart>/<entity_name>/
#     """
#     out_dir = os.path.join(_gold_base(), mart, entity_name)
#     return _write_parquet(df, out_dir, partition_by=partition_by, mode=mode)

# # ---------- Small guardrails ----------
# def assert_layer_read(path: str, expected_prefix: str) -> None:
#     """
#     Optional: ensure a job is reading from the intended layer path.
#     """
#     norm = os.path.normpath(path)
#     exp  = os.path.normpath(expected_prefix)
#     if not norm.startswith(exp):
#         raise AssertionError(f"[guard] Expected path to start with {exp}, got {norm}")
