import json
import os
from typing import Optional, Iterable

def _ensure_parent(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)

def write_json(obj, path: str, *, indent: int = 2) -> str:
    _ensure_parent(path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=indent)
    return path

def read_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def write_dataframe(df, path: str, *, format: str = "parquet",
                    mode: str = "overwrite", partitionBy: Optional[Iterable[str]] = None, **options) -> str:
    """
    Generic Spark writer for consistency (works for parquet/csv/json).
    Example:
      write_dataframe(df, '.../users_cleaned.parquet', format='parquet', mode='overwrite')
      write_dataframe(df, '.../users_cleaned.csv', format='csv', header=True)
    """
    _ensure_parent(path)
    writer = df.write.mode(mode)
    if partitionBy:
        writer = writer.partitionBy(*partitionBy)
    if format.lower() == "csv":
        # sensible CSV defaults
        options.setdefault("header", True)
        options.setdefault("escape", '"')
    writer.options(**options).format(format).save(path)
    return path

def read_dataframe(spark, path: str, *, format: Optional[str] = None, **options):
    if not format:
        # infer from extension (basic)
        ext = os.path.splitext(path)[1].lower().lstrip(".")
        format = {"parquet": "parquet", "csv": "csv", "json": "json"}.get(ext, "parquet")
    reader = spark.read.options(**options).format(format)
    return reader.load(path)
