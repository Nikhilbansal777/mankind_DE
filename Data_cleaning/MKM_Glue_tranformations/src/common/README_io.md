# IO Helper for Layered ETL (Silver → Semantic → Gold)

This README documents `MKM_Glue_tranformations/src/common/io.py`, a tiny utility that standardizes how we **read Silver outputs** and **write Semantic/Gold outputs** in this repo.

---

## Why this exists

- One place to define **where** each layer lives on disk.
- Consistent **read/write** logic (format-agnostic reads from Silver; Parquet writes for Semantic/Gold).
- Safer **layer boundaries** so Semantic never writes into Silver, etc.
- Fewer hard‑coded paths scattered across jobs.

---

## Layer roots (conventions)

- **Silver (cleaned outputs):**  
  `Data_cleaning/MKM_Data_Validation_and_cleaning/reports/cleaned_outputs/`
  - File-family directories like `users_cleaned.json/`, `orders_cleaned.parquet/`, etc.

- **Semantic (conformed/agnostic):**  
  `MKM_Glue_tranformations/transformed_outputs/semantic/<surface_name>/`

- **Gold (mart-specific):**  
  `MKM_Glue_tranformations/transformed_outputs/gold/<mart>/<entity_name>/`

All paths are resolved from the **repo root** via the standard bootstrap you already use.

---

## Public API

### `read_silver(spark, table: str) -> DataFrame`
Reads a cleaned Silver table (tries **parquet → json → csv** in that order).  
**Input name** is just the table (e.g., `"users"`, `"orders"`). It will look for:  
- `<silver_base>/users_cleaned.parquet/`
- `<silver_base>/users_cleaned.json/`
- `<silver_base>/users_cleaned.csv/`

Raises `FileNotFoundError` if nothing is found.

### `write_semantic(df, surface_name: str, partition_by: Optional[list[str]] = None, mode: str = "overwrite") -> str`
Writes a DataFrame to:
```
MKM_Glue_tranformations/transformed_outputs/semantic/<surface_name>/
```
- Format: **Parquet (Snappy)**.
- Optional `partition_by` for big surfaces (e.g., `["event_date"]`).
- Returns the output directory path.

### `write_gold(df, mart: str, entity_name: str, partition_by: Optional[list[str]] = None, mode: str = "overwrite") -> str`
Writes a DataFrame to:
```
MKM_Glue_tranformations/transformed_outputs/gold/<mart>/<entity_name>/
```
- Format: **Parquet (Snappy)**.
- Optional `partition_by` (e.g., `["ds"]`).
- Returns the output directory path.

### `assert_layer_read(path: str, expected_prefix: str) -> None`
Small guard: ensure a path starts with an expected layer root. Useful in jobs to avoid cross-layer mistakes.

---

## Quickstart (minimal job)

```python
# MKM_Glue_tranformations/src/jobs/semantic/joins/join_order_summary_semantic.py
from pyspark.sql import functions as F
from MKM_Glue_tranformations.src.common.io import read_silver, write_semantic

def build(spark):
    orders = read_silver(spark, "orders")
    items  = read_silver(spark, "order_items")

    agg = (items
           .withColumn("line_amount", F.col("quantity").cast("double") * F.col("product_price").cast("double"))
           .groupBy("orders_id")
           .agg(F.sum("line_amount").alias("items_gross"),
                F.sum("quantity").alias("items_qty")))

    out = (orders
           .select("orders_id", "user_id", "status", "created_at", "updated_at")
           .join(agg, "orders_id", "left")
           .withColumn("event_ts", F.coalesce("updated_at", "created_at"))
           .withColumn("event_date", F.to_date("event_ts")))

    return write_semantic(out, "order_summary_semantic", partition_by=["event_date"])
```

Run via a tiny orchestrator (`build_semantic.py`) that creates a Spark session and calls `build()`.

---

## Patterns & Tips

- **Prefer Parquet** for downstream layers; Silver can remain JSON for human‑readability.
- **Partition big surfaces** by a date key (`event_date`, `ds`). Use `repartition(n)` if needed.
- **Keep joins & enrichments “light”** in Semantic: normalize types, derive standard flags, aggregate to common grains.
- **Gold is star‑schema**: `dim_*` and `fact_*` built from Semantic surfaces.

---

## Troubleshooting

### `FileNotFoundError` in `read_silver`
- Verify the cleaner produced output under:  
  `Data_cleaning/MKM_Data_Validation_and_cleaning/reports/cleaned_outputs/`
- Ensure the naming matches `<table>_cleaned.<ext>/` (directory), not a single flat file.

### JSON vs Parquet
- Silver JSON is a **directory** of JSON files. Spark reads the directory, not a single `.json` file.
- If you switched Silver to Parquet, `read_silver` will pick Parquet first automatically.

### Windows paths
- Paths are built with `os.path.join` and should be Windows-safe.
- If you copy paths into shell commands, quote them if spaces exist.

---

## Extending the helper

- Add `read_semantic(...)` / `read_gold(...)` if you want symmetric readers.
- Add a small registry in this module if some surfaces always require certain partitions.

---

## Versioning

This helper is intentionally tiny. If you ever publish as a package, keep the same function signatures for backward compatibility.
