# MKM - Sales smoke test (quick post-run checks)
# Location: Data_cleaning/MKM_Glue_tranformations/src/validations/sales_smoke_test.py

from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime

# ---------------- Minimal logger ----------------
def get_logger(name: str):
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    return logging.getLogger(name)

logger = get_logger("sales_smoke_test")

# ---------------- Local Spark (no JDBC needed) ----------------
def get_local_spark(app_name: str):
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")   # local mode is enough to read Parquet/JSON
        .getOrCreate()
    )

# ---------------- Resolve OUT_BASE ----------------
HERE = Path(__file__).resolve()
# find the Glue package root to default the output path
GLUE_ROOT = next((p for p in [HERE] + list(HERE.parents) if p.name == "MKM_Glue_tranformations"), None)

OUT_BASE = os.getenv(
    "OUT_ROOT_SALES_TRANSFORMATIONS",
    str((GLUE_ROOT or HERE.parents[3]) / "transformed_outputs" / "sales_transformations")
)

DATASETS = [
    "facts/fact_orders",
    "facts/fact_order_items",
    "facts/fact_sales_lines_enriched",
    "facts/fact_payments",
    "facts/payments_by_order",
    "facts/fact_order_status_latest",
    "facts/product_sales_daily",
    "facts/users_orders_daily",
    "dims/dim_products",
    "dims/dim_users",
    "dims/dim_address",
    "dims/dim_category",
]

# ---------------- Helpers ----------------
def _latest_run_data_dir(dataset_rel: str) -> Path | None:
    ds_root = Path(OUT_BASE) / dataset_rel
    if not ds_root.exists():
        return None
    run_dirs = sorted([p for p in ds_root.glob("run_id=*") if p.is_dir()], reverse=True)
    if not run_dirs:
        return None
    data_dir = run_dirs[0] / "data"
    return data_dir if data_dir.exists() else None

def _infer_and_read(spark, data_dir: Path):
    # Your outputs are Parquet; fall back to JSON if needed
    try:
        return spark.read.parquet(str(data_dir))
    except Exception:
        return spark.read.json(str(data_dir))

def _tick(ok: bool) -> str:
    return "✅" if ok else "❌"

# ---------------- Main ----------------
def main():
    print(f"🔎 OUT_BASE: {OUT_BASE}")
    spark = get_local_spark("MKM_SalesSmokeTest")

    results = []
    for ds in DATASETS:
        data_dir = _latest_run_data_dir(ds)
        if data_dir is None:
            msg = f"{ds}: missing dataset or no run_id/*/data folder"
            logger.warning(msg)
            results.append((ds, False, msg))
            continue

        try:
            df = _infer_and_read(spark, data_dir)
            cnt = df.limit(1).count()
            cols = df.columns
            sample_cols = ", ".join(cols[:8]) + ("..." if len(cols) > 8 else "")
            ok = cnt > 0
            msg = f"{ds}: rows>0={ok} | sample_columns=[{sample_cols}] | path={data_dir}"
            (logger.info if ok else logger.warning)(msg)
            results.append((ds, ok, msg))
        except Exception as e:
            msg = f"{ds}: read error -> {e}"
            logger.exception(msg)
            results.append((ds, False, msg))

    failed = [r for r in results if not r[1]]

    print("\n===== SMOKE SUMMARY =====")
    for ds, ok, msg in results:
        print(f"{_tick(ok)} {msg}")

    print("\nOverall:", "✅ OK" if not failed else f"❌ {len(failed)} dataset(s) failed")
    spark.stop()

if __name__ == "__main__":
    main()




























# # MKM - Sales smoke test (quick post-run checks)
# # Location: Data_cleaning/MKM_Glue_tranformations/src/validations/sales_smoke_test.py

# import os
# import sys
# from pathlib import Path
# from datetime import datetime

# # ---------------- Path wiring (robust) ----------------
# HERE = Path(__file__).resolve()

# # Find the repo root that contains project_bootstrap.py and src/
# # (this is your Data_cleaning folder)
# REPO_ROOT = next(
#     (p for p in [HERE] + list(HERE.parents) if (p / "project_bootstrap.py").exists() and (p / "src").exists()),
#     None,
# )
# if REPO_ROOT is None:
#     raise RuntimeError("Could not locate repo root (folder containing project_bootstrap.py and src/).")

# # Put REPO_ROOT first so 'project_bootstrap' and 'src.*' resolve cleanly
# REPO_ROOT_STR = str(REPO_ROOT)
# if REPO_ROOT_STR not in sys.path:
#     sys.path.insert(0, REPO_ROOT_STR)

# # (Optional) also add the glue package root so you can import its modules too
# GLUE_ROOT = next((p for p in [HERE] + list(HERE.parents) if p.name == "MKM_Glue_tranformations"), None)
# if GLUE_ROOT:
#     GLUE_SRC = GLUE_ROOT / "src"
#     for p in (str(GLUE_ROOT), str(GLUE_SRC)):
#         if p not in sys.path:
#             sys.path.insert(0, p)

# # Now bootstrap, which also loads .env etc.
# from project_bootstrap import bootstrap_project_paths
# bootstrap_project_paths(__file__)

# # After bootstrap, your normal imports will work:
# from src.connections.db_connections import spark_session_for_JDBC
# from src.utils.log_utils import get_logger

# # ---------------- Imports after bootstrap ----------------
# from pyspark.sql import functions as F
# from src.connections.db_connections import spark_session_for_JDBC  # now resolves
# try:
#     from src.utils.log_utils import get_logger
# except Exception:
#     import logging
#     logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
#     get_logger = lambda name: logging.getLogger(name)

# # ---------------- Config ----------------
# # Prefer the env var your other tools use; default to the standard outputs folder
# OUT_BASE = os.getenv(
#     "OUT_ROOT_SALES_TRANSFORMATIONS",
#     str(GLUE_ROOT / "transformed_outputs" / "sales_transformations")
# )

# # datasets to sanity-check (relative to OUT_BASE)
# DATASETS = [
#     "facts/fact_orders",
#     "facts/fact_order_items",
#     "facts/fact_sales_lines_enriched",
#     "facts/fact_payments",
#     "facts/payments_by_order",
#     "facts/fact_order_status_latest",
#     "facts/product_sales_daily",
#     "facts/users_orders_daily",
#     "dims/dim_products",
#     "dims/dim_users",
#     "dims/dim_address",
#     "dims/dim_category",
# ]

# logger = get_logger("sales_smoke_test")

# # ---------------- Helpers ----------------
# def _latest_run_data_dir(dataset_rel: str) -> Path | None:
#     """
#     Returns the '.../run_id=XXXX/data' path for the latest run,
#     or None if the dataset folder is missing.
#     """
#     ds_root = Path(OUT_BASE) / dataset_rel
#     if not ds_root.exists():
#         return None

#     run_dirs = sorted([p for p in ds_root.glob("run_id=*") if p.is_dir()], reverse=True)
#     if not run_dirs:
#         return None

#     data_dir = run_dirs[0] / "data"
#     return data_dir if data_dir.exists() else None

# def _infer_and_read(spark, data_dir: Path):
#     """
#     Your outputs are JSON folders; read as JSON.
#     (If you change writer formats later, adapt here.)
#     """
#     return spark.read.json(str(data_dir))

# def _fmt_bool(ok: bool) -> str:
#     return "✅" if ok else "❌"

# # ---------------- Main ----------------
# def main():
#     print(f"🔎 OUT_BASE: {OUT_BASE}")
#     spark = spark_session_for_JDBC(app_name="MKM_SalesSmokeTest")

#     results = []
#     for ds in DATASETS:
#         data_dir = _latest_run_data_dir(ds)
#         if data_dir is None:
#             msg = f"{ds}: missing dataset or no run_id/*/data folder"
#             logger.warning(msg)
#             results.append((ds, False, msg))
#             continue

#         try:
#             df = _infer_and_read(spark, data_dir)
#             cnt = df.limit(1).count()
#             schema = ", ".join(df.columns[:8]) + ("..." if len(df.columns) > 8 else "")
#             ok = cnt > 0
#             msg = f"{ds}: rows>0={ok}  | sample_columns=[{schema}]  | path={data_dir}"
#             (logger.info if ok else logger.warning)(msg)
#             results.append((ds, ok, msg))
#         except Exception as e:
#             msg = f"{ds}: read error -> {e}"
#             logger.exception(msg)
#             results.append((ds, False, msg))

#     # Summary
#     failed = [r for r in results if not r[1]]
#     print("\n===== SMOKE SUMMARY =====")
#     for ds, ok, msg in results:
#         print(f"{_fmt_bool(ok)} {msg}")

#     print("\nOverall:", "✅ OK" if not failed else f"❌ {len(failed)} dataset(s) failed")
#     spark.stop()

# if __name__ == "__main__":
#     main()
