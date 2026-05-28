# Data_cleaning/dev_support/diagnostics/check_sales_outputs.py

"""
Quick checker for Sales Transformations outputs (read-only).

Usage examples:
  # check a few common datasets
  python -m Data_cleaning.dev_support.diagnostics.check_sales_outputs

  # check specific datasets
  python -m Data_cleaning.dev_support.diagnostics.check_sales_outputs --datasets facts/fact_order_items dims/dim_products

  # show more sample rows
  python -m Data_cleaning.dev_support.diagnostics.check_sales_outputs --limit 20
"""

# --- bootstrap (keep at top) ---
import sys
from pathlib import Path
HERE = Path(__file__).resolve()
REPO_ROOT = next(
    p for p in [HERE.parent] + list(HERE.parents)
    if (p / "project_bootstrap.py").exists()
)
sys.path.insert(0, str(REPO_ROOT))
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# --------------------------------

import argparse
from typing import List, Tuple

from pyspark.sql import SparkSession, DataFrame

# We only import read-only helpers; we will not call any path helper that creates directories.
from src.utils.path_utils import get_project_root
from src.utils.config_loader import load_env_and_get  # just to print where .env was loaded from


# ------------------------------------------------------------------------------
# Read-only path helpers (no directory creation)
# ------------------------------------------------------------------------------
def sales_base_dir() -> Path:
    """
    Prefer OUT_ROOT_SALES_TRANSFORMATIONS from .env if set; otherwise use the
    default repo-relative path. Never create directories here.
    """
    env_root = load_env_and_get("OUT_ROOT_SALES_TRANSFORMATIONS")
    if env_root:
        return Path(env_root)
    return (
        Path(get_project_root())
        / "MKM_Glue_tranformations"
        / "transformed_outputs"
        / "sales_transformations"
    )


def latest_run_path(dataset: str) -> Path:
    """
    Returns the latest run folder's 'data' directory for a given dataset.
    Example returned path:
      .../sales_transformations/facts/fact_order_items/run_id=20250922T034340Z/data
    """
    base = sales_base_dir() / dataset
    if not base.exists():
        raise FileNotFoundError(
            f"Dataset folder not found: {base}\n"
            "• Run a sales transformations job to create it, e.g.\n"
            "  python -m MKM_Glue_tranformations.src.jobs.sales_transformations.build_sales_transformations "
            f"--jobs {dataset.split('/')[-1]}\n"
            f"• Or verify your OUT_ROOT_SALES_TRANSFORMATIONS in .env points to the correct location."
        )
    runs = sorted([p for p in base.glob("run_id=*") if p.is_dir()], reverse=True)
    if not runs:
        raise FileNotFoundError(f"No run_id folders under: {base}")
    data_dir = runs[0] / "data"
    if not data_dir.exists():
        raise FileNotFoundError(f"Found latest run {runs[0].name}, but missing /data: {data_dir}")
    return data_dir


def list_partitions(data_dir: Path) -> List[str]:
    """
    Returns partition folder names like ['event_date=2025-09-22', 'event_date=2025-09-23'] if present.
    If files are written flat (no partitions), returns [].
    """
    if not data_dir.exists():
        return []
    return sorted([p.name for p in data_dir.iterdir() if p.is_dir() and "=" in p.name])


# ------------------------------------------------------------------------------
# Spark helpers
# ------------------------------------------------------------------------------
def spark_session(app_name: str = "check_sales_outputs") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def read_parquet_dir(spark: SparkSession, path: Path) -> DataFrame:
    # Most of our sales outputs are parquet by default.
    return spark.read.parquet(str(path))


def quick_null_counts(df: DataFrame, cols: List[str], limit: int = 5) -> List[Tuple[str, int]]:
    """
    Compute null counts for a few columns (to keep it snappy).
    """
    picked = [c for c in cols if c in df.columns][:limit]
    out = []
    for c in picked:
        out.append((c, df.filter(df[c].isNull()).count()))
    return out


# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------
DEFAULT_DATASETS = [
    # facts
    "facts/fact_sales_lines_enriched",
    "facts/fact_order_items",
    "facts/fact_orders",
    "facts/fact_payments",
    "facts/fact_order_status_latest",
    # dims
    "dims/dim_products",
    "dims/dim_category",
    "dims/dim_users",
    "dims/dim_address",
]


def main():
    ap = argparse.ArgumentParser(description="Check Sales Transformations outputs (read-only).")
    ap.add_argument(
        "--datasets",
        nargs="*",
        default=None,
        help="Space-separated list of dataset paths relative to sales_transformations "
             "(e.g. facts/fact_order_items dims/dim_products). Default: a common set."
    )
    ap.add_argument("--limit", type=int, default=5, help="Rows to show from each dataset (default: 5)")
    args = ap.parse_args()

    # Print where .env was loaded from (your loader already logs this)
    load_env_and_get()

    datasets = args.datasets or DEFAULT_DATASETS

    spark = spark_session()
    try:
        base_dir = sales_base_dir()
        print(f"🔎 Sales base: {base_dir}")

        for ds in datasets:
            print(f"\n===== DATASET: {ds} =====")
            try:
                data_dir = latest_run_path(ds)
                print(f"📦 Latest run data dir: {data_dir}")

                parts = list_partitions(data_dir)
                if parts:
                    print(f"🧩 Partitions ({len(parts)}): {', '.join(parts[:10])}{' ...' if len(parts) > 10 else ''}")
                else:
                    print("🧩 Partitions: (none)")

                # If partitioned, reading the top-level dir will load all partitions
                df = read_parquet_dir(spark, data_dir)
                total = df.count()
                print(f"🔢 Row count: {total}")

                print("🧬 Schema:")
                df.printSchema()

                # quick null counts for a few columns
                sample_cols = df.columns[:5]
                if sample_cols:
                    nulls = quick_null_counts(df, sample_cols, limit=5)
                    if nulls:
                        print("🚦 Null counts (first few columns):")
                        for c, n in nulls:
                            print(f"  - {c}: {n}")

                print(f"👀 Sample (limit {args.limit}):")
                df.show(args.limit, truncate=False)

            except Exception as e:
                print(f"❌ {ds}: {e}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
# EOF