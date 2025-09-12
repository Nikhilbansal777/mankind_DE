# --- bootstrap ---
import sys
from pathlib import Path
HERE = Path(__file__).resolve()
REPO_ROOT = next(p for p in [HERE.parent] + list(HERE.parents) if (p / "project_bootstrap.py").exists())
sys.path.insert(0, str(REPO_ROOT))
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# -----------------

from pyspark.sql import functions as F
from src.utils.config_loader import load_env_and_get
from .transforms_common import get_spark, read_silver, write_gold, safe_select

def run(format: str = "parquet"):
    load_env_and_get()
    spark = get_spark("gold_fact_orders")
    try:
        orders = read_silver(spark, "orders")
        # expected cleaned cols: orders_id, user_id, status, created_at, CDC flags
        cols = ["orders_id", "user_id", "status", "created_at",
                "is_paid", "is_cancelled", "is_returned", "is_refunded"]
        fact = safe_select(orders, cols).dropDuplicates(["orders_id"])
        # (Optionally join to dim_users for sk_user here if you really want SKs inside facts.)
        return write_gold(fact, "fact_orders", fmt=format)
    finally:
        spark.stop()

if __name__ == "__main__":
    run()
