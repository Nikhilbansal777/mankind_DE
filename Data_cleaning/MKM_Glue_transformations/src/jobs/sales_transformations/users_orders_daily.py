# MKM_Glue_tranformations/src/jobs/sales_transformations/users_orders_daily.py

# --- bootstrap ---
import sys
from pathlib import Path
HERE = Path(__file__).resolve()
REPO_ROOT = next(p for p in [HERE.parent] + list(HERE.parents) if (p / "project_bootstrap.py").exists())
sys.path.insert(0, str(REPO_ROOT))
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# ------------------

from pyspark.sql import functions as F
from MKM_Glue_tranformations.src.common.io import read_silver, write_sales_transform
from MKM_Glue_tranformations.src.common.semantic_utils import write_schema_lineage

def _first(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def build(spark, run_id=None):
    orders = read_silver(spark, "orders")
    users  = None
    try:
        users = read_silver(spark, "users")
    except Exception:
        users = None

    # Normalize keys
    ord_key = "orders_id" if "orders_id" in orders.columns else "order_id"
    if ord_key != "orders_id":
        orders = orders.withColumnRenamed(ord_key, "orders_id")

    user_key = _first(orders, ["user_id", "users_id"])
    if user_key and user_key != "user_id":
        orders = orders.withColumnRenamed(user_key, "user_id")

    # event ts -> event_date
    ts_col = _first(orders, ["updated_at", "update_time", "created_at", "create_time"])
    orders = orders.withColumn("event_date", F.to_date(F.col(ts_col)) if ts_col else F.lit(None).cast("date"))

    # lineage schema snapshot
    write_schema_lineage("orders", orders.columns, lineage_stage="sales_transformations")
    if users is not None:
        write_schema_lineage("users", users.columns, lineage_stage="sales_transformations")

    agg = (
        orders.groupBy("user_id", "event_date")
              .agg(
                  F.count(F.lit(1)).alias("orders_count"),
                  F.min(F.col(ts_col)).alias("first_order_ts") if ts_col else F.lit(None).alias("first_order_ts"),
                  F.max(F.col(ts_col)).alias("last_order_ts")  if ts_col else F.lit(None).alias("last_order_ts"),
              )
    )

    if users is not None and "id" in users.columns:
        users_min = users.select(F.col("id").alias("user_id"))
        agg = agg.join(users_min, on="user_id", how="left")

    keep = [c for c in ["user_id", "event_date", "orders_count", "first_order_ts", "last_order_ts"] if c in agg.columns]
    out = agg.select(*keep)

    return write_sales_transform(out, "facts/users_orders_daily", partition_by=["event_date"], run_id=run_id)
