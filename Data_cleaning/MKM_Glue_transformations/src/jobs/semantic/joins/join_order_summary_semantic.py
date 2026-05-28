# MKM_Glue_tranformations/src/jobs/semantic/joins/join_order_summary_semantic.py

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
from MKM_Glue_tranformations.src.common.io import read_silver, write_semantic


def _first_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None


def build(spark, run_id=None):
    orders = read_silver(spark, "orders")
    items  = read_silver(spark, "order_items")

    # Resolve order keys on both sides
    order_key_items  = "orders_id" if "orders_id" in items.columns else "order_id"
    order_key_orders = "orders_id" if "orders_id" in orders.columns else "order_id"

    # Standardize both to 'orders_id' for the join
    if order_key_items != "orders_id":
        items = items.withColumnRenamed(order_key_items, "orders_id")
    if order_key_orders != "orders_id":
        orders = orders.withColumnRenamed(order_key_orders, "orders_id")

    # Resolve a reasonable timestamp pair and user id
    upd_col = _first_col(orders, ["updated_at", "update_time"])
    crt_col = _first_col(orders, ["created_at", "create_time"])
    user_col = _first_col(orders, ["user_id", "users_id"])
    if user_col and user_col != "user_id":
        orders = orders.withColumnRenamed(user_col, "user_id")

    # Line amount and item aggregation
    items_agg = (
        items
        .withColumn("line_amount",
            F.col("quantity").cast("double") * F.col("product_price").cast("double"))
        .groupBy("orders_id")
        .agg(
            F.sum("line_amount").alias("items_gross"),
            F.sum("quantity").alias("items_qty")
        )
    )

    # Base order cols to bring forward
    select_cols = [c for c in ["orders_id", "user_id", "status", "created_at", "updated_at", "create_time", "update_time"] if c in orders.columns]
    out = (
        orders
        .select(*select_cols)
        .join(items_agg, "orders_id", "left")
    )

    # event_ts/date from best available
    event_ts = F.coalesce(
        *[F.col(c) for c in ["updated_at", "update_time", "created_at", "create_time"] if c in out.columns]
    )
    out = out.withColumn("event_ts", event_ts).withColumn("event_date", F.to_date("event_ts"))

    return write_semantic(out, "order_summary_semantic", partition_by=["event_date"], run_id=run_id)















# # --- bootstrap ---
# import sys
# from pathlib import Path
# HERE = Path(__file__).resolve()
# REPO_ROOT = next(p for p in [HERE.parent] + list(HERE.parents) if (p / "project_bootstrap.py").exists())
# sys.path.insert(0, str(REPO_ROOT))
# from project_bootstrap import bootstrap_project_paths
# bootstrap_project_paths(__file__)
# # ------------------

# from pyspark.sql import functions as F
# from MKM_Glue_tranformations.src.common.io import read_silver, write_semantic

# def build(spark):
#     orders = read_silver(spark, "orders")
#     items  = read_silver(spark, "order_items")

#     items_agg = (items
#         .withColumn("line_amount",
#             F.col("quantity").cast("double") * F.col("product_price").cast("double"))
#         .groupBy("order_id")
#         .agg(
#             F.sum("line_amount").alias("items_gross"),
#             F.sum("quantity").alias("items_qty"))
#     )

#     out = (orders
#         .select("order_id","user_id","status","created_at","updated_at")
#         .join(items_agg, "order_id", "left")
#         .withColumn("event_ts", F.coalesce("updated_at","created_at"))
#         .withColumn("event_date", F.to_date("event_ts"))
#     )
#     return write_semantic(out, "order_summary_semantic", partition_by=["event_date"])
