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
    spark = get_spark("gold_fact_order_items")
    try:
        items = read_silver(spark, "order_items")
        orders = read_silver(spark, "orders")

        # expected cleaned cols on items: order_items_id, order_id, product_id, quantity, product_price, subtotal, created_at, is_discounted, is_returned
        fact_cols = ["order_items_id", "order_id", "product_id", "quantity", "product_price", "subtotal", "created_at", "is_discounted", "is_returned"]
        fact = safe_select(items, fact_cols).dropDuplicates(["order_items_id"])
        res1 = write_gold(fact, "fact_order_items", fmt=format)

        # optional: simple daily revenue aggregate (order_date -> sum(subtotal))
        if {"subtotal", "order_id"}.issubset(set(items.columns)) and {"orders_id", "created_at"}.issubset(set(orders.columns)):
            items_by_order = items.select("order_id", F.col("subtotal").cast("double").alias("subtotal"))
            order_dates = orders.select(F.col("orders_id"), F.to_date("created_at").alias("order_date"))
            daily = (order_dates.join(items_by_order, order_dates.orders_id == items_by_order.order_id, "inner")
                               .groupBy("order_date").agg(F.sum("subtotal").alias("gross_revenue")))
            res2 = write_gold(daily, "agg_daily_revenue", fmt=format)
        else:
            res2 = None

        return {"fact_order_items": res1, "agg_daily_revenue": res2}
    finally:
        spark.stop()

if __name__ == "__main__":
    run()
