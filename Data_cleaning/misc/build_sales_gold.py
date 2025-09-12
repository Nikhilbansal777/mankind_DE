# --- bootstrap (keep at very top) ---
import sys, os
from pathlib import Path
HERE = Path(__file__).resolve()
REPO_ROOT = next(p for p in [HERE.parent] + list(HERE.parents) if (p / "project_bootstrap.py").exists())
sys.path.insert(0, str(REPO_ROOT))
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# ------------------------------------

import argparse
from datetime import datetime, timezone
from pyspark.sql import SparkSession, functions as F

from src.utils.config_loader import load_env_and_get
from src.utils.path_utils import cleaning_output_paths, get_project_root

GOLD_OUT = Path("Data_cleaning") / "MKM_Glue_tranformations" / "transformed_outputs" / "sales"

def _spark(app="build_sales_gold"):
    from src.connections.db_connections import spark_session_for_JDBC
    # we just need a SparkSession; reuse your JDBC helper so jars/logging are consistent
    return spark_session_for_JDBC(app_name=app)

def _silver_parquet_path(table: str) -> str:
    # uses your central helper; returns ..../<table>_cleaned.parquet (a folder)
    return cleaning_output_paths(table_name=table, file_format="parquet")

def _read_silver(spark: SparkSession, table: str):
    path = _silver_parquet_path(table)
    return spark.read.parquet(path)

def _write_gold(df, name: str, fmt: str = "parquet"):
    out_dir = GOLD_OUT / f"{name}.{fmt}"
    if fmt == "parquet":
        df.write.mode("overwrite").parquet(str(out_dir))
    elif fmt == "json":
        df.coalesce(1).write.mode("overwrite").json(str(out_dir))
    else:
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(out_dir))
    print(f"[GOLD] ✅ {name} -> {out_dir}")

def build_gold(format: str = "parquet"):
    load_env_and_get()  # loads .env for Spark/JDBC path setup etc.
    spark = _spark()

    try:
        ts = datetime.now(timezone.utc).isoformat()
        print(f"[GOLD] ▶ building sales gold at {ts}")

        # --------- read SILVER ----------
        users   = _read_silver(spark, "users")        # expects users_id, email, created_at, is_active (from CDC flags)
        products= _read_silver(spark, "products")     # expects products_id, name, price
        orders  = _read_silver(spark, "orders")       # expects orders_id, user_id, status, created_at, is_paid/is_cancelled/...
        items   = _read_silver(spark, "order_items")  # expects order_items_id, order_id, product_id, quantity, product_price, subtotal, created_at

        # Normalize minimal columns defensively (skip if missing) – no UDFs
        def _has(df, *cols): return all(c in df.columns for c in cols)

        # --------- DIM USERS ----------
        dim_users = users
        if _has(users, "users_id"):
            sel = ["users_id"]
            for c in ["email", "created_at", "is_active"]:
                if c in users.columns: sel.append(c)
            dim_users = users.select(*sel).dropDuplicates(["users_id"])
            _write_gold(dim_users, "dim_users", format)

        # --------- DIM PRODUCTS ----------
        dim_products = products
        if _has(products, "products_id"):
            sel = ["products_id"]
            for c in ["name", "price"]:
                if c in products.columns: sel.append(c)
            dim_products = products.select(*sel).dropDuplicates(["products_id"])
            _write_gold(dim_products, "dim_products", format)

        # --------- FACT ORDERS ----------
        fact_orders = orders
        if _has(orders, "orders_id"):
            sel = ["orders_id"]
            for c in ["user_id", "status", "created_at", "is_paid", "is_cancelled", "is_returned", "is_refunded"]:
                if c in orders.columns: sel.append(c)
            fact_orders = orders.select(*sel).dropDuplicates(["orders_id"])
            # Suggested sort column for Redshift later
            if "created_at" in fact_orders.columns:
                fact_orders = fact_orders.withColumn("created_at", F.col("created_at"))
            _write_gold(fact_orders, "fact_orders", format)

        # --------- FACT ORDER ITEMS ----------
        fact_items = items
        if _has(items, "order_items_id"):
            sel = ["order_items_id"]
            for c in ["order_id", "product_id", "quantity", "product_price", "subtotal", "created_at", "is_discounted", "is_returned"]:
                if c in items.columns: sel.append(c)
            fact_items = items.select(*sel).dropDuplicates(["order_items_id"])
            _write_gold(fact_items, "fact_order_items", format)

        # --------- OPTIONAL AGG (daily revenue) ----------
        if _has(items, "subtotal") and _has(orders, "orders_id", "created_at"):
            items_by_order = items.select("order_id", F.col("subtotal").cast("double").alias("subtotal"))
            orders_dates = orders.select(F.col("orders_id"), F.to_date("created_at").alias("order_date"))
            revenue_daily = (orders_dates.join(items_by_order, orders_dates.orders_id == items_by_order.order_id, "inner")
                             .groupBy("order_date").agg(F.sum("subtotal").alias("gross_revenue")))
            _write_gold(revenue_daily, "agg_daily_revenue", format)

        print("[GOLD] 🎉 build complete")

    finally:
        spark.stop()

def main():
    ap = argparse.ArgumentParser(description="Build Sales Gold (dims & facts) from Silver")
    ap.add_argument("--format", choices=["parquet", "json", "csv"], default="parquet")
    args = ap.parse_args()
    GOLD_OUT.mkdir(parents=True, exist_ok=True)
    build_gold(format=args.format)

if __name__ == "__main__":
    main()
