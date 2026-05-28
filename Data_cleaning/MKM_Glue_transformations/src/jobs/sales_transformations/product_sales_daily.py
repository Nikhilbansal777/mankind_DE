# MKM_Glue_tranformations/src/jobs/sales_transformations/product_sales_daily.py

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
    items  = read_silver(spark, "order_items")
    orders = read_silver(spark, "orders")
    prods  = None
    try:
        prods = read_silver(spark, "products")
    except Exception:
        prods = None

    # Normalize order keys
    ord_key_i = "orders_id" if "orders_id" in items.columns else "order_id"
    ord_key_o = "orders_id" if "orders_id" in orders.columns else "order_id"
    if ord_key_i != "orders_id": items  = items.withColumnRenamed(ord_key_i, "orders_id")
    if ord_key_o != "orders_id": orders = orders.withColumnRenamed(ord_key_o, "orders_id")

    # Normalize product keys
    prod_fk_i = "product_id" if "product_id" in items.columns else "products_id"
    if prod_fk_i != "product_id":
        items = items.withColumnRenamed(prod_fk_i, "product_id")

    # event_date from orders timestamp
    ts_col = _first(orders, ["updated_at", "update_time", "created_at", "create_time"])
    orders_sel = orders.select("orders_id", (F.to_date(F.col(ts_col)) if ts_col else F.lit(None).cast("date")).alias("event_date"))

    # lineage snapshots
    write_schema_lineage("order_items", items.columns, lineage_stage="sales_transformations")
    write_schema_lineage("orders", orders.columns, lineage_stage="sales_transformations")
    if prods is not None:
        write_schema_lineage("products", prods.columns, lineage_stage="sales_transformations")

    # attach event_date
    items_d = items.join(orders_sel, on="orders_id", how="left")

    # numeric fields
    qty_col   = "quantity" if "quantity" in items_d.columns else None
    price_col = "product_price" if "product_price" in items_d.columns else None

    items_d = items_d.withColumn("qty_d", F.col(qty_col).cast("double") if qty_col else F.lit(None).cast("double"))
    items_d = items_d.withColumn("price_d", F.col(price_col).cast("double") if price_col else F.lit(None).cast("double"))
    items_d = items_d.withColumn("line_amount", F.col("qty_d") * F.col("price_d"))

    # aggregate by product_id, event_date
    agg = (
        items_d.groupBy("product_id", "event_date")
               .agg(
                   F.sum("qty_d").alias("units_sold"),
                   F.sum("line_amount").alias("revenue")
               )
    )

    # optional product descriptors
    if prods is not None:
        prod_pk = "products_id" if "products_id" in prods.columns else "product_id"
        if prod_pk != "product_id":
            prods = prods.withColumnRenamed(prod_pk, "product_id")

        keep_prod = [c for c in ["product_id", "name", "brand", "category_id"] if c in prods.columns]
        prods_min = prods.select(*keep_prod)
        agg = agg.join(prods_min, on="product_id", how="left")

    keep = [c for c in ["product_id", "event_date", "units_sold", "revenue", "name", "brand", "category_id"] if c in agg.columns]
    out = agg.select(*keep)

    return write_sales_transform(out, "facts/product_sales_daily", partition_by=["event_date"], run_id=run_id)
