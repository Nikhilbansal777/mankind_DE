# MKM_Glue_tranformations/src/jobs/sales_transformations/fact_order_items.py

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
from MKM_Glue_tranformations.src.common.semantic_utils import safe_join_with_lineage

def _first(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def build(spark, run_id=None):
    orders = read_silver(spark, "orders")
    items  = read_silver(spark, "order_items")
    prods  = read_silver(spark, "products")
    cats   = read_silver(spark, "category")

    # --------------------
    # normalize keys
    # --------------------
    # orders
    ord_key_i  = "orders_id" if "orders_id" in items.columns else "order_id"
    ord_key_o  = "orders_id" if "orders_id" in orders.columns else "order_id"
    if ord_key_i != "orders_id":
        items  = items.withColumnRenamed(ord_key_i, "orders_id")
    if ord_key_o != "orders_id":
        orders = orders.withColumnRenamed(ord_key_o, "orders_id")

    # products
    prod_fk_i = "product_id" if "product_id" in items.columns else "products_id"
    prod_pk_p = "products_id" if "products_id" in prods.columns else "product_id"
    if prod_fk_i != "product_id":
        items = items.withColumnRenamed(prod_fk_i, "product_id")
    if prod_pk_p != "product_id":
        prods = prods.withColumnRenamed(prod_pk_p, "product_id")

    # ---- IMPORTANT: avoid duplicate join key after join
    # rename RIGHT key temporarily and join on that; drop it after join
    RIGHT_PROD_KEY = "product_id_right_for_join"
    if "product_id" in prods.columns:
        prods = prods.withColumnRenamed("product_id", RIGHT_PROD_KEY)

    # --------------------
    # items ⟕ products  (lineage-aware to prefix generic RHS cols)
    # --------------------
    items_enriched = safe_join_with_lineage(
        left=items,
        right=prods,
        left_table="order_items",
        right_table="products",
        on_expr=items["product_id"] == prods[RIGHT_PROD_KEY],
        how="left",
        join_key_names=("product_id", RIGHT_PROD_KEY),
        lineage_stage="sales_transformations",
    ).drop(RIGHT_PROD_KEY)  # drop the temp RHS key so only one product_id remains

    # --------------------
    # product -> category (optional), also avoid duplicate key
    # --------------------
    # ensure we have a category_id on the left side
    if "category_id" not in items_enriched.columns:
        alt_cat_fk = _first(items_enriched, ["categories_id", "category"])
        if alt_cat_fk:
            items_enriched = items_enriched.withColumnRenamed(alt_cat_fk, "category_id")

    # normalize category key on cats
    if "category_id" not in cats.columns:
        alt_cat_pk = _first(cats, ["id", "categories_id", "category"])
        if alt_cat_pk:
            cats = cats.withColumnRenamed(alt_cat_pk, "category_id")

    if "category_id" in items_enriched.columns and "category_id" in cats.columns:
        RIGHT_CAT_KEY = "category_id_right_for_join"
        cats = cats.withColumnRenamed("category_id", RIGHT_CAT_KEY)

        items_enriched = safe_join_with_lineage(
            left=items_enriched,
            right=cats,
            left_table="products",
            right_table="category",
            on_expr=items_enriched["category_id"] == cats[RIGHT_CAT_KEY],
            how="left",
            join_key_names=("category_id", RIGHT_CAT_KEY),
            lineage_stage="sales_transformations",
        ).drop(RIGHT_CAT_KEY)

    # --------------------
    # amounts
    # --------------------
    items_enriched = (
        items_enriched
        .withColumn("quantity_d",  F.col("quantity").cast("double") if "quantity" in items_enriched.columns else F.lit(None).cast("double"))
        .withColumn("price_d",     F.col("product_price").cast("double") if "product_price" in items_enriched.columns else F.lit(None).cast("double"))
        .withColumn("line_amount", F.col("quantity_d") * F.col("price_d"))
    )

    # --------------------
    # event date from orders
    # --------------------
    ts_candidates = [c for c in ["updated_at","update_time","created_at","create_time"] if c in orders.columns]
    orders_ts = F.coalesce(*[F.col(c) for c in ts_candidates]).cast("timestamp") if ts_candidates else F.lit(None).cast("timestamp")
    orders_sel = orders.select("orders_id", orders_ts.alias("order_ts"))

    out = (
        items_enriched.join(orders_sel, on="orders_id", how="left")
                      .withColumn("event_date", F.to_date("order_ts"))
    )

    # --------------------
    # select tidy fact grain
    # prefer lineage-prefixed descriptive columns (fallback if missing)
    # --------------------
    descriptive_cols = []
    if "products_name" in out.columns:
        descriptive_cols.append("products_name")
    elif "name" in out.columns:
        descriptive_cols.append("name")

    if "products_brand" in out.columns:
        descriptive_cols.append("products_brand")
    elif "brand" in out.columns:
        descriptive_cols.append("brand")

    if "category_name" in out.columns:
        descriptive_cols.append("category_name")

    base_keep = [
        "orders_id", "product_id",
        "quantity", "product_price", "line_amount",
        "event_date",
        "category_id"
    ]
    keep = [c for c in base_keep + descriptive_cols if c in out.columns]
    out = out.select(*keep)

    return write_sales_transform(out, "facts/fact_order_items", partition_by=["event_date"], run_id=run_id)

# (optional) run directly
if __name__ == "__main__":
    from src.connections.db_connections import spark_session_for_JDBC
    from src.utils.lineage import get_run_id
    from src.utils.config_loader import load_env_and_get
    load_env_and_get()
    spark = spark_session_for_JDBC(app_name="fact_order_items_single")
    try:
        print(build(spark, run_id=get_run_id()))
    finally:
        spark.stop()




























# # MKM_Glue_tranformations/src/jobs/sales_transformations/fact_order_items.py

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
# from MKM_Glue_tranformations.src.common.io import read_silver, write_sales_transform

# def _first(df, candidates):
#     for c in candidates:
#         if c in df.columns: return c
#     return None

# def build(spark, run_id=None):
#     orders = read_silver(spark, "orders")
#     items  = read_silver(spark, "order_items")
#     prods  = read_silver(spark, "products")
#     cats   = read_silver(spark, "category")

#     # normalize keys
#     ord_key_i  = "orders_id" if "orders_id" in items.columns else "order_id"
#     ord_key_o  = "orders_id" if "orders_id" in orders.columns else "order_id"
#     if ord_key_i != "orders_id": items  = items.withColumnRenamed(ord_key_i, "orders_id")
#     if ord_key_o != "orders_id": orders = orders.withColumnRenamed(ord_key_o, "orders_id")

#     prod_fk_i = "product_id" if "product_id" in items.columns else "products_id"
#     prod_pk_p = "products_id" if "products_id" in prods.columns else "product_id"
#     if prod_fk_i != "product_id": items = items.withColumnRenamed(prod_fk_i, "product_id")
#     if prod_pk_p != "product_id": prods = prods.withColumnRenamed(prod_pk_p, "product_id")

#     # product -> category
#     cat_fk = "category_id" if "category_id" in prods.columns else _first(prods, ["categories_id","category"])
#     if cat_fk and cat_fk != "category_id":
#         prods = prods.withColumnRenamed(cat_fk, "category_id")

#     # join items with products (+ category)
#     if "category_id" in prods.columns and "category_id" in cats.columns:
#         items_enriched = (
#             items.join(prods, on="product_id", how="left")
#                  .join(cats, on="category_id", how="left")
#         )
#     else:
#         items_enriched = items.join(prods, on="product_id", how="left")


#     # items_enriched = (
#     #     items.join(prods, on="product_id", how="left")
#     #          .join(cats, on="category_id", how="left") if "category_id" in prods.columns and "category_id" in cats.columns
#     #          else items.join(prods, on="product_id", how="left")
#     # )

#     # amounts
#     items_enriched = (
#         items_enriched
#         .withColumn("quantity_d", F.col("quantity").cast("double") if "quantity" in items_enriched.columns else F.lit(None).cast("double"))
#         .withColumn("price_d",    F.col("product_price").cast("double") if "product_price" in items_enriched.columns else F.lit(None).cast("double"))
#         .withColumn("line_amount", F.col("quantity_d") * F.col("price_d"))
#     )

#     # items_enriched = (
#     #     items_enriched
#     #     .withColumn("quantity_d", F.col("quantity").cast("double"))
#     #     .withColumn("price_d",    F.col("product_price").cast("double"))
#     #     .withColumn("line_amount", F.col("quantity_d") * F.col("price_d"))
#     # )

#     # event date from orders
#     ts_candidates = [c for c in ["updated_at","update_time","created_at","create_time"] if c in orders.columns]
#     orders_ts = F.coalesce(*[F.col(c) for c in ts_candidates]) if ts_candidates else F.lit(None)
#     orders_sel = orders.select("orders_id", orders_ts.alias("order_ts"))
#     out = (
#         items_enriched.join(orders_sel, on="orders_id", how="left")
#                       .withColumn("event_date", F.to_date("order_ts"))
#     )

#     # select tidy fact grain: (orders_id, product_id, [optionally order_item id])
#     keep = [c for c in [
#         "orders_id", "product_id",
#         "quantity", "product_price", "line_amount",
#         "event_date",
#         # optional descriptive fields
#         "name", "brand", "category_id", "category_name"
#     ] if c in out.columns]
#     out = out.select(*keep)

#     return write_sales_transform(out, "facts/fact_order_items", partition_by=["event_date"], run_id=run_id)
