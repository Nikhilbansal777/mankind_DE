# MKM_Glue_tranformations/src/jobs/sales_transformations/fact_sales_lines.py
#
# “Wide enriched fact” = order line + product/category + order header basics.

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
from MKM_Glue_tranformations.src.common.semantic_utils import safe_join_with_lineage, write_schema_lineage


def _first(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None


def build(spark, run_id=None):
    # Read silver
    orders = read_silver(spark, "orders")
    items  = read_silver(spark, "order_items")
    prods  = read_silver(spark, "products")
    cats   = read_silver(spark, "category")

    # Normalize keys (order)
    ord_key_o = _first(orders, ["orders_id", "order_id"]) or "order_id"
    ord_key_i = _first(items,  ["orders_id", "order_id"]) or "order_id"
    if ord_key_o != "order_id":
        orders = orders.withColumnRenamed(ord_key_o, "order_id")
    if ord_key_i != "order_id":
        items = items.withColumnRenamed(ord_key_i, "order_id")

    # Normalize keys (product)
    prod_fk_i = _first(items, ["product_id", "products_id"]) or "product_id"
    prod_pk_p = _first(prods, ["products_id", "product_id"]) or "product_id"
    if prod_fk_i != "product_id":
        items = items.withColumnRenamed(prod_fk_i, "product_id")
    if prod_pk_p != "product_id":
        prods = prods.withColumnRenamed(prod_pk_p, "product_id")

    # Normalize keys (category on products)
    cat_fk = _first(prods, ["category_id", "categories_id", "category"])
    if cat_fk and cat_fk != "category_id":
        prods = prods.withColumnRenamed(cat_fk, "category_id")

    # Lineage: capture schemas
    write_schema_lineage("orders", orders.columns,     lineage_stage="sales_transformations")
    write_schema_lineage("order_items", items.columns, lineage_stage="sales_transformations")
    write_schema_lineage("products", prods.columns,    lineage_stage="sales_transformations")
    write_schema_lineage("category", cats.columns,     lineage_stage="sales_transformations")

    # ----- Items ⟕ Products -----
    # Rename the RIGHT join key so only LEFT keeps the name "product_id"
    prods_r = prods.withColumnRenamed("product_id", "products_product_id")
    ip = safe_join_with_lineage(
        left=items,
        right=prods_r,
        left_table="order_items",
        right_table="products",
        on_expr=items["product_id"] == prods_r["products_product_id"],
        how="left",
        join_key_names=("product_id", "products_product_id"),
        lineage_stage="sales_transformations",
    )

    # ----- (ip) ⟕ Category (optional) -----
    # Rename the RIGHT join key to avoid future ambiguity on category_id
    if "category_id" in ip.columns and "category_id" in cats.columns:
        cats_r = cats.withColumnRenamed("category_id", "category_category_id")
        ipc = safe_join_with_lineage(
            left=ip,
            right=cats_r,
            left_table="order_items_products",
            right_table="category",
            on_expr=ip["category_id"] == cats_r["category_category_id"],
            how="left",
            join_key_names=("category_id", "category_category_id"),
            lineage_stage="sales_transformations",
        )
    else:
        ipc = ip

    # ----- Prepare clean orders projection -----
    # Rename RIGHT join key and timestamp aliases so we never clash
    orders_sel = orders.select(
        F.col("order_id").alias("orders_order_id"),
        F.col("user_id"),
        F.col("status"),
        (F.col("created_at") if "created_at" in orders.columns else F.lit(None).cast("timestamp")).alias("o_created_at"),
        (F.col("updated_at") if "updated_at" in orders.columns else F.lit(None).cast("timestamp")).alias("o_updated_at"),
    )

    # ----- (ipc) ⟕ (orders_sel) -----
    oipc = safe_join_with_lineage(
        left=ipc,
        right=orders_sel,
        left_table="items_products_category",
        right_table="orders",
        on_expr=ipc["order_id"] == orders_sel["orders_order_id"],
        how="left",
        join_key_names=("order_id", "orders_order_id"),
        lineage_stage="sales_transformations",
    )

    # Compute line_amount
    qcol = _first(oipc, ["quantity"])
    pcol = _first(oipc, ["product_price"])
    out = (
        oipc
        .withColumn("qty_d",   F.col(qcol).cast("double") if qcol else F.lit(None).cast("double"))
        .withColumn("price_d", F.col(pcol).cast("double") if pcol else F.lit(None).cast("double"))
        .withColumn("line_amount", F.col("qty_d") * F.col("price_d"))
    )

    # Build event_ts from ORDER aliases only (no ambiguity)
    ts_cols = [c for c in ["o_updated_at", "o_created_at"] if c in out.columns]
    out = out.withColumn(
        "event_ts",
        (F.coalesce(*[F.col(c).cast("timestamp") for c in ts_cols])
         if ts_cols else F.lit(None).cast("timestamp"))
    ).withColumn("event_date", F.to_date("event_ts"))

    # Select tidy columns (prefer prefixed descriptive cols if present)
    def pick_existing(cands):
        return [c for c in cands if c in out.columns]

    select_cols = []
    # Keep ONLY the left order key and left product key
    select_cols += pick_existing(["order_id"])
    select_cols += pick_existing(["product_id"])
    select_cols += pick_existing(["user_id", "users_id"])  # normalize below if needed
    select_cols += pick_existing(["status"])
    select_cols += pick_existing(["quantity"])
    select_cols += pick_existing(["product_price"])
    select_cols += pick_existing(["line_amount"])
    # Descriptive from products/category (these may be prefixed by lineage)
    select_cols += pick_existing(["products_name", "name"])
    select_cols += pick_existing(["products_brand", "brand"])
    select_cols += pick_existing(["category_id"])
    select_cols += pick_existing(["category_name"])
    # Timestamps
    select_cols += pick_existing(["event_ts", "event_date"])

    out = out.select(*select_cols)

    # Normalize user column name
    if "users_id" in out.columns and "user_id" not in out.columns:
        out = out.withColumnRenamed("users_id", "user_id")

    # Write
    return write_sales_transform(out, "facts/fact_sales_lines_enriched", partition_by=["event_date"], run_id=run_id)


# Optional CLI entry to run just this job
if __name__ == "__main__":
    from src.connections.db_connections import spark_session_for_JDBC
    from src.utils.lineage import get_run_id
    from src.utils.config_loader import load_env_and_get
    load_env_and_get()
    spark = spark_session_for_JDBC(app_name="fact_sales_lines_enriched_single")
    try:
        print(build(spark, run_id=get_run_id()))
    finally:
        spark.stop()
























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
# from MKM_Glue_tranformations.src.common.io import read_silver, write_transform

# def _pick(df, cols):
#     for c in cols:
#         if c in df.columns: return c
#     return None

# def build(spark, run_id=None):
#     # silver sources
#     items  = read_silver(spark, "order_items")
#     orders = read_silver(spark, "orders")
#     prods  = read_silver(spark, "products")
#     cats   = read_silver(spark, "category")
#     users  = read_silver(spark, "users")
#     try:
#         payments = read_silver(spark, "payments")
#     except Exception:
#         payments = None

#     # normalize order keys
#     ok_i = "orders_id" if "orders_id" in items.columns else "order_id"
#     ok_o = "orders_id" if "orders_id" in orders.columns else "order_id"
#     if ok_i != "orders_id": items  = items.withColumnRenamed(ok_i, "orders_id")
#     if ok_o != "orders_id": orders = orders.withColumnRenamed(ok_o, "orders_id")

#     # normalize product keys
#     pk_i = "product_id" if "product_id" in items.columns else "products_id"
#     pk_p = "products_id" if "products_id" in prods.columns else "product_id"
#     if pk_i != "product_id": items = items.withColumnRenamed(pk_i, "product_id")
#     if pk_p != "product_id": prods = prods.withColumnRenamed(pk_p, "product_id")

#     # product -> category
#     cat_fk = "category_id" if "category_id" in prods.columns else _pick(prods, ["categories_id","category"])
#     if cat_fk and cat_fk != "category_id":
#         prods = prods.withColumnRenamed(cat_fk, "category_id")

#     # join product & category
#     prod_enriched = (
#         prods.join(cats, on="category_id", how="left") if "category_id" in prods.columns and "category_id" in cats.columns
#         else prods
#     )

#     # join items -> product/category
#     out = items.join(prod_enriched, on="product_id", how="left")

#     # order attributes
#     user_fk = _pick(orders, ["user_id","users_id"])
#     if user_fk and user_fk != "user_id":
#         orders = orders.withColumnRenamed(user_fk, "user_id")

#     ts_col = _pick(orders, ["updated_at","update_time","created_at","create_time"])
#     orders_sel = orders.select(
#         "orders_id",
#         *([F.col("user_id")] if "user_id" in orders.columns else []),
#         *([F.col(ts_col).alias("order_ts")] if ts_col else [])
#     )
#     out = out.join(orders_sel, on="orders_id", how="left")

#     # user attributes (avoid PII)
#     if "user_id" in out.columns and "id" in users.columns:
#         u = users.withColumnRenamed("id","user_id")
#         out = out.join(u.select("user_id", *[c for c in ["role","status"] if c in u.columns]), on="user_id", how="left")

#     # payments (optional) — summarize per order (paid/refunded)
#     if payments is not None:
#         pk_pp = "orders_id" if "orders_id" in payments.columns else "order_id"
#         if pk_pp != "orders_id":
#             payments = payments.withColumnRenamed(pk_pp, "orders_id")
#         paid_col   = _pick(payments, ["amount","paid_amount"])
#         refund_col = _pick(payments, ["refund_amount"])
#         pay_agg = payments.groupBy("orders_id").agg(
#             (F.sum(F.col(paid_col).cast("double")) if paid_col else F.lit(0.0)).alias("order_paid_amount"),
#             (F.sum(F.col(refund_col).cast("double")) if refund_col else F.lit(0.0)).alias("order_refunded_amount")
#         )
#         out = out.join(pay_agg, on="orders_id", how="left")
#     else:
#         out = out.withColumn("order_paid_amount", F.lit(None).cast("double")) \
#                  .withColumn("order_refunded_amount", F.lit(None).cast("double"))

#     # line amounts
#     qty_col   = _pick(items, ["quantity"])
#     price_col = _pick(items, ["product_price","price"])
#     out = out.withColumn("quantity_d", F.col(qty_col).cast("double") if qty_col else F.lit(None).cast("double")) \
#              .withColumn("price_d",    F.col(price_col).cast("double") if price_col else F.lit(None).cast("double")) \
#              .withColumn("line_amount", F.col("quantity_d") * F.col("price_d"))

#     # event_date
#     out = out.withColumn("event_date", F.to_date("order_ts") if "order_ts" in out.columns else F.lit(None).cast("date"))

#     # tidy select (you can add more product/category fields if you wish)
#     keep = [c for c in [
#         "orders_id","user_id","product_id",
#         "quantity","product_price","line_amount",
#         "category_id","category_name",
#         "name","brand","status",    # from products
#         "role","status",            # from users (role/status) — keep only role if you want
#         "order_paid_amount","order_refunded_amount",
#         "event_date"
#     ] if c in out.columns]

#     # avoid duplicate "status" (product vs user). Prefer product_status name:
#     cols = []
#     seen = set()
#     for c in keep:
#         if c == "status" and c in seen:
#             cols.append(F.col(c).alias("user_status"))
#         elif c == "status":
#             cols.append(F.col(c).alias("product_status"))
#             seen.add("status")
#         else:
#             cols.append(c)

#     out = out.select(*cols)

#     return write_transform(out, "facts/fact_sales_lines_enriched", partition_by=["event_date"], run_id=run_id)
