# MKM_Glue_tranformations/src/jobs/sales_transformations/fact_orders.py

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
    orders = read_silver(spark, "orders")
    items  = read_silver(spark, "order_items")
    pays   = read_silver(spark, "payments")

    # ---------- Normalize order keys on orders & items ----------
    ord_key_o = _first(orders, ["orders_id", "order_id"]) or "order_id"
    ord_key_i = _first(items,  ["orders_id", "order_id"]) or "order_id"
    if ord_key_o != "order_id": orders = orders.withColumnRenamed(ord_key_o, "order_id")
    if ord_key_i != "order_id": items  = items.withColumnRenamed(ord_key_i, "order_id")

    # NOTE: payments may NOT have order_id; we'll handle that safely below
    pay_has_order = any(c in pays.columns for c in ["orders_id", "order_id"])
    if pay_has_order:
        ord_key_p = "order_id" if "order_id" in pays.columns else "orders_id"
        if ord_key_p != "order_id":
            pays = pays.withColumnRenamed(ord_key_p, "order_id")

    # ---------- Lineage ----------
    write_schema_lineage("orders", orders.columns, lineage_stage="sales_transformations")
    write_schema_lineage("order_items", items.columns, lineage_stage="sales_transformations")
    write_schema_lineage("payments", pays.columns, lineage_stage="sales_transformations")

    # ---------- Items aggregation (order level) ----------
    qty_col   = _first(items, ["quantity"])
    price_col = _first(items, ["product_price", "price", "unit_price"])
    items_agg = (
        items
        .withColumn("qty_d",   F.col(qty_col).cast("double")   if qty_col   else F.lit(0.0))
        .withColumn("price_d", F.col(price_col).cast("double") if price_col else F.lit(0.0))
        .withColumn("line_amount", F.col("qty_d") * F.col("price_d"))
        .groupBy("order_id")
        .agg(
            F.sum("qty_d").alias("items_qty"),
            F.sum("line_amount").alias("items_gross")
        )
    )

    # ---------- Disambiguate join keys on RIGHT sides ----------
    items_agg_r = items_agg.withColumnRenamed("order_id", "order_id_items")

    # orders ⟕ items_agg
    oi = safe_join_with_lineage(
        left=orders,
        right=items_agg_r,
        left_table="orders",
        right_table="order_items_agg",
        on_expr=orders["order_id"] == items_agg_r["order_id_items"],
        how="left",
        join_key_names=("order_id", "order_id_items"),
        lineage_stage="sales_transformations",
    )

    # ---------- Payments aggregation (optional) ----------
    oip = oi
    if pay_has_order:
        amt_col = _first(pays, ["amount", "payment_amount", "total"])
        pays_agg = (
            pays.groupBy("order_id")
                .agg(F.sum(F.col(amt_col).cast("double")).alias("payments_total")) if amt_col else
            pays.groupBy("order_id").agg(F.lit(0.0).alias("payments_total"))
        )
        pays_agg_r = pays_agg.withColumnRenamed("order_id", "order_id_pays")

        oip = safe_join_with_lineage(
            left=oi,
            right=pays_agg_r,
            left_table="orders_items",
            right_table="payments_agg",
            on_expr=F.col("order_id") == F.col("order_id_pays"),
            how="left",
            join_key_names=("order_id", "order_id_pays"),
            lineage_stage="sales_transformations",
        )
    else:
        # No order key in payments -> emit payments_total as NULL
        oip = oip.withColumn("payments_total", F.lit(None).cast("double"))

    # ---------- event_ts / event_date ----------
    ts_candidates = [c for c in ["updated_at", "update_time", "created_at", "create_time"] if c in oip.columns]
    event_ts = F.coalesce(*[F.col(c) for c in ts_candidates]) if ts_candidates else F.lit(None)
    out = (
        oip
        .withColumn("event_ts", event_ts.cast("timestamp"))
        .withColumn("event_date", F.to_date("event_ts"))
    )

    # ---------- Select final columns ----------
    keep = [c for c in [
        "order_id",
        "user_id", "users_id", "status",
        "items_qty", "items_gross", "payments_total",
        "created_at", "updated_at", "create_time", "update_time",
        "event_ts", "event_date"
    ] if c in out.columns]

    out = out.select(*keep)

    # normalize user key to user_id
    if "users_id" in out.columns and "user_id" not in out.columns:
        out = out.withColumnRenamed("users_id", "user_id")

    return write_sales_transform(out, "facts/fact_orders", partition_by=["event_date"], run_id=run_id)











































# # MKM_Glue_tranformations/src/jobs/sales_transformations/fact_orders.py

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
# from MKM_Glue_tranformations.src.common.semantic_utils import safe_join_with_lineage, write_schema_lineage

# def _first(df, candidates):
#     for c in candidates:
#         if c in df.columns: return c
#     return None

# def build(spark, run_id=None):
#     orders = read_silver(spark, "orders")
#     items  = read_silver(spark, "order_items")
#     pays   = read_silver(spark, "payments")

#     # normalize order keys across all
#     ord_key_o = _first(orders, ["orders_id","order_id"]) or "order_id"
#     ord_key_i = _first(items,  ["orders_id","order_id"]) or "order_id"
#     ord_key_p = _first(pays,   ["orders_id","order_id"]) or "order_id"

#     if ord_key_o != "order_id": orders = orders.withColumnRenamed(ord_key_o, "order_id")
#     if ord_key_i != "order_id": items  = items.withColumnRenamed(ord_key_i, "order_id")
#     if ord_key_p != "order_id": pays   = pays.withColumnRenamed(ord_key_p, "order_id")

#     # schemas to lineage (sales area)
#     write_schema_lineage("orders", orders.columns, lineage_stage="sales_transformations")
#     write_schema_lineage("order_items", items.columns, lineage_stage="sales_transformations")
#     write_schema_lineage("payments", pays.columns, lineage_stage="sales_transformations")

#     # items aggregation -> order level totals
#     items_agg = (
#         items
#         .withColumn("qty_d", F.col(_first(items, ["quantity"]) or F.lit(0)).cast("double"))
#         .withColumn("price_d", F.col(_first(items, ["product_price"]) or F.lit(0)).cast("double"))
#         .withColumn("line_amount", F.col("qty_d") * F.col("price_d"))
#         .groupBy("order_id")
#         .agg(
#             F.sum("qty_d").alias("items_qty"),
#             F.sum("line_amount").alias("items_gross")
#         )
#     )

#     # payments aggregation -> order level payments
#     amt_col = _first(pays, ["amount", "payment_amount"])
#     pays_agg = (
#         pays.groupBy("order_id")
#             .agg(F.sum(F.col(amt_col).cast("double")).alias("payments_total")) if amt_col else
#         pays.groupBy("order_id").agg(F.lit(0.0).alias("payments_total"))
#     )

#     # join orders + items_agg + pays_agg (use lineage-aware on right joins)
#     oi = safe_join_with_lineage(
#         left=orders, right=items_agg,
#         left_table="orders", right_table="order_items_agg",
#         on_expr=orders["order_id"] == items_agg["order_id"],
#         how="left", join_key_names=("order_id","order_id"),
#         lineage_stage="sales_transformations"
#     )
#     oip = safe_join_with_lineage(
#         left=oi, right=pays_agg,
#         left_table="orders_items", right_table="payments_agg",
#         on_expr=oi["order_id"] == pays_agg["order_id"],
#         how="left", join_key_names=("order_id","order_id"),
#         lineage_stage="sales_transformations"
#     )

#     # event_ts/date from orders
#     ts_candidates = [c for c in ["updated_at","update_time","created_at","create_time"] if c in oip.columns]
#     event_ts = F.coalesce(*[F.col(c) for c in ts_candidates]) if ts_candidates else F.lit(None)
#     out = oip.withColumn("event_ts", event_ts.cast("timestamp")).withColumn("event_date", F.to_date("event_ts"))

#     keep = [c for c in [
#         "order_id",
#         # user link (normalize quickly to user_id)
#         "user_id","users_id","status",
#         "items_qty", "items_gross", "payments_total",
#         "created_at","updated_at","create_time","update_time",
#         "event_ts","event_date"
#     ] if c in out.columns]
#     out = out.select(*keep)

#     # normalize user key to user_id
#     if "users_id" in out.columns and "user_id" not in out.columns:
#         out = out.withColumnRenamed("users_id", "user_id")

#     return write_sales_transform(out, "facts/fact_orders", partition_by=["event_date"], run_id=run_id)














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

# def _first(df, cols):
#     for c in cols:
#         if c in df.columns: return c
#     return None

# def build(spark, run_id=None):
#     orders = read_silver(spark, "orders")
#     items  = read_silver(spark, "order_items")

#     ok_i = "orders_id" if "orders_id" in items.columns else "order_id"
#     ok_o = "orders_id" if "orders_id" in orders.columns else "order_id"
#     if ok_i != "orders_id": items  = items.withColumnRenamed(ok_i, "orders_id")
#     if ok_o != "orders_id": orders = orders.withColumnRenamed(ok_o, "orders_id")

#     items_amt = (
#         items.withColumn("line_amount",
#                 F.col(_first(items, ["quantity"])).cast("double") *
#                 F.col(_first(items, ["product_price"])).cast("double"))
#              .groupBy("orders_id")
#              .agg(
#                  F.sum("line_amount").alias("items_gross"),
#                  F.sum(F.col(_first(items, ["quantity"])).cast("double")).alias("items_qty")
#              )
#     )

#     out = orders.join(items_amt, on="orders_id", how="left")

#     ts_col = _first(orders, ["updated_at","update_time","created_at","create_time"])
#     out = out.withColumn("event_ts", F.col(ts_col) if ts_col else F.lit(None)) \
#              .withColumn("event_date", F.to_date("event_ts"))

#     keep = [c for c in ["orders_id","user_id","status","items_gross","items_qty","event_date"] if c in out.columns]
#     if "users_id" in orders.columns and "user_id" not in keep:
#         out = out.withColumnRenamed("users_id","user_id")
#         keep = [c for c in ["orders_id","user_id","status","items_gross","items_qty","event_date"] if c in out.columns]

#     out = out.select(*keep)
#     return write_transform(out, "facts/fact_orders", partition_by=["event_date"], run_id=run_id)
