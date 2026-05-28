# MKM_Glue_tranformations/src/jobs/sales_transformations/payments_by_order_fact.py

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
from pyspark.sql import Window as W
from MKM_Glue_tranformations.src.common.io import read_silver, write_sales_transform
from MKM_Glue_tranformations.src.common.semantic_utils import write_schema_lineage

def _first(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def build(spark, run_id=None):
    payments = read_silver(spark, "payments")
    orders   = read_silver(spark, "orders")

    # lineage snapshots
    write_schema_lineage("payments", payments.columns, lineage_stage="sales_transformations")
    write_schema_lineage("orders",   orders.columns,   lineage_stage="sales_transformations")

    # ---- Normalize key on orders ----
    ord_key = _first(orders, ["orders_id", "order_id"]) or "order_id"
    if ord_key != "order_id":
        orders = orders.withColumnRenamed(ord_key, "order_id")

    # Bring only the columns we can join on
    ord_keep = ["order_id"]
    if "payment_id" in orders.columns:
        ord_keep.append("payment_id")
    if "stripe_payment_intent_id" in orders.columns:
        ord_keep.append("stripe_payment_intent_id")
    orders_link = orders.select(*ord_keep).dropDuplicates(["order_id"])

    # ---- Prepare payments join keys ----
    pay_id_col   = _first(payments, ["id"])  # payments PK
    pay_spi_col  = _first(payments, ["stripe_payment_intent_id"])

    p = payments

    # 1) payments.id == orders.payment_id
    if pay_id_col and "payment_id" in orders_link.columns:
        p = p.join(
            orders_link.select(
                F.col("order_id").alias("order_id_from_id"),
                F.col("payment_id").alias("_o_payment_id")
            ),
            on=p[pay_id_col] == F.col("_o_payment_id"),
            how="left"
        )
    else:
        p = p.withColumn("order_id_from_id", F.lit(None))

    # 2) payments.stripe_payment_intent_id == orders.payment_id  (important for your data)
    if pay_spi_col and "payment_id" in orders_link.columns:
        p = p.join(
            orders_link.select(
                F.col("order_id").alias("order_id_from_spi_to_pid"),
                F.col("payment_id").alias("_o_payment_id2")
            ),
            on=p[pay_spi_col] == F.col("_o_payment_id2"),
            how="left"
        )
    else:
        p = p.withColumn("order_id_from_spi_to_pid", F.lit(None))

    # 3) payments.stripe_payment_intent_id == orders.stripe_payment_intent_id (if both exist)
    if pay_spi_col and "stripe_payment_intent_id" in orders_link.columns:
        p = p.join(
            orders_link.select(
                F.col("order_id").alias("order_id_from_spi"),
                F.col("stripe_payment_intent_id").alias("_o_spi")
            ),
            on=p[pay_spi_col] == F.col("_o_spi"),
            how="left"
        )
    else:
        p = p.withColumn("order_id_from_spi", F.lit(None))

    # Final coalesced order_id from any join that hit
    p = (
        p.withColumn(
            "order_id",
            F.coalesce(
                F.col("order_id_from_id"),
                F.col("order_id_from_spi_to_pid"),
                F.col("order_id_from_spi")
            )
        )
        .drop("order_id_from_id", "order_id_from_spi_to_pid", "order_id_from_spi", "_o_payment_id", "_o_payment_id2", "_o_spi")
    )

    # ---- Choose timestamp / status / amount ----
    pay_ts_col  = _first(p, ["updated_at", "update_time", "created_at", "create_time"])
    status_col  = _first(p, ["status", "payment_status"])
    amount_col  = _first(p, ["amount", "payment_amount", "total"])

    if pay_ts_col:
        p = p.withColumn("_pay_ts", F.to_timestamp(F.col(pay_ts_col)))
    else:
        p = p.withColumn("_pay_ts", F.lit(None).cast("timestamp"))

    # Keep only payments that we could map to an order
    p_valid = p.filter(F.col("order_id").isNotNull())

    # If still nothing matched, write an empty but well-typed dataset (prevents checker error)
    # Build a tiny empty DF with the final schema
    if p_valid.rdd.isEmpty():
        empty = spark.createDataFrame(
            [],
            "order_id LONG, total_paid DOUBLE, payment_attempts LONG, latest_payment_status STRING, last_payment_ts TIMESTAMP, event_date DATE"
        )
        return write_sales_transform(empty, "facts/payments_by_order", partition_by=["event_date"], run_id=run_id)

    # Totals per order
    p_valid = p_valid.withColumn("amount_d", F.col(amount_col).cast("double") if amount_col else F.lit(0.0))
    totals = (
        p_valid.groupBy("order_id")
               .agg(
                   F.sum("amount_d").alias("total_paid"),
                   F.count(F.lit(1)).alias("payment_attempts")
               )
    )

    # Latest payment status per order
    if pay_ts_col:
        w = W.partitionBy("order_id").orderBy(F.col("_pay_ts").desc_nulls_last())
        latest = (
            p_valid.withColumn("rn", F.row_number().over(w))
                   .filter(F.col("rn") == 1)
                   .select(
                       "order_id",
                       (F.col(status_col) if status_col else F.lit(None)).alias("latest_payment_status"),
                       F.col("_pay_ts").alias("last_payment_ts")
                   )
        )
    else:
        latest = (
            p_valid.select(
                "order_id",
                (F.col(status_col) if status_col else F.lit(None)).alias("latest_payment_status")
            )
            .dropDuplicates(["order_id"])
            .withColumn("last_payment_ts", F.lit(None).cast("timestamp"))
        )

    out = totals.join(latest, on="order_id", how="left")

    # Partitioning column
    out = out.withColumn("event_date", F.to_date(F.col("last_payment_ts")))

    keep = [c for c in [
        "order_id", "total_paid", "payment_attempts",
        "latest_payment_status", "last_payment_ts", "event_date"
    ] if c in out.columns]
    out = out.select(*keep)

    return write_sales_transform(out, "facts/payments_by_order", partition_by=["event_date"], run_id=run_id)

if __name__ == "__main__":
    from src.connections.db_connections import spark_session_for_JDBC
    from src.utils.lineage import get_run_id
    from src.utils.config_loader import load_env_and_get
    load_env_and_get()
    spark = spark_session_for_JDBC(app_name="payments_by_order_single")
    try:
        print(build(spark, run_id=get_run_id()))
    finally:
        spark.stop()







































# # MKM_Glue_tranformations/src/jobs/sales_transformations/payments_by_order_fact.py

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
# from pyspark.sql import Window as W
# from MKM_Glue_tranformations.src.common.io import read_silver, write_sales_transform
# from MKM_Glue_tranformations.src.common.semantic_utils import write_schema_lineage

# def _first(df, candidates):
#     for c in candidates:
#         if c in df.columns:
#             return c
#     return None

# def build(spark, run_id=None):
#     payments = read_silver(spark, "payments")
#     orders   = None
#     try:
#         orders = read_silver(spark, "orders")
#     except Exception:
#         orders = None

#     # Normalize order key
#     pay_key = "orders_id" if "orders_id" in payments.columns else "order_id"
#     if pay_key != "order_id":
#         payments = payments.withColumnRenamed(pay_key, "order_id")

#     # lineage
#     write_schema_lineage("payments", payments.columns, lineage_stage="sales_transformations")
#     if orders is not None:
#         write_schema_lineage("orders", orders.columns, lineage_stage="sales_transformations")

#     # choose payment timestamp & status column
#     pay_ts_col  = _first(payments, ["updated_at", "update_time", "created_at", "create_time"])
#     status_col  = _first(payments, ["status", "payment_status"])
#     amount_col  = _first(payments, ["amount", "payment_amount", "total"])

#     # totals per order
#     totals = (
#         payments
#         .withColumn("amount_d", F.col(amount_col).cast("double") if amount_col else F.lit(0.0))
#         .groupBy("order_id")
#         .agg(
#             F.sum("amount_d").alias("total_paid"),
#             F.count(F.lit(1)).alias("payment_attempts")
#         )
#     )

#     # latest payment status per order
#     if pay_ts_col:
#         w = W.partitionBy("order_id").orderBy(F.col(pay_ts_col).desc())
#         latest = (
#             payments
#             .withColumn("rn", F.row_number().over(w))
#             .filter(F.col("rn") == 1)
#             .select(
#                 "order_id",
#                 F.col(status_col).alias("latest_payment_status") if status_col else F.lit(None).alias("latest_payment_status"),
#                 F.col(pay_ts_col).alias("last_payment_ts")
#             )
#         )
#     else:
#         latest = payments.select(
#             "order_id",
#             F.col(status_col).alias("latest_payment_status") if status_col else F.lit(None).alias("latest_payment_status"),
#             F.lit(None).alias("last_payment_ts")
#         )

#     out = totals.join(latest, on="order_id", how="left")

#     # event_date preference: last_payment_ts, else from orders.created_at
#     if "last_payment_ts" in out.columns:
#         out = out.withColumn("event_date", F.to_date("last_payment_ts"))
#     elif orders is not None:
#         ord_key = "orders_id" if "orders_id" in orders.columns else "order_id"
#         if ord_key != "order_id":
#             orders = orders.withColumnRenamed(ord_key, "order_id")
#         ord_ts = _first(orders, ["updated_at", "update_time", "created_at", "create_time"])
#         ord_dates = orders.select("order_id", (F.to_date(F.col(ord_ts)) if ord_ts else F.lit(None).cast("date")).alias("event_date"))
#         out = out.join(ord_dates, on="order_id", how="left")
#     else:
#         out = out.withColumn("event_date", F.lit(None).cast("date"))

#     keep = [c for c in ["order_id", "total_paid", "payment_attempts", "latest_payment_status", "last_payment_ts", "event_date"] if c in out.columns]
#     out = out.select(*keep)

#     return write_sales_transform(out, "facts/payments_by_order", partition_by=["event_date"], run_id=run_id)
