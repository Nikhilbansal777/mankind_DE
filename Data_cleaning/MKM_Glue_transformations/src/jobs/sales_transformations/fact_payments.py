# MKM_Glue_tranformations/src/jobs/sales_transformations/fact_payments.py


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

def _first(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def build(spark, run_id=None):
    payments = read_silver(spark, "payments")
    orders   = read_silver(spark, "orders")

    # normalize order key on payments
    pay_order_fk = _first(payments, ["order_id", "orders_id"])
    if pay_order_fk and pay_order_fk != "order_id":
        payments = payments.withColumnRenamed(pay_order_fk, "order_id")

    # normalize order key on orders
    ord_pk = _first(orders, ["orders_id", "order_id"])
    if ord_pk and ord_pk != "order_id":
        orders = orders.withColumnRenamed(ord_pk, "order_id")

    # event_ts from payments if present, else from orders
    ts_candidates_p = [c for c in ["updated_at","update_time","created_at","create_time"] if c in payments.columns]
    ts_candidates_o = [c for c in ["updated_at","update_time","created_at","create_time"] if c in orders.columns]
    event_ts = F.coalesce(*[F.col(c) for c in ts_candidates_p]) if ts_candidates_p else None
    if event_ts is None:
        event_ts = F.coalesce(*[F.col(c) for c in ts_candidates_o]) if ts_candidates_o else F.lit(None)
    # attach event_ts via left join (if coming from orders)
    if ts_candidates_p:
        p2 = payments.withColumn("event_ts", event_ts.cast("timestamp"))
    else:
        p2 = (payments.join(orders.select("order_id", event_ts.cast("timestamp").alias("event_ts")), "order_id", "left"))

    out = (
        p2
        .withColumn("event_date", F.to_date("event_ts"))
    )

    keep = [c for c in [
        "id", "payment_id",               # whichever exists
        "order_id",
        "amount", "payment_status", "payment_method",
        "created_at", "updated_at", "create_time", "update_time",
        "event_ts", "event_date"
    ] if c in out.columns]
    out = out.select(*keep)

    return write_sales_transform(out, "facts/fact_payments", partition_by=["event_date"], run_id=run_id)
















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
#         if c in df.columns:
#             return c
#     return None

# def build(spark, run_id=None):
#     pay  = read_silver(spark, "payments")
#     ords = read_silver(spark, "orders")

#     ord_key_p  = "orders_id" if "orders_id" in pay.columns else "order_id"
#     ord_key_o  = "orders_id" if "orders_id" in ords.columns else "order_id"
#     if ord_key_p != "orders_id": pay  = pay.withColumnRenamed(ord_key_p, "orders_id")
#     if ord_key_o != "orders_id": ords = ords.withColumnRenamed(ord_key_o, "orders_id")

#     paid_amt    = "amount" if "amount" in pay.columns else None
#     refund_amt  = "refund_amount" if "refund_amount" in pay.columns else None

#     out = pay
#     if paid_amt:
#         out = out.withColumn("paid_amount", F.col(paid_amt).cast("double"))
#     else:
#         out = out.withColumn("paid_amount", F.lit(None).cast("double"))

#     if refund_amt:
#         out = out.withColumn("refunded_amount", F.col(refund_amt).cast("double"))
#     else:
#         out = out.withColumn("refunded_amount", F.lit(0.0).cast("double"))

#     ts_candidates = [c for c in ["updated_at","update_time","created_at","create_time"] if c in out.columns]
#     p_ts = F.coalesce(*[F.col(c) for c in ts_candidates]) if ts_candidates else F.lit(None)
#     out = out.withColumn("payment_ts", p_ts).withColumn("event_date", F.to_date("payment_ts"))

#     keep = [c for c in ["orders_id","paid_amount","refunded_amount","status","event_date"] if c in out.columns]
#     out = out.select(*keep)

#     # attach user_id from orders if present (useful for user-level sales)
#     if "user_id" in ords.columns or "users_id" in ords.columns:
#         user_col = "user_id" if "user_id" in ords.columns else "users_id"
#         o = ords.select("orders_id", F.col(user_col).alias("user_id"))
#         out = out.join(o, on="orders_id", how="left")

#     return write_sales_transform(out, "facts/fact_payments", partition_by=["event_date"], run_id=run_id)
