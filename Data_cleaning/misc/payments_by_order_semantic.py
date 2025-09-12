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

def _pick(cols, candidates):
    return next((c for c in candidates if c in cols), None)

def build(spark):
    op = read_silver(spark, "order_payments")

    # Key normalization: prefer 'orders_id', else 'order_id'
    key = "orders_id" if "orders_id" in op.columns else ("order_id" if "order_id" in op.columns else None)
    if key is None:
        raise ValueError("order_payments has no orders_id/order_id to group by")
    op = op.withColumn("orders_id", F.col(key))  # overwrite/ensure

    # Amount & date columns (defensive)
    amount_col = _pick(op.columns, ["amount","payment_amount","paid_amount","transaction_amount","total_amount","subtotal"])
    ts_col     = _pick(op.columns, ["payment_date","updated_at","created_at"])

    aggs = [F.count(F.lit(1)).alias("payment_count")]
    if amount_col:
        aggs.append(F.sum(F.col(amount_col).cast("double")).alias("amount_paid"))
    if ts_col:
        aggs += [
            F.min(F.col(ts_col).cast("timestamp")).alias("first_payment_ts"),
            F.max(F.col(ts_col).cast("timestamp")).alias("last_payment_ts"),
        ]

    out = (op
        .groupBy("orders_id")
        .agg(*aggs)
    )

    # Event fields (typed)
    if "last_payment_ts" in out.columns:
        out = out.withColumn("event_ts", F.col("last_payment_ts").cast("timestamp"))
    else:
        out = out.withColumn("event_ts", F.lit(None).cast("timestamp"))
    out = out.withColumn("event_date", F.to_date("event_ts"))

    return write_semantic(out, "payments_by_order_semantic", partition_by=["event_date"])
