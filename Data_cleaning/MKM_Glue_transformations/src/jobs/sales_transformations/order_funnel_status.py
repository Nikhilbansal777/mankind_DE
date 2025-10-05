# MKM_Glue_tranformations/src/jobs/sales_transformations/order_funnel_status.py

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
    # Prefer history if present; fall back to orders.status
    hist = None
    try:
        hist = read_silver(spark, "order_status_history")
    except Exception:
        pass

    orders = read_silver(spark, "orders")

    # Normalize order key on orders
    ord_key_o = "orders_id" if "orders_id" in orders.columns else "order_id"
    if ord_key_o != "orders_id":
        orders = orders.withColumnRenamed(ord_key_o, "orders_id")

    # Record schema snapshots in sales lineage area
    write_schema_lineage("orders", orders.columns, lineage_stage="sales_transformations")
    if hist is not None:
        # Normalize order key on history
        ord_key_h = "orders_id" if "orders_id" in hist.columns else "order_id"
        if ord_key_h != "order_id":
            hist = hist.withColumnRenamed(ord_key_h, "order_id")
        write_schema_lineage("order_status_history", hist.columns, lineage_stage="sales_transformations")

    if hist is not None:
        # Choose status column defensively: some schemas used "to_status", yours has "status"
        status_col = "to_status" if "to_status" in hist.columns else "status"
        ts_col     = _first(hist, ["created_at", "create_time", "updated_at", "update_time"])

        w = W.partitionBy("order_id").orderBy(F.col(ts_col).desc() if ts_col else F.lit(0))
        latest = (
            hist.withColumn("rn", F.row_number().over(w))
                .filter(F.col("rn") == 1)
                .select(
                    F.col("order_id").alias("orders_id"),
                    F.col(status_col).alias("latest_status"),
                    F.col(ts_col).alias("status_changed_at") if ts_col else F.lit(None).alias("status_changed_at")
                )
        )
    else:
        # No history: take orders.status as “latest”
        status_col = _first(orders, ["status"])
        ts_col     = _first(orders, ["updated_at", "update_time", "created_at", "create_time"])
        latest = orders.select(
            "orders_id",
            F.col(status_col).alias("latest_status") if status_col else F.lit(None).alias("latest_status"),
            F.col(ts_col).alias("status_changed_at") if ts_col else F.lit(None).alias("status_changed_at")
        )

    out = latest.withColumn("event_date", F.to_date(F.col("status_changed_at")))
    keep = [c for c in ["orders_id", "latest_status", "status_changed_at", "event_date"] if c in out.columns]
    out = out.select(*keep)

    return write_sales_transform(out, "facts/order_funnel_status", partition_by=["event_date"], run_id=run_id)
