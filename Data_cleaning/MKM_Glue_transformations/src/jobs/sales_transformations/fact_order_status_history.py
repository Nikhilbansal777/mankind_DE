# MKM_Glue_tranformations/src/jobs/sales_transformations/fact_order_status_history.py

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

def build(spark, run_id=None):
    hist = read_silver(spark, "order_status_history")

    # tolerate schema variants: prefer to_status else status
    status_col = "to_status" if "to_status" in hist.columns else ("status" if "status" in hist.columns else None)
    if status_col is None:
        # no usable status present; write empty frame with schema
        out = hist.limit(0).selectExpr("cast(null as long) as order_id",
                                       "cast(null as string) as latest_status",
                                       "cast(null as timestamp) as status_changed_at")
        return write_sales_transform(out, "facts/fact_order_status_latest", run_id=run_id)

    # prefer created_at, fall back to update/create_time
    ts_cands = [c for c in ["created_at","updated_at","create_time","update_time"] if c in hist.columns]
    ts_col = F.coalesce(*[F.col(c) for c in ts_cands]) if ts_cands else F.lit(None)

    w = W.partitionBy("order_id").orderBy(F.col("created_at").desc() if "created_at" in hist.columns else ts_col.desc())
    latest = (
        hist.withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
            .select(
                "order_id",
                F.col(status_col).alias("latest_status"),
                (F.col("created_at") if "created_at" in hist.columns else ts_col).alias("status_changed_at")
            )
    )

    out = latest.withColumn("event_date", F.to_date("status_changed_at"))

    return write_sales_transform(out, "facts/fact_order_status_latest", partition_by=["event_date"], run_id=run_id)




















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
# from MKM_Glue_tranformations.src.common.io import read_silver, write_transform

# def build(spark, run_id=None):
#     hist = read_silver(spark, "order_status_history")

#     status_col = "to_status" if "to_status" in hist.columns else "status" if "status" in hist.columns else None
#     ts_col     = "created_at" if "created_at" in hist.columns else "update_time" if "update_time" in hist.columns else None
#     order_col  = "orders_id" if "orders_id" in hist.columns else "order_id"

#     if order_col != "order_id":
#         hist = hist.withColumnRenamed(order_col, "order_id")

#     if status_col is None:
#         # fallback: just keep last known status column if exists
#         status_col = "status"

#     w = W.partitionBy("order_id").orderBy(F.col(ts_col).desc() if ts_col else F.lit(1))
#     latest = (
#         hist.withColumn("rn", F.row_number().over(w))
#             .filter(F.col("rn") == 1)
#             .select(
#                 "order_id",
#                 F.col(status_col).alias("latest_status"),
#                 F.col(ts_col).alias("status_changed_at") if ts_col else F.lit(None).alias("status_changed_at")
#             )
#             .withColumn("event_date", F.to_date("status_changed_at"))
#     )

#     return write_transform(latest, "facts/fact_order_status_latest", partition_by=["event_date"], run_id=run_id)
