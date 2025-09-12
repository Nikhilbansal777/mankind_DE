# order_status_by_order_semantic.py

# --- bootstrap ---
import sys
from pathlib import Path
HERE = Path(__file__).resolve()
REPO_ROOT = next(p for p in [HERE.parent] + list(HERE.parents) if (p / "project_bootstrap.py").exists())
sys.path.insert(0, str(REPO_ROOT))
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# ------------------

import os, sys
from datetime import datetime, timezone



from pyspark.sql import functions as F
from pyspark.sql import Window as W

from src.connections.db_connections import spark_session_for_JDBC
from src.utils.config_loader import load_env_and_get
from src.utils.log_utils import get_logger
from src.utils import file_io
from src.utils.path_utils import get_semantic_output_path, get_lineage_output_path

LOGGER = get_logger("semantic.rollups.order_status_by_order")

def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def main():
    load_env_and_get()
    spark = spark_session_for_JDBC()

    jdbc_url = (load_env_and_get("DB_URL") or "").strip()
    props = {
        "user": (load_env_and_get("DB_USERNAME") or "").strip(),
        "password": (load_env_and_get("DB_PASSWORD") or "").strip(),
        "driver": "com.mysql.cj.jdbc.Driver",
    }

    run_id = _ts()
    started_at = datetime.now(timezone.utc)
    dataset = "rollups/order_status_by_order"

    try:
        LOGGER.info("starting rollup", extra={"dataset": dataset, "run_id": run_id})

        hist = spark.read.jdbc(url=jdbc_url, table="order_status_history", properties=props)

        # Expect these columns to exist: order_id, created_at, to_status
        # Window to pick latest transition per order
        w = W.partitionBy("order_id").orderBy(F.col("created_at").desc())
        latest = (
            hist.withColumn("rn", F.row_number().over(w))
                .filter(F.col("rn") == 1)
                .select(
                    "order_id",
                    F.col("to_status").alias("latest_status"),
                    F.col("created_at").alias("status_changed_at"),
                )
        )

        out_rows = latest.count()
        LOGGER.info("built rollup dataframe", extra={"rows": out_rows})

        # ---- Write SEMANTIC data ----
        data_dir = get_semantic_output_path(dataset, f"run_id={run_id}")
        os.makedirs(data_dir, exist_ok=True)
        latest.write.mode("overwrite").parquet(os.path.join(data_dir, "data"))
        LOGGER.info("semantic saved", extra={"path": data_dir})

        # ---- Write LINEAGE metrics ----
        finished_at = datetime.now(timezone.utc)
        metrics = {
            "run_id": run_id,
            "dataset": dataset,
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "inputs": {
                "order_status_history": {"rows": hist.count()}  # cheap again here; ok for small data
            },
            "output": {"rows": out_rows},
            "checks": {
                "non_empty_output": out_rows > 0,
                "status_not_null": latest.filter(F.col("latest_status").isNull()).count() == 0,
            },
        }
        lineage_path = get_lineage_output_path("semantic", "order_status_by_order", f"metrics_{run_id}.json")
        os.makedirs(os.path.dirname(lineage_path), exist_ok=True)
        file_io.write_json(metrics, lineage_path)
        LOGGER.info("lineage saved", extra={"path": lineage_path})

        LOGGER.info("rollup completed", extra={"dataset": dataset, "run_id": run_id})
    except Exception as e:
        LOGGER.error(f"rollup failed: {e}", extra={"dataset": dataset, "run_id": run_id})
        raise
    finally:
        spark.stop()
        LOGGER.info("spark session stopped", extra={"dataset": dataset, "run_id": run_id})

if __name__ == "__main__":
    main()



















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
# from MKM_Glue_tranformations.src.common.io import read_silver, write_semantic

# def _pick(cols, candidates):
#     return next((c for c in candidates if c in cols), None)

# def build(spark):
#     osh = read_silver(spark, "order_status_history")

#     # Key normalization
#     key = "orders_id" if "orders_id" in osh.columns else ("order_id" if "order_id" in osh.columns else None)
#     if key is None:
#         raise ValueError("order_status_history has no orders_id/order_id to group by")
#     osh = osh.withColumn("orders_id", F.col(key))

#     status_col = _pick(osh.columns, ["status","state","order_status"])
#     ts_col     = _pick(osh.columns, ["created_at","updated_at","change_time","status_time"])

#     aggs = [F.count(F.lit(1)).alias("status_event_count")]

#     if status_col and ts_col:
#         # Use struct min/max to get first/last (lexicographic on ts then status)
#         aggs += [
#             F.min(F.struct(F.col(ts_col).cast("timestamp"), F.col(status_col))).alias("first_rec"),
#             F.max(F.struct(F.col(ts_col).cast("timestamp"), F.col(status_col))).alias("last_rec"),
#         ]

#     out = osh.groupBy("orders_id").agg(*aggs)

#     if "first_rec" in out.columns:
#         out = (out
#             .withColumn("first_status_ts", F.col("first_rec").getItem(0))
#             .withColumn("first_status",    F.col("first_rec").getItem(1).cast("string"))
#             .drop("first_rec")
#         )
#     else:
#         out = out.withColumn("first_status_ts", F.lit(None).cast("timestamp")) \
#                  .withColumn("first_status",    F.lit(None).cast("string"))

#     if "last_rec" in out.columns:
#         out = (out
#             .withColumn("last_status_ts", F.col("last_rec").getItem(0))
#             .withColumn("last_status",    F.col("last_rec").getItem(1).cast("string"))
#             .drop("last_rec")
#         )
#     else:
#         out = out.withColumn("last_status_ts", F.lit(None).cast("timestamp")) \
#                  .withColumn("last_status",    F.lit(None).cast("string"))

#     out = out.withColumn("event_ts", F.col("last_status_ts").cast("timestamp"))
#     out = out.withColumn("event_date", F.to_date("event_ts"))

#     return write_semantic(out, "order_status_by_order_semantic", partition_by=["event_date"])
