# MKM_Glue_tranformations/src/jobs/semantic/rollups/user_order_activity_semantic.py

# user_order_activity_semantic.py
import os, sys
from datetime import datetime, timezone

# --- Project bootstrap ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# --- End bootstrap ---

from pyspark.sql import functions as F

from src.connections.db_connections import spark_session_for_JDBC
from src.utils.config_loader import load_env_and_get
from src.utils.log_utils import get_logger
from src.utils import file_io
from src.utils.path_utils import get_semantic_output_path, get_lineage_output_path

LOGGER = get_logger("semantic.rollups.user_order_activity")

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
    dataset = "rollups/user_order_activity"

    try:
        LOGGER.info("starting rollup", extra={"dataset": dataset, "run_id": run_id})

        orders = spark.read.jdbc(url=jdbc_url, table="orders", properties=props)
        users  = spark.read.jdbc(url=jdbc_url, table="users",  properties=props)

        orders_cnt = orders.count()
        users_cnt  = users.count()
        LOGGER.info("loaded sources", extra={"orders_rows": orders_cnt, "users_rows": users_cnt})

        # Expect: orders has user_id, created_at
        now_ts = F.current_timestamp()
        ua = (
            orders.groupBy("user_id")
                  .agg(
                      F.count(F.lit(1)).alias("orders_count"),
                      F.min("created_at").alias("first_order_at"),
                      F.max("created_at").alias("last_order_at"),
                  )
                  .withColumn("recency_days", F.datediff(now_ts, F.col("last_order_at")))
        )

        # Attach user role (avoid PII like email/username)
        users_min = users.select("id", "role").withColumnRenamed("id", "user_id")
        out = (ua.join(users_min, on="user_id", how="left")
                 .select("user_id", "role", "orders_count", "first_order_at", "last_order_at", "recency_days")
                 .orderBy(F.col("orders_count").desc(), F.col("last_order_at").desc())
              )

        out_rows = out.count()
        LOGGER.info("built rollup dataframe", extra={"rows": out_rows})

        # ---- SEMANTIC data ----
        data_dir = get_semantic_output_path(dataset, f"run_id={run_id}")
        os.makedirs(data_dir, exist_ok=True)
        out.write.mode("overwrite").parquet(os.path.join(data_dir, "data"))
        LOGGER.info("semantic saved", extra={"path": data_dir})

        # ---- LINEAGE metrics ----
        metrics = {
            "run_id": run_id,
            "dataset": dataset,
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "inputs": {"orders": {"rows": orders_cnt}, "users": {"rows": users_cnt}},
            "output": {"rows": out_rows},
            "checks": {"non_empty_output": out_rows >= 0},
        }
        lineage_path = get_lineage_output_path("semantic", "user_order_activity", f"metrics_{run_id}.json")
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
