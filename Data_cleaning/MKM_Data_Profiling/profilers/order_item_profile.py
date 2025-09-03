# MKM_Data_Profiling/profilers/order_items_profile.py

import sys
import os
from datetime import datetime, timezone

# --- Project path bootstrapping ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# --- End bootstrapping ---

from src.connections.db_connections import spark_session_for_JDBC
from src.utils.config_loader import load_env_and_get
from src.utils.path_utils import get_local_output_path
from src.utils.log_utils import get_logger
from src.utils import file_io
from MKM_Data_Profiling.profilers.all_common_profilers import run_common_profilers, sanitize_summary

logger = get_logger("profilers.order_items")

def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def profile_order_items_table():
    load_env_and_get()
    spark = spark_session_for_JDBC()

    jdbc_url = load_env_and_get("DB_URL")
    props = {
        "user": load_env_and_get("DB_USERNAME"),
        "password": load_env_and_get("DB_PASSWORD"),
        "driver": "com.mysql.cj.jdbc.Driver",
    }

    run_id = _ts()
    try:
        logger.info("starting profiling", extra={"run_id": run_id, "table": "order_items"})
        df = spark.read.jdbc(url=jdbc_url, table="order_items", properties=props)
        logger.info("loaded table", extra={"table": "order_items"})

        summary = sanitize_summary(run_common_profilers(df, table_name="order_items"))

        out_dir = get_local_output_path("profiling_reports", "profiling")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, f"order_items_profile_{run_id}.json")

        file_io.write_json(summary, out_path)
        logger.info("profiling saved", extra={"table": "order_items", "path": out_path, "run_id": run_id})
    except Exception as e:
        logger.error(f"profiling failed: {e}", extra={"table": "order_items", "run_id": run_id})
        raise
    finally:
        spark.stop()
        logger.info("spark session stopped", extra={"table": "order_items", "run_id": run_id})

if __name__ == "__main__":
    profile_order_items_table()

