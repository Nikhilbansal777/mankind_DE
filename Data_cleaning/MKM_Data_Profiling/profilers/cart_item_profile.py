# MKM_Data_Profiling/profilers/cart_item_profile.py

import os
import sys
import json
from datetime import datetime, timezone

# --- Project path bootstrapping ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# --- End bootstrapping ---

# Imports
from src.utils.config_loader import load_env_and_get
from src.utils.path_utils import get_local_output_path
from src.connections.db_connections import spark_session_for_JDBC
from src.utils.log_utils import get_logger
from src.utils import file_io
from MKM_Data_Profiling.profilers.all_common_profilers import run_common_profilers, sanitize_summary

logger = get_logger("profilers.cart_item")

def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def profile_cart_item_table():
    # Step 1: Load environment
    load_env_and_get()

    # Step 2: Spark session
    spark = spark_session_for_JDBC()

    jdbc_url = load_env_and_get("DB_URL")
    props = {
        "user": load_env_and_get("DB_USERNAME"),
        "password": load_env_and_get("DB_PASSWORD"),
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    run_id = _ts()
    try:
        # Step 3: Load cart_item table
        logger.info("starting profiling", extra={"run_id": run_id, "table": "cart_item"})
        df = spark.read.jdbc(url=jdbc_url, table="cart_item", properties=props)
        logger.info("loaded table", extra={"table": "cart_item"})
        print("[INFO] Loaded 'cart_item' table")

        # Step 4: Run common profilers
        raw_summary = run_common_profilers(df, table_name="cart_item")

        # Step 5: Clean/flatten JSON
        summary = sanitize_summary(raw_summary)

        # Step 6: Output path
        output_dir = get_local_output_path("profiling_reports", "profiling")
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"cart_item_profile_{run_id}.json")
        print(f"[INFO] Saving profiling report to: {output_path}")

        file_io.write_json(summary, output_path)
        logger.info("profiling saved", extra={"table": "cart_item", "path": output_path, "run_id": run_id})

        print(f"[SUCCESS] Profiling complete: cart_item")
        print(f"[INFO] Output saved to: {output_path}")

    except Exception as e:
        logger.error(f"profiling failed: {e}", extra={"table": "cart_item", "run_id": run_id})
        print(f"[ERROR] Failed profiling 'cart_item': {e}")
        raise

    finally:
        spark.stop()
        logger.info("spark session stopped", extra={"table": "cart_item", "run_id": run_id})
        print("[INFO] Spark session stopped.")

# Entry point
if __name__ == "__main__":
    profile_cart_item_table()
