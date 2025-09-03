# users_profile.py

import sys
import os
import json
from datetime import datetime, timezone

# --- Temporary path injection ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# --- End path injection ---

from src.connections.db_connections import spark_session_for_JDBC
from src.utils.config_loader import load_env_and_get
from src.utils.path_utils import get_local_output_path
from src.utils.log_utils import get_logger
from src.utils import file_io
from MKM_Data_Profiling.profilers.all_common_profilers import run_common_profilers, sanitize_summary

logger = get_logger("profilers.users")

def timestamp_for_filename() -> str:
    # 20250812T194510Z — UTC, timezone-aware
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def profile_users_table():
    # Load environment variables
    load_env_and_get()

    # Start Spark session
    spark = spark_session_for_JDBC()

    # jdbc_url = f"jdbc:mysql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    jdbc_url = load_env_and_get("DB_URL")
    props = {
        "user": load_env_and_get("DB_USERNAME"),
        "password": load_env_and_get("DB_PASSWORD"),
        "driver": "com.mysql.cj.jdbc.Driver",
    }
    # props = {
    #     "user": os.getenv("DB_USERNAME"),
    #     "password": os.getenv("DB_PASSWORD"),
    #     "driver": "com.mysql.cj.jdbc.Driver"
    # }

    run_id = timestamp_for_filename()

    try:
        
        # Log the run ID for tracking
        logger.info("starting profiling", extra={"run_id": run_id, "table": "users"})
        
        # Step 1: Read `users` table
        df = spark.read.jdbc(url=jdbc_url, table="users", properties=props)
        logger.info("loaded table", extra={"table": "users"})
        print("[INFO] Loaded 'users' table")

        # Step 2: Run reusable profilers
        raw_summary = run_common_profilers(df, table_name="users")

        # Step 3: Sanitize complex datatypes for JSON compatibility
        summary = sanitize_summary(raw_summary)

        # Step 4: Determine output path
        output_dir = get_local_output_path("profiling_reports", "profiling")
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"users_profile_{run_id}.json")

        # Step 5: Save to JSON
        file_io.write_json(summary, output_path)
        logger.info("profiling saved", extra={"table": "users", "path": output_path, "run_id": run_id})
        print(f"[SUCCESS] Profiling report saved to: {output_path}")

    except Exception as e:
        logger.error(f"profiling failed: {e}", extra={"table": "users", "run_id": run_id})
        print(f"[ERROR] Profiling failed for users: {e}")
        raise

    finally:
        spark.stop()
        logger.info("spark session stopped", extra={"table": "users", "run_id": run_id})
        print("[INFO] Spark session stopped.")


if __name__ == "__main__":
    profile_users_table()
