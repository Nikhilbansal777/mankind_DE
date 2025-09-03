# MKM_Data_Profiling/profilers/orders_profile.py

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

logger = get_logger("profilers.orders")

def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def profile_orders_table():
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
        logger.info("starting profiling", extra={"run_id": run_id, "table": "orders"})
        df = spark.read.jdbc(url=jdbc_url, table="orders", properties=props)
        logger.info("loaded table", extra={"table": "orders"})

        summary = sanitize_summary(run_common_profilers(df, table_name="orders"))

        out_dir = get_local_output_path("profiling_reports", "profiling")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, f"orders_profile_{run_id}.json")

        file_io.write_json(summary, out_path)
        logger.info("profiling saved", extra={"table": "orders", "path": out_path, "run_id": run_id})
    except Exception as e:
        logger.error(f"profiling failed: {e}", extra={"table": "orders", "run_id": run_id})
        raise
    finally:
        spark.stop()
        logger.info("spark session stopped", extra={"table": "orders", "run_id": run_id})

if __name__ == "__main__":
    profile_orders_table()






# import os
# import sys
# import json

# # 👇 Ensure root and src are discoverable
# project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
# sys.path.append(project_root)
# sys.path.append(os.path.join(project_root, "src"))
# sys.path.append(os.path.join(project_root, "MKM_Data_Profiling"))

# # ✅ Import root-level and src-level modules
# from project_bootstrap import bootstrap_project_paths
# from src.connections.db_connections import spark_session_for_JDBC
# from utils.path_utils import get_local_output_path

# # Profilers
# from profilers.profilers_common.null_counts import get_null_counts
# from profilers.profilers_common.data_types import get_column_data_types
# from profilers.profilers_common.distinct_counts import get_distinct_counts
# from profilers.profilers_common.column_stats import get_column_stats
# from profilers.profilers_common.value_frequencies import get_value_frequencies


# # --------------------------------------------
# # 🚀 Bootstrap Spark & Load Environment
# # --------------------------------------------
# bootstrap_project_paths(__file__)

# # Debug Print to Confirm .env Is Now Loaded
# print(f"[DEBUG] DB_URL loaded? → {os.getenv('DB_URL')}")
# print(f"[DEBUG] DB_USERNAME → {os.getenv('DB_USERNAME')}")
# print(f"[DEBUG] DB_PASSWORD → {os.getenv('DB_PASSWORD')}")


# spark = spark_session_for_JDBC()

# # --------------------------------------------
# # 📥 Load Orders Table from MySQL
# # --------------------------------------------
# table_name = "orders"
# print(f"[INFO] Loading table '{table_name}' from MySQL...")

# # Attempt to load the table using JDBC
# # This will raise an error if the connection fails or the table doesn't exist
# try:
#     df_orders = spark.read.jdbc(
#         url=os.getenv("DB_URL"),
#         table=table_name,
#         properties={
#             "user": os.getenv("DB_USERNAME"),
#             "password": os.getenv("DB_PASSWORD"),
#             "driver": "com.mysql.cj.jdbc.Driver"
#         }
#     )
#     print(f"[SUCCESS] Loaded table '{table_name}' via JDBC")
# except Exception as e:
#     print(f"[ERROR] Failed to load table '{table_name}' via JDBC: {e}")
#     sys.exit(1)  # Exit early to prevent false profiling


# # --------------------------------------------
# # 🔍 Run Profilers
# # --------------------------------------------
# profiling_result = {}

# # Null counts
# profiling_result["null_counts"] = get_null_counts(df_orders)

# # Inferred data types
# profiling_result["data_types"] = get_column_data_types(df_orders)

# # Distinct counts
# profiling_result["distinct_counts"] = get_distinct_counts(df_orders)

# # Column stats (for numeric & date columns)
# profiling_result["column_stats"] = get_column_stats(df_orders)

# # Value frequencies for key categorical columns
# categorical_cols = ["order_status", "payment_type"]
# profiling_result["value_frequencies"] = {
#     col: get_value_frequencies(df_orders, col) for col in categorical_cols if col in df_orders.columns
# }

# # --------------------------------------------
# # 💾 Save Profiling Output
# # --------------------------------------------
# output_path = get_local_output_path(table_name)

# # Make sure folder exists
# os.makedirs(os.path.dirname(output_path), exist_ok=True)

# with open(output_path, "w") as f:
#     json.dump({
#         "table": table_name,
#         "timestamp": spark.sparkContext._jvm.java.time.LocalDateTime.now().toString(),
#         **profiling_result
#     }, f, indent=2)

# print(f"[SUCCESS] Profiling report saved to: {output_path}")
