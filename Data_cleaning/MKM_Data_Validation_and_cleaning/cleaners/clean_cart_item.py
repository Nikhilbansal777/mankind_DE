# clean_cart_item.py

import os
import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# --- Path setup ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)

from src.utils.config_loader import load_env_and_get
from src.utils.path_utils import get_project_root

# --- Bookmark config ---
BOOKMARK_FILE = os.path.join(get_project_root(), "MKM_Data_Bookmarks", "cart_item_bookmark.json")
DEFAULT_LAST_ID = 0

def load_bookmark():
    if not os.path.exists(BOOKMARK_FILE):
        return DEFAULT_LAST_ID
    with open(BOOKMARK_FILE, "r") as f:
        return json.load(f).get("last_processed_id", DEFAULT_LAST_ID)

def update_bookmark(new_id: int):
    os.makedirs(os.path.dirname(BOOKMARK_FILE), exist_ok=True)
    with open(BOOKMARK_FILE, "w") as f:
        json.dump({"last_processed_id": int(new_id)}, f, indent=2)

def build_local_spark(jdbc_path_abs: str) -> SparkSession:
    """
    Build SparkSession locally using a normal filesystem path for the MySQL JDBC driver.
    Use a PLAIN OS path (not file:///).
    """
    # Normalize to forward slashes for Windows JVM friendliness
    jdbc_path_abs = jdbc_path_abs.replace("\\", "/")

    spark = (
        SparkSession.builder
        .appName("MKM_Clean_Cart_Item")
        .config("spark.jars", jdbc_path_abs)                 # plain path
        .config("spark.driver.extraClassPath", jdbc_path_abs)
        .config("spark.executor.extraClassPath", jdbc_path_abs)
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "1")
        .config("spark.sql.sources.commitProtocolClass",
                "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
        .getOrCreate()
    )
    print("[INFO] Spark session created (local builder) with JDBC driver")
    return spark

def clean_cart_item():
    spark = None
    try:
        # Step 1: Resolve JDBC driver path (keep .env portable)
        jdbc_path = load_env_and_get("JDBC_PATH", raise_if_not_found=True)
        proj_root = get_project_root()
        jdbc_path_abs = jdbc_path if os.path.isabs(jdbc_path) else os.path.abspath(os.path.join(proj_root, jdbc_path))

        # jar is here :
        # e:\MKM\mankind_matrix_data_engineering\Data_cleaning\src\resources\jdbc_drivers\mysql-connector-j-8.0.33.jar
        # This check should pass:
        if not os.path.exists(jdbc_path_abs):
            raise FileNotFoundError(f"JDBC driver not found at: {jdbc_path_abs}")
        print(f"[DEBUG] Using JDBC driver at: {jdbc_path_abs}")

        # Step 2: Start Spark session (built locally to avoid file:/// URI issue)
        spark = build_local_spark(jdbc_path_abs)

        # Step 3: JDBC connection details from env (already loaded above)
        db_host = load_env_and_get("DB_HOST", raise_if_not_found=True)
        db_port = load_env_and_get("DB_PORT", raise_if_not_found=True)
        db_name = load_env_and_get("DB_NAME", raise_if_not_found=True)
        db_user = load_env_and_get("DB_USERNAME", raise_if_not_found=True)
        db_pass = load_env_and_get("DB_PASSWORD", raise_if_not_found=True)

        jdbc_url = f"jdbc:mysql://{db_host}:{db_port}/{db_name}"
        props = {
            "user": db_user,
            "password": db_pass,
            "driver": "com.mysql.cj.jdbc.Driver"
        }

        # Step 4: Read entire cart_item table
        df = spark.read.jdbc(url=jdbc_url, table="cart_item", properties=props)
        print("[INFO] Read 'cart_item' table from database")

        # Step 5: Apply ID-based filtering (CDC)
        last_processed_id = load_bookmark()
        df_filtered = df.filter(col("id") > last_processed_id)
        print(f"[INFO] Filtered rows where id > {last_processed_id}")

        if df_filtered.rdd.isEmpty():
            print("[INFO] No new rows to clean. Exiting early.")
            return

        # Step 6: Minimal MVP cleaning
        df_cleaned = df_filtered.dropna(subset=["price", "product_id", "quantity", "cart_id"])

        # Step 7: Save cleaned data as JSON
        output_dir = os.path.join(get_project_root(), "MKM_Data_Validation_and_cleaning", "cleaned_outputs")
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "cart_item_cleaned.json")

        df_cleaned.coalesce(1).write.mode("overwrite").json(output_path)
        print(f"[SUCCESS] Cleaned data saved to: {output_path}")

        # Step 8: Update bookmark with max ID
        new_max_id = df_cleaned.agg({"id": "max"}).collect()[0][0]
        new_max_id = int(new_max_id) if new_max_id is not None else last_processed_id
        update_bookmark(new_max_id)
        print(f"[INFO] Bookmark updated to: {new_max_id}")

    except Exception as e:
        print(f"[ERROR] Cleaning failed: {e}")

    finally:
        try:
            if spark is not None:
                spark.stop()
                print("[INFO] Spark session stopped.")
            else:
                print("[WARN] Spark session was never started.")
        except Exception as e:
            print(f"[WARN] Spark stop encountered an issue: {e}")

if __name__ == "__main__":
    clean_cart_item()












# import sys, os
# from pyspark.sql.functions import col, when, trim, lower, lit, current_date, datediff, isnan

# # --- Path Injection ---
# project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
# sys.path.insert(0, project_root)

# from src.utils.config_loader import load_env_and_get
# from src.utils.path_utils import get_local_output_path
# from src.connections.db_connections import spark_session_for_JDBC
# from MKM_Data_Validation_and_cleaning.metadata.schema_validator import validate_schema

# # --- Init ---
# load_env_and_get()
# spark = spark_session_for_JDBC()

# # --- JDBC Info ---
# jdbc_url = f"jdbc:mysql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
# props = {
#     "user": os.getenv("DB_USERNAME"),
#     "password": os.getenv("DB_PASSWORD"),
#     "driver": "com.mysql.cj.jdbc.Driver"
# }

# try:
#     # --- Read ---
#     df = spark.read.jdbc(url=jdbc_url, table="cart_item", properties=props)
#     validate_schema(df, table_name="cart_item")

#     # --- Cleaning ---
#     cleaned_df = (
#         df
#         .withColumn("product_id", trim(col("product_id")))
#         .withColumn("cart_id", trim(col("cart_id")))
#         .withColumn("status", lower(trim(col("status"))))

#         .withColumn("quantity", when(col("quantity").isNull() | isnan(col("quantity")), lit(0)).otherwise(col("quantity")))

#         .withColumn("quantity_flag", when(col("quantity") <= 0, lit("invalid"))
#                                      .when(col("quantity") > 100, lit("too_high"))
#                                      .otherwise(lit("ok")))

#         .withColumn("price", trim(col("price")).cast("float"))

#         .withColumn("cart_age_days", datediff(current_date(), col("last_updated")))
#     )

#     # --- Save ---
#     output_path = os.path.join(get_local_output_path("cleaned_outputs"), "cart_item_cleaned.json")
#     cleaned_df.coalesce(1).write.mode("overwrite").json(output_path)

#     print("[SUCCESS] Cart item cleaning completed.")
#     print(f"[INFO] Cleaned data saved at: {output_path}")

# except Exception as e:
#     print(f"[ERROR] Cart item cleaning failed: {e}")

# finally:
#     spark.stop()
