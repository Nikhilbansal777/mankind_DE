# MKM_Data_Validation_and_cleaning/cleaners/clean_products.py

# --- bootstrap (keep at very top) ---
import sys
from pathlib import Path
HERE = Path(__file__).resolve()
REPO_ROOT = next(p for p in [HERE.parent] + list(HERE.parents) if (p / "project_bootstrap.py").exists())
sys.path.insert(0, str(REPO_ROOT))
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# ------------------------------------

from MKM_Data_Validation_and_cleaning.cleaners.run_cleaning_common import run_clean_table

if __name__ == "__main__":
    run_clean_table("products", file_format="json")









# import sys
# import os
# import pandas as pd
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, udf
# from pyspark.sql.types import StringType, TimestampType

# # --- TEMP: ensure src/ and project root is in sys.path ---
# project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
# if project_root not in sys.path:
#     sys.path.insert(0, project_root)

# # --- Load custom utilities and .env ---
# from src.utils.config_loader import load_env_and_get
# from src.utils.path_utils import get_local_output_path
# from MKM_Data_Validation_and_cleaning.metadata.schema_validator import validate_schema
# from src.utils.cleaning_rules import clean_string, clean_timestamp_str

# # --- Fetch DB credentials securely ---
# db_host = load_env_and_get("DB_HOST")
# db_port = load_env_and_get("DB_PORT")
# db_name = load_env_and_get("DB_NAME")
# db_user = load_env_and_get("DB_USERNAME")
# db_password = load_env_and_get("DB_PASSWORD")
# jdbc_driver = load_env_and_get("JDBC_PATH")

# print(f"[DEBUG] JDBC driver path resolved: {jdbc_driver}")

# # --- Create Spark Session ---
# spark = SparkSession.builder \
#     .appName("CleanProducts") \
#     .config("spark.jars", jdbc_driver) \
#     .getOrCreate()
# print("[INFO] Spark session created")

# # --- JDBC Config ---
# jdbc_url = f"jdbc:mysql://{db_host}:{db_port}/{db_name}"
# table_name = "products"
# properties = {
#     "user": db_user,
#     "password": db_password,
#     "driver": "com.mysql.cj.jdbc.Driver"
# }

# def clean_products():
#     try:
#         # Read from MySQL
#         df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)
#         print(f"[DEBUG] Loaded columns from `{table_name}`: {df.columns}")

#         # --- Schema Validation ---
#         print(f"[DEBUG] Running schema validation for table: {table_name}")
#         # is_valid, msg = validate_schema(df, table_name)
#         # if not is_valid:
#         #     raise ValueError(f"[SCHEMA ERROR] {msg}")
        
#         validate_schema(df, table_name)

#         print(f"[SCHEMA VALIDATION] {table_name}: PASSED")
#         # return True, "Schema valid"


#         # expected_schema_path = os.path.join(
#         #     project_root,
#         #     "MKM_Data_Validation_and_cleaning",
#         #     "metadata",
#         #     "expected_schemas",
#         #     "products_schema"  # Proper filename here
#         # )
#         # if not expected_schema_path.endswith(".yaml"):
#         #     expected_schema_path += ".yaml"
        
#         # print(f"[DEBUG] Schema file path resolved: {expected_schema_path}")
        
#         # is_valid, msg = validate_schema(df, expected_schema_path)
#         # if not is_valid:
#         #     raise ValueError(f"[SCHEMA ERROR] {msg}")
#         # print(f"[SCHEMA VALIDATION] {table_name}: PASSED")

#         # --- Peek at timestamps ---
#         pdf = df.select("created_at", "updated_at").toPandas()
#         print("[PRE-CLEAN] created_at sample:", pdf["created_at"].dropna().astype(str).unique()[:10])
#         print("[PRE-CLEAN] updated_at sample:", pdf["updated_at"].dropna().astype(str).unique()[:10])

#         # --- Register UDFs ---
#         clean_str_udf = udf(clean_string, StringType())
#         clean_ts_udf = udf(clean_timestamp_str, StringType())

#         # --- Apply cleaning ---
#         cleaned_df = df \
#             .withColumn("brand", clean_str_udf(col("brand"))) \
#             .withColumn("model", clean_str_udf(col("model"))) \
#             .withColumn("name", clean_str_udf(col("name"))) \
#             .withColumn("created_at", clean_ts_udf(col("created_at")).cast(TimestampType())) \
#             .withColumn("updated_at", clean_ts_udf(col("updated_at")).cast(TimestampType()))

#         # --- Write output locally ---
#         output_path = os.path.join(get_local_output_path("cleaned"), "products_cleaned.parquet")
#         cleaned_df.write.mode("overwrite").parquet(output_path)
#         print("[SUCCESS] Product cleaning complete.")
#         print(f"[OUTPUT] Cleaned file saved to: {output_path}")

#     except Exception as e:
#         print(f"[ERROR] Cleaning failed: {e}")

#     finally:
#         spark.stop()

# # --- Run ---
# if __name__ == "__main__":
#     clean_products()
