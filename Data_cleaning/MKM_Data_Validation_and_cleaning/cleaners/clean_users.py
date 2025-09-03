# MKM_Data_Validation_and_cleaning/cleaners/clean_users.py
import os, sys
from datetime import datetime, timezone

# bootstrap
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)

from src.connections.db_connections import spark_session_for_JDBC
from src.utils.config_loader import load_env_and_get
from src.utils.path_utils import cleaning_output_paths
from src.utils.cleaning_rules import load_cleaning_config, clean_dataframe_with_rules

TABLE = "users"

def main():
    load_env_and_get()
    spark = spark_session_for_JDBC(app_name=f"clean_{TABLE}")

    try:
        jdbc_url = os.getenv("DB_URL")
        props = {"user": os.getenv("DB_USERNAME"), "password": os.getenv("DB_PASSWORD"), "driver": "com.mysql.cj.jdbc.Driver"}
        df = spark.read.jdbc(url=jdbc_url, table=TABLE, properties=props)

        rules = load_cleaning_config(TABLE, config_path=os.path.join("Data_cleaning", "src", "config", "master_schema_cleaning_rules.yaml"))
        cleaned = clean_dataframe_with_rules(df, rules)

        out_path = cleaning_output_paths(TABLE, "parquet")
        cleaned.write.mode("overwrite").parquet(out_path)
        print(f"[CLEANING] ✅ {TABLE} -> {out_path}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()











# import os
# import sys
# from dotenv import load_dotenv
# from pyspark.sql import DataFrame

# # ✅ Load project root path
# project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
# if project_root not in sys.path:
#     sys.path.insert(0, project_root)

# # ✅ Load env and import project modules
# load_dotenv(os.path.join(project_root, ".env"))
# from src.connections.db_connections import spark_session_for_JDBC
# from src.utils.cleaning_rules import load_cleaning_config, clean_dataframe_with_rules

# def main():
#     print("[SUCCESS] .env loaded from:", os.path.join(project_root, ".env"))

#     spark = spark_session_for_JDBC()

#     table_name = "users"
#     print(f"🔄 Reading raw table: {table_name}")

#     jdbc_url = f"jdbc:mysql://{os.getenv('DB_URL')}"
#     properties = {
#         "user": os.getenv("DB_USERNAME"),
#         "password": os.getenv("DB_PASSWORD"),
#         "driver": "com.mysql.cj.jdbc.Driver"
#     }

#     df: DataFrame = spark.read \
#         .format("jdbc") \
#         .option("url", jdbc_url) \
#         .option("dbtable", table_name) \
#         .option("user", properties["user"]) \
#         .option("password", properties["password"]) \
#         .option("driver", properties["driver"]) \
#         .load()

#     print(f"🧹 Applying cleaning rules for: {table_name}")
#     rules = load_cleaning_config(table_name)
#     cleaned_df = clean_dataframe_with_rules(df, rules)

#     output_path = os.path.join(
#         project_root,
#         "MKM_Data_Validation_and_cleaning",
#         "cleaned_outputs",
#         "users_cleaned_json"
#     ).replace("\\", "/")

#     print(f"💾 Saving cleaned JSON data to: {output_path}")
#     # cleaned_df.write.mode("overwrite").json(output_path)
#     cleaned_df.coalesce(1).write.mode("overwrite").format("json").save(output_path)
#     print(f"[SUCCESS] User cleaning completed. Cleaned data saved at: {output_path}")
#     spark.stop()
       

# if __name__ == "__main__":
#     main()






# --- Additional Context ---
# This script is designed to clean the 'users' table from a MySQL database using Spark










###-------------------- for AWS Glue or Linux ones --------------------#
# import os
# import sys
# import json
# from pyspark.sql import SparkSession
# from dotenv import load_dotenv
# import yaml

# # --- Inject project root ---
# project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
# if project_root not in sys.path:
#     sys.path.insert(0, project_root)

# # --- Imports from project ---
# from src.utils.config_loader import load_env_and_get
# from src.utils.path_utils import get_local_output_path
# from src.utils.cleaning_rules import clean_dataframe_with_rules

# # --- Constants ---
# RAW_TABLE = "users"
# OUTPUT_FILE_NAME = "users_cleaned_json"
# RULES_PATH = os.path.join("src", "config", "master_schema_cleaning_rules.yaml")

# def main():
#     print(f"🔄 Reading raw table: {RAW_TABLE}")

#     # Load .env
#     load_dotenv()
#     db_url = os.getenv("DB_URL")
#     db_user = os.getenv("DB_USERNAME")
#     db_password = os.getenv("DB_PASSWORD")
#     jdbc_jar_path = os.path.abspath(os.getenv("JDBC_PATH"))  # Absolute path is critical

#     # Build SparkSession with JDBC driver
#     spark = (
#         SparkSession.builder
#         .appName("CleanUsersTable")
#         .config("spark.jars", jdbc_jar_path)
#         .config("spark.driver.extraClassPath", jdbc_jar_path)
#         .config("spark.executor.extraClassPath", jdbc_jar_path)
#         .getOrCreate()
#     )

#     # JDBC connection
#     props = {"user": db_user, "password": db_password}
#     jdbc_url = f"jdbc:mysql://{db_url}"

#     # Read table
#     df = spark.read.jdbc(url=jdbc_url, table=RAW_TABLE, properties=props)

#     # Load cleaning rules
#     with open(RULES_PATH, "r") as f:
#         rules = yaml.safe_load(f)
#     table_rules = rules.get("tables", {}).get(RAW_TABLE, {})

#     print(f"🧹 Applying cleaning rules for: {RAW_TABLE}")
#     cleaned_df = clean_dataframe_with_rules(df, table_rules)

#     # Output path
#     output_path = get_local_output_path(OUTPUT_FILE_NAME)
#     os.makedirs(output_path, exist_ok=True)
#     print(f"💾 Saving cleaned JSON data to: {output_path}")

#     # Updated writing logic to fix Windows NativeIO error
#     cleaned_df.coalesce(1) \
#         .write \
#         .mode("overwrite") \
#         .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
#         .format("json") \
#         .save(output_path)

#     spark.stop()

# if __name__ == "__main__":
#     main()



























# import os
# import sys
# import yaml
# from pyspark.sql import SparkSession

# # --- Path injection to ensure src is recognized ---
# project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
# if project_root not in sys.path:
#     sys.path.insert(0, project_root)

# # --- Local imports ---
# from src.utils.config_loader import load_env_and_get
# from src.utils.cleaning_rules import clean_dataframe_with_rules
# from src.utils.path_utils import get_local_output_path, get_project_root

# # Step 1: Load environment variables
# load_env_and_get()

# # Step 2: Setup Spark session
# spark = SparkSession.builder \
#     .appName("CleanUsersTable") \
#     .config("spark.jars", os.getenv("JDBC_PATH")) \
#     .getOrCreate()

# # Step 3: JDBC connection details
# jdbc_url = f"jdbc:mysql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
# props = {
#     "user": os.getenv("DB_USERNAME"),
#     "password": os.getenv("DB_PASSWORD"),
#     "driver": "com.mysql.cj.jdbc.Driver"
# }

# # Step 4: Read raw users data
# table_name = "users"
# print(f"🔄 Reading raw table: {table_name}")
# raw_df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=props)

# # Step 5: Load YAML cleaning rules (🔧 FIXED ABSOLUTE PATH)
# rules_path = os.path.join(get_project_root(), "src", "config", "master_schema_cleaning_rules.yaml")
# print(f"[DEBUG] Cleaning rules path: {rules_path}")
# with open(rules_path, "r") as f:
#     all_rules = yaml.safe_load(f)
# table_rules = all_rules.get(table_name, {})

# # Step 6: Clean the DataFrame
# print(f"🧹 Applying cleaning rules for: {table_name}")
# cleaned_df = clean_dataframe_with_rules(raw_df, table_rules)

# # Step 7: Save cleaned output
# output_path = os.path.join(get_local_output_path("cleaned_outputs"), f"{table_name}_cleaned.parquet")
# print(f"💾 Saving cleaned data to: {output_path}")
# cleaned_df.write.mode("overwrite").parquet(output_path)

# print(f"✅ Cleaning complete for table: {table_name}")
# spark.stop()







# # import sys, os
# # from pyspark.sql.functions import col, trim, rlike
# # from pyspark.sql import SparkSession

# # # --- Path Setup ---
# # project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
# # sys.path.insert(0, project_root)

# # from src.utils.config_loader import load_env_and_get
# # from src.utils.path_utils import get_local_output_path
# # from src.connections.db_connections import spark_session_for_JDBC
# # from MKM_Data_Validation_and_cleaning.metadata.schema_validator import validate_schema

# # # --- Init ---
# # load_env_and_get()
# # spark = spark_session_for_JDBC()

# # # --- JDBC Info ---
# # jdbc_url = f"jdbc:mysql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
# # props = {
# #     "user": os.getenv("DB_USERNAME"),
# #     "password": os.getenv("DB_PASSWORD"),
# #     "driver": "com.mysql.cj.jdbc.Driver"
# # }

# # try:
# #     # --- Read ---
# #     df = spark.read.jdbc(url=jdbc_url, table="users", properties=props)
# #     validate_schema(df, table_name="users")

# #     # --- Cleaning ---
# #     cleaned_df = (
# #         df
# #         .filter(col("user_id").isNotNull())
# #         .filter(col("email").rlike(r'^[\w\.-]+@[\w\.-]+\.\w+$'))
# #         .withColumn("email", trim(col("email")))
# #         .withColumn("name", trim(col("name")))
# #         .dropDuplicates(["user_id"])
# #     )

# #     # --- Save ---
# #     output_path = os.path.join(get_local_output_path("cleaned_outputs"), "users_cleaned.json")
# #     cleaned_df.coalesce(1).write.mode("overwrite").json(output_path)

# #     print("[SUCCESS] User cleaning completed.")
# #     print(f"[INFO] Cleaned data saved at: {output_path}")

# # except Exception as e:
# #     print(f"[ERROR] User cleaning failed: {e}")

# # finally:
# #     spark.stop()
