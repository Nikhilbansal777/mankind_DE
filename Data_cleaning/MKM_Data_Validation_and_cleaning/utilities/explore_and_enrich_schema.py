import os
import sys
import json
import datetime
import glob
import re
from pyspark.sql import SparkSession

# ---------- Inject project root path ----------
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)

from src.utils.config_loader import load_env_and_get
from src.connections.db_connections import spark_session_for_JDBC

def get_spark() -> SparkSession:
    return spark_session_for_JDBC(app_name="ExploreAndEnrichSchema")


def get_lowercase_tables(spark, jdbc_url, props):
    query = "(SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()) as t"
    df = spark.read.jdbc(url=jdbc_url, table=query, properties=props)
    return [r[0] for r in df.collect() if isinstance(r[0], str) and r[0].islower()]


def infer_column_types(df):
    return {f.name: str(f.dataType) for f in df.schema.fields}


def explore_table_schema(spark, jdbc_url, props, table_name):
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=props)
    types = infer_column_types(df)

    # # Detect timestamp-type columns
    # type_conversions = {
    #     col: dtype for col, dtype in types.items() if "timestamp" in dtype.lower()
    # }

    # Suggest timestamp conversions
    type_conversions = {c: "TimestampType" for c, t in types.items() if "timestamp" in t.lower()}

    # Rename 'id' to table_name + '_id' if needed
    rename_columns = {}
    if "id" in df.columns and f"{table_name}_id" not in df.columns:
        rename_columns["id"] = f"{table_name}_id"

    return {
        "rename_columns": rename_columns,
        "standardize_units": {},
        "null_replacements": {},
        "case_formatting": {},
        "type_conversions": type_conversions
    }


def get_next_version(output_dir):
    existing = glob.glob(os.path.join(output_dir, "mysql_schema_summary_v*.json"))
    versions = [int(re.search(r'_v(\d+)_', f).group(1)) for f in existing if re.search(r'_v(\d+)_', f)]
    return max(versions, default=0) + 1


def save_schema_report(data):
    # ✅ Write under Data_cleaning/MKM_Data_Validation_and_cleaning/reports
    output_dir = os.path.join("Data_cleaning", "MKM_Data_Validation_and_cleaning", "reports", "profiling_schema_summary")
    os.makedirs(output_dir, exist_ok=True)

    version = get_next_version(output_dir)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")

    versioned_path = os.path.join(output_dir, f"mysql_schema_summary_v{version}_{timestamp}.json")
    latest_path = os.path.join(output_dir, "latest_mysql_schema_summary.json")

    with open(versioned_path, "w") as f:
        json.dump(data, f, indent=2)
    with open(latest_path, "w") as f:
        json.dump(data, f, indent=2)

    return versioned_path, latest_path


def main():
    print("🔍 Starting schema exploration...")
    load_env_and_get()

    spark = get_spark()
    jdbc_url = f"jdbc:mysql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    props = {
        "user": os.getenv("DB_USERNAME"),
        "password": os.getenv("DB_PASSWORD"),
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    try:
        tables = get_lowercase_tables(spark, jdbc_url, props)
        print(f"✅ Found {len(tables)} lowercase tables: {tables}")

        enriched = {}
        for tbl in tables:
            print(f"📦 Exploring: {tbl}")
            enriched[tbl] = explore_table_schema(spark, jdbc_url, props, tbl)

        summary = {
            "version": "v1",
            "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "tables": enriched
        }

        v_path, latest_path = save_schema_report(summary)
        print(f"🗃️  Saved versioned: {os.path.abspath(v_path)}")
        print(f"📌 Saved latest: {os.path.abspath(latest_path)}")

    finally:
        spark.stop()
        print("🛑 Spark session closed.")


if __name__ == "__main__":
    main()
