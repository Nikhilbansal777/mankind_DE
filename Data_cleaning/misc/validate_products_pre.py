# validate_products_pre.py

import os, sys, json

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)

from src.utils.config_loader import load_env_and_get
from src.utils.path_utils import get_validation_report_path
from src.connections.db_connections import spark_session_for_JDBC

def validate_products_pre():
    load_env_and_get()
    spark = spark_session_for_JDBC()

    jdbc_url = f"jdbc:mysql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    props = {"user": os.getenv("DB_USERNAME"), "password": os.getenv("DB_PASSWORD"), "driver": "com.mysql.cj.jdbc.Driver"}

    try:
        df = spark.read.jdbc(url=jdbc_url, table="products", properties=props)
        df.createOrReplaceTempView("products")

        issues = {}
        issues["null_counts"] = df.selectExpr(
            "SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) as null_id",
            "SUM(CASE WHEN name IS NULL THEN 1 ELSE 0 END) as null_name",
            "SUM(CASE WHEN brand IS NULL THEN 1 ELSE 0 END) as null_brand"
        ).collect()[0].asDict()

        issues["duplicate_id_count"] = df.groupBy("id").count().filter("count > 1").count()

        output_path = get_validation_report_path("pre_cleaning", "products_validation_pre.json")
        with open(output_path, "w") as f:
            json.dump(issues, f, indent=2)

        print(f"[SUCCESS] Validation report saved to: {output_path}")
        print(json.dumps(issues, indent=2))

    except Exception as e:
        print(f"[ERROR] Validation failed: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    validate_products_pre()
