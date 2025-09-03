# validate_cart_item_pre.py

import os
import sys
import json

# --- Path setup ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# --- End bootstrapping ---

from src.connections.db_connections import spark_session_for_JDBC
from src.utils.config_loader import load_env_and_get
from src.utils.path_utils import get_local_output_path
from src.utils.path_utils import get_validation_report_path


def validate_cart_item_pre():
    print("[INFO] Starting pre-clean validation: cart_item")

    load_env_and_get()
    spark = spark_session_for_JDBC()

    jdbc_url = f"jdbc:mysql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    props = {
        "user": os.getenv("DB_USERNAME"),
        "password": os.getenv("DB_PASSWORD"),
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    try:
        df = spark.read.jdbc(url=jdbc_url, table="cart_item", properties=props)
        df.createOrReplaceTempView("cart_item")

        print("[INFO] Running validation checks on 'cart_item'...")

        issues = {}

        # 1. Null checks
        null_check = df.selectExpr(
            "SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) as null_id",
            "SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) as null_price",
            "SUM(CASE WHEN quantity IS NULL THEN 1 ELSE 0 END) as null_quantity",
            "SUM(CASE WHEN cart_id IS NULL THEN 1 ELSE 0 END) as null_cart_id"
        ).collect()[0].asDict()
        issues["null_counts"] = null_check

        # 2. Negative quantity or price
        bad_values = df.filter("quantity < 0 OR price < 0").count()
        issues["negative_values_count"] = bad_values

        # 3. Duplicate IDs (shouldn't exist)
        dup_ids = df.groupBy("id").count().filter("count > 1").count()
        issues["duplicate_id_count"] = dup_ids

        # 4. Unexpected types are usually handled during cleaning, not validation

        # Save report
        # output_dir = get_local_output_path("validation_reports", "pre_cleaning")
        # os.makedirs(output_dir, exist_ok=True)
        # output_path = os.path.join(output_dir, "cart_item_validation_pre.json")
        
        output_path = get_validation_report_path("pre_cleaning", "cart_item_validation_pre.json")

        with open(output_path, "w") as f:
            json.dump(issues, f, indent=2)

        print(f"[SUCCESS] Validation complete. Output → {output_path}")
        print(json.dumps(issues, indent=2))

    except Exception as e:
        print(f"[ERROR] Validation failed: {e}")

    finally:
        spark.stop()
        print("[INFO] Spark session stopped.")

# Entry point
if __name__ == "__main__":
    validate_cart_item_pre()
