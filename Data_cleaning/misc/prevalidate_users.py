# Data_cleaning/MKM_Data_Validation_and_cleaning/validators/pre_validations/prevalidate_users.py

import os, sys, json
from datetime import datetime, timezone

# bootstrap
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)

from src.connections.db_connections import spark_session_for_JDBC
from src.utils.config_loader import load_env_and_get
from src.utils.path_utils import get_validation_report_path
from Data_cleaning.MKM_Data_Validation_and_cleaning.metadata.schema_validator import validate_schema
from Data_cleaning.MKM_Data_Validation_and_cleaning.validators.pre_validations.validators_common.validation_checks import (
    check_not_null, check_unique
)

TABLE = "users"

def main():
    load_env_and_get()
    spark = spark_session_for_JDBC(app_name=f"prevalidate_{TABLE}")

    try:
        jdbc_url = os.getenv("DB_URL")
        props = {"user": os.getenv("DB_USERNAME"), "password": os.getenv("DB_PASSWORD"), "driver": "com.mysql.cj.jdbc.Driver"}
        df = spark.read.jdbc(url=jdbc_url, table=TABLE, properties=props)

        # 1) schema validation
        validate_schema(df, TABLE)

        # 2) basic quality checks
        issues = {}
        issues.update(check_not_null(df, ["id", "email"]))
        issues.update(check_unique(df, ["id", "email"]))

        # 3) save report
        report = {
            "table": TABLE,
            "phase": "pre_cleaning",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "row_count": df.count(),
            "issues": issues,
        }
        out = get_validation_report_path("pre_cleaning", f"{TABLE}_pre_validation.json")
        with open(out, "w", encoding="utf-8") as f: json.dump(report, f, indent=2)
        print(f"[PRE-VALIDATION] ✅ saved: {out}")

        # Optional: if severe issues, you could non-zero exit to block pipeline.
    finally:
        spark.stop()

if __name__ == "__main__":
    main()








# # validate_users_pre.py

# import os, sys, json
# from datetime import datetime, timezone

# project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
# if project_root not in sys.path:
#     sys.path.insert(0, project_root)

# from project_bootstrap import bootstrap_project_paths
# bootstrap_project_paths(__file__)

# from src.utils.config_loader import load_env_and_get
# from src.utils.path_utils import get_validation_report_path
# from src.connections.db_connections import spark_session_for_JDBC

# def validate_users_pre():
#     load_env_and_get()
#     spark = spark_session_for_JDBC()

#     jdbc_url = f"jdbc:mysql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
#     props = {"user": os.getenv("DB_USERNAME"), "password": os.getenv("DB_PASSWORD"), "driver": "com.mysql.cj.jdbc.Driver"}

#     try:
#         df = spark.read.jdbc(url=jdbc_url, table="users", properties=props)
#         df.createOrReplaceTempView("users")

#         issues = {}
#         issues["null_counts"] = df.selectExpr(
#             "SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) as null_id",
#             "SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) as null_email"
#         ).collect()[0].asDict()

#         issues["duplicate_emails"] = df.groupBy("email").count().filter("count > 1").count()

#         output_path = get_validation_report_path("pre_cleaning", "users_validation_pre.json")
#         with open(output_path, "w") as f:
#             json.dump(issues, f, indent=2)

#         print(f"[SUCCESS] Validation report saved to: {output_path}")
#         print(json.dumps(issues, indent=2))

#     except Exception as e:
#         print(f"[ERROR] Validation failed: {e}")
#     finally:
#         spark.stop()

# if __name__ == "__main__":
#     validate_users_pre()
