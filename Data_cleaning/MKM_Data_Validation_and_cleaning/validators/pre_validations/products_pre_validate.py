import os, sys

# --- bootstrap first ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# -----------------------

# import AFTER bootstrap
from MKM_Data_Validation_and_cleaning.validators.pre_validations.validators_common.run_prevalidate_common import run_prevalidate

if __name__ == "__main__":
    run_prevalidate(TABLE="products", not_null_cols=["id","name"], unique_cols=["id"])







# import os, sys, json
# from datetime import datetime, timezone

# project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
# if project_root not in sys.path:
#     sys.path.insert(0, project_root)
# from project_bootstrap import bootstrap_project_paths
# bootstrap_project_paths(__file__)

# from src.connections.db_connections import spark_session_for_JDBC
# from src.utils.config_loader import load_env_and_get
# from src.utils.path_utils import get_validation_report_path

# from MKM_Data_Validation_and_cleaning.validators.pre_validations.validators_common.validation_checks import (
#     check_not_null, check_unique
# )
# try:
#     from MKM_Data_Validation_and_cleaning.metadata.schema_validator import validate_schema
#     HAS_SCHEMA_VALIDATOR = True
# except Exception:
#     HAS_SCHEMA_VALIDATOR = False

# TABLE = "products"

# def main():
#     load_env_and_get()
#     spark = spark_session_for_JDBC(app_name=f"prevalidate_{TABLE}")
#     try:
#         jdbc_url = os.getenv("DB_URL")
#         props = {"user": os.getenv("DB_USERNAME"), "password": os.getenv("DB_PASSWORD"), "driver": "com.mysql.cj.jdbc.Driver"}
#         df = spark.read.jdbc(url=jdbc_url, table=TABLE, properties=props)

#         if HAS_SCHEMA_VALIDATOR:
#             try:
#                 validate_schema(df, TABLE)
#             except FileNotFoundError:
#                 print(f"[SCHEMA NOTICE] YAML not found for {TABLE}; skipping.")

#         issues = {
#             "not_null": check_not_null(df, ["id", "name"]),
#             "unique":   check_unique(df,   ["id"]),
#         }

#         report = {
#             "table": TABLE,
#             "phase": "pre_cleaning_validation_reports",
#             "timestamp": datetime.now(timezone.utc).isoformat(),
#             "row_count": df.count(),
#             "issues": issues,
#         }
#         out = get_validation_report_path("pre_cleaning_validation_reports", f"{TABLE}_pre_validation.json")
#         with open(out, "w", encoding="utf-8") as f: json.dump(report, f, indent=2)
#         print(f"[PRE-VALIDATION] ✅ saved: {out}")
#     finally:
#         spark.stop()

# if __name__ == "__main__":
#     main()
