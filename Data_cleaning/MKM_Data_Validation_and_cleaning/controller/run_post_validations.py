# Data_cleaning/MKM_Data_Validation_and_cleaning/controller/run_post_validations.py

import os, sys, subprocess
CONTROLLER_DIR = os.path.abspath(os.path.dirname(__file__))
ROOT = os.path.abspath(os.path.join(CONTROLLER_DIR, ".."))
SCRIPT = os.path.join(ROOT, "validators", "post_validations", "post_validate_all.py")
sys.exit(subprocess.call([sys.executable, SCRIPT] + sys.argv[1:]))








# # Data_cleaning/MKM_Data_Validation_and_cleaning/controller/run_post_validations.py
# import os
# import subprocess
# import sys

# # Controller dir  ->  .../MKM_Data_Validation_and_cleaning/controller
# CONTROLLER_DIR = os.path.abspath(os.path.dirname(__file__))
# CLEANING_ROOT = os.path.abspath(os.path.join(CONTROLLER_DIR, ".."))
# POST_DIR = os.path.join(CLEANING_ROOT, "validators", "post_validations")

# # Only tables that actually exist in your validators/post_validations as scripts
# SAFE_TABLES = [
#     "users",
#     "products",
#     "orders",
#     "order_items",
#     "order_status_history",
#     "payment",
#     "wishlist",
#     # "order_payments",  # dropped table -> intentionally not included
#     # "address", "category"  # add if/when scripts exist
# ]

# def _script_for(table: str) -> str:
#     # your files are named like: postvalidate_<table>.py
#     return os.path.join(POST_DIR, f"postvalidate_{table}.py")

# def main():
#     print("=== POST-VALIDATIONS ===")
#     print(f"Root: {CLEANING_ROOT}\n")

#     for t in SAFE_TABLES:
#         script = _script_for(t)
#         rel = os.path.relpath(script, CLEANING_ROOT)
#         if not os.path.isfile(script):
#             print(f"[SKIP] {t:22s} -> no script: {rel}")
#             continue

#         print(f"[RUN ] {t:22s} -> {rel}")
#         subprocess.check_call([sys.executable, script])  # stop on first failure
#         print(f"[OK  ] {t}")

#     print("\nAll requested POST validations completed.")

# if __name__ == "__main__":
#     main()












# import subprocess, sys

# TABLES = ["users", "products", "orders", "order_items", "payment", "wishlist"]

# def main():
#     for t in TABLES:
#         script = f"Data_cleaning/MKM_Data_Validation_and_cleaning/validators/post_validations/table_specific/postvalidate_{t}.py"
#         print(f"--> POST {t}")
#         subprocess.check_call([sys.executable, script])

# if __name__ == "__main__":
#     main()






























# import sys
# import os

# # Setup sys.path to access both validators and src modules
# current_path = os.path.abspath(__file__)
# validation_root = os.path.abspath(os.path.join(current_path, "..", ".."))
# sys.path.insert(0, validation_root)
# project_root = os.path.abspath(os.path.join(validation_root, ".."))
# sys.path.insert(0, project_root)

# from validators.validate_products_post import run_validation as validate_products
# from validators.validate_users_post import run_validation as validate_users
# from validators.validate_wishlist_post import run_validation as validate_wishlist

# # Run validations for each table
# def run_all_post_validations():
#     print("🧪 Running post-cleaning validations...")

#     result_summary = {}

#     print("🔍 Validating products...")
#     invalid_count = validate_products()
#     result_summary["products"] = {"errors_found": invalid_count > 0, "invalid_count": invalid_count}

#     print("🔍 Validating users...")
#     invalid_count = validate_users()
#     result_summary["users"] = {"errors_found": invalid_count > 0, "invalid_count": invalid_count}

#     print("🔍 Validating wishlist...")
#     invalid_count = validate_wishlist()
#     result_summary["wishlist"] = {"errors_found": invalid_count > 0, "invalid_count": invalid_count}

#     print("\n📋 Validation Summary:")
#     for table, result in result_summary.items():
#         print(f" - {table}: {'❌ Errors' if result['errors_found'] else '✅ Clean'} ({result['invalid_count']} invalid)")

#     return result_summary

# if __name__ == "__main__":
#     run_all_post_validations()
