# Data_cleaning/MKM_Data_Validation_and_cleaning/controller/run_pre_validations.py

import os
import sys
import subprocess

# Controller dir -> .../MKM_Data_Validation_and_cleaning/controller
CONTROLLER_DIR = os.path.abspath(os.path.dirname(__file__))
ROOT = os.path.abspath(os.path.join(CONTROLLER_DIR, ".."))
SCRIPT = os.path.join(ROOT, "validators", "pre_validations", "pre_validate_all.py")

if __name__ == "__main__":
    # Pass through any args, exit with the same code (good for CI)
    sys.exit(subprocess.call([sys.executable, SCRIPT] + sys.argv[1:]))













# # Data_cleaning/MKM_Data_Validation_and_cleaning/controller/run_pre_validations.py
# import os
# import subprocess
# import sys

# # Controller dir  ->  .../MKM_Data_Validation_and_cleaning/controller
# CONTROLLER_DIR = os.path.abspath(os.path.dirname(__file__))
# CLEANING_ROOT = os.path.abspath(os.path.join(CONTROLLER_DIR, ".."))
# PRE_DIR = os.path.join(CLEANING_ROOT, "validators", "pre_validations")

# # Only tables that actually exist in your validators/pre_validations as scripts
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
#     # your files are named like: <table>_pre_validate.py
#     return os.path.join(PRE_DIR, f"{table}_pre_validate.py")

# def main():
#     print("=== PRE-VALIDATIONS ===")
#     print(f"Root: {CLEANING_ROOT}\n")

#     for t in SAFE_TABLES:
#         script = _script_for(t)
#         rel = os.path.relpath(script, CLEANING_ROOT)
#         if not os.path.isfile(script):
#             print(f"[SKIP] {t:22s} -> no script: {rel}")
#             continue

#         print(f"[RUN ] {t:22s} -> {rel}")
#         # stop on first failure (same as your original check_call)
#         subprocess.check_call([sys.executable, script])
#         print(f"[OK  ] {t}")

#     print("\nAll requested PRE validations completed.")

# if __name__ == "__main__":
#     main()









# import subprocess, sys

# TABLES = ["users", "products", "orders", "order_items", "payment", "wishlist"]

# def main():
#     for t in TABLES:
#         script = f"Data_cleaning/MKM_Data_Validation_and_cleaning/validators/pre_validations/table_specific/prevalidate_{t}.py"
#         print(f"--> PRE {t}")
#         subprocess.check_call([sys.executable, script])

# if __name__ == "__main__":
#     main()
