# Data_cleaning/MKM_Data_Validation_and_cleaning/controller/run_cleaning_pipeline.py

import os, sys, subprocess
CONTROLLER_DIR = os.path.abspath(os.path.dirname(__file__))
ROOT = os.path.abspath(os.path.join(CONTROLLER_DIR, ".."))
SCRIPT = os.path.join(ROOT, "cleaners", "clean_all.py")
sys.exit(subprocess.call([sys.executable, SCRIPT] + sys.argv[1:]))










# import subprocess, sys

# TABLES = ["users", "products", "orders", "order_items", "payment", "wishlist"]

# def main():
#     for t in TABLES:
#         script = f"Data_cleaning/MKM_Data_Validation_and_cleaning/cleaners/table_specific/clean_{t}.py"
#         print(f"--> CLEAN {t}")
#         subprocess.check_call([sys.executable, script])

# if __name__ == "__main__":
#     main()
