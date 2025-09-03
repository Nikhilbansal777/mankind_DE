# Data_cleaning/MKM_Data_Validation_and_cleaning/controller/run_pre_validations.py
import subprocess, sys

TABLES = ["users", "products", "orders", "order_items", "payment", "wishlist"]

def main():
    for t in TABLES:
        script = f"Data_cleaning/MKM_Data_Validation_and_cleaning/validators/pre_validations/table_specific/prevalidate_{t}.py"
        print(f"--> PRE {t}")
        subprocess.check_call([sys.executable, script])

if __name__ == "__main__":
    main()
