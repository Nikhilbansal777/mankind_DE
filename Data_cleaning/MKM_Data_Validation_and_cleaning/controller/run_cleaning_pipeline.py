# Data_cleaning/MKM_Data_Validation_and_cleaning/controller/run_cleaning_pipeline.py
import subprocess, sys

TABLES = ["users", "products", "orders", "order_items", "payment", "wishlist"]

def main():
    for t in TABLES:
        script = f"Data_cleaning/MKM_Data_Validation_and_cleaning/cleaners/table_specific/clean_{t}.py"
        print(f"--> CLEAN {t}")
        subprocess.check_call([sys.executable, script])

if __name__ == "__main__":
    main()
