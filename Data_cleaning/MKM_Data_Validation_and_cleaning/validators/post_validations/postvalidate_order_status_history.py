# post_validations/postvalidate_order_status_history.py

# --- bootstrap ---
import sys
from pathlib import Path
HERE = Path(__file__).resolve()
REPO_ROOT = next(p for p in [HERE.parent] + list(HERE.parents) if (p / "project_bootstrap.py").exists())
sys.path.insert(0, str(REPO_ROOT))
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# -----------------

from MKM_Data_Validation_and_cleaning.validators.post_validations.run_postvalidate_common import run_postvalidate

def main():
    run_postvalidate(
        TABLE="order_status_history",
        file_format="json",
        not_null_cols=["order_status_history_id", "orders_id", "status", "changed_at"],
        unique_cols=["order_status_history_id"],
        fk_rules=[{"fk_col": "orders_id", "ref_table": "orders", "ref_key": "orders_id"}],
        enum_rules={"status": ["created", "paid", "shipped", "cancelled", "returned"]},
        cast_rules={"changed_at": "timestamp"}
    )

if __name__ == "__main__":
    main()


# --- IGNORE ---
# This file is for post-validation of the order status history table.
# It checks for not null constraints, unique constraints, foreign key relationships,    
# enumerated values for status, and correct data types for the changed_at column.
# It is part of the MKM Data Validation and Cleaning project.
# It is executed as a standalone script to perform the validations.
# It is expected to be run after the pre-validation checks have been completed.
# It uses the run_postvalidate function from the common module to perform the validations.
# It is designed to be run in a Python environment with the necessary dependencies installed.
