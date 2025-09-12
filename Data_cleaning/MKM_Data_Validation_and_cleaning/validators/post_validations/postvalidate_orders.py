# MKM_Data_Validation_and_cleaning/validators/post_validations/postvalidate_orders.py

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
        TABLE="orders",
        file_format="json",
        not_null_cols=["orders_id", "user_id", "status", "created_at"],
        unique_cols=["orders_id"],
        fk_rules=[{"fk_col": "user_id", "ref_table": "users", "ref_key": "users_id"}],
        enum_rules={"status": ["created", "paid", "shipped", "cancelled", "returned"]},
        cast_rules={"created_at": "timestamp"}
    )

if __name__ == "__main__":
    main()
