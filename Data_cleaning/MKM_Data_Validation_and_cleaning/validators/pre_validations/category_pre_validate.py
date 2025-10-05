# Data_cleaning/MKM_Data_Validation_and_cleaning/validators/pre_validations/category_pre_validate.py

# --- robust bootstrap: find repo root and import project_bootstrap ---
import sys
from pathlib import Path

HERE = Path(__file__).resolve()
REPO_ROOT = next(p for p in [HERE.parent] + list(HERE.parents) if (p / "project_bootstrap.py").exists())
sys.path.insert(0, str(REPO_ROOT))
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# ---------------------------------------------------------------------

from MKM_Data_Validation_and_cleaning.validators.pre_validations.validators_common.run_prevalidate_common import run_prevalidate

def main():
    # Based on your sample, columns include: id, name, description, created_at, updated_at, parent_id (nullable)
    run_prevalidate(
        TABLE="category",
        not_null_cols=[
            "id",
            "name",
            "created_at",
        ],
        unique_cols=["id"],
        # If you later confirm `name` is unique, switch to: unique_cols=["id", "name"]
        # and add a self-FK check for parent_id if your common runner supports it.
    )

if __name__ == "__main__":
    main()
