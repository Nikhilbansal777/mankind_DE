# Data_cleaning/MKM_Data_Validation_and_cleaning/validators/pre_validations/address_pre_validate.py

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
    # Assumptions based on your sample:
    # Columns likely include: id, user_id, address_type, street, city, state,
    # postal_code, country, created_at, updated_at, (maybe is_default)
    # Keep it conservative: only enforce fields you know exist and must be present.
    run_prevalidate(
        TABLE="address",
        not_null_cols=[
            "id",
            "user_id",
            "street",
            "city",
            "postal_code",
            "country",
            "created_at",
        ],
        unique_cols=["id"],
        # If your framework supports domain/regex checks, you can add them later:
        # domain_checks={"address_type": ["shipping", "billing"]},
        # regex_checks={"postal_code": r"^[0-9A-Za-z -]+$"},
    )

if __name__ == "__main__":
    main()
