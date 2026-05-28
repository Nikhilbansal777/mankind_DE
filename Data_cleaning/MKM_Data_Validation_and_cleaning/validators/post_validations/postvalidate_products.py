# post_validations/postvalidate_products.py

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
        TABLE="products",
        file_format="json",
        not_null_cols=["products_id", "name"],
        unique_cols=["products_id"],
        range_rules=[{"col": "price", "min": 0}],
        cast_rules={"price": "double"}
    )

if __name__ == "__main__":
    main()
