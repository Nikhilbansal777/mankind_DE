# order_status_history_pre_validate.py

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

if __name__ == "__main__":
    run_prevalidate(
        TABLE="order_status_history",
        not_null_cols=["id","order_id","status","changed_at"],
        unique_cols=["id"]
    )
