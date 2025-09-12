# MKM_Data_Validation_and_cleaning/cleaners/clean_wishlist.py

# --- bootstrap ---
import sys
from pathlib import Path
HERE = Path(__file__).resolve()
REPO_ROOT = next(p for p in [HERE.parent] + list(HERE.parents) if (p / "project_bootstrap.py").exists())
sys.path.insert(0, str(REPO_ROOT))
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# -----------------

from MKM_Data_Validation_and_cleaning.cleaners.run_cleaning_common import run_clean_table

if __name__ == "__main__":
    run_clean_table("wishlist", file_format="json")
