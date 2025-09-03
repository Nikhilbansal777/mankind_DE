# cart_item_pre_validate.py

import os, sys
# --- bootstrap first ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if project_root not in sys.path: sys.path.insert(0, project_root)
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# -----------------------
from MKM_Data_Validation_and_cleaning.validators.pre_validations.validators_common.run_prevalidate_common import run_prevalidate

if __name__ == "__main__":
    run_prevalidate(
        TABLE="cart_item",
        not_null_cols=["id","cart_id","product_id","quantity"],
        unique_cols=["id"]
    )
