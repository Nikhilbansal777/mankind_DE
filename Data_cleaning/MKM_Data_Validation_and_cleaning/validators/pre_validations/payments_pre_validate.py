# payment_pre_validate.py

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
    # Based on your sample rows, assume a conventional schema:
    # id, amount, created_at, currency, metadata(json), provider, status,
    # provider_payment_id (Stripe pi_*), updated_at, user_id (or order_id)
    # Keep constraints conservative so it won't false-fail.
    run_prevalidate(
        TABLE="payments",
        not_null_cols=[
            "id",
            "amount",
            "status",
            "created_at",
        ],
        unique_cols=["id"],
        # Add stricter checks later if you like:
        # domain_checks={"status": ["SUCCEEDED", "REQUIRES_PAYMENT_METHOD", "FAILED", "REFUNDED"]},
        # regex_checks={"provider_payment_id": r"^pi_[A-Za-z0-9]+$"},
    )

if __name__ == "__main__":
    main()











# if __name__ == "__main__":
#     run_prevalidate(
#         TABLE="payments",
#         not_null_cols=["id","user_id","amount","status"],
#         unique_cols=["id"]
#     )
