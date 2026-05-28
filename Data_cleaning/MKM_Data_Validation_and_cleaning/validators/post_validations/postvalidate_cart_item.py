# post_validations/postvalidate_cart_item.py

# --- bootstrap (keep at very top) ---
import sys
from pathlib import Path
HERE = Path(__file__).resolve()
REPO_ROOT = next(p for p in [HERE.parent] + list(HERE.parents) if (p / "project_bootstrap.py").exists())
sys.path.insert(0, str(REPO_ROOT))
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# ------------------------------------

from MKM_Data_Validation_and_cleaning.validators.post_validations.run_postvalidate_common import run_postvalidate

def main():
    # cart_item cleaned columns seen: cart_item_id, cart_id, product_id, quantity, price
    run_postvalidate(
        TABLE="cart_item",
        file_format="json",
        not_null_cols=["cart_item_id", "cart_id", "product_id", "quantity"],
        unique_cols=["cart_item_id"],
        # If you have a cleaned `cart` table, uncomment the FK to cart as well
        fk_rules=[
            # {"fk_col": "cart_id", "ref_table": "cart", "ref_key": "cart_id"},
            {"fk_col": "product_id", "ref_table": "products", "ref_key": "products_id"},
        ],
        range_rules=[{"col": "quantity", "min": 1}],
        cast_rules={
            "quantity": "int",
            "price": "double",   # present in your cleaned columns
        }
    )

if __name__ == "__main__":
    main()
