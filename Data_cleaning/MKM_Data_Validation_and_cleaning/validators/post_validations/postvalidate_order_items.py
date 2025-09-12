# post_validations/postvalidate_order_items.py

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
    # Cleaned columns observed:
    # [created_at, order_id, order_items_id, product_id, product_name, product_price, quantity, subtotal]
    run_postvalidate(
        TABLE="order_items",
        file_format="json",
        not_null_cols=["order_items_id", "order_id", "product_id", "quantity"],
        unique_cols=["order_items_id"],
        fk_rules=[
            {"fk_col": "order_id",   "ref_table": "orders",   "ref_key": "orders_id"},    # orders table uses orders_id
            {"fk_col": "product_id", "ref_table": "products", "ref_key": "products_id"},
        ],
        range_rules=[
            {"col": "quantity",      "min": 1},
            {"col": "product_price", "min": 0},
            {"col": "subtotal",      "min": 0},
        ],
        cast_rules={
            "quantity":      "int",
            "product_price": "double",
            "subtotal":      "double",
            "created_at":    "timestamp",
        },
    )

if __name__ == "__main__":
    main()
