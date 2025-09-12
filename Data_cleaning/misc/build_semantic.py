# --- bootstrap ---
import sys
from pathlib import Path
HERE = Path(__file__).resolve()
REPO_ROOT = next(p for p in [HERE.parent] + list(HERE.parents) if (p / "project_bootstrap.py").exists())
sys.path.insert(0, str(REPO_ROOT))
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# -----------------

import argparse
from .enrichments.enrich_products import run as run_products
from .enrichments.enrich_cart_items import run as run_cart
from .joins.join_users_products_wishlist import run as run_wishlist_join

def main():
    ap = argparse.ArgumentParser(description="Build Semantic (Conformed) datasets from Silver")
    ap.add_argument("--format", choices=["parquet","json","csv"], default="parquet")
    args = ap.parse_args()

    print("[SEMANTIC] product_enriched");                  run_products(args.format)
    print("[SEMANTIC] cart_item_enriched");               run_cart(args.format)
    print("[SEMANTIC] wishlist_user_product_enriched");   run_wishlist_join(args.format)

    print("[SEMANTIC] build complete")

if __name__ == "__main__":
    main()
