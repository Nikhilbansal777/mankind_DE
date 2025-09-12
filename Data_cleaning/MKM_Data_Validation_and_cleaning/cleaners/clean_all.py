# MKM_Data_Validation_and_cleaning/cleaners/clean_all.py


# --- bootstrap (keep at very top) ---
import sys
from pathlib import Path
HERE = Path(__file__).resolve()
REPO_ROOT = next(p for p in [HERE.parent] + list(HERE.parents) if (p / "project_bootstrap.py").exists())
sys.path.insert(0, str(REPO_ROOT))
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# ------------------------------------

import argparse
from MKM_Data_Validation_and_cleaning.cleaners.run_cleaning_common import run_clean_table

DEFAULT_TABLES = [
    "users", "products",
    "orders", "order_items", "order_payments", "order_status_history",
    "payment",
    "cart_item", "wishlist",
    # add if/when needed:
    # "price_history", "category",
]

def main():
    ap = argparse.ArgumentParser(description="Clean one or more tables")
    ap.add_argument("--tables", help="Comma-separated list (overrides default set)", default=None)
    ap.add_argument("--exclude", help="Comma-separated list to skip", default=None)
    ap.add_argument("--format", choices=["json", "parquet", "csv"], default="json")
    ap.add_argument("--stop-on-error", action="store_true", help="Abort on first table failure")
    args = ap.parse_args()

    if args.tables:
        tables = [t.strip() for t in args.tables.split(",") if t.strip()]
    else:
        tables = list(DEFAULT_TABLES)

    if args.exclude:
        skip = {t.strip() for t in args.exclude.split(",") if t.strip()}
        tables = [t for t in tables if t not in skip]

    print(f"[CLEANING] Plan -> {tables} (format={args.format})")

    summary = {}
    failures = []

    for t in tables:
        try:
            print(f"[CLEANING][{t}] ▶ starting")
            audit = run_clean_table(t, file_format=args.format)
            summary[t] = {
                "rows": audit.get("output_row_count"),
                "output_path": audit.get("output_path"),
                "audit_path": audit.get("audit_path"),
            }
            print(f"[CLEANING][{t}] ✅ {summary[t]['rows']} rows -> {summary[t]['output_path']}")
        except Exception as e:
            failures.append((t, str(e)))
            print(f"[CLEANING][{t}] ❌ {e}")
            if args.stop_on_error:
                break

    print("\n[CLEANING] Done.")
    if summary:
        print("[CLEANING] Outputs:")
        for t, info in summary.items():
            print(f"  - {t}: {info['rows']} rows -> {info['output_path']}")
    if failures:
        print("[CLEANING] Failures:")
        for t, err in failures:
            print(f"  - {t}: {err}")

if __name__ == "__main__":
    main()
