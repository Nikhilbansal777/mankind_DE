# Data_cleaning/MKM_Data_Validation_and_cleaning/validators/pre_validations/pre_validate_all.py

# --- bootstrap (keep at top) ---
import sys, argparse, importlib, importlib.util, runpy, os
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve()
REPO_ROOT = next(p for p in [HERE.parent] + list(HERE.parents) if (p / "project_bootstrap.py").exists())
sys.path.insert(0, str(REPO_ROOT))
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# --------------------------------

os.environ.setdefault("RUN_ID", datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))

# Match your actual script names: <table>_pre_validate.py
DEFAULT_TABLES = [
    "users", "products",
    "orders", "order_items", "order_status_history",
    "payments",           # plural (your data exists here)
    "address", "category",
    "cart_item", "wishlist",
]

def _module_name(table: str) -> str:
    # pre validators live as modules like:
    # MKM_Data_Validation_and_cleaning.validators.pre_validations.<table>_pre_validate
    return f"MKM_Data_Validation_and_cleaning.validators.pre_validations.{table}_pre_validate"

def _module_file(mod_name: str) -> str | None:
    """Return filesystem path for a module, if discoverable."""
    spec = importlib.util.find_spec(mod_name)
    return spec.origin if spec and spec.origin else None

def main():
    ap = argparse.ArgumentParser(description="Run pre-clean validations for multiple tables (in-process)")
    ap.add_argument("--tables", help="Comma-separated list (default: full set)", default=None)
    ap.add_argument("--exclude", help="Comma-separated list to skip", default=None)
    ap.add_argument("--stop-on-error", action="store_true")
    args = ap.parse_args()

    tables = [t.strip() for t in args.tables.split(",")] if args.tables else list(DEFAULT_TABLES)
    if args.exclude:
        skip = {t.strip() for t in args.exclude.split(",")}
        tables = [t for t in tables if t not in skip]

    print(f"[PRE-VALIDATION][ALL] Plan -> {tables}")
    failures, skipped = [], []

    for t in tables:
        mod_name = _module_name(t)
        try:
            mod = importlib.import_module(mod_name)
        except ModuleNotFoundError:
            print(f"[PRE-VALIDATION][{t}] SKIP (no module): {mod_name}")
            skipped.append(t)
            continue

        try:
            if hasattr(mod, "main") and callable(getattr(mod, "main")):
                print(f"[PRE-VALIDATION][{t}] ▶ running via main() (in-process)")
                mod.main()
            else:
                path = _module_file(mod_name)
                if not path or not os.path.isfile(path):
                    raise RuntimeError(f"Cannot locate file for {mod_name}")
                print(f"[PRE-VALIDATION][{t}] ▶ runpy.run_path: {path} (in-process)")
                # Execute the script as if it were __main__ (no new child process)
                runpy.run_path(path, run_name="__main__")
            print(f"[PRE-VALIDATION][{t}] ✅ done")
        except Exception as e:
            print(f"[PRE-VALIDATION][{t}] ❌ {e}")
            failures.append((t, str(e)))
            if args.stop_on_error:
                break

    if failures:
        print("[PRE-VALIDATION][ALL] Failures:")
        for t, err in failures:
            print(f"  - {t}: {err}")
        sys.exit(1)

    if skipped:
        print("[PRE-VALIDATION][ALL] Skipped:")
        for t in skipped:
            print(f"  - {t}")

if __name__ == "__main__":
    main()
