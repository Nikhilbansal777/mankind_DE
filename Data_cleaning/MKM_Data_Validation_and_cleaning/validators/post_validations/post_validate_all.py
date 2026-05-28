# --- bootstrap (keep at top) ---
import sys, argparse, importlib
from pathlib import Path
HERE = Path(__file__).resolve()
REPO_ROOT = next(p for p in [HERE.parent] + list(HERE.parents) if (p / "project_bootstrap.py").exists())
sys.path.insert(0, str(REPO_ROOT))
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# --------------------------------

DEFAULT_TABLES = [
    "users", "products",
    "orders", "order_items", "order_status_history",
    "payments",
    "cart_item", "wishlist",
]

def _module_name(table: str) -> str:
    return f"MKM_Data_Validation_and_cleaning.validators.post_validations.postvalidate_{table}"

def main():
    ap = argparse.ArgumentParser(description="Run post-clean validations for multiple tables")
    ap.add_argument("--tables", help="Comma-separated list (default: full set)", default=None)
    ap.add_argument("--exclude", help="Comma-separated list to skip", default=None)
    ap.add_argument("--stop-on-error", action="store_true")
    args = ap.parse_args()

    tables = [t.strip() for t in args.tables.split(",")] if args.tables else list(DEFAULT_TABLES)
    if args.exclude:
        skip = {t.strip() for t in args.exclude.split(",")}
        tables = [t for t in tables if t not in skip]

    print(f"[POST-VALIDATION][ALL] Plan -> {tables}")
    failures = []
    skipped  = []

    for t in tables:
        mod_name = _module_name(t)
        try:
            mod = importlib.import_module(mod_name)
        except ModuleNotFoundError as e:
            print(f"[POST-VALIDATION][{t}] SKIP (no module): {mod_name}")
            skipped.append(t)
            continue
        try:
            if hasattr(mod, "main"):
                print(f"[POST-VALIDATION][{t}] ▶ running")
                mod.main()
            else:
                raise RuntimeError(f"{mod_name} has no main()")
            print(f"[POST-VALIDATION][{t}] ✅ done")
        except Exception as e:
            print(f"[POST-VALIDATION][{t}] ❌ {e}")
            failures.append((t, str(e)))
            if args.stop_on_error:
                break

    if failures:
        print("[POST-VALIDATION][ALL] Failures:")
        for t, err in failures:
            print(f"  - {t}: {err}")
    if skipped:
        print("[POST-VALIDATION][ALL] Skipped:")
        for t in skipped:
            print(f"  - {t}")

    # propagate status to shell/CI
    sys.exit(1 if failures else 0)

if __name__ == "__main__":
    main()













# def main():
#     ap = argparse.ArgumentParser(description="Run post-clean validations for multiple tables")
#     ap.add_argument("--tables", help="Comma-separated list (default: full set)", default=None)
#     ap.add_argument("--exclude", help="Comma-separated list to skip", default=None)
#     ap.add_argument("--stop-on-error", action="store_true")
#     args = ap.parse_args()

#     tables = [t.strip() for t in args.tables.split(",")] if args.tables else list(DEFAULT_TABLES)
#     if args.exclude:
#         skip = {t.strip() for t in args.exclude.split(",")}
#         tables = [t for t in tables if t not in skip]

#     print(f"[POST-VALIDATION][ALL] Plan -> {tables}")
#     failures = []
#     skipped = []
#     for t in tables:
#         try:
#             mod = importlib.import_module(_module_name(t))
#             if hasattr(mod, "main"):
#                 print(f"[POST-VALIDATION][{t}] ▶ running")
#                 mod.main()
#             else:
#                 raise RuntimeError(f"{_module_name(t)} has no main()")
#             print(f"[POST-VALIDATION][{t}] ✅ done")
#         except Exception as e:
#             print(f"[POST-VALIDATION][{t}] ❌ {e}")
#             failures.append((t, str(e)))
#             if args.stop_on_error:
#                 break

#     if failures:
#         print("[POST-VALIDATION][ALL] Failures:")
#         for t, err in failures:
#             print(f"  - {t}: {err}")

# if __name__ == "__main__":
#     main()
