# MKM_Glue_tranformations/src/jobs/semantic/build_semantic.py
"""
Run semantic layer jobs.

Examples:
  # recommended (module mode from repo root)
  python -m MKM_Glue_tranformations.src.jobs.semantic.build_semantic

  # run only one job
  python -m MKM_Glue_tranformations.src.jobs.semantic.build_semantic --jobs order_summary

  # direct script mode (also works)
  python MKM_Glue_tranformations/src/jobs/semantic/build_semantic.py
"""

# --- bootstrap (keep at top) ---
import sys, argparse, traceback
from pathlib import Path
HERE = Path(__file__).resolve()
REPO_ROOT = next(p for p in [HERE.parent] + list(HERE.parents) if (p / "project_bootstrap.py").exists())
sys.path.insert(0, str(REPO_ROOT))
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# --------------------------------

from src.connections.db_connections import spark_session_for_JDBC
from src.utils.config_loader import load_env_and_get
from src.utils.lineage import get_run_id, write_run_manifest, write_event

# Wire jobs: try package-relative first, fall back to absolute for script mode
try:
    # package mode (python -m ...)
    from .joins.join_order_summary_semantic import build as build_order_summary
    from .enrichments.enrich_wishlist_user_product import build as build_wishlist_enriched
    from .rollups.order_status_by_order_semantic import main as _rollup_order_status_main
    from .rollups.user_order_activity_semantic import main as _rollup_user_activity_main
except ImportError:
    # script mode (python build_semantic.py)
    from MKM_Glue_tranformations.src.jobs.semantic.joins.join_order_summary_semantic import build as build_order_summary
    from MKM_Glue_tranformations.src.jobs.semantic.enrichments.enrich_wishlist_user_product import build as build_wishlist_enriched
    from MKM_Glue_tranformations.src.jobs.semantic.rollups.order_status_by_order_semantic import main as _rollup_order_status_main
    from MKM_Glue_tranformations.src.jobs.semantic.rollups.user_order_activity_semantic import main as _rollup_user_activity_main

def _wrap_rollup_as_build(fn_main):
    """
    Adapt rollups that have a main() to the same `build(spark, run_id)` signature.
    We don't want duplicate Spark sessions; pass the active one via global Spark.
    """
    def _build(_spark, run_id=None):
        # these modules start/stop Spark themselves; we simply call main()
        # if you want to unify fully, refactor those files to accept (spark, run_id)
        fn_main()
        # No deterministic path to return; rollups print their path already.
        return "<rollup_wrote_to_semantic_run_path>"
    return _build

def _jobs_registry():
    # logical name -> callable(spark) -> returns output path (string)
    return {
        "order_summary": build_order_summary,
        # "wishlist_enriched": build_wishlist_enriched,              
        "order_status_by_order": _wrap_rollup_as_build(_rollup_order_status_main),
        "user_order_activity": _wrap_rollup_as_build(_rollup_user_activity_main),      
        }


def main():
    ap = argparse.ArgumentParser(description="Build semantic surfaces")
    ap.add_argument(
        "--jobs",
        help="Comma-separated list of jobs to run (default: all). "
             "Options: order_summary, wishlist_enriched",
        default=None,
    )
    ap.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Abort immediately if a job fails",
    )
    args = ap.parse_args()

    load_env_and_get()  # prints where .env was loaded from (via bootstrap)

    jobs_map = _jobs_registry()
    if args.jobs:
        wanted = [j.strip() for j in args.jobs.split(",") if j.strip()]
        unknown = [j for j in wanted if j not in jobs_map]
        if unknown:
            raise SystemExit(f"Unknown job(s): {unknown}. Valid: {list(jobs_map)}")
        plan = wanted
    else:
        plan = list(jobs_map.keys())

    run_id = get_run_id()
    write_run_manifest("semantic", {"job": "semantic/build_semantic.py", "plan": plan}, run_id=run_id)
    write_event("semantic", {"event": "semantic_batch_start", "plan": plan}, run_id=run_id)

    print(f"[SEMANTIC] Plan -> {plan}")

    spark = spark_session_for_JDBC(app_name="build_semantic")
    outputs = []
    failures = []

    try:
        for name in plan:
            fn = jobs_map[name]
            try:
                print(f"[SEMANTIC][{name}] ▶ starting")
                out_path = fn(spark, run_id=run_id)
                outputs.append((name, out_path))
                write_event("semantic", {"event": "semantic_job_complete", "job": name, "output": out_path}, run_id=run_id)
                print(f"[SEMANTIC][{name}] ✅ {out_path}")
            except Exception as e:
                failures.append((name, str(e)))
                write_event("semantic", {"event": "semantic_job_error", "job": name, "error": str(e)}, run_id=run_id)
                print(f"[SEMANTIC][{name}] ❌ {e}")
                traceback.print_exc()
                if args.stop_on_error:
                    break

        print("\n[SEMANTIC] Done.")
        if outputs:
            print("[SEMANTIC] Outputs:")
            for name, path in outputs:
                print(f"  - {name}: {path}")
        if failures:
            print("[SEMANTIC] Failures:")
            for name, err in failures:
                print(f"  - {name}: {err}")

    finally:
        spark.stop()
        write_event("semantic", {"event": "semantic_batch_finish", "ok": len(outputs), "errors": len(failures)}, run_id=run_id)

    if outputs:
        print("\n[SEMANTIC] Outputs:")
        for name, path in outputs:
            print(f"  - {name}: {path}")
    if failures:
        print("[SEMANTIC] Failures:")
        for name, err in failures:
            print(f"  - {name}: {err}")

if __name__ == "__main__":
    main()













# # --- bootstrap (keep at top) ---
# import sys
# from pathlib import Path
# HERE = Path(__file__).resolve()
# REPO_ROOT = next(p for p in [HERE.parent] + list(HERE.parents) if (p / "project_bootstrap.py").exists())
# sys.path.insert(0, str(REPO_ROOT))
# from project_bootstrap import bootstrap_project_paths
# bootstrap_project_paths(__file__)
# # --------------------------------

# from src.connections.db_connections import spark_session_for_JDBC

# # wire in semantic jobs
# from .joins.join_order_summary_semantic import build as build_order_summary
# from .enrichments.enrich_wishlist_user_product import build as build_wishlist_enriched

# def main():
#     spark = spark_session_for_JDBC(app_name="build_semantic")
#     try:
#         outputs = []
#         outputs.append(build_order_summary(spark))
#         outputs.append(build_wishlist_enriched(spark))
#         print("\n[SEMANTIC] surfaces written:")
#         for p in outputs:
#             print("  •", p)
#     finally:
#         spark.stop()

# if __name__ == "__main__":
#     main()
