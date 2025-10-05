# MKM_Glue_tranformations/src/jobs/sales_transformations/build_sales_transformations.py

"""
Run Sales Analytics transformations.

Examples:
  python -m MKM_Glue_tranformations.src.jobs.transformations.build_transformations
  python -m MKM_Glue_tranformations.src.jobs.transformations.build_transformations --jobs fact_order_items,dim_products
"""

# --- bootstrap ---
import sys, argparse, traceback
from pathlib import Path
HERE = Path(__file__).resolve()
REPO_ROOT = next(p for p in [HERE.parent] + list(HERE.parents) if (p / "project_bootstrap.py").exists())
sys.path.insert(0, str(REPO_ROOT))
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# -----------------

from src.connections.db_connections import spark_session_for_JDBC
from src.utils.config_loader import load_env_and_get
from src.utils.lineage import get_run_id, write_run_manifest, write_event

# imports (package mode first, script-mode fallback)
try:
    # star schema / enriched fact
    from .fact_sales_lines import build as build_fact_sales_lines
    # facts
    from .fact_order_items import build as build_fact_order_items
    from .fact_orders import build as build_fact_orders
    from .fact_payments import build as build_fact_payments
    from .fact_order_status_history import build as build_fact_order_status_latest
    # dims
    from .dim_products import build as build_dim_products
    from .dim_category import build as build_dim_category
    from .dim_users import build as build_dim_users
    from .dim_address import build as build_dim_address
    # NEW: curated sales marts
    from .order_funnel_status import build as build_order_funnel_status
    from .users_orders_daily import build as build_users_orders_daily
    from .product_sales_daily import build as build_product_sales_daily
    from .payments_by_order_fact import build as build_payments_by_order_fact
except ImportError:
    from MKM_Glue_tranformations.src.jobs.sales_transformations.fact_sales_lines import build as build_fact_sales_lines
    from MKM_Glue_tranformations.src.jobs.sales_transformations.fact_order_items import build as build_fact_order_items
    from MKM_Glue_tranformations.src.jobs.sales_transformations.fact_orders import build as build_fact_orders
    from MKM_Glue_tranformations.src.jobs.sales_transformations.fact_payments import build as build_fact_payments
    from MKM_Glue_tranformations.src.jobs.sales_transformations.fact_order_status_history import build as build_fact_order_status_latest
    from MKM_Glue_tranformations.src.jobs.sales_transformations.dim_products import build as build_dim_products
    from MKM_Glue_tranformations.src.jobs.sales_transformations.dim_category import build as build_dim_category
    from MKM_Glue_tranformations.src.jobs.sales_transformations.dim_users import build as build_dim_users
    from MKM_Glue_tranformations.src.jobs.sales_transformations.dim_address import build as build_dim_address
    # NEW: curated sales marts
    from MKM_Glue_tranformations.src.jobs.sales_transformations.order_funnel_status import build as build_order_funnel_status
    from MKM_Glue_tranformations.src.jobs.sales_transformations.users_orders_daily import build as build_users_orders_daily
    from MKM_Glue_tranformations.src.jobs.sales_transformations.product_sales_daily import build as build_product_sales_daily
    from MKM_Glue_tranformations.src.jobs.sales_transformations.payments_by_order_fact import build as build_payments_by_order_fact

def _jobs_registry():
    return {
        # wide enriched fact
        "fact_sales_lines_enriched": build_fact_sales_lines,

        # modular star (facts)
        "fact_order_items": build_fact_order_items,
        "fact_orders": build_fact_orders,
        "fact_payments": build_fact_payments,
        "fact_order_status_latest": build_fact_order_status_latest,

        # modular star (dims)
        "dim_products": build_dim_products,
        "dim_category": build_dim_category,
        "dim_users": build_dim_users,
        "dim_address": build_dim_address,

        # NEW: curated sales marts
        "order_funnel_status": build_order_funnel_status,
        "users_orders_daily": build_users_orders_daily,
        "product_sales_daily": build_product_sales_daily,
        "payments_by_order_fact": build_payments_by_order_fact,
    }

def main():
    ap = argparse.ArgumentParser(description="Build Sales Analytics transformations")
    ap.add_argument("--jobs", default=None, help="Comma-separated list (default: all)")
    ap.add_argument("--stop-on-error", action="store_true")
    args = ap.parse_args()

    load_env_and_get()
    run_id = get_run_id()

    jobs_map = _jobs_registry()
    plan = [j.strip() for j in args.jobs.split(",")] if args.jobs else list(jobs_map.keys())
    unknown = [j for j in plan if j not in jobs_map]
    if unknown:
        raise SystemExit(f"Unknown job(s): {unknown}. Valid: {list(jobs_map)}")

    write_run_manifest("sales_transformations", {"job": "sales_transformations/build_sales_transformations.py", "plan": plan}, run_id=run_id)
    write_event("sales_transformations", {"event": "transform_batch_start", "plan": plan}, run_id=run_id)

    spark = spark_session_for_JDBC(app_name="build_sales_transformations")
    outputs, failures = [], []

    try:
        for name in plan:
            fn = jobs_map[name]
            try:
                print(f"[TRANSFORM][{name}] ▶ starting")
                out_path = fn(spark, run_id=run_id)
                outputs.append((name, out_path))
                write_event("sales_transformations", {"event": "transform_job_complete", "job": name, "output": out_path}, run_id=run_id)
                print(f"[TRANSFORM][{name}] ✅ {out_path}")
            except Exception as e:
                failures.append((name, str(e)))
                write_event("sales_transformations", {"event": "transform_job_error", "job": name, "error": str(e)}, run_id=run_id)
                print(f"[TRANSFORM][{name}] ❌ {e}")
                traceback.print_exc()
                if args.stop_on_error:
                    break
    finally:
        spark.stop()
        write_event("sales_transformations", {"event": "transform_batch_finish", "ok": len(outputs), "errors": len(failures)}, run_id=run_id)

    print("\n[TRANSFORM] Outputs:")
    for n, p in outputs:
        print(f"  - {n}: {p}")
    if failures:
        print("[TRANSFORM] Failures:")
        for n, e in failures:
            print(f"  - {n}: {e}")

if __name__ == "__main__":
    main()
