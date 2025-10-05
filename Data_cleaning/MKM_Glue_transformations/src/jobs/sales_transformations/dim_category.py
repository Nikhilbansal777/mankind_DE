# MKM_Glue_tranformations/src/jobs/sales_transformations/dim_category.py

# --- bootstrap ---
import sys
from pathlib import Path
HERE = Path(__file__).resolve()
REPO_ROOT = next(p for p in [HERE.parent] + list(HERE.parents) if (p / "project_bootstrap.py").exists())
sys.path.insert(0, str(REPO_ROOT))
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)
# ------------------

from pyspark.sql import functions as F
from MKM_Glue_tranformations.src.common.io import read_silver, write_sales_transform

def build(spark, run_id=None):
    cats = read_silver(spark, "category")
    keep = [c for c in ["category_id","category_name","parent_category_id"] if c in cats.columns]
    out = cats.select(*keep).dropDuplicates(["category_id"])
    out = out.withColumn("event_date", F.lit(None).cast("date"))
    return write_sales_transform(out, "dims/dim_category", partition_by=["event_date"], run_id=run_id)























# # --- bootstrap ---
# import sys
# from pathlib import Path
# HERE = Path(__file__).resolve()
# REPO_ROOT = next(p for p in [HERE.parent] + list(HERE.parents) if (p / "project_bootstrap.py").exists())
# sys.path.insert(0, str(REPO_ROOT))
# from project_bootstrap import bootstrap_project_paths
# bootstrap_project_paths(__file__)
# # ------------------

# from MKM_Glue_tranformations.src.common.io import read_silver, write_transform

# def build(spark, run_id=None):
#     c = read_silver(spark, "category")
#     keep = [x for x in ["category_id","category_name","parent_category_id","status"] if x in c.columns]
#     if not keep:
#         keep = c.columns
#     out = c.select(*keep).dropDuplicates(["category_id"]) if "category_id" in c.columns else c.dropDuplicates()
#     return write_transform(out, "dims/dim_category", run_id=run_id)
