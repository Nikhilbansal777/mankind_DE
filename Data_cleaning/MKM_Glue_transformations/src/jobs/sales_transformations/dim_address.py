# MKM_Glue_tranformations/src/jobs/sales_transformations/dim_address.py

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

def _first(df, candidates):
    for c in candidates:
        if c in df.columns: return c
    return None

def build(spark, run_id=None):
    addr = read_silver(spark, "address")

    # PK
    pk = _first(addr, ["address_id","id"])
    if pk and pk != "address_id":
        addr = addr.withColumnRenamed(pk, "address_id")

    # user key normalize
    if "users_id" in addr.columns and "user_id" not in addr.columns:
        addr = addr.withColumnRenamed("users_id","user_id")

    # street normalization
    if "street" not in addr.columns and "street_address" in addr.columns:
        addr = addr.withColumnRenamed("street_address", "street")

    # compute event_date while ts columns still exist
    ts_candidates = [c for c in ["updated_at","create_time","created_at","update_time"] if c in addr.columns]
    addr = addr.withColumn(
        "event_date",
        F.to_date(F.coalesce(*[F.col(c) for c in ts_candidates])) if ts_candidates else F.lit(None).cast("date")
    )

    keep = [c for c in [
        "address_id","user_id",
        "street","city","state","country","postal_code",
        "event_date"
    ] if c in addr.columns]
    out = addr.select(*keep).dropDuplicates(["address_id"])

    return write_sales_transform(out, "dims/dim_address", partition_by=["event_date"], run_id=run_id)



















# # MKM_Glue_tranformations/src/jobs/sales_transformations/dim_address.py

# # --- bootstrap ---
# import sys
# from pathlib import Path
# HERE = Path(__file__).resolve()
# REPO_ROOT = next(p for p in [HERE.parent] + list(HERE.parents) if (p / "project_bootstrap.py").exists())
# sys.path.insert(0, str(REPO_ROOT))
# from project_bootstrap import bootstrap_project_paths
# bootstrap_project_paths(__file__)
# # ------------------

# from pyspark.sql import functions as F
# from MKM_Glue_tranformations.src.common.io import read_silver, write_sales_transform

# def _first(df, candidates):
#     for c in candidates:
#         if c in df.columns: return c
#     return None

# def build(spark, run_id=None):
#     addr = read_silver(spark, "address")

#     pk = _first(addr, ["address_id","id"])
#     if pk and pk != "address_id":
#         addr = addr.withColumnRenamed(pk, "address_id")

#     keep = [c for c in [
#         "address_id","user_id","users_id",
#         "street","city","state","country","postal_code"
#     ] if c in addr.columns]
#     out = addr.select(*keep).dropDuplicates(["address_id"])

#     if "users_id" in out.columns and "user_id" not in out.columns:
#         out = out.withColumnRenamed("users_id","user_id")

#     ts_candidates = [c for c in ["updated_at","create_time","created_at","update_time"] if c in addr.columns]
#     out = out.withColumn("event_date", F.to_date(F.coalesce(*[F.col(c) for c in ts_candidates])) if ts_candidates else F.lit(None).cast("date"))

#     return write_sales_transform(out, "dims/dim_address", partition_by=["event_date"], run_id=run_id)


























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
#     a = read_silver(spark, "address")
#     keep = [c for c in ["address_id","user_id","city","state","country","zip"] if c in a.columns]
#     if not keep: keep = a.columns
#     out = a.select(*keep).dropDuplicates(["address_id"]) if "address_id" in a.columns else a.dropDuplicates()
#     return write_transform(out, "dims/dim_address", run_id=run_id)
