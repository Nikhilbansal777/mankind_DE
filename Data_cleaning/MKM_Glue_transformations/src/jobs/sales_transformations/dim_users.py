# MKM_Glue_tranformations/src/jobs/sales_transformations/dim_users.py

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
    users = read_silver(spark, "users")

    pk = _first(users, ["users_id","user_id","id"]) or "id"
    if pk != "user_id":
        users = users.withColumnRenamed(pk, "user_id")

    # compute event_date while ts columns still exist
    ts_candidates = [c for c in ["updated_at","create_time","created_at","update_time"] if c in users.columns]
    users = users.withColumn(
        "event_date",
        F.to_date(F.coalesce(*[F.col(c) for c in ts_candidates])) if ts_candidates else F.lit(None).cast("date")
    )

    # keep non-PII or minimal PII (adjust to your policy)
    keep = [c for c in [
        "user_id", "role", "username", "email", "status", "event_date"
    ] if c in users.columns]
    out = users.select(*keep).dropDuplicates(["user_id"])

    return write_sales_transform(out, "dims/dim_users", partition_by=["event_date"], run_id=run_id)



































# # MKM_Glue_tranformations/src/jobs/sales_transformations/dim_users.py

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
#     users = read_silver(spark, "users")

#     pk = _first(users, ["users_id","user_id","id"]) or "id"
#     if pk != "user_id":
#         users = users.withColumnRenamed(pk, "user_id")

#     # keep non-PII or minimal PII (adjust to your policy)
#     keep = [c for c in [
#         "user_id", "role", "username", "email", "status"
#     ] if c in users.columns]
#     out = users.select(*keep).dropDuplicates(["user_id"])

#     ts_candidates = [c for c in ["updated_at","create_time","created_at","update_time"] if c in users.columns]
#     out = out.withColumn("event_date", F.to_date(F.coalesce(*[F.col(c) for c in ts_candidates])) if ts_candidates else F.lit(None).cast("date"))

#     return write_sales_transform(out, "dims/dim_users", partition_by=["event_date"], run_id=run_id)





















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
# from MKM_Glue_tranformations.src.common.io import read_silver, write_transform

# def build(spark, run_id=None):
#     u = read_silver(spark, "users")
#     pk = "users_id" if "users_id" in u.columns else "user_id" if "user_id" in u.columns else "id"
#     if pk != "user_id":
#         u = u.withColumnRenamed(pk, "user_id")

#     keep = ["user_id"]
#     for c in ["role","status","created_at","update_time"]:
#         if c in u.columns: keep.append(c)

#     out = u.select(*keep).dropDuplicates(["user_id"])
#     return write_transform(out, "dims/dim_users", run_id=run_id)

