# Data_cleaning/MKM_Glue_tranformations/src/jobs/semantic/enrichments/enrich_wishlist_user_product.py

# join safe even when wishlist is empty / missing FKs
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
from pyspark.sql import types as T
from MKM_Glue_tranformations.src.common.io import read_silver, write_semantic
from MKM_Glue_tranformations.src.common.semantic_utils import safe_join_with_lineage

def _mk_event_ts(df, candidates):
    cols = [F.col(c) for c in candidates if c in df.columns]
    return df.withColumn("__event_ts", (F.coalesce(*cols) if cols else F.lit(None)).cast("timestamp"))

def _ensure_cols(df, spec):
    """
    Ensure columns exist with the given Spark types (as NULLs if missing).
    spec: {col_name: SparkType}
    """
    for c, t in spec.items():
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast(t))
    return df

def build(spark, run_id=None):
    # 1) Read silver
    w = read_silver(spark, "wishlist")
    u = read_silver(spark, "users")
    p = read_silver(spark, "products")

    print("WISHLIST columns:", w.columns)
    print("USERS columns:", u.columns)
    print("PRODUCTS columns:", p.columns)

    w.printSchema()
    u.printSchema()
    p.printSchema()
    
    print("Counts -> wishlist:", w.count(), "users:", u.count(), "products:", p.count())

    # If any are empty-without-schema, add the minimum expected columns
    w = _ensure_cols(w, {
        "wishlist_id": T.LongType(),
        "user_id": T.LongType(),
        "users_id": T.LongType(),
        "product_id": T.LongType(),
        "products_id": T.LongType(),
        "created_at": T.TimestampType(),
        "updated_at": T.TimestampType(),
        "name": T.StringType(),
        "brand": T.StringType(),
    })
    u = _ensure_cols(u, {
        "user_id": T.LongType(),
        "users_id": T.LongType(),
        "email": T.StringType(),
        "username": T.StringType(),
        "create_time": T.TimestampType(),
        "update_time": T.TimestampType(),
    })
    p = _ensure_cols(p, {
        "product_id": T.LongType(),
        "products_id": T.LongType(),
        "name": T.StringType(),
        "brand": T.StringType(),
        "created_at": T.TimestampType(),
        "updated_at": T.TimestampType(),
    })

    # 2) Per-table event_ts (typed)
    w = _mk_event_ts(w, ["updated_at", "created_at"])
    u = _mk_event_ts(u, ["update_time", "create_time"])
    p = _mk_event_ts(p, ["updated_at", "created_at"])

    # 3) Resolve join keys (choose existing ones; if duplicates exist, prefer *_id)
    wl_user_fk = "user_id" if "user_id" in w.columns else "users_id"
    wl_prod_fk = "product_id" if "product_id" in w.columns else "products_id"
    users_pk   = "users_id" if "users_id" in u.columns else "user_id"
    prod_pk    = "products_id" if "products_id" in p.columns else "product_id"

    # Make sure the chosen keys exist (create as NULLs if they don't, to avoid AnalysisException on empty sets)
    for c in [wl_user_fk, wl_prod_fk]:
        if c not in w.columns:
            w = w.withColumn(c, F.lit(None).cast(T.LongType()))
    for c in [users_pk]:
        if c not in u.columns:
            u = u.withColumn(c, F.lit(None).cast(T.LongType()))
    for c in [prod_pk]:
        if c not in p.columns:
            p = p.withColumn(c, F.lit(None).cast(T.LongType()))

    # 4) Lineage-aware joins
    w_u = safe_join_with_lineage(
        left=w, right=u,
        left_table="wishlist", right_table="users",
        on_expr=w[wl_user_fk] == u[users_pk],
        how="left",
        join_key_names=(wl_user_fk, users_pk),
    )
    w_u_p = safe_join_with_lineage(
        left=w_u, right=p,
        left_table="wishlist", right_table="products",
        on_expr=w_u[wl_prod_fk] == p[prod_pk],
        how="left",
        join_key_names=(wl_prod_fk, prod_pk),
    )

    # 5) Standardize FK names
    out = w_u_p
    if wl_user_fk != "user_id":
        out = out.withColumnRenamed(wl_user_fk, "user_id")
    if wl_prod_fk != "product_id":
        out = out.withColumnRenamed(wl_prod_fk, "product_id")

    # 6) Final event_ts / event_date
    out = (out
        .withColumn(
            "event_ts",
            F.coalesce(
                F.col("__event_ts"),
                F.col("users__event_ts"),
                F.col("products__event_ts")
            ).cast("timestamp")
        )
        .withColumn("event_date", F.to_date("event_ts"))
    )

    # 7) Canonical attributes
    name_candidates  = [c for c in ["products_name", "name", "wishlist_name"] if c in out.columns]
    brand_candidates = [c for c in ["products_brand", "brand", "wishlist_brand"] if c in out.columns]
    out = out.withColumn("final_name",
                         F.coalesce(*[F.col(c) for c in name_candidates]) if name_candidates else F.lit(None).cast("string"))
    out = out.withColumn("final_brand",
                         F.coalesce(*[F.col(c) for c in brand_candidates]) if brand_candidates else F.lit(None).cast("string"))

    # 8) Select tidy output
    select_cols = [c for c in [
        "wishlist_id", "user_id", "product_id",
        "users_id", "products_id",
        "final_name", "final_brand",
        "email", "username",
        "event_ts", "event_date"
    ] if c in out.columns]
    out = out.select(*select_cols)

    # 9) Write (run-scoped)
    return write_semantic(out, "enriched/wishlist_user_product_enriched",
                          partition_by=["event_date"], run_id=run_id)



# it is not suitable if wishlist has empty rows
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
# from MKM_Glue_tranformations.src.common.io import read_silver, write_semantic
# from MKM_Glue_tranformations.src.common.semantic_utils import safe_join_with_lineage

# def _mk_event_ts(df, candidates):
#     """
#     Build a per-DF __event_ts with a concrete TimestampType, never NullType.
#     """
#     cols = [F.col(c) for c in candidates if c in df.columns]
#     return df.withColumn("__event_ts", (F.coalesce(*cols) if cols else F.lit(None)).cast("timestamp"))

#     # if cols:
#     #     return df.withColumn("__event_ts", F.coalesce(*cols).cast("timestamp"))
#     # else:
#     #     return df.withColumn("__event_ts", F.lit(None).cast("timestamp"))

# def build(spark, run_id=None):
#     # 1) Read silver
#     w = read_silver(spark, "wishlist")
#     u = read_silver(spark, "users")
#     p = read_silver(spark, "products")

#     # 2) Per-table event_ts (typed)
#     w = _mk_event_ts(w, ["updated_at", "created_at"])
#     u = _mk_event_ts(u, ["update_time", "create_time"])
#     p = _mk_event_ts(p, ["updated_at", "created_at"])

#     # 3) Resolve join keys
#     wl_user_fk = "user_id" if "user_id" in w.columns else "users_id"
#     wl_prod_fk = "product_id" if "product_id" in w.columns else "products_id"
#     users_pk   = "users_id" if "users_id" in u.columns else "user_id"
#     prod_pk    = "products_id" if "products_id" in p.columns else "product_id"

#     # 4) Lineage-aware joins (RIGHT side generic collisions auto-prefixed)
#     w_u = safe_join_with_lineage(
#         left=w, right=u,
#         left_table="wishlist", right_table="users",
#         on_expr=w[wl_user_fk] == u[users_pk],
#         how="left",
#         join_key_names=(wl_user_fk, users_pk)
#     )
#     w_u_p = safe_join_with_lineage(
#         left=w_u, right=p,
#         left_table="wishlist", right_table="products",
#         on_expr=w_u[wl_prod_fk] == p[prod_pk],
#         how="left",
#         join_key_names=(wl_prod_fk, prod_pk)
#     )

#     # 5) Standardize FK names
#     out = w_u_p
#     if wl_user_fk != "user_id":
#         out = out.withColumnRenamed(wl_user_fk, "user_id")
#     if wl_prod_fk != "product_id":
#         out = out.withColumnRenamed(wl_prod_fk, "product_id")

#     # 6) Final event_ts / event_date (avoid 'void' by casting)
#     out = (out
#         .withColumn(
#             "event_ts",
#             F.coalesce(
#                 F.col("__event_ts"),          # from wishlist (left)
#                 F.col("users__event_ts"),     # renamed by helper on first join
#                 F.col("products__event_ts")   # renamed by helper on second join
#             ).cast("timestamp")
#         )
#         .withColumn("event_date", F.to_date("event_ts"))
#     )

#     # 7) Canonical attributes
#     name_candidates  = [c for c in ["products_name", "name", "wishlist_name"] if c in out.columns]
#     brand_candidates = [c for c in ["products_brand", "brand", "wishlist_brand"] if c in out.columns]
#     out = out.withColumn("final_name",
#                          F.coalesce(*[F.col(c) for c in name_candidates]) if name_candidates else F.lit(None).cast("string"))
#     out = out.withColumn("final_brand",
#                          F.coalesce(*[F.col(c) for c in brand_candidates]) if brand_candidates else F.lit(None).cast("string"))

#     # 8) Select tidy output
#     select_cols = [c for c in [
#         "wishlist_id", "user_id", "product_id",
#         "users_id", "products_id",
#         "final_name", "final_brand",
#         "email", "username",
#         "event_ts", "event_date"
#     ] if c in out.columns]
#     out = out.select(*select_cols)

#     # 9) Write
#     return write_semantic(out, "enriched/wishlist_user_product_enriched", partition_by=["event_date"], run_id=run_id)



























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
# from MKM_Glue_tranformations.src.common.io import read_silver, write_semantic
# from MKM_Glue_tranformations.src.common.semantic_utils import safe_join_with_lineage

# def _mk_event_ts(df, candidates):
#     """
#     Build a per-DF event_ts with a concrete TimestampType, never NullType.
#     If none of the candidate columns exist, produce NULL::timestamp.
#     """
#     cols = [F.col(c) for c in candidates if c in df.columns]
#     if cols:
#         return df.withColumn("__event_ts", F.coalesce(*cols).cast("timestamp"))
#     else:
#         return df.withColumn("__event_ts", F.lit(None).cast("timestamp"))

# def build(spark):
#     # 1) Read silver
#     w = read_silver(spark, "wishlist")
#     u = read_silver(spark, "users")
#     p = read_silver(spark, "products")

#     # 2) Per-table event_ts (typed!)
#     # wishlist often has created_at/updated_at (if not, still yields NULL::timestamp)
#     w = _mk_event_ts(w, ["updated_at", "created_at"])
#     # users has update_time/create_time after cleaning rules
#     u = _mk_event_ts(u, ["update_time", "create_time"])
#     # products has updated_at/created_at
#     p = _mk_event_ts(p, ["updated_at", "created_at"])

#     # 3) Key resolution (handles users_id vs user_id, etc.)
#     # We’ll prefer the conventional names after join
#     wl_user_fk = "user_id" if "user_id" in w.columns else "users_id"
#     wl_prod_fk = "product_id" if "product_id" in w.columns else "products_id"
#     users_pk   = "users_id" if "users_id" in u.columns else "user_id"
#     prod_pk    = "products_id" if "products_id" in p.columns else "product_id"

#     if wl_user_fk not in w.columns or users_pk not in u.columns:
#         raise ValueError(f"Cannot join wishlist→users: wl FK '{wl_user_fk}' / users PK '{users_pk}' not found")
#     if wl_prod_fk not in w.columns or prod_pk not in p.columns:
#         raise ValueError(f"Cannot join wishlist→products: wl FK '{wl_prod_fk}' / products PK '{prod_pk}' not found")

#     # 4) Lineage-aware joins: auto-prefix only overlapping generic columns (name, brand, …) on RIGHT side
#     w_u, _ = safe_join_with_lineage(
#         left=w, right=u,
#         left_table="wishlist", right_table="users",
#         on_expr=w[wl_user_fk] == u[users_pk],
#         how="left"
#     )
#     w_u_p, _ = safe_join_with_lineage(
#         left=w_u, right=p,
#         left_table="wishlist", right_table="products",
#         on_expr=w_u[wl_prod_fk] == p[prod_pk],
#         how="left"
#     )

#     # 5) Standardize FK column names in the result (so downstream is stable)
#     out = w_u_p
#     if wl_user_fk != "user_id":
#         out = out.withColumnRenamed(wl_user_fk, "user_id")
#     if wl_prod_fk != "product_id":
#         out = out.withColumnRenamed(wl_prod_fk, "product_id")

#     # 6) Final, typed event_ts / event_date (never NullType)
#     out = (
#         out.withColumn(
#             "event_ts",
#             F.coalesce(
#                 F.col("__event_ts"),          # from wishlist
#                 F.col("users__event_ts"),     # safe_join prefixes right side with table name
#                 F.col("products__event_ts")
#             ).cast("timestamp")               # ensure TimestampType
#         )
#         .withColumn("event_date", F.to_date("event_ts"))
#     )

#     # 7) Canonical “final” attributes (strings)
#     #  - safe_join_with_lineage will have renamed RIGHT-side generic collisions to `users_*` / `products_*`
#     #  - wishlist keeps its own generic fields (if it has them)
#     name_candidates  = [c for c in ["products_name", "name", "wishlist_name"] if c in out.columns]
#     brand_candidates = [c for c in ["products_brand", "brand", "wishlist_brand"] if c in out.columns]

#     if name_candidates:
#         out = out.withColumn("final_name", F.coalesce(*[F.col(c) for c in name_candidates]).cast("string"))
#     else:
#         out = out.withColumn("final_name", F.lit(None).cast("string"))

#     if brand_candidates:
#         out = out.withColumn("final_brand", F.coalesce(*[F.col(c) for c in brand_candidates]).cast("string"))
#     else:
#         out = out.withColumn("final_brand", F.lit(None).cast("string"))

#     # 8) Select tidy output
#     select_cols = [c for c in [
#         "wishlist_id",               # from wishlist
#         "user_id",                   # standardized FK
#         "product_id",                # standardized FK
#         "users_id",                  # keep source PKs (optional)
#         "products_id",
#         "final_name", "final_brand",
#         "email", "username",         # users fields (if present)
#         "event_ts", "event_date"
#     ] if c in out.columns]

#     out = out.select(*select_cols)

#     # 9) Write (partitioned by event_date)
#     return write_semantic(out, "wishlist_user_product_enriched", partition_by=["event_date"])











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
# from MKM_Glue_tranformations.src.common.io import read_silver, write_semantic

# def build(spark):
#     wishlist = read_silver(spark, "wishlist")
#     users    = read_silver(spark, "users")
#     products = read_silver(spark, "products")

#     out = (wishlist
#         .join(users,   "user_id",    "left")
#         .join(products,"product_id", "left")
#         .withColumn("event_ts", F.coalesce("updated_at","created_at"))
#         .withColumn("event_date", F.to_date("event_ts"))
#     )
#     return write_semantic(out, "wishlist_user_product_enriched", partition_by=["event_date"])
