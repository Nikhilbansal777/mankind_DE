# MKM_Glue_tranformations/src/jobs/sales_transformations/dim_products.py

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
from MKM_Glue_tranformations.src.common.semantic_utils import safe_join_with_lineage, write_schema_lineage


def _first(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None


def build(spark, run_id=None):
    # Read silver tables
    prods = read_silver(spark, "products")
    cats  = read_silver(spark, "category")

    # -------------------------------
    # Normalize keys / column names
    # -------------------------------
    # Product PK
    prod_pk = _first(prods, ["product_id", "products_id"]) or "product_id"
    if prod_pk != "product_id":
        prods = prods.withColumnRenamed(prod_pk, "product_id")

    # Category FK on products
    cat_fk = _first(prods, ["category_id", "categories_id", "category"])
    if cat_fk and cat_fk != "category_id":
        prods = prods.withColumnRenamed(cat_fk, "category_id")

    # Category PK on category table
    cat_pk = _first(cats, ["category_id", "categories_id", "id"]) or "category_id"
    if cat_pk != "category_id":
        cats = cats.withColumnRenamed(cat_pk, "category_id")

    # Lineage snapshots (optional but helpful)
    write_schema_lineage("products", prods.columns, lineage_stage="sales_transformations")
    write_schema_lineage("category", cats.columns,   lineage_stage="sales_transformations")

    # ------------------------------------------------------
    # Join products to category WITHOUT ambiguous columns
    # (rename category.category_id on the RIGHT before join)
    # ------------------------------------------------------
    if "category_id" in prods.columns and "category_id" in cats.columns:
        cats_r = cats.withColumnRenamed("category_id", "category_category_id")
        out = safe_join_with_lineage(
            left=prods,
            right=cats_r,
            left_table="products",
            right_table="category",
            on_expr=prods["category_id"] == cats_r["category_category_id"],
            how="left",
            join_key_names=("category_id", "category_category_id"),
            lineage_stage="sales_transformations",
        )
    else:
        out = prods

    # -----------------------------------------
    # Select tidy dimension columns (disambiguated)
    # -----------------------------------------
    # Prefer unprefixed columns; fall back to lineage-prefixed ones if present.
    keep = []

    # product_id is mandatory
    if "product_id" in out.columns:
        keep.append("product_id")

    # name / brand / description / sku may be prefixed by lineage join
    if "name" in out.columns:
        keep.append("name")
    elif "products_name" in out.columns:
        keep.append("products_name")

    if "brand" in out.columns:
        keep.append("brand")
    elif "products_brand" in out.columns:
        keep.append("products_brand")

    for col in ("description", "sku"):
        if col in out.columns:
            keep.append(col)
        elif f"products_{col}" in out.columns:
            keep.append(f"products_{col}")

    # category_id (from products) should remain; category name can be from category table
    if "category_id" in out.columns:
        keep.append("category_id")

    if "category_name" in out.columns:
        keep.append("category_name")
    elif "category_category_name" in out.columns:
        keep.append("category_category_name")

    out = out.select(*keep).dropDuplicates(["product_id"])

    # Write dimension (no partitioning needed)
    return write_sales_transform(out, "dims/dim_products", partition_by=None, run_id=run_id)


if __name__ == "__main__":
    from src.connections.db_connections import spark_session_for_JDBC
    from src.utils.lineage import get_run_id
    from src.utils.config_loader import load_env_and_get
    load_env_and_get()
    spark = spark_session_for_JDBC(app_name="dim_products_single")
    try:
        print(build(spark, run_id=get_run_id()))
    finally:
        spark.stop()































# # MKM_Glue_tranformations/src/jobs/sales_transformations/dim_products.py

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
# from MKM_Glue_tranformations.src.common.semantic_utils import safe_join_with_lineage

# def _first(df, candidates):
#     for c in candidates:
#         if c in df.columns: return c
#     return None

# def build(spark, run_id=None):
#     prods = read_silver(spark, "products")
#     cats  = read_silver(spark, "category")

#     prod_pk = _first(prods, ["products_id","product_id"]) or "product_id"
#     if prod_pk != "product_id": prods = prods.withColumnRenamed(prod_pk, "product_id")

#     cat_fk = _first(prods, ["category_id","categories_id","category"])
#     if cat_fk and cat_fk != "category_id":
#         prods = prods.withColumnRenamed(cat_fk, "category_id")

#     if "category_id" in prods.columns and "category_id" in cats.columns:
#         out = safe_join_with_lineage(
#             left=prods, right=cats,
#             left_table="products", right_table="category",
#             on_expr=prods["category_id"] == cats["category_id"],
#             how="left", join_key_names=("category_id","category_id"),
#             lineage_stage="sales_transformations"
#         )
#     else:
#         out = prods

#     # select tidy dimension columns
#     keep = [c for c in [
#         "product_id", "name", "brand", "description",
#         "category_id", "category_name"
#     ] if c in out.columns]
#     out = out.select(*keep).dropDuplicates(["product_id"])

#     # small event_date for partitioning (if timestamps present)
#     ts_candidates = [c for c in ["updated_at","create_time","created_at","update_time"] if c in prods.columns]
#     out = out.withColumn("event_date", F.to_date(F.coalesce(*[F.col(c) for c in ts_candidates])) if ts_candidates else F.lit(None).cast("date"))

#     return write_sales_transform(out, "dims/dim_products", partition_by=["event_date"], run_id=run_id)




















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
#     p = read_silver(spark, "products")
#     c = read_silver(spark, "category")

#     pk = "products_id" if "products_id" in p.columns else "product_id"
#     if pk != "product_id": p = p.withColumnRenamed(pk, "product_id")

#     cat_fk = "category_id" if "category_id" in p.columns else None
#     if not cat_fk:
#         # tolerate missing — keep product-only dim
#         out = p.select(
#             "product_id",
#             *[c for c in ["name","brand","description","status"] if c in p.columns]
#         ).dropDuplicates(["product_id"])
#         return write_transform(out, "dims/dim_products", run_id=run_id)

#     out = (
#         p.join(c, on="category_id", how="left")
#          .select(
#             "product_id","category_id",
#             *[x for x in ["name","brand","description","status","category_name"] if x in (p.columns + c.columns)]
#          )
#          .dropDuplicates(["product_id"])
#     )
#     return write_transform(out, "dims/dim_products", run_id=run_id)

