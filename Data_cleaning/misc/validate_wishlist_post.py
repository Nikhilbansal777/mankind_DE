import sys
import os

# ✅ sys.path setup
current_file_path = os.path.abspath(__file__)
validation_root = os.path.abspath(os.path.join(current_file_path, "..", ".."))
sys.path.insert(0, validation_root)

project_root = os.path.abspath(os.path.join(validation_root, ".."))
sys.path.insert(0, project_root)

# ✅ Imports
from src.connections.db_connections import spark_session_for_JDBC
from validators.validators_common import validation_checks as vc
from pyspark.sql.functions import col

# ✅ Start Spark session
spark = spark_session_for_JDBC()

# ✅ Load cleaned wishlist JSON
wishlist_path = "MKM_Data_Validation_and_cleaning/cleaned_outputs/dummy_cleaned_wishlist.json"
df = spark.read.option("multiline", True).json(wishlist_path)

# ✅ Load cleaned products JSON to verify brand/name correctness
products_path = "MKM_Data_Validation_and_cleaning/cleaned_outputs/dummy_cleaned_products.json"
df_products = spark.read.option("multiline", True).json(products_path)

# ✅ Rename product ID for join compatibility
df_products = df_products.withColumnRenamed("id", "product_id") \
                         .withColumnRenamed("name", "product_name") \
                         .withColumnRenamed("brand", "product_brand")

# ✅ Join wishlist + products to compare fields
wishlist_joined = df.join(df_products, on="product_id", how="left")

# ✅ Identify mismatches
brand_mismatch_df = wishlist_joined.filter(col("brand") != col("product_brand"))
name_mismatch_df = wishlist_joined.filter(col("name") != col("product_name"))

# ✅ Auto-correct brand & name using product reference
df = wishlist_joined.withColumn("brand", col("product_brand")) \
                    .withColumn("name", col("product_name")) \
                    .select(df.columns)  # Select only original columns


# ✅ Log mismatches for traceability
report_dir = "MKM_Data_Validation_and_cleaning/reports/validation_reports/"
os.makedirs(report_dir, exist_ok=True)

if brand_mismatch_df.count() > 0:
    brand_mismatch_df.select("product_id", "brand", "product_brand") \
        .write.mode("overwrite").json(report_dir + "wishlist_brand_mismatches.json")
    print("[⚠️] Brand mismatches found and overwritten in wishlist.")

if name_mismatch_df.count() > 0:
    name_mismatch_df.select("product_id", "name", "product_name") \
        .write.mode("overwrite").json(report_dir + "wishlist_name_mismatches.json")
    print("[⚠️] Name mismatches found and overwritten in wishlist.")

# ✅ Run regular validations after corrections
df = vc.check_nulls(df, "id")
df = vc.check_positive(df, "id")

df = vc.check_nulls(df, "user_id")
df = vc.check_positive(df, "user_id")

df = vc.check_nulls(df, "product_id")
df = vc.check_positive(df, "product_id")

df = vc.check_non_empty_string(df, "name")
df = vc.check_non_empty_string(df, "brand")

df = vc.check_positive(df, "price")
df = vc.check_numeric_range(df, "rating", 0.0, 5.0)

df = vc.check_positive(df, "review_count")
df = vc.check_numeric_range(df, "discounted_price", 0.0, float("inf"))

# ✅ Collect validation failures
invalid_df = df.filter(
    (~col("id_not_null")) |
    (~col("id_positive")) |
    (~col("user_id_not_null")) |
    (~col("user_id_positive")) |
    (~col("product_id_not_null")) |
    (~col("product_id_positive")) |
    (~col("name_non_empty")) |
    (~col("brand_non_empty")) |
    (~col("price_positive")) |
    (~col("rating_in_range")) |
    (~col("review_count_positive")) |
    (~col("discounted_price_in_range"))
)

# ✅ Save final validation report (if any)
output_path = "MKM_Data_Validation_and_cleaning/reports/validation_reports/wishlist_post_validation_failed.json"

if invalid_df.count() == 0:
    print("[✅] No validation errors found in cleaned wishlist dataset.")
else:
    invalid_df.write.mode("overwrite").json(output_path)
    print(f"[✅] Post-cleaning validation report saved to: {output_path}")





# import sys
# import os

# # ✅ Set up sys.path for importing src/ and validators/
# current_file_path = os.path.abspath(__file__)
# validation_root = os.path.abspath(os.path.join(current_file_path, "..", ".."))
# sys.path.insert(0, validation_root)

# project_root = os.path.abspath(os.path.join(validation_root, ".."))
# sys.path.insert(0, project_root)

# # ✅ Imports
# from src.connections.db_connections import spark_session_for_JDBC
# from validators.validators_common import validation_checks as vc
# from pyspark.sql.functions import col

# # ✅ Start Spark session
# spark = spark_session_for_JDBC()

# # ✅ Load cleaned wishlist JSON
# cleaned_path = "MKM_Data_Validation_and_cleaning/cleaned_outputs/dummy_cleaned_wishlist.json"
# df = spark.read.option("multiline", True).json(cleaned_path)

# # ✅ Apply validation rules
# df = vc.check_nulls(df, "id")
# df = vc.check_positive(df, "id")

# df = vc.check_nulls(df, "user_id")
# df = vc.check_positive(df, "user_id")

# df = vc.check_nulls(df, "product_id")
# df = vc.check_positive(df, "product_id")

# df = vc.check_non_empty_string(df, "name")
# df = vc.check_non_empty_string(df, "brand")

# df = vc.check_positive(df, "price")
# df = vc.check_numeric_range(df, "rating", 0.0, 5.0)

# df = vc.check_positive(df, "review_count")
# df = vc.check_numeric_range(df, "discounted_price", 0.0, float("inf"))

# # ✅ Collect invalid rows
# invalid_df = df.filter(
#     (~col("id_not_null")) |
#     (~col("id_positive")) |
#     (~col("user_id_not_null")) |
#     (~col("user_id_positive")) |
#     (~col("product_id_not_null")) |
#     (~col("product_id_positive")) |
#     (~col("name_non_empty")) |
#     (~col("brand_non_empty")) |
#     (~col("price_positive")) |
#     (~col("rating_in_range")) |
#     (~col("review_count_positive")) |
#     (~col("discounted_price_in_range"))
# )

# # ✅ Write to report
# output_path = "MKM_Data_Validation_and_cleaning/reports/validation_reports/wishlist_post_validation_failed.json"

# if invalid_df.count() == 0:
#     print("[✅] No validation errors found in cleaned wishlist dataset.")
# else:
#     invalid_df.write.mode("overwrite").json(output_path)
#     print(f"[✅] Post-cleaning validation report saved to: {output_path}")
