import os
import sys

# Step 1: Add project root to sys.path
current_file = os.path.abspath(__file__)
project_root = os.path.abspath(os.path.join(current_file, "..", "..", "..", ".."))
sys.path.insert(0, project_root)

# Step 2: Bootstrap for src access
from project_bootstrap import bootstrap_project_paths
bootstrap_project_paths(__file__)

# Step 3: Imports
from src.connections.db_connections import spark_session_for_JDBC
from pyspark.sql.functions import col, coalesce

# Step 4: Start Spark session
spark = spark_session_for_JDBC()

# Step 5: Load cleaned input files
products_path = "MKM_Data_Validation_and_cleaning/cleaned_outputs/dummy_cleaned_products.json"
users_path = "MKM_Data_Validation_and_cleaning/cleaned_outputs/dummy_cleaned_users.json"
wishlist_path = "MKM_Data_Validation_and_cleaning/cleaned_outputs/corrected/wishlist_corrected.json"

df_products = spark.read.option("multiline", True).json(products_path)
df_users = spark.read.option("multiline", True).json(users_path)
df_wishlist = spark.read.option("multiline", True).json(wishlist_path)

# Step 6: Rename columns to avoid collisions
df_products = df_products.withColumnRenamed("id", "product_id") \
                         .withColumnRenamed("name", "product_name") \
                         .withColumnRenamed("brand", "product_brand")

df_users = df_users.withColumnRenamed("id", "user_id") \
                   .withColumnRenamed("name", "user_name") \
                   .withColumnRenamed("email", "user_email")

df_wishlist = df_wishlist.withColumnRenamed("id", "wishlist_id") \
                         .withColumnRenamed("name", "wishlist_name") \
                         .withColumnRenamed("brand", "wishlist_brand")

# Step 7: Join wishlist + products
wishlist_with_products = df_wishlist.join(
    df_products,
    on="product_id",
    how="left"
)

# Step 8: Join the result with users
wishlist_enriched = wishlist_with_products.join(
    df_users,
    on="user_id",
    how="left"
)

# Step 9: Full lineage preservation
wishlist_enriched = wishlist_enriched \
    .withColumnRenamed("wishlist_name", "wishlist_name_original") \
    .withColumnRenamed("wishlist_brand", "wishlist_brand_original") \
    .withColumn("final_name", coalesce("product_name", "wishlist_name_original")) \
    .withColumn("final_brand", coalesce("product_brand", "wishlist_brand_original"))

# Step 10: Save lineage for ML training/audit
lineage_output_path = "MKM_Data_Validation_and_cleaning/lineage/wishlist_name_brand_lineage.json"
wishlist_enriched.select(
    "wishlist_id", "product_id",
    "wishlist_name_original", "product_name", "final_name",
    "wishlist_brand_original", "product_brand", "final_brand"
).write.mode("overwrite").json(lineage_output_path)

# Step 11: Final output selection for transformation layer
final_output_df = wishlist_enriched.select(
    "wishlist_id", "user_id", "product_id",
    "final_name", "final_brand",
    "user_email", "user_name",
    "price", "discounted_price", "rating", "review_count"
)

# Step 12: Save enriched output
output_path = "MKM_Glue_tranformations/src/jobs/transformed_outputs/join_enrich_transform.json"
final_output_df.write.mode("overwrite").json(output_path)

print(f"[✅] Transformation completed.")
print(f"[📂] Final output: {output_path}")
print(f"[🧠] ML Lineage file: {lineage_output_path}")











# import os
# import sys

# # Step 1: Manually add the project root path to sys.path
# current_file = os.path.abspath(__file__)
# project_root = os.path.abspath(os.path.join(current_file, "..", "..", "..", ".."))  # Go 4 levels up
# sys.path.insert(0, project_root)

# # Step 2: Now import project_bootstrap
# from project_bootstrap import bootstrap_project_paths

# # Step 3: Call bootstrap to add src/ path
# bootstrap_project_paths(__file__)

# # Step 4: Now you can safely import from src
# from src.connections.db_connections import spark_session_for_JDBC
# from pyspark.sql.functions import col



# # Spark session
# spark = spark_session_for_JDBC()

# # Load all cleaned datasets
# products_path = "MKM_Data_Validation_and_cleaning/cleaned_outputs/dummy_cleaned_products.json"
# users_path = "MKM_Data_Validation_and_cleaning/cleaned_outputs/dummy_cleaned_users.json"
# wishlist_path = "MKM_Data_Validation_and_cleaning/cleaned_outputs/corrected/wishlist_corrected.json"

# df_products = spark.read.option("multiline", True).json(products_path)
# df_users = spark.read.option("multiline", True).json(users_path)
# df_wishlist = spark.read.option("multiline", True).json(wishlist_path)

# # Rename before joining (to avoid collisions)
# df_products = df_products.withColumnRenamed("id", "product_id") \
#                          .withColumnRenamed("brand", "product_brand") \
#                          .withColumnRenamed("name", "product_name")

# df_users = df_users.withColumnRenamed("id", "user_id") \
#                    .withColumnRenamed("name", "user_name") \
#                    .withColumnRenamed("email", "user_email")

# df_wishlist = df_wishlist.withColumnRenamed("id", "wishlist_id") \
#                          .withColumnRenamed("name", "product_name") \
#                          .withColumnRenamed("brand", "product_brand")

# # Join: wishlist + products
# wishlist_with_products = df_wishlist.join(
#     df_products,
#     on="product_id",
#     how="left"
# )

# # Join: above + users
# wishlist_enriched = wishlist_with_products.join(
#     df_users,
#     on="user_id",
#     how="left"
# )

# # Optional: select only needed columns or rename if you want to flatten the schema

# # Save output
# output_path = "MKM_Glue_transformations/src/jobs/transformed_outputs/join_enrich_transform.json"

# wishlist_enriched.write.mode("overwrite").json(output_path)

# print(f"[✅] Transformation completed. Output saved to: {output_path}")
