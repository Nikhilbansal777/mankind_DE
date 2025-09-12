import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, current_date
from dotenv import load_dotenv

# Load environment variables
load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"))

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Join Users, Products, Wishlist") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

# Paths
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
cleaned_base_path = os.path.join(project_root, "MKM_Data_Validation_and_cleaning", "cleaned_outputs")
output_base_path = os.path.join(project_root, "MKM_Glue_tranformations", "enriched_outputs")

# Load cleaned datasets
products_df = spark.read.option("multiline", "true").json(os.path.join(cleaned_base_path, "products_cleaned.json"))
users_df = spark.read.option("multiline", "true").json(os.path.join(cleaned_base_path, "users_cleaned.json"))
wishlist_df = spark.read.option("multiline", "true").json(os.path.join(cleaned_base_path, "wishlist_cleaned.json"))

# Check schemas
print("✅ Loaded Schemas:")
products_df.printSchema()
users_df.printSchema()
wishlist_df.printSchema()

# JOIN: wishlist + users
wishlist_users_df = wishlist_df.join(
    users_df,
    on="user_id",
    how="inner"
)

# JOIN: wishlist_users + products
enriched_df = wishlist_users_df.join(
    products_df,
    on="product_id",
    how="inner"
)

# Enrichment: Add wishlist age in days if `wishlist.create_time` exists
if "create_time" in wishlist_df.columns:
    enriched_df = enriched_df.withColumn(
        "wishlist_age_days",
        datediff(current_date(), col("create_time"))
    )

# Output path
output_path = os.path.join(output_base_path, "wishlist_user_product_enriched.json")

# Save enriched dataset
enriched_df.coalesce(1).write.mode("overwrite").json(output_path)
print(f"\n🎉 Enriched file saved to: {output_path}\n")

spark.stop()
