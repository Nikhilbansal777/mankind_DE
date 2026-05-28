from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# JDBC connection setup
jdbc_url = f"jdbc:mysql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
properties = {
    "user": os.getenv("DB_USERNAME"),
    "password": os.getenv("DB_PASSWORD"),
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Local output path (simulate S3)
output_path = "E:/mankind_matrix_project/MKM_Glue_Transformations/sample_output/enriched_product_summary"

# Spark session
spark = SparkSession.builder         .appName("EnrichProducts")         .config("spark.jars", "file:///" + os.getenv("JDBC_DRIVER_PATH").replace('\\', '/'))         .config("spark.driver.extraClassPath", "file:///" + os.getenv("JDBC_DRIVER_PATH").replace('\\', '/'))         .getOrCreate()

def read_table(table_name):
    return spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)

# Read all required tables
products = read_table("products")
images = read_table("product_images")
specs = read_table("product_specifications")
inventories = read_table("inventories")

# Join: products + images
prod_img = products.join(images, on="PRODUCT_ID", how="left")

# Join: add specifications
prod_img_specs = prod_img.join(specs, on="PRODUCT_ID", how="left")

# Join: add inventory info
enriched_df = prod_img_specs.join(inventories, on="PRODUCT_ID", how="left")

# Optional: remove any rows with missing essential product fields
cleaned_df = enriched_df.dropna(subset=["PRODUCT_ID", "PRODUCT_NAME"])

# Write the final enriched dataset
cleaned_df.write.mode("overwrite").parquet(output_path)

print(f"✅ Enriched product summary written to: {output_path}")