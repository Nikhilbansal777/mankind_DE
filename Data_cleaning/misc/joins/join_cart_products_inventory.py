from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# JDBC connection
jdbc_url = f"jdbc:mysql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
properties = {
    "user": os.getenv("DB_USERNAME"),
    "password": os.getenv("DB_PASSWORD"),
    "driver": "com.mysql.cj.jdbc.Driver"
}

output_path = "E:/mankind_matrix_project/MKM_Glue_Transformations/sample_output/enriched_cart_items"

spark = SparkSession.builder         .appName("EnrichCartItems")         .config("spark.jars", "file:///" + os.getenv("JDBC_DRIVER_PATH").replace('\\', '/'))         .config("spark.driver.extraClassPath", "file:///" + os.getenv("JDBC_DRIVER_PATH").replace('\\', '/'))         .getOrCreate()

def read_table_safe(table_name):
    try:
        df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)
        print(f"✅ Successfully loaded table: {table_name}")
        return df
    except Exception as e:
        print(f"❌ Failed to load table: {table_name} — {e}")
        return None

try:
    cart_item = read_table_safe("cart_item")
    products = read_table_safe("products")
    inventories = read_table_safe("inventories")

    if cart_item and products and inventories:
        # Join cart_item with products
        cart_with_products = cart_item.join(products, on="PRODUCT_ID", how="left")

        # Join with inventory
        enriched_cart = cart_with_products.join(inventories, on="PRODUCT_ID", how="left")

        # Drop rows missing cart linkage
        cleaned = enriched_cart.dropna(subset=["CART_ID", "PRODUCT_ID"])

        # Write output locally
        cleaned.write.mode("overwrite").parquet(output_path)
        print(f"✅ Enriched cart data written to: {output_path}")
    else:
        print("🚫 One or more required tables could not be loaded. Skipping join.")

except Exception as err:
    print(f"❗ Critical error in transformation pipeline: {err}")