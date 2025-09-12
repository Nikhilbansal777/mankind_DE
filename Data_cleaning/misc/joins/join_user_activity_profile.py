from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

jdbc_url = f"jdbc:mysql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
properties = {
    "user": os.getenv("DB_USERNAME"),
    "password": os.getenv("DB_PASSWORD"),
    "driver": "com.mysql.cj.jdbc.Driver"
}

output_path = "E:/mankind_matrix_project/MKM_Glue_Transformations/sample_output/enriched_user_activity"

spark = SparkSession.builder         .appName("EnrichUserActivity")         .config("spark.jars", "file:///" + os.getenv("JDBC_DRIVER_PATH").replace('\\', '/'))         .config("spark.driver.extraClassPath", "file:///" + os.getenv("JDBC_DRIVER_PATH").replace('\\', '/'))         .getOrCreate()

def read_table(table_name):
    return spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)

users = read_table("user_entity")
wishlist = read_table("wishlist")
cart = read_table("cart")
reviews = read_table("reviews")
recent_views = read_table("recently_viewed_products")

user_wishlist = users.join(wishlist, on="USER_ID", how="left")
user_cart = user_wishlist.join(cart, on="USER_ID", how="left")
user_reviews = user_cart.join(reviews, on="USER_ID", how="left")
enriched_user = user_reviews.join(recent_views, on="USER_ID", how="left")

cleaned_user = enriched_user.dropna(subset=["USER_ID"])

cleaned_user.write.mode("overwrite").parquet(output_path)

print(f"✅ Enriched user activity written to: {output_path}")