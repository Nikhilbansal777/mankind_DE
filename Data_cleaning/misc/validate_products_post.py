import sys
import os

# ✅ This points sys.path to: E:/mankind_matrix_project/MKM_Data_Validation_and_cleaning/
current_file_path = os.path.abspath(__file__)
validation_root = os.path.abspath(os.path.join(current_file_path, "..", ".."))
sys.path.insert(0, validation_root)

# ✅ This points sys.path to: E:/mankind_matrix_project/
project_root = os.path.abspath(os.path.join(validation_root, ".."))
sys.path.insert(0, project_root)

# ✅ Now both imports will work:
from src.connections.db_connections import spark_session_for_JDBC
from validators.validators_common import validation_checks as vc
from pyspark.sql.functions import col


# ✅ Start Spark session
spark = spark_session_for_JDBC()

# ✅ Load cleaned products JSON
cleaned_path = "MKM_Data_Validation_and_cleaning/cleaned_outputs/dummy_cleaned_products.json"
df = spark.read.option("multiline", True).json(cleaned_path)

# ✅ Apply post-cleaning validations
df = vc.check_nulls(df, "id")
df = vc.check_positive(df, "id")

df = vc.check_non_empty_string(df, "brand")
df = vc.check_non_empty_string(df, "name")
df = vc.check_non_empty_string(df, "sku")
df = vc.check_non_empty_string(df, "model")

df = vc.check_numeric_range(df, "average_rating", 0.0, 5.0)

df = vc.check_timestamp_castable(df, "created_at")
df = vc.check_timestamp_castable(df, "updated_at")

df = vc.check_allowed_values(df, "is_active", [True, False])
df = vc.check_allowed_values(df, "is_featured", [True, False])

# ✅ Filter failed records (any failed check returns False)
invalid_df = df.filter(
    (~col("id_not_null")) |
    (~col("id_positive")) |
    (~col("brand_non_empty")) |
    (~col("name_non_empty")) |
    (~col("sku_non_empty")) |
    (~col("model_non_empty")) |
    (~col("average_rating_in_range")) |
    (~col("created_at_is_timestamp")) |
    (~col("updated_at_is_timestamp")) |
    (~col("is_active_allowed")) |
    (~col("is_featured_allowed"))
)

# ✅ Show and write invalid rows to report
invalid_df.show(truncate=False)

output_path = "MKM_Data_Validation_and_cleaning/reports/validation_reports/products_post_validation_failed.json"
if invalid_df.count() == 0:
    print("[✅] No validation errors found in cleaned products dataset.")
else:
    invalid_df.write.mode("overwrite").json(output_path)
    print(f"[✅] Post-cleaning validation report saved to: {output_path}")

