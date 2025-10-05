from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName("ValidationMetrics").getOrCreate()

df = spark.read.parquet("E:/mankind_matrix_project/MKM_Glue_Transformations/sample_output/enriched_product_summary")

total_count = df.count()
null_counts = {col: df.filter(df[col].isNull()).count() for col in df.columns}
print(f"🔍 Total records: {total_count}")
print("❗ Null counts per column:")
for col, count in null_counts.items():
    print(f"  - {col}: {count}")