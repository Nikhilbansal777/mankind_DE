from src.etl_framework.base_etl_job import BaseETLJob
from pyspark.sql import DataFrame
from typing import Dict

class EnrichProductsJob(BaseETLJob):
    def transform(self, tables: Dict[str, DataFrame]) -> DataFrame:
        products = tables["products"]
        specs = tables["product_specifications"]
        images = tables["product_images"]
        inventories = tables["inventories"]

        df = products.join(specs, "PRODUCT_ID", "left") \
                     .join(images, "PRODUCT_ID", "left") \
                     .join(inventories, "PRODUCT_ID", "left")
        return df

if __name__ == "__main__":
    job = EnrichProductsJob(
        job_name="enrich_products",
        required_tables=["products", "product_specifications", "product_images", "inventories"],
        output_path="E:/mankind_matrix_project/MKM_Glue_Transformations/sample_output/enriched_products"
    )
    job.run()