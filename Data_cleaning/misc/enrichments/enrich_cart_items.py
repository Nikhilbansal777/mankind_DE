from src.etl_framework.base_etl_job import BaseETLJob
from pyspark.sql import DataFrame
from typing import Dict

class EnrichCartItemsJob(BaseETLJob):
    def transform(self, tables: Dict[str, DataFrame]) -> DataFrame:
        cart_items = tables["cart_item"]
        products = tables["products"]
        inventories = tables["inventories"]

        df = cart_items.join(products, "PRODUCT_ID", "left") \
                       .join(inventories, "PRODUCT_ID", "left")
        return df

if __name__ == "__main__":
    job = EnrichCartItemsJob(
        job_name="enrich_cart_items",
        required_tables=["cart_item", "products", "inventories"],
        output_path="E:/mankind_matrix_project/MKM_Glue_Transformations/sample_output/enriched_cart_items"
    )
    job.run()

    