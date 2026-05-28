"""
Order-centric transformations focusing on order processing and analysis.
"""
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

class OrderTransformations:
    @staticmethod
    def enrich_order_details(
        orders_df: SparkDataFrame,
        order_items_df: SparkDataFrame,
        products_df: SparkDataFrame
    ) -> SparkDataFrame:
        """
        Enrich orders with detailed item and product information.
        """
        items_with_products = order_items_df.join(products_df, "product_id")
        
        item_metrics = items_with_products.groupBy("order_id").agg(
            F.collect_list("product_name").alias("products"),
            F.sum("quantity").alias("total_items"),
            F.sum(F.col("quantity") * F.col("price")).alias("items_total"),
            F.count("product_id").alias("unique_products")
        )
        
        return orders_df.join(item_metrics, "order_id")
    
    @staticmethod
    def analyze_order_status_flow(
        orders_df: SparkDataFrame,
        status_history_df: SparkDataFrame
    ) -> SparkDataFrame:
        """
        Analyze the flow of order statuses with timing information.
        """
        window_spec = Window.partitionBy("order_id").orderBy("created_at")
        
        status_flow = (
            status_history_df
            .withColumn("prev_status", F.lag("status").over(window_spec))
            .withColumn("next_status", F.lead("status").over(window_spec))
            .withColumn("time_in_status",
                F.unix_timestamp(F.lead("created_at").over(window_spec)) -
                F.unix_timestamp("created_at")
            )
            .groupBy("order_id")
            .agg(
                F.collect_list("status").alias("status_sequence"),
                F.collect_list("time_in_status").alias("status_durations"),
                F.count("*").alias("status_changes")
            )
        )
        
        return orders_df.join(status_flow, "order_id")
    
    @staticmethod
    def analyze_order_payments(
        orders_df: SparkDataFrame,
        order_payments_df: SparkDataFrame,
        payments_df: SparkDataFrame
    ) -> SparkDataFrame:
        """
        Analyze payment patterns and issues in orders.
        """
        payment_info = (
            order_payments_df
            .join(payments_df, "payment_id")
            .groupBy("order_id")
            .agg(
                F.collect_list("payment_type").alias("payment_methods"),
                F.collect_list("status").alias("payment_statuses"),
                F.count("*").alias("payment_attempts"),
                F.sum(F.when(F.col("status") == "success", 1).otherwise(0))
                .alias("successful_payments")
            )
        )
        
        return (
            orders_df
            .join(payment_info, "order_id")
            .withColumn(
                "payment_success_rate",
                F.col("successful_payments") / F.col("payment_attempts")
            )
        )
    
    @staticmethod
    def get_order_fulfillment_metrics(
        orders_df: SparkDataFrame,
        status_history_df: SparkDataFrame
    ) -> SparkDataFrame:
        """
        Calculate order fulfillment and delivery metrics.
        """
        fulfillment_times = (
            status_history_df
            .filter(F.col("status").isin(["created", "delivered"]))
            .groupBy("order_id")
            .pivot("status")
            .agg(F.first("created_at"))
        )
        
        return (
            orders_df
            .join(fulfillment_times, "order_id")
            .withColumn(
                "fulfillment_time_hours",
                F.round(
                    (F.unix_timestamp("delivered") - F.unix_timestamp("created")) / 3600,
                    2
                )
            )
        )
