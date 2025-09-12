"""
User-centric transformations focusing on user behavior and patterns.
"""
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

class UserTransformations:
    @staticmethod
    def enrich_user_order_history(users_df: SparkDataFrame, orders_df: SparkDataFrame) -> SparkDataFrame:
        """
        Enrich user data with their order history metrics.
        """
        order_metrics = orders_df.groupBy("user_id").agg(
            F.count("order_id").alias("total_orders"),
            F.sum("total_amount").alias("total_spent"),
            F.avg("total_amount").alias("avg_order_value"),
            F.min("created_at").alias("first_order_date"),
            F.max("created_at").alias("last_order_date")
        )
        
        return users_df.join(order_metrics, "user_id", "left")
    
    @staticmethod
    def get_user_product_preferences(
        users_df: SparkDataFrame, 
        order_items_df: SparkDataFrame,
        products_df: SparkDataFrame
    ) -> SparkDataFrame:
        """
        Analyze user product preferences based on order history.
        """
        user_products = (
            order_items_df
            .join(products_df, "product_id")
            .groupBy("user_id", "category")
            .agg(
                F.count("*").alias("category_purchase_count"),
                F.sum("quantity").alias("category_items_bought")
            )
        )
        
        window_spec = Window.partitionBy("user_id")
        
        return (
            user_products
            .withColumn(
                "category_preference_rank",
                F.rank().over(window_spec.orderBy(F.desc("category_purchase_count")))
            )
            .join(users_df, "user_id")
        )
    
    @staticmethod
    def analyze_user_cart_behavior(
        users_df: SparkDataFrame,
        cart_items_df: SparkDataFrame,
        orders_df: SparkDataFrame
    ) -> SparkDataFrame:
        """
        Analyze user shopping cart behavior patterns.
        """
        cart_metrics = cart_items_df.groupBy("user_id").agg(
            F.count("*").alias("total_cart_adds"),
            F.countDistinct("product_id").alias("unique_products_in_cart"),
            F.avg("quantity").alias("avg_cart_quantity")
        )
        
        orders_count = orders_df.groupBy("user_id").count().alias("order_count")
        
        return (
            users_df
            .join(cart_metrics, "user_id", "left")
            .join(orders_count, "user_id", "left")
            .withColumn(
                "cart_to_order_ratio",
                F.col("total_cart_adds") / F.col("order_count")
            )
        )
    
    @staticmethod
    def create_user_segments(enriched_users_df: SparkDataFrame) -> SparkDataFrame:
        """
        Create user segments based on order history and behavior.
        """
        return (
            enriched_users_df
            .withColumn(
                "recency_days",
                F.datediff(F.current_date(), F.col("last_order_date"))
            )
            .withColumn("customer_segment", 
                F.when(F.col("total_orders") > 10, "VIP")
                .when(F.col("total_orders") > 5, "Regular")
                .when(F.col("total_orders") > 0, "New")
                .otherwise("Inactive")
            )
            .withColumn("spending_segment",
                F.when(F.col("total_spent") > 1000, "High")
                .when(F.col("total_spent") > 500, "Medium")
                .when(F.col("total_spent") > 0, "Low")
                .otherwise("None")
            )
        )
