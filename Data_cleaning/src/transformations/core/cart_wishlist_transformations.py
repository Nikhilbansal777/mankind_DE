"""
Cart and Wishlist specific transformations focusing on user shopping behavior.
"""
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

class CartWishlistTransformations:
    @staticmethod
    def analyze_cart_abandonment(
        cart_items_df: SparkDataFrame,
        orders_df: SparkDataFrame,
        order_items_df: SparkDataFrame
    ) -> SparkDataFrame:
        """
        Analyze cart abandonment patterns.
        """
        # Get successful purchases
        purchases = (
            order_items_df
            .join(orders_df, "order_id")
            .select("user_id", "product_id", "created_at")
            .withColumnRenamed("created_at", "purchase_time")
        )
        
        # Analyze cart items
        cart_analysis = (
            cart_items_df
            .join(purchases, ["user_id", "product_id"], "left")
            .withColumn(
                "was_purchased",
                F.when(F.col("purchase_time").isNotNull(), True).otherwise(False)
            )
            .withColumn(
                "time_to_purchase",
                F.when(F.col("purchase_time").isNotNull(),
                    F.unix_timestamp("purchase_time") - F.unix_timestamp("created_at"))
                .otherwise(None)
            )
        )
        
        return cart_analysis.groupBy("user_id", "product_id").agg(
            F.sum(F.when(F.col("was_purchased"), 1).otherwise(0)).alias("times_purchased"),
            F.sum(F.when(F.col("was_purchased").isNull(), 1).otherwise(0)).alias("times_abandoned"),
            F.avg("time_to_purchase").alias("avg_time_to_purchase")
        )
    
    @staticmethod
    def analyze_wishlist_conversion(
        wishlist_df: SparkDataFrame,
        orders_df: SparkDataFrame,
        order_items_df: SparkDataFrame
    ) -> SparkDataFrame:
        """
        Analyze wishlist to purchase conversion patterns.
        """
        # Get purchase information
        purchases = (
            order_items_df
            .join(orders_df, "order_id")
            .select("user_id", "product_id", "created_at")
            .withColumnRenamed("created_at", "purchase_time")
        )
        
        # Analyze wishlist items
        wishlist_analysis = (
            wishlist_df
            .join(purchases, ["user_id", "product_id"], "left")
            .withColumn(
                "was_purchased",
                F.when(F.col("purchase_time").isNotNull(), True).otherwise(False)
            )
            .withColumn(
                "time_to_purchase",
                F.when(F.col("purchase_time").isNotNull(),
                    F.unix_timestamp("purchase_time") - F.unix_timestamp("created_at"))
                .otherwise(None)
            )
        )
        
        return wishlist_analysis.groupBy("user_id", "product_id").agg(
            F.count("*").alias("wishlist_adds"),
            F.sum(F.when(F.col("was_purchased"), 1).otherwise(0)).alias("purchases_from_wishlist"),
            F.avg("time_to_purchase").alias("avg_time_to_purchase")
        )
    
    @staticmethod
    def analyze_shopping_patterns(
        cart_items_df: SparkDataFrame,
        wishlist_df: SparkDataFrame,
        orders_df: SparkDataFrame,
        order_items_df: SparkDataFrame
    ) -> SparkDataFrame:
        """
        Analyze user shopping patterns across cart and wishlist.
        """
        # Cart patterns
        cart_patterns = cart_items_df.groupBy("user_id").agg(
            F.count("*").alias("cart_additions"),
            F.countDistinct("product_id").alias("unique_cart_products"),
            F.avg("quantity").alias("avg_cart_quantity")
        )
        
        # Wishlist patterns
        wishlist_patterns = wishlist_df.groupBy("user_id").agg(
            F.count("*").alias("wishlist_additions"),
            F.countDistinct("product_id").alias("unique_wishlist_products")
        )
        
        # Purchase patterns
        purchase_patterns = (
            order_items_df
            .join(orders_df, "order_id")
            .groupBy("user_id")
            .agg(
                F.count("order_id").alias("orders"),
                F.countDistinct("product_id").alias("unique_purchased_products"),
                F.avg("quantity").alias("avg_purchase_quantity")
            )
        )
        
        # Combine all patterns
        return (
            cart_patterns
            .join(wishlist_patterns, "user_id", "left")
            .join(purchase_patterns, "user_id", "left")
            .withColumn(
                "cart_to_purchase_ratio",
                F.col("orders") / F.col("cart_additions")
            )
            .withColumn(
                "wishlist_to_purchase_ratio",
                F.col("orders") / F.col("wishlist_additions")
            )
        )
