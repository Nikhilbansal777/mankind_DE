"""
Product-centric transformations focusing on product performance and inventory.
"""
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

class ProductTransformations:
    @staticmethod
    def analyze_product_performance(
        products_df: SparkDataFrame,
        order_items_df: SparkDataFrame
    ) -> SparkDataFrame:
        """
        Analyze product performance based on order history.
        """
        sales_metrics = order_items_df.groupBy("product_id").agg(
            F.count("order_id").alias("total_orders"),
            F.sum("quantity").alias("total_units_sold"),
            F.sum(F.col("quantity") * F.col("price")).alias("total_revenue"),
            F.avg("quantity").alias("avg_quantity_per_order")
        )
        
        return (
            products_df
            .join(sales_metrics, "product_id", "left")
            .withColumn(
                "revenue_rank",
                F.rank().over(Window.orderBy(F.desc("total_revenue")))
            )
        )
    
    @staticmethod
    def analyze_product_interactions(
        products_df: SparkDataFrame,
        cart_items_df: SparkDataFrame,
        wishlist_df: SparkDataFrame,
        order_items_df: SparkDataFrame
    ) -> SparkDataFrame:
        """
        Analyze how users interact with products across cart, wishlist, and orders.
        """
        cart_metrics = cart_items_df.groupBy("product_id").agg(
            F.count("*").alias("cart_adds"),
            F.countDistinct("user_id").alias("unique_cart_users")
        )
        
        wishlist_metrics = wishlist_df.groupBy("product_id").agg(
            F.count("*").alias("wishlist_adds"),
            F.countDistinct("user_id").alias("unique_wishlist_users")
        )
        
        order_metrics = order_items_df.groupBy("product_id").agg(
            F.count("order_id").alias("successful_purchases"),
            F.countDistinct("user_id").alias("unique_buyers")
        )
        
        return (
            products_df
            .join(cart_metrics, "product_id", "left")
            .join(wishlist_metrics, "product_id", "left")
            .join(order_metrics, "product_id", "left")
            .withColumn(
                "cart_to_purchase_ratio",
                F.col("successful_purchases") / F.col("cart_adds")
            )
            .withColumn(
                "wishlist_to_purchase_ratio",
                F.col("successful_purchases") / F.col("wishlist_adds")
            )
        )
    
    @staticmethod
    def analyze_category_performance(
        products_df: SparkDataFrame,
        order_items_df: SparkDataFrame
    ) -> SparkDataFrame:
        """
        Analyze performance metrics by product category.
        """
        category_metrics = (
            products_df
            .join(order_items_df, "product_id")
            .groupBy("category")
            .agg(
                F.count("order_id").alias("category_orders"),
                F.sum("quantity").alias("category_units_sold"),
                F.sum(F.col("quantity") * F.col("price")).alias("category_revenue"),
                F.countDistinct("product_id").alias("unique_products")
            )
        )
        
        window_spec = Window.orderBy(F.desc("category_revenue"))
        
        return (
            category_metrics
            .withColumn("category_rank", F.rank().over(window_spec))
            .withColumn(
                "revenue_share",
                F.col("category_revenue") / F.sum("category_revenue").over(Window.partitionBy())
            )
        )
    
    @staticmethod
    def get_product_affinity(
        order_items_df: SparkDataFrame,
        products_df: SparkDataFrame,
        min_support: float = 0.01
    ) -> SparkDataFrame:
        """
        Analyze which products are commonly bought together.
        """
        # Get total number of orders for support calculation
        total_orders = order_items_df.select("order_id").distinct().count()
        
        # Self-join to find product pairs
        items1 = order_items_df.select("order_id", "product_id").alias("items1")
        items2 = order_items_df.select("order_id", "product_id").alias("items2")
        
        product_pairs = (
            items1
            .join(items2, "order_id")
            .where("items1.product_id < items2.product_id")
            .groupBy("items1.product_id", "items2.product_id")
            .agg(F.count("*").alias("pair_frequency"))
            .where(F.col("pair_frequency") / total_orders >= min_support)
        )
        
        return (
            product_pairs
            .join(
                products_df.select("product_id", "product_name"),
                product_pairs.product_id == products_df.product_id
            )
            .withColumnRenamed("product_name", "product1_name")
            .join(
                products_df.select("product_id", "product_name"),
                product_pairs.product_id == products_df.product_id
            )
            .withColumnRenamed("product_name", "product2_name")
            .select(
                "product1_name", "product2_name",
                "pair_frequency",
                (F.col("pair_frequency") / total_orders).alias("support")
            )
        )
