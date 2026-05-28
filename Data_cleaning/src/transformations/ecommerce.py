"""
E-commerce specific transformations for the core tables (users, products, cart items, wishlist, payments).
This module contains transformations specifically designed for e-commerce data analysis.
"""

from typing import List, Dict, Any, Optional
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from .base import BaseTransformer

class EcommerceTransformer(BaseTransformer):
    @staticmethod
    def enrich_user_profile(
        users_df: SparkDataFrame,
        orders_df: SparkDataFrame,
        cart_df: SparkDataFrame,
        wishlist_df: SparkDataFrame
    ) -> SparkDataFrame:
        """
        Enrich user profiles with their shopping behavior.
        
        Args:
            users_df: Users DataFrame
            orders_df: Orders DataFrame
            cart_df: Cart items DataFrame
            wishlist_df: Wishlist DataFrame
            
        Returns:
            Enriched users DataFrame
        """
        # Aggregate cart behavior
        cart_aggs = {
            "total_cart_items": "COUNT(DISTINCT product_id)",
            "total_cart_value": "SUM(quantity * price)",
            "avg_cart_value": "AVG(quantity * price)"
        }
        cart_summary = EcommerceTransformer.aggregate_data(
            cart_df, ["user_id"], cart_aggs
        )

        # Aggregate wishlist behavior
        wishlist_aggs = {
            "total_wishlist_items": "COUNT(DISTINCT product_id)",
            "wishlist_total_value": "SUM(price)"
        }
        wishlist_summary = EcommerceTransformer.aggregate_data(
            wishlist_df, ["user_id"], wishlist_aggs
        )

        # Join all summaries with user profile
        enriched_df = users_df
        for df in [cart_summary, wishlist_summary]:
            enriched_df = EcommerceTransformer.join_dataframes(
                enriched_df, df, ["user_id"], "left"
            )

        return enriched_df

    @staticmethod
    def create_product_insights(
        products_df: SparkDataFrame,
        cart_df: SparkDataFrame,
        wishlist_df: SparkDataFrame
    ) -> SparkDataFrame:
        """
        Create product insights by combining cart and wishlist data.
        
        Args:
            products_df: Products DataFrame
            cart_df: Cart items DataFrame
            wishlist_df: Wishlist DataFrame
            
        Returns:
            Products DataFrame with added insights
        """
        # Calculate product popularity metrics
        cart_metrics = {
            "cart_frequency": "COUNT(DISTINCT user_id)",
            "total_quantity_carted": "SUM(quantity)",
            "cart_to_wishlist_ratio": "COUNT(DISTINCT user_id) / COUNT(DISTINCT wishlist_df.user_id)"
        }
        cart_summary = EcommerceTransformer.aggregate_data(
            cart_df, ["product_id"], cart_metrics
        )

        wishlist_metrics = {
            "wishlist_frequency": "COUNT(DISTINCT user_id)",
            "wishlist_value": "SUM(price)"
        }
        wishlist_summary = EcommerceTransformer.aggregate_data(
            wishlist_df, ["product_id"], wishlist_metrics
        )

        # Combine all metrics with product data
        enriched_products = products_df
        for df in [cart_summary, wishlist_summary]:
            enriched_products = EcommerceTransformer.join_dataframes(
                enriched_products, df, ["product_id"], "left"
            )

        # Add derived metrics
        calculations = {
            "popularity_score": "(COALESCE(cart_frequency, 0) * 2 + COALESCE(wishlist_frequency, 0)) / 3",
            "conversion_rate": "CASE WHEN wishlist_frequency > 0 THEN COALESCE(cart_frequency, 0) / wishlist_frequency ELSE NULL END"
        }
        return EcommerceTransformer.enrich_with_calculations(enriched_products, calculations)

    @staticmethod
    def analyze_user_product_affinity(
        cart_df: SparkDataFrame,
        wishlist_df: SparkDataFrame
    ) -> SparkDataFrame:
        """
        Create user-product affinity analysis.
        
        Args:
            cart_df: Cart items DataFrame
            wishlist_df: Wishlist DataFrame
            
        Returns:
            User-product affinity DataFrame
        """
        # Combine cart and wishlist actions
        cart_actions = cart_df.select(
            "user_id", "product_id", 
            F.lit("cart").alias("action_type"),
            "timestamp", "quantity", "price"
        )
        wishlist_actions = wishlist_df.select(
            "user_id", "product_id", 
            F.lit("wishlist").alias("action_type"),
            "timestamp", F.lit(1).alias("quantity"), "price"
        )
        all_actions = cart_actions.union(wishlist_actions)

        # Calculate user-product affinity metrics
        window_exprs = {
            "action_sequence": "ROW_NUMBER() OVER (PARTITION BY user_id, product_id ORDER BY timestamp)",
            "days_since_first_action": "DATEDIFF(timestamp, FIRST_VALUE(timestamp) OVER (PARTITION BY user_id, product_id ORDER BY timestamp))",
            "total_actions": "COUNT(*) OVER (PARTITION BY user_id, product_id)"
        }
        
        return EcommerceTransformer.window_calculations(
            all_actions,
            ["user_id", "product_id"],
            ["timestamp"],
            window_exprs
        )

    @staticmethod
    def create_payment_analytics(
        payments_df: SparkDataFrame,
        users_df: SparkDataFrame
    ) -> SparkDataFrame:
        """
        Create payment analytics with user segments.
        
        Args:
            payments_df: Payments DataFrame
            users_df: Users DataFrame
            
        Returns:
            Payment analytics DataFrame
        """
        # Calculate payment metrics per user
        payment_metrics = {
            "total_payments": "COUNT(*)",
            "total_amount": "SUM(amount)",
            "avg_payment": "AVG(amount)",
            "payment_frequency": "COUNT(*) / (DATEDIFF(MAX(timestamp), MIN(timestamp)) + 1)"
        }
        user_payments = EcommerceTransformer.aggregate_data(
            payments_df, ["user_id"], payment_metrics
        )

        # Add user segments based on payment behavior
        calculations = {
            "user_segment": """
                CASE 
                    WHEN total_amount > 1000 AND payment_frequency > 0.5 THEN 'High Value'
                    WHEN total_amount > 500 OR payment_frequency > 0.3 THEN 'Medium Value'
                    ELSE 'Low Value'
                END
            """
        }
        segmented_payments = EcommerceTransformer.enrich_with_calculations(
            user_payments, calculations
        )

        # Join with user data
        return EcommerceTransformer.join_dataframes(
            users_df, segmented_payments, ["user_id"], "left"
        )
