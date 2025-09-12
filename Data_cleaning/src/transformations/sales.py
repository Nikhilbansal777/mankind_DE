"""
Sales analytics specific transformations for Redshift schema.
This module contains transformations specifically designed for sales analytics.
"""

from typing import List, Dict, Any, Optional
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from .base import BaseTransformer

class SalesTransformer(BaseTransformer):
    @staticmethod
    def calculate_sales_metrics(df: SparkDataFrame) -> SparkDataFrame:
        """
        Calculate common sales metrics like revenue, profit margins, etc.
        
        Args:
            df: Input DataFrame with sales data
            
        Returns:
            DataFrame with additional sales metrics
        """
        calculations = {
            "total_revenue": "quantity * unit_price",
            "gross_profit": "total_revenue - (quantity * unit_cost)",
            "profit_margin": "ROUND((gross_profit / total_revenue) * 100, 2)",
        }
        return SalesTransformer.enrich_with_calculations(df, calculations)

    @staticmethod
    def create_sales_summary(df: SparkDataFrame) -> SparkDataFrame:
        """
        Create a summary of sales by various dimensions.
        
        Args:
            df: Input DataFrame with sales data
            
        Returns:
            Summarized sales DataFrame
        """
        group_by_cols = ["product_id", "customer_id", "date"]
        agg_expressions = {
            "total_quantity": "SUM(quantity)",
            "total_revenue": "SUM(total_revenue)",
            "total_profit": "SUM(gross_profit)",
            "avg_profit_margin": "AVG(profit_margin)"
        }
        return SalesTransformer.aggregate_data(df, group_by_cols, agg_expressions)

    @staticmethod
    def add_time_based_metrics(df: SparkDataFrame) -> SparkDataFrame:
        """
        Add time-based sales metrics like moving averages and growth rates.
        
        Args:
            df: Input DataFrame with sales data
            
        Returns:
            DataFrame with time-based metrics
        """
        window_exprs = {
            "rolling_avg_revenue": "AVG(total_revenue) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)",
            "revenue_rank": "RANK() OVER (ORDER BY total_revenue DESC)",
            "prev_period_revenue": "LAG(total_revenue, 1) OVER (ORDER BY date)",
            "revenue_growth": "((total_revenue - LAG(total_revenue, 1) OVER (ORDER BY date)) / LAG(total_revenue, 1) OVER (ORDER BY date)) * 100"
        }
        return df.withColumn("date", F.to_date("date")).select("*", *[
            F.expr(expr).alias(col) for col, expr in window_exprs.items()
        ])
