"""
Base transformation operations and utilities.
This module contains core transformation functions that are domain-agnostic.
"""

from typing import List, Dict, Any, Optional
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

class BaseTransformer:
    @staticmethod
    def join_dataframes(
        left_df: SparkDataFrame,
        right_df: SparkDataFrame,
        join_keys: List[str],
        join_type: str = "inner"
    ) -> SparkDataFrame:
        """
        Join two Spark DataFrames based on specified keys.
        
        Args:
            left_df: Left DataFrame
            right_df: Right DataFrame
            join_keys: List of column names to join on
            join_type: Type of join (inner, left, right, full)
            
        Returns:
            Joined DataFrame
        """
        return left_df.join(right_df, join_keys, join_type)

    @staticmethod
    def enrich_with_calculations(
        df: SparkDataFrame,
        calculations: Dict[str, str]
    ) -> SparkDataFrame:
        """
        Add calculated columns to a DataFrame.
        
        Args:
            df: Input DataFrame
            calculations: Dictionary mapping new column names to their SQL expressions
            
        Returns:
            Enriched DataFrame
        """
        for col_name, expression in calculations.items():
            df = df.withColumn(col_name, F.expr(expression))
        return df

    @staticmethod
    def aggregate_data(
        df: SparkDataFrame,
        group_by_cols: List[str],
        agg_expressions: Dict[str, str]
    ) -> SparkDataFrame:
        """
        Perform aggregation operations on DataFrame.
        
        Args:
            df: Input DataFrame
            group_by_cols: Columns to group by
            agg_expressions: Dictionary mapping output columns to aggregation expressions
            
        Returns:
            Aggregated DataFrame
        """
        return df.groupBy(group_by_cols).agg(
            *[F.expr(expr).alias(col) for col, expr in agg_expressions.items()]
        )

    @staticmethod
    def window_calculations(
        df: SparkDataFrame,
        partition_cols: List[str],
        order_cols: List[str],
        window_exprs: Dict[str, str]
    ) -> SparkDataFrame:
        """
        Add window function calculations to DataFrame.
        
        Args:
            df: Input DataFrame
            partition_cols: Columns to partition by
            order_cols: Columns to order by
            window_exprs: Dictionary mapping new column names to window expressions
            
        Returns:
            DataFrame with window calculations
        """
        window_spec = (
            F.Window()
            .partitionBy(partition_cols)
            .orderBy(order_cols)
        )
        
        for col_name, expression in window_exprs.items():
            df = df.withColumn(col_name, F.expr(expression).over(window_spec))
        return df
