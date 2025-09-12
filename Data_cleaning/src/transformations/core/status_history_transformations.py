"""
Order Status History specific transformations.
"""
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List

class StatusHistoryTransformations:
    @staticmethod
    def analyze_status_transitions(
        status_history_df: SparkDataFrame,
        orders_df: SparkDataFrame
    ) -> SparkDataFrame:
        """
        Analyze status transition patterns and timing.
        """
        window_spec = Window.partitionBy("order_id").orderBy("created_at")
        
        transitions = (
            status_history_df
            .withColumn("prev_status", F.lag("status").over(window_spec))
            .withColumn("next_status", F.lead("status").over(window_spec))
            .withColumn("time_in_status",
                F.unix_timestamp(F.lead("created_at").over(window_spec)) -
                F.unix_timestamp("created_at")
            )
        )
        
        return (
            transitions
            .where(F.col("prev_status").isNotNull())
            .groupBy("prev_status", "status")
            .agg(
                F.count("*").alias("transition_count"),
                F.avg("time_in_status").alias("avg_transition_time"),
                F.min("time_in_status").alias("min_transition_time"),
                F.max("time_in_status").alias("max_transition_time")
            )
        )

    @staticmethod
    def calculate_status_durations(
        status_history_df: SparkDataFrame,
        target_statuses: List[str] = None
    ) -> SparkDataFrame:
        """
        Calculate how long orders spend in each status.
        """
        if target_statuses is None:
            target_statuses = ["processing", "shipped", "delivered"]
            
        window_spec = Window.partitionBy("order_id").orderBy("created_at")
        
        return (
            status_history_df
            .where(F.col("status").isin(target_statuses))
            .withColumn("next_timestamp", 
                       F.lead("created_at").over(window_spec))
            .withColumn("duration_hours",
                       F.round(
                           (F.unix_timestamp("next_timestamp") - 
                            F.unix_timestamp("created_at")) / 3600,
                           2
                       ))
            .groupBy("status")
            .agg(
                F.avg("duration_hours").alias("avg_duration_hours"),
                F.percentile_approx("duration_hours", 0.5).alias("median_duration_hours"),
                F.percentile_approx("duration_hours", 0.9).alias("p90_duration_hours")
            )
        )

    @staticmethod
    def identify_bottleneck_statuses(
        status_history_df: SparkDataFrame,
        threshold_hours: float = 24.0
    ) -> SparkDataFrame:
        """
        Identify status stages that frequently take longer than expected.
        """
        window_spec = Window.partitionBy("order_id").orderBy("created_at")
        
        status_times = (
            status_history_df
            .withColumn("next_timestamp", 
                       F.lead("created_at").over(window_spec))
            .withColumn("duration_hours",
                       (F.unix_timestamp("next_timestamp") - 
                        F.unix_timestamp("created_at")) / 3600)
        )
        
        return (
            status_times
            .where(F.col("duration_hours") > threshold_hours)
            .groupBy("status")
            .agg(
                F.count("*").alias("delayed_transitions"),
                F.avg("duration_hours").alias("avg_delay_hours"),
                F.max("duration_hours").alias("max_delay_hours"),
                F.min("order_id").alias("example_order_id")
            )
            .orderBy(F.desc("delayed_transitions"))
        )

    @staticmethod
    def analyze_status_patterns(
        status_history_df: SparkDataFrame,
        orders_df: SparkDataFrame
    ) -> SparkDataFrame:
        """
        Analyze common status progression patterns.
        """
        # First, get status sequences for each order
        window_spec = Window.partitionBy("order_id").orderBy("created_at")
        
        status_sequences = (
            status_history_df
            .withColumn("seq_num", F.row_number().over(window_spec))
            .groupBy("order_id")
            .agg(
                F.collect_list("status").alias("status_sequence"),
                F.collect_list("created_at").alias("status_timestamps"),
                F.count("*").alias("total_status_changes")
            )
        )
        
        # Join with orders to get more context
        return (
            status_sequences
            .join(orders_df, "order_id")
            .withColumn("total_duration_hours",
                       F.round(
                           (F.unix_timestamp(F.element_at("status_timestamps", -1)) -
                            F.unix_timestamp(F.element_at("status_timestamps", 1))) / 3600,
                           2
                       ))
            .withColumn("first_status", F.element_at("status_sequence", 1))
            .withColumn("last_status", F.element_at("status_sequence", -1))
        )
