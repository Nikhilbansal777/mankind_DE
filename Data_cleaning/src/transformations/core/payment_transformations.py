"""
Payment and Order Payment specific transformations.
"""
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

class PaymentTransformations:
    @staticmethod
    def analyze_payment_methods(
        payments_df: SparkDataFrame,
        order_payments_df: SparkDataFrame,
        orders_df: SparkDataFrame
    ) -> SparkDataFrame:
        """
        Analyze payment method usage and success rates.
        """
        payment_analysis = (
            order_payments_df
            .join(payments_df, "payment_id")
            .join(orders_df, "order_id")
            .groupBy("payment_type")
            .agg(
                F.count("*").alias("total_attempts"),
                F.sum(F.when(F.col("status") == "success", 1).otherwise(0)).alias("successful_payments"),
                F.sum(F.when(F.col("status") != "success", 1).otherwise(0)).alias("failed_payments"),
                F.avg(F.when(F.col("status") == "success", 1).otherwise(0)).alias("success_rate"),
                F.sum("amount").alias("total_amount_processed"),
                F.avg("amount").alias("avg_transaction_amount")
            )
        )
        
        window_spec = Window.orderBy(F.desc("total_amount_processed"))
        
        return (
            payment_analysis
            .withColumn("volume_rank", F.rank().over(window_spec))
            .withColumn(
                "payment_share",
                F.col("total_amount_processed") / F.sum("total_amount_processed").over(Window.partitionBy())
            )
        )

    @staticmethod
    def analyze_payment_patterns(
        payments_df: SparkDataFrame,
        order_payments_df: SparkDataFrame,
        users_df: SparkDataFrame
    ) -> SparkDataFrame:
        """
        Analyze user payment patterns and preferences.
        """
        user_payments = (
            order_payments_df
            .join(payments_df, "payment_id")
            .join(users_df, "user_id")
            .groupBy("user_id")
            .agg(
                F.collect_set("payment_type").alias("payment_methods_used"),
                F.size(F.collect_set("payment_type")).alias("num_payment_methods"),
                F.first("payment_type").alias("first_payment_method"),
                F.last("payment_type").alias("last_payment_method"),
                F.count("*").alias("total_payment_attempts"),
                F.sum(F.when(F.col("status") == "success", 1).otherwise(0)).alias("successful_payments"),
                F.avg("amount").alias("avg_payment_amount")
            )
        )
        
        return (
            user_payments
            .withColumn("payment_success_rate", 
                       F.col("successful_payments") / F.col("total_payment_attempts"))
            .withColumn("payment_reliability",
                       F.when(F.col("payment_success_rate") >= 0.95, "High")
                       .when(F.col("payment_success_rate") >= 0.8, "Medium")
                       .otherwise("Low"))
        )

    @staticmethod
    def analyze_payment_failures(
        payments_df: SparkDataFrame,
        order_payments_df: SparkDataFrame
    ) -> SparkDataFrame:
        """
        Analyze payment failure patterns and reasons.
        """
        return (
            order_payments_df
            .join(payments_df, "payment_id")
            .where(F.col("status") != "success")
            .groupBy("payment_type", "error_code", "error_message")
            .agg(
                F.count("*").alias("failure_count"),
                F.avg("amount").alias("avg_failed_amount"),
                F.min("created_at").alias("first_failure"),
                F.max("created_at").alias("last_failure")
            )
            .withColumn(
                "failure_frequency",
                F.col("failure_count") / 
                F.sum("failure_count").over(Window.partitionBy("payment_type"))
            )
        )
