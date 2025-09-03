# MKM_Data_Profiling/profilers/profilers_common/null_counts.py



#--new script------------
# from pyspark.sql import DataFrame
# from pyspark.sql.functions import col, sum as spark_sum, when

# def get_null_counts(df: DataFrame) -> dict:
#     """
#     Returns a mapping of column -> null count for the given DataFrame.
#     Example: {"id": 0, "email": 3, "price": 10}
#     """
#     if df is None:
#         raise ValueError("[ERROR] No DataFrame provided to get_null_counts")

#     exprs = [spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns]
#     return df.select(exprs).first().asDict()



from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as spark_sum, when

def get_null_counts(df: DataFrame) -> dict:
    """
    Returns a dictionary with the count of nulls per column.
    """
    null_counts = (
        df.select([
            spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
            for c in df.columns
        ])
        .first()
        .asDict()
    )
    return {"null_counts": null_counts}


# def get_null_counts(df):
#     null_exprs = [
#         _sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
#         for c in df.columns
#     ]
#     null_counts_row = df.agg(*null_exprs).collect()[0].asDict()
#     return null_counts_row
