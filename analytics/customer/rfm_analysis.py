from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.settings import ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("DineIQ RFM Analysis") \
    .getOrCreate()


customer_segmentation_folder = f"{ANALYTICS_DATA_FOLDER}/customer_segmentation"
output_folder = f"{ANALYTICS_DATA_FOLDER}/rfm_analysis"


customer_segments = spark.read.parquet(
    f"{customer_segmentation_folder}/customer_segments"
)



rfm = customer_segments.select(
    "customer_id",
    F.col("customer_recency").alias("recency"),
    F.col("customer_frequency").alias("frequency"),
    F.col("customer_monetary_value").alias("monetary_value"),
    "customer_segment"
)



recency_window = Window.orderBy(
    F.col("recency").desc(),
    F.col("customer_id")
)

frequency_window = Window.orderBy(
    F.col("frequency").asc(),
    F.col("customer_id")
)

monetary_window = Window.orderBy(
    F.col("monetary_value").asc(),
    F.col("customer_id")
)


rfm = rfm.withColumn(
    "recency_score",
    F.ntile(5).over(recency_window)
).withColumn(
    "frequency_score",
    F.ntile(5).over(frequency_window)
).withColumn(
    "monetary_score",
    F.ntile(5).over(monetary_window)
)



rfm = rfm.withColumn(
    "rfm_score",
    (
        F.col("recency_score") +
        F.col("frequency_score") +
        F.col("monetary_score")
    )
).withColumn(
    "rfm_code",
    F.concat(
        F.col("recency_score").cast("string"),
        F.col("frequency_score").cast("string"),
        F.col("monetary_score").cast("string")
    )
)



rfm = rfm.withColumn(
    "rfm_value_group",
    F.when(
        F.col("rfm_score") >= 13,
        "Top Value"
    ).when(
        F.col("rfm_score") >= 10,
        "High Value"
    ).when(
        F.col("rfm_score") >= 7,
        "Medium Value"
    ).otherwise(
        "Low Value"
    )
)


rfm_summary = rfm.groupBy(
    "rfm_value_group"
).agg(
    F.count("*").alias("customer_count"),
    F.round(F.avg("recency"), 2).alias("average_recency"),
    F.round(F.avg("frequency"), 2).alias("average_frequency"),
    F.round(F.avg("monetary_value"), 2).alias("average_monetary_value"),
    F.round(F.avg("rfm_score"), 2).alias("average_rfm_score")
)


rfm_segment_summary = rfm.groupBy(
    "customer_segment"
).agg(
    F.count("*").alias("customer_count"),
    F.round(F.avg("recency_score"), 2).alias("average_recency_score"),
    F.round(F.avg("frequency_score"), 2).alias("average_frequency_score"),
    F.round(F.avg("monetary_score"), 2).alias("average_monetary_score"),
    F.round(F.avg("rfm_score"), 2).alias("average_rfm_score")
)



print("\n========RFM VALUE GROUPS========")
rfm_summary.orderBy(
    F.desc("average_rfm_score")
).show(
    truncate=False
)


print("\n========RFM BY CUSTOMER SEGMENT========")
rfm_segment_summary.orderBy(
    F.desc("average_rfm_score")
).show(
    truncate=False
)


print("\n========SAMPLE RFM CUSTOMERS========")
rfm.orderBy(
    F.desc("rfm_score"),
    F.asc("customer_id")
).show(
    20,
    truncate=False
)


rfm.write.mode("overwrite").parquet(
    f"{output_folder}/customer_rfm"
)


rfm_summary.write.mode("overwrite").parquet(
    f"{output_folder}/rfm_summary"
)


rfm_segment_summary.write.mode("overwrite").parquet(
    f"{output_folder}/rfm_segment_summary"
)


print("\nRFM analysis completed successfully.")


spark.stop()
