from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import INTEGRATED_DATA_FOLDER, ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("DineIQ Peak Period Analysis") \
    .getOrCreate()


integrated_folder = INTEGRATED_DATA_FOLDER
output_folder = f"{ANALYTICS_DATA_FOLDER}/peak_period"


transactions = spark.read.parquet(
    f"{integrated_folder}/transactions"
)


order_level = transactions.select(
    "order_id",
    "restaurant_id",
    "restaurant_name",
    "order_datetime",
    "ordering_channel",
    "total_amount"
).dropDuplicates(
    ["order_id"]
)

hourly_orders = order_level.withColumn(
    "order_hour",
    F.hour("order_datetime")
)


hourly_order_counts = hourly_orders.groupBy(
    "order_hour"
).agg(
    F.countDistinct("order_id").alias("order_count"),
    F.round(F.sum("total_amount"), 2).alias("total_revenue"),
    F.round(F.avg("total_amount"), 2).alias("average_order_value")
)


total_orders = order_level.select(
    "order_id"
).distinct().count()


hourly_patterns = hourly_order_counts.withColumn(
    "order_percentage",
    F.round(
        (
            F.col("order_count") /
            F.lit(total_orders)
        ) * 100,
        2
    )
).orderBy(
    "order_hour"
)


daily_orders = order_level.withColumn(
    "day_of_week_number",
    F.dayofweek("order_datetime")
).withColumn(
    "day_of_week",
    F.date_format("order_datetime", "EEEE")
)


daily_order_counts = daily_orders.groupBy(
    "day_of_week_number",
    "day_of_week"
).agg(
    F.countDistinct("order_id").alias("order_count"),
    F.round(F.sum("total_amount"), 2).alias("total_revenue"),
    F.round(F.avg("total_amount"), 2).alias("average_order_value")
)


daily_patterns = daily_order_counts.withColumn(
    "order_percentage",
    F.round(
        (
            F.col("order_count") /
            F.lit(total_orders)
        ) * 100,
        2
    )
).orderBy(
    "day_of_week_number"
)


weekend_orders = daily_orders.withColumn(
    "period_type",
    F.when(
        F.col("day_of_week").isin("Friday", "Saturday", "Sunday"),
        "Weekend"
    ).otherwise(
        "Weekday"
    )
)


weekend_order_counts = weekend_orders.groupBy(
    "period_type"
).agg(
    F.countDistinct("order_id").alias("order_count"),
    F.round(F.sum("total_amount"), 2).alias("total_revenue"),
    F.round(F.avg("total_amount"), 2).alias("average_order_value")
)


weekend_patterns = weekend_order_counts.withColumn(
    "days_in_period",
    F.when(
        F.col("period_type") == "Weekend",
        3
    ).otherwise(
        4
    )
).withColumn(
    "average_orders_per_day",
    F.round(
        F.col("order_count") /
        F.col("days_in_period"),
        2
    )
).withColumn(
    "order_percentage",
    F.round(
        (
            F.col("order_count") /
            F.lit(total_orders)
        ) * 100,
        2
    )
).orderBy(
    F.desc("average_orders_per_day")
)


monthly_orders = order_level.withColumn(
    "year_month",
    F.date_format("order_datetime", "yyyy-MM")
)


monthly_order_counts = monthly_orders.groupBy(
    "year_month"
).agg(
    F.countDistinct("order_id").alias("order_count"),
    F.round(F.sum("total_amount"), 2).alias("total_revenue"),
    F.round(F.avg("total_amount"), 2).alias("average_order_value")
)


monthly_trends = monthly_order_counts.withColumn(
    "order_percentage",
    F.round(
        (
            F.col("order_count") /
            F.lit(total_orders)
        ) * 100,
        2
    )
).orderBy(
    "year_month"
)


seasonal_orders = order_level.withColumn(
    "order_month",
    F.month("order_datetime")
).withColumn(
    "season",
    F.when(
        F.col("order_month").isin(12, 1, 2),
        "Winter"
    ).when(
        F.col("order_month").isin(3, 4, 5),
        "Spring"
    ).when(
        F.col("order_month").isin(6, 7, 8),
        "Summer"
    ).otherwise(
        "Autumn"
    )
).withColumn(
    "year_month",
    F.date_format("order_datetime", "yyyy-MM")
)


seasonal_order_counts = seasonal_orders.groupBy(
    "season"
).agg(
    F.countDistinct("order_id").alias("order_count"),
    F.round(F.sum("total_amount"), 2).alias("total_revenue"),
    F.round(F.avg("total_amount"), 2).alias("average_order_value"),
    F.countDistinct("year_month").alias("months_in_period")
)


seasonal_trends = seasonal_order_counts.withColumn(
    "average_orders_per_month",
    F.round(
        F.col("order_count") /
        F.col("months_in_period"),
        2
    )
).withColumn(
    "average_revenue_per_month",
    F.round(
        F.col("total_revenue") /
        F.col("months_in_period"),
        2
    )
).withColumn(
    "order_percentage",
    F.round(
        (
            F.col("order_count") /
            F.lit(total_orders)
        ) * 100,
        2
    )
).orderBy(
    F.desc("average_orders_per_month")
)


location_orders = order_level.withColumn(
    "order_hour",
    F.hour("order_datetime")
).withColumn(
    "day_of_week_number",
    F.dayofweek("order_datetime")
).withColumn(
    "day_of_week",
    F.date_format("order_datetime", "EEEE")
)


location_hour_counts = location_orders.groupBy(
    "restaurant_id",
    "restaurant_name",
    "order_hour"
).agg(
    F.countDistinct("order_id").alias("order_count"),
    F.round(F.sum("total_amount"), 2).alias("total_revenue")
)


location_hour_window = Window.partitionBy(
    "restaurant_id"
).orderBy(
    F.desc("order_count"),
    F.asc("order_hour")
)


location_peak_hours = location_hour_counts.withColumn(
    "peak_rank",
    F.row_number().over(location_hour_window)
).filter(
    F.col("peak_rank") == 1
).select(
    "restaurant_id",
    "restaurant_name",
    F.col("order_hour").alias("peak_hour"),
    F.col("order_count").alias("peak_hour_orders"),
    F.col("total_revenue").alias("peak_hour_revenue")
)


location_day_counts = location_orders.groupBy(
    "restaurant_id",
    "restaurant_name",
    "day_of_week_number",
    "day_of_week"
).agg(
    F.countDistinct("order_id").alias("order_count"),
    F.round(F.sum("total_amount"), 2).alias("total_revenue")
)


location_day_window = Window.partitionBy(
    "restaurant_id"
).orderBy(
    F.desc("order_count"),
    F.asc("day_of_week_number")
)


location_peak_days = location_day_counts.withColumn(
    "peak_rank",
    F.row_number().over(location_day_window)
).filter(
    F.col("peak_rank") == 1
).select(
    "restaurant_id",
    F.col("day_of_week").alias("peak_day"),
    F.col("order_count").alias("peak_day_orders"),
    F.col("total_revenue").alias("peak_day_revenue")
)


location_peaks = location_peak_hours.join(
    location_peak_days,
    on="restaurant_id",
    how="inner"
).orderBy(
    "restaurant_id"
)


channel_orders = order_level.withColumn(
    "order_hour",
    F.hour("order_datetime")
).withColumn(
    "day_of_week_number",
    F.dayofweek("order_datetime")
).withColumn(
    "day_of_week",
    F.date_format("order_datetime", "EEEE")
)


channel_hour_counts = channel_orders.groupBy(
    "ordering_channel",
    "order_hour"
).agg(
    F.countDistinct("order_id").alias("order_count"),
    F.round(F.sum("total_amount"), 2).alias("total_revenue")
)


channel_hour_window = Window.partitionBy(
    "ordering_channel"
).orderBy(
    F.desc("order_count"),
    F.asc("order_hour")
)


channel_peak_hours = channel_hour_counts.withColumn(
    "peak_rank",
    F.row_number().over(channel_hour_window)
).filter(
    F.col("peak_rank") == 1
).select(
    "ordering_channel",
    F.col("order_hour").alias("peak_hour"),
    F.col("order_count").alias("peak_hour_orders"),
    F.col("total_revenue").alias("peak_hour_revenue")
)


channel_day_counts = channel_orders.groupBy(
    "ordering_channel",
    "day_of_week_number",
    "day_of_week"
).agg(
    F.countDistinct("order_id").alias("order_count"),
    F.round(F.sum("total_amount"), 2).alias("total_revenue")
)


channel_day_window = Window.partitionBy(
    "ordering_channel"
).orderBy(
    F.desc("order_count"),
    F.asc("day_of_week_number")
)


channel_peak_days = channel_day_counts.withColumn(
    "peak_rank",
    F.row_number().over(channel_day_window)
).filter(
    F.col("peak_rank") == 1
).select(
    "ordering_channel",
    F.col("day_of_week").alias("peak_day"),
    F.col("order_count").alias("peak_day_orders"),
    F.col("total_revenue").alias("peak_day_revenue")
)


channel_peaks = channel_peak_hours.join(
    channel_peak_days,
    on="ordering_channel",
    how="inner"
).orderBy(
    "ordering_channel"
)


print("\n========HOURLY ORDER PATTERNS========")
hourly_patterns.select(
    "order_hour",
    "order_count",
    F.format_number(
        "total_revenue",
        2
    ).alias("total_revenue"),
    "average_order_value",
    "order_percentage"
).show(
    24,
    truncate=False
)

hourly_patterns.write.mode("overwrite").parquet(
    f"{output_folder}/hourly_patterns"
)

print("\nHourly pattern analysis completed successfully.")


print("\n========DAILY ORDER PATTERNS========")
daily_patterns.select(
    "day_of_week",
    "order_count",
    F.format_number(
        "total_revenue",
        2
    ).alias("total_revenue"),
    "average_order_value",
    "order_percentage"
).show(
    7,
    truncate=False
)

daily_patterns.write.mode("overwrite").parquet(
    f"{output_folder}/daily_patterns"
)

print("\nPeak period analysis completed successfully.")


print("\n========WEEKEND ORDER PATTERNS========")
weekend_patterns.select(
    "period_type",
    "order_count",
    F.format_number(
        "total_revenue",
        2
    ).alias("total_revenue"),
    "average_order_value",
    "days_in_period",
    "average_orders_per_day",
    "order_percentage"
).show(
    truncate=False
)

weekend_patterns.write.mode("overwrite").parquet(
    f"{output_folder}/weekend_patterns"
)


print("\n========MONTHLY ORDER TRENDS========")
monthly_trends.select(
    "year_month",
    "order_count",
    F.format_number(
        "total_revenue",
        2
    ).alias("total_revenue"),
    "average_order_value",
    "order_percentage"
).show(
    truncate=False
)

monthly_trends.write.mode("overwrite").parquet(
    f"{output_folder}/monthly_trends"
)


print("\n========SEASONAL ORDER TRENDS========")
seasonal_trends.select(
    "season",
    "order_count",
    F.format_number(
        "total_revenue",
        2
    ).alias("total_revenue"),
    "average_order_value",
    "months_in_period",
    "average_orders_per_month",
    F.format_number(
        "average_revenue_per_month",
        2
    ).alias("average_revenue_per_month"),
    "order_percentage"
).show(
    truncate=False
)

seasonal_trends.write.mode("overwrite").parquet(
    f"{output_folder}/seasonal_trends"
)


print("\n========LOCATION-SPECIFIC PEAKS========")
location_peaks.select(
    "restaurant_id",
    "restaurant_name",
    "peak_hour",
    "peak_hour_orders",
    F.format_number(
        "peak_hour_revenue",
        2
    ).alias("peak_hour_revenue"),
    "peak_day",
    "peak_day_orders",
    F.format_number(
        "peak_day_revenue",
        2
    ).alias("peak_day_revenue")
).show(
    20,
    truncate=False
)

location_peaks.write.mode("overwrite").parquet(
    f"{output_folder}/location_peaks"
)


print("\n========CHANNEL ORDER PEAKS========")
channel_peaks.select(
    "ordering_channel",
    "peak_hour",
    "peak_hour_orders",
    F.format_number(
        "peak_hour_revenue",
        2
    ).alias("peak_hour_revenue"),
    "peak_day",
    "peak_day_orders",
    F.format_number(
        "peak_day_revenue",
        2
    ).alias("peak_day_revenue")
).show(
    truncate=False
)

channel_peaks.write.mode("overwrite").parquet(
    f"{output_folder}/channel_peaks"
)


spark.stop()