from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.settings import INTEGRATED_DATA_FOLDER, ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("DineIQ Ordering Channel Analysis") \
    .getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "8")


transactions = spark.read.parquet(
    f"{INTEGRATED_DATA_FOLDER}/transactions"
)

output_folder = f"{ANALYTICS_DATA_FOLDER}/ordering_channel_analysis"


sales = transactions.filter(
    F.col("order_status") == "Completed"
).withColumn(
    "line_revenue",
    F.col("line_total").cast("double")
).withColumn(
    "line_cost",
    F.col("item_cost").cast("double") *
    F.col("quantity").cast("double")
).withColumn(
    "line_margin",
    F.col("line_revenue") -
    F.col("line_cost")
)


order_level = sales.groupBy(
    "order_id",
    "customer_id",
    "ordering_channel"
).agg(
    F.first(
        "order_datetime",
        ignorenulls=True
    ).alias("order_datetime"),
    F.first(
        "promotion_id",
        ignorenulls=True
    ).alias("promotion_id"),
    F.first(
        "order_subtotal",
        ignorenulls=True
    ).cast("double").alias("order_subtotal"),
    F.first(
        "order_discount_amount",
        ignorenulls=True
    ).cast("double").alias("order_discount_amount"),
    F.first(
        "total_amount",
        ignorenulls=True
    ).cast("double").alias("order_total"),
    F.sum(
        F.col("quantity").cast("double")
    ).alias("basket_size"),
    F.sum(
        "line_revenue"
    ).alias("item_revenue"),
    F.sum(
        "line_cost"
    ).alias("item_cost"),
    F.sum(
        "line_margin"
    ).alias("contribution_margin")
).withColumn(
    "discount_pct",
    F.when(
        F.col("order_subtotal") > 0,
        F.round(
            (
                F.col("order_discount_amount") /
                F.col("order_subtotal")
            ) * 100,
            4
        )
    ).otherwise(
        F.lit(0.0)
    )
).withColumn(
    "promotion_used",
    (
        F.col("promotion_id").isNotNull()
    ) &
    (
        F.col("order_discount_amount") > 0
    )
)


channel_summary = order_level.groupBy(
    "ordering_channel"
).agg(
    F.countDistinct(
        "order_id"
    ).alias("order_count"),
    F.countDistinct(
        "customer_id"
    ).alias("customer_count"),
    F.round(
        F.sum("order_total"),
        2
    ).alias("channel_revenue"),
    F.round(
        F.avg("basket_size"),
        2
    ).alias("average_basket_size"),
    F.round(
        F.expr(
            "percentile_approx(basket_size, 0.5)"
        ),
        2
    ).alias("median_basket_size"),
    F.round(
        F.avg("order_total"),
        2
    ).alias("average_order_value"),
    F.round(
        F.avg("order_discount_amount"),
        2
    ).alias("average_discount_amount"),
    F.round(
        F.avg("discount_pct"),
        2
    ).alias("average_discount_pct"),
    F.sum(
        F.col("promotion_used").cast("int")
    ).alias("promotion_orders"),
    F.round(
        F.sum("item_revenue"),
        2
    ).alias("item_revenue"),
    F.round(
        F.sum("item_cost"),
        2
    ).alias("item_cost"),
    F.round(
        F.sum("contribution_margin"),
        2
    ).alias("contribution_margin")
).withColumn(
    "promotion_order_rate_pct",
    F.when(
        F.col("order_count") > 0,
        F.round(
            (
                F.col("promotion_orders") /
                F.col("order_count")
            ) * 100,
            2
        )
    ).otherwise(
        F.lit(0.0)
    )
).withColumn(
    "profit_percentage",
    F.when(
        F.col("item_revenue") > 0,
        F.round(
            (
                F.col("contribution_margin") /
                F.col("item_revenue")
            ) * 100,
            2
        )
    )
).withColumn(
    "order_share_pct",
    F.round(
        (
            F.col("order_count") /
            F.sum("order_count").over(
                Window.partitionBy()
            )
        ) * 100,
        2
    )
).withColumn(
    "revenue_share_pct",
    F.round(
        (
            F.col("channel_revenue") /
            F.sum("channel_revenue").over(
                Window.partitionBy()
            )
        ) * 100,
        2
    )
)


channel_item_preferences = sales.groupBy(
    "ordering_channel",
    "item_id"
).agg(
    F.first(
        "item_name",
        ignorenulls=True
    ).alias("item_name"),
    F.first(
        "category_name",
        ignorenulls=True
    ).alias("category_name"),
    F.sum(
        F.col("quantity").cast("double")
    ).alias("quantity_sold"),
    F.countDistinct(
        "order_id"
    ).alias("item_order_count"),
    F.round(
        F.sum("line_revenue"),
        2
    ).alias("item_revenue"),
    F.round(
        F.sum("line_margin"),
        2
    ).alias("item_contribution_margin")
)


item_rank_window = Window.partitionBy(
    "ordering_channel"
).orderBy(
    F.desc("quantity_sold"),
    F.desc("item_order_count"),
    F.asc("item_id")
)


channel_menu_preferences = channel_item_preferences.withColumn(
    "preference_rank",
    F.row_number().over(
        item_rank_window
    )
).filter(
    F.col("preference_rank") <= 10
)


channel_category_preferences = sales.groupBy(
    "ordering_channel",
    "category_id"
).agg(
    F.first(
        "category_name",
        ignorenulls=True
    ).alias("category_name"),
    F.sum(
        F.col("quantity").cast("double")
    ).alias("quantity_sold"),
    F.countDistinct(
        "order_id"
    ).alias("category_order_count")
)


category_rank_window = Window.partitionBy(
    "ordering_channel"
).orderBy(
    F.desc("quantity_sold"),
    F.desc("category_order_count"),
    F.asc("category_id")
)


top_category = channel_category_preferences.withColumn(
    "category_rank",
    F.row_number().over(
        category_rank_window
    )
).filter(
    F.col("category_rank") == 1
).select(
    "ordering_channel",
    F.col("category_id").alias("top_category_id"),
    F.col("category_name").alias("top_category_name"),
    F.col("quantity_sold").alias("top_category_quantity")
)


hourly_patterns = order_level.withColumn(
    "order_hour",
    F.hour("order_datetime")
).groupBy(
    "ordering_channel",
    "order_hour"
).agg(
    F.countDistinct(
        "order_id"
    ).alias("hour_order_count"),
    F.round(
        F.sum("order_total"),
        2
    ).alias("hour_revenue")
)


hour_rank_window = Window.partitionBy(
    "ordering_channel"
).orderBy(
    F.desc("hour_order_count"),
    F.desc("hour_revenue"),
    F.asc("order_hour")
)


peak_hour = hourly_patterns.withColumn(
    "hour_rank",
    F.row_number().over(
        hour_rank_window
    )
).filter(
    F.col("hour_rank") == 1
).select(
    "ordering_channel",
    F.col("order_hour").alias("peak_order_hour"),
    F.col("hour_order_count").alias("peak_hour_orders"),
    F.col("hour_revenue").alias("peak_hour_revenue")
)


daily_patterns = order_level.withColumn(
    "day_of_week_number",
    F.dayofweek("order_datetime")
).withColumn(
    "day_of_week",
    F.date_format(
        "order_datetime",
        "EEEE"
    )
).groupBy(
    "ordering_channel",
    "day_of_week_number",
    "day_of_week"
).agg(
    F.countDistinct(
        "order_id"
    ).alias("day_order_count"),
    F.round(
        F.sum("order_total"),
        2
    ).alias("day_revenue")
)


day_rank_window = Window.partitionBy(
    "ordering_channel"
).orderBy(
    F.desc("day_order_count"),
    F.desc("day_revenue"),
    F.asc("day_of_week_number")
)


peak_day = daily_patterns.withColumn(
    "day_rank",
    F.row_number().over(
        day_rank_window
    )
).filter(
    F.col("day_rank") == 1
).select(
    "ordering_channel",
    F.col("day_of_week").alias("peak_day_of_week"),
    F.col("day_order_count").alias("peak_day_orders"),
    F.col("day_revenue").alias("peak_day_revenue")
)


channel_peak_periods = peak_hour.join(
    peak_day,
    "ordering_channel",
    "inner"
).withColumn(
    "peak_daypart",
    F.when(
        F.col("peak_order_hour").between(5, 10),
        "Morning"
    ).when(
        F.col("peak_order_hour").between(11, 15),
        "Lunch"
    ).when(
        F.col("peak_order_hour").between(16, 21),
        "Dinner"
    ).otherwise(
        "Late Night"
    )
)


channel_summary = channel_summary.join(
    top_category,
    "ordering_channel",
    "left"
).join(
    channel_peak_periods.select(
        "ordering_channel",
        "peak_order_hour",
        "peak_day_of_week",
        "peak_daypart"
    ),
    "ordering_channel",
    "left"
)


print("\n========ORDERING CHANNEL SUMMARY========")
channel_summary.orderBy(
    F.desc("order_count")
).show(
    truncate=False
)


print("\n========TOP MENU PREFERENCES BY CHANNEL========")
channel_menu_preferences.orderBy(
    "ordering_channel",
    "preference_rank"
).show(
    50,
    truncate=False
)


print("\n========PEAK PERIODS BY CHANNEL========")
channel_peak_periods.orderBy(
    "ordering_channel"
).show(
    truncate=False
)


channel_summary.write.mode(
    "overwrite"
).parquet(
    f"{output_folder}/channel_summary"
)


channel_menu_preferences.write.mode(
    "overwrite"
).parquet(
    f"{output_folder}/channel_menu_preferences"
)


channel_peak_periods.write.mode(
    "overwrite"
).parquet(
    f"{output_folder}/channel_peak_periods"
)


print("\nOrdering channel analysis completed successfully.")


spark.stop()
