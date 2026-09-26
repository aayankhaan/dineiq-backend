from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("DineIQ Tricky Menu Performance Cases") \
    .getOrCreate()

integrated_folder = "integrated_data"
analytics_folder = "analytics_data"
output_folder = "analytics_data/tricky_menu_cases"

transactions = spark.read.parquet(
    f"{integrated_folder}/transactions"
)

menu_profitability = spark.read.parquet(
    f"{analytics_folder}/menu_profitability"
)

menu_classification = spark.read.parquet(
    f"{analytics_folder}/menu_classification"
)


quantity_median = menu_profitability.approxQuantile(
    "quantity_sold", [0.5], 0.01
)[0]

profit_median = menu_profitability.approxQuantile(
    "profit_percentage", [0.5], 0.01
)[0]

rating_median = menu_profitability.approxQuantile(
    "average_rating", [0.5], 0.01
)[0]

wastage_upper = menu_profitability.approxQuantile(
    "wastage_percentage", [0.75], 0.01
)[0]

promotion_upper = menu_profitability.approxQuantile(
    "promotion_dependency", [0.75], 0.01
)[0]


location_sales = transactions.groupBy(
    "item_id",
    "restaurant_id"
).agg(
    F.sum("quantity").alias("location_quantity")
)

location_stats = location_sales.groupBy(
    "item_id"
).agg(
    F.min("location_quantity").alias("min_location_quantity"),
    F.max("location_quantity").alias("max_location_quantity"),
    F.avg("location_quantity").alias("avg_location_quantity")
).withColumn(
    "location_variation",
    F.when(
        F.col("avg_location_quantity") > 0,
        (
            F.col("max_location_quantity") -
            F.col("min_location_quantity")
        ) / F.col("avg_location_quantity")
    ).otherwise(0)
)

location_variation_upper = location_stats.approxQuantile(
    "location_variation", [0.75], 0.01
)[0]


daily_item_sales = transactions.groupBy(
    "item_id",
    F.to_date("order_datetime").alias("order_date")
).agg(
    F.sum("quantity").alias("daily_quantity")
).withColumn(
    "is_weekend",
    F.dayofweek("order_date").isin(1, 7)
)

weekend_sales = daily_item_sales.groupBy(
    "item_id"
).agg(
    F.avg(
        F.when(
            F.col("is_weekend"),
            F.col("daily_quantity")
        )
    ).alias("weekend_daily_average"),
    F.avg(
        F.when(
            ~F.col("is_weekend"),
            F.col("daily_quantity")
        )
    ).alias("weekday_daily_average")
).withColumn(
    "weekend_lift",
    F.when(
        F.col("weekday_daily_average") > 0,
        F.col("weekend_daily_average") /
        F.col("weekday_daily_average")
    ).otherwise(0)
)

weekend_upper = weekend_sales.approxQuantile(
    "weekend_lift", [0.75], 0.01
)[0]


calendar_month_sales = transactions.groupBy(
    "item_id",
    F.year("order_datetime").alias("year"),
    F.month("order_datetime").alias("month")
).agg(
    F.sum("quantity").alias("monthly_quantity")
)

monthly_sales = calendar_month_sales.groupBy(
    "item_id",
    "month"
).agg(
    F.avg("monthly_quantity").alias("average_month_quantity")
)

seasonal_stats = monthly_sales.groupBy(
    "item_id"
).agg(
    F.min("average_month_quantity").alias("min_monthly_quantity"),
    F.max("average_month_quantity").alias("max_monthly_quantity"),
    F.avg("average_month_quantity").alias("avg_monthly_quantity")
).withColumn(
    "seasonal_variation",
    F.when(
        F.col("avg_monthly_quantity") > 0,
        (
            F.col("max_monthly_quantity") -
            F.col("min_monthly_quantity")
        ) / F.col("avg_monthly_quantity")
    ).otherwise(0)
)

seasonal_upper = seasonal_stats.approxQuantile(
    "seasonal_variation", [0.75], 0.01
)[0]


history = transactions.groupBy(
    "item_id"
).agg(
    F.min("order_datetime").alias("first_order_date"),
    F.max("order_datetime").alias("last_order_date"),
    F.countDistinct(
        F.date_trunc("month", "order_datetime")
    ).alias("history_months")
)


tricky_cases = menu_classification.join(
    location_stats.select(
        "item_id",
        "location_variation"
    ),
    "item_id",
    "left"
).join(
    weekend_sales.select(
        "item_id",
        "weekend_lift"
    ),
    "item_id",
    "left"
).join(
    seasonal_stats.select(
        "item_id",
        "seasonal_variation"
    ),
    "item_id",
    "left"
).join(
    history,
    "item_id",
    "left"
)

tricky_cases = tricky_cases.withColumn(
    "high_selling_loss_making",
    (F.col("quantity_sold") >= F.lit(quantity_median)) &
    (F.col("contribution_margin") < 0)
).withColumn(
    "highly_profitable_rarely_purchased",
    (F.col("quantity_sold") < F.lit(quantity_median)) &
    (F.col("profit_percentage") >= F.lit(profit_median))
).withColumn(
    "popular_excessive_wastage",
    (F.col("quantity_sold") >= F.lit(quantity_median)) &
    (F.col("wastage_percentage") > F.lit(wastage_upper))
).withColumn(
    "highly_rated_poor_profitability",
    (F.col("average_rating") >= F.lit(rating_median)) &
    (F.col("profit_percentage") < F.lit(profit_median))
).withColumn(
    "low_rated_high_sales",
    (F.col("average_rating") < F.lit(rating_median)) &
    (F.col("quantity_sold") >= F.lit(quantity_median))
).withColumn(
    "promotion_dependent",
    (F.col("promotion_dependency") > 0) &
    (F.col("promotion_dependency") >= F.lit(promotion_upper))
).withColumn(
    "different_across_locations",
    F.col("location_variation") >= F.lit(location_variation_upper)
).withColumn(
    "weekend_performer",
    (F.col("weekend_lift") >= F.lit(weekend_upper)) &
    (F.col("weekend_lift") > 1)
).withColumn(
    "seasonal_item",
    F.col("seasonal_variation") >= F.lit(seasonal_upper)
).withColumn(
    "insufficient_history",
    F.col("history_months") < 3
)

case_columns = [
    "high_selling_loss_making",
    "highly_profitable_rarely_purchased",
    "popular_excessive_wastage",
    "highly_rated_poor_profitability",
    "low_rated_high_sales",
    "promotion_dependent",
    "different_across_locations",
    "weekend_performer",
    "seasonal_item",
    "insufficient_history"
]

tricky_cases = tricky_cases.withColumn(
    "tricky_case_count",
    sum(
        F.col(column).cast("int")
        for column in case_columns
    )
)


print("\n========TRICKY MENU PERFORMANCE CASES========")

for column in case_columns:
    count = tricky_cases.filter(
        F.col(column)
    ).count()

    print(f"{column}: {count}")

print("\n========ITEMS WITH TRICKY CASES========")
tricky_cases.filter(
    F.col("tricky_case_count") > 0
).select(
    "item_id",
    "item_name",
    "performance_class",
    "tricky_case_count",
    *case_columns
).orderBy(
    F.desc("tricky_case_count"),
    F.asc("item_id")
).show(150, truncate=False)

tricky_cases.write.mode("overwrite").parquet(
    output_folder
)

print("\nTricky menu performance case analysis completed successfully.")

spark.stop()
