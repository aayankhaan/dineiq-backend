from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("DineIQ Exploratory Data Analysis") \
    .getOrCreate()

integrated_folder = "integrated_data"
feature_folder = "feature_data"
output_folder = "eda_outputs"

transactions = spark.read.parquet(
    f"{integrated_folder}/transactions"
)

item_features = spark.read.parquet(
    f"{feature_folder}/item_features"
)

location_features = spark.read.parquet(
    f"{feature_folder}/location_features"
)


item_sales = transactions.groupBy(
    "item_id",
    "item_name"
).agg(
    F.sum("quantity").alias("quantity_sold")
)

top_selling_dishes = item_sales.orderBy(
    F.desc("quantity_sold"),
    F.asc("item_id")
).limit(10)

lowest_selling_dishes = item_sales.orderBy(
    F.asc("quantity_sold"),
    F.asc("item_id")
).limit(10)

highest_revenue_dishes = item_features.select(
    "item_id",
    "item_name",
    "item_revenue"
).orderBy(
    F.desc("item_revenue"),
    F.asc("item_id")
).limit(10)

highest_profit_dishes = item_features.select(
    "item_id",
    "item_name",
    "contribution_margin"
).orderBy(
    F.desc("contribution_margin"),
    F.asc("item_id")
).limit(10)

highest_margin_dishes = item_features.select(
    "item_id",
    "item_name",
    "profit_percentage"
).orderBy(
    F.desc("profit_percentage"),
    F.asc("item_id")
).limit(10)

high_wastage_dishes = item_features.select(
    "item_id",
    "item_name",
    "wastage_percentage"
).orderBy(
    F.desc("wastage_percentage"),
    F.asc("item_id")
).limit(10)

best_rated_dishes = item_features.select(
    "item_id",
    "item_name",
    "average_rating"
).orderBy(
    F.desc("average_rating"),
    F.asc("item_id")
).limit(10)

poorly_rated_dishes = item_features.select(
    "item_id",
    "item_name",
    "average_rating"
).orderBy(
    F.asc("average_rating"),
    F.asc("item_id")
).limit(10)

popular_categories = transactions.groupBy(
    "category_id",
    "category_name"
).agg(
    F.sum("quantity").alias("quantity_sold"),
    F.countDistinct("order_id").alias("order_frequency"),
    F.sum("line_total").cast("decimal(18,2)").alias("category_revenue")
).orderBy(
    F.desc("quantity_sold"),
    F.asc("category_id")
)

order_level = transactions.select(
    "order_id",
    "order_datetime",
    "ordering_channel",
    "restaurant_id",
    "restaurant_name",
    "total_amount"
).dropDuplicates(
    ["order_id"]
)

peak_ordering_periods = order_level.groupBy(
    F.hour("order_datetime").alias("order_hour")
).agg(
    F.countDistinct("order_id").alias("order_count"),
    F.sum("total_amount").cast("decimal(18,2)").alias("revenue")
).orderBy(
    F.desc("order_count"),
    F.asc("order_hour")
)


location_sales_patterns = location_features.select(
    "restaurant_id",
    "restaurant_name",
    "location_orders",
    "location_revenue",
    "location_average_order_value",
    "peak_hour_frequency",
    "weekend_order_ratio",
    "location_performance"
).orderBy(
    F.desc("location_revenue"),
    F.asc("restaurant_id")
)


channel_ordering_patterns = order_level.groupBy(
    "ordering_channel"
).agg(
    F.countDistinct("order_id").alias("order_count"),
    F.sum("total_amount").cast("decimal(18,2)").alias("channel_revenue"),
    F.round(F.avg("total_amount"), 2).alias("average_order_value")
).orderBy(
    F.desc("order_count"),
    F.asc("ordering_channel")
)


promotion_driven_sales = transactions.groupBy(
    "item_id",
    "item_name"
).agg(
    F.countDistinct("order_id").alias("total_orders"),
    F.countDistinct(
        F.when(
            F.col("item_discount_amount") > 0,
            F.col("order_id")
        )
    ).alias("promotion_orders"),
    F.sum("line_total").cast("decimal(18,2)").alias("item_revenue"),
    F.sum("item_discount_amount").cast("decimal(18,2)").alias("discount_amount")
).withColumn(
    "promotion_dependency",
    F.round(
        F.when(
            F.col("total_orders") > 0,
            F.col("promotion_orders") / F.col("total_orders") * 100
        ).otherwise(0),
        2
    )
).orderBy(
    F.desc("promotion_dependency"),
    F.desc("promotion_orders"),
    F.asc("item_id")
)


eda_outputs = {
    "top_selling_dishes": top_selling_dishes,
    "lowest_selling_dishes": lowest_selling_dishes,
    "highest_revenue_dishes": highest_revenue_dishes,
    "highest_profit_dishes": highest_profit_dishes,
    "highest_margin_dishes": highest_margin_dishes,
    "high_wastage_dishes": high_wastage_dishes,
    "best_rated_dishes": best_rated_dishes,
    "poorly_rated_dishes": poorly_rated_dishes,
    "popular_categories": popular_categories,
    "peak_ordering_periods": peak_ordering_periods,
    "location_sales_patterns": location_sales_patterns,
    "channel_ordering_patterns": channel_ordering_patterns,
    "promotion_driven_sales": promotion_driven_sales
}

for name, dataframe in eda_outputs.items():
    print(f"\n========{name.upper().replace('_', ' ')}========")
    dataframe.show(10, truncate=False)

    dataframe.write.mode("overwrite").parquet(
        f"{output_folder}/{name}"
    )

print("\nExploratory data analysis completed successfully.")

spark.stop()
