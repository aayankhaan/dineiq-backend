from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("DineIQ Feature Engineering") \
    .getOrCreate()

integrated_folder = "integrated_data"
feature_folder = "feature_data"

transactions = spark.read.parquet(f"{integrated_folder}/transactions")
ratings = spark.read.parquet(f"{integrated_folder}/ratings")
pricing = spark.read.parquet(f"{integrated_folder}/pricing_history")
wastage = spark.read.parquet(f"{integrated_folder}/wastage")
recipes = spark.read.parquet(f"{integrated_folder}/recipes")

# ==================== ITEM FEATURES ====================

item_revenue = transactions.groupBy(
    "item_id",
    "item_name"
).agg(
    F.sum("line_total").cast("decimal(18,2)").alias("item_revenue")
)

item_cost = transactions.withColumn(
    "line_cost",
    F.col("quantity") * F.col("item_cost")
).groupBy(
    "item_id",
    "item_name"
).agg(
    F.sum("line_cost").cast("decimal(18,2)").alias("cost")
)

item_features = item_revenue.join(
    item_cost,
    ["item_id", "item_name"],
    "inner"
)

item_features = item_features.withColumn(
    "contribution_margin",
    (F.col("item_revenue") - F.col("cost")).cast("decimal(18,2)")
)

item_features = item_features.withColumn(
    "profit_percentage",
    F.round(
        (F.col("contribution_margin") / F.col("item_revenue")) * 100,
        2
    )
)

order_frequency = transactions.groupBy(
    "item_id",
    "item_name"
).agg(
    F.countDistinct("order_id").alias("order_frequency")
)

item_features = item_features.join(
    order_frequency,
    ["item_id", "item_name"],
    "inner"
)

total_orders = transactions.select("order_id").distinct().count()

item_features = item_features.withColumn(
    "item_popularity",
    F.round(
        (F.col("order_frequency") / F.lit(total_orders)) * 100,
        2
    )
)

repeat_customers = transactions.groupBy(
    "item_id",
    "customer_id"
).agg(
    F.countDistinct("order_id").alias("customer_item_orders")
).groupBy(
    "item_id"
).agg(
    F.count("*").alias("item_customers"),
    F.sum(
        F.when(F.col("customer_item_orders") > 1, 1).otherwise(0)
    ).alias("repeat_customers")
).withColumn(
    "repeat_purchase_rate",
    F.round(
        (F.col("repeat_customers") / F.col("item_customers")) * 100,
        2
    )
).select(
    "item_id",
    "repeat_purchase_rate"
)

item_features = item_features.join(
    repeat_customers,
    "item_id",
    "left"
)

rating_features = ratings.groupBy(
    "item_id"
).agg(
    F.round(F.avg("rating"), 2).alias("average_rating")
)

rating_monthly = ratings.withColumn(
    "rating_month",
    F.date_trunc("month", F.col("rating_date"))
).groupBy(
    "item_id",
    "rating_month"
).agg(
    F.avg("rating").alias("monthly_rating")
)

rating_window = Window.partitionBy("item_id").orderBy("rating_month")

rating_trend = rating_monthly.withColumn(
    "previous_rating",
    F.lag("monthly_rating").over(rating_window)
).withColumn(
    "rating_change",
    F.col("monthly_rating") - F.col("previous_rating")
).groupBy(
    "item_id"
).agg(
    F.round(F.avg("rating_change"), 2).alias("rating_trend")
)

item_features = item_features.join(
    rating_features,
    "item_id",
    "left"
).join(
    rating_trend,
    "item_id",
    "left"
)


promotion_features = transactions.groupBy(
    "item_id"
).agg(
    F.countDistinct("order_id").alias("total_item_orders"),
    F.countDistinct(
        F.when(
            F.col("item_discount_amount") > 0,
            F.col("order_id")
        )
    ).alias("promotion_orders"),
    F.sum("line_total").alias("total_item_revenue"),
    F.sum("item_discount_amount").alias("total_item_discount")
).withColumn(
    "promotion_dependency",
    F.round(
        (F.col("promotion_orders") / F.col("total_item_orders")) * 100,
        2
    )
).withColumn(
    "discount_percentage",
    F.round(
        (
            F.col("total_item_discount") /
            (F.col("total_item_revenue") + F.col("total_item_discount"))
        ) * 100,
        2
    )
).select(
    "item_id",
    "promotion_dependency",
    "discount_percentage"
)

item_features = item_features.join(
    promotion_features,
    "item_id",
    "left"
)

item_quantity = transactions.groupBy(
    "item_id"
).agg(
    F.sum("quantity").alias("quantity_sold")
)

recipe_usage = recipes.select(
    F.col("menu_item_id").alias("item_id"),
    "ingredient_id",
    "quantity_required"
).join(
    item_quantity,
    "item_id",
    "inner"
).withColumn(
    "ingredient_demand",
    F.col("quantity_required") * F.col("quantity_sold")
)

ingredient_total_demand = recipe_usage.groupBy(
    "ingredient_id"
).agg(
    F.sum("ingredient_demand").alias("total_ingredient_demand")
)

wastage_by_ingredient = wastage.groupBy(
    "ingredient_id"
).agg(
    F.sum("quantity").alias("ingredient_wastage")
)

wastage_allocation = recipe_usage.join(
    ingredient_total_demand,
    "ingredient_id",
    "inner"
).join(
    wastage_by_ingredient,
    "ingredient_id",
    "left"
).fillna(
    0,
    subset=["ingredient_wastage"]
).withColumn(
    "allocated_wastage",
    F.when(
        F.col("total_ingredient_demand") > 0,
        F.col("ingredient_wastage") *
        (F.col("ingredient_demand") / F.col("total_ingredient_demand"))
    ).otherwise(0)
)

wastage_features = wastage_allocation.groupBy(
    "item_id"
).agg(
    F.sum("ingredient_demand").alias("ingredient_usage"),
    F.sum("allocated_wastage").alias("allocated_wastage")
).withColumn(
    "wastage_percentage",
    F.round(
        F.when(
            (F.col("ingredient_usage") + F.col("allocated_wastage")) > 0,
            (
                F.col("allocated_wastage") /
                (F.col("ingredient_usage") + F.col("allocated_wastage"))
            ) * 100
        ).otherwise(0),
        2
    )
).select(
    "item_id",
    "wastage_percentage"
)

item_features = item_features.join(
    wastage_features,
    "item_id",
    "left"
)

price_window = Window.partitionBy("item_id").orderBy(
    F.col("effective_date").desc()
)

price_change = pricing.withColumn(
    "price_rank",
    F.row_number().over(price_window)
).filter(
    F.col("price_rank") == 1
).withColumn(
    "price_change_percentage",
    F.round(
        F.when(
            F.col("old_price") > 0,
            ((F.col("new_price") - F.col("old_price")) / F.col("old_price")) * 100
        ).otherwise(0),
        2
    )
).select(
    "item_id",
    "price_change_percentage"
)

item_features = item_features.join(
    price_change,
    "item_id",
    "left"
)

# ==================== CUSTOMER FEATURES ====================

reference_date = transactions.agg(
    F.max("order_datetime").alias("max_date")
).first()["max_date"]

customer_orders = transactions.select(
    "customer_id",
    "order_id",
    "order_datetime",
    "total_amount",
    "ordering_channel"
).dropDuplicates(
    ["order_id"]
)

customer_features = customer_orders.groupBy(
    "customer_id"
).agg(
    F.datediff(
        F.lit(reference_date),
        F.max("order_datetime")
    ).alias("customer_recency"),
    F.countDistinct("order_id").alias("customer_frequency"),
    F.sum("total_amount").cast("decimal(18,2)").alias("customer_monetary_value"),
    F.round(F.avg("total_amount"), 2).alias("average_order_value")
)

channel_counts = customer_orders.groupBy(
    "customer_id",
    "ordering_channel"
).agg(
    F.count("*").alias("channel_orders")
)

channel_window = Window.partitionBy("customer_id").orderBy(
    F.desc("channel_orders"),
    F.asc("ordering_channel")
)

channel_preference = channel_counts.withColumn(
    "channel_rank",
    F.row_number().over(channel_window)
).filter(
    F.col("channel_rank") == 1
).select(
    "customer_id",
    F.col("ordering_channel").alias("channel_preference")
)

customer_features = customer_features.join(
    channel_preference,
    "customer_id",
    "left"
)

# ==================== ORDER FEATURES ====================

order_features = transactions.groupBy(
    "order_id"
).agg(
    F.sum("quantity").alias("basket_size")
)

# ==================== LOCATION FEATURES ====================

location_orders = transactions.select(
    "restaurant_id",
    "restaurant_name",
    "order_id",
    "order_datetime",
    "total_amount"
).dropDuplicates(
    ["order_id"]
)

location_features = location_orders.groupBy(
    "restaurant_id",
    "restaurant_name"
).agg(
    F.countDistinct("order_id").alias("location_orders"),
    F.sum("total_amount").cast("decimal(18,2)").alias("location_revenue"),
    F.round(F.avg("total_amount"), 2).alias("location_average_order_value"),
    F.round(
        F.avg(
            F.when(
                F.hour("order_datetime").between(18, 21),
                1
            ).otherwise(0)
        ) * 100,
        2
    ).alias("peak_hour_frequency"),
    F.round(
        F.avg(
            F.when(
                F.dayofweek("order_datetime").isin(1, 7),
                1
            ).otherwise(0)
        ) * 100,
        2
    ).alias("weekend_order_ratio")
).withColumn(
    "location_performance",
    F.col("location_revenue")
)

item_features = item_features.fillna({
    "rating_trend": 0.0,
    "price_change_percentage": 0.0
})

# ==================== OUTPUT ====================


print("\n========ITEM FEATURES========")
item_features.show(10, truncate=False)

print("\n========CUSTOMER FEATURES========")
customer_features.show(10, truncate=False)

print("\n========ORDER FEATURES========")
order_features.show(10, truncate=False)

print("\n========LOCATION FEATURES========")
location_features.show(10, truncate=False)

item_features.write.mode("overwrite").parquet(
    f"{feature_folder}/item_features"
)

customer_features.write.mode("overwrite").parquet(
    f"{feature_folder}/customer_features"
)

order_features.write.mode("overwrite").parquet(
    f"{feature_folder}/order_features"
)

location_features.write.mode("overwrite").parquet(
    f"{feature_folder}/location_features"
)

print("\nFeature engineering completed successfully.")

spark.stop()