from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.settings import INTEGRATED_DATA_FOLDER, FEATURE_DATA_FOLDER, ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("DineIQ Customer Segmentation") \
    .getOrCreate()


integrated_folder = INTEGRATED_DATA_FOLDER
feature_folder = FEATURE_DATA_FOLDER
output_folder = f"{ANALYTICS_DATA_FOLDER}/customer_segmentation"


transactions = spark.read.parquet(f"{integrated_folder}/transactions")
customer_features = spark.read.parquet(f"{feature_folder}/customer_features")



customer_orders = transactions.select(
    "customer_id",
    "order_id",
    "order_datetime",
    "total_amount",
    "ordering_channel"
).dropDuplicates(
    ["order_id"]
)


reference_date = customer_orders.agg(
    F.max("order_datetime").alias("max_date")
).first()["max_date"]


customer_activity = customer_orders.groupBy(
    "customer_id"
).agg(
    F.min("order_datetime").alias("first_order_date"),
    F.max("order_datetime").alias("last_order_date"),
    F.countDistinct("order_id").alias("total_orders")
).withColumn(
    "customer_lifetime_days",
    F.greatest(
        F.datediff(
            F.col("last_order_date"),
            F.col("first_order_date")
        ) + 1,
        F.lit(1)
    )
).withColumn(
    "visit_frequency",
    F.round(
        F.col("total_orders") /
        F.greatest(
            F.col("customer_lifetime_days") / F.lit(30.0),
            F.lit(1.0)
        ),
        2
    )
)

category_counts = transactions.groupBy(
    "customer_id",
    "category_name"
).agg(
    F.countDistinct("order_id").alias("category_orders"),
    F.sum("quantity").alias("category_quantity")
)


category_window = Window.partitionBy(
    "customer_id"
).orderBy(
    F.desc("category_orders"),
    F.desc("category_quantity"),
    F.asc("category_name")
)


favorite_category = category_counts.withColumn(
    "category_rank",
    F.row_number().over(category_window)
).filter(
    F.col("category_rank") == 1
).select(
    "customer_id",
    F.col("category_name").alias("favorite_category")
)


promotion_orders = transactions.groupBy(
    "customer_id",
    "order_id"
).agg(
    F.max(
        F.when(
            F.col("item_discount_amount") > 0,
            1
        ).otherwise(0)
    ).alias("used_promotion")
)


promotion_sensitivity = promotion_orders.groupBy(
    "customer_id"
).agg(
    F.countDistinct("order_id").alias("promotion_total_orders"),
    F.sum("used_promotion").alias("promotion_orders")
).withColumn(
    "promotion_sensitivity",
    F.round(
        (
            F.col("promotion_orders") /
            F.col("promotion_total_orders")
        ) * 100,
        2
    )
).select(
    "customer_id",
    "promotion_sensitivity"
)



time_orders = customer_orders.withColumn(
    "order_hour",
    F.hour("order_datetime")
).withColumn(
    "time_of_day",
    F.when(
        F.col("order_hour").between(5, 10),
        "Breakfast"
    ).when(
        F.col("order_hour").between(11, 15),
        "Lunch"
    ).when(
        F.col("order_hour").between(16, 21),
        "Dinner"
    ).otherwise(
        "Late Night"
    )
)


time_counts = time_orders.groupBy(
    "customer_id",
    "time_of_day"
).agg(
    F.countDistinct("order_id").alias("time_orders")
)


time_window = Window.partitionBy(
    "customer_id"
).orderBy(
    F.desc("time_orders"),
    F.asc("time_of_day")
)


time_preference = time_counts.withColumn(
    "time_rank",
    F.row_number().over(time_window)
).filter(
    F.col("time_rank") == 1
).select(
    "customer_id",
    F.col("time_of_day").alias("time_of_day_preference")
)


repeat_behavior = customer_activity.select(
    "customer_id",
    F.when(
        F.col("total_orders") > 1,
        True
    ).otherwise(
        False
    ).alias("repeat_customer")
)

customer_profile = customer_features.join(
    customer_activity.select(
        "customer_id",
        "first_order_date",
        "last_order_date",
        "customer_lifetime_days",
        "visit_frequency"
    ),
    "customer_id",
    "left"
).join(
    favorite_category,
    "customer_id",
    "left"
).join(
    promotion_sensitivity,
    "customer_id",
    "left"
).join(
    time_preference,
    "customer_id",
    "left"
).join(
    repeat_behavior,
    "customer_id",
    "left"
)

frequency_q50, frequency_q75 = customer_profile.approxQuantile(
    "customer_frequency",
    [0.50, 0.75],
    0.01
)

monetary_q50, monetary_q75 = customer_profile.approxQuantile(
    "customer_monetary_value",
    [0.50, 0.75],
    0.01
)

recency_q25, recency_q75 = customer_profile.approxQuantile(
    "customer_recency",
    [0.25, 0.75],
    0.01
)

promotion_users = customer_profile.filter(
    F.col("promotion_sensitivity") > 0
)

promotion_q50 = promotion_users.approxQuantile(
    "promotion_sensitivity",
    [0.50],
    0.01
)[0]


customer_segments = customer_profile.withColumn(
    "customer_segment",
    F.when(
        (F.col("customer_frequency") >= F.lit(frequency_q75)) &
        (F.col("customer_monetary_value") >= F.lit(monetary_q75)) &
        (F.col("customer_recency") <= F.lit(recency_q25)),
        "High-Value Loyal Customers"
    ).when(
        (F.col("customer_frequency") == 1) &
        (F.col("customer_recency") <= F.lit(recency_q25)),
        "New Customers"
    ).when(
        (F.col("customer_recency") >= F.lit(recency_q75)) &
        (
            (F.col("customer_frequency") >= F.lit(frequency_q50)) |
            (F.col("customer_monetary_value") >= F.lit(monetary_q50))
        ),
        "At-Risk Customers"
    ).when(
        (F.col("promotion_sensitivity") > 0) &
        (F.col("promotion_sensitivity") >= F.lit(promotion_q50)),
        "Promotion-Driven Customers"
    ).when(
        F.col("customer_frequency") >= F.lit(frequency_q50),
        "Frequent Customers"
    ).otherwise(
        "Occasional Customers"
    )
)



customer_segments = customer_segments.withColumn(
    "segment_reason",
    F.when(
        F.col("customer_segment") == "High-Value Loyal Customers",
        F.lit("Recent customer with high order frequency and high monetary value.")
    ).when(
        F.col("customer_segment") == "Frequent Customers",
        F.lit("Customer orders frequently and remains actively engaged.")
    ).when(
        F.col("customer_segment") == "Promotion-Driven Customers",
        F.lit("A high share of the customer's orders contain promotional discounts.")
    ).when(
        F.col("customer_segment") == "At-Risk Customers",
        F.lit("Previously valuable or frequent customer with declining recent engagement.")
    ).when(
        F.col("customer_segment") == "New Customers",
        F.lit("Recent customer with only one recorded order.")
    ).otherwise(
        F.lit("Customer purchases occasionally without strong loyalty or promotion dependence.")
    )
).withColumn(
    "recommended_strategy",
    F.when(
        F.col("customer_segment") == "High-Value Loyal Customers",
        F.lit("Retain with VIP rewards, loyalty benefits, and premium personalized recommendations.")
    ).when(
        F.col("customer_segment") == "Frequent Customers",
        F.lit("Encourage higher basket value through cross-sell, bundles, and loyalty rewards.")
    ).when(
        F.col("customer_segment") == "Promotion-Driven Customers",
        F.lit("Target with controlled promotions and value-focused recommendations while protecting margin.")
    ).when(
        F.col("customer_segment") == "At-Risk Customers",
        F.lit("Use re-engagement campaigns and personalized retention offers.")
    ).when(
        F.col("customer_segment") == "New Customers",
        F.lit("Use onboarding offers and personalized recommendations to encourage a second purchase.")
    ).otherwise(
        F.lit("Use low-cost personalized recommendations and engagement campaigns to increase visit frequency.")
    )
)


segment_summary = customer_segments.groupBy(
    "customer_segment"
).agg(
    F.count("*").alias("customer_count"),
    F.round(F.avg("customer_recency"), 2).alias("average_recency"),
    F.round(F.avg("customer_frequency"), 2).alias("average_frequency"),
    F.round(F.avg("customer_monetary_value"), 2).alias("average_monetary_value"),
    F.round(F.avg("average_order_value"), 2).alias("average_order_value"),
    F.round(F.avg("visit_frequency"), 2).alias("average_visit_frequency"),
    F.round(F.avg("promotion_sensitivity"), 2).alias("average_promotion_sensitivity")
).withColumn(
    "customer_percentage",
    F.round(
        (
            F.col("customer_count") /
            F.lit(customer_segments.count())
        ) * 100,
        2
    )
)


segment_definitions = spark.createDataFrame([
    (
        "High-Value Loyal Customers",
        "High frequency, high monetary value, and recent activity.",
        "Retention / VIP",
        "Retain with VIP rewards, loyalty benefits, and premium personalized recommendations."
    ),
    (
        "Frequent Customers",
        "Above-median frequency with active recent engagement.",
        "Growth / Cross-Sell",
        "Encourage higher basket value through cross-sell, bundles, and loyalty rewards."
    ),
    (
        "Promotion-Driven Customers",
        "Promotion usage at or above the upper-quartile promotion threshold.",
        "Promotion Targeting",
        "Use controlled promotions and value-focused recommendations while protecting margin."
    ),
    (
        "At-Risk Customers",
        "High recency with historically meaningful frequency or monetary value.",
        "Retention",
        "Use re-engagement campaigns and personalized retention offers."
    ),
    (
        "New Customers",
        "Only one recorded order.",
        "Onboarding",
        "Encourage a second purchase using onboarding offers and personalized recommendations."
    ),
    (
        "Occasional Customers",
        "Customers not meeting stronger loyalty, promotion, risk, or new-customer patterns.",
        "Engagement",
        "Use low-cost personalized recommendations and campaigns to increase visit frequency."
    )
], [
    "customer_segment",
    "segment_definition",
    "business_objective",
    "recommended_strategy"
])


print("\n========CUSTOMER SEGMENTATION THRESHOLDS========")
print(f"Frequency Median: {frequency_q50}")
print(f"Frequency 75th Percentile: {frequency_q75}")
print(f"Monetary Median: {monetary_q50}")
print(f"Monetary 75th Percentile: {monetary_q75}")
print(f"Recency 25th Percentile: {recency_q25}")
print(f"Recency 75th Percentile: {recency_q75}")
print(f"Promotion Sensitivity Median Among Promotion Users: {promotion_q50}")


print("\n========CUSTOMER SEGMENTS========")
segment_summary.orderBy(
    F.desc("customer_count")
).show(
    truncate=False
)


print("\n========SAMPLE CUSTOMER PROFILES========")
customer_segments.select(
    "customer_id",
    "customer_recency",
    "customer_frequency",
    "customer_monetary_value",
    "average_order_value",
    "visit_frequency",
    "favorite_category",
    "promotion_sensitivity",
    "channel_preference",
    "time_of_day_preference",
    "repeat_customer",
    "customer_segment",
    "segment_reason",
    "recommended_strategy"
).show(
    20,
    truncate=False
)


customer_segments.write.mode("overwrite").parquet(
    f"{output_folder}/customer_segments"
)


segment_summary.write.mode("overwrite").parquet(
    f"{output_folder}/segment_summary"
)


segment_definitions.write.mode("overwrite").parquet(
    f"{output_folder}/segment_definitions"
)


print("\nCustomer segmentation completed successfully.")


spark.stop()