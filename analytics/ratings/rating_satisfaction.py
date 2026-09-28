from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import INTEGRATED_DATA_FOLDER, ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("DineIQ Rating and Satisfaction Analysis") \
    .getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "8")


integrated_folder = INTEGRATED_DATA_FOLDER
output_folder = f"{ANALYTICS_DATA_FOLDER}/rating_satisfaction"


transactions = spark.read.parquet(
    f"{integrated_folder}/transactions"
)

ratings = spark.read.parquet(
    f"{integrated_folder}/ratings"
)



transaction_base = transactions.filter(
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


purchase_context = transaction_base.groupBy(
    "order_id",
    "customer_id",
    "restaurant_id",
    "item_id"
).agg(
    F.first(
        "item_name",
        ignorenulls=True
    ).alias(
        "item_name"
    ),
    F.first(
        "category_name",
        ignorenulls=True
    ).alias(
        "category_name"
    ),
    F.first(
        "restaurant_name",
        ignorenulls=True
    ).alias(
        "restaurant_name"
    ),
    F.first(
        "restaurant_city",
        ignorenulls=True
    ).alias(
        "restaurant_city"
    ),
    F.first(
        "restaurant_area",
        ignorenulls=True
    ).alias(
        "restaurant_area"
    ),
    F.sum(
        F.col("quantity").cast("double")
    ).alias(
        "purchase_quantity"
    ),
    F.sum(
        "line_revenue"
    ).alias(
        "purchase_revenue"
    ),
    F.sum(
        "line_margin"
    ).alias(
        "purchase_margin"
    ),
    F.max(
        F.when(
            F.col("item_discount_amount") > 0,
            1
        ).otherwise(0)
    ).alias(
        "promotion_used"
    )
)


rating_context = ratings.alias(
    "r"
).join(
    purchase_context.alias(
        "p"
    ),
    [
        "order_id",
        "customer_id",
        "restaurant_id",
        "item_id"
    ],
    "inner"
).select(
    F.col("r.rating_id"),
    F.col("r.customer_id"),
    F.col("r.order_id"),
    F.col("r.restaurant_id"),
    F.col("r.item_id"),
    F.col("r.rating").cast("double").alias(
        "rating"
    ),
    F.to_date(
        F.col("r.rating_date")
    ).alias(
        "rating_date"
    ),
    F.col("p.item_name"),
    F.col("p.category_name"),
    F.col("p.restaurant_name"),
    F.col("p.restaurant_city"),
    F.col("p.restaurant_area"),
    F.col("p.purchase_quantity"),
    F.col("p.purchase_revenue"),
    F.col("p.purchase_margin"),
    F.when(
        F.col("p.promotion_used") == 1,
        "Promoted"
    ).otherwise(
        "Non-Promoted"
    ).alias(
        "promotion_status"
    )
)



item_sales = transaction_base.groupBy(
    "item_id"
).agg(
    F.first(
        "item_name",
        ignorenulls=True
    ).alias(
        "item_name"
    ),
    F.first(
        "category_name",
        ignorenulls=True
    ).alias(
        "category_name"
    ),
    F.countDistinct(
        "order_id"
    ).alias(
        "order_count"
    ),
    F.sum(
        F.col("quantity").cast("double")
    ).alias(
        "quantity_sold"
    ),
    F.round(
        F.sum("line_revenue"),
        2
    ).alias(
        "revenue"
    ),
    F.round(
        F.sum("line_margin"),
        2
    ).alias(
        "contribution_margin"
    )
).withColumn(
    "profit_percentage",
    F.when(
        F.col("revenue") > 0,
        F.round(
            (
                F.col("contribution_margin") /
                F.col("revenue")
            ) * 100,
            2
        )
    )
)


item_repeat = transaction_base.groupBy(
    "item_id",
    "customer_id"
).agg(
    F.countDistinct(
        "order_id"
    ).alias(
        "customer_item_orders"
    )
).groupBy(
    "item_id"
).agg(
    F.count(
        "*"
    ).alias(
        "item_customers"
    ),
    F.sum(
        F.when(
            F.col("customer_item_orders") > 1,
            1
        ).otherwise(0)
    ).alias(
        "repeat_customers"
    )
).withColumn(
    "repeat_purchase_rate_pct",
    F.when(
        F.col("item_customers") > 0,
        F.round(
            (
                F.col("repeat_customers") /
                F.col("item_customers")
            ) * 100,
            2
        )
    ).otherwise(
        F.lit(0.0)
    )
)


item_ratings = rating_context.groupBy(
    "item_id"
).agg(
    F.count(
        "*"
    ).alias(
        "rating_count"
    ),
    F.round(
        F.avg("rating"),
        3
    ).alias(
        "average_rating"
    ),
    F.round(
        (
            F.sum(
                F.when(
                    F.col("rating") >= 4,
                    1
                ).otherwise(0)
            ) /
            F.count("*")
        ) * 100,
        2
    ).alias(
        "positive_rating_rate_pct"
    ),
    F.round(
        (
            F.sum(
                F.when(
                    F.col("rating") <= 2,
                    1
                ).otherwise(0)
            ) /
            F.count("*")
        ) * 100,
        2
    ).alias(
        "low_rating_rate_pct"
    ),
    F.round(
        F.avg(
            F.when(
                F.col("promotion_status") == "Promoted",
                F.col("rating")
            )
        ),
        3
    ).alias(
        "promoted_average_rating"
    ),
    F.round(
        F.avg(
            F.when(
                F.col("promotion_status") == "Non-Promoted",
                F.col("rating")
            )
        ),
        3
    ).alias(
        "non_promoted_average_rating"
    )
).withColumn(
    "promotion_rating_gap",
    F.when(
        F.col("promoted_average_rating").isNotNull() &
        F.col("non_promoted_average_rating").isNotNull(),
        F.round(
            F.col("promoted_average_rating") -
            F.col("non_promoted_average_rating"),
            3
        )
    )
)


item_rating_performance = item_sales.join(
    item_repeat,
    "item_id",
    "left"
).join(
    item_ratings,
    "item_id",
    "left"
)


location_sales = transaction_base.groupBy(
    "restaurant_id"
).agg(
    F.first(
        "restaurant_name",
        ignorenulls=True
    ).alias(
        "restaurant_name"
    ),
    F.first(
        "restaurant_city",
        ignorenulls=True
    ).alias(
        "restaurant_city"
    ),
    F.first(
        "restaurant_area",
        ignorenulls=True
    ).alias(
        "restaurant_area"
    ),
    F.countDistinct(
        "order_id"
    ).alias(
        "order_count"
    ),
    F.sum(
        F.col("quantity").cast("double")
    ).alias(
        "quantity_sold"
    ),
    F.round(
        F.sum("line_revenue"),
        2
    ).alias(
        "revenue"
    ),
    F.round(
        F.sum("line_margin"),
        2
    ).alias(
        "contribution_margin"
    )
).withColumn(
    "profit_percentage",
    F.when(
        F.col("revenue") > 0,
        F.round(
            (
                F.col("contribution_margin") /
                F.col("revenue")
            ) * 100,
            2
        )
    )
)


location_repeat = transaction_base.groupBy(
    "restaurant_id",
    "customer_id"
).agg(
    F.countDistinct(
        "order_id"
    ).alias(
        "customer_location_orders"
    )
).groupBy(
    "restaurant_id"
).agg(
    F.count(
        "*"
    ).alias(
        "location_customers"
    ),
    F.sum(
        F.when(
            F.col("customer_location_orders") > 1,
            1
        ).otherwise(0)
    ).alias(
        "repeat_customers"
    )
).withColumn(
    "repeat_purchase_rate_pct",
    F.when(
        F.col("location_customers") > 0,
        F.round(
            (
                F.col("repeat_customers") /
                F.col("location_customers")
            ) * 100,
            2
        )
    ).otherwise(
        F.lit(0.0)
    )
)


location_ratings = rating_context.groupBy(
    "restaurant_id"
).agg(
    F.count(
        "*"
    ).alias(
        "rating_count"
    ),
    F.round(
        F.avg("rating"),
        3
    ).alias(
        "average_rating"
    ),
    F.round(
        (
            F.sum(
                F.when(
                    F.col("rating") >= 4,
                    1
                ).otherwise(0)
            ) /
            F.count("*")
        ) * 100,
        2
    ).alias(
        "positive_rating_rate_pct"
    ),
    F.round(
        F.avg(
            F.when(
                F.col("promotion_status") == "Promoted",
                F.col("rating")
            )
        ),
        3
    ).alias(
        "promoted_average_rating"
    ),
    F.round(
        F.avg(
            F.when(
                F.col("promotion_status") == "Non-Promoted",
                F.col("rating")
            )
        ),
        3
    ).alias(
        "non_promoted_average_rating"
    )
).withColumn(
    "promotion_rating_gap",
    F.when(
        F.col("promoted_average_rating").isNotNull() &
        F.col("non_promoted_average_rating").isNotNull(),
        F.round(
            F.col("promoted_average_rating") -
            F.col("non_promoted_average_rating"),
            3
        )
    )
)


location_rating_performance = location_sales.join(
    location_repeat,
    "restaurant_id",
    "left"
).join(
    location_ratings,
    "restaurant_id",
    "left"
)




rating_time_promotion = rating_context.withColumn(
    "rating_month",
    F.to_date(
        F.date_trunc(
            "month",
            "rating_date"
        )
    )
).groupBy(
    "rating_month",
    "promotion_status"
).agg(
    F.count(
        "*"
    ).alias(
        "rating_count"
    ),
    F.countDistinct(
        "customer_id"
    ).alias(
        "rating_customers"
    ),
    F.round(
        F.avg("rating"),
        3
    ).alias(
        "average_rating"
    ),
    F.round(
        (
            F.sum(
                F.when(
                    F.col("rating") >= 4,
                    1
                ).otherwise(0)
            ) /
            F.count("*")
        ) * 100,
        2
    ).alias(
        "positive_rating_rate_pct"
    ),
    F.round(
        (
            F.sum(
                F.when(
                    F.col("rating") <= 2,
                    1
                ).otherwise(0)
            ) /
            F.count("*")
        ) * 100,
        2
    ).alias(
        "low_rating_rate_pct"
    )
).orderBy(
    "rating_month",
    "promotion_status"
)



rating_sales_corr = item_rating_performance.stat.corr(
    "average_rating",
    "quantity_sold"
)

rating_revenue_corr = item_rating_performance.stat.corr(
    "average_rating",
    "revenue"
)

rating_margin_corr = item_rating_performance.stat.corr(
    "average_rating",
    "contribution_margin"
)

rating_repeat_corr = item_rating_performance.stat.corr(
    "average_rating",
    "repeat_purchase_rate_pct"
)


print("\n========RATING RELATIONSHIPS========")
print(
    f"Rating vs Quantity Sold: "
    f"{rating_sales_corr:.4f}"
)
print(
    f"Rating vs Revenue: "
    f"{rating_revenue_corr:.4f}"
)
print(
    f"Rating vs Contribution Margin: "
    f"{rating_margin_corr:.4f}"
)
print(
    f"Rating vs Repeat Purchase Rate: "
    f"{rating_repeat_corr:.4f}"
)


print("\n========TOP RATED MENU ITEMS========")
item_rating_performance.select(
    "item_id",
    "item_name",
    "category_name",
    "rating_count",
    "average_rating",
    "positive_rating_rate_pct",
    "quantity_sold",
    "revenue",
    "contribution_margin",
    "profit_percentage",
    "repeat_purchase_rate_pct",
    "promoted_average_rating",
    "non_promoted_average_rating",
    "promotion_rating_gap"
).orderBy(
    F.desc("average_rating"),
    F.desc("rating_count")
).show(
    20,
    truncate=False
)


print("\n========LOCATION SATISFACTION========")
location_rating_performance.select(
    "restaurant_id",
    "restaurant_name",
    "restaurant_city",
    "restaurant_area",
    "rating_count",
    "average_rating",
    "positive_rating_rate_pct",
    "revenue",
    "contribution_margin",
    "profit_percentage",
    "repeat_purchase_rate_pct",
    "promotion_rating_gap"
).orderBy(
    F.desc("average_rating")
).show(
    20,
    truncate=False
)


print("\n========MONTHLY PROMOTION RATING TREND========")
rating_time_promotion.show(
    50,
    truncate=False
)

outputs = {
    "item_rating_performance": item_rating_performance,
    "location_rating_performance": location_rating_performance,
    "rating_time_promotion": rating_time_promotion
}


for name, frame in outputs.items():
    frame.write.mode(
        "overwrite"
    ).parquet(
        f"{output_folder}/{name}"
    )


print("\nRating and satisfaction analysis completed successfully.")


spark.stop()
