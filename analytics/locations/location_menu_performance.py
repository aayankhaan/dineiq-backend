from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import INTEGRATED_DATA_FOLDER, ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("DineIQ Location Specific Menu Performance") \
    .getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "8")


transactions = spark.read.parquet(
    f"{INTEGRATED_DATA_FOLDER}/transactions"
)

ratings = spark.read.parquet(
    f"{INTEGRATED_DATA_FOLDER}/ratings"
)

item_daily_wastage = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/wastage/item_daily_wastage"
)

output_folder = f"{ANALYTICS_DATA_FOLDER}/location_menu_performance"


sales = transactions.filter(
    F.col("order_status") == "Completed"
).withColumn(
    "sale_date",
    F.to_date("order_datetime")
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


dataset_end = sales.agg(
    F.max("sale_date").alias("dataset_end")
).first()["dataset_end"]


base_performance = sales.groupBy(
    "restaurant_id",
    "item_id"
).agg(
    F.first(
        "restaurant_name",
        ignorenulls=True
    ).alias("restaurant_name"),
    F.first(
        "restaurant_city",
        ignorenulls=True
    ).alias("restaurant_city"),
    F.first(
        "restaurant_area",
        ignorenulls=True
    ).alias("restaurant_area"),
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
    ).alias("order_frequency"),
    F.countDistinct(
        "customer_id"
    ).alias("customer_count"),
    F.round(
        F.sum("line_revenue"),
        2
    ).alias("item_revenue"),
    F.round(
        F.sum("line_cost"),
        2
    ).alias("item_cost"),
    F.round(
        F.sum("line_margin"),
        2
    ).alias("contribution_margin")
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
)


customer_item_orders = sales.groupBy(
    "restaurant_id",
    "item_id",
    "customer_id"
).agg(
    F.countDistinct(
        "order_id"
    ).alias("customer_item_orders")
)


repeat_purchase = customer_item_orders.groupBy(
    "restaurant_id",
    "item_id"
).agg(
    F.count(
        "*"
    ).alias("item_customers"),
    F.sum(
        F.when(
            F.col("customer_item_orders") > 1,
            1
        ).otherwise(0)
    ).alias("repeat_customers")
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


location_item_ratings = ratings.groupBy(
    "restaurant_id",
    "item_id"
).agg(
    F.count(
        "*"
    ).alias("rating_count"),
    F.round(
        F.avg(
            F.col("rating").cast("double")
        ),
        3
    ).alias("average_rating")
)


location_item_wastage = item_daily_wastage.groupBy(
    "restaurant_id",
    "item_id"
).agg(
    F.round(
        F.sum(
            F.col("estimated_wastage_cost").cast("double")
        ),
        2
    ).alias("estimated_wastage_cost"),
    F.round(
        F.sum(
            F.col("estimated_wasted_servings").cast("double")
        ),
        2
    ).alias("estimated_wasted_servings"),
    F.round(
        F.sum(
            F.col("estimated_preparation_quantity").cast("double")
        ),
        2
    ).alias("estimated_preparation_quantity")
)


promotion_dependency = sales.groupBy(
    "restaurant_id",
    "item_id"
).agg(
    F.countDistinct(
        "order_id"
    ).alias("promotion_total_orders"),
    F.countDistinct(
        F.when(
            F.col("item_discount_amount") > 0,
            F.col("order_id")
        )
    ).alias("promoted_orders")
).withColumn(
    "promotion_dependency_pct",
    F.when(
        F.col("promotion_total_orders") > 0,
        F.round(
            (
                F.col("promoted_orders") /
                F.col("promotion_total_orders")
            ) * 100,
            2
        )
    ).otherwise(
        F.lit(0.0)
    )
)


recent_sales = sales.withColumn(
    "period",
    F.when(
        F.col("sale_date") >
        F.date_sub(
            F.lit(dataset_end),
            30
        ),
        "Recent"
    ).when(
        (
            F.col("sale_date") >
            F.date_sub(
                F.lit(dataset_end),
                60
            )
        ) &
        (
            F.col("sale_date") <=
            F.date_sub(
                F.lit(dataset_end),
                30
            )
        ),
        "Previous"
    )
).filter(
    F.col("period").isNotNull()
).groupBy(
    "restaurant_id",
    "item_id"
).pivot(
    "period",
    [
        "Previous",
        "Recent"
    ]
).agg(
    F.sum(
        F.col("quantity").cast("double")
    )
).fillna(
    0.0
).withColumnRenamed(
    "Previous",
    "previous_30d_quantity"
).withColumnRenamed(
    "Recent",
    "recent_30d_quantity"
).withColumn(
    "sales_trend_pct",
    F.when(
        F.col("previous_30d_quantity") > 0,
        F.round(
            (
                (
                    F.col("recent_30d_quantity") -
                    F.col("previous_30d_quantity")
                ) /
                F.col("previous_30d_quantity")
            ) * 100,
            2
        )
    ).when(
        F.col("recent_30d_quantity") == 0,
        F.lit(-100.0)
    )
)


location_item_performance = base_performance.join(
    repeat_purchase.select(
        "restaurant_id",
        "item_id",
        "repeat_customers",
        "repeat_purchase_rate_pct"
    ),
    [
        "restaurant_id",
        "item_id"
    ],
    "left"
).join(
    location_item_ratings,
    [
        "restaurant_id",
        "item_id"
    ],
    "left"
).join(
    location_item_wastage,
    [
        "restaurant_id",
        "item_id"
    ],
    "left"
).join(
    promotion_dependency.select(
        "restaurant_id",
        "item_id",
        "promoted_orders",
        "promotion_dependency_pct"
    ),
    [
        "restaurant_id",
        "item_id"
    ],
    "left"
).join(
    recent_sales,
    [
        "restaurant_id",
        "item_id"
    ],
    "left"
).fillna(
    {
        "repeat_customers": 0,
        "repeat_purchase_rate_pct": 0.0,
        "rating_count": 0,
        "estimated_wastage_cost": 0.0,
        "estimated_wasted_servings": 0.0,
        "estimated_preparation_quantity": 0.0,
        "promoted_orders": 0,
        "promotion_dependency_pct": 0.0,
        "previous_30d_quantity": 0.0,
        "recent_30d_quantity": 0.0
    }
).withColumn(
    "wastage_percentage",
    F.when(
        F.col("estimated_preparation_quantity") > 0,
        F.round(
            (
                F.col("estimated_wasted_servings") /
                F.col("estimated_preparation_quantity")
            ) * 100,
            2
        )
    ).otherwise(
        F.lit(0.0)
    )
)


location_thresholds = location_item_performance.groupBy(
    "restaurant_id"
).agg(
    F.expr(
        "percentile_approx(quantity_sold, 0.5)"
    ).alias("location_quantity_median"),
    F.expr(
        "percentile_approx(profit_percentage, 0.5)"
    ).alias("location_profit_median"),
    F.expr(
        "percentile_approx(contribution_margin, 0.5)"
    ).alias("location_margin_median"),
    F.expr(
        "percentile_approx(average_rating, 0.5)"
    ).alias("location_rating_median"),
    F.expr(
        "percentile_approx(repeat_purchase_rate_pct, 0.5)"
    ).alias("location_repeat_median"),
    F.expr(
        "percentile_approx(wastage_percentage, 0.75)"
    ).alias("location_wastage_upper")
)


classified = location_item_performance.join(
    location_thresholds,
    "restaurant_id",
    "inner"
)


high_demand = (
    F.col("quantity_sold") >=
    F.col("location_quantity_median")
)

high_profitability = (
    (
        F.col("profit_percentage") >=
        F.col("location_profit_median")
    ) &
    (
        F.col("contribution_margin") >=
        F.col("location_margin_median")
    )
)

acceptable_wastage = (
    F.col("wastage_percentage") <=
    F.col("location_wastage_upper")
)

strong_quality_signal = (
    F.coalesce(
        F.col("average_rating") >=
        F.col("location_rating_median"),
        F.lit(False)
    ) |
    (
        F.col("repeat_purchase_rate_pct") >=
        F.col("location_repeat_median")
    )
)


classified = classified.withColumn(
    "performance_class",
    F.when(
        high_demand &
        high_profitability &
        acceptable_wastage,
        "Profit Driver"
    ).when(
        high_demand,
        "Volume Driver"
    ).when(
        (
            ~high_demand
        ) &
        acceptable_wastage &
        (
            high_profitability |
            strong_quality_signal
        ),
        "Hidden Opportunity"
    ).otherwise(
        "Low Performer"
    )
)


class_distribution = classified.groupBy(
    "performance_class"
).agg(
    F.count(
        "*"
    ).alias("item_location_count")
).orderBy(
    F.desc("item_location_count")
)


cross_location_differences = classified.groupBy(
    "item_id",
    "item_name"
).agg(
    F.countDistinct(
        "restaurant_id"
    ).alias("location_count"),
    F.countDistinct(
        "performance_class"
    ).alias("distinct_class_count"),
    F.sort_array(
        F.collect_set(
            "performance_class"
        )
    ).alias("location_classes")
).filter(
    F.col("distinct_class_count") > 1
).orderBy(
    F.desc("distinct_class_count"),
    F.desc("location_count"),
    F.asc("item_id")
)


print("\n========LOCATION-SPECIFIC MENU CLASS DISTRIBUTION========")
class_distribution.show(
    truncate=False
)


print("\n========LOCATION-SPECIFIC MENU PERFORMANCE========")
classified.select(
    "restaurant_id",
    "restaurant_name",
    "item_id",
    "item_name",
    "category_name",
    "quantity_sold",
    "item_revenue",
    "contribution_margin",
    "profit_percentage",
    "repeat_purchase_rate_pct",
    "average_rating",
    "wastage_percentage",
    "promotion_dependency_pct",
    "sales_trend_pct",
    "performance_class"
).orderBy(
    "restaurant_id",
    "performance_class",
    F.desc("quantity_sold")
).show(
    50,
    truncate=False
)


print("\n========ITEMS WITH DIFFERENT CLASSES ACROSS LOCATIONS========")
cross_location_differences.show(
    50,
    truncate=False
)


classified.write.mode(
    "overwrite"
).parquet(
    f"{output_folder}/location_menu_performance"
)


cross_location_differences.write.mode(
    "overwrite"
).parquet(
    f"{output_folder}/cross_location_differences"
)


print("\nLocation-specific menu performance completed successfully.")


spark.stop()
