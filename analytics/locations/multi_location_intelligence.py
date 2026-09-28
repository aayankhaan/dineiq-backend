from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.settings import INTEGRATED_DATA_FOLDER, ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("DineIQ Multi Location Intelligence") \
    .getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "8")


output_folder = f"{ANALYTICS_DATA_FOLDER}/multi_location_intelligence"


transactions = spark.read.parquet(
    f"{INTEGRATED_DATA_FOLDER}/transactions"
)

ratings = spark.read.parquet(
    f"{INTEGRATED_DATA_FOLDER}/ratings"
)

location_wastage = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/wastage/location_wastage"
)

promotion_effectiveness = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/promotion_effectiveness/promotion_effectiveness"
)


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
    "restaurant_id"
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
        "total_amount",
        ignorenulls=True
    ).cast("double").alias("order_total")
)


location_orders = order_level.groupBy(
    "restaurant_id"
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
    F.countDistinct(
        "order_id"
    ).alias("total_orders"),
    F.countDistinct(
        "customer_id"
    ).alias("customer_count"),
    F.round(
        F.sum("order_total"),
        2
    ).alias("revenue"),
    F.round(
        F.avg("order_total"),
        2
    ).alias("average_order_value")
)


location_profitability = sales.groupBy(
    "restaurant_id"
).agg(
    F.sum(
        F.col("quantity").cast("double")
    ).alias("items_sold"),
    F.round(
        F.sum("line_revenue"),
        2
    ).alias("item_revenue"),
    F.round(
        F.sum("line_cost"),
        2
    ).alias("estimated_item_cost"),
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




customer_location_orders = order_level.groupBy(
    "restaurant_id",
    "customer_id"
).agg(
    F.countDistinct(
        "order_id"
    ).alias("customer_orders")
)


location_repeat = customer_location_orders.groupBy(
    "restaurant_id"
).agg(
    F.count(
        "*"
    ).alias("location_customers"),
    F.sum(
        F.when(
            F.col("customer_orders") > 1,
            1
        ).otherwise(0)
    ).alias("repeat_customers")
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


location_ratings = ratings.groupBy(
    "restaurant_id"
).agg(
    F.count(
        "*"
    ).alias("rating_count"),
    F.round(
        F.avg(
            F.col("rating").cast("double")
        ),
        3
    ).alias("average_rating"),
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
    ).alias("positive_rating_rate_pct")
)



location_wastage_metrics = location_wastage.select(
    "restaurant_id",
    F.col(
        "estimated_wastage_cost"
    ).cast(
        "double"
    ).alias(
        "estimated_wastage_cost"
    ),
    F.col(
        "estimated_wasted_servings"
    ).cast(
        "double"
    ).alias(
        "estimated_wasted_servings"
    ),
    F.col(
        "estimated_preparation_quantity"
    ).cast(
        "double"
    ).alias(
        "estimated_preparation_quantity"
    ),
    F.col(
        "preparation_surplus_pct"
    ).cast(
        "double"
    ).alias(
        "preparation_surplus_pct"
    )
)




location_promotions = promotion_effectiveness.groupBy(
    "restaurant_id"
).agg(
    F.count(
        "*"
    ).alias("promotion_count"),
    F.sum(
        F.col("promotion_used").cast("int")
    ).alias("used_promotion_count"),
    F.sum(
        F.when(
            F.col("promotion_assessment") == "Effective",
            1
        ).otherwise(0)
    ).alias("effective_promotions"),
    F.sum(
        F.when(
            F.col("promotion_assessment") == "Mixed",
            1
        ).otherwise(0)
    ).alias("mixed_promotions"),
    F.sum(
        F.when(
            F.col("promotion_assessment") == "Ineffective",
            1
        ).otherwise(0)
    ).alias("ineffective_promotions"),
    F.sum(
        F.when(
            F.col("promotion_assessment") == "No Usage",
            1
        ).otherwise(0)
    ).alias("unused_promotions"),
    F.round(
        F.avg(
            F.col("positive_kpi_count").cast("double")
        ),
        2
    ).alias("average_promotion_positive_kpis"),
    F.round(
        F.avg(
            F.when(
                F.col("promotion_used"),
                F.col("margin_lift_pct").cast("double")
            )
        ),
        2
    ).alias("average_promotion_margin_lift_pct")
).withColumn(
    "promotion_effective_rate_pct",
    F.when(
        F.col("used_promotion_count") > 0,
        F.round(
            (
                F.col("effective_promotions") /
                F.col("used_promotion_count")
            ) * 100,
            2
        )
    )
)



item_location_performance = sales.groupBy(
    "restaurant_id",
    "item_id"
).agg(
    F.sum(
        F.col("quantity").cast("double")
    ).alias("item_quantity_sold"),
    F.round(
        F.sum("line_revenue"),
        2
    ).alias("item_revenue"),
    F.round(
        F.sum("line_margin"),
        2
    ).alias("item_contribution_margin")
).withColumn(
    "item_profit_percentage",
    F.when(
        F.col("item_revenue") > 0,
        F.round(
            (
                F.col("item_contribution_margin") /
                F.col("item_revenue")
            ) * 100,
            2
        )
    )
)


location_menu_performance = item_location_performance.groupBy(
    "restaurant_id"
).agg(
    F.countDistinct(
        "item_id"
    ).alias("active_menu_items"),
    F.sum(
        F.when(
            F.col("item_contribution_margin") > 0,
            1
        ).otherwise(0)
    ).alias("profitable_menu_items"),
    F.sum(
        F.when(
            F.col("item_contribution_margin") < 0,
            1
        ).otherwise(0)
    ).alias("loss_making_menu_items"),
    F.sum(
        F.when(
            F.col("item_contribution_margin") == 0,
            1
        ).otherwise(0)
    ).alias("break_even_menu_items"),
    F.round(
        F.avg("item_profit_percentage"),
        2
    ).alias("average_item_profit_percentage"),
    F.round(
        F.expr(
            "percentile_approx(item_revenue, 0.5)"
        ),
        2
    ).alias("median_item_revenue"),
    F.round(
        F.max("item_revenue"),
        2
    ).alias("top_item_revenue"),
    F.round(
        F.sum("item_revenue"),
        2
    ).alias("menu_item_revenue")
).withColumn(
    "profitable_menu_item_rate_pct",
    F.when(
        F.col("active_menu_items") > 0,
        F.round(
            (
                F.col("profitable_menu_items") /
                F.col("active_menu_items")
            ) * 100,
            2
        )
    ).otherwise(
        F.lit(0.0)
    )
).withColumn(
    "top_item_revenue_share_pct",
    F.when(
        F.col("menu_item_revenue") > 0,
        F.round(
            (
                F.col("top_item_revenue") /
                F.col("menu_item_revenue")
            ) * 100,
            2
        )
    ).otherwise(
        F.lit(0.0)
    )
)



location_comparison = location_orders.join(
    location_profitability,
    "restaurant_id",
    "left"
).join(
    location_repeat.select(
        "restaurant_id",
        "repeat_customers",
        "repeat_purchase_rate_pct"
    ),
    "restaurant_id",
    "left"
).join(
    location_wastage_metrics,
    "restaurant_id",
    "left"
).join(
    location_ratings,
    "restaurant_id",
    "left"
).join(
    location_promotions,
    "restaurant_id",
    "left"
).join(
    location_menu_performance,
    "restaurant_id",
    "left"
).fillna(
    {
        "items_sold": 0.0,
        "item_revenue": 0.0,
        "estimated_item_cost": 0.0,
        "contribution_margin": 0.0,
        "repeat_customers": 0,
        "repeat_purchase_rate_pct": 0.0,
        "estimated_wastage_cost": 0.0,
        "estimated_wasted_servings": 0.0,
        "estimated_preparation_quantity": 0.0,
        "preparation_surplus_pct": 0.0,
        "rating_count": 0,
        "positive_rating_rate_pct": 0.0,
        "promotion_count": 0,
        "used_promotion_count": 0,
        "effective_promotions": 0,
        "mixed_promotions": 0,
        "ineffective_promotions": 0,
        "unused_promotions": 0,
        "average_promotion_positive_kpis": 0.0,
        "active_menu_items": 0,
        "profitable_menu_items": 0,
        "loss_making_menu_items": 0,
        "break_even_menu_items": 0,
        "average_item_profit_percentage": 0.0,
        "median_item_revenue": 0.0,
        "top_item_revenue": 0.0,
        "menu_item_revenue": 0.0,
        "profitable_menu_item_rate_pct": 0.0,
        "top_item_revenue_share_pct": 0.0
    }
).withColumn(
    "wastage_cost_pct_of_item_revenue",
    F.when(
        F.col("item_revenue") > 0,
        F.round(
            (
                F.col("estimated_wastage_cost") /
                F.col("item_revenue")
            ) * 100,
            2
        )
    ).otherwise(
        F.lit(0.0)
    )
)



location_comparison = location_comparison.withColumn(
    "revenue_rank",
    F.dense_rank().over(
        Window.orderBy(
            F.desc("revenue")
        )
    )
).withColumn(
    "profitability_rank",
    F.dense_rank().over(
        Window.orderBy(
            F.desc("profit_percentage")
        )
    )
).withColumn(
    "average_order_value_rank",
    F.dense_rank().over(
        Window.orderBy(
            F.desc("average_order_value")
        )
    )
).withColumn(
    "customer_count_rank",
    F.dense_rank().over(
        Window.orderBy(
            F.desc("customer_count")
        )
    )
).withColumn(
    "repeat_purchase_rank",
    F.dense_rank().over(
        Window.orderBy(
            F.desc("repeat_purchase_rate_pct")
        )
    )
).withColumn(
    "wastage_rank",
    F.dense_rank().over(
        Window.orderBy(
            F.asc("wastage_cost_pct_of_item_revenue")
        )
    )
).withColumn(
    "rating_rank",
    F.dense_rank().over(
        Window.orderBy(
            F.col("average_rating").desc_nulls_last()
        )
    )
).withColumn(
    "promotion_effectiveness_rank",
    F.when(
        F.col("promotion_effective_rate_pct").isNotNull(),
        F.dense_rank().over(
            Window.orderBy(
                F.col(
                    "promotion_effective_rate_pct"
                ).desc_nulls_last()
            )
        )
    )
).withColumn(
    "menu_performance_rank",
    F.dense_rank().over(
        Window.orderBy(
            F.desc("profitable_menu_item_rate_pct")
        )
    )
)




network_summary = location_comparison.agg(
    F.count(
        "*"
    ).alias("location_count"),
    F.sum(
        "total_orders"
    ).alias("network_orders"),
    F.round(
        F.sum("revenue"),
        2
    ).alias("network_revenue"),
    F.round(
        F.sum("item_revenue"),
        2
    ).alias("network_item_revenue"),
    F.round(
        F.sum("contribution_margin"),
        2
    ).alias("network_contribution_margin"),
    F.round(
        F.sum("estimated_wastage_cost"),
        2
    ).alias("network_estimated_wastage_cost"),
    F.sum(
        "promotion_count"
    ).alias("network_promotion_count"),
    F.sum(
        "used_promotion_count"
    ).alias("network_used_promotion_count"),
    F.sum(
        "effective_promotions"
    ).alias("network_effective_promotions"),
    F.round(
        F.avg("repeat_purchase_rate_pct"),
        2
    ).alias("average_location_repeat_purchase_rate_pct"),
    F.round(
        F.avg("profitable_menu_item_rate_pct"),
        2
    ).alias("average_profitable_menu_item_rate_pct")
).withColumn(
    "network_profit_percentage",
    F.when(
        F.col("network_item_revenue") > 0,
        F.round(
            (
                F.col("network_contribution_margin") /
                F.col("network_item_revenue")
            ) * 100,
            2
        )
    )
).withColumn(
    "network_average_order_value",
    F.when(
        F.col("network_orders") > 0,
        F.round(
            F.col("network_revenue") /
            F.col("network_orders"),
            2
        )
    )
).withColumn(
    "network_promotion_effective_rate_pct",
    F.when(
        F.col("network_used_promotion_count") > 0,
        F.round(
            (
                F.col("network_effective_promotions") /
                F.col("network_used_promotion_count")
            ) * 100,
            2
        )
    )
)


network_rating = ratings.agg(
    F.round(
        F.avg(
            F.col("rating").cast("double")
        ),
        3
    ).alias("network_average_rating")
)


network_customers = order_level.agg(
    F.countDistinct(
        "customer_id"
    ).alias("network_customer_count")
)


network_summary = network_summary.crossJoin(
    network_rating
).crossJoin(
    network_customers
)



print("\n========MULTI-LOCATION NETWORK SUMMARY========")
network_summary.show(
    truncate=False
)


print("\n========LOCATION COMPARISON========")
location_comparison.select(
    "restaurant_id",
    "restaurant_name",
    "restaurant_city",
    "restaurant_area",
    "total_orders",
    "revenue",
    "contribution_margin",
    "profit_percentage",
    "average_order_value",
    "customer_count",
    "repeat_purchase_rate_pct",
    "estimated_wastage_cost",
    "wastage_cost_pct_of_item_revenue",
    "average_rating",
    "promotion_count",
    "used_promotion_count",
    "effective_promotions",
    "promotion_effective_rate_pct",
    "active_menu_items",
    "profitable_menu_item_rate_pct",
    "revenue_rank",
    "profitability_rank",
    "average_order_value_rank",
    "customer_count_rank",
    "repeat_purchase_rank",
    "wastage_rank",
    "rating_rank",
    "promotion_effectiveness_rank",
    "menu_performance_rank"
).orderBy(
    "revenue_rank",
    "restaurant_id"
).show(
    50,
    truncate=False
)




location_comparison.write.mode(
    "overwrite"
).parquet(
    f"{output_folder}/location_comparison"
)


network_summary.write.mode(
    "overwrite"
).parquet(
    f"{output_folder}/network_summary"
)


print("\nMulti-location intelligence completed successfully.")


spark.stop()
