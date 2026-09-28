from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.settings import INTEGRATED_DATA_FOLDER, ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("DineIQ Slow Moving Dish Detection") \
    .getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "8")


transactions = spark.read.parquet(
    f"{INTEGRATED_DATA_FOLDER}/transactions"
)

item_daily_wastage = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/wastage/item_daily_wastage"
)

output_folder = f"{ANALYTICS_DATA_FOLDER}/slow_moving_dishes"


# ==================== SALES BASE ====================


sales = transactions.filter(
    F.col("order_status") == "Completed"
).withColumn(
    "sale_date",
    F.to_date("order_datetime")
).withColumn(
    "line_margin",
    F.col("line_total").cast("double") -
    (
        F.col("item_cost").cast("double") *
        F.col("quantity").cast("double")
    )
)


dataset_end = sales.agg(
    F.max("sale_date").alias("dataset_end")
).first()["dataset_end"]


item_sales = sales.groupBy(
    "restaurant_id",
    "item_id"
).agg(
    F.first(
        "restaurant_name",
        ignorenulls=True
    ).alias("restaurant_name"),
    F.first(
        "item_name",
        ignorenulls=True
    ).alias("item_name"),
    F.first(
        "category_name",
        ignorenulls=True
    ).alias("category_name"),
    F.min(
        "sale_date"
    ).alias("first_sale_date"),
    F.max(
        "sale_date"
    ).alias("last_sale_date"),
    F.sum(
        F.col("quantity").cast("double")
    ).alias("quantity_sold"),
    F.countDistinct(
        "order_id"
    ).alias("purchase_frequency"),
    F.countDistinct(
        "customer_id"
    ).alias("customer_count"),
    F.round(
        F.sum(
            F.col("line_total").cast("double")
        ),
        2
    ).alias("revenue"),
    F.round(
        F.sum("line_margin"),
        2
    ).alias("contribution_margin")
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
).withColumn(
    "days_since_last_purchase",
    F.datediff(
        F.lit(dataset_end),
        F.col("last_sale_date")
    )
)


# ==================== PURCHASE GAPS ====================


purchase_days = sales.select(
    "restaurant_id",
    "item_id",
    "sale_date"
).distinct()


gap_window = Window.partitionBy(
    "restaurant_id",
    "item_id"
).orderBy(
    "sale_date"
)


purchase_gaps = purchase_days.withColumn(
    "previous_sale_date",
    F.lag(
        "sale_date"
    ).over(gap_window)
).withColumn(
    "purchase_gap_days",
    F.datediff(
        "sale_date",
        "previous_sale_date"
    )
).groupBy(
    "restaurant_id",
    "item_id"
).agg(
    F.round(
        F.avg("purchase_gap_days"),
        2
    ).alias("average_purchase_gap_days"),
    F.max(
        "purchase_gap_days"
    ).alias("maximum_purchase_gap_days")
)


# ==================== REPEAT PURCHASE ====================


repeat_purchase = sales.groupBy(
    "restaurant_id",
    "item_id",
    "customer_id"
).agg(
    F.countDistinct(
        "order_id"
    ).alias("customer_item_orders")
).groupBy(
    "restaurant_id",
    "item_id"
).agg(
    F.count(
        "*"
    ).alias("buyers"),
    F.sum(
        F.when(
            F.col("customer_item_orders") > 1,
            1
        ).otherwise(0)
    ).alias("repeat_buyers")
).withColumn(
    "repeat_purchase_rate_pct",
    F.round(
        (
            F.col("repeat_buyers") /
            F.col("buyers")
        ) * 100,
        2
    )
)


# ==================== WASTAGE ====================


wastage = item_daily_wastage.groupBy(
    "restaurant_id",
    "item_id"
).agg(
    F.round(
        F.sum(
            F.col("estimated_wastage_cost").cast("double")
        ),
        2
    ).alias("wastage_cost"),
    F.round(
        F.sum(
            F.col("estimated_preparation_quantity").cast("double")
        ),
        2
    ).alias("estimated_preparation_quantity"),
    F.round(
        F.sum(
            F.col("demand_quantity").cast("double")
        ),
        2
    ).alias("wastage_analysis_demand")
)


# ==================== SALES TREND ====================


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


# ==================== COMBINE ====================


performance = item_sales.join(
    purchase_gaps,
    [
        "restaurant_id",
        "item_id"
    ],
    "left"
).join(
    repeat_purchase,
    [
        "restaurant_id",
        "item_id"
    ],
    "left"
).join(
    wastage,
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
        "average_purchase_gap_days": 0.0,
        "maximum_purchase_gap_days": 0,
        "repeat_purchase_rate_pct": 0.0,
        "wastage_cost": 0.0,
        "estimated_preparation_quantity": 0.0,
        "wastage_analysis_demand": 0.0,
        "previous_30d_quantity": 0.0,
        "recent_30d_quantity": 0.0
    }
).withColumn(
    "wastage_to_margin_pct",
    F.when(
        F.col("contribution_margin") > 0,
        F.round(
            (
                F.col("wastage_cost") /
                F.col("contribution_margin")
            ) * 100,
            2
        )
    ).when(
        F.col("wastage_cost") > 0,
        F.lit(100.0)
    ).otherwise(
        F.lit(0.0)
    )
)


# ==================== DATA-DRIVEN THRESHOLDS ====================


thresholds = performance.agg(
    F.expr(
        "percentile_approx(quantity_sold, 0.25)"
    ).alias("low_sales_threshold"),
    F.expr(
        "percentile_approx(purchase_frequency, 0.25)"
    ).alias("low_frequency_threshold"),
    F.expr(
        "percentile_approx(average_purchase_gap_days, 0.75)"
    ).alias("long_gap_threshold"),
    F.expr(
        "percentile_approx(repeat_purchase_rate_pct, 0.25)"
    ).alias("low_repeat_threshold"),
    F.expr(
        "percentile_approx(wastage_to_margin_pct, 0.75)"
    ).alias("high_wastage_threshold"),
    F.expr(
        "percentile_approx(profit_percentage, 0.25)"
    ).alias("weak_profit_threshold"),
    F.expr(
        "percentile_approx(sales_trend_pct, 0.25)"
    ).alias("poor_trend_threshold")
)


slow_moving_dishes = performance.crossJoin(
    thresholds
).withColumn(
    "low_sales_volume",
    F.col("quantity_sold") <=
    F.col("low_sales_threshold")
).withColumn(
    "low_purchase_frequency",
    F.col("purchase_frequency") <=
    F.col("low_frequency_threshold")
).withColumn(
    "long_purchase_gaps",
    F.col("average_purchase_gap_days") >=
    F.col("long_gap_threshold")
).withColumn(
    "low_repeat_purchase",
    F.col("repeat_purchase_rate_pct") <=
    F.col("low_repeat_threshold")
).withColumn(
    "high_wastage",
    F.when(
        F.col("high_wastage_threshold") > 0,
        F.col("wastage_to_margin_pct") >=
        F.col("high_wastage_threshold")
    ).otherwise(
        F.col("wastage_to_margin_pct") > 0
    )
).withColumn(
    "weak_profitability",
    F.col("profit_percentage") <=
    F.col("weak_profit_threshold")
).withColumn(
    "poor_sales_trend",
    F.coalesce(
        (
            F.col("sales_trend_pct") < 0
        ) &
        (
            F.col("sales_trend_pct") <=
            F.col("poor_trend_threshold")
        ),
        F.lit(False)
    )
).withColumn(
    "slow_signal_count",
    F.col("low_sales_volume").cast("int") +
    F.col("low_purchase_frequency").cast("int") +
    F.col("long_purchase_gaps").cast("int") +
    F.col("low_repeat_purchase").cast("int") +
    F.col("high_wastage").cast("int") +
    F.col("weak_profitability").cast("int") +
    F.col("poor_sales_trend").cast("int")
).withColumn(
    "slow_moving_dish",
    (
        F.col("slow_signal_count") >= 4
    ) &
    (
        F.col("low_sales_volume") |
        F.col("low_purchase_frequency")
    )
).withColumn(
    "slow_moving_severity",
    F.when(
        ~F.col("slow_moving_dish"),
        "None"
    ).when(
        F.col("slow_signal_count") >= 6,
        "Critical"
    ).when(
        F.col("slow_signal_count") == 5,
        "High"
    ).otherwise(
        "Medium"
    )
)


# ==================== SUMMARY ====================


summary = slow_moving_dishes.agg(
    F.count(
        "*"
    ).alias("item_location_pairs"),
    F.sum(
        F.col("slow_moving_dish").cast("int")
    ).alias("slow_moving_pairs"),
    F.sum(
        F.when(
            F.col("slow_moving_severity") == "Critical",
            1
        ).otherwise(0)
    ).alias("critical_slow_movers"),
    F.sum(
        F.when(
            F.col("slow_moving_severity") == "High",
            1
        ).otherwise(0)
    ).alias("high_slow_movers"),
    F.sum(
        F.when(
            F.col("slow_moving_severity") == "Medium",
            1
        ).otherwise(0)
    ).alias("medium_slow_movers")
).withColumn(
    "slow_moving_rate_pct",
    F.round(
        (
            F.col("slow_moving_pairs") /
            F.col("item_location_pairs")
        ) * 100,
        2
    )
)


print("\n========SLOW-MOVING DISH SUMMARY========")
summary.show(
    truncate=False
)


print("\n========TOP SLOW-MOVING DISHES========")
slow_moving_dishes.filter(
    F.col("slow_moving_dish")
).select(
    "restaurant_id",
    "restaurant_name",
    "item_id",
    "item_name",
    "category_name",
    "slow_moving_severity",
    "slow_signal_count",
    "quantity_sold",
    "purchase_frequency",
    "average_purchase_gap_days",
    "days_since_last_purchase",
    "repeat_purchase_rate_pct",
    "wastage_cost",
    "wastage_to_margin_pct",
    "contribution_margin",
    "profit_percentage",
    "previous_30d_quantity",
    "recent_30d_quantity",
    "sales_trend_pct"
).orderBy(
    F.desc("slow_signal_count"),
    F.asc("quantity_sold")
).show(
    30,
    truncate=False
)


# ==================== SAVE ====================


slow_moving_dishes.write.mode(
    "overwrite"
).parquet(
    f"{output_folder}/slow_moving_dishes"
)


summary.write.mode(
    "overwrite"
).parquet(
    f"{output_folder}/slow_moving_summary"
)


print("\nSlow-moving dish detection completed successfully.")


spark.stop()
