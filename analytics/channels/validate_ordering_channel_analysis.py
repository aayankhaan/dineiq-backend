from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import INTEGRATED_DATA_FOLDER, ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("Validate DineIQ Ordering Channel Analysis") \
    .getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")


output_folder = f"{ANALYTICS_DATA_FOLDER}/ordering_channel_analysis"


summary = spark.read.parquet(
    f"{output_folder}/channel_summary"
)

preferences = spark.read.parquet(
    f"{output_folder}/channel_menu_preferences"
)

peaks = spark.read.parquet(
    f"{output_folder}/channel_peak_periods"
)

transactions = spark.read.parquet(
    f"{INTEGRATED_DATA_FOLDER}/transactions"
)


passed = 0
failed = 0


def check(name, condition):
    global passed, failed

    if condition:
        print(f"[PASS] {name}")
        passed += 1
    else:
        print(f"[FAIL] {name}")
        failed += 1


print("\n========ORDERING CHANNEL VALIDATION========")


check(
    "Channel summary contains data",
    summary.count() > 0
)


check(
    "Menu preference output contains data",
    preferences.count() > 0
)


check(
    "Peak period output contains data",
    peaks.count() > 0
)


source_channels = transactions.filter(
    F.col("order_status") == "Completed"
).select(
    "ordering_channel"
).distinct()


check(
    "Every completed-order channel is represented",
    summary.select(
        "ordering_channel"
    ).distinct().count() ==
    source_channels.count()
)


check(
    "Channel rows are unique",
    summary.groupBy(
        "ordering_channel"
    ).count().filter(
        F.col("count") != 1
    ).count() == 0
)


expected_channels = {
    "Dine-in",
    "Takeaway",
    "Website/App",
    "Third-Party Delivery"
}


actual_channels = {
    row["ordering_channel"]
    for row in source_channels.collect()
}


check(
    "Known dataset channels are present",
    expected_channels.issubset(
        actual_channels
    )
)


required_metrics = {
    "average_basket_size",
    "average_order_value",
    "average_discount_amount",
    "average_discount_pct",
    "promotion_order_rate_pct",
    "profit_percentage",
    "top_category_name",
    "peak_order_hour",
    "peak_day_of_week"
}


check(
    "All Step 35 comparison metrics are present",
    required_metrics.issubset(
        set(summary.columns)
    )
)


check(
    "Average basket size is positive",
    summary.filter(
        F.col("average_basket_size") <= 0
    ).count() == 0
)


check(
    "Average order value is positive",
    summary.filter(
        F.col("average_order_value") <= 0
    ).count() == 0
)


check(
    "Average discount percentage is non-negative",
    summary.filter(
        F.col("average_discount_pct") < 0
    ).count() == 0
)


check(
    "Promotion order rate stays between zero and 100",
    summary.filter(
        (
            F.col("promotion_order_rate_pct") < 0
        ) |
        (
            F.col("promotion_order_rate_pct") > 100
        )
    ).count() == 0
)


check(
    "Profit percentage is available",
    summary.filter(
        F.col("profit_percentage").isNull()
    ).count() == 0
)


check(
    "Order shares sum to approximately 100 percent",
    abs(
        float(
            summary.agg(
                F.sum("order_share_pct").alias("value")
            ).first()["value"]
        ) - 100.0
    ) <= 0.05
)


check(
    "Revenue shares sum to approximately 100 percent",
    abs(
        float(
            summary.agg(
                F.sum("revenue_share_pct").alias("value")
            ).first()["value"]
        ) - 100.0
    ) <= 0.05
)


check(
    "Average order value reconciles with revenue and order count",
    summary.filter(
        F.abs(
            F.col("average_order_value") -
            (
                F.col("channel_revenue") /
                F.col("order_count")
            )
        ) > 0.02
    ).count() == 0
)


check(
    "Profit percentage reconciles with item revenue and contribution margin",
    summary.filter(
        F.col("item_revenue") > 0
    ).filter(
        F.abs(
            F.col("profit_percentage") -
            (
                (
                    F.col("contribution_margin") /
                    F.col("item_revenue")
                ) * 100
            )
        ) > 0.02
    ).count() == 0
)


check(
    "Menu preferences have ranks one through ten only",
    preferences.filter(
        (
            F.col("preference_rank") < 1
        ) |
        (
            F.col("preference_rank") > 10
        )
    ).count() == 0
)


check(
    "Each channel has ten menu preferences",
    preferences.groupBy(
        "ordering_channel"
    ).count().filter(
        F.col("count") != 10
    ).count() == 0
)


check(
    "Preference quantities are positive",
    preferences.filter(
        F.col("quantity_sold") <= 0
    ).count() == 0
)


check(
    "Peak period rows are unique by channel",
    peaks.groupBy(
        "ordering_channel"
    ).count().filter(
        F.col("count") != 1
    ).count() == 0
)


check(
    "Peak hours stay between zero and 23",
    peaks.filter(
        (
            F.col("peak_order_hour") < 0
        ) |
        (
            F.col("peak_order_hour") > 23
        )
    ).count() == 0
)


check(
    "Peak days are populated",
    peaks.filter(
        F.col("peak_day_of_week").isNull()
    ).count() == 0
)


check(
    "Peak dayparts use valid labels",
    peaks.filter(
        ~F.col("peak_daypart").isin(
            "Morning",
            "Lunch",
            "Dinner",
            "Late Night"
        )
    ).count() == 0
)


total = passed + failed


print(
    f"\n========VALIDATION RESULT: "
    f"{passed}/{total} PASS========"
)


if failed == 0:
    print(
        "\nOrdering channel validation PASSED."
    )
else:
    print(
        "\nOrdering channel validation FAILED."
    )


spark.stop()
