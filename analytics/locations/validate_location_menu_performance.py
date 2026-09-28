from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("Validate DineIQ Location Specific Menu Performance") \
    .getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")


output_folder = f"{ANALYTICS_DATA_FOLDER}/location_menu_performance"


performance = spark.read.parquet(
    f"{output_folder}/location_menu_performance"
)

differences = spark.read.parquet(
    f"{output_folder}/cross_location_differences"
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


print("\n========LOCATION-SPECIFIC MENU PERFORMANCE VALIDATION========")


check(
    "Location menu performance contains data",
    performance.count() > 0
)


check(
    "Restaurant item pairs are unique",
    performance.groupBy(
        "restaurant_id",
        "item_id"
    ).count().filter(
        F.col("count") != 1
    ).count() == 0
)


allowed_classes = {
    "Profit Driver",
    "Volume Driver",
    "Hidden Opportunity",
    "Low Performer"
}


check(
    "Only required Step 34 classes are used",
    performance.filter(
        ~F.col("performance_class").isin(
            list(allowed_classes)
        )
    ).count() == 0
)


for class_name in sorted(allowed_classes):
    check(
        f"{class_name} is identified",
        performance.filter(
            F.col("performance_class") ==
            class_name
        ).count() > 0
    )


required_metrics = {
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
}


check(
    "Classification includes supporting performance metrics",
    required_metrics.issubset(
        set(performance.columns)
    )
)


check(
    "Quantity sold is positive",
    performance.filter(
        F.col("quantity_sold") <= 0
    ).count() == 0
)


check(
    "Revenue is positive",
    performance.filter(
        F.col("item_revenue") <= 0
    ).count() == 0
)


check(
    "Repeat purchase rate stays between zero and 100",
    performance.filter(
        (
            F.col("repeat_purchase_rate_pct") < 0
        ) |
        (
            F.col("repeat_purchase_rate_pct") > 100
        )
    ).count() == 0
)


check(
    "Ratings stay between one and five when available",
    performance.filter(
        F.col("average_rating").isNotNull()
    ).filter(
        (
            F.col("average_rating") < 1
        ) |
        (
            F.col("average_rating") > 5
        )
    ).count() == 0
)


check(
    "Wastage percentage stays between zero and 100",
    performance.filter(
        (
            F.col("wastage_percentage") < 0
        ) |
        (
            F.col("wastage_percentage") > 100
        )
    ).count() == 0
)


check(
    "Promotion dependency stays between zero and 100",
    performance.filter(
        (
            F.col("promotion_dependency_pct") < 0
        ) |
        (
            F.col("promotion_dependency_pct") > 100
        )
    ).count() == 0
)


profit_driver_errors = performance.filter(
    F.col("performance_class") ==
    "Profit Driver"
).filter(
    ~(
        (
            F.col("quantity_sold") >=
            F.col("location_quantity_median")
        ) &
        (
            F.col("profit_percentage") >=
            F.col("location_profit_median")
        ) &
        (
            F.col("contribution_margin") >=
            F.col("location_margin_median")
        ) &
        (
            F.col("wastage_percentage") <=
            F.col("location_wastage_upper")
        )
    )
).count()


check(
    "Profit Drivers satisfy high demand high profitability and acceptable wastage",
    profit_driver_errors == 0
)


volume_driver_errors = performance.filter(
    F.col("performance_class") ==
    "Volume Driver"
).filter(
    F.col("quantity_sold") <
    F.col("location_quantity_median")
).count()


check(
    "Volume Drivers satisfy high local demand",
    volume_driver_errors == 0
)


hidden_opportunity_errors = performance.filter(
    F.col("performance_class") ==
    "Hidden Opportunity"
).filter(
    (
        F.col("quantity_sold") >=
        F.col("location_quantity_median")
    ) |
    (
        F.col("wastage_percentage") >
        F.col("location_wastage_upper")
    )
).count()


check(
    "Hidden Opportunities have lower demand and acceptable wastage",
    hidden_opportunity_errors == 0
)


check(
    "Cross location differences contain data",
    differences.count() > 0
)


check(
    "Difference rows represent multiple locations",
    differences.filter(
        F.col("location_count") < 2
    ).count() == 0
)


check(
    "Difference rows contain more than one class",
    differences.filter(
        F.col("distinct_class_count") < 2
    ).count() == 0
)


actual_differences = performance.groupBy(
    "item_id"
).agg(
    F.countDistinct(
        "performance_class"
    ).alias("class_count")
).filter(
    F.col("class_count") > 1
).count()


check(
    "Difference output matches classified data",
    differences.select(
        "item_id"
    ).distinct().count() ==
    actual_differences
)


total = passed + failed


print(
    f"\n========VALIDATION RESULT: "
    f"{passed}/{total} PASS========"
)


if failed == 0:
    print(
        "\nLocation-specific menu performance validation PASSED."
    )
else:
    print(
        "\nLocation-specific menu performance validation FAILED."
    )


spark.stop()
