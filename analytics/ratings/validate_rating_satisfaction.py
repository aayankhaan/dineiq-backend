from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import INTEGRATED_DATA_FOLDER, ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("Validate DineIQ Rating Satisfaction") \
    .getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")


integrated_folder = INTEGRATED_DATA_FOLDER
output_folder = f"{ANALYTICS_DATA_FOLDER}/rating_satisfaction"


transactions = spark.read.parquet(
    f"{integrated_folder}/transactions"
)

ratings = spark.read.parquet(
    f"{integrated_folder}/ratings"
)

item_rating_performance = spark.read.parquet(
    f"{output_folder}/item_rating_performance"
)

location_rating_performance = spark.read.parquet(
    f"{output_folder}/location_rating_performance"
)

rating_time_promotion = spark.read.parquet(
    f"{output_folder}/rating_time_promotion"
)


passed = 0
failed = 0


def check(name, condition, details=""):
    global passed, failed

    if condition:
        print(f"[PASS] {name}")
        passed += 1
    else:
        suffix = (
            f" - {details}"
            if details
            else ""
        )

        print(
            f"[FAIL] {name}{suffix}"
        )
        failed += 1


print("\n========RATING SATISFACTION VALIDATION========")



for name, frame in [
    ("Item rating performance", item_rating_performance),
    ("Location rating performance", location_rating_performance),
    ("Rating time/promotion", rating_time_promotion)
]:
    check(
        f"{name} contains data",
        frame.count() > 0
    )



required_item_columns = {
    "item_id",
    "item_name",
    "rating_count",
    "average_rating",
    "positive_rating_rate_pct",
    "low_rating_rate_pct",
    "quantity_sold",
    "revenue",
    "contribution_margin",
    "profit_percentage",
    "repeat_purchase_rate_pct",
    "promoted_average_rating",
    "non_promoted_average_rating",
    "promotion_rating_gap"
}


check(
    "Item analysis contains all required Step 29 dimensions",
    required_item_columns.issubset(
        set(
            item_rating_performance.columns
        )
    )
)


check(
    "Item IDs are unique",
    item_rating_performance.groupBy(
        "item_id"
    ).count().filter(
        F.col("count") > 1
    ).count() == 0
)


check(
    "Item ratings stay between one and five",
    item_rating_performance.filter(
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
    "Item positive-rating rates stay between zero and 100",
    item_rating_performance.filter(
        F.col("positive_rating_rate_pct").isNotNull()
    ).filter(
        (
            F.col("positive_rating_rate_pct") < 0
        ) |
        (
            F.col("positive_rating_rate_pct") > 100
        )
    ).count() == 0
)


check(
    "Item repeat-purchase rates stay between zero and 100",
    item_rating_performance.filter(
        (
            F.col("repeat_purchase_rate_pct") < 0
        ) |
        (
            F.col("repeat_purchase_rate_pct") > 100
        )
    ).count() == 0
)


check(
    "Item sales and profitability values are non-negative where expected",
    item_rating_performance.filter(
        (
            F.col("quantity_sold") < 0
        ) |
        (
            F.col("revenue") < 0
        )
    ).count() == 0
)



required_location_columns = {
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
}


check(
    "Location analysis contains all required Step 29 dimensions",
    required_location_columns.issubset(
        set(
            location_rating_performance.columns
        )
    )
)


check(
    "Restaurant IDs are unique",
    location_rating_performance.groupBy(
        "restaurant_id"
    ).count().filter(
        F.col("count") > 1
    ).count() == 0
)


check(
    "Location ratings stay between one and five",
    location_rating_performance.filter(
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
    "Location repeat-purchase rates stay between zero and 100",
    location_rating_performance.filter(
        (
            F.col("repeat_purchase_rate_pct") < 0
        ) |
        (
            F.col("repeat_purchase_rate_pct") > 100
        )
    ).count() == 0
)


required_time_columns = {
    "rating_month",
    "promotion_status",
    "rating_count",
    "rating_customers",
    "average_rating",
    "positive_rating_rate_pct",
    "low_rating_rate_pct"
}


check(
    "Time/promotion analysis contains required fields",
    required_time_columns.issubset(
        set(
            rating_time_promotion.columns
        )
    )
)


promotion_statuses = {
    row["promotion_status"]
    for row in rating_time_promotion.select(
        "promotion_status"
    ).distinct().collect()
}


check(
    "Promotion status uses Promoted and Non-Promoted classes",
    promotion_statuses.issubset(
        {
            "Promoted",
            "Non-Promoted"
        }
    ) and
    len(
        promotion_statuses
    ) == 2,
    f"Found: {sorted(promotion_statuses)}"
)


check(
    "Monthly ratings stay between one and five",
    rating_time_promotion.filter(
        (
            F.col("average_rating") < 1
        ) |
        (
            F.col("average_rating") > 5
        )
    ).count() == 0
)


check(
    "Monthly rating counts are positive",
    rating_time_promotion.filter(
        F.col("rating_count") <= 0
    ).count() == 0
)


check(
    "Time/promotion rows are unique",
    rating_time_promotion.groupBy(
        "rating_month",
        "promotion_status"
    ).count().filter(
        F.col("count") > 1
    ).count() == 0
)


completed_transaction_keys = transactions.filter(
    F.col("order_status") == "Completed"
).select(
    "order_id",
    "customer_id",
    "restaurant_id",
    "item_id"
).distinct()


matched_rating_count = ratings.select(
    "rating_id",
    "order_id",
    "customer_id",
    "restaurant_id",
    "item_id"
).join(
    completed_transaction_keys,
    [
        "order_id",
        "customer_id",
        "restaurant_id",
        "item_id"
    ],
    "inner"
).select(
    "rating_id"
).distinct().count()


output_rating_count = rating_time_promotion.agg(
    F.sum(
        "rating_count"
    ).alias(
        "total"
    )
).first()["total"]


check(
    "Time/promotion rating count reconciles to valid rating purchases",
    int(output_rating_count) ==
    matched_rating_count,
    (
        f"Output: {output_rating_count}, "
        f"matched source ratings: {matched_rating_count}"
    )
)


check(
    "All analyzed items exist in completed transactions",
    item_rating_performance.select(
        "item_id"
    ).join(
        completed_transaction_keys.select(
            "item_id"
        ).distinct(),
        "item_id",
        "left_anti"
    ).count() == 0
)


check(
    "All analyzed locations exist in completed transactions",
    location_rating_performance.select(
        "restaurant_id"
    ).join(
        completed_transaction_keys.select(
            "restaurant_id"
        ).distinct(),
        "restaurant_id",
        "left_anti"
    ).count() == 0
)


total_checks = passed + failed


print(
    f"\n========VALIDATION RESULT: "
    f"{passed}/{total_checks} PASS========"
)


if failed == 0:
    print(
        "\nRating satisfaction validation PASSED."
    )
else:
    print(
        "\nRating satisfaction validation FAILED."
    )


spark.stop()
