from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("DineIQ Tricky Menu Cases Validation") \
    .getOrCreate()

analytics_folder = "analytics_data"

tricky_cases = spark.read.parquet(
    f"{analytics_folder}/tricky_menu_cases"
)

menu_classification = spark.read.parquet(
    f"{analytics_folder}/menu_classification"
)

case_columns = [
    "high_selling_loss_making",
    "highly_profitable_rarely_purchased",
    "popular_excessive_wastage",
    "highly_rated_poor_profitability",
    "low_rated_high_sales",
    "promotion_dependent",
    "different_across_locations",
    "weekend_performer",
    "seasonal_item",
    "insufficient_history"
]

required_columns = [
    "item_id",
    "item_name",
    "performance_class",
    "location_variation",
    "weekend_lift",
    "seasonal_variation",
    "first_order_date",
    "last_order_date",
    "history_months",
    *case_columns,
    "tricky_case_count"
]

validation_results = []

def add_result(check, count):
    validation_results.append({
        "check": check,
        "count": count,
        "status": "PASS" if count == 0 else "FAIL"
    })

missing_columns = [
    column for column in required_columns
    if column not in tricky_cases.columns
]

add_result(
    "Missing required tricky-case columns",
    len(missing_columns)
)

add_result(
    "Duplicate item IDs",
    tricky_cases.groupBy(
        "item_id"
    ).count().filter(
        F.col("count") > 1
    ).count()
)

add_result(
    "Missing classified menu items",
    menu_classification.select(
        "item_id"
    ).join(
        tricky_cases.select("item_id"),
        "item_id",
        "left_anti"
    ).count()
)

add_result(
    "Unexpected tricky-case menu items",
    tricky_cases.select(
        "item_id"
    ).join(
        menu_classification.select("item_id"),
        "item_id",
        "left_anti"
    ).count()
)

add_result(
    "Null tricky-case flags",
    tricky_cases.filter(
        F.greatest(
            *[
                F.col(column).isNull().cast("int")
                for column in case_columns
            ]
        ) == 1
    ).count()
)

add_result(
    "Invalid history values",
    tricky_cases.filter(
        (F.col("history_months") <= 0) |
        (F.col("last_order_date") < F.col("first_order_date"))
    ).count()
)

add_result(
    "Invalid location variation",
    tricky_cases.filter(
        F.col("location_variation") < 0
    ).count()
)

add_result(
    "Invalid weekend lift",
    tricky_cases.filter(
        F.col("weekend_lift") < 0
    ).count()
)

add_result(
    "Invalid seasonal variation",
    tricky_cases.filter(
        F.col("seasonal_variation") < 0
    ).count()
)

calculated_case_count = sum(
    F.col(column).cast("int")
    for column in case_columns
)

add_result(
    "Incorrect tricky-case counts",
    tricky_cases.filter(
        F.col("tricky_case_count") != calculated_case_count
    ).count()
)

add_result(
    "High-selling loss-making logic violations",
    tricky_cases.filter(
        F.col("high_selling_loss_making") &
        (F.col("contribution_margin") >= 0)
    ).count()
)

add_result(
    "Popular excessive-wastage logic violations",
    tricky_cases.filter(
        F.col("popular_excessive_wastage") &
        F.col("acceptable_wastage")
    ).count()
)

add_result(
    "Weekend performer logic violations",
    tricky_cases.filter(
        F.col("weekend_performer") &
        (F.col("weekend_lift") <= 1)
    ).count()
)

add_result(
    "Insufficient-history logic violations",
    tricky_cases.filter(
        F.col("insufficient_history") &
        (F.col("history_months") >= 3)
    ).count()
)

missing_case_types = sum(
    1 for column in case_columns
    if tricky_cases.filter(F.col(column)).count() == 0
)

add_result(
    "Required SRS case types with no detected examples",
    missing_case_types
)

add_result(
    "Tricky-case row count mismatch",
    abs(
        tricky_cases.count() -
        menu_classification.count()
    )
)

print("\n========TRICKY MENU CASES VALIDATION========\n")

for result in validation_results:
    print(
        f"{result['status']} | "
        f"{result['check']}: "
        f"{result['count']}"
    )

passed = sum(
    result["status"] == "PASS"
    for result in validation_results
)

print(
    f"\nValidation Result: "
    f"{passed}/{len(validation_results)} checks passed"
)

if passed == len(validation_results):
    print(
        "Tricky menu performance analysis is complete "
        "and covers all 10 required SRS scenarios."
    )
else:
    print(
        "Tricky menu case validation failed. "
        "Review failed checks before continuing."
    )

spark.stop()
