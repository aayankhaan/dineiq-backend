from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("DineIQ Menu Classification Validation") \
    .getOrCreate()

classification = spark.read.parquet(
    "analytics_data/menu_classification"
)

menu_profitability = spark.read.parquet(
    "analytics_data/menu_profitability"
)

valid_classes = [
    "Profit Driver",
    "Volume Driver",
    "Hidden Opportunity",
    "Low Performer"
]

required_columns = [
    "item_id",
    "item_name",
    "quantity_sold",
    "item_revenue",
    "cost",
    "contribution_margin",
    "profit_percentage",
    "average_rating",
    "repeat_purchase_rate",
    "wastage_percentage",
    "promotion_dependency",
    "sales_trend",
    "high_demand",
    "high_profitability",
    "good_rating",
    "strong_repeat_purchase",
    "acceptable_wastage",
    "performance_class"
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
    if column not in classification.columns
]

add_result(
    "Missing required classification columns",
    len(missing_columns)
)

add_result(
    "Duplicate item IDs",
    classification.groupBy(
        "item_id"
    ).count().filter(
        F.col("count") > 1
    ).count()
)

add_result(
    "Missing profitability items",
    menu_profitability.select(
        "item_id"
    ).join(
        classification.select("item_id"),
        "item_id",
        "left_anti"
    ).count()
)

add_result(
    "Unexpected classification items",
    classification.select(
        "item_id"
    ).join(
        menu_profitability.select("item_id"),
        "item_id",
        "left_anti"
    ).count()
)

add_result(
    "Null performance classes",
    classification.filter(
        F.col("performance_class").isNull()
    ).count()
)

add_result(
    "Invalid performance classes",
    classification.filter(
        ~F.col("performance_class").isin(valid_classes)
    ).count()
)

add_result(
    "Missing classification categories",
    len(
        set(valid_classes) -
        set(
            row["performance_class"]
            for row in classification.select(
                "performance_class"
            ).distinct().collect()
        )
    )
)

add_result(
    "Profit Driver rule violations",
    classification.filter(
        (F.col("performance_class") == "Profit Driver") &
        ~(
            F.col("high_demand") &
            F.col("high_profitability") &
            F.col("acceptable_wastage")
        )
    ).count()
)

add_result(
    "Volume Driver rule violations",
    classification.filter(
        (F.col("performance_class") == "Volume Driver") &
        ~(
            F.col("high_demand") &
            ~F.col("high_profitability")
        )
    ).count()
)

add_result(
    "Hidden Opportunity rule violations",
    classification.filter(
        (F.col("performance_class") == "Hidden Opportunity") &
        ~(
            ~F.col("high_demand") &
            (
                F.col("high_profitability") |
                F.col("good_rating") |
                F.col("strong_repeat_purchase")
            ) &
            F.col("acceptable_wastage")
        )
    ).count()
)

add_result(
    "Low Performer rule violations",
    classification.filter(
        (F.col("performance_class") == "Low Performer") &
        (
            (
                F.col("high_demand") &
                F.col("high_profitability") &
                F.col("acceptable_wastage")
            ) |
            (
                F.col("high_demand") &
                ~F.col("high_profitability")
            ) |
            (
                ~F.col("high_demand") &
                (
                    F.col("high_profitability") |
                    F.col("good_rating") |
                    F.col("strong_repeat_purchase")
                ) &
                F.col("acceptable_wastage")
            )
        )
    ).count()
)

add_result(
    "Classification row count mismatch",
    abs(
        classification.count() -
        menu_profitability.count()
    )
)

print("\n========MENU CLASSIFICATION VALIDATION========\n")

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
        "Menu performance classification is complete, "
        "consistent, and follows the defined multi-dimensional rules."
    )
else:
    print(
        "Menu classification validation failed. "
        "Review failed checks before continuing."
    )

spark.stop()
