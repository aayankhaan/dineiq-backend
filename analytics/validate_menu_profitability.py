from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("DineIQ Menu Profitability Validation") \
    .getOrCreate()

profitability = spark.read.parquet(
    "analytics_data/menu_profitability"
)

menu_items = spark.read.parquet(
    "processed_data/menu_items"
)

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
    "sales_trend"
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
    if column not in profitability.columns
]

add_result(
    "Missing required profitability columns",
    len(missing_columns)
)

add_result(
    "Duplicate item IDs",
    profitability.groupBy(
        "item_id"
    ).count().filter(
        F.col("count") > 1
    ).count()
)

add_result(
    "Missing menu item rows",
    menu_items.select(
        F.col("menu_items_id").alias("item_id")
    ).join(
        profitability.select("item_id"),
        "item_id",
        "left_anti"
    ).count()
)

null_condition = None

for column in required_columns:
    current = F.col(column).isNull()
    null_condition = current if null_condition is None else null_condition | current

add_result(
    "Null profitability values",
    profitability.filter(
        null_condition
    ).count()
)

add_result(
    "Invalid quantity sold",
    profitability.filter(
        F.col("quantity_sold") < 0
    ).count()
)

add_result(
    "Invalid revenue",
    profitability.filter(
        F.col("item_revenue") < 0
    ).count()
)

add_result(
    "Invalid cost",
    profitability.filter(
        F.col("cost") < 0
    ).count()
)

add_result(
    "Incorrect contribution margin",
    profitability.filter(
        F.abs(
            F.col("contribution_margin") -
            (F.col("item_revenue") - F.col("cost"))
        ) > 0.01
    ).count()
)

add_result(
    "Incorrect profit percentage",
    profitability.filter(
        F.abs(
            F.col("profit_percentage") -
            F.when(
                F.col("item_revenue") > 0,
                (F.col("contribution_margin") / F.col("item_revenue")) * 100
            ).otherwise(0)
        ) > 0.02
    ).count()
)

add_result(
    "Invalid average rating",
    profitability.filter(
        ~F.col("average_rating").between(1, 5)
    ).count()
)

add_result(
    "Invalid repeat-purchase rate",
    profitability.filter(
        ~F.col("repeat_purchase_rate").between(0, 100)
    ).count()
)

add_result(
    "Invalid wastage percentage",
    profitability.filter(
        ~F.col("wastage_percentage").between(0, 100)
    ).count()
)

add_result(
    "Invalid promotion dependency",
    profitability.filter(
        ~F.col("promotion_dependency").between(0, 100)
    ).count()
)

add_result(
    "Invalid sales trend",
    profitability.filter(
        F.isnan("sales_trend")
    ).count()
)

add_result(
    "Profitability row count mismatch",
    abs(
        profitability.count() -
        menu_items.select("menu_items_id").distinct().count()
    )
)

print("\n========MENU PROFITABILITY VALIDATION========\n")

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
        "Menu profitability analysis is consistent "
        "and satisfies the required analytical dimensions."
    )
else:
    print(
        "Menu profitability validation failed. "
        "Review failed checks before continuing."
    )

spark.stop()
