from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from config.settings import EDA_OUTPUT_FOLDER

spark = SparkSession.builder \
    .appName("DineIQ EDA Validation") \
    .getOrCreate()

output_folder = EDA_OUTPUT_FOLDER

output_names = [
    "top_selling_dishes",
    "lowest_selling_dishes",
    "highest_revenue_dishes",
    "highest_profit_dishes",
    "highest_margin_dishes",
    "high_wastage_dishes",
    "best_rated_dishes",
    "poorly_rated_dishes",
    "popular_categories",
    "peak_ordering_periods",
    "location_sales_patterns",
    "channel_ordering_patterns",
    "promotion_driven_sales"
]

outputs = {
    name: spark.read.parquet(f"{output_folder}/{name}")
    for name in output_names
}

validation_results = []

def add_result(check, count):
    validation_results.append({
        "check": check,
        "count": count,
        "status": "PASS" if count == 0 else "FAIL"
    })

def duplicate_count(df, column):
    return df.groupBy(column).count().filter(F.col("count") > 1).count()

def null_count(df, columns):
    condition = None

    for column in columns:
        current = F.col(column).isNull()
        condition = current if condition is None else condition | current

    return df.filter(condition).count()

for name, dataframe in outputs.items():
    add_result(
        f"Empty output - {name}",
        1 if dataframe.count() == 0 else 0
    )

add_result(
    "Top-selling dishes row count",
    abs(outputs["top_selling_dishes"].count() - 10)
)

add_result(
    "Lowest-selling dishes row count",
    abs(outputs["lowest_selling_dishes"].count() - 10)
)

add_result(
    "Highest-revenue dishes row count",
    abs(outputs["highest_revenue_dishes"].count() - 10)
)

add_result(
    "Highest-profit dishes row count",
    abs(outputs["highest_profit_dishes"].count() - 10)
)

add_result(
    "Highest-margin dishes row count",
    abs(outputs["highest_margin_dishes"].count() - 10)
)

add_result(
    "High-wastage dishes row count",
    abs(outputs["high_wastage_dishes"].count() - 10)
)

add_result(
    "Best-rated dishes row count",
    abs(outputs["best_rated_dishes"].count() - 10)
)

add_result(
    "Poorly-rated dishes row count",
    abs(outputs["poorly_rated_dishes"].count() - 10)
)

item_outputs = [
    "top_selling_dishes",
    "lowest_selling_dishes",
    "highest_revenue_dishes",
    "highest_profit_dishes",
    "highest_margin_dishes",
    "high_wastage_dishes",
    "best_rated_dishes",
    "poorly_rated_dishes",
    "promotion_driven_sales"
]

for name in item_outputs:
    add_result(
        f"Duplicate item IDs - {name}",
        duplicate_count(outputs[name], "item_id")
    )

for name, dataframe in outputs.items():
    add_result(
        f"Null values - {name}",
        null_count(dataframe, dataframe.columns)
    )

add_result(
    "Invalid top-selling quantities",
    outputs["top_selling_dishes"].filter(
        F.col("quantity_sold") <= 0
    ).count()
)

add_result(
    "Invalid lowest-selling quantities",
    outputs["lowest_selling_dishes"].filter(
        F.col("quantity_sold") <= 0
    ).count()
)

add_result(
    "Invalid revenue values",
    outputs["highest_revenue_dishes"].filter(
        F.col("item_revenue") < 0
    ).count()
)

add_result(
    "Invalid margin percentages",
    outputs["highest_margin_dishes"].filter(
        ~F.col("profit_percentage").between(0, 100)
    ).count()
)

add_result(
    "Invalid wastage percentages",
    outputs["high_wastage_dishes"].filter(
        ~F.col("wastage_percentage").between(0, 100)
    ).count()
)

add_result(
    "Invalid best-rating values",
    outputs["best_rated_dishes"].filter(
        ~F.col("average_rating").between(1, 5)
    ).count()
)

add_result(
    "Invalid poor-rating values",
    outputs["poorly_rated_dishes"].filter(
        ~F.col("average_rating").between(1, 5)
    ).count()
)

add_result(
    "Invalid category values",
    outputs["popular_categories"].filter(
        (F.col("quantity_sold") <= 0) |
        (F.col("order_frequency") <= 0) |
        (F.col("category_revenue") < 0)
    ).count()
)

add_result(
    "Invalid peak ordering hours",
    outputs["peak_ordering_periods"].filter(
        ~F.col("order_hour").between(0, 23) |
        (F.col("order_count") <= 0) |
        (F.col("revenue") < 0)
    ).count()
)

add_result(
    "Duplicate location IDs",
    duplicate_count(
        outputs["location_sales_patterns"],
        "restaurant_id"
    )
)

add_result(
    "Invalid location values",
    outputs["location_sales_patterns"].filter(
        (F.col("location_orders") <= 0) |
        (F.col("location_revenue") < 0) |
        (F.col("location_average_order_value") < 0) |
        ~F.col("peak_hour_frequency").between(0, 100) |
        ~F.col("weekend_order_ratio").between(0, 100)
    ).count()
)

add_result(
    "Duplicate ordering channels",
    duplicate_count(
        outputs["channel_ordering_patterns"],
        "ordering_channel"
    )
)

add_result(
    "Invalid channel values",
    outputs["channel_ordering_patterns"].filter(
        (F.col("order_count") <= 0) |
        (F.col("channel_revenue") < 0) |
        (F.col("average_order_value") < 0)
    ).count()
)

add_result(
    "Invalid promotion values",
    outputs["promotion_driven_sales"].filter(
        (F.col("total_orders") <= 0) |
        (F.col("promotion_orders") < 0) |
        (F.col("promotion_orders") > F.col("total_orders")) |
        (F.col("item_revenue") < 0) |
        (F.col("discount_amount") < 0) |
        ~F.col("promotion_dependency").between(0, 100)
    ).count()
)

print("\n========EDA VALIDATION========\n")

for result in validation_results:
    print(
        f"{result['status']} | "
        f"{result['check']}: "
        f"{result['count']}"
    )

passed = sum(result["status"] == "PASS" for result in validation_results)

print(f"\nValidation Result: {passed}/{len(validation_results)} checks passed")

if passed == len(validation_results):
    print("EDA outputs are consistent and ready for the next analysis stage.")
else:
    print("EDA validation failed. Review failed checks before continuing.")

spark.stop()
