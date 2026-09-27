from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from config.settings import FEATURE_DATA_FOLDER, PROCESSED_DATA_FOLDER

spark = SparkSession.builder \
    .appName("DineIQ Feature Validation") \
    .getOrCreate()

feature_folder = FEATURE_DATA_FOLDER
processed_folder = PROCESSED_DATA_FOLDER

item_features = spark.read.parquet(f"{feature_folder}/item_features")
customer_features = spark.read.parquet(f"{feature_folder}/customer_features")
order_features = spark.read.parquet(f"{feature_folder}/order_features")
location_features = spark.read.parquet(f"{feature_folder}/location_features")

menu_items = spark.read.parquet(f"{processed_folder}/menu_items")
orders = spark.read.parquet(f"{processed_folder}/orders")
restaurants = spark.read.parquet(f"{processed_folder}/restaurants")

validation_results = []

def add_result(check, count):
    validation_results.append({
        "check": check,
        "count": count,
        "status": "PASS" if count == 0 else "FAIL"
    })

def duplicate_count(df, column):
    return df.groupBy(column).count().filter(F.col("count") > 1).count()

def missing_columns(df, required_columns):
    return len([column for column in required_columns if column not in df.columns])

item_required_columns = [
    "item_id",
    "item_name",
    "item_revenue",
    "cost",
    "contribution_margin",
    "profit_percentage",
    "order_frequency",
    "item_popularity",
    "repeat_purchase_rate",
    "average_rating",
    "rating_trend",
    "promotion_dependency",
    "discount_percentage",
    "wastage_percentage",
    "price_change_percentage"
]

customer_required_columns = [
    "customer_id",
    "customer_recency",
    "customer_frequency",
    "customer_monetary_value",
    "average_order_value",
    "channel_preference"
]

order_required_columns = [
    "order_id",
    "basket_size"
]

location_required_columns = [
    "restaurant_id",
    "restaurant_name",
    "location_orders",
    "location_revenue",
    "location_average_order_value",
    "peak_hour_frequency",
    "weekend_order_ratio",
    "location_performance"
]

add_result(
    "Missing required item feature columns",
    missing_columns(item_features, item_required_columns)
)

add_result(
    "Missing required customer feature columns",
    missing_columns(customer_features, customer_required_columns)
)

add_result(
    "Missing required order feature columns",
    missing_columns(order_features, order_required_columns)
)

add_result(
    "Missing required location feature columns",
    missing_columns(location_features, location_required_columns)
)

add_result(
    "Duplicate item feature IDs",
    duplicate_count(item_features, "item_id")
)

add_result(
    "Duplicate customer feature IDs",
    duplicate_count(customer_features, "customer_id")
)

add_result(
    "Duplicate order feature IDs",
    duplicate_count(order_features, "order_id")
)

add_result(
    "Duplicate location feature IDs",
    duplicate_count(location_features, "restaurant_id")
)

add_result(
    "Missing item feature rows",
    abs(menu_items.count() - item_features.count())
)

add_result(
    "Missing order feature rows",
    abs(orders.count() - order_features.count())
)

add_result(
    "Missing location feature rows",
    abs(restaurants.count() - location_features.count())
)

add_result(
    "Null item feature values",
    item_features.filter(
        F.col("item_revenue").isNull() |
        F.col("cost").isNull() |
        F.col("contribution_margin").isNull() |
        F.col("profit_percentage").isNull() |
        F.col("order_frequency").isNull() |
        F.col("item_popularity").isNull() |
        F.col("repeat_purchase_rate").isNull() |
        F.col("average_rating").isNull() |
        F.col("rating_trend").isNull() |
        F.col("promotion_dependency").isNull() |
        F.col("discount_percentage").isNull() |
        F.col("wastage_percentage").isNull() |
        F.col("price_change_percentage").isNull()
    ).count()
)

add_result(
    "Invalid item numeric values",
    item_features.filter(
        (F.col("item_revenue") < 0) |
        (F.col("cost") < 0) |
        (F.col("order_frequency") < 0)
    ).count()
)

add_result(
    "Invalid item percentage ranges",
    item_features.filter(
        ~F.col("item_popularity").between(0, 100) |
        ~F.col("repeat_purchase_rate").between(0, 100) |
        ~F.col("promotion_dependency").between(0, 100) |
        ~F.col("discount_percentage").between(0, 100) |
        ~F.col("wastage_percentage").between(0, 100)
    ).count()
)

add_result(
    "Invalid average rating range",
    item_features.filter(
        ~F.col("average_rating").between(1, 5)
    ).count()
)

add_result(
    "Incorrect contribution margin",
    item_features.filter(
        F.abs(
            F.col("contribution_margin").cast("double") -
            (
                F.col("item_revenue").cast("double") -
                F.col("cost").cast("double")
            )
        ) > 0.02
    ).count()
)

add_result(
    "Incorrect profit percentage",
    item_features.filter(
        (F.col("item_revenue") > 0) &
        (
            F.abs(
                F.col("profit_percentage") -
                (
                    F.col("contribution_margin").cast("double") /
                    F.col("item_revenue").cast("double") * 100
                )
            ) > 0.02
        )
    ).count()
)

add_result(
    "Null customer feature values",
    customer_features.filter(
        F.col("customer_recency").isNull() |
        F.col("customer_frequency").isNull() |
        F.col("customer_monetary_value").isNull() |
        F.col("average_order_value").isNull() |
        F.col("channel_preference").isNull()
    ).count()
)

add_result(
    "Invalid customer feature values",
    customer_features.filter(
        (F.col("customer_recency") < 0) |
        (F.col("customer_frequency") <= 0) |
        (F.col("customer_monetary_value") < 0) |
        (F.col("average_order_value") < 0)
    ).count()
)

add_result(
    "Incorrect customer average order value",
    customer_features.filter(
        F.abs(
            F.col("average_order_value") -
            (
                F.col("customer_monetary_value") /
                F.col("customer_frequency")
            )
        ) > 0.02
    ).count()
)

add_result(
    "Invalid basket size",
    order_features.filter(
        F.col("basket_size").isNull() |
        (F.col("basket_size") <= 0)
    ).count()
)

add_result(
    "Null location feature values",
    location_features.filter(
        F.col("location_orders").isNull() |
        F.col("location_revenue").isNull() |
        F.col("location_average_order_value").isNull() |
        F.col("peak_hour_frequency").isNull() |
        F.col("weekend_order_ratio").isNull() |
        F.col("location_performance").isNull()
    ).count()
)

add_result(
    "Invalid location feature values",
    location_features.filter(
        (F.col("location_orders") <= 0) |
        (F.col("location_revenue") < 0) |
        (F.col("location_average_order_value") < 0) |
        ~F.col("peak_hour_frequency").between(0, 100) |
        ~F.col("weekend_order_ratio").between(0, 100)
    ).count()
)

add_result(
    "Incorrect location average order value",
    location_features.filter(
        F.abs(
            F.col("location_average_order_value") -
            (
                F.col("location_revenue") /
                F.col("location_orders")
            )
        ) > 0.02
    ).count()
)

add_result(
    "Incorrect location performance",
    location_features.filter(
        F.abs(
            F.col("location_performance") -
            F.col("location_revenue")
        ) > 0.02
    ).count()
)

print("\n========FEATURE VALIDATION========\n")

for result in validation_results:
    print(
        f"{result['status']} | "
        f"{result['check']}: "
        f"{result['count']}"
    )

passed = sum(result["status"] == "PASS" for result in validation_results)

print(f"\nValidation Result: {passed}/{len(validation_results)} checks passed")

if passed == len(validation_results):
    print("Feature engineering outputs are consistent and ready for analysis.")
else:
    print("Feature validation failed. Review failed checks before continuing.")

spark.stop()
