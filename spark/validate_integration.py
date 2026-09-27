from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from config.settings import PROCESSED_DATA_FOLDER, INTEGRATED_DATA_FOLDER

spark = SparkSession.builder \
    .appName("DineIQ Integration Validation") \
    .getOrCreate()

processed_folder = PROCESSED_DATA_FOLDER
integrated_folder = INTEGRATED_DATA_FOLDER

orders = spark.read.parquet(f"{processed_folder}/orders")
order_items = spark.read.parquet(f"{processed_folder}/order_items")
menu_items = spark.read.parquet(f"{processed_folder}/menu_items")
ingredients = spark.read.parquet(f"{processed_folder}/ingredients")
restaurants = spark.read.parquet(f"{processed_folder}/restaurants")

transactions = spark.read.parquet(f"{integrated_folder}/transactions")
recipes = spark.read.parquet(f"{integrated_folder}/recipes")
inventory = spark.read.parquet(f"{integrated_folder}/inventory")
wastage = spark.read.parquet(f"{integrated_folder}/wastage")
ratings = spark.read.parquet(f"{integrated_folder}/ratings")
pricing_history = spark.read.parquet(f"{integrated_folder}/pricing_history")
restaurant_menu = spark.read.parquet(f"{integrated_folder}/restaurant_menu")
menu_inventory = spark.read.parquet(f"{integrated_folder}/menu_inventory")
menu_wastage = spark.read.parquet(f"{integrated_folder}/menu_wastage")

validation_results = []

def add_result(check, count):
    validation_results.append({
        "check": check,
        "count": count,
        "status": "PASS" if count == 0 else "FAIL"
    })

add_result(
    "Transaction row inflation or loss",
    abs(order_items.count() - transactions.count())
)

add_result(
    "Duplicate transaction order_item_id",
    transactions.groupBy("order_item_id").count().filter(F.col("count") > 1).count()
)

add_result(
    "Broken transaction order reference",
    transactions.alias("t").join(
        orders.select("order_id").alias("o"),
        F.col("t.order_id") == F.col("o.order_id"),
        "left_anti"
    ).count()
)

add_result(
    "Broken transaction menu item reference",
    transactions.alias("t").join(
        menu_items.select("menu_items_id").alias("m"),
        F.col("t.item_id") == F.col("m.menu_items_id"),
        "left_anti"
    ).count()
)

add_result(
    "Broken transaction restaurant reference",
    transactions.alias("t").join(
        restaurants.select("restaurant_id").alias("r"),
        F.col("t.restaurant_id") == F.col("r.restaurant_id"),
        "left_anti"
    ).count()
)

add_result(
    "Duplicate recipe relationship ID",
    recipes.groupBy("menu_item_ingredient_id").count().filter(F.col("count") > 1).count()
)

add_result(
    "Broken recipe menu item reference",
    recipes.alias("r").join(
        menu_items.select("menu_items_id").alias("m"),
        F.col("r.menu_item_id") == F.col("m.menu_items_id"),
        "left_anti"
    ).count()
)

add_result(
    "Broken recipe ingredient reference",
    recipes.alias("r").join(
        ingredients.select("ingredient_id").alias("i"),
        F.col("r.ingredient_id") == F.col("i.ingredient_id"),
        "left_anti"
    ).count()
)

add_result(
    "Duplicate inventory ID",
    inventory.groupBy("inventory_id").count().filter(F.col("count") > 1).count()
)

add_result(
    "Duplicate wastage ID",
    wastage.groupBy("wastage_id").count().filter(F.col("count") > 1).count()
)

add_result(
    "Duplicate rating ID",
    ratings.groupBy("rating_id").count().filter(F.col("count") > 1).count()
)

add_result(
    "Duplicate pricing history ID",
    pricing_history.groupBy("price_history_id").count().filter(F.col("count") > 1).count()
)

add_result(
    "Duplicate restaurant menu ID",
    restaurant_menu.groupBy("restaurant_menu_id").count().filter(F.col("count") > 1).count()
)

add_result(
    "Missing transaction item cost",
    transactions.filter(F.col("item_cost").isNull()).count()
)

add_result(
    "Broken menu inventory menu item reference",
    menu_inventory.alias("mi").join(
        menu_items.select("menu_items_id").alias("m"),
        F.col("mi.menu_item_id") == F.col("m.menu_items_id"),
        "left_anti"
    ).count()
)

add_result(
    "Broken menu inventory ingredient reference",
    menu_inventory.alias("mi").join(
        ingredients.select("ingredient_id").alias("i"),
        F.col("mi.ingredient_id") == F.col("i.ingredient_id"),
        "left_anti"
    ).count()
)

add_result(
    "Broken menu wastage menu item reference",
    menu_wastage.alias("mw").join(
        menu_items.select("menu_items_id").alias("m"),
        F.col("mw.menu_item_id") == F.col("m.menu_items_id"),
        "left_anti"
    ).count()
)

add_result(
    "Broken menu wastage ingredient reference",
    menu_wastage.alias("mw").join(
        ingredients.select("ingredient_id").alias("i"),
        F.col("mw.ingredient_id") == F.col("i.ingredient_id"),
        "left_anti"
    ).count()
)

print("\n========INTEGRATION VALIDATION========\n")

for result in validation_results:
    print(
        f"{result['status']} | "
        f"{result['check']}: "
        f"{result['count']}"
    )

passed = sum(result["status"] == "PASS" for result in validation_results)

print(f"\nValidation Result: {passed}/{len(validation_results)} checks passed")

if passed == len(validation_results):
    print("Integrated analytical layer is consistent and ready for feature engineering.")
else:
    print("Integration validation failed. Review failed checks before feature engineering.")