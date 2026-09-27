from pyspark.sql import SparkSession
from spark.schemas import *
from config.settings import RAW_DATA_FOLDER

spark = SparkSession.builder \
    .appName("DineIQ") \
    .getOrCreate()

customers = spark.read.option("header", True).schema(customers_schema).csv(f"{RAW_DATA_FOLDER}/customers.csv")
ingredients = spark.read.option("header", True).schema(ingredients_schema).csv(f"{RAW_DATA_FOLDER}/ingredients.csv")
inventory = spark.read.option("header", True).schema(inventory_schema).csv(f"{RAW_DATA_FOLDER}/inventory.csv")
menu_categories = spark.read.option("header", True).schema(menu_category_schema).csv(f"{RAW_DATA_FOLDER}/menu_categories.csv")
menu_items = spark.read.option("header", True).schema(menu_items_schema).csv(f"{RAW_DATA_FOLDER}/menu_items.csv")
menu_item_ingredients = spark.read.option("header", True).schema(menu_item_ingredients_schema).csv(f"{RAW_DATA_FOLDER}/menu_item_ingredients.csv")
order_items = spark.read.option("header", True).schema(order_item_schema).csv(f"{RAW_DATA_FOLDER}/order_items.csv")
orders = spark.read.option("header", True).schema(orders_schema).csv(f"{RAW_DATA_FOLDER}/orders.csv")
pricing_history = spark.read.option("header", True).schema(price_history_schema).csv(f"{RAW_DATA_FOLDER}/pricing_history.csv")
promotions = spark.read.option("header", True).schema(promotion_schema).csv(f"{RAW_DATA_FOLDER}/promotions.csv")
ratings = spark.read.option("header", True).schema(rating_schema).csv(f"{RAW_DATA_FOLDER}/ratings.csv")
restaurant_menu_items = spark.read.option("header", True).schema(restaurant_menu_schema).csv(f"{RAW_DATA_FOLDER}/restaurant_menu_items.csv")
restaurants = spark.read.option("header", True).schema(restaurant_schema).csv(f"{RAW_DATA_FOLDER}/restaurants.csv")
wastage = spark.read.option("header", True).schema(wastage_schema).csv(f"{RAW_DATA_FOLDER}/wastage.csv")

datasets = {
    "Customers": customers,
    "Ingredients": ingredients,
    "Inventory": inventory,
    "Menu Categories": menu_categories,
    "Menu Items": menu_items,
    "Menu Item Ingredients": menu_item_ingredients,
    "Order Items": order_items,
    "Orders": orders,
    "Pricing History": pricing_history,
    "Promotions": promotions,
    "Ratings": ratings,
    "Restaurant Menu Items": restaurant_menu_items,
    "Restaurants": restaurants,
    "Wastage": wastage,
}

for name, df in datasets.items():
    print(f"{name}: {df.count():,}")

print("\n========SCHEMA INFERENCE========\n")

inferred_orders = spark.read.option(
    "header", True
).option(
    "inferSchema", True
).csv(f"{RAW_DATA_FOLDER}/orders.csv")

inferred_orders.printSchema()

print("\n========PARTITION HANDLING========\n")

order_items = spark.read.option("header", True).schema(order_item_schema).csv(f"{RAW_DATA_FOLDER}/order_items.csv")

print("Order Items partitions before:", order_items.rdd.getNumPartitions())
order_items = order_items.repartition(8)
print("Order Items partitions after:", order_items.rdd.getNumPartitions())

# spark.stop()