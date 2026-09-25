from pyspark.sql import SparkSession
from spark.schemas import *

spark = SparkSession.builder \
    .appName("DineIQ") \
    .getOrCreate()

customers = spark.read.option("header", True).schema(customers_schema).csv("raw_data/customers.csv")
ingredients = spark.read.option("header", True).schema(ingredients_schema).csv("raw_data/ingredients.csv")
inventory = spark.read.option("header", True).schema(inventory_schema).csv("raw_data/inventory.csv")
menu_categories = spark.read.option("header", True).schema(menu_category_schema).csv("raw_data/menu_categories.csv")
menu_items = spark.read.option("header", True).schema(menu_items_schema).csv("raw_data/menu_items.csv")
menu_item_ingredients = spark.read.option("header", True).schema(menu_item_ingredients_schema).csv("raw_data/menu_item_ingredients.csv")
order_items = spark.read.option("header", True).schema(order_item_schema).csv("raw_data/order_items.csv")
orders = spark.read.option("header", True).schema(orders_schema).csv("raw_data/orders.csv")
pricing_history = spark.read.option("header", True).schema(price_history_schema).csv("raw_data/pricing_history.csv")
promotions = spark.read.option("header", True).schema(promotion_schema).csv("raw_data/promotions.csv")
ratings = spark.read.option("header", True).schema(rating_schema).csv("raw_data/ratings.csv")
restaurant_menu_items = spark.read.option("header", True).schema(restaurant_menu_schema).csv("raw_data/restaurant_menu_items.csv")
restaurants = spark.read.option("header", True).schema(restaurant_schema).csv("raw_data/restaurants.csv")
wastage = spark.read.option("header", True).schema(wastage_schema).csv("raw_data/wastage.csv")

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

# spark.stop()