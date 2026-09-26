from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("DineIQ Data Integration") \
    .getOrCreate()

processed_folder = "processed_data"
integrated_folder = "integrated_data"

customers = spark.read.parquet(f"{processed_folder}/customers")
restaurants = spark.read.parquet(f"{processed_folder}/restaurants")
menu_categories = spark.read.parquet(f"{processed_folder}/menu_categories")
ingredients = spark.read.parquet(f"{processed_folder}/ingredients")
menu_items = spark.read.parquet(f"{processed_folder}/menu_items")
menu_item_ingredients = spark.read.parquet(f"{processed_folder}/menu_item_ingredients")
restaurant_menu_items = spark.read.parquet(f"{processed_folder}/restaurant_menu_items")
pricing_history = spark.read.parquet(f"{processed_folder}/pricing_history")
promotions = spark.read.parquet(f"{processed_folder}/promotions")
inventory = spark.read.parquet(f"{processed_folder}/inventory")
orders = spark.read.parquet(f"{processed_folder}/orders")
order_items = spark.read.parquet(f"{processed_folder}/order_items")
ratings = spark.read.parquet(f"{processed_folder}/ratings")
wastage = spark.read.parquet(f"{processed_folder}/wastage")
menu_item_costs = spark.read.parquet(f"{processed_folder}/menu_item_costs")

print("\n========TRANSACTION INTEGRATION========\n")

transaction_data = order_items.alias("oi").join(
    orders.alias("o"),
    F.col("oi.order_id") == F.col("o.order_id"),
    "inner"
).join(
    customers.alias("c"),
    F.col("o.customer_id") == F.col("c.customer_id"),
    "inner"
).join(
    restaurants.alias("r"),
    F.col("o.restaurant_id") == F.col("r.restaurant_id"),
    "inner"
).join(
    menu_items.alias("m"),
    F.col("oi.item_id") == F.col("m.menu_items_id"),
    "inner"
).join(
    menu_categories.alias("mc"),
    F.col("m.cat_id") == F.col("mc.category_id"),
    "inner"
).join(
    promotions.alias("p"),
    F.col("o.promotion_id") == F.col("p.promotion_id"),
    "left"
).join(
    menu_item_costs.alias("cost"),
    F.col("oi.item_id") == F.col("cost.menu_item_id"),
    "left"
).select(
    F.col("oi.order_item_id"),
    F.col("o.order_id"),
    F.col("o.customer_id"),
    F.col("o.restaurant_id"),
    F.col("oi.item_id"),
    F.col("m.cat_id").alias("category_id"),
    F.col("o.promotion_id"),
    F.col("o.order_datetime"),
    F.col("o.ordering_channel"),
    F.col("o.order_status"),
    F.col("o.payment_method"),
    F.col("oi.quantity"),
    F.col("oi.unit_price"),
    F.col("oi.discount_amount").alias("item_discount_amount"),
    F.col("oi.line_total"),
    F.col("o.subtotal").alias("order_subtotal"),
    F.col("o.discount_amount").alias("order_discount_amount"),
    F.col("o.tax_amount"),
    F.col("o.delivery_fee"),
    F.col("o.total_amount"),
    F.col("c.signup_date"),
    F.col("c.birth_year"),
    F.col("c.gender"),
    F.col("c.city").alias("customer_city"),
    F.col("r.name").alias("restaurant_name"),
    F.col("r.city").alias("restaurant_city"),
    F.col("r.area").alias("restaurant_area"),
    F.col("m.name").alias("item_name"),
    F.col("m.base_price"),
    F.col("m.prep_time_minutes"),
    F.col("mc.name").alias("category_name"),
    F.col("cost.item_cost"),
    F.col("p.name").alias("promotion_name"),
    F.col("p.discount_type"),
    F.col("p.discount_value")
)

print("Clean order items:", order_items.count())
print("Integrated transaction rows:", transaction_data.count())

print("\n========OPERATIONAL INTEGRATION========\n")

recipe_data = menu_item_ingredients.alias("mi").join(
    menu_items.alias("m"),
    F.col("mi.menu_item_id") == F.col("m.menu_items_id"),
    "inner"
).join(
    ingredients.alias("i"),
    F.col("mi.ingredient_id") == F.col("i.ingredient_id"),
    "inner"
).select(
    F.col("mi.menu_item_ingredient_id"),
    F.col("mi.menu_item_id"),
    F.col("m.name").alias("item_name"),
    F.col("m.cat_id").alias("category_id"),
    F.col("mi.ingredient_id"),
    F.col("i.ingredient").alias("ingredient_name"),
    F.col("mi.quantity_required"),
    F.col("i.unit"),
    F.col("i.unit_cost")
)

inventory_data = inventory.alias("inv").join(
    ingredients.alias("i"),
    F.col("inv.ingredient_id") == F.col("i.ingredient_id"),
    "inner"
).join(
    restaurants.alias("r"),
    F.col("inv.restaurant_id") == F.col("r.restaurant_id"),
    "inner"
).select(
    F.col("inv.*"),
    F.col("i.ingredient").alias("ingredient_name"),
    F.col("r.name").alias("restaurant_name"),
    F.col("r.city").alias("restaurant_city"),
    F.col("r.area").alias("restaurant_area")
)

wastage_data = wastage.alias("w").join(
    ingredients.alias("i"),
    F.col("w.ingredient_id") == F.col("i.ingredient_id"),
    "inner"
).join(
    restaurants.alias("r"),
    F.col("w.restaurant_id") == F.col("r.restaurant_id"),
    "inner"
).select(
    F.col("w.*"),
    F.col("i.ingredient").alias("ingredient_name"),
    F.col("r.name").alias("restaurant_name"),
    F.col("r.city").alias("restaurant_city"),
    F.col("r.area").alias("restaurant_area")
)


menu_inventory_data = recipe_data.alias("rec").join(
    inventory_data.alias("inv"),
    F.col("rec.ingredient_id") == F.col("inv.ingredient_id"),
    "inner"
).select(
    F.col("rec.menu_item_id"),
    F.col("rec.item_name"),
    F.col("rec.category_id"),
    F.col("rec.ingredient_id"),
    F.col("rec.ingredient_name"),
    F.col("rec.quantity_required"),
    F.col("inv.inventory_id"),
    F.col("inv.restaurant_id"),
    F.col("inv.restaurant_name"),
    F.col("inv.restaurant_city"),
    F.col("inv.restaurant_area"),
    F.col("inv.quantity_received"),
    F.col("inv.quantity_remaining"),
    F.col("inv.received_date"),
    F.col("inv.expiry_date")
)

menu_wastage_data = recipe_data.alias("rec").join(
    wastage_data.alias("w"),
    F.col("rec.ingredient_id") == F.col("w.ingredient_id"),
    "inner"
).select(
    F.col("rec.menu_item_id"),
    F.col("rec.item_name"),
    F.col("rec.category_id"),
    F.col("rec.ingredient_id"),
    F.col("rec.ingredient_name"),
    F.col("rec.quantity_required"),
    F.col("w.wastage_id"),
    F.col("w.inventory_id"),
    F.col("w.restaurant_id"),
    F.col("w.restaurant_name"),
    F.col("w.restaurant_city"),
    F.col("w.restaurant_area"),
    F.col("w.wastage_date"),
    F.col("w.quantity").alias("wastage_quantity"),
    F.col("w.cost").alias("wastage_cost"),
    F.col("w.reason").alias("wastage_reason")
)


print("Recipe rows:", recipe_data.count())
print("Inventory rows:", inventory_data.count())
print("Wastage rows:", wastage_data.count())
print("Menu inventory rows:", menu_inventory_data.count())
print("Menu wastage rows:", menu_wastage_data.count())

print("\n========SUPPORTING ANALYTICAL TABLES========\n")

rating_data = ratings.alias("ra").join(
    menu_items.alias("m"),
    F.col("ra.item_id") == F.col("m.menu_items_id"),
    "inner"
).join(
    restaurants.alias("r"),
    F.col("ra.restaurant_id") == F.col("r.restaurant_id"),
    "inner"
).select(
    F.col("ra.*"),
    F.col("m.name").alias("item_name"),
    F.col("r.name").alias("restaurant_name")
)

pricing_data = pricing_history.alias("ph").join(
    menu_items.alias("m"),
    F.col("ph.item_id") == F.col("m.menu_items_id"),
    "inner"
).join(
    restaurants.alias("r"),
    F.col("ph.restaurant_id") == F.col("r.restaurant_id"),
    "inner"
).select(
    F.col("ph.*"),
    F.col("m.name").alias("item_name"),
    F.col("r.name").alias("restaurant_name")
)

restaurant_menu_data = restaurant_menu_items.alias("rm").join(
    menu_items.alias("m"),
    F.col("rm.item_id") == F.col("m.menu_items_id"),
    "inner"
).join(
    restaurants.alias("r"),
    F.col("rm.restaurant_id") == F.col("r.restaurant_id"),
    "inner"
).select(
    F.col("rm.*"),
    F.col("m.name").alias("item_name"),
    F.col("r.name").alias("restaurant_name")
)

print("Rating rows:", rating_data.count())
print("Pricing history rows:", pricing_data.count())
print("Restaurant menu rows:", restaurant_menu_data.count())

transaction_data.write.mode("overwrite").parquet(f"{integrated_folder}/transactions")
recipe_data.write.mode("overwrite").parquet(f"{integrated_folder}/recipes")
inventory_data.write.mode("overwrite").parquet(f"{integrated_folder}/inventory")
wastage_data.write.mode("overwrite").parquet(f"{integrated_folder}/wastage")
rating_data.write.mode("overwrite").parquet(f"{integrated_folder}/ratings")
pricing_data.write.mode("overwrite").parquet(f"{integrated_folder}/pricing_history")
restaurant_menu_data.write.mode("overwrite").parquet(f"{integrated_folder}/restaurant_menu")
menu_inventory_data.write.mode("overwrite").parquet(f"{integrated_folder}/menu_inventory")
menu_wastage_data.write.mode("overwrite").parquet(f"{integrated_folder}/menu_wastage")

print("\nIntegrated analytical datasets saved to integrated_data/")