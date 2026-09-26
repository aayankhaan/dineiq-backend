from pyspark.sql import functions as F
from spark.ingest import spark

input_folder = "processed_data"

customers = spark.read.parquet(f"{input_folder}/customers")
restaurants = spark.read.parquet(f"{input_folder}/restaurants")
menu_items = spark.read.parquet(f"{input_folder}/menu_items")
ingredients = spark.read.parquet(f"{input_folder}/ingredients")
inventory = spark.read.parquet(f"{input_folder}/inventory")
orders = spark.read.parquet(f"{input_folder}/orders")
order_items = spark.read.parquet(f"{input_folder}/order_items")
ratings = spark.read.parquet(f"{input_folder}/ratings")
wastage = spark.read.parquet(f"{input_folder}/wastage")

def duplicate_count(df, column):
    return df.groupBy(column).count().filter(F.col("count") > 1).count()

def invalid_reference_count(df, column, reference_df, reference_column):
    return df.join(
        reference_df.select(
            F.col(reference_column).alias("valid_reference")
        ),
        F.col(column) == F.col("valid_reference"),
        "left_anti"
    ).filter(F.col(column).isNotNull()).count()

checks = [
    ("Duplicate order IDs", duplicate_count(orders, "order_id")),
    ("Duplicate order item IDs", duplicate_count(order_items, "order_item_id")),
    ("Missing order customer IDs", orders.filter(F.col("customer_id").isNull()).count()),
    ("Invalid order customer IDs", invalid_reference_count(orders, "customer_id", customers, "customer_id")),
    ("Invalid order restaurant IDs", invalid_reference_count(orders, "restaurant_id", restaurants, "restaurant_id")),
    ("Invalid order discounts", orders.filter(F.col("discount_amount") > F.col("subtotal")).count()),
    ("Invalid order dates", orders.filter(
        (F.col("order_datetime") < F.lit("2025-03-01")) |
        (F.col("order_datetime") >= F.lit("2026-09-01"))
    ).count()),
    ("Invalid order item quantities", order_items.filter(F.col("quantity") < 1).count()),
    ("Invalid order item IDs", invalid_reference_count(order_items, "item_id", menu_items, "menu_items_id")),
    ("Invalid order references", invalid_reference_count(order_items, "order_id", orders, "order_id")),
    ("Invalid menu prices", menu_items.filter(F.col("base_price") <= 0).count()),
    ("Invalid inventory quantities", inventory.filter(
        (F.col("quantity_received") < 0) |
        (F.col("quantity_remaining") < 0) |
        (F.col("quantity_remaining") > F.col("quantity_received"))
    ).count()),
    ("Invalid inventory ingredient IDs", invalid_reference_count(inventory, "ingredient_id", ingredients, "ingredient_id")),
    ("Invalid ratings", ratings.filter(~F.col("rating").between(1, 5)).count()),
    ("Invalid rating order IDs", invalid_reference_count(ratings, "order_id", orders, "order_id")),
    ("Invalid rating item IDs", invalid_reference_count(ratings, "item_id", menu_items, "menu_items_id")),
    ("Invalid wastage ingredient IDs", invalid_reference_count(wastage, "ingredient_id", ingredients, "ingredient_id")),
    ("Invalid wastage inventory IDs", invalid_reference_count(wastage, "inventory_id", inventory, "inventory_id"))
]

print("\n========POST-CLEAN VALIDATION========\n")

failed = 0

for name, count in checks:
    status = "PASS" if count == 0 else "FAIL"
    print(f"{status} | {name}: {count}")

    if count != 0:
        failed += 1

print("\n=====================================")

if failed == 0:
    print("ALL CLEANING VALIDATION CHECKS PASSED")
else:
    print(f"{failed} VALIDATION CHECK(S) FAILED")

spark.stop()