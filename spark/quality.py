import os
import csv
from pyspark.sql import functions as F
from spark.ingest import *
from config.settings import REPORTS_FOLDER

quality_report = []

def add_report(dataset, check, column, count):
    quality_report.append({
        "dataset": dataset,
        "check": check,
        "column": column,
        "issue_count": count,
        "status": "PASS" if count == 0 else "ISSUES FOUND"
    })


def check_nulls(df, dataset):
    null_checks = []
    for column in df.columns:
        null_checks.append(F.count(F.when(F.col(column).isNull(), 1)).alias(f"{column}_nulls"))

    result = df.select(*null_checks)
    result.show(truncate=False)

    row = result.first()

    for column in df.columns:
        count = row[f"{column}_nulls"]
        add_report(dataset, "Null Values", column, count)


def check_duplicates(df, column, dataset):
    duplicates = df.groupBy(column).count().filter(F.col("count") > 1)
    count = duplicates.count()
    print(f"{column}: {count} duplicate ids")
    add_report(dataset, "Duplicate IDs", column, count)

    if count > 0:
        duplicates.show()


def check_negative(df, columns, dataset):
    for column in columns:
        count = df.filter(F.col(column) < 0).count()
        print(f"{column}: {count} negative values")
        add_report(dataset, "Negative Values", column, count)


def check_invalid_values(df, column, valid_values, dataset):
    invalid = df.filter(F.col(column).isNotNull() & ~F.col(column).isin(valid_values))
    count = invalid.count()
    print(f"{column}: {count} invalid values")
    add_report(dataset, "Invalid Values", column, count)

    if count > 0:
        invalid.select(column).distinct().show()


def check_invalid_range(df, column, minimum=None, maximum=None, dataset=""):
    condition = F.lit(False)

    if minimum is not None:
        condition = condition | (F.col(column) < minimum)
    if maximum is not None:
        condition = condition | (F.col(column) > maximum)

    invalid = df.filter(F.col(column).isNotNull() & condition)
    count = invalid.count()
    print(f"{column}: {count} values outside valid range")
    add_report(dataset, "Invalid Range", column, count)

    if count > 0:
        invalid.select(column).show()


def check_foreign_key(df, column, reference_df, reference_column, dataset):
    invalid = df.filter(F.col(column).isNotNull()).join(
        reference_df.select(F.col(reference_column).alias("_reference_id")),
        F.col(column) == F.col("_reference_id"), "left_anti"
    )

    count = invalid.count()
    print(f"{column}: {count} invalid references")
    add_report(dataset, "Invalid Foreign Key", column, count)

    if count > 0:
        invalid.select(column).distinct().show()


def check_cancelled_orders(df):
    count = df.filter(F.col("order_status") == "Cancelled").count()
    print(f"Cancelled transactions: {count}")
    add_report("Orders", "Cancelled Transactions", "order_status", count)


def check_incorrect_order_discounts(df):
    invalid = df.filter(
        (F.col("discount_amount") < 0) |
        (F.col("discount_amount") > F.col("subtotal"))
    )

    count = invalid.count()
    print(f"Incorrect order discounts: {count}")
    add_report("Orders", "Incorrect Discounts", "discount_amount", count)

    if count > 0:
        invalid.select("order_id", "subtotal", "discount_amount").show()


def check_impossible_wastage(wastage_df, inventory_df):
    invalid = wastage_df.alias("w").join(
        inventory_df.alias("i"),
        F.col("w.inventory_id") == F.col("i.inventory_id"),
        "left"
    ).filter(
        F.col("i.inventory_id").isNull() |
        (F.col("w.quantity") <= 0) |
        (F.col("w.quantity") > F.col("i.quantity_received")) |
        (F.col("w.wastage_date") < F.col("i.received_date")) |
        (F.col("w.wastage_date") > F.col("i.expiry_date"))
    )

    count = invalid.count()
    print(f"Impossible wastage records: {count}")
    add_report("Wastage", "Impossible Wastage", "quantity", count)

    if count > 0:
        invalid.select(
            "w.wastage_id",
            "w.inventory_id",
            "w.quantity",
            "i.quantity_received",
            "w.wastage_date",
            "i.received_date",
            "i.expiry_date"
        ).show()


def check_order_dates(df):
    invalid = df.filter(
        F.col("order_datetime").isNull() |
        (F.to_date(F.col("order_datetime")) < F.lit("2025-03-01").cast("date")) |
        (F.to_date(F.col("order_datetime")) > F.lit("2026-08-31").cast("date"))
    )

    count = invalid.count()
    print(f"Invalid order dates: {count}")
    add_report("Orders", "Invalid Dates", "order_datetime", count)

    if count > 0:
        invalid.select("order_id", "order_datetime").show()


def check_inconsistent_units(ingredients, inventory, wastage):
    valid_units = ["kg", "liters", "pieces"]

    print("Checking invalid ingredient units...")
    invalid_ingredients = ingredients.filter(~F.col("unit").isin(valid_units))
    count = invalid_ingredients.count()
    print(f"Invalid ingredient units: {count}")
    add_report("Ingredients", "Invalid Units", "unit", count)

    print("Checking inventory unit consistency...")
    invalid_inventory_units = inventory.alias("inv").join(
        ingredients.alias("ing"),
        F.col("inv.ingredient_id") == F.col("ing.ingredient_id"),
        "left"
    ).filter(
        F.col("ing.ingredient_id").isNull() |
        (F.col("inv.unit") != F.col("ing.unit"))
    )

    count = invalid_inventory_units.count()
    print(f"Inconsistent inventory units: {count}")
    add_report("Inventory", "Inconsistent Units", "unit", count)

    print("Checking wastage unit consistency...")
    invalid_wastage_units = wastage.alias("w").join(
        inventory.alias("inv"),
        F.col("w.inventory_id") == F.col("inv.inventory_id"),
        "left"
    ).filter(
        F.col("inv.inventory_id").isNull() |
        (F.col("w.unit") != F.col("inv.unit"))
    )

    count = invalid_wastage_units.count()
    print(f"Inconsistent wastage units: {count}")
    add_report("Wastage", "Inconsistent Units", "unit", count)


def save_quality_report():
    os.makedirs(REPORTS_FOLDER, exist_ok=True)
    with open(f"{REPORTS_FOLDER}/data_quality_report.csv", "w", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=["dataset", "check", "column", "issue_count", "status"]
        )

        print("writing report..")
        writer.writeheader()
        writer.writerows(quality_report)

    print("\nData quality report saved to data/reports/data_quality_report.csv")


print("\n===== ORDERS =====")
check_nulls(orders, "Orders")
check_duplicates(orders, "order_id", "Orders")
check_negative(orders, ["subtotal", "discount_amount", "tax_amount", "delivery_fee", "total_amount"], "Orders")
check_invalid_values(orders, "order_status", ["Completed", "Cancelled", "Refunded"], "Orders")
check_invalid_values(orders, "ordering_channel", ["Dine-in", "Takeaway", "Website/App", "Third-Party Delivery"], "Orders")
check_invalid_values(orders, "payment_method", ["Cash", "Card", "Digital Wallet"], "Orders")
check_incorrect_order_discounts(orders)
check_cancelled_orders(orders)
check_order_dates(orders)
check_foreign_key(orders, "customer_id", customers, "customer_id", "Orders")
check_foreign_key(orders, "restaurant_id", restaurants, "restaurant_id", "Orders")
check_foreign_key(orders, "promotion_id", promotions, "promotion_id", "Orders")


print("\n===== ORDER ITEMS =====")
check_nulls(order_items, "Order Items")
check_duplicates(order_items, "order_item_id", "Order Items")
check_negative(order_items, ["quantity", "unit_price", "discount_amount", "line_total"], "Order Items")
check_invalid_range(order_items, "quantity", minimum=1, dataset="Order Items")
check_foreign_key(order_items, "order_id", orders, "order_id", "Order Items")
check_foreign_key(order_items, "item_id", menu_items, "menu_items_id", "Order Items")


print("\n===== CUSTOMERS =====")
check_nulls(customers, "Customers")
check_duplicates(customers, "customer_id", "Customers")


print("\n===== RESTAURANTS =====")
check_nulls(restaurants, "Restaurants")
check_duplicates(restaurants, "restaurant_id", "Restaurants")


print("\n===== MENU CATEGORIES =====")
check_nulls(menu_categories, "Menu Categories")
check_duplicates(menu_categories, "category_id", "Menu Categories")


print("\n===== MENU ITEMS =====")
check_nulls(menu_items, "Menu Items")
check_duplicates(menu_items, "menu_items_id", "Menu Items")
check_negative(menu_items, ["base_price", "prep_time_minutes"], "Menu Items")
check_invalid_range(menu_items, "base_price", minimum=1, dataset="Menu Items")
check_foreign_key(menu_items, "cat_id", menu_categories, "category_id", "Menu Items")


print("\n===== INGREDIENTS =====")
check_nulls(ingredients, "Ingredients")
check_duplicates(ingredients, "ingredient_id", "Ingredients")
check_negative(ingredients, ["unit_cost"], "Ingredients")
check_invalid_range(ingredients, "unit_cost", minimum=0, dataset="Ingredients")
check_invalid_values(ingredients, "unit", ["kg", "liters", "pieces"], "Ingredients")


print("\n===== MENU ITEM INGREDIENTS =====")
check_nulls(menu_item_ingredients, "Menu Item Ingredients")
check_duplicates(menu_item_ingredients, "menu_item_ingredient_id", "Menu Item Ingredients")
check_invalid_range(menu_item_ingredients, "quantity_required", minimum=0.001, dataset="Menu Item Ingredients")
check_foreign_key(menu_item_ingredients, "menu_item_id", menu_items, "menu_items_id", "Menu Item Ingredients")
check_foreign_key(menu_item_ingredients, "ingredient_id", ingredients, "ingredient_id", "Menu Item Ingredients")


print("\n===== RESTAURANT MENU ITEMS =====")
check_nulls(restaurant_menu_items, "Restaurant Menu Items")
check_duplicates(restaurant_menu_items, "restaurant_menu_id", "Restaurant Menu Items")
check_negative(restaurant_menu_items, ["price"], "Restaurant Menu Items")
check_foreign_key(restaurant_menu_items, "restaurant_id", restaurants, "restaurant_id", "Restaurant Menu Items")
check_foreign_key(restaurant_menu_items, "item_id", menu_items, "menu_items_id", "Restaurant Menu Items")


print("\n===== PRICING HISTORY =====")
check_nulls(pricing_history, "Pricing History")
check_duplicates(pricing_history, "price_history_id", "Pricing History")
check_negative(pricing_history, ["old_price", "new_price"], "Pricing History")
check_foreign_key(pricing_history, "restaurant_id", restaurants, "restaurant_id", "Pricing History")
check_foreign_key(pricing_history, "item_id", menu_items, "menu_items_id", "Pricing History")


print("\n===== PROMOTIONS =====")
check_nulls(promotions, "Promotions")
check_duplicates(promotions, "promotion_id", "Promotions")
check_negative(promotions, ["discount_value", "minimum_order_value"], "Promotions")
check_foreign_key(promotions, "restaurant_id", restaurants, "restaurant_id", "Promotions")
check_foreign_key(promotions, "item_id", menu_items, "menu_items_id", "Promotions")


print("\n===== RATINGS =====")
check_nulls(ratings, "Ratings")
check_duplicates(ratings, "rating_id", "Ratings")
check_invalid_range(ratings, "rating", minimum=1, maximum=5, dataset="Ratings")
check_foreign_key(ratings, "customer_id", customers, "customer_id", "Ratings")
check_foreign_key(ratings, "order_id", orders, "order_id", "Ratings")
check_foreign_key(ratings, "restaurant_id", restaurants, "restaurant_id", "Ratings")
check_foreign_key(ratings, "item_id", menu_items, "menu_items_id", "Ratings")


print("\n===== INVENTORY =====")
check_nulls(inventory, "Inventory")
check_duplicates(inventory, "inventory_id", "Inventory")
check_negative(inventory, ["quantity_received", "quantity_remaining", "unit_cost"], "Inventory")
check_invalid_range(inventory, "quantity_received", minimum=0.01, dataset="Inventory")
check_foreign_key(inventory, "restaurant_id", restaurants, "restaurant_id", "Inventory")
check_foreign_key(inventory, "ingredient_id", ingredients, "ingredient_id", "Inventory")


print("\n===== WASTAGE =====")
check_nulls(wastage, "Wastage")
check_duplicates(wastage, "wastage_id", "Wastage")
check_negative(wastage, ["quantity", "unit_cost", "cost"], "Wastage")
check_invalid_range(wastage, "quantity", minimum=0.01, dataset="Wastage")
check_foreign_key(wastage, "restaurant_id", restaurants, "restaurant_id", "Wastage")
check_foreign_key(wastage, "ingredient_id", ingredients, "ingredient_id", "Wastage")
check_foreign_key(wastage, "inventory_id", inventory, "inventory_id", "Wastage")
check_impossible_wastage(wastage, inventory)


print("\n=== UNIT CONSISTENCY ===")
check_inconsistent_units(ingredients, inventory, wastage)


save_quality_report()