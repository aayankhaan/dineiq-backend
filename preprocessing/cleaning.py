import os
import csv
from pyspark.sql import functions as F
from spark.ingest import *

output_folder = "processed_data"
report_folder = "reports"

os.makedirs(output_folder, exist_ok=True)
os.makedirs(report_folder, exist_ok=True)

cleaning_report = []

def add_cleaning_report(dataset, issue, action, affected_rows, reason):
    cleaning_report.append({"dataset": dataset, "issue": issue, "action": action, "affected_rows": affected_rows, "reason": reason})

def count_unit_mismatches(df, ingredients_df):
    return df.alias("d").join(ingredients_df.alias("i"), F.col("d.ingredient_id") == F.col("i.ingredient_id"), "inner").filter(F.col("d.unit") != F.col("i.unit")).count()

def count_invalid_references(df, column, reference_df, reference_column):
    return df.alias("d").join(reference_df.select(F.col(reference_column).alias("valid_reference_id")), F.col(f"d.{column}") == F.col("valid_reference_id"), "left_anti").filter(F.col(f"d.{column}").isNotNull()).count()

def remove_duplicates(df, column):
    return df.dropDuplicates([column])

def keep_valid_reference(df, column, reference_df, reference_column):
    return df.join(
        reference_df.select(
            F.col(reference_column).alias("valid_reference_id")
        ),
        F.col(column) == F.col("valid_reference_id"),
        "inner"
    ).drop("valid_reference_id")

def remove_invalid_quantities(df):
    return df.filter(F.col("quantity") >= 1)

def remove_missing_customer_ids(df):
    return df.filter(F.col("customer_id").isNotNull())

def remove_invalid_discount(df):
    return df.filter(
        (F.col("discount_amount") >= 0) &
        (F.col("discount_amount") <= F.col("subtotal"))
    )

def remove_invalid_order_dates(df):
    return df.filter(
        (F.col("order_datetime") >= F.lit("2025-03-01")) &
        (F.col("order_datetime") < F.lit("2026-09-01"))
    )

def clean_ingredient_data(df):
    return df.filter(
        (F.col("ingredient_id").isNotNull()) &
        (F.col("unit_cost") > 0) &
        (F.col("unit").isNotNull())
    )

def clean_menu_item_ingredient_data(df, menu_items_df, ingredients_df):
    return df.filter(
        F.col("quantity_required") > 0
    ).join(
        menu_items_df.select(
            F.col("menu_items_id").alias("valid_menu_item_id")
        ),
        F.col("menu_item_id") == F.col("valid_menu_item_id"),
        "inner"
    ).drop("valid_menu_item_id").join(
        ingredients_df.select(
            F.col("ingredient_id").alias("valid_ingredient_id")
        ),
        F.col("ingredient_id") == F.col("valid_ingredient_id"),
        "inner"
    ).drop("valid_ingredient_id")

def calculate_menu_item_costs(menu_item_ingredients_df, ingredients_df):
    return menu_item_ingredients_df.join(
        ingredients_df.select(
            "ingredient_id",
            "unit_cost"
        ),
        "ingredient_id",
        "inner"
    ).withColumn(
        "ingredient_cost",
        F.col("quantity_required") * F.col("unit_cost")
    ).groupBy(
        "menu_item_id"
    ).agg(
        F.round(F.sum("ingredient_cost"), 2).alias("item_cost")
    )

def calculate_average_restaurant_prices(restaurant_menu_items_df):
    return restaurant_menu_items_df.filter(
        F.col("price") > 0
    ).groupBy("item_id").agg(
        F.round(F.avg("price"), 2).alias("average_restaurant_price")
    )

def calculate_average_markup(menu_items_df, menu_item_costs_df):
    return menu_items_df.join(
        menu_item_costs_df,
        F.col("menu_items_id") == F.col("menu_item_id"),
        "inner"
    ).filter(
        (F.col("base_price") > 0) &
        (F.col("item_cost") > 0)
    ).withColumn(
        "markup_ratio",
        F.col("base_price") / F.col("item_cost")
    ).agg(
        F.round(F.avg("markup_ratio"), 2).alias("average_markup")
    ).first()["average_markup"]

def fix_invalid_menu_prices(df, restaurant_prices_df, item_costs_df, average_markup):
    return df.join(
        restaurant_prices_df,
        F.col("menu_items_id") == F.col("item_id"),
        "left"
    ).drop("item_id").join(
        item_costs_df,
        F.col("menu_items_id") == F.col("menu_item_id"),
        "left"
    ).drop("menu_item_id").withColumn(
        "base_price",
        F.when(
            F.col("base_price") <= 0,
            F.round(
                F.when(
                    (F.col("average_restaurant_price").isNotNull()) &
                    (F.col("average_restaurant_price") > F.col("item_cost")),
                    F.col("average_restaurant_price")
                ).when(
                    F.col("item_cost").isNotNull(),
                    F.col("item_cost") * F.lit(average_markup)
                ).otherwise(
                    F.col("base_price")
                ),
                2
            )
        ).otherwise(F.col("base_price"))
    ).drop("average_restaurant_price", "item_cost")

def fix_inventory_units(df, ingredients_df):
    return df.join(
        ingredients_df.select(
            "ingredient_id",
            F.col("unit").alias("correct_unit")
        ),
        "ingredient_id",
        "left"
    ).withColumn(
        "unit",
        F.coalesce(F.col("correct_unit"), F.col("unit"))
    ).drop("correct_unit")

def remove_invalid_inventory_quantities(df):
    return df.filter(
        (F.col("quantity_received") >= 0) &
        (F.col("quantity_remaining") >= 0) &
        (F.col("quantity_remaining") <= F.col("quantity_received")) &
        (F.col("unit_cost") >= 0)
    )

def fix_wastage_units(df, ingredients_df):
    return df.join(
        ingredients_df.select(
            "ingredient_id",
            F.col("unit").alias("correct_unit")
        ),
        "ingredient_id",
        "left"
    ).withColumn(
        "unit",
        F.coalesce(F.col("correct_unit"), F.col("unit"))
    ).drop("correct_unit")

def remove_impossible_wastage(df, inventory_df):
    return df.alias("w").join(
        inventory_df.select(
            "inventory_id",
            "quantity_received"
        ).alias("i"),
        F.col("w.inventory_id") == F.col("i.inventory_id"),
        "inner"
    ).filter(
        (F.col("w.quantity") >= 0) &
        (F.col("w.quantity") <= F.col("i.quantity_received"))
    ).select("w.*")

def fix_wastage_cost(df):
    return df.withColumn(
        "cost",
        F.round(F.col("quantity") * F.col("unit_cost"), 2)
    )

def clean_restaurant_menu_item_data(df, restaurants_df, menu_items_df):
    return df.filter(
        F.col("price") > 0
    ).join(
        restaurants_df.select(
            F.col("restaurant_id").alias("valid_restaurant_id")
        ),
        F.col("restaurant_id") == F.col("valid_restaurant_id"),
        "inner"
    ).drop("valid_restaurant_id").join(
        menu_items_df.select(
            F.col("menu_items_id").alias("valid_item_id")
        ),
        F.col("item_id") == F.col("valid_item_id"),
        "inner"
    ).drop("valid_item_id")

def clean_pricing_history_data(df, restaurants_df, menu_items_df):
    return df.filter(
        (F.col("old_price") > 0) &
        (F.col("new_price") > 0) &
        F.col("effective_date").isNotNull()
    ).join(
        restaurants_df.select(
            F.col("restaurant_id").alias("valid_restaurant_id")
        ),
        F.col("restaurant_id") == F.col("valid_restaurant_id"),
        "inner"
    ).drop("valid_restaurant_id").join(
        menu_items_df.select(
            F.col("menu_items_id").alias("valid_item_id")
        ),
        F.col("item_id") == F.col("valid_item_id"),
        "inner"
    ).drop("valid_item_id")

def clean_promotions_data(df, restaurants_df, menu_items_df):
    return df.filter(
        (F.col("discount_value") >= 0) &
        (F.col("start_date").isNotNull()) &
        (F.col("end_date").isNotNull()) &
        (F.col("end_date") >= F.col("start_date")) &
        (F.col("minimum_order_value").isNull() | (F.col("minimum_order_value") >= 0))
    ).filter(
        F.col("restaurant_id").isNull() |
        F.col("restaurant_id").isin([row.restaurant_id for row in restaurants_df.select("restaurant_id").collect()])
    ).filter(
        F.col("item_id").isNull() |
        F.col("item_id").isin([row.menu_items_id for row in menu_items_df.select("menu_items_id").collect()])
    )

def clean_ratings_data(df, customers_df, orders_df, restaurants_df, menu_items_df):
    return df.filter(
        (F.col("rating") >= 1) &
        (F.col("rating") <= 5)
    ).join(
        customers_df.select(F.col("customer_id").alias("valid_customer_id")),
        F.col("customer_id") == F.col("valid_customer_id"),
        "inner"
    ).drop("valid_customer_id").join(
        orders_df.select(F.col("order_id").alias("valid_order_id")),
        F.col("order_id") == F.col("valid_order_id"),
        "inner"
    ).drop("valid_order_id").join(
        restaurants_df.select(F.col("restaurant_id").alias("valid_restaurant_id")),
        F.col("restaurant_id") == F.col("valid_restaurant_id"),
        "inner"
    ).drop("valid_restaurant_id").join(
        menu_items_df.select(F.col("menu_items_id").alias("valid_item_id")),
        F.col("item_id") == F.col("valid_item_id"),
        "inner"
    ).drop("valid_item_id")

def save_cleaning_report():
    with open(f"{report_folder}/cleaning_report.csv", "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=["dataset", "issue", "action", "affected_rows", "reason"])
        writer.writeheader()
        writer.writerows(cleaning_report)

def save_processed_data():
    datasets = {
        "customers": clean_customers,
        "restaurants": clean_restaurants,
        "menu_categories": clean_menu_categories,
        "ingredients": clean_ingredients,
        "menu_items": clean_menu_items,
        "menu_item_ingredients": clean_menu_item_ingredients,
        "restaurant_menu_items": clean_restaurant_menu_items,
        "pricing_history": clean_pricing_history,
        "promotions": clean_promotions,
        "inventory": clean_inventory,
        "orders": clean_orders,
        "order_items": clean_order_items,
        "ratings": clean_ratings,
        "wastage": clean_wastage,
        "menu_item_costs": menu_item_costs
    }
    for name, df in datasets.items():
        df.write.mode("overwrite").parquet(f"{output_folder}/{name}")

clean_customers = remove_duplicates(customers, "customer_id")
clean_restaurants = remove_duplicates(restaurants, "restaurant_id")
clean_menu_categories = remove_duplicates(menu_categories, "category_id")
clean_ingredients = remove_duplicates(ingredients, "ingredient_id")
clean_ingredients = clean_ingredient_data(clean_ingredients)

clean_menu_item_ingredients = remove_duplicates(menu_item_ingredients, "menu_item_ingredient_id")
clean_menu_item_ingredients = clean_menu_item_ingredient_data(clean_menu_item_ingredients, menu_items, clean_ingredients)

menu_item_costs = calculate_menu_item_costs(clean_menu_item_ingredients, clean_ingredients)

clean_restaurant_menu_items = remove_duplicates(restaurant_menu_items, "restaurant_menu_id")
clean_restaurant_menu_items = clean_restaurant_menu_item_data(clean_restaurant_menu_items, clean_restaurants, menu_items)

average_restaurant_prices = calculate_average_restaurant_prices(clean_restaurant_menu_items)
average_markup = calculate_average_markup(menu_items, menu_item_costs)
clean_menu_items = fix_invalid_menu_prices(menu_items, average_restaurant_prices, menu_item_costs, average_markup)
clean_menu_items = remove_duplicates(clean_menu_items, "menu_items_id")
clean_menu_items = keep_valid_reference(clean_menu_items, "cat_id", clean_menu_categories, "category_id")

clean_inventory = remove_duplicates(inventory, "inventory_id")
clean_inventory = keep_valid_reference(clean_inventory, "restaurant_id", clean_restaurants, "restaurant_id")
clean_inventory = keep_valid_reference(clean_inventory, "ingredient_id", clean_ingredients, "ingredient_id")
clean_inventory = fix_inventory_units(clean_inventory, clean_ingredients)
clean_inventory = remove_invalid_inventory_quantities(clean_inventory)

clean_promotions = remove_duplicates(promotions, "promotion_id")
clean_promotions = clean_promotions_data(clean_promotions, clean_restaurants, clean_menu_items)

clean_orders = remove_duplicates(orders, "order_id")
clean_orders = remove_missing_customer_ids(clean_orders)
clean_orders = keep_valid_reference(clean_orders, "customer_id", clean_customers, "customer_id")
clean_orders = keep_valid_reference(clean_orders, "restaurant_id", clean_restaurants, "restaurant_id")
clean_orders = remove_invalid_discount(clean_orders)
clean_orders = remove_invalid_order_dates(clean_orders)

clean_order_items = remove_duplicates(order_items, "order_item_id")
clean_order_items = remove_invalid_quantities(clean_order_items)
clean_order_items = keep_valid_reference(clean_order_items, "order_id", clean_orders, "order_id")
clean_order_items = keep_valid_reference(clean_order_items, "item_id", clean_menu_items, "menu_items_id")
clean_order_items = clean_order_items.filter(
    (F.col("unit_price") >= 0) &
    (F.col("discount_amount") >= 0) &
    (F.col("line_total") >= 0)
)

clean_pricing_history = remove_duplicates(pricing_history, "price_history_id")
clean_pricing_history = clean_pricing_history_data(clean_pricing_history, clean_restaurants, clean_menu_items)

clean_ratings = remove_duplicates(ratings, "rating_id")
clean_ratings = clean_ratings_data(clean_ratings, clean_customers, clean_orders, clean_restaurants, clean_menu_items)

clean_wastage = remove_duplicates(wastage, "wastage_id")
clean_wastage = keep_valid_reference(clean_wastage, "ingredient_id", clean_ingredients, "ingredient_id")
clean_wastage = keep_valid_reference(clean_wastage, "inventory_id", clean_inventory, "inventory_id")
clean_wastage = keep_valid_reference(clean_wastage, "restaurant_id", clean_restaurants, "restaurant_id")
clean_wastage = fix_wastage_units(clean_wastage, clean_ingredients)
clean_wastage = remove_impossible_wastage(clean_wastage, clean_inventory)
clean_wastage = clean_wastage.filter(F.col("unit_cost") >= 0)
clean_wastage = fix_wastage_cost(clean_wastage)

counts = {
    "Customers": (customers.count(), clean_customers.count()),
    "Restaurants": (restaurants.count(), clean_restaurants.count()),
    "Menu Categories": (menu_categories.count(), clean_menu_categories.count()),
    "Ingredients": (ingredients.count(), clean_ingredients.count()),
    "Menu Items": (menu_items.count(), clean_menu_items.count()),
    "Menu Item Ingredients": (menu_item_ingredients.count(), clean_menu_item_ingredients.count()),
    "Restaurant Menu Items": (restaurant_menu_items.count(), clean_restaurant_menu_items.count()),
    "Pricing History": (pricing_history.count(), clean_pricing_history.count()),
    "Promotions": (promotions.count(), clean_promotions.count()),
    "Inventory": (inventory.count(), clean_inventory.count()),
    "Orders": (orders.count(), clean_orders.count()),
    "Order Items": (order_items.count(), clean_order_items.count()),
    "Ratings": (ratings.count(), clean_ratings.count()),
    "Wastage": (wastage.count(), clean_wastage.count())
}

inventory_unit_mismatches = count_unit_mismatches(inventory, clean_ingredients)
wastage_unit_mismatches = count_unit_mismatches(wastage, clean_ingredients)

cleaning_decisions = [
    ("Orders", "Duplicate order record", "Deduplicated", orders.count() - remove_duplicates(orders, "order_id").count(), "Duplicate order_id records are not valid independent transactions"),
    ("Orders", "Missing customer ID", "Removed", orders.filter(F.col("customer_id").isNull()).count(), "Customer could not be reliably identified"),
    ("Orders", "Invalid customer reference", "Removed", count_invalid_references(orders, "customer_id", clean_customers, "customer_id"), "Customer ID did not exist in customer master data"),
    ("Orders", "Invalid restaurant reference", "Removed", count_invalid_references(orders, "restaurant_id", clean_restaurants, "restaurant_id"), "Restaurant ID did not exist in restaurant master data"),
    ("Orders", "Incorrect discount", "Removed", orders.filter((F.col("discount_amount") < 0) | (F.col("discount_amount") > F.col("subtotal"))).count(), "Discount must be non-negative and cannot exceed the order subtotal"),
    ("Orders", "Invalid transaction date", "Removed", orders.filter((F.col("order_datetime") < F.lit("2025-03-01")) | (F.col("order_datetime") >= F.lit("2026-09-01"))).count(), "Order date fell outside the dataset transaction period"),
    ("Order Items", "Duplicate order-line record", "Deduplicated", order_items.count() - remove_duplicates(order_items, "order_item_id").count(), "Duplicate order_item_id records are not valid independent order lines"),
    ("Order Items", "Invalid quantity", "Removed", order_items.filter(F.col("quantity") < 1).count(), "Order item quantity must be at least 1"),
    ("Order Items", "Invalid menu item reference", "Removed", count_invalid_references(order_items, "item_id", clean_menu_items, "menu_items_id"), "Menu item ID did not exist in cleaned menu item data"),
    ("Order Items", "Invalid order dependency", "Removed", count_invalid_references(order_items, "order_id", clean_orders, "order_id"), "Referenced order did not survive order cleaning"),
    ("Menu Items", "Invalid base price", "Corrected", menu_items.filter(F.col("base_price") <= 0).count(), "Price reconstructed using restaurant pricing when cost-safe, otherwise ingredient cost and observed average markup"),
    ("Inventory", "Inconsistent unit", "Corrected", inventory_unit_mismatches, "Unit standardized using ingredient master data"),
    ("Inventory", "Impossible quantity", "Removed", inventory.filter((F.col("quantity_received") < 0) | (F.col("quantity_remaining") < 0) | (F.col("quantity_remaining") > F.col("quantity_received")) | (F.col("unit_cost") < 0)).count(), "Inventory quantities and unit cost must represent a valid inventory batch"),
    ("Ratings", "Invalid rating", "Removed", ratings.filter((F.col("rating") < 1) | (F.col("rating") > 5)).count(), "Rating must be between 1 and 5"),
    ("Ratings", "Invalid order dependency", "Removed", count_invalid_references(ratings, "order_id", clean_orders, "order_id"), "Referenced order did not survive order cleaning"),
    ("Wastage", "Invalid ingredient reference", "Removed", count_invalid_references(wastage, "ingredient_id", clean_ingredients, "ingredient_id"), "Ingredient ID did not exist in cleaned ingredient data"),
    ("Wastage", "Invalid inventory dependency", "Removed", count_invalid_references(wastage, "inventory_id", clean_inventory, "inventory_id"), "Referenced inventory batch did not survive inventory cleaning"),
    ("Wastage", "Inconsistent unit", "Corrected", wastage_unit_mismatches, "Unit standardized using ingredient master data"),
    ("Wastage", "Invalid cost", "Corrected", wastage.filter(F.round(F.col("quantity") * F.col("unit_cost"), 2) != F.round(F.col("cost"), 2)).count(), "Cost recalculated as quantity multiplied by unit cost")
]

for decision in cleaning_decisions:
    add_cleaning_report(*decision)

print("\n========CLEANING SUMMARY========\n")
for dataset, values in counts.items():
    print(f"{dataset}: {values[0]} -> {values[1]}")

print("\n========MENU ITEM PRICE CLEANING========\n")
print("Average markup:", average_markup)
print("Invalid menu prices before:", menu_items.filter(F.col("base_price") <= 0).count())
print("Invalid menu prices after:", clean_menu_items.filter(F.col("base_price") <= 0).count())

save_cleaning_report()
save_processed_data()

print("\nCleaning report saved to reports/cleaning_report.csv")
print("Processed Parquet datasets saved to processed_data/")
