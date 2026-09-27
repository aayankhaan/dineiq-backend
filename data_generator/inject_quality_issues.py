import pandas as pd
import random
from config.settings import RAW_DATA_FOLDER

random.seed(42)

raw_data_folder = RAW_DATA_FOLDER

print("loading raw datasets...")

orders = pd.read_csv(f"{raw_data_folder}/orders.csv")
order_items = pd.read_csv(f"{raw_data_folder}/order_items.csv")
menu_items = pd.read_csv(f"{raw_data_folder}/menu_items.csv")
ratings = pd.read_csv(f"{raw_data_folder}/ratings.csv")
inventory = pd.read_csv(f"{raw_data_folder}/inventory.csv")
wastage = pd.read_csv(f"{raw_data_folder}/wastage.csv")

def random_indexes(df, amount, excluded=None):
    available = list(df.index)

    if excluded:
        available = [index for index in available if index not in excluded]

    return random.sample(available, amount)

print("injecting quality issues..")

used_order_indexes = set()
used_order_item_indexes = set()
used_inventory_indexes = set()
used_wastage_indexes = set()

missing_customer_indexes = random_indexes(orders, 50, used_order_indexes)
used_order_indexes.update(missing_customer_indexes)
orders.loc[missing_customer_indexes, "customer_id"] = None

invalid_restaurant_indexes = random_indexes(orders, 25, used_order_indexes)
used_order_indexes.update(invalid_restaurant_indexes)
orders.loc[invalid_restaurant_indexes, "restaurant_id"] = 9999

incorrect_discount_indexes = random_indexes(orders, 30, used_order_indexes)
used_order_indexes.update(incorrect_discount_indexes)
orders.loc[incorrect_discount_indexes, "discount_amount"] = orders.loc[incorrect_discount_indexes, "subtotal"] + 500

invalid_date_indexes = random_indexes(orders, 20, used_order_indexes)
used_order_indexes.update(invalid_date_indexes)
orders.loc[invalid_date_indexes, "order_datetime"] = "2035-01-01 12:00:00"

duplicate_order_item_indexes = random_indexes(order_items, 100)
duplicate_order_items = order_items.loc[duplicate_order_item_indexes].copy()
order_items = pd.concat([order_items, duplicate_order_items], ignore_index=True)

negative_quantity_indexes = random_indexes(order_items, 50, used_order_item_indexes)
used_order_item_indexes.update(negative_quantity_indexes)
order_items.loc[negative_quantity_indexes, "quantity"] = -1

invalid_item_indexes = random_indexes(order_items, 30, used_order_item_indexes)
used_order_item_indexes.update(invalid_item_indexes)
order_items.loc[invalid_item_indexes, "item_id"] = 9999

invalid_price_indexes = random_indexes(menu_items, 10)
for index in invalid_price_indexes[:5]:
    menu_items.loc[index, "base_price"] = 0

for index in invalid_price_indexes[5:]:
    menu_items.loc[index, "base_price"] = -100

invalid_rating_indexes = random_indexes(ratings, 50)
for index in invalid_rating_indexes[:25]:
    ratings.loc[index, "rating"] = 0

for index in invalid_rating_indexes[25:]:
    ratings.loc[index, "rating"] = 6

inventory_unit_indexes = random_indexes(inventory, 30, used_inventory_indexes)
used_inventory_indexes.update(inventory_unit_indexes)

for index in inventory_unit_indexes:
    current_unit = inventory.loc[index, "unit"]

    if current_unit == "kg":
        inventory.loc[index, "unit"] = "liters"
    elif current_unit == "liters":
        inventory.loc[index, "unit"] = "pieces"
    else:
        inventory.loc[index, "unit"] = "kg"

impossible_inventory_indexes = random_indexes(inventory, 25, used_inventory_indexes)
used_inventory_indexes.update(impossible_inventory_indexes)

inventory.loc[impossible_inventory_indexes, "quantity_remaining"] = (
    inventory.loc[impossible_inventory_indexes, "quantity_received"] + 100
)

wastage_unit_indexes = random_indexes(wastage, 25, used_wastage_indexes)
used_wastage_indexes.update(wastage_unit_indexes)

for index in wastage_unit_indexes:
    current_unit = wastage.loc[index, "unit"]

    if current_unit == "kg":
        wastage.loc[index, "unit"] = "liters"
    elif current_unit == "liters":
        wastage.loc[index, "unit"] = "pieces"
    else:
        wastage.loc[index, "unit"] = "kg"

impossible_wastage_indexes = random_indexes(wastage, 30, used_wastage_indexes)
used_wastage_indexes.update(impossible_wastage_indexes)

inventory_received_lookup = inventory.set_index("inventory_id")["quantity_received"].to_dict()

for index in impossible_wastage_indexes:
    inventory_id = wastage.loc[index, "inventory_id"]
    quantity_received = inventory_received_lookup[inventory_id]
    wastage.loc[index, "quantity"] = quantity_received + 100

invalid_ingredient_indexes = random_indexes(wastage, 20, used_wastage_indexes)
used_wastage_indexes.update(invalid_ingredient_indexes)
wastage.loc[invalid_ingredient_indexes, "ingredient_id"] = 9999

print("saving corrupted datasets..")
orders["customer_id"] = orders["customer_id"].astype("Int64")
orders_df = pd.DataFrame(orders)
orders_df["promotion_id"] = orders_df["promotion_id"].astype("Int64")
orders_df.to_csv(f"{raw_data_folder}/orders.csv", index=False)
order_items.to_csv(f"{raw_data_folder}/order_items.csv", index=False)
menu_items.to_csv(f"{raw_data_folder}/menu_items.csv", index=False)
ratings.to_csv(f"{raw_data_folder}/ratings.csv", index=False)
inventory.to_csv(f"{raw_data_folder}/inventory.csv", index=False)
wastage.to_csv(f"{raw_data_folder}/wastage.csv", index=False)

print("\nQUALITY ISSUES INJECTED")
print("------------------------")
print("Missing customer IDs:        50")
print("Invalid restaurant IDs:      25")
print("Incorrect discounts:         30")
print("Invalid order dates:         20")
print("Duplicate order lines:      100")
print("Negative quantities:         50")
print("Invalid menu IDs:            30")
print("Invalid menu prices:         10")
print("Invalid ratings:             50")
print("Inventory unit mismatches:   30")
print("Impossible inventory:        25")
print("Wastage unit mismatches:     25")
print("Impossible wastage:          30")
print("Invalid ingredient IDs:      20")
print("------------------------")
print("Quality issue injection complete.")