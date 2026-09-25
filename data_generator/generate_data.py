import os
import pandas as pd
from faker import Faker
from datetime import date, timedelta
from faker_pk import FakerPKProvider
import random

random.seed(42)
Faker.seed(42)

fake = Faker()
fake.add_provider(FakerPKProvider)

output_folder = "raw_data"

DATASET_START = date(2025, 3, 1)
DATASET_END = date(2026, 8, 31)

city_areas = {
    "Karachi": [
        "Clifton", "DHA", "Gulshan-e-Iqbal", "North Nazimabad", "Saddar", "Bahadurabad", "Korangi"
    ],
    "Lahore": [
        "Gulberg", "DHA", "Johar Town", "Model Town", "Bahria Town", "Garden Town", "Cantt", "Wapda Town"
    ],
    "Islamabad": [
        "F-6", "F-7", "F-8", "F-10", "G-9", "G-11", "Blue Area", "I-8"
    ],
    "Rawalpindi": [
        "Saddar", "Bahria Town", "Satellite Town", "Chaklala", "Westridge", "PWD", "Commercial Market", "Adiala Road"
    ],
    "Peshawar": [
        "Hayatabad", "University Town", "Saddar", "Gulbahar", "Tehkal", "Warsak Road", "Ring Road", "Board Bazaar"
    ]
}
category_pools = {
    1: {  
        "styles": ["Garlic", "Spicy", "Crispy", "Loaded", "Honey Mustard", "Cheesy",
                   "Chatpata", "Masala", "Peri Peri", "Tandoori"],
        "types": ["Fries", "Wings", "Nuggets", "Cheese Sticks", "Samosa Bites",
                  "Pakora", "Chicken Rolls", "Spring Rolls"]
    },
    2: { 
        "styles": ["Reshmi", "Tikka", "Behari", "Malai", "Kasturi", "Achari",
                   "Chargha Style", "Peshawari", "Angara", "Bihari"],
        "types": ["Chicken Seekh", "Beef Boti", "Kebab", "Chops", "Sajji",
                  "Tikka Leg Piece", "Malai Boti", "Chicken Chargha"]
    },
    3: {  
        "styles": ["Classic", "Crispy", "Spicy", "Smoky", "Double", "Loaded",
                   "Grilled", "Zafrani", "Desi Style"],
        "types": ["Beef", "Chicken", "Zinger", "Club", "Steak", "Bihari Boti"],
        "endings": ["Burger", "Cheese Burger", "Sandwich", "Wrap"]
    },
    4: { 
        "styles": ["Peshawari", "Makhni", "Achari", "Desi White", "Koyla", "Shinwari",
                   "Lahori", "Green Chilli", "Dhaba Style", "Kala Namak"],
        "types": ["Chicken", "Mutton", "Paneer", "Beef"],
        "endings": ["Karahi", "Handi", "Balti"]
    },
    5: { 
        "styles": ["Sindhi", "Bombay", "Special VIP", "Nawabi", "Kachay Gosht Ki",
                   "Hyderabadi", "Karachi Style", "Dum"],
        "types": ["Chicken", "Beef", "Mutton", "Egg Fried", "Prawn"],
        "endings": ["Biryani", "Pulao", "Rice", "Tehari"]
    },
    6: {  
        "styles": ["Creamy Alfredo", "Spicy Arrabbiata", "Cheesy Baked", "Chilli Garlic",
                   "Peri Peri", "Chinese Style", "White Sauce", "Schezwan"],
        "types": ["Chicken", "Prawn", "Vegetable", "Beef"],
        "endings": ["Pasta", "Noodles", "Spaghetti", "Chowmein"]
    },
    7: {
        "styles": [
            "Chicken Tikka", "Fajita", "Mughlai", "Pepperoni",
            "Veggie", "Malai Boti", "BBQ Chicken", "Tandoori",
            "Cheese", "Achari Chicken", "Behari Boti",
            "Spicy Sausage", "Hawaiian", "Loaded Meat", "Karahi"
        ],
        "endings": [
            "Pizza",
            "Supreme Pizza",
            "Special Pizza"
        ]
    },
    8: {  
        "styles": ["Shahi", "Warm", "Chilled", "Chocolate", "Lotus Biscoff",
                   "Kesar Pista", "Caramel", "Rabri"],
        "types": ["Kheer", "Gulab Jamun", "Brownie", "Ice Cream", "Molten Lava Cake",
                  "Ras Malai", "Kulfi"]
    },
    9: { 
        "styles": ["Karak", "Doodh Patti", "Kashmiri", "Green", "Espresso", "Cappuccino",
                   "Adrak", "Elaichi", "Peshawari Qehwa"],
        "endings": ["Chai", "Tea", "Coffee", "Qehwa"]
    },
    10: { 
        "styles": ["Mint", "Classic", "Mango", "Blueberry", "Chocolate", "Fresh Lime",
                   "Rose", "Strawberry"],
        "types": ["Lemonade", "Margarita", "Shake", "Lassi", "Soda", "Sharbat"]
    },
    11: { 
        "styles": ["Chicken", "Beef", "Seekh Kebab", "Aloo", "Keema", "Malai Boti",
                   "Spicy Chicken", "Achari"],
        "endings": ["Roll", "Paratha", "Wrap", "Frankie"]
    },
    12: { 
        "styles": ["Aloo", "Chana", "Mix Veg", "Daal", "Bhindi", "Palak", "Raita"],
        "types": ["Chana Masala", "Daal Mash", "Daal Chana", "Raita", "Salad", "Chutney"]
    }
}
category_price_ranges = {
    1: (250, 650),    
    2: (500, 1800),   
    3: (350, 950),    
    4: (450, 1600),   
    5: (400, 1400), 
    6: (450, 1100),  
    7: (700, 2200),  
    8: (200, 700), 
    9: (100, 350),  
    10: (150, 500),   
    11: (200, 600),  
    12: (150, 450),  
}
category_prep_time = {
    1: (5, 12), 2: (15, 30), 3: (8, 15), 4: (18, 35),
    5: (15, 30), 6: (10, 20), 7: (12, 22), 8: (3, 10),
    9: (3, 8), 10: (2, 6), 11: (6, 15), 12: (10, 20),
}
reasons = [
        "Cost Increase",
        "Market Adjustment",
        "Supplier Cost Change",
        "Menu Revision",
        "Seasonal Adjustment"
    ]
promotion_names = [
    "Ramadan Iftar Deal",
    "Ramadan Sehri Special",
    "Eid Special",
    "Eid Family Feast",
    "Independence Day Special",
    "Defence Day Deal",
    "Weekend Special",
    "Friday Feast",
    "Family Dinner Deal",
    "Lunch Special",
    "Midnight Deal",
    "Student Special",
    "Summer Special",
    "Winter Special",
    "Monsoon Deal",
    "Happy Hour",
    "Payday Special",
    "Online Order Deal",
    "Delivery Special",
    "New Year Special"
]
percentage_discounts = [5, 10, 15, 20, 25, 30]
flat_discounts = [100, 150, 200, 250, 300, 500]
wastage_reasons = ["Expired", "Spoiled", "Overproduction", "Damaged"]
prefixes = ["Desi", "Khyber", "Shahi", "Hot", "Savor", "Royal", "Gourmet"]
suffixes = ["Dhaba", "Karahi & BBQ", "Shinwari", "Foods", "Kitchen", "Lounge"]

def generate_prep_time(cat_id):
    low, high = category_prep_time.get(cat_id, (5, 20))
    return random.randint(low, high)

def generate_price_and_cost(cat_id):
    low, high = category_price_ranges.get(cat_id)
    base_price = round(random.randint(low, high), -1)
    food_cost_pct = random.uniform(0.28, 0.38)
    base_cost = round(base_price * food_cost_pct, -1)
    return base_price, base_cost

def generate_restaurant_price(base_price):
    multiplier = random.uniform(0.90, 1.10)

    price = base_price * multiplier
    return round(price, -1)

def get_price_at_date(restaurant_id, item_id, order_date, restaurant_menu_items, pricing_history):
    current = next(x["price"] for x in restaurant_menu_items if x["restaurant_id"] == restaurant_id and x["item_id"] == item_id)
    changes = [x for x in pricing_history if x["restaurant_id"] == restaurant_id and x["item_id"] == item_id]

    if not changes:
        return current

    changes.sort(key=lambda x: x["effective_date"])

    for change in changes:
        if order_date < change["effective_date"]:
            return change["old_price"]

    return changes[-1]["new_price"]

used_restaurant_names = set()
def generate_restaurant_name(city, area, max_attempts=50):
    for _ in range(max_attempts):
        style_format = random.choice([1, 2, 3])
        
        if style_format == 1:
            name = f"{area} {random.choice(suffixes)}"
        elif style_format == 2:
            name = f"The {city} {random.choice(suffixes)}"
        else:
            name = f"{random.choice(prefixes)} {random.choice(suffixes)}"
        if name not in used_restaurant_names:
            used_restaurant_names.add(name)
            return f"{name} - {area}"
    raise RuntimeError("failed to generate new name")

used_menu_item_names = set()
def generate_menu_item_name(cat_id, max_attempts=50):
    components = category_pools.get(cat_id, {"styles": ["Generic"], "types": ["Dish"]})

    for _ in range(max_attempts):
        style_format = random.choice(components.get("styles", [""]))
        item_type = random.choice(components.get("types", [""]))
        ending = random.choice(components.get("endings", [""]))

        name_parts = [part for part in [style_format, item_type, ending] if part]
        item_name = " ".join(name_parts)

        if item_name not in used_menu_item_names:
            used_menu_item_names.add(item_name)
            return item_name
    raise RuntimeError("failed to generate new name")

def generate_customers(n=100):
    customers = []
    for i in range(1, n + 1):
        customers.append({
            "customer_id": i,
            "signup_date": fake.date_between(start_date=DATASET_END - timedelta(days=1460), end_date=DATASET_END),
            "birth_year": fake.date_of_birth(minimum_age=16, maximum_age=60).year,
            "gender": random.choice(["M","F"]),
            "city": random.choice(list(city_areas.keys()))
        })
    return customers

def generate_restaurant(n=20):
    restaurants = []
    for i in range(1, n + 1):
        city = random.choice(list(city_areas.keys()))
        area = random.choice(city_areas[city])
        restaurants.append({
            "restaurants_id": i,
            "name": generate_restaurant_name(city, area),
            "city": city,
            "area": area,
            "opening_date": fake.date_between(start_date=DATASET_END - timedelta(days=5110), end_date=DATASET_END),
            "status": "Active"
        })
    return restaurants

def generate_menu_categories():

    categories_data = [
        {"category_id": 1, "name": "Appetizers"},
        {"category_id": 2, "name": "BBQ & Grills"},
        {"category_id": 3, "name": "Burgers & Sandwiches"},
        {"category_id": 4, "name": "Karahi & Handi"},
        {"category_id": 5, "name": "Rice & Biryani"},
        {"category_id": 6, "name": "Pasta & Noodles"},
        {"category_id": 7, "name": "Pizza"},
        {"category_id": 8, "name": "Desserts"},
        {"category_id": 9, "name": "Hot Beverages"},
        {"category_id": 10, "name": "Cold Beverages"},
        {"category_id": 11, "name": "Rolls & Parathas"},
        {"category_id": 12, "name": "Salan & Sides"}
    ]
    return categories_data

def generate_menu_items(n=150):
    items = []
    categories = generate_menu_categories()
    cat_ids = [c["category_id"] for c in categories]

    for i in range(1, n + 1):
        cat_id = cat_ids[(i - 1) % len(cat_ids)]
        base_price, base_cost = generate_price_and_cost(cat_id)
        items.append({
            "menu_items_id": i,
            "cat_id": cat_id,
            "name": generate_menu_item_name(cat_id),
            "base_price": base_price,
            "base_cost": base_cost,
            "prep_time_minutes": generate_prep_time(cat_id),
            "introduced_date": fake.date_between(start_date=DATASET_END - timedelta(days=1460), end_date=DATASET_END),
            "discontinued_date": None
        })
    return items

def generate_restaurant_menu_items(restaurants, menu_items):
    restaurant_menu = []
    restaurant_menu_id = 1

    for restaurant in restaurants:
        for item in menu_items:

            if random.random() < 0.80:
                restaurant_price = generate_restaurant_price(item["base_price"])

                restaurant_menu.append({
                    "restaurant_menu_id": restaurant_menu_id,
                    "restaurant_id": restaurant["restaurants_id"],
                    "item_id": item["menu_items_id"],
                    "price": restaurant_price,
                    "is_available": True
                })

                restaurant_menu_id += 1

    return restaurant_menu

def generate_pricing_history(restaurant_menu_items, menu_items, restaurants):
    pricing_history = []
    price_history_id = 1
    item_lookup = {item["menu_items_id"]: item for item in menu_items}
    restaurant_lookup = {restaurant["restaurants_id"]: restaurant for restaurant in restaurants}

    for restaurant_item in restaurant_menu_items:

        item_id = restaurant_item["item_id"]
        restaurant_id = restaurant_item["restaurant_id"]
        current_price = restaurant_item["price"]

        item = item_lookup[item_id]
        restaurant = restaurant_lookup[restaurant_id]
        introduced_date = item["introduced_date"]
        opening_date = restaurant["opening_date"]

        history_start = max(DATASET_START, introduced_date, opening_date)

        if history_start >= DATASET_END - timedelta(days=30):
            continue

        num_changes = random.choices([0, 1, 2, 3],weights=[45, 35, 15, 5])[0]

        if num_changes == 0:
            continue

        available_days = (DATASET_END - history_start).days

        if available_days < num_changes * 30:
            num_changes = max(1, available_days // 30)

        if num_changes <= 0:
            continue

        possible_days = range(30, available_days + 1)

        if len(possible_days) < num_changes:
            continue

        offsets = sorted(random.sample(list(possible_days), num_changes))

        change_dates = [
            history_start + timedelta(days=offset)
            for offset in offsets
        ]

        prices = [current_price]

        for _ in range(num_changes):
            if random.random() < 0.85:
                change_pct = random.uniform(0.03, 0.10)
                previous_price = prices[0] / (1 + change_pct)
            else:
                change_pct = random.uniform(0.03, 0.08)
                previous_price = prices[0] / (1 - change_pct)

            previous_price = round(previous_price, -1)

            if previous_price == prices[0]:
                previous_price -= 10

            prices.insert(0, previous_price)

        for index, effective_date in enumerate(change_dates):

            pricing_history.append({
                "price_history_id": price_history_id,
                "restaurant_id": restaurant_id,
                "item_id": item_id,
                "old_price": prices[index],
                "new_price": prices[index + 1],
                "effective_date": effective_date,
                "reason": random.choice(reasons)
            })

            price_history_id += 1

    return pricing_history

def generate_promotion(n, restaurants, restaurant_menu_items, inventories):
    promotions = []

    for i in range(1, n + 1):
        inventory_driven = random.random() < 0.30

        if inventory_driven:
            valid_inventory = [
                inv for inv in inventories
                if inv["quantity_remaining"] >= inv["quantity_received"] * 0.40
                and inv["expiry_date"] <= DATASET_END
                and inv["expiry_date"] > DATASET_START + timedelta(days=2)
            ]

            if valid_inventory:
                inventory = random.choice(valid_inventory)
                restaurant_id = inventory["restaurant_id"]
                item_id = inventory["item_id"]

                selected_item = next(item for item in restaurant_menu_items if item["restaurant_id"] == restaurant_id and item["item_id"] == item_id)
                item_price = selected_item["price"]

                days_before_expiry = random.randint(2, 5)
                start_date = max(inventory["received_date"], inventory["expiry_date"] - timedelta(days=days_before_expiry))
                end_date = inventory["expiry_date"]

                name = random.choice(["Stock Clearance Deal", "Fresh Stock Special", "Limited Time Deal", "Chef's Special"])
            else:
                inventory_driven = False

        if not inventory_driven:
            restaurant = random.choice(restaurants)
            restaurant_id = restaurant["restaurants_id"]

            available_items = [item for item in restaurant_menu_items if item["restaurant_id"] == restaurant_id and item["is_available"]]

            if not available_items:
                continue

            selected_item = random.choice(available_items)
            item_id = selected_item["item_id"]
            item_price = selected_item["price"]

            start_date = fake.date_between(start_date=DATASET_START, end_date=DATASET_END - timedelta(days=3))
            duration_days = random.choice([3, 5, 7, 10, 14, 21, 30])
            end_date = min(start_date + timedelta(days=duration_days), DATASET_END)
            name = random.choice(promotion_names)

        discount_type = random.choice(["Percentage", "Flat"])

        if discount_type == "Percentage":
            discount_value = random.choice(percentage_discounts)
        else:
            max_discount = item_price * 0.60
            valid_discounts = [discount for discount in flat_discounts if discount <= max_discount]

            if valid_discounts:
                discount_value = random.choice(valid_discounts)
            else:
                discount_value = int(max_discount // 10) * 10

        minimum_order_value = random.choice([None, None, 500, 750, 1000, 1500, 2000, 2500])

        if random.random() < 0.35:
            coupon_code = "SAVE" + str(random.randint(10, 99))
        else:
            coupon_code = None

        promotions.append({
            "promotion_id": i,
            "name": name,
            "discount_type": discount_type,
            "discount_value": discount_value,
            "start_date": start_date,
            "end_date": end_date,
            "restaurant_id": restaurant_id,
            "item_id": item_id,
            "minimum_order_value": minimum_order_value,
            "coupon_code": coupon_code
        })

    return promotions

def generate_inventory(restaurant_menu_items, menu_items):
    inventories = []
    inventory_id = 1
    item_lookup = {item["menu_items_id"]: item for item in menu_items}

    for restaurant_item in restaurant_menu_items:
        restaurant_id = restaurant_item["restaurant_id"]
        item_id = restaurant_item["item_id"]
        item = item_lookup[item_id]

        inventory_start = max(DATASET_START, item["introduced_date"])
        if inventory_start >= DATASET_END:
            continue

        available_days = (DATASET_END - inventory_start).days
        num_batches = max(1, available_days // random.randint(20, 35))

        for _ in range(num_batches):
            received_date = inventory_start + timedelta(days=random.randint(0, max(0, available_days - 14)))
            quantity_received = random.randint(20, 120)
            quantity_remaining = random.randint(0, quantity_received)
            unit_cost = round(item["base_cost"] * random.uniform(0.90, 1.10), -1)
            expiry_date = min(received_date + timedelta(days=random.randint(3, 14)), DATASET_END)

            inventories.append({
                "inventory_id": inventory_id,
                "restaurant_id": restaurant_id,
                "item_id": item_id,
                "quantity_received": quantity_received,
                "quantity_remaining": quantity_remaining,
                "unit_cost": unit_cost,
                "received_date": received_date,
                "expiry_date": expiry_date
            })

            inventory_id += 1

    return inventories

def generate_orders(n, customers, restaurants, restaurant_menu_items, menu_items, pricing_history, promotions):
    orders = []
    order_items = []
    order_item_id = 1

    item_lookup = {x["menu_items_id"]: x for x in menu_items}
    restaurants_by_city = {}
    menu_by_restaurant = {}
    promotions_by_restaurant = {}
    price_lookup = {}
    pricing_lookup = {}

    for restaurant in restaurants:
        restaurants_by_city.setdefault(restaurant["city"], []).append(restaurant)

    for item in restaurant_menu_items:
        if item["is_available"]:
            menu_by_restaurant.setdefault(item["restaurant_id"], []).append(item)
        price_lookup[(item["restaurant_id"], item["item_id"])] = item["price"]

    for promotion in promotions:
        promotions_by_restaurant.setdefault(promotion["restaurant_id"], []).append(promotion)

    for change in pricing_history:
        pricing_lookup.setdefault((change["restaurant_id"], change["item_id"]), []).append(change)

    for changes in pricing_lookup.values():
        changes.sort(key=lambda x: x["effective_date"])

    for order_id in range(1, n + 1):
        customer = random.choice(customers)
        customer_id = customer["customer_id"]

        same_city = restaurants_by_city.get(customer["city"], [])
        restaurant = random.choice(same_city if same_city and random.random() < 0.97 else restaurants)
        restaurant_id = restaurant["restaurants_id"]

        earliest_date = max(DATASET_START, customer["signup_date"], restaurant["opening_date"])
        if earliest_date >= DATASET_END:
            continue

        order_date = fake.date_between(start_date=earliest_date, end_date=DATASET_END)

        if random.random() < 0.55:
            hour = random.choice([12, 13, 14, 18, 19, 20, 21, 22])
        else:
            hour = random.randint(10, 23)

        minute = random.randint(0, 59)
        order_datetime = pd.Timestamp(order_date.year, order_date.month, order_date.day, hour, minute)

        available_items = [
            x for x in menu_by_restaurant.get(restaurant_id, [])
            if item_lookup[x["item_id"]]["introduced_date"] <= order_date
            and (item_lookup[x["item_id"]]["discontinued_date"] is None or item_lookup[x["item_id"]]["discontinued_date"] >= order_date)
        ]

        if not available_items:
            continue

        available_item_ids = {item["item_id"] for item in available_items}
        active_promotions = [
            x for x in promotions_by_restaurant.get(restaurant_id, [])
            if x["start_date"] <= order_date <= x["end_date"]
            and x["item_id"] in available_item_ids
        ]

        promotion = random.choice(active_promotions) if active_promotions and random.random() < 0.85 else None
        promotion_id = promotion["promotion_id"] if promotion else None

        channel = random.choices(["Dine-in", "Takeaway", "Website/App", "Third-Party Delivery"],weights=[40, 20, 20, 20])[0]

        order_status = random.choices(["Completed", "Cancelled"], weights=[97, 3])[0]

        if promotion:
            promoted_item = next(x for x in available_items if x["item_id"] == promotion["item_id"])
            selected_items = [promoted_item]
        else:
            selected_items = []

        target_lines = random.randint(8, 12)

        while len(selected_items) < target_lines:
            selected_items.append(random.choice(available_items))

        subtotal = 0
        discount_amount = 0
        current_order_items = []

        for selected_item in selected_items:
            item_id = selected_item["item_id"]
            quantity = random.choices([1, 2, 3], weights=[75, 20, 5])[0]

            unit_price = price_lookup[(restaurant_id, item_id)]
            changes = pricing_lookup.get((restaurant_id, item_id), [])

            for change in changes:
                if order_date < change["effective_date"]:
                    unit_price = change["old_price"]
                    break
                unit_price = change["new_price"]

            unit_cost = round(item_lookup[item_id]["base_cost"] * random.uniform(0.90, 1.10), -1)

            line_subtotal = unit_price * quantity
            line_discount = 0

            if promotion and item_id == promotion["item_id"]:
                if promotion["discount_type"] == "Percentage":
                    line_discount = line_subtotal * (promotion["discount_value"] / 100)
                else:
                    line_discount = min(promotion["discount_value"], line_subtotal * 0.60)

            line_discount = round(line_discount, 2)
            line_total = round(line_subtotal - line_discount, 2)

            subtotal += line_subtotal
            discount_amount += line_discount

            order_item = {
                "order_item_id": order_item_id,
                "order_id": order_id,
                "item_id": item_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "unit_cost": unit_cost,
                "discount_amount": line_discount,
                "line_total": line_total
            }

            order_items.append(order_item)
            current_order_items.append(order_item)
            order_item_id += 1

        if promotion and promotion["minimum_order_value"] and subtotal < promotion["minimum_order_value"]:
            promotion_id = None
            discount_amount = 0

            for item in current_order_items:
                item["discount_amount"] = 0
                item["line_total"] = item["unit_price"] * item["quantity"]

        tax_amount = round((subtotal - discount_amount) * 0.05, 2)

        if channel == "Third-Party Delivery":
            delivery_fee = random.choice([100, 150, 200, 250])
        elif channel == "Website/App":
            delivery_fee = random.choice([0, 100, 150])
        else:
            delivery_fee = 0

        total_amount = round(subtotal - discount_amount + tax_amount + delivery_fee, 2)

        if order_status == "Cancelled":
            total_amount = 0

        orders.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "restaurant_id": restaurant_id,
            "promotion_id": int(promotion_id) if promotion_id is not None else None,
            "order_datetime": order_datetime,
            "ordering_channel": channel,
            "order_status": order_status,
            "subtotal": round(subtotal, 2),
            "discount_amount": round(discount_amount, 2),
            "tax_amount": tax_amount,
            "delivery_fee": delivery_fee,
            "total_amount": total_amount,
            "payment_method": random.choice(["Cash", "Card", "Digital Wallet"])
        })

    return orders, order_items

def generate_wastage(inventories, n=50000):
    wastage = []
    wastage_id = 1

    remaining_stock = {inventory["inventory_id"]: inventory["quantity_remaining"] for inventory in inventories}
    valid_inventories = [inventory for inventory in inventories if inventory["quantity_remaining"] > 0]

    while len(wastage) < n:
        inventory = random.choice(valid_inventories)
        available_quantity = remaining_stock[inventory["inventory_id"]]

        if available_quantity <= 0:
            continue

        quantity = random.randint(1, min(available_quantity, 5))
        reason = random.choices(["Expired", "Spoiled", "Overproduction", "Damaged"],weights=[60, 20, 15, 5])[0]

        if reason == "Expired":
            wastage_date = inventory["expiry_date"]
        else:
            wastage_date = fake.date_between(start_date=inventory["received_date"], end_date=min(inventory["expiry_date"], DATASET_END))

        unit_cost = inventory["unit_cost"]
        cost = round(quantity * unit_cost, 2)

        wastage.append({
            "wastage_id": wastage_id,
            "restaurant_id": inventory["restaurant_id"],
            "item_id": inventory["item_id"],
            "inventory_id": inventory["inventory_id"],
            "wastage_date": wastage_date,
            "quantity": quantity,
            "unit_cost": unit_cost,
            "cost": cost,
            "reason": reason
        })

        remaining_stock[inventory["inventory_id"]] -= quantity
        wastage_id += 1

    return wastage

def generate_ratings(n, orders, order_items):
    ratings = []
    completed_orders = [order for order in orders if order["order_status"] == "Completed"]
    order_lookup = {order["order_id"]: order for order in completed_orders}
    items_by_order = {}

    for item in order_items:
        if item["order_id"] in order_lookup:
            items_by_order.setdefault(item["order_id"], []).append(item)

    for i in range(1, n + 1):
        order = random.choice(completed_orders)
        item = random.choice(items_by_order[order["order_id"]])

        rating = random.choices([1, 2, 3, 4, 5],weights=[4, 6, 15, 35, 40])[0]

        days_after = random.randint(0, 7)
        rating_date = order["order_datetime"].date() + timedelta(days=days_after)
        rating_date = min(rating_date, DATASET_END)

        ratings.append({
            "rating_id": i,
            "customer_id": order["customer_id"],
            "order_id": order["order_id"],
            "restaurant_id": order["restaurant_id"],
            "item_id": item["item_id"],
            "rating": rating,
            "rating_date": rating_date
        })

    return ratings

os.makedirs(output_folder, exist_ok=True)

print("generating customers...")
customers = generate_customers(50000)

print("generating restaurants...")
restaurants = generate_restaurant(20)

print("generating menu categories...")
menu_categories = generate_menu_categories()

print("generating menu items...")
menu_items = generate_menu_items(150)

print("generating restaurant menu items...")
restaurant_menu_items = generate_restaurant_menu_items(restaurants, menu_items)

print("generating pricing history...")
pricing_history = generate_pricing_history(restaurant_menu_items, menu_items, restaurants)

print("generating inventory...")
inventories = generate_inventory(restaurant_menu_items, menu_items)

print("generating promotions...")
promotions = generate_promotion(100, restaurants, restaurant_menu_items, inventories)

print("generating orders and order items...")
orders, order_items = generate_orders(
    100000,
    customers,
    restaurants,
    restaurant_menu_items,
    menu_items,
    pricing_history,
    promotions
)

print("generating wastage...")
wastage = generate_wastage(inventories)

print("generating ratings...")
ratings = generate_ratings(100000, orders, order_items)

print("Saving csv files")

pd.DataFrame(customers).to_csv(f"{output_folder}/customers.csv", index=False)
pd.DataFrame(restaurants).to_csv(f"{output_folder}/restaurants.csv", index=False)
pd.DataFrame(menu_categories).to_csv(f"{output_folder}/menu_categories.csv", index=False)
pd.DataFrame(menu_items).to_csv(f"{output_folder}/menu_items.csv", index=False)
pd.DataFrame(restaurant_menu_items).to_csv(f"{output_folder}/restaurant_menu_items.csv", index=False)
pd.DataFrame(pricing_history).to_csv(f"{output_folder}/pricing_history.csv", index=False)
pd.DataFrame(inventories).to_csv(f"{output_folder}/inventory.csv", index=False)
pd.DataFrame(promotions).to_csv(f"{output_folder}/promotions.csv", index=False)
orders_df = pd.DataFrame(orders)
orders_df["promotion_id"] = orders_df["promotion_id"].astype("Int64")
orders_df.to_csv(f"{output_folder}/orders.csv", index=False)
pd.DataFrame(order_items).to_csv(f"{output_folder}/order_items.csv", index=False)
pd.DataFrame(wastage).to_csv(f"{output_folder}/wastage.csv", index=False)
pd.DataFrame(ratings).to_csv(f"{output_folder}/ratings.csv", index=False)

print("\nDATA GENERATION COMPLETE")
print("------------------------")
print(f"Customers:             {len(customers):,}")
print(f"Restaurants:           {len(restaurants):,}")
print(f"Menu Categories:       {len(menu_categories):,}")
print(f"Menu Items:            {len(menu_items):,}")
print(f"Restaurant Menu Items: {len(restaurant_menu_items):,}")
print(f"Pricing History:       {len(pricing_history):,}")
print(f"Inventory:             {len(inventories):,}")
print(f"Promotions:            {len(promotions):,}")
print(f"Orders:                {len(orders):,}")
print(f"Order Items:           {len(order_items):,}")
print(f"Wastage:               {len(wastage):,}")
print(f"Ratings:               {len(ratings):,}")
print("------------------------")
print(f"Saved to: {output_folder}/")
