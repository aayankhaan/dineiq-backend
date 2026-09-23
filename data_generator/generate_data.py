import numpy as np
import pandas as pd
from faker import Faker
from datetime import date, timedelta
from faker_pk import FakerPKProvider
import random

np.random.seed(42)
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
            "signup_date": fake.date_between(start_date="-4y", end_date="today"),
            "birth_year": fake.date_of_birth(minimum_age=16, maximum_age=60).year,
            "gender": random.choice(["M","F"]),
            "city": random.choice(list(city_areas.keys()))
        })
    return pd.DataFrame(customers)

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
            "opening_date": fake.date_between(start_date="-14y", end_date="today"),
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
            "id": i,
            "cat_id": cat_id,
            "name": generate_menu_item_name(cat_id),
            "base_price": base_price,
            "base_cost": base_cost,
            "prep_time_minutes": generate_prep_time(cat_id),
            "introduced_date": fake.date_between(start_date="-4y", end_date="today"),
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
                    "item_id": item["id"],
                    "price": restaurant_price,
                    "is_available": True
                })

                restaurant_menu_id += 1

    return restaurant_menu

def generate_pricing_history(restaurant_menu_items, menu_items):
    pricing_history = []
    price_history_id = 1
    item_lookup = {item["id"]: item for item in menu_items}

    for restaurant_item in restaurant_menu_items:

        item_id = restaurant_item["item_id"]
        restaurant_id = restaurant_item["restaurant_id"]
        current_price = restaurant_item["price"]

        item = item_lookup[item_id]
        introduced_date = item["introduced_date"]

        history_start = max(DATASET_START, introduced_date)

        if history_start >= DATASET_END - timedelta(days=30):
            continue

        num_changes = random.choices([0, 1, 2, 3],weights=[45, 35, 15, 5],k=1)[0]

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
