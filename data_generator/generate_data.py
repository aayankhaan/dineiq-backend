import os
import pandas as pd
from faker import Faker
from datetime import date, timedelta
from faker_pk import FakerPKProvider
import random
from config.settings import RAW_DATA_FOLDER, DATASET_END, DATASET_START

random.seed(42)
Faker.seed(42)

fake = Faker()
fake.add_provider(FakerPKProvider)

output_folder = RAW_DATA_FOLDER


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
ingredient_pool = {
    "Chicken": "kg",
    "Beef": "kg",
    "Mutton": "kg",
    "Prawns": "kg",
    "Sausage": "kg",
    "Potatoes": "kg",
    "Onions": "kg",
    "Tomatoes": "kg",
    "Bell Peppers": "kg",
    "Green Chillies": "kg",
    "Lettuce": "kg",
    "Spinach": "kg",
    "Okra": "kg",
    "Mixed Vegetables": "kg",
    "Chickpeas": "kg",
    "Rice": "kg",
    "Flour": "kg",
    "Pasta": "kg",
    "Noodles": "kg",
    "Lentils": "kg",
    "Burger Buns": "pieces",
    "Tortillas": "pieces",
    "Parathas": "pieces",
    "Pizza Dough": "pieces",
    "Spring Roll Wrappers": "pieces",
    "Eggs": "pieces",
    "Cheese": "kg",
    "Paneer": "kg",
    "Milk": "liters",
    "Cream": "liters",
    "Yogurt": "kg",
    "Butter": "kg",
    "Cooking Oil": "liters",
    "Tomato Sauce": "liters",
    "Burger Sauce": "liters",
    "Soy Sauce": "liters",
    "Hot Sauce": "liters",
    "BBQ Sauce": "liters",
    "Mayonnaise": "liters",
    "Salt": "kg",
    "Sugar": "kg",
    "Black Pepper": "kg",
    "Red Chilli Powder": "kg",
    "Garam Masala": "kg",
    "Biryani Masala": "kg",
    "Tikka Masala": "kg",
    "Chaat Masala": "kg",
    "Ginger Garlic Paste": "kg",
    "Tea Leaves": "kg",
    "Coffee": "kg",
    "Chocolate": "kg",
    "Ice Cream": "liters",
    "Fruit Pulp": "liters",
    "Lemon/Lime": "kg",
    "Soda Water": "liters"
}
ingredient_unit_cost = {"Chicken": 650, "Beef": 900, "Mutton": 1500, "Prawns": 1200, "Sausage": 850, "Potatoes": 120, "Onions": 140, "Tomatoes": 180, "Bell Peppers": 450, "Green Chillies": 300, "Lettuce": 220, "Spinach": 160, "Okra": 240, "Mixed Vegetables": 260, "Chickpeas": 300, "Rice": 320, "Flour": 180, "Pasta": 450, "Noodles": 400, "Lentils": 350, "Burger Buns": 55, "Tortillas": 45, "Parathas": 60, "Pizza Dough": 180, "Spring Roll Wrappers": 25, "Eggs": 35, "Cheese": 1600, "Paneer": 1100, "Milk": 300, "Cream": 750, "Yogurt": 280, "Butter": 1400, "Cooking Oil": 550, "Tomato Sauce": 500, "Burger Sauce": 650, "Soy Sauce": 700, "Hot Sauce": 650, "BBQ Sauce": 750, "Mayonnaise": 700, "Salt": 80, "Sugar": 170, "Black Pepper": 1400, "Red Chilli Powder": 900, "Garam Masala": 1200, "Biryani Masala": 1300, "Tikka Masala": 1250, "Chaat Masala": 1000, "Ginger Garlic Paste": 500, "Tea Leaves": 1800, "Coffee": 3500, "Chocolate": 1800, "Ice Cream": 700, "Fruit Pulp": 650, "Lemon/Lime": 350, "Soda Water": 120}
common_ingredient_map = {
    7: ["Pizza Dough", "Tomato Sauce", "Cheese"],
}
type_ingredient_map = {
    1: {
        "Fries": ["Potatoes", "Cooking Oil", "Salt"],
        "Wings": ["Chicken", "Flour", "Cooking Oil"],
        "Nuggets": ["Chicken", "Flour", "Cooking Oil"],
        "Cheese Sticks": ["Cheese", "Flour", "Cooking Oil"],
        "Samosa Bites": ["Potatoes", "Flour", "Cooking Oil"],
        "Pakora": ["Onions", "Flour", "Cooking Oil"],
        "Chicken Rolls": ["Chicken", "Flour", "Cooking Oil"],
        "Spring Rolls": ["Mixed Vegetables", "Spring Roll Wrappers", "Cooking Oil"],
    },
    2: {
        "Chicken Seekh": ["Chicken", "Ginger Garlic Paste", "Garam Masala"],
        "Beef Boti": ["Beef", "Ginger Garlic Paste", "Garam Masala"],
        "Kebab": ["Beef", "Ginger Garlic Paste", "Garam Masala"],
        "Chops": ["Mutton", "Ginger Garlic Paste", "Garam Masala"],
        "Sajji": ["Chicken", "Salt", "Garam Masala"],
        "Tikka Leg Piece": ["Chicken", "Tikka Masala", "Yogurt"],
        "Malai Boti": ["Chicken", "Cream", "Yogurt"],
        "Chicken Chargha": ["Chicken", "Garam Masala", "Cooking Oil"],
    },
    3: {
        "Beef": ["Beef", "Burger Buns", "Burger Sauce"],
        "Chicken": ["Chicken", "Burger Buns", "Burger Sauce"],
        "Zinger": ["Chicken", "Flour", "Burger Buns"],
        "Club": ["Chicken", "Burger Buns", "Mayonnaise"],
        "Steak": ["Beef", "Burger Buns", "BBQ Sauce"],
        "Bihari Boti": ["Beef", "Ginger Garlic Paste", "Tortillas"],
    },
    4: {
        "Chicken": ["Chicken", "Tomatoes", "Cooking Oil"],
        "Mutton": ["Mutton", "Tomatoes", "Cooking Oil"],
        "Paneer": ["Paneer", "Tomatoes", "Cooking Oil"],
        "Beef": ["Beef", "Tomatoes", "Cooking Oil"],
    },
    5: {
        "Chicken": ["Chicken"],
        "Beef": ["Beef"],
        "Mutton": ["Mutton"],
        "Egg Fried": ["Eggs"],
        "Prawn": ["Prawns"],
    },
    6: {
        "Chicken": ["Chicken"],
        "Prawn": ["Prawns"],
        "Vegetable": ["Mixed Vegetables"],
        "Beef": ["Beef"],
    },
    8: {
        "Kheer": ["Milk", "Rice", "Sugar"],
        "Gulab Jamun": ["Milk", "Flour", "Sugar"],
        "Brownie": ["Chocolate", "Flour", "Sugar"],
        "Ice Cream": ["Ice Cream", "Milk"],
        "Molten Lava Cake": ["Chocolate", "Flour", "Sugar"],
        "Ras Malai": ["Milk", "Paneer", "Sugar"],
        "Kulfi": ["Milk", "Sugar", "Cream"],
    },
    10: {
        "Lemonade": ["Lemon/Lime", "Sugar", "Soda Water"],
        "Margarita": ["Lemon/Lime", "Sugar"],
        "Shake": ["Milk", "Ice Cream", "Sugar"],
        "Lassi": ["Yogurt", "Sugar"],
        "Soda": ["Soda Water"],
        "Sharbat": ["Fruit Pulp", "Sugar"],
    },
    12: {
        "Chana Masala": ["Chickpeas", "Tomatoes", "Garam Masala"],
        "Daal Mash": ["Lentils", "Onions"],
        "Daal Chana": ["Lentils", "Chickpeas"],
        "Raita": ["Yogurt"],
        "Salad": ["Tomatoes", "Onions", "Lettuce"],
        "Chutney": ["Tomatoes", "Green Chillies"],
    },
}
style_ingredient_map = {
    1: {
        "Garlic": ["Ginger Garlic Paste"],
        "Spicy": ["Red Chilli Powder"],
        "Crispy": ["Flour"],
        "Loaded": ["Cheese"],
        "Honey Mustard": ["Mayonnaise"],
        "Cheesy": ["Cheese"],
        "Chatpata": ["Chaat Masala"],
        "Masala": ["Garam Masala"],
        "Peri Peri": ["Hot Sauce"],
        "Tandoori": ["Tikka Masala"],
    },
    2: {
        "Reshmi": ["Cream"],
        "Tikka": ["Tikka Masala"],
        "Behari": ["Chaat Masala"],
        "Malai": ["Cream"],
        "Kasturi": ["Cheese"],
        "Achari": ["Red Chilli Powder"],
        "Chargha Style": ["Cooking Oil"],
        "Peshawari": ["Garam Masala"],
        "Angara": ["Red Chilli Powder"],
        "Bihari": ["Yogurt"],
    },
    3: {
        "Classic": ["Mayonnaise"],
        "Crispy": ["Flour"],
        "Spicy": ["Hot Sauce"],
        "Smoky": ["BBQ Sauce"],
        "Double": ["Cheese"],
        "Loaded": ["Cheese"],
        "Grilled": ["Cooking Oil"],
        "Zafrani": ["Cream"],
        "Desi Style": ["Chaat Masala"],
    },
    4: {
        "Peshawari": ["Garam Masala"],
        "Makhni": ["Butter", "Cream"],
        "Achari": ["Red Chilli Powder"],
        "Desi White": ["Yogurt"],
        "Koyla": ["Butter"],
        "Shinwari": ["Black Pepper"],
        "Lahori": ["Garam Masala"],
        "Green Chilli": ["Green Chillies"],
        "Dhaba Style": ["Ginger Garlic Paste"],
        "Kala Namak": ["Salt"],
    },
    5: {
        "Sindhi": ["Yogurt"],
        "Bombay": ["Red Chilli Powder"],
        "Special VIP": ["Cream"],
        "Nawabi": ["Cream"],
        "Kachay Gosht Ki": ["Yogurt"],
        "Hyderabadi": ["Yogurt"],
        "Karachi Style": ["Red Chilli Powder"],
        "Dum": ["Cooking Oil"],
    },
    6: {
        "Creamy Alfredo": ["Cream", "Cheese"],
        "Spicy Arrabbiata": ["Tomato Sauce", "Red Chilli Powder"],
        "Cheesy Baked": ["Cheese"],
        "Chilli Garlic": ["Ginger Garlic Paste", "Hot Sauce"],
        "Peri Peri": ["Hot Sauce"],
        "Chinese Style": ["Soy Sauce"],
        "White Sauce": ["Cream", "Butter"],
        "Schezwan": ["Soy Sauce", "Red Chilli Powder"],
    },
    7: {
        "Chicken Tikka": ["Chicken", "Tikka Masala"],
        "Fajita": ["Chicken", "Bell Peppers"],
        "Mughlai": ["Chicken", "Cream"],
        "Pepperoni": ["Sausage"],
        "Veggie": ["Mixed Vegetables"],
        "Malai Boti": ["Chicken", "Cream"],
        "BBQ Chicken": ["Chicken", "BBQ Sauce"],
        "Tandoori": ["Chicken", "Tikka Masala"],
        "Cheese": ["Cheese"],
        "Achari Chicken": ["Chicken", "Red Chilli Powder"],
        "Behari Boti": ["Beef", "Ginger Garlic Paste"],
        "Spicy Sausage": ["Sausage", "Red Chilli Powder"],
        "Hawaiian": ["Cheese"],
        "Loaded Meat": ["Beef", "Sausage", "Chicken"],
        "Karahi": ["Chicken", "Tomatoes"],
    },
    8: {
        "Shahi": ["Cream"],
        "Warm": ["Butter"],
        "Chilled": ["Milk"],
        "Chocolate": ["Chocolate"],
        "Lotus Biscoff": ["Sugar"],
        "Kesar Pista": ["Cream"],
        "Caramel": ["Sugar"],
        "Rabri": ["Milk", "Cream"],
    },
    9: {
        "Karak": ["Milk"],
        "Doodh Patti": ["Milk"],
        "Kashmiri": ["Milk"],
        "Green": ["Tea Leaves"],
        "Espresso": ["Coffee"],
        "Cappuccino": ["Coffee", "Milk"],
        "Adrak": ["Tea Leaves"],
        "Elaichi": ["Tea Leaves"],
        "Peshawari Qehwa": ["Tea Leaves"],
    },
    10: {
        "Mint": ["Sugar"],
        "Classic": ["Sugar"],
        "Mango": ["Fruit Pulp"],
        "Blueberry": ["Fruit Pulp"],
        "Chocolate": ["Chocolate"],
        "Fresh Lime": ["Lemon/Lime"],
        "Rose": ["Sugar"],
        "Strawberry": ["Fruit Pulp"],
    },
    11: {
        "Chicken": ["Chicken"],
        "Beef": ["Beef"],
        "Seekh Kebab": ["Beef", "Ginger Garlic Paste", "Garam Masala"],
        "Aloo": ["Potatoes"],
        "Keema": ["Beef", "Ginger Garlic Paste"],
        "Malai Boti": ["Chicken", "Cream"],
        "Spicy Chicken": ["Chicken", "Red Chilli Powder"],
        "Achari": ["Chicken", "Red Chilli Powder"],
    },
    12: {
        "Aloo": ["Potatoes"],
        "Chana": ["Chickpeas"],
        "Mix Veg": ["Mixed Vegetables"],
        "Daal": ["Lentils"],
        "Bhindi": ["Okra"],
        "Palak": ["Spinach"],
        "Raita": ["Yogurt"],
    },
}
ending_ingredient_map = {
    3: {
        "Burger": [],
        "Cheese Burger": ["Cheese"],
        "Sandwich": [],
        "Wrap": ["Tortillas"],
    },
    5: {
        "Biryani": ["Rice", "Biryani Masala"],
        "Pulao": ["Rice", "Garam Masala"],
        "Rice": ["Rice"],
        "Tehari": ["Rice", "Garam Masala"],
    },
    6: {
        "Pasta": ["Pasta"],
        "Noodles": ["Noodles"],
        "Spaghetti": ["Pasta"],
        "Chowmein": ["Noodles"],
    },
    9: {
        "Chai": ["Tea Leaves", "Milk", "Sugar"],
        "Tea": ["Tea Leaves", "Sugar"],
        "Coffee": ["Coffee", "Milk", "Sugar"],
        "Qehwa": ["Tea Leaves", "Sugar"],
    },
    11: {
        "Roll": ["Parathas", "Cooking Oil"],
        "Paratha": ["Parathas", "Cooking Oil"],
        "Wrap": ["Tortillas"],
        "Frankie": ["Tortillas", "Cooking Oil"],
    },
}

def generate_prep_time(cat_id):
    low, high = category_prep_time.get(cat_id, (5, 20))
    return random.randint(low, high)

def generate_price(cat_id):
    low, high = category_price_ranges.get(cat_id)
    base_price = round(random.randint(low, high), -1)
    return base_price

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
        style = random.choice(components.get("styles", [""]))
        item_type = random.choice(components.get("types", [""]))
        ending = random.choice(components.get("endings", [""]))

        name_parts = [part for part in [style, item_type, ending] if part]
        item_name = " ".join(name_parts)

        if item_name not in used_menu_item_names:
            used_menu_item_names.add(item_name)
            return item_name, style, item_type, ending
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
            "restaurant_id": i,
            "name": generate_restaurant_name(city, area),
            "city": city,
            "area": area,
            "opening_date": fake.date_between(start_date=DATASET_END - timedelta(days=5110), end_date=DATASET_END),
            "status": "Active"
        })
    return restaurants

def generate_ingredient(ingredient_list=ingredient_pool):
    ingredients = []
    for index, (name, unit) in enumerate(ingredient_list.items(), start=1):
        ingredients.append({
            "ingredient_id": index,
            "ingredient": name,
            "unit": unit,
            "unit_cost": ingredient_unit_cost[name]
        })
    return ingredients

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
    components_list = []
    categories = generate_menu_categories()
    cat_ids = [c["category_id"] for c in categories]

    for i in range(1, n + 1):
        cat_id = cat_ids[(i - 1) % len(cat_ids)]
        base_price = generate_price(cat_id)
        name, style, item_type, ending = generate_menu_item_name(cat_id)

        items.append({
            "menu_items_id": i,
            "cat_id": cat_id,
            "name": name,
            "base_price": base_price,
            "prep_time_minutes": generate_prep_time(cat_id),
            "introduced_date": fake.date_between(start_date=DATASET_END - timedelta(days=1460), end_date=DATASET_END),
            "discontinued_date": None
        })

        components_list.append({
            "menu_items_id": i,
            "cat_id": cat_id,
            "style": style,
            "type": item_type,
            "ending": ending
        })

    return items, components_list

def random_quantity(ingredient_name, unit):

    meats = ["Chicken", "Beef", "Mutton", "Prawns", "Sausage"]
    vegetables = ["Potatoes", "Onions", "Tomatoes", "Bell Peppers", "Green Chillies", "Lettuce", "Spinach", "Okra", "Mixed Vegetables", "Chickpeas"]
    grains = ["Rice", "Flour", "Pasta", "Noodles", "Lentils"]
    dairy_kg = ["Cheese", "Paneer", "Yogurt", "Butter"]
    spices = ["Salt", "Black Pepper", "Red Chilli Powder", "Garam Masala", "Biryani Masala", "Tikka Masala", "Chaat Masala"]
    sauces = ["Tomato Sauce", "Burger Sauce", "Soy Sauce", "Hot Sauce", "BBQ Sauce", "Mayonnaise"]
    liquids = ["Milk", "Cream", "Cooking Oil", "Ice Cream", "Fruit Pulp", "Soda Water"]
    pieces = ["Burger Buns", "Tortillas", "Parathas", "Pizza Dough", "Spring Roll Wrappers", "Eggs"]

    if ingredient_name in meats:
        return round(random.uniform(0.10, 0.30), 3)

    elif ingredient_name in vegetables:
        return round(random.uniform(0.03, 0.20), 3)

    elif ingredient_name in grains:
        return round(random.uniform(0.05, 0.25), 3)

    elif ingredient_name in dairy_kg:
        return round(random.uniform(0.02, 0.15), 3)

    elif ingredient_name in spices:
        return round(random.uniform(0.002, 0.015), 3)

    elif ingredient_name == "Ginger Garlic Paste":
        return round(random.uniform(0.005, 0.030), 3)

    elif ingredient_name == "Tea Leaves":
        return round(random.uniform(0.003, 0.010), 3)

    elif ingredient_name == "Coffee":
        return round(random.uniform(0.005, 0.020), 3)

    elif ingredient_name == "Sugar":
        return round(random.uniform(0.010, 0.040), 3)

    elif ingredient_name == "Chocolate":
        return round(random.uniform(0.02, 0.08), 3)

    elif ingredient_name == "Lemon/Lime":
        return round(random.uniform(0.03, 0.10), 3)

    elif ingredient_name in sauces:
        return round(random.uniform(0.01, 0.05), 3)

    elif ingredient_name in liquids:
        return round(random.uniform(0.02, 0.30), 3)

    elif ingredient_name in pieces:
        return 1.0

    if unit == "kg":
        return round(random.uniform(0.02, 0.15), 3)
    elif unit == "liters":
        return round(random.uniform(0.02, 0.20), 3)
    elif unit == "pieces":
        return 1.0
    
def generate_menu_item_ingredients(components_list, ingredient_pool):
    """Builds the menu_item -> ingredient BOM table using the
    common/type/style/ending ingredient maps above."""
    ingredient_lookup = {name: idx for idx, name in enumerate(ingredient_pool.keys(), start=1)}
    menu_item_ingredients = []
    row_id = 1
    unmapped_items = []

    for comp in components_list:
        cat_id = comp["cat_id"]
        style = comp["style"]
        item_type = comp["type"]
        ending = comp["ending"]

        needed = set()
        needed.update(common_ingredient_map.get(cat_id, []))
        needed.update(type_ingredient_map.get(cat_id, {}).get(item_type, []))
        needed.update(style_ingredient_map.get(cat_id, {}).get(style, []))
        needed.update(ending_ingredient_map.get(cat_id, {}).get(ending, []))

        if not needed:
            unmapped_items.append(comp)
            continue

        for ingredient_name in needed:
            unit = ingredient_pool[ingredient_name]
            menu_item_ingredients.append({
                "menu_item_ingredient_id": row_id,
                "menu_item_id": comp["menu_items_id"],
                "ingredient_id": ingredient_lookup[ingredient_name],
                "quantity_required": random_quantity(ingredient_name, unit)
            })
            row_id += 1

    if unmapped_items:
        print(f"WARNING: {len(unmapped_items)} menu items had no ingredient mapping match:")
        for item in unmapped_items:
            print(f"  menu_items_id={item['menu_items_id']} cat_id={item['cat_id']} "
                  f"style='{item['style']}' type='{item['type']}' ending='{item['ending']}'")

    return menu_item_ingredients

def generate_restaurant_menu_items(restaurants, menu_items):
    restaurant_menu = []
    restaurant_menu_id = 1

    for restaurant in restaurants:
        for item in menu_items:

            if random.random() < 0.80:
                restaurant_price = generate_restaurant_price(item["base_price"])

                restaurant_menu.append({
                    "restaurant_menu_id": restaurant_menu_id,
                    "restaurant_id": restaurant["restaurant_id"],
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
    restaurant_lookup = {restaurant["restaurant_id"]: restaurant for restaurant in restaurants}

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

def generate_promotion(n, restaurants, restaurant_menu_items, inventories, menu_item_ingredients):
    promotions = []

    ingredient_items = {}
    for recipe in menu_item_ingredients:
        ingredient_id = recipe["ingredient_id"]
        item_id = recipe["menu_item_id"]
        ingredient_items.setdefault(ingredient_id, []).append(item_id)

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
                ingredient_id = inventory["ingredient_id"]
                possible_item_ids = ingredient_items.get(ingredient_id, [])

                available_items = [
                    item for item in restaurant_menu_items
                    if item["restaurant_id"] == restaurant_id
                    and item["item_id"] in possible_item_ids
                    and item["is_available"]
                ]

                if available_items:
                    selected_item = random.choice(available_items)
                    item_id = selected_item["item_id"]
                    item_price = selected_item["price"]

                    days_before_expiry = random.randint(2, 5)
                    start_date = max(
                        inventory["received_date"],
                        inventory["expiry_date"] - timedelta(days=days_before_expiry)
                    )
                    end_date = inventory["expiry_date"]

                    name = random.choice(["Stock Clearance Deal","Fresh Stock Special","Limited Time Deal","Chef's Special"])
                else:
                    inventory_driven = False
            else:
                inventory_driven = False

        if not inventory_driven:
            restaurant = random.choice(restaurants)
            restaurant_id = restaurant["restaurant_id"]
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

def generate_inventory(restaurant_menu_items, menu_item_ingredients, ingredients, menu_items):
    inventories = []
    inventory_id = 1
    ingredient_lookup = {ingredient["ingredient_id"]: ingredient for ingredient in ingredients}
    recipes_by_item = {}
    item_lookup = {item["menu_items_id"]: item for item in menu_items}

    for recipe in menu_item_ingredients:
        recipes_by_item.setdefault(recipe["menu_item_id"], set()).add(recipe["ingredient_id"])

    restaurant_ingredients = {}
    for restaurant_item in restaurant_menu_items:
        restaurant_id = restaurant_item["restaurant_id"]
        item_id = restaurant_item["item_id"]
        for ingredient_id in recipes_by_item.get(item_id, []):
            restaurant_ingredients.setdefault(restaurant_id, set()).add(ingredient_id)

    for restaurant_id, ingredient_ids in restaurant_ingredients.items():
        for ingredient_id in ingredient_ids:
            ingredient = ingredient_lookup[ingredient_id]
            related_items = [item_lookup[item["item_id"]] for item in restaurant_menu_items if item["restaurant_id"] == restaurant_id and ingredient_id in recipes_by_item.get(item["item_id"], set())]
            inventory_start = max(DATASET_START, min(item["introduced_date"] for item in related_items))
            if inventory_start >= DATASET_END:
                continue

            available_days = (DATASET_END - inventory_start).days
            num_batches = max(1, available_days // random.randint(20, 35))

            for _ in range(num_batches):
                received_date = inventory_start + timedelta(days=random.randint(0, max(0, available_days - 14)))
                if ingredient["unit"] == "pieces":
                    quantity_received = random.randint(40, 250)
                    quantity_remaining = random.randint(0, quantity_received)
                else:
                    quantity_received = round(random.uniform(10, 80), 2)
                    quantity_remaining = round(random.uniform(0, quantity_received), 2)
                unit_cost = round(ingredient["unit_cost"] * random.uniform(0.90, 1.10), 2)
                expiry_date = min(received_date + timedelta(days=random.randint(3, 30)), DATASET_END)

                inventories.append({
                    "inventory_id": inventory_id,
                    "restaurant_id": restaurant_id,
                    "ingredient_id": ingredient_id,
                    "quantity_received": quantity_received,
                    "quantity_remaining": quantity_remaining,
                    "unit": ingredient["unit"],
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
    customer_profiles = {}
    restaurant_demand_weights = {}
    item_popularity = {}

    for restaurant in restaurants:
        restaurants_by_city.setdefault(restaurant["city"], []).append(restaurant)
        restaurant_demand_weights[restaurant["restaurant_id"]] = random.uniform(0.75, 1.35)

    for item in restaurant_menu_items:
        if item["is_available"]:
            menu_by_restaurant.setdefault(item["restaurant_id"], []).append(item)
        price_lookup[(item["restaurant_id"], item["item_id"])] = item["price"]
        item_popularity[(item["restaurant_id"], item["item_id"])] = random.uniform(0.55, 1.45)

    for promotion in promotions:
        promotions_by_restaurant.setdefault(promotion["restaurant_id"], []).append(promotion)

    for change in pricing_history:
        pricing_lookup.setdefault((change["restaurant_id"], change["item_id"]), []).append(change)

    for changes in pricing_lookup.values():
        changes.sort(key=lambda x: x["effective_date"])

    for customer in customers:
        customer_profiles[customer["customer_id"]] = {
            "frequency_weight": random.choices([0.40, 0.80, 1.20, 2.00, 3.00], weights=[12, 28, 32, 20, 8])[0],
            "promotion_sensitivity": random.uniform(0.05, 0.90),
            "preferred_channel": random.choices(
                ["Dine-in", "Takeaway", "Website/App", "Third-Party Delivery"],
                weights=[38, 18, 24, 20]
            )[0],
            "preferred_categories": random.sample(range(1, 13), random.choice([2, 3, 3, 4])),
            "preferred_period": random.choices(["Lunch", "Dinner", "Late Night"], weights=[35, 55, 10])[0]
        }

    eligible_customers = [
        customer for customer in customers
        if max(DATASET_START, customer["signup_date"]) < DATASET_END
    ]
    customer_weights = [
        customer_profiles[customer["customer_id"]]["frequency_weight"]
        for customer in eligible_customers
    ]

    category_affinities = {
        1: [3, 7, 10],
        2: [5, 10, 12],
        3: [1, 10],
        4: [10, 12],
        5: [10, 12],
        6: [1, 10],
        7: [1, 10],
        8: [9, 10],
        9: [1, 8],
        10: [1, 2, 3, 4, 5, 6, 7, 11],
        11: [1, 10, 12],
        12: [2, 4, 5, 11]
    }

    def date_demand_weight(order_date):
        weight = 1.0
        weekday = order_date.weekday()
        month = order_date.month

        if weekday == 4:
            weight *= 1.18
        elif weekday == 5:
            weight *= 1.30
        elif weekday == 6:
            weight *= 1.22
        elif weekday == 0:
            weight *= 0.88

        if month in [6, 7, 8]:
            weight *= 1.08
        elif month in [11, 12, 1]:
            weight *= 1.12
        elif month in [2, 3]:
            weight *= 0.94

        dataset_days = max(1, (DATASET_END - DATASET_START).days)
        progress = max(0, (order_date - DATASET_START).days) / dataset_days
        return weight * (0.92 + 0.16 * progress)

    def choose_order_date(earliest_date):
        available_days = (DATASET_END - earliest_date).days

        for _ in range(20):
            candidate = earliest_date + timedelta(days=random.randint(0, available_days))
            if random.random() < min(1.0, date_demand_weight(candidate) / 1.45):
                return candidate

        return earliest_date + timedelta(days=random.randint(0, available_days))

    def choose_hour(preferred_period, order_date):
        if preferred_period == "Lunch":
            periods, weights = ["Lunch", "Dinner", "Late Night"], [65, 30, 5]
        elif preferred_period == "Dinner":
            periods, weights = ["Lunch", "Dinner", "Late Night"], [20, 70, 10]
        else:
            periods, weights = ["Lunch", "Dinner", "Late Night"], [15, 50, 35]

        if order_date.weekday() in [4, 5, 6]:
            weights[1] += 8
            weights[2] += 7

        period = random.choices(periods, weights=weights)[0]

        if period == "Lunch":
            return random.choices([11, 12, 13, 14, 15], weights=[8, 24, 32, 25, 11])[0]
        if period == "Dinner":
            return random.choices([17, 18, 19, 20, 21, 22], weights=[5, 12, 23, 27, 22, 11])[0]
        return random.choices([22, 23], weights=[55, 45])[0]

    def choose_channel(preferred_channel, hour, order_date):
        if 12 <= hour <= 15:
            weights = {"Dine-in": 42, "Takeaway": 27, "Website/App": 17, "Third-Party Delivery": 14}
        elif 18 <= hour <= 22:
            weights = {"Dine-in": 36, "Takeaway": 14, "Website/App": 24, "Third-Party Delivery": 26}
        else:
            weights = {"Dine-in": 18, "Takeaway": 15, "Website/App": 28, "Third-Party Delivery": 39}

        if order_date.weekday() in [4, 5, 6]:
            weights["Dine-in"] += 5
            weights["Third-Party Delivery"] += 5

        weights[preferred_channel] += 20
        channels = list(weights.keys())
        return random.choices(channels, weights=list(weights.values()))[0]

    def current_price(restaurant_id, item_id, order_date):
        unit_price = price_lookup[(restaurant_id, item_id)]
        changes = pricing_lookup.get((restaurant_id, item_id), [])

        for change in changes:
            if order_date < change["effective_date"]:
                return change["old_price"]
            unit_price = change["new_price"]

        return unit_price

    def weighted_item(candidates, restaurant_id, order_date, preferred_categories):
        weights = []

        for candidate in candidates:
            item_id = candidate["item_id"]
            menu_item = item_lookup[item_id]
            category_id = menu_item["cat_id"]
            weight = item_popularity[(restaurant_id, item_id)]

            if category_id in preferred_categories:
                weight *= 1.65

            price = current_price(restaurant_id, item_id, order_date)
            price_ratio = price / max(1, menu_item["base_price"])
            if price_ratio > 1.0:
                weight *= max(0.45, 1.0 - ((price_ratio - 1.0) * 1.8))

            if category_id == 10 and order_date.month in [5, 6, 7, 8, 9]:
                weight *= 1.35
            elif category_id == 9 and order_date.month in [11, 12, 1, 2]:
                weight *= 1.45
            elif category_id in [4, 5, 7] and order_date.month in [11, 12, 1]:
                weight *= 1.12

            weights.append(max(weight, 0.05))

        return random.choices(candidates, weights=weights)[0]

    def affinity_item(selected_items, available_items, restaurant_id, order_date, preferred_categories):
        selected_ids = {item["item_id"] for item in selected_items}
        affinity_categories = []

        for selected_item in selected_items:
            category_id = item_lookup[selected_item["item_id"]]["cat_id"]
            affinity_categories.extend(category_affinities.get(category_id, []))

        candidates = [
            item for item in available_items
            if item["item_id"] not in selected_ids
            and item_lookup[item["item_id"]]["cat_id"] in affinity_categories
        ]

        if not candidates:
            return None

        return weighted_item(candidates, restaurant_id, order_date, preferred_categories)

    for order_id in range(1, n + 1):
        customer = random.choices(eligible_customers, weights=customer_weights)[0]
        customer_id = customer["customer_id"]
        customer_profile = customer_profiles[customer_id]

        same_city = restaurants_by_city.get(customer["city"], [])
        restaurant_pool = same_city if same_city and random.random() < 0.97 else restaurants
        restaurant = random.choices(
            restaurant_pool,
            weights=[restaurant_demand_weights[x["restaurant_id"]] for x in restaurant_pool]
        )[0]
        restaurant_id = restaurant["restaurant_id"]

        earliest_date = max(DATASET_START, customer["signup_date"], restaurant["opening_date"])
        if earliest_date >= DATASET_END:
            continue

        order_date = choose_order_date(earliest_date)
        hour = choose_hour(customer_profile["preferred_period"], order_date)

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

        promotion_chance = 0.08 + (customer_profile["promotion_sensitivity"] * 0.42)
        promotion = random.choice(active_promotions) if active_promotions and random.random() < promotion_chance else None
        promotion_id = promotion["promotion_id"] if promotion else None

        channel = choose_channel(customer_profile["preferred_channel"], hour, order_date)

        order_status = random.choices(["Completed", "Cancelled"], weights=[97, 3])[0]

        if promotion:
            promoted_item = next(x for x in available_items if x["item_id"] == promotion["item_id"])
            selected_items = [promoted_item]
        else:
            selected_items = [
                weighted_item(
                    available_items,
                    restaurant_id,
                    order_date,
                    customer_profile["preferred_categories"]
                )
            ]

        target_lines = random.choices([1, 2, 3, 4, 5, 6], weights=[13, 28, 30, 18, 8, 3])[0]

        while len(selected_items) < target_lines:
            next_item = None

            if random.random() < 0.68:
                next_item = affinity_item(
                    selected_items,
                    available_items,
                    restaurant_id,
                    order_date,
                    customer_profile["preferred_categories"]
                )

            if next_item is None:
                selected_ids = {item["item_id"] for item in selected_items}
                remaining_items = [item for item in available_items if item["item_id"] not in selected_ids]

                if not remaining_items:
                    break

                next_item = weighted_item(
                    remaining_items,
                    restaurant_id,
                    order_date,
                    customer_profile["preferred_categories"]
                )

            selected_items.append(next_item)

        subtotal = 0
        discount_amount = 0
        current_order_items = []

        for selected_item in selected_items:
            item_id = selected_item["item_id"]
            quantity = random.choices([1, 2, 3, 4], weights=[78, 16, 5, 1])[0]

            unit_price = current_price(restaurant_id, item_id, order_date)

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

    inventory_weights = []
    for inventory in valid_inventories:
        received = max(float(inventory["quantity_received"]), 0.01)
        remaining_ratio = float(inventory["quantity_remaining"]) / received
        shelf_life = max(1, (inventory["expiry_date"] - inventory["received_date"]).days)
        weight = 0.25 + (remaining_ratio * 2.20)
        if shelf_life <= 7:
            weight *= 1.45
        elif shelf_life <= 14:
            weight *= 1.20
        inventory_weights.append(max(weight, 0.05))

    attempts = 0
    max_attempts = n * 20

    while len(wastage) < n and attempts < max_attempts:
        attempts += 1
        inventory = random.choices(valid_inventories, weights=inventory_weights)[0]
        available_quantity = remaining_stock[inventory["inventory_id"]]
        if available_quantity <= 0:
            continue

        received = max(float(inventory["quantity_received"]), 0.01)
        remaining_ratio = available_quantity / received
        shelf_life = max(1, (inventory["expiry_date"] - inventory["received_date"]).days)

        if remaining_ratio >= 0.60:
            reason_weights = [52, 18, 25, 5]
        elif remaining_ratio >= 0.30:
            reason_weights = [45, 25, 23, 7]
        else:
            reason_weights = [30, 38, 22, 10]

        if shelf_life <= 7:
            reason_weights[0] += 15

        reason = random.choices(
            ["Expired", "Spoiled", "Overproduction", "Damaged"],
            weights=reason_weights
        )[0]

        if inventory["unit"] == "pieces":
            if available_quantity < 1:
                continue
            if reason == "Expired":
                quantity = random.choices([1, 2, 3, 4], weights=[55, 27, 13, 5])[0]
            elif reason == "Overproduction":
                quantity = random.choices([1, 2, 3], weights=[60, 30, 10])[0]
            else:
                quantity = random.choices([1, 2], weights=[85, 15])[0]
            quantity = min(quantity, int(available_quantity))
        else:
            if reason == "Expired":
                max_wastage = min(available_quantity, max(0.10, received * 0.08))
            elif reason == "Spoiled":
                max_wastage = min(available_quantity, max(0.08, received * 0.05))
            elif reason == "Overproduction":
                max_wastage = min(available_quantity, max(0.05, received * 0.04))
            else:
                max_wastage = min(available_quantity, max(0.03, received * 0.02))
            if max_wastage < 0.01:
                continue
            quantity = round(random.uniform(0.01, max_wastage), 2)

        if reason == "Expired":
            wastage_date = inventory["expiry_date"]
        elif reason == "Overproduction":
            start_date = inventory["received_date"] + timedelta(days=max(0, shelf_life // 2))
            wastage_date = fake.date_between(
                start_date=min(start_date, inventory["expiry_date"]),
                end_date=min(inventory["expiry_date"], DATASET_END)
            )
        else:
            wastage_date = fake.date_between(
                start_date=inventory["received_date"],
                end_date=min(inventory["expiry_date"], DATASET_END)
            )

        unit_cost = inventory["unit_cost"]
        cost = round(quantity * unit_cost, 2)
        wastage.append({
            "wastage_id": wastage_id,
            "restaurant_id": inventory["restaurant_id"],
            "ingredient_id": inventory["ingredient_id"],
            "inventory_id": inventory["inventory_id"],
            "wastage_date": wastage_date,
            "quantity": quantity,
            "unit": inventory["unit"],
            "unit_cost": unit_cost,
            "cost": cost,
            "reason": reason
        })
        remaining_stock[inventory["inventory_id"]] = round(remaining_stock[inventory["inventory_id"]] - quantity, 2)
        wastage_id += 1

    if len(wastage) < n:
        print(f"WARNING: generated {len(wastage):,} wastage rows instead of requested {n:,}")

    return wastage

def generate_ratings(n, orders, order_items):
    ratings = []
    completed_orders = [order for order in orders if order["order_status"] == "Completed"]
    order_lookup = {order["order_id"]: order for order in completed_orders}
    items_by_order = {}

    for item in order_items:
        if item["order_id"] in order_lookup:
            items_by_order.setdefault(item["order_id"], []).append(item)

    restaurant_quality = {}
    item_quality = {}

    for order in completed_orders:
        restaurant_id = order["restaurant_id"]
        if restaurant_id not in restaurant_quality:
            restaurant_quality[restaurant_id] = random.uniform(-0.35, 0.35)

    for item in order_items:
        item_id = item["item_id"]
        if item_id not in item_quality:
            item_quality[item_id] = random.uniform(-0.30, 0.30)

    rating_candidates = [order for order in completed_orders if order["order_id"] in items_by_order]

    for i in range(1, n + 1):
        order = random.choice(rating_candidates)
        item = random.choice(items_by_order[order["order_id"]])

        score = 4.05
        score += restaurant_quality[order["restaurant_id"]]
        score += item_quality[item["item_id"]]

        if order["discount_amount"] > 0:
            score += 0.08
        if order["ordering_channel"] == "Third-Party Delivery":
            score -= 0.10
        elif order["ordering_channel"] == "Dine-in":
            score += 0.05
        if order["delivery_fee"] >= 200:
            score -= 0.08

        score += random.gauss(0, 0.72)
        if random.random() < 0.025:
            score -= random.uniform(1.0, 2.0)

        rating = max(1, min(5, int(round(score))))

        days_after = random.choices([0, 1, 2, 3, 4, 5, 6, 7], weights=[24, 28, 18, 11, 7, 5, 4, 3])[0]
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

print("generating ingredients...")
ingredients = generate_ingredient()

print("generating menu items...")
menu_items, menu_item_components = generate_menu_items(150)

print("generating menu item ingredients (BOM)...")
menu_item_ingredients = generate_menu_item_ingredients(menu_item_components, ingredient_pool)

print("generating restaurant menu items...")
restaurant_menu_items = generate_restaurant_menu_items(restaurants, menu_items)

print("generating pricing history...")
pricing_history = generate_pricing_history(restaurant_menu_items, menu_items, restaurants)

print("generating inventory...")
inventories = generate_inventory(restaurant_menu_items, menu_item_ingredients, ingredients, menu_items)

print("generating promotions...")
promotions = generate_promotion(100, restaurants, restaurant_menu_items, inventories, menu_item_ingredients)

print("generating orders and order items...")
orders, order_items = generate_orders(
    400000,
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
pd.DataFrame(ingredients).to_csv(f"{output_folder}/ingredients.csv", index=False)
pd.DataFrame(menu_items).to_csv(f"{output_folder}/menu_items.csv", index=False)
pd.DataFrame(menu_item_ingredients).to_csv(f"{output_folder}/menu_item_ingredients.csv", index=False)
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
print(f"Ingredients:           {len(ingredients):,}")
print(f"Menu Items:            {len(menu_items):,}")
print(f"Recipe Rows:           {len(menu_item_ingredients):,}")
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
