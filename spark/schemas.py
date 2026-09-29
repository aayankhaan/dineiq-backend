from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType, DateType, BooleanType

customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("signup_date", DateType(), True),
    StructField("birth_year", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("city", StringType(), True)
])

ingredients_schema = StructType([
    StructField("ingredient_id", IntegerType(), True),
    StructField("ingredient", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("unit_cost", DoubleType(), True)
])

inventory_schema = StructType([
    StructField("inventory_id", IntegerType(), True),
    StructField("restaurant_id", IntegerType(), True),
    StructField("ingredient_id", IntegerType(), True),
    StructField("quantity_received", DoubleType(), True),
    StructField("quantity_remaining", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("unit_cost", DoubleType(), True),
    StructField("received_date", DateType(), True),
    StructField("expiry_date", DateType(), True)
])

menu_category_schema = StructType([
    StructField("category_id", IntegerType(), True),
    StructField("name", StringType(), True),
])

menu_items_schema = StructType([
    StructField("menu_items_id", IntegerType(), True),
    StructField("cat_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("base_price", IntegerType(), True),
    StructField("prep_time_minutes", IntegerType(), True),
    StructField("introduced_date", DateType(), True),
    StructField("discontinued_date", DateType(), True),
    StructField("is_available", BooleanType(), True)
])

menu_item_ingredients_schema = StructType([
    StructField("menu_item_ingredient_id", IntegerType(), True),
    StructField("menu_item_id", IntegerType(), True),
    StructField("ingredient_id", IntegerType(), True),
    StructField("quantity_required", DoubleType(), True)
])

order_item_schema = StructType([
    StructField("order_item_id", IntegerType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount_amount", DoubleType(), True),
    StructField("line_total", DoubleType(), True)
])

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("restaurant_id", IntegerType(), True),
    StructField("promotion_id", IntegerType(), True),
    StructField("order_datetime", TimestampType(), True),
    StructField("ordering_channel", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("subtotal", DoubleType(), True),
    StructField("discount_amount", DoubleType(), True),
    StructField("tax_amount", DoubleType(), True),
    StructField("delivery_fee", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_method", StringType(), True),
])

price_history_schema = StructType([
    StructField("price_history_id", IntegerType(), True),
    StructField("restaurant_id", IntegerType(), True), 
    StructField("item_id", IntegerType(), True),
    StructField("old_price", DoubleType(), True),
    StructField("new_price", DoubleType(), True),
    StructField("effective_date", DateType(), True),
    StructField("reason", StringType(), True)
])

promotion_schema = StructType([
    StructField("promotion_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("discount_type", StringType(), True),
    StructField("discount_value", IntegerType(), True),
    StructField("start_date", DateType(), True),
    StructField("end_date", DateType(), True),
    StructField("restaurant_id", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("minimum_order_value", DoubleType(), True),
    StructField("coupon_code", StringType(), True)
])

rating_schema = StructType([
    StructField("rating_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("restaurant_id", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("rating_date", DateType(), True),
])

restaurant_menu_schema = StructType([
    StructField("restaurant_menu_id", IntegerType(), True),
    StructField("restaurant_id", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("is_available", BooleanType(), True)
])

restaurant_schema = StructType([
    StructField("restaurant_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("area", StringType(), True),
    StructField("opening_date", DateType(), True),
    StructField("status", StringType(), True)
])

wastage_schema = StructType([
    StructField("wastage_id", IntegerType(), True),
    StructField("restaurant_id", IntegerType(), True),
    StructField("ingredient_id", IntegerType(), True),
    StructField("inventory_id", IntegerType(), True),
    StructField("wastage_date", DateType(), True),
    StructField("quantity", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("unit_cost", DoubleType(), True),
    StructField("cost", DoubleType(), True),
    StructField("reason", StringType(), True)  
])
