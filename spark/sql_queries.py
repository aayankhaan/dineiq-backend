from pyspark.sql import SparkSession
from config.settings import INTEGRATED_DATA_FOLDER

spark = SparkSession.builder \
    .appName("DineIQ Spark SQL") \
    .getOrCreate()

integrated_folder = INTEGRATED_DATA_FOLDER
sql_output_folder = f"{INTEGRATED_DATA_FOLDER}/sql_results"

transactions = spark.read.parquet(f"{integrated_folder}/transactions")
ratings = spark.read.parquet(f"{integrated_folder}/ratings")
pricing_history = spark.read.parquet(f"{integrated_folder}/pricing_history")
wastage = spark.read.parquet(f"{integrated_folder}/wastage")

transactions.createOrReplaceTempView("transactions")
ratings.createOrReplaceTempView("ratings")
pricing_history.createOrReplaceTempView("pricing_history")
wastage.createOrReplaceTempView("wastage")

print("\n========LOCATION PERFORMANCE========\n")

location_performance = spark.sql("""
    SELECT
        restaurant_id,
        restaurant_name,
        restaurant_city,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(quantity) AS items_sold,
        CAST(ROUND(SUM(line_total), 2) AS DECIMAL(18,2)) AS total_revenue
    FROM transactions
    WHERE order_status != 'Cancelled'
    GROUP BY restaurant_id, restaurant_name, restaurant_city
    ORDER BY total_revenue DESC
""")

location_performance.show(20, truncate=False)

print("\n========MENU ITEM PERFORMANCE========\n")

menu_performance = spark.sql("""
    SELECT
        item_id,
        item_name,
        category_name,
        SUM(quantity) AS quantity_sold,
        CAST(ROUND(SUM(line_total), 2) AS DECIMAL(18,2)) AS total_revenue,
        CAST(ROUND(SUM(item_cost * quantity), 2) AS DECIMAL(18,2)) AS estimated_cost,
        CAST(ROUND(SUM(line_total) - SUM(item_cost * quantity), 2) AS DECIMAL(18,2)) AS contribution_margin
    FROM transactions
    WHERE order_status != 'Cancelled'
    GROUP BY item_id, item_name, category_name
    ORDER BY total_revenue DESC
""")

menu_performance.show(20, truncate=False)

print("\n========CATEGORY PERFORMANCE========\n")

category_performance = spark.sql("""
    SELECT
        category_id,
        category_name,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(quantity) AS quantity_sold,
        CAST(ROUND(SUM(line_total), 2) AS DECIMAL(18,2)) AS total_revenue
    FROM transactions
    WHERE order_status != 'Cancelled'
    GROUP BY category_id, category_name
    ORDER BY total_revenue DESC
""")

category_performance.show(20, truncate=False)

print("\n========RATING PERFORMANCE========\n")

rating_performance = spark.sql("""
    SELECT
        item_id,
        item_name,
        COUNT(*) AS total_ratings,
        CAST(ROUND(AVG(rating), 2) AS DECIMAL(18,2)) AS average_rating
    FROM ratings
    GROUP BY item_id, item_name
    ORDER BY average_rating DESC, total_ratings DESC
""")

rating_performance.show(20, truncate=False)

print("\n========WASTAGE BY LOCATION========\n")

wastage_performance = spark.sql("""
    SELECT
        restaurant_id,
        restaurant_name,
        restaurant_city,
        CAST(ROUND(SUM(quantity), 2) AS DECIMAL(18,2)) AS total_wastage_quantity,
        CAST(ROUND(SUM(cost), 2) AS DECIMAL(18,2)) AS total_wastage_cost
    FROM wastage
    GROUP BY restaurant_id, restaurant_name, restaurant_city
    ORDER BY total_wastage_cost DESC
""")

wastage_performance.show(20, truncate=False)

location_performance.write.mode("overwrite").parquet(f"{sql_output_folder}/location_performance")
menu_performance.write.mode("overwrite").parquet(f"{sql_output_folder}/menu_performance")
category_performance.write.mode("overwrite").parquet(f"{sql_output_folder}/category_performance")
rating_performance.write.mode("overwrite").parquet(f"{sql_output_folder}/rating_performance")
wastage_performance.write.mode("overwrite").parquet(f"{sql_output_folder}/wastage_performance")

print("\nspark SQL analytical results saved to data/integrated/sql_results/")
print("Spark SQL analytical queries completed successfully.")