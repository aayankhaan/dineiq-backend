from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("DineIQ Menu Profitability Analysis") \
    .getOrCreate()

integrated_folder = "integrated_data"
feature_folder = "feature_data"
output_folder = "analytics_data"

transactions = spark.read.parquet(
    f"{integrated_folder}/transactions"
)

item_features = spark.read.parquet(
    f"{feature_folder}/item_features"
)


quantity_sold = transactions.groupBy(
    "item_id"
).agg(
    F.sum("quantity").alias("quantity_sold")
)


monthly_sales = transactions.groupBy(
    "item_id",
    F.date_trunc("month", "order_datetime").alias("sales_month")
).agg(
    F.sum("quantity").alias("monthly_quantity")
)

sales_window = Window.partitionBy(
    "item_id"
).orderBy(
    "sales_month"
)

monthly_sales = monthly_sales.withColumn(
    "previous_month_quantity",
    F.lag("monthly_quantity").over(sales_window)
).withColumn(
    "monthly_sales_change",
    F.when(
        F.col("previous_month_quantity") > 0,
        (
            (F.col("monthly_quantity") - F.col("previous_month_quantity")) /
            F.col("previous_month_quantity")
        ) * 100
    )
)

sales_trend = monthly_sales.groupBy(
    "item_id"
).agg(
    F.round(
        F.avg("monthly_sales_change"),
        2
    ).alias("sales_trend")
).fillna(
    0,
    subset=["sales_trend"]
)


menu_profitability = item_features.join(
    quantity_sold,
    "item_id",
    "left"
).join(
    sales_trend,
    "item_id",
    "left"
).fillna({
    "quantity_sold": 0,
    "sales_trend": 0.0
}).select(
    "item_id",
    "item_name",
    "quantity_sold",
    "item_revenue",
    "cost",
    "contribution_margin",
    "profit_percentage",
    "average_rating",
    "repeat_purchase_rate",
    "wastage_percentage",
    "promotion_dependency",
    "sales_trend"
).orderBy(
    F.desc("contribution_margin"),
    F.asc("item_id")
)

print("\n========MENU PROFITABILITY ANALYSIS========")
menu_profitability.show(20, truncate=False)

menu_profitability.write.mode("overwrite").parquet(
    f"{output_folder}/menu_profitability"
)

print("\nMenu profitability analysis completed successfully.")

spark.stop()
