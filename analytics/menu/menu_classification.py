from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from config.settings import ANALYTICS_DATA_FOLDER

spark = SparkSession.builder \
    .appName("DineIQ Menu Performance Classification") \
    .getOrCreate()

input_folder = ANALYTICS_DATA_FOLDER
output_folder = ANALYTICS_DATA_FOLDER

menu_profitability = spark.read.parquet(
    f"{input_folder}/menu_profitability"
)


quantity_median = menu_profitability.approxQuantile(
    "quantity_sold", [0.5], 0.01
)[0]

profit_median = menu_profitability.approxQuantile(
    "profit_percentage", [0.5], 0.01
)[0]

margin_median = menu_profitability.approxQuantile(
    "contribution_margin", [0.5], 0.01
)[0]

rating_median = menu_profitability.approxQuantile(
    "average_rating", [0.5], 0.01
)[0]

repeat_median = menu_profitability.approxQuantile(
    "repeat_purchase_rate", [0.5], 0.01
)[0]

wastage_upper = menu_profitability.approxQuantile(
    "wastage_percentage", [0.75], 0.01
)[0]

print("\n========CLASSIFICATION THRESHOLDS========")
print(f"Quantity Sold Median: {quantity_median}")
print(f"Profit Percentage Median: {profit_median}")
print(f"Contribution Margin Median: {margin_median}")
print(f"Average Rating Median: {rating_median}")
print(f"Repeat Purchase Median: {repeat_median}")
print(f"Wastage 75th Percentile: {wastage_upper}")


classified = menu_profitability.withColumn(
    "high_demand",
    F.col("quantity_sold") >= F.lit(quantity_median)
).withColumn(
    "high_profitability",
    (
        (F.col("profit_percentage") >= F.lit(profit_median)) &
        (F.col("contribution_margin") >= F.lit(margin_median))
    )
).withColumn(
    "good_rating",
    F.col("average_rating") >= F.lit(rating_median)
).withColumn(
    "strong_repeat_purchase",
    F.col("repeat_purchase_rate") >= F.lit(repeat_median)
).withColumn(
    "acceptable_wastage",
    F.col("wastage_percentage") <= F.lit(wastage_upper)
)

classified = classified.withColumn(
    "performance_class",
    F.when(
        F.col("high_demand") &
        F.col("high_profitability") &
        F.col("acceptable_wastage"),
        "Profit Driver"
    ).when(
        F.col("high_demand") &
        ~F.col("high_profitability"),
        "Volume Driver"
    ).when(
        ~F.col("high_demand") &
        (
            F.col("high_profitability") |
            F.col("good_rating") |
            F.col("strong_repeat_purchase")
        ) &
        F.col("acceptable_wastage"),
        "Hidden Opportunity"
    ).otherwise(
        "Low Performer"
    )
)

classified = classified.orderBy(
    "performance_class",
    F.desc("contribution_margin"),
    F.asc("item_id")
)

print("\n========MENU PERFORMANCE CLASSIFICATION========")
classified.select(
    "item_id",
    "item_name",
    "quantity_sold",
    "profit_percentage",
    "contribution_margin",
    "average_rating",
    "repeat_purchase_rate",
    "wastage_percentage",
    "performance_class"
).show(150, truncate=False)

print("\n========CLASS DISTRIBUTION========")
classified.groupBy(
    "performance_class"
).count().orderBy(
    F.desc("count")
).show(truncate=False)

classified.write.mode("overwrite").parquet(
    f"{output_folder}/menu_classification"
)

print("\nMenu performance classification completed successfully.")

spark.stop()
