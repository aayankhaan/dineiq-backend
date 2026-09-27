from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.settings import ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("DineIQ Bundle and Cross-Sell Recommendations") \
    .getOrCreate()


market_basket_folder = f"{ANALYTICS_DATA_FOLDER}/market_basket"
output_folder = f"{ANALYTICS_DATA_FOLDER}/bundle_recommendations"


rules = spark.read.parquet(
    f"{market_basket_folder}/suitable_rules"
)


recommendation_base = rules.filter(
    F.col("lift") > 1.0
).withColumn(
    "recommendation_type",
    F.when(
        F.col("lift") >= 2.0,
        "Combo Meal"
    ).when(
        F.col("confidence") >= 0.08,
        "Upsell Combination"
    ).otherwise(
        "Cross-Sell Opportunity"
    )
).withColumn(
    "recommended_action",
    F.concat(
        F.lit("Recommend "),
        F.col("consequent_item_name"),
        F.lit(" with "),
        F.col("antecedent_item_name")
    )
).withColumn(
    "evidence",
    F.concat(
        F.lit("Support "),
        F.round(F.col("support") * 100, 2),
        F.lit("%, confidence "),
        F.round(F.col("confidence") * 100, 2),
        F.lit("%, lift "),
        F.round(F.col("lift"), 2)
    )
)


pair_dedup_window = Window.partitionBy(
    F.least(F.col("antecedent_item_id"), F.col("consequent_item_id")),
    F.greatest(F.col("antecedent_item_id"), F.col("consequent_item_id"))
).orderBy(
    F.desc("lift"),
    F.desc("confidence"),
    F.desc("support")
)

recommendation_base = recommendation_base.withColumn(
    "pair_rank",
    F.row_number().over(pair_dedup_window)
).filter(
    F.col("pair_rank") == 1
).drop("pair_rank")


frequently_paired_dishes = recommendation_base.select(
    "antecedent_item_id",
    "antecedent_item_name",
    "consequent_item_id",
    "consequent_item_name",
    "pair_order_count",
    "support",
    "confidence",
    "lift",
    "association_strength"
).orderBy(
    F.desc("lift"),
    F.desc("confidence"),
    F.desc("support")
)


recommendations = recommendation_base.select(
    "antecedent_item_id",
    "antecedent_item_name",
    "consequent_item_id",
    "consequent_item_name",
    "recommendation_type",
    "recommended_action",
    "evidence",
    "pair_order_count",
    "support",
    "confidence",
    "lift",
    "association_strength"
).orderBy(
    F.desc("lift"),
    F.desc("confidence"),
    F.desc("support")
)


item_window = Window.partitionBy(
    "antecedent_item_id"
).orderBy(
    F.desc("lift"),
    F.desc("confidence"),
    F.desc("support")
)


top_item_recommendations = recommendations.withColumn(
    "recommendation_rank",
    F.row_number().over(item_window)
).filter(
    F.col("recommendation_rank") <= 5
)

recommendation_summary = recommendations.groupBy(
    "recommendation_type"
).agg(
    F.count("*").alias("recommendation_count"),
    F.round(F.avg("support"), 6).alias("average_support"),
    F.round(F.avg("confidence"), 6).alias("average_confidence"),
    F.round(F.avg("lift"), 4).alias("average_lift")
).orderBy(
    F.desc("recommendation_count")
)


print("\n========RECOMMENDATION SUMMARY========")
recommendation_summary.show(
    truncate=False
)


print("\n========TOP BUNDLE AND CROSS-SELL RECOMMENDATIONS========")
recommendations.show(
    20,
    truncate=False
)


print("\n========TOP ITEM RECOMMENDATIONS========")
top_item_recommendations.orderBy(
    "antecedent_item_id",
    "recommendation_rank"
).show(
    20,
    truncate=False
)


recommendations.write.mode("overwrite").parquet(
    f"{output_folder}/recommendations"
)


frequently_paired_dishes.write.mode("overwrite").parquet(
    f"{output_folder}/frequently_paired_dishes"
)


top_item_recommendations.write.mode("overwrite").parquet(
    f"{output_folder}/top_item_recommendations"
)


recommendation_summary.write.mode("overwrite").parquet(
    f"{output_folder}/recommendation_summary"
)


print("\nBundle and cross-sell recommendations completed successfully.")


spark.stop()