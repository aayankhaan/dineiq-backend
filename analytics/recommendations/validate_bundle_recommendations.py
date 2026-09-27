from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("Validate DineIQ Bundle Recommendations") \
    .getOrCreate()


output_folder = f"{ANALYTICS_DATA_FOLDER}/bundle_recommendations"

recommendations = spark.read.parquet(f"{output_folder}/recommendations")
paired = spark.read.parquet(f"{output_folder}/frequently_paired_dishes")
top_items = spark.read.parquet(f"{output_folder}/top_item_recommendations")
summary = spark.read.parquet(f"{output_folder}/recommendation_summary")


passed = 0
failed = 0


def check(name, condition):
    global passed, failed

    if condition:
        print(f"[PASS] {name}")
        passed += 1
    else:
        print(f"[FAIL] {name}")
        failed += 1


# ==================== OUTPUT VALIDATION ====================


check("Recommendations contain data", recommendations.count() > 0)
check("Frequently paired dishes contain data", paired.count() > 0)
check("Top item recommendations contain data", top_items.count() > 0)
check("Recommendation summary contains data", summary.count() > 0)


# ==================== REQUIRED FIELDS ====================


required_columns = [
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
]


for column in required_columns:
    check(
        f"Required recommendation column exists: {column}",
        column in recommendations.columns
    )


# ==================== EVIDENCE VALIDATION ====================


check(
    "Every recommendation has association-rule evidence",
    recommendations.filter(
        F.col("support").isNull() |
        F.col("confidence").isNull() |
        F.col("lift").isNull()
    ).count() == 0
)

check(
    "Every recommendation has positive association lift",
    recommendations.filter(
        F.col("lift") <= 1.0
    ).count() == 0
)

check(
    "Support values are valid",
    recommendations.filter(
        (F.col("support") <= 0) |
        (F.col("support") > 1)
    ).count() == 0
)

check(
    "Confidence values are valid",
    recommendations.filter(
        (F.col("confidence") <= 0) |
        (F.col("confidence") > 1)
    ).count() == 0
)

check(
    "Recommended items differ from source items",
    recommendations.filter(
        F.col("antecedent_item_id") == F.col("consequent_item_id")
    ).count() == 0
)

check(
    "Every recommendation contains readable evidence",
    recommendations.filter(
        F.col("evidence").isNull() |
        (F.length(F.col("evidence")) == 0)
    ).count() == 0
)


# ==================== SRS RECOMMENDATION TYPES ====================


types = {
    row["recommendation_type"]
    for row in recommendations.select(
        "recommendation_type"
    ).distinct().collect()
}


check(
    "Combo meal recommendations exist",
    "Combo Meal" in types
)

check(
    "Cross-sell recommendations exist",
    "Cross-Sell Opportunity" in types
)

check(
    "Upsell combination recommendations exist",
    "Upsell Combination" in types
)

check(
    "Frequently paired dish output exists",
    paired.count() > 0
)


# ==================== TOP ITEM RECOMMENDATIONS ====================


check(
    "Top item recommendation ranks are valid",
    top_items.filter(
        (F.col("recommendation_rank") < 1) |
        (F.col("recommendation_rank") > 5)
    ).count() == 0
)

check(
    "No item has more than five top recommendations",
    top_items.groupBy(
        "antecedent_item_id"
    ).count().filter(
        F.col("count") > 5
    ).count() == 0
)


# ==================== SUMMARY VALIDATION ====================


check(
    "Summary recommendation counts match output",
    summary.agg(
        F.sum("recommendation_count").alias("total")
    ).first()["total"] == recommendations.count()
)


# ==================== FINAL RESULT ====================


total_checks = passed + failed


print("\n========BUNDLE RECOMMENDATION VALIDATION========")
print(f"Passed: {passed}/{total_checks}")
print(f"Failed: {failed}/{total_checks}")


if failed == 0:
    print("\nBundle and cross-sell recommendation validation PASSED.")
else:
    print("\nBundle and cross-sell recommendation validation FAILED.")


spark.stop()
