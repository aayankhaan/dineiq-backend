from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import ANALYTICS_DATA_FOLDER, ML_DATA_FOLDER


spark = SparkSession.builder \
    .appName("Validate DineIQ Recommendation Engine") \
    .getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")


output_folder = f"{ANALYTICS_DATA_FOLDER}/recommendations"


recommendations = spark.read.parquet(
    f"{output_folder}/recommendations"
)

summary = spark.read.parquet(
    f"{output_folder}/recommendation_summary"
)

location_menu = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/location_menu_performance/location_menu_performance"
)

slow_moving = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/slow_moving_dishes/slow_moving_dishes"
)

price_sensitivity = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/price_sensitivity/item_price_sensitivity"
)

promotion_effectiveness = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/promotion_effectiveness/promotion_effectiveness"
)

time_period_forecast = spark.read.parquet(
    f"{ML_DATA_FOLDER}/forecasting/time_period_forecast"
)


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


print("\n========RECOMMENDATION ENGINE VALIDATION========")


check(
    "Recommendation output contains data",
    recommendations.count() > 0
)


check(
    "Recommendation summary contains data",
    summary.count() > 0
)


required_columns = {
    "recommendation_id",
    "recommendation_type",
    "source_analysis",
    "recommendation_scope",
    "entity_key",
    "recommended_action",
    "evidence_summary",
    "impact_value",
    "impact_unit"
}


check(
    "Required recommendation columns exist",
    required_columns.issubset(
        set(recommendations.columns)
    )
)


check(
    "Recommendation IDs are unique",
    recommendations.select(
        "recommendation_id"
    ).distinct().count() ==
    recommendations.count()
)


check(
    "Recommendation IDs are populated",
    recommendations.filter(
        F.col("recommendation_id").isNull() |
        (F.trim("recommendation_id") == "")
    ).count() == 0
)


check(
    "Recommendation actions are populated",
    recommendations.filter(
        F.col("recommended_action").isNull() |
        (F.trim("recommended_action") == "")
    ).count() == 0
)


check(
    "Recommendation evidence is populated",
    recommendations.filter(
        F.col("evidence_summary").isNull() |
        (F.trim("evidence_summary") == "")
    ).count() == 0
)


check(
    "Impact values are non-negative",
    recommendations.filter(
        F.col("impact_value") < 0
    ).count() == 0
)


expected_types = {
    "Promote Hidden Opportunity",
    "Reduce Preparation Quantity",
    "Review Price Strategy",
    "Bundle Frequently Purchased Items",
    "Review or Redesign Low Performer",
    "Increase Stock Before Predicted Peak",
    "Target At-Risk Customer Segment",
    "Review Ineffective Promotion",
    "Investigate Anomalous Location"
}


actual_types = {
    row["recommendation_type"]
    for row in recommendations.select(
        "recommendation_type"
    ).distinct().collect()
}


check(
    "All nine Step 37 recommendation areas are represented",
    expected_types.issubset(
        actual_types
    )
)


hidden_source = location_menu.select(
    "restaurant_id",
    "item_id",
    "performance_class",
    "contribution_margin",
    "location_margin_median"
)


hidden_recommendations = recommendations.filter(
    F.col("recommendation_type") ==
    "Promote Hidden Opportunity"
).join(
    hidden_source,
    [
        "restaurant_id",
        "item_id"
    ],
    "left"
)


check(
    "Hidden Opportunity recommendations use actual high-margin Hidden Opportunities",
    hidden_recommendations.filter(
        (
            F.col("performance_class") !=
            "Hidden Opportunity"
        ) |
        (
            F.col("contribution_margin") <= 0
        ) |
        (
            F.col("contribution_margin") <
            F.col("location_margin_median")
        )
    ).count() == 0
)


wastage_source = location_menu.select(
    "restaurant_id",
    "item_id",
    "wastage_percentage",
    "location_wastage_upper"
)


wastage_recommendations = recommendations.filter(
    F.col("recommendation_type") ==
    "Reduce Preparation Quantity"
).join(
    wastage_source,
    [
        "restaurant_id",
        "item_id"
    ],
    "left"
)


check(
    "Preparation reduction recommendations use above-threshold wastage",
    wastage_recommendations.filter(
        F.col("wastage_percentage") <=
        F.col("location_wastage_upper")
    ).count() == 0
)


price_source = price_sensitivity.select(
    "item_id",
    "price_sensitivity_class"
)


price_recommendations = recommendations.filter(
    F.col("recommendation_type") ==
    "Review Price Strategy"
).join(
    price_source,
    "item_id",
    "left"
)


check(
    "Pricing recommendations use highly price-sensitive items",
    price_recommendations.filter(
        F.col("price_sensitivity_class") !=
        "Highly Price Sensitive"
    ).count() == 0
)


bundle_recommendations = recommendations.filter(
    F.col("recommendation_type") ==
    "Bundle Frequently Purchased Items"
)


check(
    "Bundle recommendations have positive association lift",
    bundle_recommendations.filter(
        F.col("impact_value") <= 1.10
    ).count() == 0
)


low_source = location_menu.select(
    "restaurant_id",
    "item_id",
    "performance_class"
).join(
    slow_moving.select(
        "restaurant_id",
        "item_id",
        "slow_moving_dish"
    ),
    [
        "restaurant_id",
        "item_id"
    ],
    "inner"
)


low_recommendations = recommendations.filter(
    F.col("recommendation_type") ==
    "Review or Redesign Low Performer"
).join(
    low_source,
    [
        "restaurant_id",
        "item_id"
    ],
    "left"
)


check(
    "Low Performer recommendations combine low performance and slow movement",
    low_recommendations.filter(
        (
            F.col("performance_class") !=
            "Low Performer"
        ) |
        (
            ~F.col("slow_moving_dish")
        )
    ).count() == 0
)


peak_recommendations = recommendations.filter(
    F.col("recommendation_type") ==
    "Increase Stock Before Predicted Peak"
)


check(
    "Predicted peak recommendations are generated",
    peak_recommendations.count() > 0
)


check(
    "Predicted peak recommendations use positive forecast demand",
    peak_recommendations.filter(
        F.col("impact_value") <= 0
    ).count() == 0
)


check(
    "Forecast source contains predicted demand",
    time_period_forecast.filter(
        F.col("predicted_demand") > 0
    ).count() > 0
)


segment_recommendations = recommendations.filter(
    F.col("recommendation_type") ==
    "Target At-Risk Customer Segment"
)


check(
    "Customer targeting recommendations identify a segment",
    segment_recommendations.filter(
        F.col("customer_segment").isNull() |
        (F.trim("customer_segment") == "")
    ).count() == 0
)


promotion_source = promotion_effectiveness.select(
    "promotion_id",
    "promotion_assessment"
)


promotion_recommendations = recommendations.filter(
    F.col("recommendation_type") ==
    "Review Ineffective Promotion"
).join(
    promotion_source,
    "promotion_id",
    "left"
)


check(
    "Promotion review recommendations use ineffective promotions",
    promotion_recommendations.filter(
        F.col("promotion_assessment") !=
        "Ineffective"
    ).count() == 0
)


location_recommendations = recommendations.filter(
    F.col("recommendation_type") ==
    "Investigate Anomalous Location"
)


check(
    "Anomalous location recommendations identify restaurants",
    location_recommendations.filter(
        F.col("restaurant_id").isNull()
    ).count() == 0
)


check(
    "Anomalous location impact rates are positive",
    location_recommendations.filter(
        F.col("impact_value") <= 0
    ).count() == 0
)


summary_total = summary.agg(
    F.sum(
        "recommendation_count"
    ).alias("value")
).first()["value"]


check(
    "Recommendation summary reconciles to recommendation rows",
    int(summary_total) ==
    recommendations.count()
)


check(
    "Each recommendation type has exactly one summary row",
    summary.groupBy(
        "recommendation_type"
    ).count().filter(
        F.col("count") != 1
    ).count() == 0
)


total = passed + failed


print(
    f"\n========VALIDATION RESULT: "
    f"{passed}/{total} PASS========"
)


if failed == 0:
    print(
        "\nRecommendation engine validation PASSED."
    )
else:
    print(
        "\nRecommendation engine validation FAILED."
    )


spark.stop()
