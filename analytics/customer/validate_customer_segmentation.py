from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("Validate DineIQ Customer Segmentation") \
    .getOrCreate()


output_folder = f"{ANALYTICS_DATA_FOLDER}/customer_segmentation"

segments = spark.read.parquet(f"{output_folder}/customer_segments")
summary = spark.read.parquet(f"{output_folder}/segment_summary")
definitions = spark.read.parquet(f"{output_folder}/segment_definitions")


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


# ==================== REQUIRED OUTPUTS ====================


check(
    "Customer segmentation output contains data",
    segments.count() > 0
)

check(
    "Segment summary contains data",
    summary.count() > 0
)

check(
    "Segment definitions contain data",
    definitions.count() > 0
)


# ==================== REQUIRED CUSTOMER FEATURES ====================


required_columns = [
    "customer_id",
    "customer_recency",
    "customer_frequency",
    "customer_monetary_value",
    "average_order_value",
    "visit_frequency",
    "favorite_category",
    "promotion_sensitivity",
    "channel_preference",
    "time_of_day_preference",
    "repeat_customer",
    "customer_segment",
    "segment_reason",
    "recommended_strategy"
]


for column in required_columns:
    check(
        f"Required column exists: {column}",
        column in segments.columns
    )


# ==================== DATA COMPLETENESS ====================


check(
    "Customer IDs are unique",
    segments.select("customer_id").distinct().count() == segments.count()
)

check(
    "Customer IDs are not null",
    segments.filter(F.col("customer_id").isNull()).count() == 0
)

check(
    "Customer segments are not null",
    segments.filter(F.col("customer_segment").isNull()).count() == 0
)

check(
    "Segment reasons are not null",
    segments.filter(F.col("segment_reason").isNull()).count() == 0
)

check(
    "Recommended strategies are not null",
    segments.filter(F.col("recommended_strategy").isNull()).count() == 0
)

check(
    "Favorite categories are populated",
    segments.filter(F.col("favorite_category").isNull()).count() == 0
)

check(
    "Channel preferences are populated",
    segments.filter(F.col("channel_preference").isNull()).count() == 0
)

check(
    "Time-of-day preferences are populated",
    segments.filter(F.col("time_of_day_preference").isNull()).count() == 0
)


# ==================== VALUE VALIDATION ====================


check(
    "Recency values are non-negative",
    segments.filter(F.col("customer_recency") < 0).count() == 0
)

check(
    "Frequency values are positive",
    segments.filter(F.col("customer_frequency") <= 0).count() == 0
)

check(
    "Monetary values are non-negative",
    segments.filter(F.col("customer_monetary_value") < 0).count() == 0
)

check(
    "Average order values are non-negative",
    segments.filter(F.col("average_order_value") < 0).count() == 0
)

check(
    "Visit frequency values are positive",
    segments.filter(F.col("visit_frequency") <= 0).count() == 0
)

check(
    "Promotion sensitivity is between 0 and 100",
    segments.filter(
        (F.col("promotion_sensitivity") < 0) |
        (F.col("promotion_sensitivity") > 100)
    ).count() == 0
)


# ==================== SIX REQUIRED SEGMENTS ====================


expected_segments = {
    "High-Value Loyal Customers",
    "Frequent Customers",
    "Promotion-Driven Customers",
    "At-Risk Customers",
    "New Customers",
    "Occasional Customers"
}


actual_segments = {
    row["customer_segment"]
    for row in segments.select("customer_segment").distinct().collect()
}


check(
    "Exactly six customer segments exist",
    len(actual_segments) == 6
)

check(
    "All expected customer segments exist",
    actual_segments == expected_segments
)

check(
    "Every customer segment contains customers",
    summary.filter(F.col("customer_count") <= 0).count() == 0
)


# ==================== SEGMENT SUMMARY VALIDATION ====================


check(
    "Summary contains exactly six rows",
    summary.count() == 6
)

check(
    "Summary customer counts equal segmented customers",
    summary.agg(
        F.sum("customer_count").alias("total")
    ).first()["total"] == segments.count()
)

summary_percentage = summary.agg(
    F.sum("customer_percentage").alias("total_percentage")
).first()["total_percentage"]


check(
    "Segment percentages total approximately 100 percent",
    abs(float(summary_percentage) - 100.0) <= 0.1
)


# ==================== SEGMENT DEFINITION VALIDATION ====================


check(
    "Definitions contain exactly six rows",
    definitions.count() == 6
)

definition_segments = {
    row["customer_segment"]
    for row in definitions.select("customer_segment").distinct().collect()
}


check(
    "Definitions cover all six customer segments",
    definition_segments == expected_segments
)

check(
    "Every segment has a business objective",
    definitions.filter(
        F.col("business_objective").isNull() |
        (F.trim(F.col("business_objective")) == "")
    ).count() == 0
)

check(
    "Every segment has a recommended strategy",
    definitions.filter(
        F.col("recommended_strategy").isNull() |
        (F.trim(F.col("recommended_strategy")) == "")
    ).count() == 0
)


# ==================== BEHAVIORAL LOGIC VALIDATION ====================


new_customers = segments.filter(
    F.col("customer_segment") == "New Customers"
)


check(
    "New customers have exactly one order",
    new_customers.filter(
        F.col("customer_frequency") != 1
    ).count() == 0
)


occasional_customers = segments.filter(
    F.col("customer_segment") == "Occasional Customers"
)


check(
    "Occasional customers have lower-than-median frequency",
    occasional_customers.filter(
        F.col("customer_frequency") >= 7
    ).count() == 0
)


promotion_driven = segments.filter(
    F.col("customer_segment") == "Promotion-Driven Customers"
)


check(
    "Promotion-driven customers have actual promotion usage",
    promotion_driven.filter(
        F.col("promotion_sensitivity") <= 0
    ).count() == 0
)


high_value = segments.filter(
    F.col("customer_segment") == "High-Value Loyal Customers"
)


check(
    "High-value loyal customers are repeat customers",
    high_value.filter(
        F.col("repeat_customer") == False
    ).count() == 0
)


at_risk = segments.filter(
    F.col("customer_segment") == "At-Risk Customers"
)


check(
    "At-risk customers contain repeat customers",
    at_risk.filter(
        F.col("repeat_customer") == True
    ).count() > 0
)


# ==================== TARGETING READINESS ====================


check(
    "Every customer has a segment reason",
    segments.filter(
        F.trim(F.col("segment_reason")) == ""
    ).count() == 0
)

check(
    "Every customer has an actionable recommendation",
    segments.filter(
        F.trim(F.col("recommended_strategy")) == ""
    ).count() == 0
)


# ==================== FINAL RESULT ====================


total_checks = passed + failed


print("\n========CUSTOMER SEGMENTATION VALIDATION========")
print(f"Passed: {passed}/{total_checks}")
print(f"Failed: {failed}/{total_checks}")


if failed == 0:
    print("\nCustomer segmentation validation PASSED.")
else:
    print("\nCustomer segmentation validation FAILED.")


spark.stop()
