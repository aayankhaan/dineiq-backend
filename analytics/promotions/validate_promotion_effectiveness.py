from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import (
    PROCESSED_DATA_FOLDER,
    ANALYTICS_DATA_FOLDER
)


spark = SparkSession.builder \
    .appName("Validate DineIQ Promotion Effectiveness") \
    .getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")


processed_folder = PROCESSED_DATA_FOLDER
output_folder = f"{ANALYTICS_DATA_FOLDER}/promotion_effectiveness"


promotions = spark.read.parquet(
    f"{processed_folder}/promotions"
)

promotion_effectiveness = spark.read.parquet(
    f"{output_folder}/promotion_effectiveness"
)

promotion_customer_behavior = spark.read.parquet(
    f"{output_folder}/promotion_customer_behavior"
)

effectiveness_summary = spark.read.parquet(
    f"{output_folder}/effectiveness_summary"
)

analysis_metadata = spark.read.parquet(
    f"{output_folder}/analysis_metadata"
)


passed = 0
failed = 0


def check(name, condition, details=""):
    global passed, failed

    if condition:
        print(f"[PASS] {name}")
        passed += 1
    else:
        suffix = (
            f" - {details}"
            if details
            else ""
        )

        print(
            f"[FAIL] {name}{suffix}"
        )
        failed += 1


print("\n========PROMOTION EFFECTIVENESS VALIDATION========")


# ==================== OUTPUTS ====================


for name, frame in [
    ("Promotion effectiveness", promotion_effectiveness),
    ("Promotion customer behavior", promotion_customer_behavior),
    ("Effectiveness summary", effectiveness_summary),
    ("Analysis metadata", analysis_metadata)
]:
    check(
        f"{name} contains data",
        frame.count() > 0
    )


# ==================== PROMOTION COVERAGE ====================


source_count = promotions.select(
    "promotion_id"
).distinct().count()


analysis_count = promotion_effectiveness.select(
    "promotion_id"
).distinct().count()


check(
    "Every promotion is evaluated",
    source_count == analysis_count,
    (
        f"Source: {source_count}, "
        f"analysis: {analysis_count}"
    )
)


check(
    "Promotion effectiveness IDs are unique",
    promotion_effectiveness.groupBy(
        "promotion_id"
    ).count().filter(
        F.col("count") > 1
    ).count() == 0
)


# ==================== SRS COVERAGE ====================


required_columns = {
    "promotion_order_count",
    "promotion_revenue",
    "promotion_contribution_margin",
    "acquired_customers",
    "repeat_purchase_customers",
    "repeat_purchase_rate_pct",
    "promotion_average_order_value",
    "pre_estimated_wastage_cost",
    "during_estimated_wastage_cost",
    "post_estimated_wastage_cost",
    "post_promotion_return_customers",
    "post_promotion_return_rate_pct",
    "post_target_item_repeat_customers",
    "post_target_item_repeat_rate_pct",
    "pre_target_demand",
    "during_target_demand",
    "post_target_demand",
    "promotion_assessment"
}


check(
    "All Step 27 SRS dimensions are present",
    required_columns.issubset(
        set(
            promotion_effectiveness.columns
        )
    )
)


# ==================== BASIC VALUES ====================


for column in [
    "promotion_order_count",
    "promotion_customer_count",
    "promotion_revenue",
    "promotion_discount_amount",
    "acquired_customers",
    "repeat_purchase_customers",
    "post_promotion_return_customers",
    "post_target_item_repeat_customers",
    "pre_estimated_wastage_cost",
    "during_estimated_wastage_cost",
    "post_estimated_wastage_cost"
]:
    check(
        f"{column} is non-negative",
        promotion_effectiveness.filter(
            F.col(column) < 0
        ).count() == 0
    )


for column in [
    "customer_acquisition_rate_pct",
    "repeat_purchase_rate_pct",
    "post_promotion_return_rate_pct",
    "post_target_item_repeat_rate_pct"
]:
    check(
        f"{column} stays between zero and 100",
        promotion_effectiveness.filter(
            (
                F.col(column) < 0
            ) |
            (
                F.col(column) > 100
            )
        ).count() == 0
    )


check(
    "Acquired customers do not exceed promotion customers",
    promotion_effectiveness.filter(
        F.col("acquired_customers") >
        F.col("promotion_customers")
    ).count() == 0
)


check(
    "Repeat customers do not exceed promotion customers",
    promotion_effectiveness.filter(
        F.col("repeat_purchase_customers") >
        F.col("promotion_customers")
    ).count() == 0
)


check(
    "Post-promotion return customers do not exceed promotion customers",
    promotion_effectiveness.filter(
        F.col(
            "post_promotion_return_customers"
        ) >
        F.col(
            "promotion_customers"
        )
    ).count() == 0
)


check(
    "Post-target repeat customers do not exceed promotion customers",
    promotion_effectiveness.filter(
        F.col(
            "post_target_item_repeat_customers"
        ) >
        F.col(
            "promotion_customers"
        )
    ).count() == 0
)


# ==================== WINDOW VALIDATION ====================


check(
    "Promotion windows have positive campaign duration",
    promotion_effectiveness.filter(
        F.col("promotion_days") <= 0
    ).count() == 0
)


check(
    "Pre-window days are non-negative",
    promotion_effectiveness.filter(
        F.col("pre_days") < 0
    ).count() == 0
)


check(
    "Post-window days are non-negative",
    promotion_effectiveness.filter(
        F.col("post_days") < 0
    ).count() == 0
)


check(
    "Pre window ends before promotion starts",
    promotion_effectiveness.filter(
        F.col("pre_days") > 0
    ).filter(
        F.col("pre_end_date") >=
        F.col("start_date")
    ).count() == 0
)


check(
    "Post window starts after promotion ends",
    promotion_effectiveness.filter(
        F.col("post_days") > 0
    ).filter(
        F.col("post_start_date") <=
        F.col("end_date")
    ).count() == 0
)


# ==================== CUSTOMER BEHAVIOR ====================


check(
    "Promotion customer rows are unique by promotion and customer",
    promotion_customer_behavior.groupBy(
        "promotion_id",
        "customer_id"
    ).count().filter(
        F.col("count") > 1
    ).count() == 0
)


check(
    "Acquired-customer flag is boolean",
    promotion_customer_behavior.filter(
        F.col("acquired_customer").isNull()
    ).count() == 0
)


check(
    "Post-promotion order counts are non-negative",
    promotion_customer_behavior.filter(
        F.col("post_promotion_orders") < 0
    ).count() == 0
)


check(
    "Post-target order counts are non-negative",
    promotion_customer_behavior.filter(
        F.col("post_target_item_orders") < 0
    ).count() == 0
)


# ==================== MULTI-KPI EFFECTIVENESS ====================


valid_assessments = {
    "Effective",
    "Mixed",
    "Ineffective",
    "No Usage"
}


found_assessments = {
    row["promotion_assessment"]
    for row in promotion_effectiveness.select(
        "promotion_assessment"
    ).distinct().collect()
}


check(
    "Promotion assessments use documented classes",
    found_assessments.issubset(
        valid_assessments
    ),
    f"Found: {sorted(found_assessments)}"
)


check(
    "Positive KPI count stays between zero and eight",
    promotion_effectiveness.filter(
        (
            F.col("positive_kpi_count") < 0
        ) |
        (
            F.col("positive_kpi_count") > 8
        )
    ).count() == 0
)


check(
    "No-usage promotions have zero promotion orders",
    promotion_effectiveness.filter(
        F.col("promotion_assessment") ==
        "No Usage"
    ).filter(
        F.col("promotion_order_count") != 0
    ).count() == 0
)


check(
    "Used promotions are not marked No Usage",
    promotion_effectiveness.filter(
        F.col("promotion_order_count") > 0
    ).filter(
        F.col("promotion_assessment") ==
        "No Usage"
    ).count() == 0
)


check(
    "Effective promotions require at least five positive KPIs",
    promotion_effectiveness.filter(
        F.col("promotion_assessment") ==
        "Effective"
    ).filter(
        F.col("positive_kpi_count") < 5
    ).count() == 0
)


check(
    "Effective promotions require positive margin lift",
    promotion_effectiveness.filter(
        F.col("promotion_assessment") ==
        "Effective"
    ).filter(
        F.coalesce(
            F.col("margin_lift_pct"),
            F.lit(-999.0)
        ) <= 0
    ).count() == 0
)


check(
    "Effective promotions cannot increase wastage",
    promotion_effectiveness.filter(
        F.col("promotion_assessment") ==
        "Effective"
    ).filter(
        F.col("wastage_change_pct") > 0
    ).count() == 0
)


check(
    "Sales growth alone cannot make a promotion Effective",
    promotion_effectiveness.filter(
        F.col("promotion_assessment") ==
        "Effective"
    ).filter(
        (
            F.coalesce(
                F.col("demand_lift_pct"),
                F.lit(0.0)
            ) > 0
        ) &
        (
            F.coalesce(
                F.col("margin_lift_pct"),
                F.lit(-999.0)
            ) <= 0
        )
    ).count() == 0
)


# ==================== SUMMARY ====================


summary_count = effectiveness_summary.agg(
    F.sum(
        "promotion_count"
    ).alias(
        "total"
    )
).first()["total"]


check(
    "Effectiveness summary reconciles to promotion output",
    int(summary_count) ==
    promotion_effectiveness.count()
)


# ==================== METHODOLOGY ====================


metadata_components = {
    row["analysis_component"]
    for row in analysis_metadata.select(
        "analysis_component"
    ).distinct().collect()
}


for component in [
    "comparison_windows",
    "order_volume",
    "revenue_margin",
    "customer_acquisition",
    "repeat_purchase",
    "average_order_value",
    "wastage",
    "effectiveness",
    "causality"
]:
    check(
        f"Methodology documented: {component}",
        component in
        metadata_components
    )


# ==================== FINAL ====================


total_checks = passed + failed


print(
    f"\n========VALIDATION RESULT: "
    f"{passed}/{total_checks} PASS========"
)


if failed == 0:
    print(
        "\nPromotion effectiveness validation PASSED."
    )
else:
    print(
        "\nPromotion effectiveness validation FAILED."
    )


spark.stop()
