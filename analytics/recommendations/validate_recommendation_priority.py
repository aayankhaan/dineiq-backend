from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.settings import ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("Validate DineIQ Recommendation Priority") \
    .getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")


recommendations = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/recommendations/recommendations"
)

priority = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/recommendation_priority/prioritized_recommendations"
)

summary = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/recommendation_priority/priority_summary"
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


print("\n========RECOMMENDATION PRIORITY VALIDATION========")


check(
    "Prioritized recommendation output contains data",
    priority.count() > 0
)


check(
    "Priority summary contains data",
    summary.count() > 0
)


check(
    "Every Step 37 recommendation is prioritized",
    priority.count() ==
    recommendations.count()
)


check(
    "Recommendation IDs remain unique",
    priority.select(
        "recommendation_id"
    ).distinct().count() ==
    priority.count()
)


check(
    "No Step 37 recommendation is missing",
    recommendations.join(
        priority.select(
            "recommendation_id"
        ),
        "recommendation_id",
        "left_anti"
    ).count() == 0
)


required_columns = {
    "recommendation_id",
    "impact_value",
    "impact_unit",
    "impact_basis",
    "priority_rank_within_type",
    "impact_percentile",
    "priority_score",
    "priority",
    "priority_method",
    "priority_reason"
}


check(
    "Required Step 39 priority fields exist",
    required_columns.issubset(
        set(priority.columns)
    )
)


allowed_priorities = {
    "Low",
    "Medium",
    "High",
    "Critical"
}


actual_priorities = {
    row["priority"]
    for row in priority.select(
        "priority"
    ).distinct().collect()
}


check(
    "Only allowed priority levels are used",
    actual_priorities.issubset(
        allowed_priorities
    )
)


check(
    "All four priority levels are represented",
    allowed_priorities.issubset(
        actual_priorities
    )
)


check(
    "Priority scores stay between zero and 100",
    priority.filter(
        (
            F.col("priority_score") < 0
        ) |
        (
            F.col("priority_score") > 100
        )
    ).count() == 0
)


check(
    "Impact percentiles stay between zero and one",
    priority.filter(
        (
            F.col("impact_percentile") < 0
        ) |
        (
            F.col("impact_percentile") > 1
        )
    ).count() == 0
)


check(
    "Impact values are populated",
    priority.filter(
        F.col("impact_value").isNull()
    ).count() == 0
)


check(
    "Impact units are populated",
    priority.filter(
        F.col("impact_unit").isNull() |
        (F.trim("impact_unit") == "")
    ).count() == 0
)


check(
    "Impact basis is populated",
    priority.filter(
        F.col("impact_basis").isNull() |
        (F.trim("impact_basis") == "")
    ).count() == 0
)


check(
    "Priority reasons are populated",
    priority.filter(
        F.col("priority_reason").isNull() |
        (F.trim("priority_reason") == "")
    ).count() == 0
)


check(
    "Priority method is populated",
    priority.filter(
        F.col("priority_method").isNull() |
        (F.trim("priority_method") == "")
    ).count() == 0
)


check(
    "Only recommendations with complete Step 38 evidence are prioritized",
    priority.filter(
        ~F.col("evidence_complete")
    ).count() == 0
)


check(
    "Priority ranks are positive",
    priority.filter(
        F.col("priority_rank_within_type") < 1
    ).count() == 0
)


check(
    "Priority ranks do not exceed recommendation type counts",
    priority.filter(
        F.col("priority_rank_within_type") >
        F.col("recommendation_type_count")
    ).count() == 0
)


check(
    "Critical priority uses top-impact recommendations",
    priority.filter(
        F.col("priority") == "Critical"
    ).filter(
        F.col("impact_percentile") < 0.90
    ).count() == 0
)


check(
    "High priority follows the configured impact range",
    priority.filter(
        F.col("priority") == "High"
    ).filter(
        (
            F.col("impact_percentile") < 0.65
        ) |
        (
            F.col("impact_percentile") >= 0.90
        )
    ).count() == 0
)


check(
    "Medium priority follows the configured impact range",
    priority.filter(
        F.col("priority") == "Medium"
    ).filter(
        (
            F.col("impact_percentile") < 0.35
        ) |
        (
            F.col("impact_percentile") >= 0.65
        )
    ).count() == 0
)


check(
    "Low priority follows the configured impact range",
    priority.filter(
        F.col("priority") == "Low"
    ).filter(
        F.col("impact_percentile") >= 0.35
    ).count() == 0
)


top_impact = priority.groupBy(
    "recommendation_type"
).agg(
    F.max(
        "impact_value"
    ).alias("maximum_impact_value")
)


check(
    "Highest-impact recommendation in every type is Critical",
    priority.join(
        top_impact,
        "recommendation_type",
        "inner"
    ).filter(
        F.col("impact_value") ==
        F.col("maximum_impact_value")
    ).filter(
        F.col("priority") !=
        "Critical"
    ).count() == 0
)


impact_window = Window.partitionBy(
    "recommendation_type"
).orderBy(
    F.desc("impact_value"),
    F.asc("recommendation_id")
)


ordered = priority.withColumn(
    "previous_impact_value",
    F.lag(
        "impact_value"
    ).over(
        impact_window
    )
).withColumn(
    "previous_priority_score",
    F.lag(
        "priority_score"
    ).over(
        impact_window
    )
)


check(
    "Lower business impact never receives a higher priority score within a type",
    ordered.filter(
        F.col("previous_impact_value").isNotNull()
    ).filter(
        (
            F.col("impact_value") <
            F.col("previous_impact_value")
        ) &
        (
            F.col("priority_score") >
            F.col("previous_priority_score")
        )
    ).count() == 0
)


unit_counts = priority.groupBy(
    "recommendation_type"
).agg(
    F.countDistinct(
        "impact_unit"
    ).alias("impact_unit_count")
)


check(
    "Each recommendation type uses one comparable impact unit",
    unit_counts.filter(
        F.col("impact_unit_count") != 1
    ).count() == 0
)


summary_total = summary.agg(
    F.sum(
        "recommendation_count"
    ).alias("value")
).first()["value"]


check(
    "Priority summary reconciles to prioritized recommendations",
    int(summary_total) ==
    priority.count()
)


check(
    "Priority summary has one row per priority level",
    summary.select(
        "priority"
    ).distinct().count() ==
    summary.count()
)


check(
    "Priority summary percentages total approximately 100",
    abs(
        float(
            summary.agg(
                F.sum(
                    "recommendation_percentage"
                ).alias("value")
            ).first()["value"]
        ) - 100.0
    ) <= 0.05
)


total = passed + failed


print(
    f"\n========VALIDATION RESULT: "
    f"{passed}/{total} PASS========"
)


if failed == 0:
    print(
        "\nRecommendation priority validation PASSED."
    )
else:
    print(
        "\nRecommendation priority validation FAILED."
    )


spark.stop()
