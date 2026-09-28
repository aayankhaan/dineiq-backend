from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("Validate DineIQ Recommendation Evidence") \
    .getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")


recommendations = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/recommendations/recommendations"
)

evidence = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/recommendation_evidence/recommendation_evidence"
)

summary = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/recommendation_evidence/evidence_coverage_summary"
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


print("\n========RECOMMENDATION EVIDENCE VALIDATION========")


check(
    "Recommendation evidence output contains data",
    evidence.count() > 0
)


check(
    "Evidence coverage summary contains data",
    summary.count() > 0
)


check(
    "Every Step 37 recommendation has evidence",
    evidence.select(
        "recommendation_id"
    ).distinct().count() ==
    recommendations.select(
        "recommendation_id"
    ).distinct().count()
)


check(
    "No recommendation is lost during evidence generation",
    recommendations.join(
        evidence.select(
            "recommendation_id"
        ).distinct(),
        "recommendation_id",
        "left_anti"
    ).count() == 0
)


required_columns = {
    "recommendation_id",
    "recommended_action",
    "evidence_order",
    "evidence_category",
    "evidence_label",
    "evidence_value_text",
    "evidence_statement",
    "evidence_count",
    "available_evidence_count",
    "evidence_complete"
}


check(
    "Required evidence fields exist",
    required_columns.issubset(
        set(evidence.columns)
    )
)


check(
    "Every recommendation has at least two evidence points",
    evidence.select(
        "recommendation_id",
        "evidence_count"
    ).distinct().filter(
        F.col("evidence_count") < 2
    ).count() == 0
)


check(
    "Every recommendation has at least two available evidence points",
    evidence.select(
        "recommendation_id",
        "available_evidence_count"
    ).distinct().filter(
        F.col("available_evidence_count") < 2
    ).count() == 0
)


check(
    "Every recommendation is marked evidence complete",
    evidence.filter(
        ~F.col("evidence_complete")
    ).count() == 0
)


check(
    "Evidence orders start at one",
    evidence.groupBy(
        "recommendation_id"
    ).agg(
        F.min("evidence_order").alias("minimum_order")
    ).filter(
        F.col("minimum_order") != 1
    ).count() == 0
)


check(
    "Evidence orders contain no duplicates per recommendation",
    evidence.groupBy(
        "recommendation_id",
        "evidence_order"
    ).count().filter(
        F.col("count") != 1
    ).count() == 0
)


check(
    "Evidence labels are populated",
    evidence.filter(
        F.col("evidence_label").isNull() |
        (F.trim("evidence_label") == "")
    ).count() == 0
)


check(
    "Evidence statements are populated",
    evidence.filter(
        F.col("evidence_statement").isNull() |
        (F.trim("evidence_statement") == "")
    ).count() == 0
)


check(
    "Recommended actions are retained",
    evidence.filter(
        F.col("recommended_action").isNull() |
        (F.trim("recommended_action") == "")
    ).count() == 0
)


source_actions = recommendations.select(
    "recommendation_id",
    F.col(
        "recommended_action"
    ).alias(
        "source_recommended_action"
    )
)


check(
    "Recommended actions match Step 37",
    evidence.join(
        source_actions,
        "recommendation_id",
        "inner"
    ).filter(
        F.col("recommended_action") !=
        F.col("source_recommended_action")
    ).count() == 0
)


source_types = recommendations.select(
    "recommendation_id",
    F.col(
        "recommendation_type"
    ).alias(
        "source_recommendation_type"
    ),
    F.col(
        "source_analysis"
    ).alias(
        "source_analysis_expected"
    )
)


check(
    "Evidence retains the original recommendation type and analysis source",
    evidence.join(
        source_types,
        "recommendation_id",
        "inner"
    ).filter(
        (
            F.col("recommendation_type") !=
            F.col("source_recommendation_type")
        ) |
        (
            F.col("source_analysis") !=
            F.col("source_analysis_expected")
        )
    ).count() == 0
)


summary_total = summary.agg(
    F.sum(
        "recommendation_count"
    ).alias("value")
).first()["value"]


check(
    "Coverage summary recommendation count reconciles to Step 37",
    int(summary_total) ==
    recommendations.count()
)


summary_evidence_total = summary.agg(
    F.sum(
        "evidence_point_count"
    ).alias("value")
).first()["value"]


check(
    "Coverage summary evidence count reconciles",
    int(summary_evidence_total) ==
    evidence.count()
)


check(
    "Evidence availability stays between zero and 100 percent",
    summary.filter(
        (
            F.col("evidence_availability_pct") < 0
        ) |
        (
            F.col("evidence_availability_pct") > 100
        )
    ).count() == 0
)


check(
    "Every recommendation type is represented in evidence summary",
    summary.select(
        "recommendation_type"
    ).distinct().count() ==
    recommendations.select(
        "recommendation_type"
    ).distinct().count()
)


check(
    "Recommendation shares total approximately 100 percent",
    abs(
        float(
            summary.agg(
                F.sum(
                    "recommendation_share_pct"
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
        "\nRecommendation evidence validation PASSED."
    )
else:
    print(
        "\nRecommendation evidence validation FAILED."
    )


spark.stop()
