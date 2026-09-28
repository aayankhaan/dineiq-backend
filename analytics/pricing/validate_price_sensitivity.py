from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("Validate DineIQ Price Sensitivity") \
    .getOrCreate()


price_intelligence_folder = f"{ANALYTICS_DATA_FOLDER}/price_intelligence"
output_folder = f"{ANALYTICS_DATA_FOLDER}/price_sensitivity"


price_change_events = spark.read.parquet(
    f"{price_intelligence_folder}/price_change_events"
)

item_price_sensitivity = spark.read.parquet(
    f"{output_folder}/item_price_sensitivity"
)

sensitivity_summary = spark.read.parquet(
    f"{output_folder}/sensitivity_summary"
)

sensitivity_thresholds = spark.read.parquet(
    f"{output_folder}/sensitivity_thresholds"
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


print("\n========PRICE SENSITIVITY VALIDATION========")


# ==================== REQUIRED OUTPUTS ====================


outputs = [
    ("Item price sensitivity", item_price_sensitivity),
    ("Sensitivity summary", sensitivity_summary),
    ("Sensitivity thresholds", sensitivity_thresholds),
    ("Analysis metadata", analysis_metadata)
]


for name, frame in outputs:
    check(
        f"{name} contains data",
        frame.count() > 0
    )


# ==================== ITEM COVERAGE ====================


source_item_count = price_change_events.select(
    "item_id"
).distinct().count()


analysis_item_count = item_price_sensitivity.select(
    "item_id"
).distinct().count()


check(
    "Every item with pricing history is represented",
    source_item_count ==
    analysis_item_count,
    (
        f"Source: {source_item_count}, "
        f"analysis: {analysis_item_count}"
    )
)


check(
    "Item sensitivity IDs are unique",
    item_price_sensitivity.groupBy(
        "item_id"
    ).count().filter(
        F.col("count") > 1
    ).count() == 0
)


# ==================== SRS CLASSIFICATION ====================


valid_classes = {
    "Highly Price Sensitive",
    "Moderately Price Sensitive",
    "Low Price Sensitivity"
}


suitable_classes = {
    row[
        "price_sensitivity_class"
    ]
    for row in item_price_sensitivity.filter(
        F.col(
            "suitable_for_price_sensitivity"
        )
    ).select(
        "price_sensitivity_class"
    ).distinct().collect()
}


check(
    "Suitable items use only required SRS classes",
    suitable_classes.issubset(
        valid_classes
    ),
    f"Found: {sorted(suitable_classes)}"
)


check(
    "All three SRS sensitivity classes are represented",
    suitable_classes ==
    valid_classes,
    f"Found: {sorted(suitable_classes)}"
)


check(
    "Unsuitable items are marked as insufficient history",
    item_price_sensitivity.filter(
        ~F.col(
            "suitable_for_price_sensitivity"
        ) &
        (
            F.col(
                "price_sensitivity_class"
            ) !=
            "Insufficient Price History"
        )
    ).count() == 0
)


# ==================== EVIDENCE VALIDATION ====================


required_evidence_columns = {
    "total_price_change_events",
    "evaluable_price_change_events",
    "significant_demand_change_events",
    "expected_direction_events",
    "evaluable_locations",
    "average_absolute_price_change_pct",
    "median_capped_absolute_elasticity",
    "median_absolute_demand_change_pct",
    "significant_response_rate_pct",
    "expected_direction_rate_pct",
    "price_sensitivity_score",
    "evidence_strength",
    "evidence_summary"
}


check(
    "Historical price and order evidence is retained",
    required_evidence_columns.issubset(
        set(
            item_price_sensitivity.columns
        )
    )
)


threshold_row = sensitivity_thresholds.first()


minimum_events = int(
    threshold_row[
        "minimum_evaluable_events"
    ]
)


elasticity_cap = float(
    threshold_row[
        "absolute_elasticity_cap"
    ]
)


check(
    "Suitable items meet minimum historical evidence",
    item_price_sensitivity.filter(
        F.col(
            "suitable_for_price_sensitivity"
        ) &
        (
            F.col(
                "evaluable_price_change_events"
            ) <
            F.lit(
                minimum_events
            )
        )
    ).count() == 0
)


check(
    "Insufficient items remain below minimum evidence",
    item_price_sensitivity.filter(
        ~F.col(
            "suitable_for_price_sensitivity"
        ) &
        (
            F.col(
                "evaluable_price_change_events"
            ) >=
            F.lit(
                minimum_events
            )
        )
    ).count() == 0
)


check(
    "Median capped elasticity respects extreme-value cap",
    item_price_sensitivity.filter(
        F.col(
            "median_capped_absolute_elasticity"
        ) >
        F.lit(
            elasticity_cap
        ) +
        F.lit(
            0.0001
        )
    ).count() == 0
)


check(
    "Sensitivity scores are non-negative",
    item_price_sensitivity.filter(
        F.col(
            "price_sensitivity_score"
        ) < 0
    ).count() == 0
)


for column in [
    "significant_response_rate_pct",
    "expected_direction_rate_pct",
    "expected_significant_rate_pct"
]:
    check(
        f"{column} stays between zero and 100",
        item_price_sensitivity.filter(
            (
                F.col(
                    column
                ) < 0
            ) |
            (
                F.col(
                    column
                ) > 100
            )
        ).count() == 0
    )


check(
    "Significant event counts do not exceed evaluable events",
    item_price_sensitivity.filter(
        F.col(
            "significant_demand_change_events"
        ) >
        F.col(
            "evaluable_price_change_events"
        )
    ).count() == 0
)


check(
    "Expected-direction event counts do not exceed evaluable events",
    item_price_sensitivity.filter(
        F.col(
            "expected_direction_events"
        ) >
        F.col(
            "evaluable_price_change_events"
        )
    ).count() == 0
)


# ==================== SOURCE EVENT RECONCILIATION ====================


source_evaluable_count = price_change_events.filter(
    F.col("evaluable_change") &
    F.col("price_elasticity_proxy").isNotNull() &
    F.col("demand_change_pct").isNotNull()
).count()


aggregated_evaluable_count = item_price_sensitivity.agg(
    F.sum(
        "evaluable_price_change_events"
    ).alias(
        "total"
    )
).first()["total"]


check(
    "Aggregated evaluable events reconcile to Step 25",
    int(
        aggregated_evaluable_count
    ) ==
    int(
        source_evaluable_count
    ),
    (
        f"Step 25: {source_evaluable_count}, "
        f"Step 26: {aggregated_evaluable_count}"
    )
)


# ==================== THRESHOLD VALIDATION ====================


low_threshold = float(
    threshold_row[
        "low_sensitivity_threshold"
    ]
)


high_threshold = float(
    threshold_row[
        "high_sensitivity_threshold"
    ]
)


check(
    "High sensitivity threshold is not below low threshold",
    high_threshold >=
    low_threshold
)


check(
    "Highly sensitive items meet high threshold",
    item_price_sensitivity.filter(
        F.col(
            "price_sensitivity_class"
        ) ==
        "Highly Price Sensitive"
    ).filter(
        F.col(
            "price_sensitivity_score"
        ) <
        F.lit(
            high_threshold
        )
    ).count() == 0
)


check(
    "Moderately sensitive items fall between thresholds",
    item_price_sensitivity.filter(
        F.col(
            "price_sensitivity_class"
        ) ==
        "Moderately Price Sensitive"
    ).filter(
        (
            F.col(
                "price_sensitivity_score"
            ) <
            F.lit(
                low_threshold
            )
        ) |
        (
            F.col(
                "price_sensitivity_score"
            ) >=
            F.lit(
                high_threshold
            )
        )
    ).count() == 0
)


check(
    "Low-sensitivity items remain below low threshold",
    item_price_sensitivity.filter(
        F.col(
            "price_sensitivity_class"
        ) ==
        "Low Price Sensitivity"
    ).filter(
        F.col(
            "price_sensitivity_score"
        ) >=
        F.lit(
            low_threshold
        )
    ).count() == 0
)


# ==================== SUMMARY VALIDATION ====================


summary_item_count = sensitivity_summary.agg(
    F.sum(
        "item_count"
    ).alias(
        "total"
    )
).first()["total"]


check(
    "Sensitivity summary reconciles to item output",
    int(
        summary_item_count
    ) ==
    item_price_sensitivity.count()
)


# ==================== METHODOLOGY VALIDATION ====================


metadata_components = {
    row[
        "analysis_component"
    ]
    for row in analysis_metadata.select(
        "analysis_component"
    ).distinct().collect()
}


for component in [
    "suitable_items",
    "elasticity",
    "direction_consistency",
    "sensitivity_score",
    "classification",
    "interpretation"
]:
    check(
        f"Methodology documented: {component}",
        component in
        metadata_components
    )


# ==================== FINAL RESULT ====================


total_checks = passed + failed


print(
    f"\n========VALIDATION RESULT: "
    f"{passed}/{total_checks} PASS========"
)


if failed == 0:
    print(
        "\nPrice-sensitivity validation PASSED."
    )
else:
    print(
        "\nPrice-sensitivity validation FAILED."
    )


spark.stop()
