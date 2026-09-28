from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import INTEGRATED_DATA_FOLDER, ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("Validate DineIQ Price Intelligence") \
    .getOrCreate()


integrated_folder = INTEGRATED_DATA_FOLDER
output_folder = f"{ANALYTICS_DATA_FOLDER}/price_intelligence"


pricing_history = spark.read.parquet(
    f"{integrated_folder}/pricing_history"
)


price_level_performance = spark.read.parquet(
    f"{output_folder}/price_level_performance"
)

global_price_relationships = spark.read.parquet(
    f"{output_folder}/global_price_relationships"
)

item_price_relationships = spark.read.parquet(
    f"{output_folder}/item_price_relationships"
)

price_change_events = spark.read.parquet(
    f"{output_folder}/price_change_events"
)

significant_response_items = spark.read.parquet(
    f"{output_folder}/significant_response_items"
)

price_intelligence_summary = spark.read.parquet(
    f"{output_folder}/price_intelligence_summary"
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


print("\n========PRICE INTELLIGENCE VALIDATION========")


# ==================== REQUIRED OUTPUTS ====================


outputs = [
    ("Price-level performance", price_level_performance),
    ("Global price relationships", global_price_relationships),
    ("Item price relationships", item_price_relationships),
    ("Price-change events", price_change_events),
    ("Significant response items", significant_response_items),
    ("Price intelligence summary", price_intelligence_summary),
    ("Analysis metadata", analysis_metadata)
]


for name, frame in outputs:
    check(
        f"{name} contains data",
        frame.count() > 0
    )


# ==================== SRS RELATIONSHIP COVERAGE ====================


required_price_level_columns = {
    "observed_price",
    "demand_quantity",
    "average_daily_demand",
    "revenue",
    "contribution_margin",
    "discount_percentage",
    "average_rating",
    "repeat_purchase_rate"
}


check(
    "Price-level analysis covers all required SRS relationships",
    required_price_level_columns.issubset(
        set(
            price_level_performance.columns
        )
    )
)


required_relationship_columns = {
    "price_demand_correlation",
    "price_revenue_correlation",
    "price_margin_correlation",
    "price_discount_correlation",
    "price_rating_correlation",
    "price_repeat_purchase_correlation"
}


check(
    "Global relationship output contains all required relationships",
    required_relationship_columns.issubset(
        set(
            global_price_relationships.columns
        )
    )
)


# ==================== PRICE CHANGE COVERAGE ====================


source_price_change_count = pricing_history.select(
    "price_history_id"
).distinct().count()


analysis_price_change_count = price_change_events.select(
    "price_history_id"
).distinct().count()


check(
    "Every historical price change is analyzed",
    source_price_change_count ==
    analysis_price_change_count,
    (
        f"Source: {source_price_change_count}, "
        f"analyzed: {analysis_price_change_count}"
    )
)


check(
    "Price-change IDs remain unique",
    price_change_events.groupBy(
        "price_history_id"
    ).count().filter(
        F.col("count") > 1
    ).count() == 0
)


required_event_columns = {
    "old_price",
    "new_price",
    "price_change_pct",
    "effective_date",
    "pre_daily_demand",
    "post_daily_demand",
    "demand_change_pct",
    "pre_revenue_per_day",
    "post_revenue_per_day",
    "revenue_change_pct",
    "pre_margin_per_day",
    "post_margin_per_day",
    "margin_change_pct",
    "pre_discount_percentage",
    "post_discount_percentage",
    "pre_average_rating",
    "post_average_rating",
    "pre_repeat_purchase_rate",
    "post_repeat_purchase_rate",
    "price_elasticity_proxy",
    "significant_demand_change",
    "demand_change_direction"
}


check(
    "Price-change events contain complete before/after evidence",
    required_event_columns.issubset(
        set(
            price_change_events.columns
        )
    )
)


# ==================== VALUE VALIDATION ====================


check(
    "Observed prices are positive",
    price_level_performance.filter(
        F.col(
            "observed_price"
        ) <= 0
    ).count() == 0
)


check(
    "Demand quantities are non-negative",
    price_level_performance.filter(
        F.col(
            "demand_quantity"
        ) < 0
    ).count() == 0
)


check(
    "Revenue is non-negative",
    price_level_performance.filter(
        F.col(
            "revenue"
        ) < 0
    ).count() == 0
)


check(
    "Discount percentages are between zero and 100",
    price_level_performance.filter(
        (
            F.col(
                "discount_percentage"
            ) < 0
        ) |
        (
            F.col(
                "discount_percentage"
            ) > 100
        )
    ).count() == 0
)


check(
    "Repeat-purchase rates are between zero and 100",
    price_level_performance.filter(
        (
            F.col(
                "repeat_purchase_rate"
            ) < 0
        ) |
        (
            F.col(
                "repeat_purchase_rate"
            ) > 100
        )
    ).count() == 0
)


check(
    "Average ratings are valid when present",
    price_level_performance.filter(
        F.col(
            "average_rating"
        ).isNotNull() &
        (
            (
                F.col(
                    "average_rating"
                ) < 1
            ) |
            (
                F.col(
                    "average_rating"
                ) > 5
            )
        )
    ).count() == 0
)


check(
    "Historical old prices are positive",
    price_change_events.filter(
        F.col(
            "old_price"
        ) <= 0
    ).count() == 0
)


check(
    "Historical new prices are positive",
    price_change_events.filter(
        F.col(
            "new_price"
        ) <= 0
    ).count() == 0
)


check(
    "Pre and post window lengths are positive",
    price_change_events.filter(
        (
            F.col(
                "pre_window_days"
            ) <= 0
        ) |
        (
            F.col(
                "post_window_days"
            ) <= 0
        )
    ).count() == 0
)


check(
    "Pre and post demand are non-negative",
    price_change_events.filter(
        (
            F.col(
                "pre_demand_quantity"
            ) < 0
        ) |
        (
            F.col(
                "post_demand_quantity"
            ) < 0
        )
    ).count() == 0
)


check(
    "Pre and post repeat-purchase rates are valid",
    price_change_events.filter(
        (
            F.col(
                "pre_repeat_purchase_rate"
            ) < 0
        ) |
        (
            F.col(
                "pre_repeat_purchase_rate"
            ) > 100
        ) |
        (
            F.col(
                "post_repeat_purchase_rate"
            ) < 0
        ) |
        (
            F.col(
                "post_repeat_purchase_rate"
            ) > 100
        )
    ).count() == 0
)


check(
    "Pre and post ratings are valid when present",
    price_change_events.filter(
        (
            F.col(
                "pre_average_rating"
            ).isNotNull() &
            (
                (
                    F.col(
                        "pre_average_rating"
                    ) < 1
                ) |
                (
                    F.col(
                        "pre_average_rating"
                    ) > 5
                )
            )
        ) |
        (
            F.col(
                "post_average_rating"
            ).isNotNull() &
            (
                (
                    F.col(
                        "post_average_rating"
                    ) < 1
                ) |
                (
                    F.col(
                        "post_average_rating"
                    ) > 5
                )
            )
        )
    ).count() == 0
)


# ==================== SIGNIFICANT DEMAND CHANGE ====================


summary_row = price_intelligence_summary.first()


significant_threshold = float(
    summary_row[
        "significant_demand_threshold_pct"
    ]
)


check(
    "Significant demand threshold is at least 10 percent",
    significant_threshold >= 10.0,
    f"Found: {significant_threshold}"
)


check(
    "Evaluable price changes exist",
    int(
        summary_row[
            "evaluable_price_change_events"
        ]
    ) > 0
)


check(
    "Significant demand-change events exist",
    int(
        summary_row[
            "significant_demand_change_events"
        ]
    ) > 0
)


check(
    "Significant events satisfy the selected threshold",
    price_change_events.filter(
        F.col(
            "significant_demand_change"
        ) &
        (
            F.col(
                "absolute_demand_change_pct"
            ) <
            F.col(
                "significant_demand_threshold_pct"
            )
        )
    ).count() == 0
)


check(
    "Non-evaluable events are never marked significant",
    price_change_events.filter(
        F.col(
            "significant_demand_change"
        ) &
        (
            ~F.col(
                "evaluable_change"
            )
        )
    ).count() == 0
)


check(
    "Significant-response item output matches significant events",
    significant_response_items.select(
        "item_id"
    ).distinct().count() ==
    price_change_events.filter(
        F.col(
            "significant_demand_change"
        )
    ).select(
        "item_id"
    ).distinct().count()
)


# ==================== CORRELATION VALIDATION ====================


relationship_row = global_price_relationships.first()


for column in required_relationship_columns:
    value = relationship_row[column]

    check(
        f"{column} is valid when available",
        (
            value is None or
            (
                -1.0 <=
                float(value) <=
                1.0
            )
        ),
        f"Found: {value}"
    )


check(
    "Items with price relationships have multiple observed price levels",
    item_price_relationships.filter(
        F.col(
            "observed_price_levels"
        ) < 2
    ).count() == 0
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
    "price_relationships",
    "price_change_windows",
    "isolated_price_changes",
    "significant_demand_change",
    "price_elasticity_proxy",
    "ratings"
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
        "\nPrice intelligence validation PASSED."
    )
else:
    print(
        "\nPrice intelligence validation FAILED."
    )


spark.stop()
