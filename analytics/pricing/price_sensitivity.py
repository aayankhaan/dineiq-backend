from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("DineIQ Price Sensitivity") \
    .getOrCreate()


price_intelligence_folder = f"{ANALYTICS_DATA_FOLDER}/price_intelligence"
output_folder = f"{ANALYTICS_DATA_FOLDER}/price_sensitivity"

MIN_EVALUABLE_EVENTS = 2
ELASTICITY_CAP = 5.0


price_change_events = spark.read.parquet(
    f"{price_intelligence_folder}/price_change_events"
)


# ==================== EVENT-LEVEL SENSITIVITY EVIDENCE ====================


event_evidence = price_change_events.withColumn(
    "expected_demand_direction",
    (
        F.col("price_change_pct") *
        F.col("demand_change_pct")
    ) < 0
).withColumn(
    "absolute_price_elasticity",
    F.abs(
        F.col("price_elasticity_proxy")
    )
).withColumn(
    "capped_absolute_price_elasticity",
    F.least(
        F.col("absolute_price_elasticity"),
        F.lit(ELASTICITY_CAP)
    )
)


evaluable_events = event_evidence.filter(
    F.col("evaluable_change") &
    F.col("price_elasticity_proxy").isNotNull() &
    F.col("demand_change_pct").isNotNull()
)


# ==================== ITEM-LEVEL EVIDENCE ====================


item_evidence = event_evidence.groupBy(
    "item_id",
    "item_name",
    "category_id",
    "category_name"
).agg(
    F.countDistinct(
        "price_history_id"
    ).alias(
        "total_price_change_events"
    ),
    F.sum(
        F.when(
            F.col("evaluable_change") &
            F.col("price_elasticity_proxy").isNotNull() &
            F.col("demand_change_pct").isNotNull(),
            1
        ).otherwise(0)
    ).alias(
        "evaluable_price_change_events"
    ),
    F.sum(
        F.when(
            F.col("evaluable_change") &
            F.col("significant_demand_change"),
            1
        ).otherwise(0)
    ).alias(
        "significant_demand_change_events"
    ),
    F.sum(
        F.when(
            F.col("evaluable_change") &
            F.col("expected_demand_direction"),
            1
        ).otherwise(0)
    ).alias(
        "expected_direction_events"
    ),
    F.sum(
        F.when(
            F.col("evaluable_change") &
            F.col("significant_demand_change") &
            F.col("expected_demand_direction"),
            1
        ).otherwise(0)
    ).alias(
        "expected_significant_events"
    ),
    F.countDistinct(
        F.when(
            F.col("evaluable_change"),
            F.col("restaurant_id")
        )
    ).alias(
        "evaluable_locations"
    ),
    F.round(
        F.avg(
            F.when(
                F.col("evaluable_change"),
                F.abs(
                    F.col("price_change_pct")
                )
            )
        ),
        4
    ).alias(
        "average_absolute_price_change_pct"
    ),
    F.round(
        F.expr(
            """
            percentile_approx(
                CASE
                    WHEN evaluable_change = true
                    AND price_elasticity_proxy IS NOT NULL
                    THEN least(abs(price_elasticity_proxy), 5.0)
                END,
                0.5,
                1000
            )
            """
        ),
        4
    ).alias(
        "median_capped_absolute_elasticity"
    ),
    F.round(
        F.expr(
            """
            percentile_approx(
                CASE
                    WHEN evaluable_change = true
                    AND demand_change_pct IS NOT NULL
                    THEN abs(demand_change_pct)
                END,
                0.5,
                1000
            )
            """
        ),
        4
    ).alias(
        "median_absolute_demand_change_pct"
    ),
    F.round(
        F.avg(
            F.when(
                F.col("evaluable_change"),
                F.col("revenue_change_pct")
            )
        ),
        4
    ).alias(
        "average_revenue_change_pct"
    ),
    F.round(
        F.avg(
            F.when(
                F.col("evaluable_change"),
                F.col("margin_change_pct")
            )
        ),
        4
    ).alias(
        "average_margin_change_pct"
    )
).withColumn(
    "significant_response_rate_pct",
    F.when(
        F.col("evaluable_price_change_events") > 0,
        F.round(
            (
                F.col("significant_demand_change_events") /
                F.col("evaluable_price_change_events")
            ) * 100,
            2
        )
    ).otherwise(
        F.lit(0.0)
    )
).withColumn(
    "expected_direction_rate_pct",
    F.when(
        F.col("evaluable_price_change_events") > 0,
        F.round(
            (
                F.col("expected_direction_events") /
                F.col("evaluable_price_change_events")
            ) * 100,
            2
        )
    ).otherwise(
        F.lit(0.0)
    )
).withColumn(
    "expected_significant_rate_pct",
    F.when(
        F.col("evaluable_price_change_events") > 0,
        F.round(
            (
                F.col("expected_significant_events") /
                F.col("evaluable_price_change_events")
            ) * 100,
            2
        )
    ).otherwise(
        F.lit(0.0)
    )
).withColumn(
    "suitable_for_price_sensitivity",
    F.col("evaluable_price_change_events") >=
    F.lit(MIN_EVALUABLE_EVENTS)
).withColumn(
    "evidence_strength",
    F.when(
        F.col("evaluable_price_change_events") >= 5,
        "Strong"
    ).when(
        F.col("evaluable_price_change_events") >= 3,
        "Good"
    ).when(
        F.col("evaluable_price_change_events") >= 2,
        "Minimum"
    ).otherwise(
        "Insufficient"
    )
)


# ==================== ROBUST SENSITIVITY SCORE ====================


item_evidence = item_evidence.withColumn(
    "direction_consistency_factor",
    F.col("expected_direction_rate_pct") /
    F.lit(100.0)
).withColumn(
    "significant_response_factor",
    F.col("significant_response_rate_pct") /
    F.lit(100.0)
).withColumn(
    "price_sensitivity_score",
    F.when(
        F.col("suitable_for_price_sensitivity"),
        F.round(
            F.coalesce(
                F.col("median_capped_absolute_elasticity"),
                F.lit(0.0)
            ) *
            (
                F.lit(0.50) +
                F.lit(0.50) *
                F.col("direction_consistency_factor")
            ) *
            (
                F.lit(0.70) +
                F.lit(0.30) *
                F.col("significant_response_factor")
            ),
            4
        )
    )
)


suitable_scores = item_evidence.filter(
    F.col("suitable_for_price_sensitivity")
).select(
    "price_sensitivity_score"
)


score_quantiles = suitable_scores.approxQuantile(
    "price_sensitivity_score",
    [0.33, 0.67],
    0.01
)


if len(score_quantiles) != 2:
    raise ValueError(
        "Not enough suitable menu items to derive "
        "price-sensitivity thresholds."
    )


low_threshold = float(
    score_quantiles[0]
)

high_threshold = float(
    score_quantiles[1]
)


item_price_sensitivity = item_evidence.withColumn(
    "low_sensitivity_threshold",
    F.lit(
        low_threshold
    )
).withColumn(
    "high_sensitivity_threshold",
    F.lit(
        high_threshold
    )
).withColumn(
    "price_sensitivity_class",
    F.when(
        ~F.col("suitable_for_price_sensitivity"),
        "Insufficient Price History"
    ).when(
        F.col("price_sensitivity_score") >=
        F.lit(high_threshold),
        "Highly Price Sensitive"
    ).when(
        F.col("price_sensitivity_score") >=
        F.lit(low_threshold),
        "Moderately Price Sensitive"
    ).otherwise(
        "Low Price Sensitivity"
    )
).withColumn(
    "evidence_summary",
    F.concat_ws(
        " | ",
        F.concat(
            F.lit("Evaluable changes: "),
            F.col(
                "evaluable_price_change_events"
            ).cast("string")
        ),
        F.concat(
            F.lit("Median capped |elasticity|: "),
            F.coalesce(
                F.col(
                    "median_capped_absolute_elasticity"
                ).cast("string"),
                F.lit("N/A")
            )
        ),
        F.concat(
            F.lit("Expected-direction rate: "),
            F.col(
                "expected_direction_rate_pct"
            ).cast("string"),
            F.lit("%")
        ),
        F.concat(
            F.lit("Significant-response rate: "),
            F.col(
                "significant_response_rate_pct"
            ).cast("string"),
            F.lit("%")
        )
    )
).orderBy(
    F.desc_nulls_last(
        "price_sensitivity_score"
    )
)


# ==================== SUMMARY ====================


sensitivity_summary = item_price_sensitivity.groupBy(
    "price_sensitivity_class"
).agg(
    F.count(
        "*"
    ).alias(
        "item_count"
    ),
    F.round(
        F.avg(
            "price_sensitivity_score"
        ),
        4
    ).alias(
        "average_sensitivity_score"
    ),
    F.round(
        F.avg(
            "median_capped_absolute_elasticity"
        ),
        4
    ).alias(
        "average_median_capped_elasticity"
    ),
    F.round(
        F.avg(
            "expected_direction_rate_pct"
        ),
        2
    ).alias(
        "average_expected_direction_rate_pct"
    ),
    F.round(
        F.avg(
            "significant_response_rate_pct"
        ),
        2
    ).alias(
        "average_significant_response_rate_pct"
    )
).orderBy(
    F.desc(
        "average_sensitivity_score"
    )
)


sensitivity_thresholds = spark.createDataFrame([
    (
        float(low_threshold),
        float(high_threshold),
        int(MIN_EVALUABLE_EVENTS),
        float(ELASTICITY_CAP),
        "33rd and 67th percentiles of robust sensitivity score among suitable items"
    )
], [
    "low_sensitivity_threshold",
    "high_sensitivity_threshold",
    "minimum_evaluable_events",
    "absolute_elasticity_cap",
    "threshold_method"
])


analysis_metadata = spark.createDataFrame([
    (
        "suitable_items",
        f"Menu items require at least {MIN_EVALUABLE_EVENTS} evaluable historical price-change events before sensitivity is classified."
    ),
    (
        "elasticity",
        f"Absolute event elasticity is capped at {ELASTICITY_CAP:.1f} before item-level median aggregation so extreme percentage swings do not dominate classification."
    ),
    (
        "direction_consistency",
        "Expected-direction evidence means demand falls after a price increase or rises after a price decrease."
    ),
    (
        "sensitivity_score",
        "The robust score combines median capped absolute elasticity, expected-direction consistency, and the rate of significant demand responses."
    ),
    (
        "classification",
        "Suitable items are classified using the 33rd and 67th percentiles of the robust sensitivity score into Low, Moderate, and High price sensitivity."
    ),
    (
        "interpretation",
        "Price sensitivity is descriptive historical evidence and should not be interpreted as proof that price changes alone caused demand changes."
    )
], [
    "analysis_component",
    "method"
])


# ==================== RESULTS ====================


print("\n========PRICE SENSITIVITY THRESHOLDS========")
sensitivity_thresholds.show(
    truncate=False
)


print("\n========PRICE SENSITIVITY SUMMARY========")
sensitivity_summary.show(
    truncate=False
)


print("\n========MOST PRICE-SENSITIVE MENU ITEMS========")
item_price_sensitivity.filter(
    F.col(
        "suitable_for_price_sensitivity"
    )
).select(
    "item_id",
    "item_name",
    "category_name",
    "evaluable_price_change_events",
    "evaluable_locations",
    "median_capped_absolute_elasticity",
    "median_absolute_demand_change_pct",
    "expected_direction_rate_pct",
    "significant_response_rate_pct",
    "price_sensitivity_score",
    "price_sensitivity_class",
    "evidence_strength"
).show(
    30,
    truncate=False
)


# ==================== SAVE OUTPUTS ====================


outputs = {
    "item_price_sensitivity": item_price_sensitivity,
    "sensitivity_summary": sensitivity_summary,
    "sensitivity_thresholds": sensitivity_thresholds,
    "analysis_metadata": analysis_metadata
}


for name, frame in outputs.items():
    frame.write.mode(
        "overwrite"
    ).parquet(
        f"{output_folder}/{name}"
    )


print("\nPrice-sensitivity analysis completed successfully.")


spark.stop()
