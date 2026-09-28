from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import ML_DATA_FOLDER, MODEL_FOLDER


spark = SparkSession.builder \
    .appName("Validate DineIQ Wastage Risk Prediction") \
    .getOrCreate()


output_folder = f"{ML_DATA_FOLDER}/wastage_risk"

MODEL_VERSION = "wastage_risk_v2"


risk_model_data = spark.read.parquet(
    f"{output_folder}/risk_model_data"
)

validation_threshold_metrics = spark.read.parquet(
    f"{output_folder}/validation_threshold_metrics"
)

test_predictions = spark.read.parquet(
    f"{output_folder}/test_predictions"
)

high_risk_predictions = spark.read.parquet(
    f"{output_folder}/high_risk_predictions"
)

item_risk_summary = spark.read.parquet(
    f"{output_folder}/item_risk_summary"
)

period_risk_summary = spark.read.parquet(
    f"{output_folder}/period_risk_summary"
)

model_metrics = spark.read.parquet(
    f"{output_folder}/model_metrics"
)

metadata = spark.read.parquet(
    f"{output_folder}/risk_metadata"
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


print("\n========WASTAGE RISK VALIDATION========")


# ==================== REQUIRED OUTPUTS ====================


outputs = [
    ("Risk model data", risk_model_data),
    ("Validation threshold metrics", validation_threshold_metrics),
    ("Test predictions", test_predictions),
    ("High-risk predictions", high_risk_predictions),
    ("Item risk summary", item_risk_summary),
    ("Period risk summary", period_risk_summary),
    ("Model metrics", model_metrics),
    ("Risk metadata", metadata)
]


for name, frame in outputs:
    check(
        f"{name} contains data",
        frame.count() > 0
    )


check(
    "Saved wastage-risk model exists",
    Path(
        f"{MODEL_FOLDER}/{MODEL_VERSION}"
    ).exists()
)


# ==================== METADATA ====================


check(
    "Risk metadata contains exactly one row",
    metadata.count() == 1
)


metadata_row = metadata.first()


check(
    "Expected model version is recorded",
    metadata_row[
        "model_version"
    ] ==
    MODEL_VERSION
)


check(
    "Prediction target is meaningful high-cost wastage",
    metadata_row[
        "prediction_target"
    ] ==
    "next_day_high_wastage_cost"
)


check(
    "High-wastage training quantile is valid",
    0.0 <
    float(
        metadata_row[
            "high_wastage_quantile"
        ]
    ) <
    1.0
)


high_wastage_cost_threshold = float(
    metadata_row[
        "high_wastage_cost_threshold"
    ]
)


check(
    "High-wastage cost threshold is positive",
    high_wastage_cost_threshold > 0
)


# ==================== TIME-AWARE VALIDATION ====================


check(
    "Random splitting is disabled",
    metadata_row[
        "random_split_used"
    ] is False
)


check(
    "Future outcome features are disabled",
    metadata_row[
        "future_outcome_features_used"
    ] is False
)


check(
    "Training ends before validation starts",
    metadata_row[
        "training_end_date"
    ] <
    metadata_row[
        "validation_start_date"
    ]
)


check(
    "Validation ends before test starts",
    metadata_row[
        "validation_end_date"
    ] <
    metadata_row[
        "test_start_date"
    ]
)


check(
    "Test ends at historical end",
    metadata_row[
        "test_end_date"
    ] ==
    metadata_row[
        "historical_end_date"
    ]
)


check(
    "Every prediction target occurs after its feature date",
    risk_model_data.filter(
        F.col(
            "target_date"
        ) <=
        F.col(
            "feature_date"
        )
    ).count() == 0
)


check(
    "All test predictions stay in unseen test period",
    test_predictions.filter(
        (
            F.col(
                "target_date"
            ) <
            F.lit(
                metadata_row[
                    "test_start_date"
                ]
            )
        ) |
        (
            F.col(
                "target_date"
            ) >
            F.lit(
                metadata_row[
                    "test_end_date"
                ]
            )
        )
    ).count() == 0
)


# ==================== FEATURE LEAKAGE VALIDATION ====================


numeric_features = set(
    metadata_row[
        "numeric_features"
    ].split(",")
)


required_features = {
    "historical_average_demand",
    "rolling_7_demand_std",
    "historical_average_wastage_cost",
    "historical_positive_wastage_cost",
    "rolling_7_wastage_days",
    "rolling_28_wastage_days",
    "historical_wastage_rate",
    "target_day_of_week_number",
    "target_month_number",
    "target_is_weekend",
    "lag_1_promotion_share_pct",
    "menu_popularity_28d",
    "forecast_demand_proxy",
    "lag_1_preparation_quantity",
    "active_inventory_batches",
    "active_inventory_received_quantity",
    "average_days_to_expiry",
    "expiring_within_7_days_ratio",
    "expiring_within_7_days_received_quantity"
}


check(
    "Required wastage-risk predictive features are present",
    required_features.issubset(
        numeric_features
    )
)


check(
    "Final quantity_remaining is not used as a historical feature",
    "average_inventory_remaining_ratio" not in
    numeric_features
)


check(
    "Inventory feature policy uses receipt-time information",
    metadata_row[
        "feature_policy"
    ] ==
    "past_only_lags_rolling_history_and_receipt_time_inventory_context"
)


# ==================== HIGH-RISK LABEL VALIDATION ====================


label_mismatches = risk_model_data.filter(
    F.col(
        "label"
    ) !=
    F.when(
        F.col(
            "next_wastage_cost"
        ) >=
        F.lit(
            high_wastage_cost_threshold
        ),
        1.0
    ).otherwise(
        0.0
    )
).count()


check(
    "High-risk labels exactly match the training-derived cost threshold",
    label_mismatches == 0,
    f"Mismatched rows: {label_mismatches}"
)


positive_below_threshold = risk_model_data.filter(
    (
        F.col(
            "label"
        ) == 1.0
    ) &
    (
        F.col(
            "next_wastage_cost"
        ) <
        F.lit(
            high_wastage_cost_threshold
        )
    )
).count()


check(
    "Positive high-risk labels exclude trivial wastage costs",
    positive_below_threshold == 0
)


label_values = {
    int(
        row[
            "label"
        ]
    )
    for row in risk_model_data.select(
        "label"
    ).distinct().collect()
}


check(
    "Risk dataset contains both classes",
    label_values == {
        0,
        1
    },
    f"Found: {sorted(label_values)}"
)


# ==================== VALIDATION THRESHOLD SELECTION ====================


selected_threshold = float(
    metadata_row[
        "selected_probability_threshold"
    ]
)


threshold_values = {
    float(
        row[
            "threshold"
        ]
    )
    for row in validation_threshold_metrics.select(
        "threshold"
    ).distinct().collect()
}


check(
    "Selected probability threshold comes from validation candidates",
    selected_threshold in
    threshold_values
)


best_validation_row = validation_threshold_metrics.orderBy(
    F.desc(
        "macro_f1"
    ),
    F.desc(
        "balanced_accuracy"
    ),
    F.desc(
        "accuracy"
    )
).first()


check(
    "Selected probability threshold maximizes validation macro F1",
    abs(
        float(
            best_validation_row[
                "threshold"
            ]
        ) -
        selected_threshold
    ) <= 0.000001
)


# ==================== TEST PREDICTIONS ====================


check(
    "Risk probabilities are between zero and one",
    test_predictions.filter(
        (
            F.col(
                "risk_probability"
            ) < 0
        ) |
        (
            F.col(
                "risk_probability"
            ) > 1
        )
    ).count() == 0
)


check(
    "Risk probabilities are not null",
    test_predictions.filter(
        F.col(
            "risk_probability"
        ).isNull()
    ).count() == 0
)


check(
    "Test actual labels match the high-wastage threshold",
    test_predictions.filter(
        F.col(
            "actual_high_wastage_risk"
        ) !=
        F.when(
            F.col(
                "next_wastage_cost"
            ) >=
            F.col(
                "high_wastage_cost_threshold"
            ),
            1
        ).otherwise(
            0
        )
    ).count() == 0
)


prediction_values = {
    int(
        row[
            "predicted_high_wastage_risk"
        ]
    )
    for row in test_predictions.select(
        "predicted_high_wastage_risk"
    ).distinct().collect()
}


check(
    "Test predictions are binary",
    prediction_values.issubset(
        {
            0,
            1
        }
    ) and
    len(
        prediction_values
    ) > 0
)


check(
    "Risk levels match binary predictions",
    test_predictions.filter(
        (
            (
                F.col(
                    "predicted_high_wastage_risk"
                ) == 1
            ) &
            (
                F.col(
                    "risk_level"
                ) != "High Risk"
            )
        ) |
        (
            (
                F.col(
                    "predicted_high_wastage_risk"
                ) == 0
            ) &
            (
                F.col(
                    "risk_level"
                ) != "Low Risk"
            )
        )
    ).count() == 0
)


# ==================== MODEL METRICS ====================


check(
    "Model metrics contain exactly one test row",
    model_metrics.count() == 1
)


metric_row = model_metrics.first()


for metric in [
    "accuracy",
    "precision",
    "recall",
    "specificity",
    "balanced_accuracy",
    "positive_f1",
    "negative_f1",
    "macro_f1",
    "auc_roc",
    "auc_pr",
    "majority_baseline_accuracy",
    "majority_baseline_macro_f1"
]:
    value = float(
        metric_row[
            metric
        ]
    )

    check(
        f"{metric} is between zero and one",
        0.0 <= value <= 1.0,
        f"Found: {value}"
    )


confusion_total = (
    int(
        metric_row[
            "true_positive"
        ]
    ) +
    int(
        metric_row[
            "true_negative"
        ]
    ) +
    int(
        metric_row[
            "false_positive"
        ]
    ) +
    int(
        metric_row[
            "false_negative"
        ]
    )
)


check(
    "Confusion-matrix counts equal test row count",
    confusion_total ==
    int(
        metric_row[
            "row_count"
        ]
    )
)


recomputed_accuracy = test_predictions.agg(
    F.avg(
        F.when(
            F.col(
                "actual_high_wastage_risk"
            ) ==
            F.col(
                "predicted_high_wastage_risk"
            ),
            1.0
        ).otherwise(
            0.0
        )
    ).alias(
        "accuracy"
    )
).first()["accuracy"]


check(
    "Reported accuracy matches raw predictions",
    abs(
        float(
            metric_row[
                "accuracy"
            ]
        ) -
        float(
            recomputed_accuracy
        )
    ) <= 0.000001
)


check(
    "Model macro F1 beats majority baseline macro F1",
    float(
        metric_row[
            "macro_f1"
        ]
    ) >
    float(
        metric_row[
            "majority_baseline_macro_f1"
        ]
    ),
    (
        f"Model: {float(metric_row['macro_f1']):.4f}, "
        f"baseline: {float(metric_row['majority_baseline_macro_f1']):.4f}"
    )
)


positive_prevalence = (
    float(
        metric_row[
            "positive_actual_rows"
        ]
    ) /
    float(
        metric_row[
            "row_count"
        ]
    )
)


check(
    "PR AUC beats positive-class prevalence",
    float(
        metric_row[
            "auc_pr"
        ]
    ) >
    positive_prevalence,
    (
        f"AUC-PR: {float(metric_row['auc_pr']):.4f}, "
        f"prevalence: {positive_prevalence:.4f}"
    )
)


check(
    "Classification performance reaches SRS target",
    (
        float(
            metric_row[
                "accuracy"
            ]
        ) >= 0.85
    ) |
    (
        float(
            metric_row[
                "macro_f1"
            ]
        ) >= 0.80
    ),
    (
        f"Accuracy: {float(metric_row['accuracy']):.4f}, "
        f"Macro F1: {float(metric_row['macro_f1']):.4f}"
    )
)


# ==================== BUSINESS OUTPUTS ====================


check(
    "High-risk output contains only predicted high-risk rows",
    high_risk_predictions.filter(
        F.col(
            "predicted_high_wastage_risk"
        ) != 1
    ).count() == 0
)


check(
    "High-risk output exactly matches flagged test predictions",
    high_risk_predictions.count() ==
    test_predictions.filter(
        F.col(
            "predicted_high_wastage_risk"
        ) == 1
    ).count()
)


check(
    "Item risk summary covers all tested menu items",
    item_risk_summary.select(
        "item_id"
    ).distinct().count() ==
    test_predictions.select(
        "item_id"
    ).distinct().count()
)


check(
    "Period risk summary covers all test dates",
    period_risk_summary.select(
        "target_date"
    ).distinct().count() ==
    test_predictions.select(
        "target_date"
    ).distinct().count()
)


print(
    f"\n========VALIDATION RESULT: "
    f"{passed}/{passed + failed} PASS========"
)


if failed == 0:
    print(
        "\nWastage risk prediction validation PASSED."
    )
else:
    print(
        "\nWastage risk prediction validation FAILED."
    )


spark.stop()
