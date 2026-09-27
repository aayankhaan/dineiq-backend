from pathlib import Path
import math

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import ML_DATA_FOLDER


spark = SparkSession.builder \
    .appName("Validate DineIQ Forecast Accuracy Evaluation") \
    .getOrCreate()


validation_folder = f"{ML_DATA_FOLDER}/forecasting_validation"
output_folder = f"{ML_DATA_FOLDER}/forecasting_evaluation"

MODEL_VERSION = "demand_forecasting_v1"
VALIDATION_VERSION = "time_aware_validation_v1"
EVALUATION_VERSION = "forecast_evaluation_v1"


outputs = {
    "Series metrics": (
        f"{output_folder}/series_metrics"
    ),
    "Level summary": (
        f"{output_folder}/level_summary"
    ),
    "Overall test evaluation": (
        f"{output_folder}/overall_test_evaluation"
    ),
    "Forecast errors": (
        f"{output_folder}/forecast_errors"
    ),
    "High-risk periods": (
        f"{output_folder}/high_risk_periods"
    ),
    "Evaluation metadata": (
        f"{output_folder}/evaluation_metadata"
    )
}


backtest_outputs = {
    "Overall": (
        f"{validation_folder}/overall_backtest"
    ),
    "Menu item": (
        f"{validation_folder}/menu_item_backtest"
    ),
    "Menu category": (
        f"{validation_folder}/menu_category_backtest"
    ),
    "Restaurant location": (
        f"{validation_folder}/restaurant_location_backtest"
    ),
    "Time period": (
        f"{validation_folder}/time_period_backtest"
    )
}


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


print("\n========FORECAST EVALUATION VALIDATION========")


all_outputs_exist = True

for output_name, output_path in outputs.items():
    exists = Path(
        output_path
    ).exists()

    check(
        f"{output_name} output exists",
        exists
    )

    all_outputs_exist = (
        all_outputs_exist and
        exists
    )


for output_name, output_path in backtest_outputs.items():
    exists = Path(
        output_path
    ).exists()

    check(
        f"{output_name} backtest input exists",
        exists
    )

    all_outputs_exist = (
        all_outputs_exist and
        exists
    )


if not all_outputs_exist:
    total_checks = passed + failed

    print(
        f"\n========VALIDATION RESULT: "
        f"{passed}/{total_checks} PASS========"
    )

    spark.stop()

    raise SystemExit(1)


series_metrics = spark.read.parquet(
    outputs["Series metrics"]
)

level_summary = spark.read.parquet(
    outputs["Level summary"]
)

overall_test = spark.read.parquet(
    outputs["Overall test evaluation"]
)

forecast_errors = spark.read.parquet(
    outputs["Forecast errors"]
)

high_risk = spark.read.parquet(
    outputs["High-risk periods"]
)

metadata = spark.read.parquet(
    outputs["Evaluation metadata"]
)


overall_backtest = spark.read.parquet(
    backtest_outputs["Overall"]
)

item_backtest = spark.read.parquet(
    backtest_outputs["Menu item"]
)

category_backtest = spark.read.parquet(
    backtest_outputs["Menu category"]
)

location_backtest = spark.read.parquet(
    backtest_outputs[
        "Restaurant location"
    ]
)

period_backtest = spark.read.parquet(
    backtest_outputs["Time period"]
)




check(
    "Evaluation metadata contains exactly one row",
    metadata.count() == 1
)


metadata_row = metadata.first()

threshold = float(
    metadata_row[
        "high_risk_underforecast_threshold_pct"
    ]
)


check(
    "Final evaluation uses the unseen test split",
    metadata_row[
        "evaluation_split"
    ] == "test"
)

check(
    "MAE evaluation is enabled",
    metadata_row[
        "metric_mae"
    ] is True
)

check(
    "RMSE evaluation is enabled",
    metadata_row[
        "metric_rmse"
    ] is True
)

check(
    "MAPE evaluation is enabled",
    metadata_row[
        "metric_mape"
    ] is True
)

check(
    "R-squared evaluation is enabled",
    metadata_row[
        "metric_r2"
    ] is True
)

check(
    "MAPE zero-demand handling is documented",
    metadata_row[
        "mape_zero_actual_policy"
    ] == "exclude_zero_actual_rows"
)

check(
    "Seasonal naive baseline is documented",
    metadata_row[
        "baseline_method"
    ] == "seasonal_naive_lag_7_recursive"
)

check(
    "Expected model version is recorded",
    metadata_row[
        "model_version"
    ] == MODEL_VERSION
)

check(
    "Expected validation version is recorded",
    metadata_row[
        "validation_version"
    ] == VALIDATION_VERSION
)

check(
    "Expected evaluation version is recorded",
    metadata_row[
        "evaluation_version"
    ] == EVALUATION_VERSION
)



required_metric_columns = {
    "model_mae",
    "baseline_mae",
    "model_rmse",
    "baseline_rmse",
    "model_mape",
    "baseline_mape",
    "model_r2",
    "baseline_r2",
    "mae_improvement_pct",
    "rmse_improvement_pct",
    "mape_improvement_pct"
}


check(
    "Series metrics contain required evaluation measures",
    required_metric_columns.issubset(
        set(series_metrics.columns)
    )
)

check(
    "Level summary contains required evaluation measures",
    required_metric_columns.issubset(
        set(level_summary.columns)
    )
)


metric_nonnegative_columns = [
    "model_mae",
    "baseline_mae",
    "model_rmse",
    "baseline_rmse",
    "model_mape",
    "baseline_mape"
]


for column in metric_nonnegative_columns:
    check(
        f"{column} contains no negative values",
        series_metrics.filter(
            F.col(column) < 0
        ).count() == 0
    )


expected_levels = {
    "Overall Daily",
    "Menu Item",
    "Menu Category",
    "Restaurant Location",
    "Selected Time Period"
}


test_levels = {
    row[
        "forecast_level"
    ]
    for row in level_summary.filter(
        F.col("split") == "test"
    ).select(
        "forecast_level"
    ).distinct().collect()
}


validation_levels = {
    row[
        "forecast_level"
    ]
    for row in level_summary.filter(
        F.col("split") == "validation"
    ).select(
        "forecast_level"
    ).distinct().collect()
}


check(
    "All forecast levels have test evaluation",
    test_levels == expected_levels,
    f"Found: {sorted(test_levels)}"
)

check(
    "All forecast levels have validation evaluation",
    validation_levels == expected_levels,
    f"Found: {sorted(validation_levels)}"
)



expected_series_count = (
    1 +
    item_backtest.select(
        "item_id"
    ).distinct().count() +
    category_backtest.select(
        "category_id"
    ).distinct().count() +
    location_backtest.select(
        "restaurant_id"
    ).distinct().count() +
    period_backtest.select(
        "order_hour"
    ).distinct().count()
)


test_series_count = series_metrics.filter(
    F.col("split") == "test"
).count()


validation_series_count = series_metrics.filter(
    F.col("split") == "validation"
).count()


check(
    "Test metrics contain one row per forecast series",
    test_series_count ==
    expected_series_count,
    (
        f"Expected {expected_series_count}, "
        f"found {test_series_count}"
    )
)

check(
    "Validation metrics contain one row per forecast series",
    validation_series_count ==
    expected_series_count,
    (
        f"Expected {expected_series_count}, "
        f"found {validation_series_count}"
    )
)

expected_test_rows = (
    overall_backtest.filter(
        F.col("split") == "test"
    ).count() +
    item_backtest.filter(
        F.col("split") == "test"
    ).count() +
    category_backtest.filter(
        F.col("split") == "test"
    ).count() +
    location_backtest.filter(
        F.col("split") == "test"
    ).count() +
    period_backtest.filter(
        F.col("split") == "test"
    ).count()
)


check(
    "Forecast error output contains every test prediction",
    forecast_errors.count() ==
    expected_test_rows,
    (
        f"Expected {expected_test_rows}, "
        f"found {forecast_errors.count()}"
    )
)



error_mismatch = forecast_errors.filter(
    F.abs(
        F.col("error") -
        (
            F.col("predicted_demand") -
            F.col("actual_demand")
        )
    ) > 0.0001
).count()


absolute_error_mismatch = forecast_errors.filter(
    F.abs(
        F.col("absolute_error") -
        F.abs(
            F.col("error")
        )
    ) > 0.0001
).count()


check(
    "Forecast error values are calculated correctly",
    error_mismatch == 0
)

check(
    "Absolute forecast errors are calculated correctly",
    absolute_error_mismatch == 0
)




check(
    "Overall test evaluation contains exactly one row",
    overall_test.count() == 1
)


overall_row = overall_test.first()


overall_test_rows = overall_backtest.filter(
    F.col("split") == "test"
).withColumn(
    "absolute_error",
    F.abs(
        F.col("predicted_demand") -
        F.col("actual_demand")
    )
).withColumn(
    "squared_error",
    F.pow(
        F.col("predicted_demand") -
        F.col("actual_demand"),
        2
    )
).withColumn(
    "absolute_percentage_error",
    F.when(
        F.col("actual_demand") > 0,
        F.abs(
            F.col("predicted_demand") -
            F.col("actual_demand")
        ) /
        F.col("actual_demand") *
        100
    )
)


recomputed = overall_test_rows.agg(
    F.avg(
        "absolute_error"
    ).alias(
        "mae"
    ),
    F.sqrt(
        F.avg(
            "squared_error"
        )
    ).alias(
        "rmse"
    ),
    F.avg(
        "absolute_percentage_error"
    ).alias(
        "mape"
    )
).first()


check(
    "Overall MAE matches test predictions",
    abs(
        float(
            overall_row[
                "model_mae"
            ]
        ) -
        float(
            recomputed["mae"]
        )
    ) <= 0.01
)

check(
    "Overall RMSE matches test predictions",
    abs(
        float(
            overall_row[
                "model_rmse"
            ]
        ) -
        float(
            recomputed["rmse"]
        )
    ) <= 0.01
)

check(
    "Overall MAPE matches test predictions",
    abs(
        float(
            overall_row[
                "model_mape"
            ]
        ) -
        float(
            recomputed["mape"]
        )
    ) <= 0.01
)



check(
    "Overall forecast improves on baseline MAE",
    overall_row[
        "model_mae"
    ] <
    overall_row[
        "baseline_mae"
    ],
    (
        f"Model: {overall_row['model_mae']}, "
        f"baseline: {overall_row['baseline_mae']}"
    )
)

check(
    "Overall forecast improves on baseline RMSE",
    overall_row[
        "model_rmse"
    ] <
    overall_row[
        "baseline_rmse"
    ],
    (
        f"Model: {overall_row['model_rmse']}, "
        f"baseline: {overall_row['baseline_rmse']}"
    )
)

check(
    "Overall forecast improves on baseline MAPE",
    overall_row[
        "model_mape"
    ] <
    overall_row[
        "baseline_mape"
    ],
    (
        f"Model: {overall_row['model_mape']}, "
        f"baseline: {overall_row['baseline_mape']}"
    )
)

check(
    "Baseline improvement requirement is marked passed",
    metadata_row[
        "baseline_requirement_passed"
    ] is True
)



check(
    "Overall test MAPE has eligible nonzero-demand rows",
    overall_row[
        "mape_eligible_rows"
    ] > 0
)

check(
    "Overall test R-squared is available",
    overall_row[
        "model_r2"
    ] is not None and
    not math.isnan(
        float(
            overall_row[
                "model_r2"
            ]
        )
    )
)


invalid_high_risk = high_risk.filter(
    (
        F.col(
            "forecast_direction"
        ) != "Under-Forecast"
    ) |
    (
        F.col(
            "underforecast_percentage"
        ) < threshold
    ) |
    (
        F.col(
            "high_risk_demand_period"
        ) != True
    )
).count()


check(
    "High-risk rows satisfy the configured under-forecast threshold",
    invalid_high_risk == 0,
    f"Invalid rows: {invalid_high_risk}"
)


expected_high_risk_count = forecast_errors.filter(
    F.col(
        "high_risk_demand_period"
    ) == True
).count()


check(
    "High-risk output contains every flagged test row",
    high_risk.count() ==
    expected_high_risk_count,
    (
        f"Expected {expected_high_risk_count}, "
        f"found {high_risk.count()}"
    )
)





for name, frame in [
    (
        "Series metrics",
        series_metrics
    ),
    (
        "Level summary",
        level_summary
    ),
    (
        "Overall test evaluation",
        overall_test
    ),
    (
        "Forecast errors",
        forecast_errors
    )
]:
    check(
        f"{name} uses expected model version",
        frame.filter(
            F.col("model_version") !=
            MODEL_VERSION
        ).count() == 0
    )

    check(
        f"{name} uses expected validation version",
        frame.filter(
            F.col("validation_version") !=
            VALIDATION_VERSION
        ).count() == 0
    )

    check(
        f"{name} uses expected evaluation version",
        frame.filter(
            F.col("evaluation_version") !=
            EVALUATION_VERSION
        ).count() == 0
    )



total_checks = passed + failed


print(
    f"\n========VALIDATION RESULT: "
    f"{passed}/{total_checks} PASS========"
)


if failed == 0:
    print(
        "\nForecast accuracy evaluation validation PASSED."
    )
else:
    print(
        "\nForecast accuracy evaluation validation FAILED."
    )


spark.stop()
