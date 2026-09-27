import argparse
import numpy as np
import pandas as pd

from pyspark.sql import SparkSession

from config.settings import ML_DATA_FOLDER


spark = SparkSession.builder \
    .appName("DineIQ Forecast Accuracy Evaluation") \
    .getOrCreate()

spark.conf.set(
    "spark.sql.execution.arrow.pyspark.enabled",
    "false"
)


validation_folder = f"{ML_DATA_FOLDER}/forecasting_validation"
output_folder = f"{ML_DATA_FOLDER}/forecasting_evaluation"

MODEL_VERSION = "demand_forecasting_v1"
VALIDATION_VERSION = "time_aware_validation_v1"
EVALUATION_VERSION = "forecast_evaluation_v1"

DEFAULT_HIGH_RISK_THRESHOLD = 15.0


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate DineIQ chronological demand forecasts "
            "against actual demand and the seasonal baseline."
        )
    )

    parser.add_argument(
        "--high-risk-threshold",
        type=float,
        default=DEFAULT_HIGH_RISK_THRESHOLD,
        help=(
            "Minimum under-forecast percentage used to flag "
            "high-risk demand periods."
        )
    )

    return parser.parse_args()


def safe_improvement(
    baseline_metric,
    model_metric
):
    if (
        baseline_metric is None or
        pd.isna(baseline_metric) or
        baseline_metric == 0 or
        model_metric is None or
        pd.isna(model_metric)
    ):
        return np.nan

    return (
        (
            baseline_metric -
            model_metric
        ) /
        baseline_metric
    ) * 100


def calculate_metrics(
    frame
):
    actual = frame[
        "actual_demand"
    ].astype(float).to_numpy()

    prediction = frame[
        "predicted_demand"
    ].astype(float).to_numpy()

    baseline = frame[
        "baseline_prediction"
    ].astype(float).to_numpy()

    model_error = (
        prediction -
        actual
    )

    baseline_error = (
        baseline -
        actual
    )

    model_absolute_error = np.abs(
        model_error
    )

    baseline_absolute_error = np.abs(
        baseline_error
    )

    model_mae = float(
        np.mean(
            model_absolute_error
        )
    )

    baseline_mae = float(
        np.mean(
            baseline_absolute_error
        )
    )

    model_rmse = float(
        np.sqrt(
            np.mean(
                np.square(
                    model_error
                )
            )
        )
    )

    baseline_rmse = float(
        np.sqrt(
            np.mean(
                np.square(
                    baseline_error
                )
            )
        )
    )

    positive_mask = (
        actual > 0
    )

    mape_eligible_rows = int(
        np.sum(
            positive_mask
        )
    )

    if mape_eligible_rows > 0:
        model_mape = float(
            np.mean(
                model_absolute_error[
                    positive_mask
                ] /
                actual[
                    positive_mask
                ]
            ) * 100
        )

        baseline_mape = float(
            np.mean(
                baseline_absolute_error[
                    positive_mask
                ] /
                actual[
                    positive_mask
                ]
            ) * 100
        )
    else:
        model_mape = np.nan
        baseline_mape = np.nan

    actual_total = float(
        np.sum(
            actual
        )
    )

    if actual_total > 0:
        model_wape = float(
            np.sum(
                model_absolute_error
            ) /
            actual_total *
            100
        )

        baseline_wape = float(
            np.sum(
                baseline_absolute_error
            ) /
            actual_total *
            100
        )
    else:
        model_wape = np.nan
        baseline_wape = np.nan

    actual_mean = float(
        np.mean(
            actual
        )
    )

    total_sum_of_squares = float(
        np.sum(
            np.square(
                actual -
                actual_mean
            )
        )
    )

    if total_sum_of_squares > 0:
        model_r2 = float(
            1 -
            (
                np.sum(
                    np.square(
                        model_error
                    )
                ) /
                total_sum_of_squares
            )
        )

        baseline_r2 = float(
            1 -
            (
                np.sum(
                    np.square(
                        baseline_error
                    )
                ) /
                total_sum_of_squares
            )
        )
    else:
        model_r2 = np.nan
        baseline_r2 = np.nan

    model_bias = float(
        np.mean(
            model_error
        )
    )

    baseline_bias = float(
        np.mean(
            baseline_error
        )
    )

    underforecast_rows = int(
        np.sum(
            prediction < actual
        )
    )

    overforecast_rows = int(
        np.sum(
            prediction > actual
        )
    )

    return {
        "row_count": int(
            len(frame)
        ),
        "mape_eligible_rows": (
            mape_eligible_rows
        ),
        "actual_demand_total": round(
            actual_total,
            4
        ),
        "model_mae": round(
            model_mae,
            4
        ),
        "baseline_mae": round(
            baseline_mae,
            4
        ),
        "mae_improvement_pct": round(
            safe_improvement(
                baseline_mae,
                model_mae
            ),
            4
        ),
        "model_rmse": round(
            model_rmse,
            4
        ),
        "baseline_rmse": round(
            baseline_rmse,
            4
        ),
        "rmse_improvement_pct": round(
            safe_improvement(
                baseline_rmse,
                model_rmse
            ),
            4
        ),
        "model_mape": round(
            model_mape,
            4
        ),
        "baseline_mape": round(
            baseline_mape,
            4
        ),
        "mape_improvement_pct": round(
            safe_improvement(
                baseline_mape,
                model_mape
            ),
            4
        ),
        "model_wape": round(
            model_wape,
            4
        ),
        "baseline_wape": round(
            baseline_wape,
            4
        ),
        "wape_improvement_pct": round(
            safe_improvement(
                baseline_wape,
                model_wape
            ),
            4
        ),
        "model_r2": round(
            model_r2,
            6
        ),
        "baseline_r2": round(
            baseline_r2,
            6
        ),
        "model_bias": round(
            model_bias,
            4
        ),
        "baseline_bias": round(
            baseline_bias,
            4
        ),
        "underforecast_rows": (
            underforecast_rows
        ),
        "overforecast_rows": (
            overforecast_rows
        ),
        "model_beats_baseline_mae": bool(
            model_mae <
            baseline_mae
        ),
        "model_beats_baseline_rmse": bool(
            model_rmse <
            baseline_rmse
        ),
        "model_beats_baseline_mape": bool(
            (
                not pd.isna(
                    model_mape
                )
            ) and
            (
                not pd.isna(
                    baseline_mape
                )
            ) and
            (
                model_mape <
                baseline_mape
            )
        )
    }


def quality_label(
    mape
):
    if (
        mape is None or
        pd.isna(mape)
    ):
        return "Not Available"

    if mape <= 10:
        return "Excellent"

    if mape <= 20:
        return "Good"

    if mape <= 30:
        return "Moderate"

    return "High Error"


def prepare_backtest(
    frame
):
    output = frame[
        [
            "forecast_date",
            "split",
            "actual_demand",
            "predicted_demand",
            "baseline_prediction",
            "forecast_origin_date",
            "forecast_horizon_day",
            "selected_alpha",
            "forecast_level",
            "model_name",
            "model_version",
            "validation_version",
            "entity_id",
            "entity_name"
        ]
    ].copy()

    output[
        "forecast_date"
    ] = pd.to_datetime(
        output[
            "forecast_date"
        ]
    )

    output[
        "forecast_origin_date"
    ] = pd.to_datetime(
        output[
            "forecast_origin_date"
        ]
    )

    return output


def evaluate_series(
    frame
):
    rows = []

    for (
        split_name,
        forecast_level,
        entity_id,
        entity_name
    ), group in frame.groupby(
        [
            "split",
            "forecast_level",
            "entity_id",
            "entity_name"
        ],
        sort=True
    ):
        metrics = calculate_metrics(
            group
        )

        rows.append({
            "split": split_name,
            "forecast_level": (
                forecast_level
            ),
            "entity_id": str(
                entity_id
            ),
            "entity_name": str(
                entity_name
            ),
            **metrics,
            "forecast_quality": (
                quality_label(
                    metrics[
                        "model_mape"
                    ]
                )
            ),
            "model_version": (
                MODEL_VERSION
            ),
            "validation_version": (
                VALIDATION_VERSION
            ),
            "evaluation_version": (
                EVALUATION_VERSION
            )
        })

    return pd.DataFrame(
        rows
    )


def evaluate_levels(
    frame,
    series_metrics
):
    rows = []

    for (
        split_name,
        forecast_level
    ), group in frame.groupby(
        [
            "split",
            "forecast_level"
        ],
        sort=True
    ):
        metrics = calculate_metrics(
            group
        )

        matching_series = series_metrics[
            (
                series_metrics[
                    "split"
                ] == split_name
            ) &
            (
                series_metrics[
                    "forecast_level"
                ] == forecast_level
            )
        ]

        series_count = int(
            len(
                matching_series
            )
        )

        improved_series_count = int(
            matching_series[
                "model_beats_baseline_mae"
            ].sum()
        )

        if series_count > 0:
            improved_series_pct = (
                improved_series_count /
                series_count *
                100
            )
        else:
            improved_series_pct = np.nan

        median_series_mape = float(
            matching_series[
                "model_mape"
            ].median()
        )

        rows.append({
            "split": split_name,
            "forecast_level": (
                forecast_level
            ),
            "series_count": (
                series_count
            ),
            "improved_series_count": (
                improved_series_count
            ),
            "improved_series_pct": round(
                improved_series_pct,
                4
            ),
            "median_series_mape": round(
                median_series_mape,
                4
            ),
            **metrics,
            "forecast_quality": (
                quality_label(
                    metrics[
                        "model_mape"
                    ]
                )
            ),
            "model_version": (
                MODEL_VERSION
            ),
            "validation_version": (
                VALIDATION_VERSION
            ),
            "evaluation_version": (
                EVALUATION_VERSION
            )
        })

    return pd.DataFrame(
        rows
    )


def build_error_analysis(
    frame,
    high_risk_threshold
):
    errors = frame[
        frame["split"] == "test"
    ].copy()

    errors["error"] = (
        errors[
            "predicted_demand"
        ] -
        errors[
            "actual_demand"
        ]
    )

    errors["absolute_error"] = np.abs(
        errors["error"]
    )

    errors[
        "squared_error"
    ] = np.square(
        errors["error"]
    )

    errors[
        "absolute_percentage_error"
    ] = np.where(
        errors[
            "actual_demand"
        ] > 0,
        (
            errors[
                "absolute_error"
            ] /
            errors[
                "actual_demand"
            ]
        ) * 100,
        np.nan
    )

    errors[
        "underforecast_amount"
    ] = np.maximum(
        (
            errors[
                "actual_demand"
            ] -
            errors[
                "predicted_demand"
            ]
        ),
        0
    )

    errors[
        "underforecast_percentage"
    ] = np.where(
        errors[
            "actual_demand"
        ] > 0,
        (
            errors[
                "underforecast_amount"
            ] /
            errors[
                "actual_demand"
            ]
        ) * 100,
        0.0
    )

    errors[
        "forecast_direction"
    ] = np.select(
        [
            errors["error"] < 0,
            errors["error"] > 0
        ],
        [
            "Under-Forecast",
            "Over-Forecast"
        ],
        default="Exact"
    )

    errors[
        "high_risk_demand_period"
    ] = (
        (
            errors[
                "forecast_direction"
            ] == "Under-Forecast"
        ) &
        (
            errors[
                "underforecast_percentage"
            ] >=
            high_risk_threshold
        )
    )

    errors[
        "model_version"
    ] = MODEL_VERSION

    errors[
        "validation_version"
    ] = VALIDATION_VERSION

    errors[
        "evaluation_version"
    ] = EVALUATION_VERSION

    return errors


def write_dataframe(
    frame,
    path
):
    output = frame.copy()

    for column in [
        "forecast_date",
        "forecast_origin_date",
        "test_start_date",
        "test_end_date"
    ]:
        if column in output.columns:
            output[column] = pd.to_datetime(
                output[column]
            ).dt.date

    spark.createDataFrame(
        output
    ).write.mode(
        "overwrite"
    ).parquet(
        path
    )


args = parse_args()

if args.high_risk_threshold < 0:
    raise ValueError(
        "High-risk threshold cannot be negative."
    )


overall_backtest = spark.read.parquet(
    f"{validation_folder}/overall_backtest"
).toPandas()

item_backtest = spark.read.parquet(
    f"{validation_folder}/menu_item_backtest"
).toPandas()

category_backtest = spark.read.parquet(
    f"{validation_folder}/menu_category_backtest"
).toPandas()

location_backtest = spark.read.parquet(
    f"{validation_folder}/restaurant_location_backtest"
).toPandas()

period_backtest = spark.read.parquet(
    f"{validation_folder}/time_period_backtest"
).toPandas()


all_backtests = pd.concat(
    [
        prepare_backtest(
            overall_backtest
        ),
        prepare_backtest(
            item_backtest
        ),
        prepare_backtest(
            category_backtest
        ),
        prepare_backtest(
            location_backtest
        ),
        prepare_backtest(
            period_backtest
        )
    ],
    ignore_index=True
)


series_metrics = evaluate_series(
    all_backtests
)


level_summary = evaluate_levels(
    all_backtests,
    series_metrics
)


test_summary = level_summary[
    level_summary["split"] == "test"
].copy().sort_values(
    "forecast_level"
)


overall_test = test_summary[
    test_summary[
        "forecast_level"
    ] == "Overall Daily"
].copy()


if overall_test.empty:
    raise ValueError(
        "Overall Daily test evaluation is missing."
    )


overall_row = overall_test.iloc[0]


baseline_requirement_passed = bool(
    overall_row[
        "model_beats_baseline_mae"
    ] and
    overall_row[
        "model_beats_baseline_rmse"
    ] and
    overall_row[
        "model_beats_baseline_mape"
    ]
)


forecast_errors = build_error_analysis(
    all_backtests,
    args.high_risk_threshold
)


high_risk_periods = forecast_errors[
    forecast_errors[
        "high_risk_demand_period"
    ]
].copy().sort_values(
    [
        "underforecast_percentage",
        "underforecast_amount"
    ],
    ascending=[
        False,
        False
    ]
)


test_dates = all_backtests[
    all_backtests["split"] == "test"
][
    "forecast_date"
]


evaluation_metadata = pd.DataFrame([
    {
        "evaluation_split": "test",
        "test_start_date": (
            test_dates.min()
        ),
        "test_end_date": (
            test_dates.max()
        ),
        "metric_mae": True,
        "metric_rmse": True,
        "metric_mape": True,
        "metric_r2": True,
        "extra_metric_wape": True,
        "mape_zero_actual_policy": (
            "exclude_zero_actual_rows"
        ),
        "baseline_method": (
            "seasonal_naive_lag_7_recursive"
        ),
        "baseline_requirement": (
            "overall_test_model_must_beat_"
            "baseline_on_mae_rmse_mape"
        ),
        "baseline_requirement_passed": (
            baseline_requirement_passed
        ),
        "high_risk_underforecast_threshold_pct": float(
            args.high_risk_threshold
        ),
        "model_version": (
            MODEL_VERSION
        ),
        "validation_version": (
            VALIDATION_VERSION
        ),
        "evaluation_version": (
            EVALUATION_VERSION
        )
    }
])


print("\n========FORECAST ACCURACY EVALUATION========")
print(
    "Final Evaluation Split: TEST ONLY"
)


display_columns = [
    "forecast_level",
    "series_count",
    "model_mae",
    "baseline_mae",
    "mae_improvement_pct",
    "model_rmse",
    "baseline_rmse",
    "rmse_improvement_pct",
    "model_mape",
    "baseline_mape",
    "mape_improvement_pct",
    "model_r2",
    "improved_series_pct",
    "forecast_quality"
]


print("\n========TEST METRICS BY FORECAST LEVEL========")
print(
    test_summary[
        display_columns
    ].to_string(
        index=False
    )
)


print("\n========OVERALL FORECAST PERFORMANCE========")
print(
    f"MAE: "
    f"{overall_row['model_mae']:.2f} "
    f"(Baseline: "
    f"{overall_row['baseline_mae']:.2f}, "
    f"Improvement: "
    f"{overall_row['mae_improvement_pct']:.2f}%)"
)

print(
    f"RMSE: "
    f"{overall_row['model_rmse']:.2f} "
    f"(Baseline: "
    f"{overall_row['baseline_rmse']:.2f}, "
    f"Improvement: "
    f"{overall_row['rmse_improvement_pct']:.2f}%)"
)

print(
    f"MAPE: "
    f"{overall_row['model_mape']:.2f}% "
    f"(Baseline: "
    f"{overall_row['baseline_mape']:.2f}%, "
    f"Improvement: "
    f"{overall_row['mape_improvement_pct']:.2f}%)"
)

print(
    f"R²: "
    f"{overall_row['model_r2']:.4f} "
    f"(Baseline: "
    f"{overall_row['baseline_r2']:.4f})"
)

print(
    f"WAPE: "
    f"{overall_row['model_wape']:.2f}% "
    f"(Baseline: "
    f"{overall_row['baseline_wape']:.2f}%)"
)

print(
    f"Forecast Quality: "
    f"{overall_row['forecast_quality']}"
)

print(
    f"Baseline Requirement Passed: "
    f"{baseline_requirement_passed}"
)


print("\n========HIGHEST TEST MAPE SERIES========")

worst_series = series_metrics[
    series_metrics["split"] == "test"
].sort_values(
    "model_mape",
    ascending=False
).head(
    10
)

print(
    worst_series[
        [
            "forecast_level",
            "entity_id",
            "entity_name",
            "model_mape",
            "baseline_mape",
            "mae_improvement_pct",
            "forecast_quality"
        ]
    ].to_string(
        index=False
    )
)


print("\n========HIGH-RISK DEMAND PERIODS========")
print(
    f"Threshold: "
    f"{args.high_risk_threshold:.2f}% "
    f"under-forecast"
)

print(
    f"Flagged Test Rows: "
    f"{len(high_risk_periods):,}"
)

if not high_risk_periods.empty:
    print(
        high_risk_periods[
            [
                "forecast_date",
                "forecast_level",
                "entity_name",
                "actual_demand",
                "predicted_demand",
                "underforecast_amount",
                "underforecast_percentage"
            ]
        ].head(
            20
        ).to_string(
            index=False
        )
    )


write_dataframe(
    series_metrics,
    f"{output_folder}/series_metrics"
)

write_dataframe(
    level_summary,
    f"{output_folder}/level_summary"
)

write_dataframe(
    overall_test,
    f"{output_folder}/overall_test_evaluation"
)

write_dataframe(
    forecast_errors,
    f"{output_folder}/forecast_errors"
)

write_dataframe(
    high_risk_periods,
    f"{output_folder}/high_risk_periods"
)

write_dataframe(
    evaluation_metadata,
    f"{output_folder}/evaluation_metadata"
)


print("\n========FORECAST EVALUATION COMPLETE========")
print(
    f"Series Metric Rows: "
    f"{len(series_metrics):,}"
)
print(
    f"Level Summary Rows: "
    f"{len(level_summary):,}"
)
print(
    f"Test Error Rows: "
    f"{len(forecast_errors):,}"
)
print(
    f"High-Risk Rows: "
    f"{len(high_risk_periods):,}"
)
print(
    f"Evaluation Version: "
    f"{EVALUATION_VERSION}"
)


spark.stop()
