import argparse
import numpy as np
import pandas as pd

from pyspark.sql import SparkSession
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from config.settings import ML_DATA_FOLDER


spark = SparkSession.builder \
    .appName("DineIQ Time-Aware Forecast Validation") \
    .getOrCreate()

spark.conf.set(
    "spark.sql.execution.arrow.pyspark.enabled",
    "false"
)


forecast_folder = f"{ML_DATA_FOLDER}/forecasting"
output_folder = f"{ML_DATA_FOLDER}/forecasting_validation"

MODEL_NAME = "Ridge Time-Series Regression"
MODEL_VERSION = "demand_forecasting_v1"
VALIDATION_VERSION = "time_aware_validation_v1"

DEFAULT_VALIDATION_DAYS = 31
DEFAULT_TEST_DAYS = 31
MINIMUM_TRAINING_ROWS = 60

ALPHA_CANDIDATES = [
    0.1,
    1.0,
    5.0,
    10.0,
    25.0,
    50.0
]

feature_columns = [
    "trend_index",
    "day_of_week_sin",
    "day_of_week_cos",
    "month_sin",
    "month_cos",
    "day_of_month_sin",
    "day_of_month_cos",
    "is_weekend",
    "lag_1",
    "lag_7",
    "lag_14",
    "lag_28",
    "rolling_mean_7",
    "rolling_mean_28",
    "rolling_std_7"
]


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Run chronological train/validation/test "
            "backtesting for DineIQ demand forecasting."
        )
    )

    parser.add_argument(
        "--validation-days",
        type=int,
        default=DEFAULT_VALIDATION_DAYS,
        help=(
            "Number of chronological days reserved "
            "for model selection."
        )
    )

    parser.add_argument(
        "--test-days",
        type=int,
        default=DEFAULT_TEST_DAYS,
        help=(
            "Number of final chronological days "
            "reserved as unseen test data."
        )
    )

    return parser.parse_args()


def add_historical_features(
    series,
    historical_start
):
    featured = series.sort_values(
        "order_date"
    ).copy()

    dates = pd.to_datetime(
        featured["order_date"]
    )

    featured["trend_index"] = (
        dates -
        pd.Timestamp(historical_start)
    ).dt.days.astype(float)

    day_of_week = dates.dt.dayofweek
    month = dates.dt.month
    day_of_month = dates.dt.day

    featured["day_of_week_sin"] = np.sin(
        2 * np.pi * day_of_week / 7
    )

    featured["day_of_week_cos"] = np.cos(
        2 * np.pi * day_of_week / 7
    )

    featured["month_sin"] = np.sin(
        2 * np.pi * (month - 1) / 12
    )

    featured["month_cos"] = np.cos(
        2 * np.pi * (month - 1) / 12
    )

    featured["day_of_month_sin"] = np.sin(
        2 * np.pi * (day_of_month - 1) / 31
    )

    featured["day_of_month_cos"] = np.cos(
        2 * np.pi * (day_of_month - 1) / 31
    )

    featured["is_weekend"] = (
        day_of_week >= 4
    ).astype(int)

    featured["lag_1"] = featured[
        "demand"
    ].shift(1)

    featured["lag_7"] = featured[
        "demand"
    ].shift(7)

    featured["lag_14"] = featured[
        "demand"
    ].shift(14)

    featured["lag_28"] = featured[
        "demand"
    ].shift(28)

    shifted_demand = featured[
        "demand"
    ].shift(1)

    featured["rolling_mean_7"] = shifted_demand.rolling(
        window=7,
        min_periods=7
    ).mean()

    featured["rolling_mean_28"] = shifted_demand.rolling(
        window=28,
        min_periods=28
    ).mean()

    featured["rolling_std_7"] = shifted_demand.rolling(
        window=7,
        min_periods=7
    ).std().fillna(0.0)

    return featured


def build_future_features(
    forecast_date,
    historical_start,
    history_values
):
    date = pd.Timestamp(
        forecast_date
    )

    day_of_week = date.dayofweek
    month = date.month
    day_of_month = date.day

    return {
        "trend_index": float(
            (
                date -
                pd.Timestamp(historical_start)
            ).days
        ),
        "day_of_week_sin": np.sin(
            2 * np.pi * day_of_week / 7
        ),
        "day_of_week_cos": np.cos(
            2 * np.pi * day_of_week / 7
        ),
        "month_sin": np.sin(
            2 * np.pi * (month - 1) / 12
        ),
        "month_cos": np.cos(
            2 * np.pi * (month - 1) / 12
        ),
        "day_of_month_sin": np.sin(
            2 * np.pi * (day_of_month - 1) / 31
        ),
        "day_of_month_cos": np.cos(
            2 * np.pi * (day_of_month - 1) / 31
        ),
        "is_weekend": int(
            day_of_week >= 4
        ),
        "lag_1": float(
            history_values[-1]
        ),
        "lag_7": float(
            history_values[-7]
        ),
        "lag_14": float(
            history_values[-14]
        ),
        "lag_28": float(
            history_values[-28]
        ),
        "rolling_mean_7": float(
            np.mean(
                history_values[-7:]
            )
        ),
        "rolling_mean_28": float(
            np.mean(
                history_values[-28:]
            )
        ),
        "rolling_std_7": float(
            np.std(
                history_values[-7:],
                ddof=1
            )
        )
    }


def train_model(
    series,
    historical_start,
    alpha
):
    featured = add_historical_features(
        series,
        historical_start
    )

    training_data = featured.dropna(
        subset=feature_columns + ["demand"]
    ).copy()

    if (
        len(training_data) < MINIMUM_TRAINING_ROWS or
        training_data["demand"].nunique() <= 1
    ):
        return None

    model = Pipeline([
        (
            "scaler",
            StandardScaler()
        ),
        (
            "regressor",
            Ridge(
                alpha=alpha
            )
        )
    ])

    model.fit(
        training_data[
            feature_columns
        ],
        training_data["demand"]
    )

    return model


def recursive_model_forecast(
    model,
    history,
    forecast_dates,
    historical_start
):
    history_values = history[
        "demand"
    ].astype(float).tolist()

    predictions = []

    for forecast_date in forecast_dates:
        if model is None:
            if len(history_values) >= 7:
                prediction = float(
                    history_values[-7]
                )
            else:
                prediction = float(
                    np.mean(history_values)
                )
        else:
            future_features = build_future_features(
                forecast_date,
                historical_start,
                history_values
            )

            feature_frame = pd.DataFrame([
                future_features
            ])

            prediction = float(
                model.predict(
                    feature_frame[
                        feature_columns
                    ]
                )[0]
            )

        prediction = max(
            0.0,
            prediction
        )

        predictions.append(
            prediction
        )

        history_values.append(
            prediction
        )

    return np.array(
        predictions,
        dtype=float
    )


def recursive_baseline_forecast(
    history,
    forecast_dates
):
    history_values = history[
        "demand"
    ].astype(float).tolist()

    predictions = []

    for _ in forecast_dates:
        if len(history_values) >= 7:
            prediction = float(
                history_values[-7]
            )
        else:
            prediction = float(
                np.mean(history_values)
            )

        prediction = max(
            0.0,
            prediction
        )

        predictions.append(
            prediction
        )

        history_values.append(
            prediction
        )

    return np.array(
        predictions,
        dtype=float
    )


def choose_alpha(
    training_series,
    validation_series,
    historical_start
):
    validation_dates = pd.to_datetime(
        validation_series[
            "order_date"
        ]
    )

    actual = validation_series[
        "demand"
    ].astype(float).to_numpy()

    best_alpha = ALPHA_CANDIDATES[0]
    best_mae = None
    best_model_name = MODEL_NAME

    for alpha in ALPHA_CANDIDATES:
        model = train_model(
            training_series,
            historical_start,
            alpha
        )

        predictions = recursive_model_forecast(
            model,
            training_series,
            validation_dates,
            historical_start
        )

        mae = float(
            np.mean(
                np.abs(
                    actual -
                    predictions
                )
            )
        )

        if (
            best_mae is None or
            mae < best_mae
        ):
            best_alpha = alpha
            best_mae = mae
            best_model_name = (
                MODEL_NAME
                if model is not None
                else "Seasonal Naive Fallback"
            )

    return (
        float(best_alpha),
        float(best_mae),
        best_model_name
    )


def build_prediction_frame(
    actual_series,
    predictions,
    baseline_predictions,
    split_name,
    forecast_origin_date,
    selected_alpha,
    model_name,
    forecast_level,
    entity_id,
    entity_name
):
    output = pd.DataFrame({
        "forecast_date": pd.to_datetime(
            actual_series[
                "order_date"
            ]
        ),
        "split": split_name,
        "actual_demand": actual_series[
            "demand"
        ].astype(float).to_numpy(),
        "predicted_demand": np.round(
            predictions,
            2
        ),
        "baseline_prediction": np.round(
            baseline_predictions,
            2
        ),
        "forecast_origin_date": pd.Timestamp(
            forecast_origin_date
        ),
        "forecast_horizon_day": np.arange(
            1,
            len(actual_series) + 1
        ),
        "selected_alpha": float(
            selected_alpha
        ),
        "forecast_level": forecast_level,
        "model_name": model_name,
        "model_version": MODEL_VERSION,
        "validation_version": VALIDATION_VERSION
    })

    output["entity_id"] = str(
        entity_id
    )

    output["entity_name"] = str(
        entity_name
    )

    return output


def validate_single_series(
    series,
    forecast_level,
    entity_id,
    entity_name,
    historical_start,
    training_end,
    validation_start,
    validation_end,
    test_start,
    test_end
):
    series = series.sort_values(
        "order_date"
    ).copy()

    series["order_date"] = pd.to_datetime(
        series["order_date"]
    )

    training_series = series[
        series["order_date"] <= training_end
    ].copy()

    validation_series = series[
        (
            series["order_date"] >=
            validation_start
        ) &
        (
            series["order_date"] <=
            validation_end
        )
    ].copy()

    test_series = series[
        (
            series["order_date"] >=
            test_start
        ) &
        (
            series["order_date"] <=
            test_end
        )
    ].copy()

    selected_alpha, validation_mae, validation_model_name = choose_alpha(
        training_series,
        validation_series,
        historical_start
    )

    validation_model = train_model(
        training_series,
        historical_start,
        selected_alpha
    )

    validation_predictions = recursive_model_forecast(
        validation_model,
        training_series,
        pd.to_datetime(
            validation_series[
                "order_date"
            ]
        ),
        historical_start
    )

    validation_baseline = recursive_baseline_forecast(
        training_series,
        pd.to_datetime(
            validation_series[
                "order_date"
            ]
        )
    )

    validation_output = build_prediction_frame(
        validation_series,
        validation_predictions,
        validation_baseline,
        "validation",
        training_end,
        selected_alpha,
        validation_model_name,
        forecast_level,
        entity_id,
        entity_name
    )

    final_training_series = series[
        series["order_date"] <= validation_end
    ].copy()

    final_model = train_model(
        final_training_series,
        historical_start,
        selected_alpha
    )

    final_model_name = (
        MODEL_NAME
        if final_model is not None
        else "Seasonal Naive Fallback"
    )

    test_predictions = recursive_model_forecast(
        final_model,
        final_training_series,
        pd.to_datetime(
            test_series[
                "order_date"
            ]
        ),
        historical_start
    )

    test_baseline = recursive_baseline_forecast(
        final_training_series,
        pd.to_datetime(
            test_series[
                "order_date"
            ]
        )
    )

    test_output = build_prediction_frame(
        test_series,
        test_predictions,
        test_baseline,
        "test",
        validation_end,
        selected_alpha,
        final_model_name,
        forecast_level,
        entity_id,
        entity_name
    )

    model_selection = {
        "forecast_level": forecast_level,
        "entity_id": str(
            entity_id
        ),
        "entity_name": str(
            entity_name
        ),
        "selected_alpha": float(
            selected_alpha
        ),
        "validation_mae": round(
            validation_mae,
            4
        ),
        "selection_split": "validation",
        "test_used_for_selection": False,
        "final_training_end_date": pd.Timestamp(
            validation_end
        ),
        "model_name": final_model_name,
        "model_version": MODEL_VERSION,
        "validation_version": VALIDATION_VERSION
    }

    return pd.concat(
        [
            validation_output,
            test_output
        ],
        ignore_index=True
    ), model_selection


def validate_grouped_series(
    history,
    entity_id_column,
    entity_name_column,
    forecast_level,
    historical_start,
    training_end,
    validation_start,
    validation_end,
    test_start,
    test_end,
    progress_every=None
):
    outputs = []
    selections = []

    groups = list(
        history.groupby(
            entity_id_column,
            sort=True
        )
    )

    total_groups = len(
        groups
    )

    for index, (
        entity_id,
        series
    ) in enumerate(
        groups,
        start=1
    ):
        entity_name = series[
            entity_name_column
        ].iloc[0]

        output, selection = validate_single_series(
            series,
            forecast_level,
            entity_id,
            entity_name,
            historical_start,
            training_end,
            validation_start,
            validation_end,
            test_start,
            test_end
        )

        output[
            entity_id_column
        ] = entity_id

        output[
            entity_name_column
        ] = entity_name

        outputs.append(
            output
        )

        selections.append(
            selection
        )

        if (
            progress_every is not None and
            (
                index % progress_every == 0 or
                index == total_groups
            )
        ):
            print(
                f"{forecast_level}: "
                f"{index}/{total_groups} series complete"
            )

    combined = pd.concat(
        outputs,
        ignore_index=True
    )

    return combined, selections


def write_dataframe(
    frame,
    path
):
    output = frame.copy()

    for column in [
        "order_date",
        "forecast_date",
        "forecast_origin_date",
        "historical_start_date",
        "historical_end_date",
        "training_start_date",
        "training_end_date",
        "validation_start_date",
        "validation_end_date",
        "test_start_date",
        "test_end_date",
        "final_training_end_date"
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

if args.validation_days <= 0:
    raise ValueError(
        "Validation days must be greater than zero."
    )

if args.test_days <= 0:
    raise ValueError(
        "Test days must be greater than zero."
    )


historical_overall = spark.read.parquet(
    f"{forecast_folder}/historical_overall_demand"
).toPandas()

historical_items = spark.read.parquet(
    f"{forecast_folder}/historical_menu_item_demand"
).toPandas()

historical_categories = spark.read.parquet(
    f"{forecast_folder}/historical_menu_category_demand"
).toPandas()

historical_locations = spark.read.parquet(
    f"{forecast_folder}/historical_restaurant_location_demand"
).toPandas()

historical_periods = spark.read.parquet(
    f"{forecast_folder}/historical_time_period_demand"
).toPandas()


for frame in [
    historical_overall,
    historical_items,
    historical_categories,
    historical_locations,
    historical_periods
]:
    frame["order_date"] = pd.to_datetime(
        frame["order_date"]
    )


historical_start = pd.Timestamp(
    historical_overall[
        "order_date"
    ].min()
)

historical_end = pd.Timestamp(
    historical_overall[
        "order_date"
    ].max()
)

test_end = historical_end

test_start = (
    test_end -
    pd.Timedelta(
        days=args.test_days - 1
    )
)

validation_end = (
    test_start -
    pd.Timedelta(days=1)
)

validation_start = (
    validation_end -
    pd.Timedelta(
        days=args.validation_days - 1
    )
)

training_start = historical_start

training_end = (
    validation_start -
    pd.Timedelta(days=1)
)


if training_end <= training_start:
    raise ValueError(
        "The configured validation and test windows "
        "leave insufficient chronological training data."
    )


training_days = (
    training_end -
    training_start
).days + 1


if training_days < (
    MINIMUM_TRAINING_ROWS +
    28
):
    raise ValueError(
        "The chronological training window is too short "
        "for the required lag and rolling features."
    )


print("\n========TIME-AWARE VALIDATION SETUP========")
print(
    f"Historical Period: "
    f"{historical_start.date()} "
    f"to {historical_end.date()}"
)
print(
    f"Training Period: "
    f"{training_start.date()} "
    f"to {training_end.date()}"
)
print(
    f"Validation Period: "
    f"{validation_start.date()} "
    f"to {validation_end.date()}"
)
print(
    f"Test Period: "
    f"{test_start.date()} "
    f"to {test_end.date()}"
)
print(
    f"Validation Days: "
    f"{args.validation_days}"
)
print(
    f"Test Days: "
    f"{args.test_days}"
)
print(
    "Random Split Used: No"
)
print(
    "Future Actuals Used as Forecast Features: No"
)
print(
    "Forecast Method: Recursive multi-step"
)


print("\nValidating overall daily demand...")

overall_output, overall_selection = validate_single_series(
    historical_overall,
    "Overall Daily",
    "overall",
    "Overall Demand",
    historical_start,
    training_end,
    validation_start,
    validation_end,
    test_start,
    test_end
)


print("\nValidating menu-item demand...")

item_output, item_selections = validate_grouped_series(
    historical_items,
    "item_id",
    "item_name",
    "Menu Item",
    historical_start,
    training_end,
    validation_start,
    validation_end,
    test_start,
    test_end,
    progress_every=25
)


print("\nValidating menu-category demand...")

category_output, category_selections = validate_grouped_series(
    historical_categories,
    "category_id",
    "category_name",
    "Menu Category",
    historical_start,
    training_end,
    validation_start,
    validation_end,
    test_start,
    test_end,
    progress_every=6
)


print("\nValidating restaurant-location demand...")

location_output, location_selections = validate_grouped_series(
    historical_locations,
    "restaurant_id",
    "restaurant_name",
    "Restaurant Location",
    historical_start,
    training_end,
    validation_start,
    validation_end,
    test_start,
    test_end,
    progress_every=5
)


print("\nValidating selected time-period demand...")

period_output, period_selections = validate_grouped_series(
    historical_periods,
    "order_hour",
    "time_period",
    "Selected Time Period",
    historical_start,
    training_end,
    validation_start,
    validation_end,
    test_start,
    test_end,
    progress_every=6
)


model_selection = pd.DataFrame(
    [
        overall_selection,
        *item_selections,
        *category_selections,
        *location_selections,
        *period_selections
    ]
)


split_metadata = pd.DataFrame([
    {
        "historical_start_date": historical_start,
        "historical_end_date": historical_end,
        "training_start_date": training_start,
        "training_end_date": training_end,
        "validation_start_date": validation_start,
        "validation_end_date": validation_end,
        "test_start_date": test_start,
        "test_end_date": test_end,
        "training_days": int(
            training_days
        ),
        "validation_days": int(
            args.validation_days
        ),
        "test_days": int(
            args.test_days
        ),
        "random_split_used": False,
        "future_actuals_used_as_features": False,
        "feature_history_policy": (
            "past_actuals_then_recursive_predictions"
        ),
        "model_selection_split": "validation",
        "test_used_for_model_selection": False,
        "baseline_method": "seasonal_naive_lag_7_recursive",
        "alpha_candidates": ",".join(
            str(alpha)
            for alpha in ALPHA_CANDIDATES
        ),
        "model_name": MODEL_NAME,
        "model_version": MODEL_VERSION,
        "validation_version": VALIDATION_VERSION
    }
])


print("\n========OVERALL TEST BACKTEST========")

print(
    overall_output[
        overall_output["split"] == "test"
    ][
        [
            "forecast_date",
            "actual_demand",
            "predicted_demand",
            "baseline_prediction"
        ]
    ].to_string(
        index=False
    )
)


print("\n========MODEL SELECTION SUMMARY========")

selection_summary = model_selection.groupby(
    [
        "forecast_level",
        "selected_alpha"
    ]
).size().reset_index(
    name="series_count"
)

print(
    selection_summary.to_string(
        index=False
    )
)


write_dataframe(
    overall_output,
    f"{output_folder}/overall_backtest"
)

write_dataframe(
    item_output,
    f"{output_folder}/menu_item_backtest"
)

write_dataframe(
    category_output,
    f"{output_folder}/menu_category_backtest"
)

write_dataframe(
    location_output,
    f"{output_folder}/restaurant_location_backtest"
)

write_dataframe(
    period_output,
    f"{output_folder}/time_period_backtest"
)

write_dataframe(
    model_selection,
    f"{output_folder}/model_selection"
)

write_dataframe(
    split_metadata,
    f"{output_folder}/split_metadata"
)


print("\n========TIME-AWARE VALIDATION COMPLETE========")
print(
    f"Overall Backtest Rows: "
    f"{len(overall_output):,}"
)
print(
    f"Menu Item Backtest Rows: "
    f"{len(item_output):,}"
)
print(
    f"Menu Category Backtest Rows: "
    f"{len(category_output):,}"
)
print(
    f"Restaurant Location Backtest Rows: "
    f"{len(location_output):,}"
)
print(
    f"Time-Period Backtest Rows: "
    f"{len(period_output):,}"
)
print(
    f"Model Selection Rows: "
    f"{len(model_selection):,}"
)
print(
    f"Validation Version: "
    f"{VALIDATION_VERSION}"
)


spark.stop()
