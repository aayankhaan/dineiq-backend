import argparse
import os
import joblib
import numpy as np
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from config.settings import INTEGRATED_DATA_FOLDER, ML_DATA_FOLDER, MODEL_FOLDER


spark = SparkSession.builder \
    .appName("DineIQ Demand Forecasting") \
    .getOrCreate()


integrated_folder = INTEGRATED_DATA_FOLDER
output_folder = f"{ML_DATA_FOLDER}/forecasting"
model_folder = MODEL_FOLDER

MODEL_NAME = "Ridge Time-Series Regression"
MODEL_VERSION = "demand_forecasting_v1"
DEFAULT_FORECAST_DAYS = 30
MINIMUM_TRAINING_ROWS = 60

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
        description="Generate DineIQ future demand forecasts."
    )

    parser.add_argument(
        "--forecast-days",
        type=int,
        default=DEFAULT_FORECAST_DAYS,
        help="Number of future days to forecast."
    )

    parser.add_argument(
        "--hours",
        type=str,
        default="",
        help=(
            "Optional comma-separated order hours to forecast, "
            "for example 12,13,19,20. "
            "If omitted, all observed order hours are used."
        )
    )

    return parser.parse_args()


def parse_selected_hours(hours_argument):
    if not hours_argument.strip():
        return None

    selected_hours = sorted({
        int(value.strip())
        for value in hours_argument.split(",")
        if value.strip()
    })

    invalid_hours = [
        hour
        for hour in selected_hours
        if hour < 0 or hour > 23
    ]

    if invalid_hours:
        raise ValueError(
            f"Invalid forecast hours: {invalid_hours}. "
            f"Hours must be between 0 and 23."
        )

    return selected_hours


def complete_daily_series(
    frame,
    historical_start,
    historical_end,
    entity_id_column=None,
    entity_name_column=None
):
    frame = frame.copy()
    frame["order_date"] = pd.to_datetime(
        frame["order_date"]
    )

    all_dates = pd.DataFrame({
        "order_date": pd.date_range(
            historical_start,
            historical_end,
            freq="D"
        )
    })

    if entity_id_column is None:
        completed = all_dates.merge(
            frame[
                [
                    "order_date",
                    "demand"
                ]
            ],
            on="order_date",
            how="left"
        )

        completed["demand"] = completed[
            "demand"
        ].fillna(0.0)

        return completed.sort_values(
            "order_date"
        ).reset_index(drop=True)

    entity_columns = [
        entity_id_column
    ]

    if entity_name_column is not None:
        entity_columns.append(
            entity_name_column
        )

    entities = frame[
        entity_columns
    ].drop_duplicates().reset_index(
        drop=True
    )

    entities["_join_key"] = 1
    all_dates["_join_key"] = 1

    grid = entities.merge(
        all_dates,
        on="_join_key",
        how="inner"
    ).drop(
        columns=["_join_key"]
    )

    completed = grid.merge(
        frame,
        on=[
            *entity_columns,
            "order_date"
        ],
        how="left"
    )

    completed["demand"] = completed[
        "demand"
    ].fillna(0.0)

    return completed.sort_values(
        [
            entity_id_column,
            "order_date"
        ]
    ).reset_index(drop=True)


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


def train_series_model(
    series,
    historical_start
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
                alpha=5.0
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


def forecast_series(
    series,
    forecast_days,
    historical_start,
    historical_end
):
    series = series.sort_values(
        "order_date"
    ).copy()

    model = train_series_model(
        series,
        historical_start
    )

    history_values = series[
        "demand"
    ].astype(float).tolist()

    forecasts = []

    for horizon_day in range(
        1,
        forecast_days + 1
    ):
        forecast_date = (
            pd.Timestamp(historical_end) +
            pd.Timedelta(days=horizon_day)
        )

        if model is None:
            if len(history_values) >= 7:
                prediction = float(
                    history_values[-7]
                )
            else:
                prediction = float(
                    np.mean(history_values)
                )

            model_name = "Seasonal Naive Fallback"
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

            model_name = MODEL_NAME

        prediction = max(
            0.0,
            prediction
        )

        forecasts.append({
            "forecast_date": forecast_date,
            "forecast_horizon_day": horizon_day,
            "predicted_demand": round(
                prediction,
                2
            ),
            "model_name": model_name,
            "model_version": MODEL_VERSION
        })

        history_values.append(
            prediction
        )

    return pd.DataFrame(
        forecasts
    ), model


def forecast_grouped_series(
    history,
    entity_id_column,
    entity_name_column,
    forecast_level,
    forecast_days,
    historical_start,
    historical_end
):
    forecast_frames = []
    trained_models = {}

    for entity_id, series in history.groupby(
        entity_id_column,
        sort=True
    ):
        series = series.sort_values(
            "order_date"
        ).copy()

        entity_name = None

        if entity_name_column is not None:
            entity_name = series[
                entity_name_column
            ].iloc[0]

        forecasts, model = forecast_series(
            series,
            forecast_days,
            historical_start,
            historical_end
        )

        forecasts[entity_id_column] = entity_id

        if entity_name_column is not None:
            forecasts[entity_name_column] = entity_name

        forecasts["forecast_level"] = forecast_level
        forecasts["training_end_date"] = pd.Timestamp(
            historical_end
        )

        forecast_frames.append(
            forecasts
        )

        trained_models[
            entity_id
        ] = model

    combined = pd.concat(
        forecast_frames,
        ignore_index=True
    )

    output_columns = [
        "forecast_date",
        entity_id_column
    ]

    if entity_name_column is not None:
        output_columns.append(
            entity_name_column
        )

    output_columns.extend([
        "forecast_level",
        "forecast_horizon_day",
        "predicted_demand",
        "training_end_date",
        "model_name",
        "model_version"
    ])

    return combined[
        output_columns
    ], trained_models


def write_dataframe(
    frame,
    path
):
    output = frame.copy()

    for column in [
        "order_date",
        "forecast_date",
        "training_end_date",
        "historical_start_date",
        "historical_end_date",
        "forecast_start_date",
        "forecast_end_date"
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

if args.forecast_days <= 0:
    raise ValueError(
        "Forecast days must be greater than zero."
    )

selected_hours = parse_selected_hours(
    args.hours
)

os.makedirs(
    output_folder,
    exist_ok=True
)

os.makedirs(
    model_folder,
    exist_ok=True
)

transactions = spark.read.parquet(
    f"{integrated_folder}/transactions"
).select(
    "order_id",
    "order_datetime",
    "restaurant_id",
    "restaurant_name",
    "item_id",
    "item_name",
    "category_id",
    "category_name",
    "quantity"
).filter(
    F.col("quantity") > 0
).withColumn(
    "order_date",
    F.to_date("order_datetime")
).withColumn(
    "order_hour",
    F.hour("order_datetime")
)


date_range = transactions.agg(
    F.min("order_date").alias(
        "historical_start"
    ),
    F.max("order_date").alias(
        "historical_end"
    )
).first()

historical_start = pd.Timestamp(
    date_range["historical_start"]
)

historical_end = pd.Timestamp(
    date_range["historical_end"]
)

forecast_start = (
    historical_end +
    pd.Timedelta(days=1)
)

forecast_end = (
    historical_end +
    pd.Timedelta(
        days=args.forecast_days
    )
)


print("\n========DEMAND FORECASTING SETUP========")
print(
    f"Historical Period: "
    f"{historical_start.date()} "
    f"to {historical_end.date()}"
)
print(
    f"Forecast Period: "
    f"{forecast_start.date()} "
    f"to {forecast_end.date()}"
)
print(
    f"Forecast Days: "
    f"{args.forecast_days}"
)
print(
    f"Target: Quantity Demand"
)


overall_history_raw = transactions.groupBy(
    "order_date"
).agg(
    F.sum("quantity").cast(
        "double"
    ).alias(
        "demand"
    )
).orderBy(
    "order_date"
).toPandas()


item_history_raw = transactions.groupBy(
    "order_date",
    "item_id",
    "item_name"
).agg(
    F.sum("quantity").cast(
        "double"
    ).alias(
        "demand"
    )
).orderBy(
    "item_id",
    "order_date"
).toPandas()


category_history_raw = transactions.groupBy(
    "order_date",
    "category_id",
    "category_name"
).agg(
    F.sum("quantity").cast(
        "double"
    ).alias(
        "demand"
    )
).orderBy(
    "category_id",
    "order_date"
).toPandas()


location_history_raw = transactions.groupBy(
    "order_date",
    "restaurant_id",
    "restaurant_name"
).agg(
    F.sum("quantity").cast(
        "double"
    ).alias(
        "demand"
    )
).orderBy(
    "restaurant_id",
    "order_date"
).toPandas()


time_period_source = transactions

if selected_hours is not None:
    time_period_source = time_period_source.filter(
        F.col("order_hour").isin(
            selected_hours
        )
    )


time_period_history_raw = time_period_source.groupBy(
    "order_date",
    "order_hour"
).agg(
    F.sum("quantity").cast(
        "double"
    ).alias(
        "demand"
    )
).orderBy(
    "order_hour",
    "order_date"
).toPandas()


if time_period_history_raw.empty:
    raise ValueError(
        "No order records exist for the selected forecast hours."
    )


observed_hours = sorted(
    int(hour)
    for hour in time_period_history_raw[
        "order_hour"
    ].dropna().unique()
)


overall_history = complete_daily_series(
    overall_history_raw,
    historical_start,
    historical_end
)


item_history = complete_daily_series(
    item_history_raw,
    historical_start,
    historical_end,
    "item_id",
    "item_name"
)


category_history = complete_daily_series(
    category_history_raw,
    historical_start,
    historical_end,
    "category_id",
    "category_name"
)


location_history = complete_daily_series(
    location_history_raw,
    historical_start,
    historical_end,
    "restaurant_id",
    "restaurant_name"
)


time_period_history_raw[
    "time_period"
] = time_period_history_raw[
    "order_hour"
].apply(
    lambda hour: (
        f"{int(hour):02d}:00-"
        f"{int(hour):02d}:59"
    )
)


time_period_history = complete_daily_series(
    time_period_history_raw,
    historical_start,
    historical_end,
    "order_hour",
    "time_period"
)


overall_forecast, overall_model = forecast_series(
    overall_history,
    args.forecast_days,
    historical_start,
    historical_end
)

overall_forecast[
    "forecast_level"
] = "Overall Daily"

overall_forecast[
    "training_end_date"
] = historical_end

overall_forecast = overall_forecast[
    [
        "forecast_date",
        "forecast_level",
        "forecast_horizon_day",
        "predicted_demand",
        "training_end_date",
        "model_name",
        "model_version"
    ]
]


item_forecast, item_models = forecast_grouped_series(
    item_history,
    "item_id",
    "item_name",
    "Menu Item",
    args.forecast_days,
    historical_start,
    historical_end
)


category_forecast, category_models = forecast_grouped_series(
    category_history,
    "category_id",
    "category_name",
    "Menu Category",
    args.forecast_days,
    historical_start,
    historical_end
)


location_forecast, location_models = forecast_grouped_series(
    location_history,
    "restaurant_id",
    "restaurant_name",
    "Restaurant Location",
    args.forecast_days,
    historical_start,
    historical_end
)


time_period_forecast, time_period_models = forecast_grouped_series(
    time_period_history,
    "order_hour",
    "time_period",
    "Selected Time Period",
    args.forecast_days,
    historical_start,
    historical_end
)


print("\n========OVERALL DAILY FORECAST========")
print(
    overall_forecast[
        [
            "forecast_date",
            "forecast_horizon_day",
            "predicted_demand"
        ]
    ].to_string(
        index=False
    )
)


print("\n========TOP MENU ITEM FORECASTS========")
print(
    item_forecast.groupby(
        [
            "item_id",
            "item_name"
        ],
        as_index=False
    )["predicted_demand"].sum().sort_values(
        "predicted_demand",
        ascending=False
    ).head(
        10
    ).to_string(
        index=False
    )
)


print("\n========MENU CATEGORY FORECASTS========")
print(
    category_forecast.groupby(
        [
            "category_id",
            "category_name"
        ],
        as_index=False
    )["predicted_demand"].sum().sort_values(
        "predicted_demand",
        ascending=False
    ).to_string(
        index=False
    )
)


print("\n========RESTAURANT LOCATION FORECASTS========")
print(
    location_forecast.groupby(
        [
            "restaurant_id",
            "restaurant_name"
        ],
        as_index=False
    )["predicted_demand"].sum().sort_values(
        "predicted_demand",
        ascending=False
    ).to_string(
        index=False
    )
)


print("\n========SELECTED TIME-PERIOD FORECASTS========")
print(
    time_period_forecast.groupby(
        [
            "order_hour",
            "time_period"
        ],
        as_index=False
    )["predicted_demand"].sum().sort_values(
        "order_hour"
    ).to_string(
        index=False
    )
)


write_dataframe(
    overall_history,
    f"{output_folder}/historical_overall_demand"
)

write_dataframe(
    item_history,
    f"{output_folder}/historical_menu_item_demand"
)

write_dataframe(
    category_history,
    f"{output_folder}/historical_menu_category_demand"
)

write_dataframe(
    location_history,
    f"{output_folder}/historical_restaurant_location_demand"
)

write_dataframe(
    time_period_history,
    f"{output_folder}/historical_time_period_demand"
)

write_dataframe(
    overall_forecast,
    f"{output_folder}/overall_daily_forecast"
)

write_dataframe(
    item_forecast,
    f"{output_folder}/menu_item_forecast"
)

write_dataframe(
    category_forecast,
    f"{output_folder}/menu_category_forecast"
)

write_dataframe(
    location_forecast,
    f"{output_folder}/restaurant_location_forecast"
)

write_dataframe(
    time_period_forecast,
    f"{output_folder}/time_period_forecast"
)


metadata = pd.DataFrame([
    {
        "model_name": MODEL_NAME,
        "model_version": MODEL_VERSION,
        "forecast_target": "quantity",
        "forecast_unit": "menu_item_units",
        "historical_start_date": historical_start,
        "historical_end_date": historical_end,
        "forecast_start_date": forecast_start,
        "forecast_end_date": forecast_end,
        "forecast_days": int(
            args.forecast_days
        ),
        "selected_hours": ",".join(
            str(hour)
            for hour in observed_hours
        ),
        "menu_item_series": int(
            item_history[
                "item_id"
            ].nunique()
        ),
        "menu_category_series": int(
            category_history[
                "category_id"
            ].nunique()
        ),
        "restaurant_location_series": int(
            location_history[
                "restaurant_id"
            ].nunique()
        ),
        "time_period_series": int(
            time_period_history[
                "order_hour"
            ].nunique()
        )
    }
])


write_dataframe(
    metadata,
    f"{output_folder}/forecast_metadata"
)


joblib.dump(
    {
        "model_name": MODEL_NAME,
        "model_version": MODEL_VERSION,
        "feature_columns": feature_columns,
        "model": overall_model
    },
    f"{model_folder}/{MODEL_VERSION}_overall.joblib"
)

joblib.dump(
    {
        "model_name": MODEL_NAME,
        "model_version": MODEL_VERSION,
        "feature_columns": feature_columns,
        "models": item_models
    },
    f"{model_folder}/{MODEL_VERSION}_menu_items.joblib"
)

joblib.dump(
    {
        "model_name": MODEL_NAME,
        "model_version": MODEL_VERSION,
        "feature_columns": feature_columns,
        "models": category_models
    },
    f"{model_folder}/{MODEL_VERSION}_menu_categories.joblib"
)

joblib.dump(
    {
        "model_name": MODEL_NAME,
        "model_version": MODEL_VERSION,
        "feature_columns": feature_columns,
        "models": location_models
    },
    f"{model_folder}/{MODEL_VERSION}_restaurant_locations.joblib"
)

joblib.dump(
    {
        "model_name": MODEL_NAME,
        "model_version": MODEL_VERSION,
        "feature_columns": feature_columns,
        "models": time_period_models
    },
    f"{model_folder}/{MODEL_VERSION}_time_periods.joblib"
)


print("\n========DEMAND FORECASTING COMPLETE========")
print(
    f"Overall Forecast Rows: "
    f"{len(overall_forecast):,}"
)
print(
    f"Menu Item Forecast Rows: "
    f"{len(item_forecast):,}"
)
print(
    f"Menu Category Forecast Rows: "
    f"{len(category_forecast):,}"
)
print(
    f"Restaurant Location Forecast Rows: "
    f"{len(location_forecast):,}"
)
print(
    f"Time-Period Forecast Rows: "
    f"{len(time_period_forecast):,}"
)
print(
    f"Model Version: "
    f"{MODEL_VERSION}"
)


spark.stop()
