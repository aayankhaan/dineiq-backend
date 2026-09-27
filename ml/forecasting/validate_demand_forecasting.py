from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import ML_DATA_FOLDER, MODEL_FOLDER


spark = SparkSession.builder \
    .appName("Validate DineIQ Demand Forecasting") \
    .getOrCreate()


output_folder = f"{ML_DATA_FOLDER}/forecasting"
model_folder = MODEL_FOLDER

MODEL_VERSION = "demand_forecasting_v1"


outputs = {
    "Historical overall demand": (
        f"{output_folder}/historical_overall_demand"
    ),
    "Historical menu item demand": (
        f"{output_folder}/historical_menu_item_demand"
    ),
    "Historical menu category demand": (
        f"{output_folder}/historical_menu_category_demand"
    ),
    "Historical restaurant location demand": (
        f"{output_folder}/historical_restaurant_location_demand"
    ),
    "Historical time-period demand": (
        f"{output_folder}/historical_time_period_demand"
    ),
    "Overall daily forecast": (
        f"{output_folder}/overall_daily_forecast"
    ),
    "Menu item forecast": (
        f"{output_folder}/menu_item_forecast"
    ),
    "Menu category forecast": (
        f"{output_folder}/menu_category_forecast"
    ),
    "Restaurant location forecast": (
        f"{output_folder}/restaurant_location_forecast"
    ),
    "Time-period forecast": (
        f"{output_folder}/time_period_forecast"
    ),
    "Forecast metadata": (
        f"{output_folder}/forecast_metadata"
    )
}


model_paths = [
    f"{model_folder}/{MODEL_VERSION}_overall.joblib",
    f"{model_folder}/{MODEL_VERSION}_menu_items.joblib",
    f"{model_folder}/{MODEL_VERSION}_menu_categories.joblib",
    f"{model_folder}/{MODEL_VERSION}_restaurant_locations.joblib",
    f"{model_folder}/{MODEL_VERSION}_time_periods.joblib"
]


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


print("\n========DEMAND FORECASTING VALIDATION========")

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


for model_path in model_paths:
    check(
        f"Saved model exists: {Path(model_path).name}",
        Path(model_path).exists()
    )


if not all_outputs_exist:
    total_checks = passed + failed

    print(
        f"\n========VALIDATION RESULT: "
        f"{passed}/{total_checks} PASS========"
    )

    spark.stop()

    raise SystemExit(1)


historical_overall = spark.read.parquet(
    outputs["Historical overall demand"]
)

historical_items = spark.read.parquet(
    outputs["Historical menu item demand"]
)

historical_categories = spark.read.parquet(
    outputs["Historical menu category demand"]
)

historical_locations = spark.read.parquet(
    outputs["Historical restaurant location demand"]
)

historical_periods = spark.read.parquet(
    outputs["Historical time-period demand"]
)

overall_forecast = spark.read.parquet(
    outputs["Overall daily forecast"]
)

item_forecast = spark.read.parquet(
    outputs["Menu item forecast"]
)

category_forecast = spark.read.parquet(
    outputs["Menu category forecast"]
)

location_forecast = spark.read.parquet(
    outputs["Restaurant location forecast"]
)

time_period_forecast = spark.read.parquet(
    outputs["Time-period forecast"]
)

metadata = spark.read.parquet(
    outputs["Forecast metadata"]
)



check(
    "Forecast metadata contains exactly one row",
    metadata.count() == 1
)


metadata_row = metadata.first()


def as_date(value):
    if hasattr(value, "date"):
        return value.date()

    return value


forecast_days = int(
    metadata_row["forecast_days"]
)

historical_start = as_date(
    metadata_row[
        "historical_start_date"
    ]
)

historical_end = as_date(
    metadata_row[
        "historical_end_date"
    ]
)

forecast_start = as_date(
    metadata_row[
        "forecast_start_date"
    ]
)

forecast_end = as_date(
    metadata_row[
        "forecast_end_date"
    ]
)


check(
    "Forecast period is configurable and positive",
    forecast_days > 0,
    f"Forecast days: {forecast_days}"
)

check(
    "Future forecast starts after historical data ends",
    forecast_start > historical_end,
    (
        f"History ends {historical_end}, "
        f"forecast starts {forecast_start}"
    )
)

check(
    "Forecast metadata uses expected model version",
    metadata_row["model_version"] == MODEL_VERSION,
    f"Found: {metadata_row['model_version']}"
)

check(
    "Forecast target is quantity demand",
    metadata_row["forecast_target"] == "quantity",
    f"Found: {metadata_row['forecast_target']}"
)


historical_frames = [
    (
        "Overall historical demand",
        historical_overall
    ),
    (
        "Menu item historical demand",
        historical_items
    ),
    (
        "Menu category historical demand",
        historical_categories
    ),
    (
        "Restaurant location historical demand",
        historical_locations
    ),
    (
        "Time-period historical demand",
        historical_periods
    )
]


for name, frame in historical_frames:
    check(
        f"{name} contains data",
        frame.count() > 0
    )

    check(
        f"{name} contains no negative demand",
        frame.filter(
            F.col("demand") < 0
        ).count() == 0
    )


check(
    "Historical overall demand starts on metadata start date",
    historical_overall.agg(
        F.min("order_date").alias(
            "minimum_date"
        )
    ).first()["minimum_date"] == historical_start
)

check(
    "Historical overall demand ends on metadata end date",
    historical_overall.agg(
        F.max("order_date").alias(
            "maximum_date"
        )
    ).first()["maximum_date"] == historical_end
)


common_forecast_columns = {
    "forecast_date",
    "forecast_level",
    "forecast_horizon_day",
    "predicted_demand",
    "training_end_date",
    "model_name",
    "model_version"
}


forecast_frames = [
    (
        "Overall",
        overall_forecast
    ),
    (
        "Menu item",
        item_forecast
    ),
    (
        "Menu category",
        category_forecast
    ),
    (
        "Restaurant location",
        location_forecast
    ),
    (
        "Time-period",
        time_period_forecast
    )
]


for name, frame in forecast_frames:
    check(
        f"{name} forecast contains data",
        frame.count() > 0
    )

    check(
        f"{name} forecast contains required common columns",
        common_forecast_columns.issubset(
            set(frame.columns)
        )
    )

    check(
        f"{name} forecast contains no negative predictions",
        frame.filter(
            F.col("predicted_demand") < 0
        ).count() == 0
    )

    check(
        f"{name} forecast contains no null predictions",
        frame.filter(
            F.col("predicted_demand").isNull()
        ).count() == 0
    )

    check(
        f"{name} forecast dates are strictly future dates",
        frame.filter(
            F.col("forecast_date") <= F.col(
                "training_end_date"
            )
        ).count() == 0
    )

    check(
        f"{name} forecast uses expected model version",
        frame.filter(
            F.col("model_version") != MODEL_VERSION
        ).count() == 0
    )

    distinct_dates = frame.select(
        "forecast_date"
    ).distinct().count()

    check(
        f"{name} forecast covers configured horizon",
        distinct_dates == forecast_days,
        (
            f"Expected {forecast_days}, "
            f"found {distinct_dates}"
        )
    )


check(
    "Menu item forecast contains item identifiers and names",
    {
        "item_id",
        "item_name"
    }.issubset(
        set(item_forecast.columns)
    )
)

check(
    "Menu category forecast contains category identifiers and names",
    {
        "category_id",
        "category_name"
    }.issubset(
        set(category_forecast.columns)
    )
)

check(
    "Restaurant location forecast contains location identifiers and names",
    {
        "restaurant_id",
        "restaurant_name"
    }.issubset(
        set(location_forecast.columns)
    )
)

check(
    "Selected time-period forecast contains hour and period fields",
    {
        "order_hour",
        "time_period"
    }.issubset(
        set(time_period_forecast.columns)
    )
)


check(
    "All historical menu items receive forecasts",
    historical_items.select(
        "item_id"
    ).distinct().count() ==
    item_forecast.select(
        "item_id"
    ).distinct().count()
)

check(
    "All historical menu categories receive forecasts",
    historical_categories.select(
        "category_id"
    ).distinct().count() ==
    category_forecast.select(
        "category_id"
    ).distinct().count()
)

check(
    "All historical restaurant locations receive forecasts",
    historical_locations.select(
        "restaurant_id"
    ).distinct().count() ==
    location_forecast.select(
        "restaurant_id"
    ).distinct().count()
)

check(
    "All selected historical time periods receive forecasts",
    historical_periods.select(
        "order_hour"
    ).distinct().count() ==
    time_period_forecast.select(
        "order_hour"
    ).distinct().count()
)


item_incomplete = item_forecast.groupBy(
    "item_id"
).agg(
    F.countDistinct(
        "forecast_date"
    ).alias(
        "forecast_dates"
    )
).filter(
    F.col("forecast_dates") != forecast_days
).count()


category_incomplete = category_forecast.groupBy(
    "category_id"
).agg(
    F.countDistinct(
        "forecast_date"
    ).alias(
        "forecast_dates"
    )
).filter(
    F.col("forecast_dates") != forecast_days
).count()


location_incomplete = location_forecast.groupBy(
    "restaurant_id"
).agg(
    F.countDistinct(
        "forecast_date"
    ).alias(
        "forecast_dates"
    )
).filter(
    F.col("forecast_dates") != forecast_days
).count()


period_incomplete = time_period_forecast.groupBy(
    "order_hour"
).agg(
    F.countDistinct(
        "forecast_date"
    ).alias(
        "forecast_dates"
    )
).filter(
    F.col("forecast_dates") != forecast_days
).count()


check(
    "Every menu item has a complete forecast horizon",
    item_incomplete == 0,
    f"Incomplete item series: {item_incomplete}"
)

check(
    "Every menu category has a complete forecast horizon",
    category_incomplete == 0,
    f"Incomplete category series: {category_incomplete}"
)

check(
    "Every restaurant location has a complete forecast horizon",
    location_incomplete == 0,
    f"Incomplete location series: {location_incomplete}"
)

check(
    "Every selected time period has a complete forecast horizon",
    period_incomplete == 0,
    f"Incomplete time-period series: {period_incomplete}"
)


check(
    "Overall forecast starts on metadata forecast start date",
    overall_forecast.agg(
        F.min("forecast_date").alias(
            "minimum_date"
        )
    ).first()["minimum_date"] == forecast_start
)

check(
    "Overall forecast ends on metadata forecast end date",
    overall_forecast.agg(
        F.max("forecast_date").alias(
            "maximum_date"
        )
    ).first()["maximum_date"] == forecast_end
)


invalid_horizon_rows = overall_forecast.filter(
    (F.col("forecast_horizon_day") < 1) |
    (
        F.col("forecast_horizon_day") >
        forecast_days
    )
).count()


check(
    "Forecast horizon day values are valid",
    invalid_horizon_rows == 0,
    f"Invalid rows: {invalid_horizon_rows}"
)


invalid_hours = time_period_forecast.filter(
    (F.col("order_hour") < 0) |
    (F.col("order_hour") > 23)
).count()


check(
    "Selected time-period hours are valid",
    invalid_hours == 0,
    f"Invalid rows: {invalid_hours}"
)


model_names = set()

for _, frame in forecast_frames:
    model_names.update(
        row["model_name"]
        for row in frame.select(
            "model_name"
        ).distinct().collect()
    )


check(
    "Forecasts identify the model used",
    len(model_names) > 0 and
    None not in model_names,
    f"Found: {sorted(model_names)}"
)



total_checks = passed + failed


print(
    f"\n========VALIDATION RESULT: "
    f"{passed}/{total_checks} PASS========"
)


if failed == 0:
    print(
        "\nDemand forecasting validation PASSED."
    )
else:
    print(
        "\nDemand forecasting validation FAILED."
    )


spark.stop()
