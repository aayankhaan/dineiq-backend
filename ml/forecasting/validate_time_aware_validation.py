from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import ML_DATA_FOLDER


spark = SparkSession.builder \
    .appName("Validate DineIQ Time-Aware Forecast Validation") \
    .getOrCreate()


forecast_folder = f"{ML_DATA_FOLDER}/forecasting"
output_folder = f"{ML_DATA_FOLDER}/forecasting_validation"

MODEL_VERSION = "demand_forecasting_v1"
VALIDATION_VERSION = "time_aware_validation_v1"

ALPHA_CANDIDATES = {
    0.1,
    1.0,
    5.0,
    10.0,
    25.0,
    50.0
}


outputs = {
    "Overall backtest": (
        f"{output_folder}/overall_backtest"
    ),
    "Menu item backtest": (
        f"{output_folder}/menu_item_backtest"
    ),
    "Menu category backtest": (
        f"{output_folder}/menu_category_backtest"
    ),
    "Restaurant location backtest": (
        f"{output_folder}/restaurant_location_backtest"
    ),
    "Time-period backtest": (
        f"{output_folder}/time_period_backtest"
    ),
    "Model selection": (
        f"{output_folder}/model_selection"
    ),
    "Split metadata": (
        f"{output_folder}/split_metadata"
    )
}


historical_outputs = {
    "Overall": (
        f"{forecast_folder}/historical_overall_demand"
    ),
    "Menu item": (
        f"{forecast_folder}/historical_menu_item_demand"
    ),
    "Menu category": (
        f"{forecast_folder}/historical_menu_category_demand"
    ),
    "Restaurant location": (
        f"{forecast_folder}/historical_restaurant_location_demand"
    ),
    "Time period": (
        f"{forecast_folder}/historical_time_period_demand"
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


def as_date(value):
    if hasattr(value, "date"):
        return value.date()

    return value


print("\n========TIME-AWARE VALIDATION CHECKS========")


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


for output_name, output_path in historical_outputs.items():
    exists = Path(
        output_path
    ).exists()

    check(
        f"{output_name} historical input exists",
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


overall_backtest = spark.read.parquet(
    outputs["Overall backtest"]
)

item_backtest = spark.read.parquet(
    outputs["Menu item backtest"]
)

category_backtest = spark.read.parquet(
    outputs["Menu category backtest"]
)

location_backtest = spark.read.parquet(
    outputs["Restaurant location backtest"]
)

period_backtest = spark.read.parquet(
    outputs["Time-period backtest"]
)

model_selection = spark.read.parquet(
    outputs["Model selection"]
)

metadata = spark.read.parquet(
    outputs["Split metadata"]
)


historical_overall = spark.read.parquet(
    historical_outputs["Overall"]
)

historical_items = spark.read.parquet(
    historical_outputs["Menu item"]
)

historical_categories = spark.read.parquet(
    historical_outputs["Menu category"]
)

historical_locations = spark.read.parquet(
    historical_outputs["Restaurant location"]
)

historical_periods = spark.read.parquet(
    historical_outputs["Time period"]
)




check(
    "Split metadata contains exactly one row",
    metadata.count() == 1
)


metadata_row = metadata.first()

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

training_start = as_date(
    metadata_row[
        "training_start_date"
    ]
)

training_end = as_date(
    metadata_row[
        "training_end_date"
    ]
)

validation_start = as_date(
    metadata_row[
        "validation_start_date"
    ]
)

validation_end = as_date(
    metadata_row[
        "validation_end_date"
    ]
)

test_start = as_date(
    metadata_row[
        "test_start_date"
    ]
)

test_end = as_date(
    metadata_row[
        "test_end_date"
    ]
)

validation_days = int(
    metadata_row[
        "validation_days"
    ]
)

test_days = int(
    metadata_row[
        "test_days"
    ]
)


check(
    "Training begins at historical start",
    training_start == historical_start
)

check(
    "Training ends before validation starts",
    training_end < validation_start,
    (
        f"Training end: {training_end}, "
        f"validation start: {validation_start}"
    )
)

check(
    "Validation ends before test starts",
    validation_end < test_start,
    (
        f"Validation end: {validation_end}, "
        f"test start: {test_start}"
    )
)

check(
    "Test ends at historical data end",
    test_end == historical_end,
    (
        f"Test end: {test_end}, "
        f"history end: {historical_end}"
    )
)

check(
    "Validation window length matches configuration",
    (
        validation_end -
        validation_start
    ).days + 1 == validation_days
)

check(
    "Test window length matches configuration",
    (
        test_end -
        test_start
    ).days + 1 == test_days
)

check(
    "Random splitting is disabled",
    metadata_row[
        "random_split_used"
    ] is False
)

check(
    "Future actual values are not used as features",
    metadata_row[
        "future_actuals_used_as_features"
    ] is False
)

check(
    "Past-only recursive feature policy is recorded",
    metadata_row[
        "feature_history_policy"
    ] == (
        "past_actuals_then_recursive_predictions"
    )
)

check(
    "Model selection uses validation data only",
    (
        metadata_row[
            "model_selection_split"
        ] == "validation" and
        metadata_row[
            "test_used_for_model_selection"
        ] is False
    )
)

check(
    "Seasonal naive baseline is recorded",
    metadata_row[
        "baseline_method"
    ] == (
        "seasonal_naive_lag_7_recursive"
    )
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



required_columns = {
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
}


backtest_frames = [
    (
        "Overall",
        overall_backtest
    ),
    (
        "Menu item",
        item_backtest
    ),
    (
        "Menu category",
        category_backtest
    ),
    (
        "Restaurant location",
        location_backtest
    ),
    (
        "Time period",
        period_backtest
    )
]


for name, frame in backtest_frames:
    check(
        f"{name} backtest contains data",
        frame.count() > 0
    )

    check(
        f"{name} backtest contains required columns",
        required_columns.issubset(
            set(frame.columns)
        )
    )

    split_values = {
        row["split"]
        for row in frame.select(
            "split"
        ).distinct().collect()
    }

    check(
        f"{name} contains validation and test splits",
        split_values == {
            "validation",
            "test"
        },
        f"Found: {sorted(split_values)}"
    )

    check(
        f"{name} actual demand is non-negative",
        frame.filter(
            F.col("actual_demand") < 0
        ).count() == 0
    )

    check(
        f"{name} predictions are non-negative",
        frame.filter(
            F.col("predicted_demand") < 0
        ).count() == 0
    )

    check(
        f"{name} baseline predictions are non-negative",
        frame.filter(
            F.col("baseline_prediction") < 0
        ).count() == 0
    )

    check(
        f"{name} contains no null forecast values",
        frame.filter(
            F.col("actual_demand").isNull() |
            F.col("predicted_demand").isNull() |
            F.col("baseline_prediction").isNull()
        ).count() == 0
    )

    check(
        f"{name} forecast origin is always before forecast date",
        frame.filter(
            F.col("forecast_origin_date") >=
            F.col("forecast_date")
        ).count() == 0
    )

    check(
        f"{name} validation forecasts originate at training cutoff",
        frame.filter(
            (F.col("split") == "validation") &
            (
                F.col("forecast_origin_date") !=
                F.lit(training_end)
            )
        ).count() == 0
    )

    check(
        f"{name} test forecasts originate at validation cutoff",
        frame.filter(
            (F.col("split") == "test") &
            (
                F.col("forecast_origin_date") !=
                F.lit(validation_end)
            )
        ).count() == 0
    )

    check(
        f"{name} validation dates stay inside validation window",
        frame.filter(
            (F.col("split") == "validation") &
            (
                (
                    F.col("forecast_date") <
                    F.lit(validation_start)
                ) |
                (
                    F.col("forecast_date") >
                    F.lit(validation_end)
                )
            )
        ).count() == 0
    )

    check(
        f"{name} test dates stay inside test window",
        frame.filter(
            (F.col("split") == "test") &
            (
                (
                    F.col("forecast_date") <
                    F.lit(test_start)
                ) |
                (
                    F.col("forecast_date") >
                    F.lit(test_end)
                )
            )
        ).count() == 0
    )

    check(
        f"{name} uses expected model version",
        frame.filter(
            F.col("model_version") != MODEL_VERSION
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
        f"{name} selected alpha values are valid",
        frame.filter(
            ~F.col("selected_alpha").isin(
                list(
                    ALPHA_CANDIDATES
                )
            )
        ).count() == 0
    )



def check_group_horizons(
    name,
    frame,
    entity_column
):
    validation_incomplete = frame.filter(
        F.col("split") == "validation"
    ).groupBy(
        entity_column
    ).agg(
        F.countDistinct(
            "forecast_date"
        ).alias(
            "days"
        )
    ).filter(
        F.col("days") != validation_days
    ).count()

    test_incomplete = frame.filter(
        F.col("split") == "test"
    ).groupBy(
        entity_column
    ).agg(
        F.countDistinct(
            "forecast_date"
        ).alias(
            "days"
        )
    ).filter(
        F.col("days") != test_days
    ).count()

    check(
        f"Every {name} series has complete validation horizon",
        validation_incomplete == 0,
        (
            f"Incomplete series: "
            f"{validation_incomplete}"
        )
    )

    check(
        f"Every {name} series has complete test horizon",
        test_incomplete == 0,
        (
            f"Incomplete series: "
            f"{test_incomplete}"
        )
    )


check(
    "Overall validation horizon is complete",
    overall_backtest.filter(
        F.col("split") == "validation"
    ).select(
        "forecast_date"
    ).distinct().count() == validation_days
)

check(
    "Overall test horizon is complete",
    overall_backtest.filter(
        F.col("split") == "test"
    ).select(
        "forecast_date"
    ).distinct().count() == test_days
)


check_group_horizons(
    "menu item",
    item_backtest,
    "item_id"
)

check_group_horizons(
    "menu category",
    category_backtest,
    "category_id"
)

check_group_horizons(
    "restaurant location",
    location_backtest,
    "restaurant_id"
)

check_group_horizons(
    "time-period",
    period_backtest,
    "order_hour"
)



check(
    "All menu items are included in backtesting",
    item_backtest.select(
        "item_id"
    ).distinct().count() ==
    historical_items.select(
        "item_id"
    ).distinct().count()
)

check(
    "All menu categories are included in backtesting",
    category_backtest.select(
        "category_id"
    ).distinct().count() ==
    historical_categories.select(
        "category_id"
    ).distinct().count()
)

check(
    "All restaurant locations are included in backtesting",
    location_backtest.select(
        "restaurant_id"
    ).distinct().count() ==
    historical_locations.select(
        "restaurant_id"
    ).distinct().count()
)

check(
    "All selected time periods are included in backtesting",
    period_backtest.select(
        "order_hour"
    ).distinct().count() ==
    historical_periods.select(
        "order_hour"
    ).distinct().count()
)



overall_mismatch = overall_backtest.alias(
    "b"
).join(
    historical_overall.alias(
        "h"
    ),
    F.col(
        "b.forecast_date"
    ) == F.col(
        "h.order_date"
    ),
    "left"
).filter(
    F.abs(
        F.col("b.actual_demand") -
        F.col("h.demand")
    ) > 0.000001
).count()


item_mismatch = item_backtest.alias(
    "b"
).join(
    historical_items.alias(
        "h"
    ),
    (
        F.col("b.item_id") ==
        F.col("h.item_id")
    ) &
    (
        F.col("b.forecast_date") ==
        F.col("h.order_date")
    ),
    "left"
).filter(
    F.abs(
        F.col("b.actual_demand") -
        F.col("h.demand")
    ) > 0.000001
).count()


category_mismatch = category_backtest.alias(
    "b"
).join(
    historical_categories.alias(
        "h"
    ),
    (
        F.col("b.category_id") ==
        F.col("h.category_id")
    ) &
    (
        F.col("b.forecast_date") ==
        F.col("h.order_date")
    ),
    "left"
).filter(
    F.abs(
        F.col("b.actual_demand") -
        F.col("h.demand")
    ) > 0.000001
).count()


location_mismatch = location_backtest.alias(
    "b"
).join(
    historical_locations.alias(
        "h"
    ),
    (
        F.col("b.restaurant_id") ==
        F.col("h.restaurant_id")
    ) &
    (
        F.col("b.forecast_date") ==
        F.col("h.order_date")
    ),
    "left"
).filter(
    F.abs(
        F.col("b.actual_demand") -
        F.col("h.demand")
    ) > 0.000001
).count()


period_mismatch = period_backtest.alias(
    "b"
).join(
    historical_periods.alias(
        "h"
    ),
    (
        F.col("b.order_hour") ==
        F.col("h.order_hour")
    ) &
    (
        F.col("b.forecast_date") ==
        F.col("h.order_date")
    ),
    "left"
).filter(
    F.abs(
        F.col("b.actual_demand") -
        F.col("h.demand")
    ) > 0.000001
).count()


check(
    "Overall backtest actuals match historical demand",
    overall_mismatch == 0
)

check(
    "Menu item backtest actuals match historical demand",
    item_mismatch == 0
)

check(
    "Menu category backtest actuals match historical demand",
    category_mismatch == 0
)

check(
    "Restaurant location backtest actuals match historical demand",
    location_mismatch == 0
)

check(
    "Time-period backtest actuals match historical demand",
    period_mismatch == 0
)


expected_selection_rows = (
    1 +
    historical_items.select(
        "item_id"
    ).distinct().count() +
    historical_categories.select(
        "category_id"
    ).distinct().count() +
    historical_locations.select(
        "restaurant_id"
    ).distinct().count() +
    historical_periods.select(
        "order_hour"
    ).distinct().count()
)


check(
    "Model selection has one row per forecast series",
    model_selection.count() ==
    expected_selection_rows,
    (
        f"Expected {expected_selection_rows}, "
        f"found {model_selection.count()}"
    )
)

check(
    "Model selection uses only the validation split",
    model_selection.filter(
        F.col("selection_split") !=
        "validation"
    ).count() == 0
)

check(
    "Test data is never used for model selection",
    model_selection.filter(
        F.col(
            "test_used_for_selection"
        ) == True
    ).count() == 0
)

check(
    "Final models train only through validation end",
    model_selection.filter(
        F.col("final_training_end_date") !=
        F.lit(validation_end)
    ).count() == 0
)

check(
    "Validation MAE values are valid",
    model_selection.filter(
        F.col("validation_mae").isNull() |
        (F.col("validation_mae") < 0)
    ).count() == 0
)

check(
    "Selected alpha values belong to configured candidates",
    model_selection.filter(
        ~F.col("selected_alpha").isin(
            list(
                ALPHA_CANDIDATES
            )
        )
    ).count() == 0
)



total_checks = passed + failed


print(
    f"\n========VALIDATION RESULT: "
    f"{passed}/{total_checks} PASS========"
)


if failed == 0:
    print(
        "\nTime-aware forecast validation PASSED."
    )
else:
    print(
        "\nTime-aware forecast validation FAILED."
    )


spark.stop()
