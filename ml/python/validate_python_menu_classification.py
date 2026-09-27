import os
import json
import joblib
import pandas as pd
from config.settings import ANALYTICS_DATA_FOLDER, ML_DATA_FOLDER, MODEL_FOLDER

input_folder = ANALYTICS_DATA_FOLDER
output_folder = f"{ML_DATA_FOLDER}/python"
model_folder = MODEL_FOLDER

MODEL_VERSION = "python_menu_classification_v1"

feature_columns = [
    "quantity_sold",
    "item_revenue",
    "cost",
    "contribution_margin",
    "profit_percentage",
    "average_rating",
    "repeat_purchase_rate",
    "wastage_percentage",
    "promotion_dependency",
    "sales_trend"
]

expected_classes = {
    "Profit Driver",
    "Volume Driver",
    "Hidden Opportunity",
    "Low Performer"
}

checks = []

def check(name, condition, details=""):
    passed = bool(condition)
    checks.append(
        {
            "check": name,
            "status": "PASS" if passed else "FAIL",
            "details": details
        }
    )
    print(
        f"[{'PASS' if passed else 'FAIL'}] "
        f"{name}"
        + (f" - {details}" if details else "")
    )

print("\n========PYTHON ML VALIDATION========")

input_path = f"{input_folder}/menu_classification"
metrics_path = f"{output_folder}/menu_classification_model_metrics.parquet"
test_metrics_path = f"{output_folder}/menu_classification_test_metrics.parquet"
predictions_path = f"{output_folder}/menu_classification_predictions.parquet"
confusion_path = f"{output_folder}/menu_classification_confusion_matrix.parquet"
model_path = f"{model_folder}/{MODEL_VERSION}.joblib"
metadata_path = f"{model_folder}/{MODEL_VERSION}_metadata.json"

check(
    "Input menu classification dataset exists",
    os.path.exists(input_path)
)

check(
    "Model comparison metrics output exists",
    os.path.exists(metrics_path)
)

check(
    "Test metrics output exists",
    os.path.exists(test_metrics_path)
)

check(
    "Prediction output exists",
    os.path.exists(predictions_path)
)

check(
    "Confusion matrix output exists",
    os.path.exists(confusion_path)
)

check(
    "Saved Python model exists",
    os.path.exists(model_path)
)

check(
    "Model metadata exists",
    os.path.exists(metadata_path)
)

required_files_exist = all(
    [
        os.path.exists(input_path),
        os.path.exists(metrics_path),
        os.path.exists(test_metrics_path),
        os.path.exists(predictions_path),
        os.path.exists(confusion_path),
        os.path.exists(model_path),
        os.path.exists(metadata_path)
    ]
)

if required_files_exist:
    source_data = pd.read_parquet(
        input_path
    )
    metrics = pd.read_parquet(
        metrics_path
    )
    test_metrics = pd.read_parquet(
        test_metrics_path
    )
    predictions = pd.read_parquet(
        predictions_path
    )
    confusion = pd.read_parquet(
        confusion_path
    )

    with open(
        metadata_path,
        "r",
        encoding="utf-8"
    ) as file:
        metadata = json.load(file)

    model = joblib.load(
        model_path
    )

    required_source_columns = {
        "item_id",
        "item_name",
        "performance_class",
        *feature_columns
    }

    check(
        "Source dataset contains required ML columns",
        required_source_columns.issubset(
            source_data.columns
        )
    )

    check(
        "All 10 required features are recorded",
        metadata.get("feature_columns") == feature_columns,
        f"{len(metadata.get('feature_columns', []))} features"
    )

    check(
        "Target column is performance_class",
        metadata.get("target_column") == "performance_class"
    )

    source_classes = set(
        source_data["performance_class"]
        .dropna()
        .unique()
    )

    check(
        "All four menu performance classes exist",
        source_classes == expected_classes,
        ", ".join(sorted(source_classes))
    )

    algorithms = set(
        metadata.get(
            "algorithms_tested",
            []
        )
    )

    check(
        "Three independent Python algorithms were tested",
        algorithms == {
            "Logistic Regression",
            "Decision Tree",
            "Random Forest"
        },
        ", ".join(sorted(algorithms))
    )

    check(
        "Hyperparameters are recorded for all models",
        set(
            metadata.get(
                "hyperparameters",
                {}
            ).keys()
        ) == algorithms
    )

    training_records = metadata.get(
        "training_records",
        0
    )
    validation_records = metadata.get(
        "validation_records",
        0
    )
    testing_records = metadata.get(
        "testing_records",
        0
    )

    check(
        "Training split contains records",
        training_records > 0,
        str(training_records)
    )

    check(
        "Validation split contains records",
        validation_records > 0,
        str(validation_records)
    )

    check(
        "Testing split contains records",
        testing_records > 0,
        str(testing_records)
    )

    check(
        "Train, validation and test counts cover ML dataset",
        (
            training_records +
            validation_records +
            testing_records
        ) == len(
            source_data[
                list(required_source_columns)
            ].dropna()
        )
    )

    required_metric_columns = {
        "model_name",
        "train_accuracy",
        "train_weighted_precision",
        "train_weighted_recall",
        "train_f1",
        "validation_accuracy",
        "validation_weighted_precision",
        "validation_weighted_recall",
        "validation_f1"
    }

    check(
        "Training and validation metrics are recorded",
        required_metric_columns.issubset(
            metrics.columns
        )
    )

    check(
        "Metrics contain all three tested models",
        set(metrics["model_name"]) == algorithms
    )

    metric_value_columns = [
        column
        for column in required_metric_columns
        if column != "model_name"
    ]

    metrics_in_range = metrics[
        metric_value_columns
    ].apply(
        lambda column: column.between(
            0,
            1
        ).all()
    ).all()

    check(
        "Training and validation metrics are valid",
        metrics_in_range
    )

    required_test_columns = {
        "model_name",
        "model_version",
        "test_accuracy",
        "test_weighted_precision",
        "test_weighted_recall",
        "test_f1"
    }

    check(
        "Final test metrics are recorded",
        required_test_columns.issubset(
            test_metrics.columns
        )
    )

    test_metric_columns = [
        "test_accuracy",
        "test_weighted_precision",
        "test_weighted_recall",
        "test_f1"
    ]

    test_metrics_in_range = test_metrics[
        test_metric_columns
    ].apply(
        lambda column: column.between(
            0,
            1
        ).all()
    ).all()

    check(
        "Final test metrics are valid",
        test_metrics_in_range
    )

    best_metrics_row = metrics.sort_values(
        "validation_f1",
        ascending=False
    ).iloc[0]

    selected_model = metadata.get(
        "selected_model"
    )

    check(
        "Selected model matches highest validation F1",
        selected_model == best_metrics_row["model_name"],
        str(selected_model)
    )

    check(
        "Test metrics use selected model",
        (
            len(test_metrics) == 1 and
            test_metrics.iloc[0]["model_name"] ==
            selected_model
        )
    )

    required_prediction_columns = {
        "item_id",
        "item_name",
        "actual_class",
        "predicted_class",
        "is_correct",
        "probability",
        "model_name",
        "model_version"
    }

    check(
        "Prediction output contains required evidence columns",
        required_prediction_columns.issubset(
            predictions.columns
        )
    )

    check(
        "Prediction count matches held-out test size",
        len(predictions) == testing_records,
        f"{len(predictions)} predictions"
    )

    check(
        "Predictions contain only valid performance classes",
        (
            set(predictions["actual_class"])
            .issubset(expected_classes) and
            set(predictions["predicted_class"])
            .issubset(expected_classes)
        )
    )

    check(
        "Prediction probabilities are valid",
        predictions["probability"].between(
            0,
            1
        ).all()
    )

    calculated_correct = (
        predictions["actual_class"] ==
        predictions["predicted_class"]
    )

    check(
        "Prediction correctness flags are accurate",
        (
            calculated_correct ==
            predictions["is_correct"]
        ).all()
    )

    calculated_accuracy = (
        calculated_correct.mean()
    )

    recorded_accuracy = float(
        test_metrics.iloc[0][
            "test_accuracy"
        ]
    )

    check(
        "Prediction accuracy matches recorded test accuracy",
        abs(
            calculated_accuracy -
            recorded_accuracy
        ) < 1e-12,
        f"{calculated_accuracy:.4f}"
    )

    check(
        "Prediction model version is consistent",
        set(
            predictions["model_version"]
        ) == {
            MODEL_VERSION
        }
    )

    check(
        "Prediction model name is consistent",
        set(
            predictions["model_name"]
        ) == {
            selected_model
        }
    )

    required_confusion_columns = {
        "actual_class",
        "predicted_class",
        "count"
    }

    check(
        "Confusion matrix contains required columns",
        required_confusion_columns.issubset(
            confusion.columns
        )
    )

    check(
        "Confusion matrix covers all test predictions",
        int(confusion["count"].sum()) ==
        testing_records,
        str(int(confusion["count"].sum()))
    )

    check(
        "Model version metadata is correct",
        metadata.get("model_version") ==
        MODEL_VERSION
    )

    check(
        "Saved model can generate predictions",
        hasattr(model, "predict") and
        hasattr(model, "predict_proba")
    )

    check(
        "No Spark prediction columns are used as Python features",
        not any(
            column in feature_columns
            for column in [
                "prediction",
                "predicted_class",
                "spark_prediction",
                "spark_result"
            ]
        )
    )

passed_checks = sum(
    result["status"] == "PASS"
    for result in checks
)

failed_checks = len(checks) - passed_checks

print("\n========VALIDATION SUMMARY========")
print(f"Total Checks: {len(checks)}")
print(f"Passed: {passed_checks}")
print(f"Failed: {failed_checks}")

if failed_checks == 0:
    print(
        "Python menu classification validation "
        "completed successfully."
    )
else:
    print(
        "Python menu classification validation "
        "failed."
    )
    raise SystemExit(1)
