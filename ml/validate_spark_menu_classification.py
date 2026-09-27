from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array

spark = SparkSession.builder \
    .appName("DineIQ Validate Spark MLlib Menu Classification") \
    .getOrCreate()

output_folder = "ml_data"
model_folder = "models"

MODEL_VERSION = "spark_menu_classification_v1"

metrics_path = f"{output_folder}/menu_classification_model_metrics"
predictions_path = f"{output_folder}/menu_classification_predictions"
confusion_path = f"{output_folder}/menu_classification_confusion_matrix"
saved_model_path = f"{model_folder}/{MODEL_VERSION}"

EXPECTED_MODELS = {
    "Logistic Regression",
    "Decision Tree",
    "Random Forest"
}

EXPECTED_CLASSES = {
    "Profit Driver",
    "Volume Driver",
    "Hidden Opportunity",
    "Low Performer"
}

checks = []


def check(name, condition, details=""):
    passed = bool(condition)
    checks.append((name, passed, details))
    status = "PASS" if passed else "FAIL"
    suffix = f" - {details}" if details else ""
    print(f"[{status}] {name}{suffix}")


print("\n========SPARK MLLIB MENU CLASSIFICATION VALIDATION========")

metrics_exists = Path(metrics_path).exists()
predictions_exists = Path(predictions_path).exists()
confusion_exists = Path(confusion_path).exists()
model_exists = Path(saved_model_path).exists()

check("Model metrics output exists", metrics_exists)
check("Prediction output exists", predictions_exists)
check("Confusion matrix output exists", confusion_exists)
check("Saved Spark model exists", model_exists)

if not all([
    metrics_exists,
    predictions_exists,
    confusion_exists,
    model_exists
]):
    passed = sum(1 for _, result, _ in checks if result)
    total = len(checks)
    print(f"\n========VALIDATION RESULT: {passed}/{total} PASS========")
    spark.stop()
    raise SystemExit(1)

metrics = spark.read.parquet(metrics_path)
predictions = spark.read.parquet(predictions_path)
confusion = spark.read.parquet(confusion_path)

required_metric_columns = {
    "model_name",
    "accuracy",
    "weighted_precision",
    "weighted_recall",
    "f1"
}

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

required_confusion_columns = {
    "actual_class",
    "predicted_class",
    "count"
}

check(
    "Model metrics contain required columns",
    required_metric_columns.issubset(set(metrics.columns))
)

check(
    "Predictions contain required columns",
    required_prediction_columns.issubset(set(predictions.columns))
)

check(
    "Confusion matrix contains required columns",
    required_confusion_columns.issubset(set(confusion.columns))
)

model_names = {
    row["model_name"]
    for row in metrics.select("model_name").distinct().collect()
}

check(
    "All three required algorithms were compared",
    EXPECTED_MODELS.issubset(model_names),
    f"Found: {sorted(model_names)}"
)

metrics_count = metrics.count()

check(
    "Exactly three model comparison rows exist",
    metrics_count == 3,
    f"Rows: {metrics_count}"
)

invalid_metric_count = metrics.filter(
    F.col("model_name").isNull() |
    F.col("accuracy").isNull() |
    F.col("weighted_precision").isNull() |
    F.col("weighted_recall").isNull() |
    F.col("f1").isNull()
).count()

check(
    "Model comparison metrics contain no nulls",
    invalid_metric_count == 0,
    f"Invalid rows: {invalid_metric_count}"
)

out_of_range_count = metrics.filter(
    (F.col("accuracy") < 0.0) |
    (F.col("accuracy") > 1.0) |
    (F.col("weighted_precision") < 0.0) |
    (F.col("weighted_precision") > 1.0) |
    (F.col("weighted_recall") < 0.0) |
    (F.col("weighted_recall") > 1.0) |
    (F.col("f1") < 0.0) |
    (F.col("f1") > 1.0)
).count()

check(
    "All evaluation metrics are between 0 and 1",
    out_of_range_count == 0,
    f"Invalid rows: {out_of_range_count}"
)

prediction_count = predictions.count()

check(
    "Held-out test predictions exist",
    prediction_count > 0,
    f"Predictions: {prediction_count}"
)

null_prediction_count = predictions.filter(
    F.col("item_id").isNull() |
    F.col("actual_class").isNull() |
    F.col("predicted_class").isNull() |
    F.col("is_correct").isNull() |
    F.col("probability").isNull() |
    F.col("model_name").isNull() |
    F.col("model_version").isNull()
).count()

check(
    "Prediction output contains no required null values",
    null_prediction_count == 0,
    f"Invalid rows: {null_prediction_count}"
)

actual_classes = {
    row["actual_class"]
    for row in predictions.select(
        "actual_class"
    ).distinct().collect()
}

predicted_classes = {
    row["predicted_class"]
    for row in predictions.select(
        "predicted_class"
    ).distinct().collect()
}

check(
    "All expected classes appear in held-out actual results",
    EXPECTED_CLASSES.issubset(actual_classes),
    f"Found: {sorted(actual_classes)}"
)

check(
    "Predicted classes are valid performance classes",
    predicted_classes.issubset(EXPECTED_CLASSES),
    f"Found: {sorted(predicted_classes)}"
)

incorrect_flag_count = predictions.filter(
    F.col("is_correct") != (
        F.col("actual_class") == F.col("predicted_class")
    )
).count()

check(
    "is_correct matches actual versus predicted class",
    incorrect_flag_count == 0,
    f"Incorrect flags: {incorrect_flag_count}"
)

model_versions = {
    row["model_version"]
    for row in predictions.select(
        "model_version"
    ).distinct().collect()
}

check(
    "Predictions use expected model version",
    model_versions == {MODEL_VERSION},
    f"Found: {sorted(model_versions)}"
)

selected_models = {
    row["model_name"]
    for row in predictions.select(
        "model_name"
    ).distinct().collect()
}

check(
    "Predictions come from one selected final model",
    len(selected_models) == 1,
    f"Found: {sorted(selected_models)}"
)

best_validation_model = metrics.orderBy(
    F.desc("f1")
).first()["model_name"]

selected_model = next(iter(selected_models))

check(
    "Selected model matches highest validation F1",
    selected_model == best_validation_model,
    f"Selected: {selected_model}, Best: {best_validation_model}"
)

confusion_total = confusion.agg(
    F.sum("count").alias("total")
).first()["total"]

check(
    "Confusion matrix covers every test prediction",
    confusion_total == prediction_count,
    f"Matrix: {confusion_total}, Predictions: {prediction_count}"
)

duplicate_prediction_count = predictions.groupBy(
    "item_id"
).count().filter(
    F.col("count") > 1
).count()

check(
    "Each test item has exactly one prediction",
    duplicate_prediction_count == 0,
    f"Duplicate items: {duplicate_prediction_count}"
)

probability_size_count = predictions.withColumn(
    "probability_array",
    vector_to_array("probability")
).filter(
    F.size("probability_array") < 4
).count()

check(
    "Prediction probabilities cover all four classes",
    probability_size_count == 0,
    f"Invalid rows: {probability_size_count}"
)

probability_sum_count = predictions.withColumn(
    "probability_array",
    vector_to_array("probability")
).withColumn(
    "probability_sum",
    F.aggregate(
        "probability_array",
        F.lit(0.0),
        lambda total, value: total + value
    )
).filter(
    F.abs(F.col("probability_sum") - 1.0) > 0.000001
).count()

check(
    "Prediction probabilities sum to 1",
    probability_sum_count == 0,
    f"Invalid rows: {probability_sum_count}"
)

try:
    loaded_model = PipelineModel.load(saved_model_path)
    check(
        "Saved Spark model can be loaded",
        loaded_model is not None
    )
except Exception as error:
    check(
        "Saved Spark model can be loaded",
        False,
        str(error)
    )

passed = sum(
    1
    for _, result, _ in checks
    if result
)

total = len(checks)

print(
    f"\n========VALIDATION RESULT: "
    f"{passed}/{total} PASS========"
)

if passed != total:
    spark.stop()
    raise SystemExit(1)

print(
    "Spark MLlib menu classification "
    "validation completed successfully."
)

spark.stop()
