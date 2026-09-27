from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from config.settings import ANALYTICS_DATA_FOLDER, ML_DATA_FOLDER, MODEL_FOLDER

spark = SparkSession.builder \
    .appName("DineIQ Spark MLlib Menu Classification") \
    .getOrCreate()

input_folder = ANALYTICS_DATA_FOLDER
output_folder = ML_DATA_FOLDER
model_folder = MODEL_FOLDER

MODEL_VERSION = "spark_menu_classification_v1"

menu_classification = spark.read.parquet(
    f"{input_folder}/menu_classification"
)


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

ml_data = menu_classification.select(
    "item_id",
    "item_name",
    "performance_class",
    *feature_columns
).dropna()

print("\n========ML DATASET========")
print(f"Total Records: {ml_data.count()}")

ml_data.groupBy(
    "performance_class"
).count().orderBy(
    F.desc("count")
).show(truncate=False)


split_window = Window.partitionBy(
    "performance_class"
).orderBy(
    F.rand(seed=42)
)

count_window = Window.partitionBy(
    "performance_class"
)

split_data = ml_data.withColumn(
    "row_number",
    F.row_number().over(split_window)
).withColumn(
    "class_count",
    F.count("*").over(count_window)
).withColumn(
    "split_ratio",
    F.col("row_number") / F.col("class_count")
)

train_data = split_data.filter(
    F.col("split_ratio") <= 0.70
).drop(
    "row_number",
    "class_count",
    "split_ratio"
)

validation_data = split_data.filter(
    (F.col("split_ratio") > 0.70) &
    (F.col("split_ratio") <= 0.85)
).drop(
    "row_number",
    "class_count",
    "split_ratio"
)

test_data = split_data.filter(
    F.col("split_ratio") > 0.85
).drop(
    "row_number",
    "class_count",
    "split_ratio"
)

print("\n========DATA SPLIT========")
print(f"Training Records: {train_data.count()}")
print(f"Validation Records: {validation_data.count()}")
print(f"Testing Records: {test_data.count()}")


label_indexer = StringIndexer(
    inputCol="performance_class",
    outputCol="label",
    handleInvalid="keep"
)

assembler = VectorAssembler(
    inputCols=feature_columns,
    outputCol="raw_features",
    handleInvalid="keep"
)

scaler = StandardScaler(
    inputCol="raw_features",
    outputCol="features",
    withStd=True,
    withMean=True
)


models = {
    "Logistic Regression": LogisticRegression(
        featuresCol="features",
        labelCol="label",
        maxIter=100,
        regParam=0.1,
        elasticNetParam=0.0
    ),
    "Decision Tree": DecisionTreeClassifier(
        featuresCol="features",
        labelCol="label",
        maxDepth=5,
        seed=42
    ),
    "Random Forest": RandomForestClassifier(
        featuresCol="features",
        labelCol="label",
        numTrees=100,
        maxDepth=6,
        seed=42
    )
}

evaluators = {
    "accuracy": MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="accuracy"
    ),
    "weighted_precision": MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="weightedPrecision"
    ),
    "weighted_recall": MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="weightedRecall"
    ),
    "f1": MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="f1"
    )
}

model_results = []
trained_models = {}

for model_name, classifier in models.items():
    print(f"\n========TRAINING {model_name.upper()}========")

    pipeline = Pipeline(
        stages=[
            label_indexer,
            assembler,
            scaler,
            classifier
        ]
    )

    trained_model = pipeline.fit(train_data)
    validation_predictions = trained_model.transform(
        validation_data
    )

    metrics = {
        metric_name: evaluator.evaluate(
            validation_predictions
        )
        for metric_name, evaluator in evaluators.items()
    }

    model_results.append({
        "model_name": model_name,
        "accuracy": metrics["accuracy"],
        "weighted_precision": metrics["weighted_precision"],
        "weighted_recall": metrics["weighted_recall"],
        "f1": metrics["f1"]
    })

    trained_models[model_name] = trained_model

    print(f"Accuracy: {metrics['accuracy']:.4f}")
    print(
        f"Weighted Precision: "
        f"{metrics['weighted_precision']:.4f}"
    )
    print(
        f"Weighted Recall: "
        f"{metrics['weighted_recall']:.4f}"
    )
    print(f"F1 Score: {metrics['f1']:.4f}")


results_df = spark.createDataFrame(
    model_results
).orderBy(
    F.desc("f1")
)

print("\n========MODEL COMPARISON========")
results_df.show(truncate=False)

best_result = results_df.first()
best_model_name = best_result["model_name"]
best_model = trained_models[best_model_name]

print(
    f"\nSelected Model: {best_model_name} "
    f"(Validation F1: {best_result['f1']:.4f})"
)


test_predictions = best_model.transform(
    test_data
)

test_metrics = {
    metric_name: evaluator.evaluate(
        test_predictions
    )
    for metric_name, evaluator in evaluators.items()
}

print("\n========FINAL TEST RESULTS========")
print(f"Accuracy: {test_metrics['accuracy']:.4f}")
print(
    f"Weighted Precision: "
    f"{test_metrics['weighted_precision']:.4f}"
)
print(
    f"Weighted Recall: "
    f"{test_metrics['weighted_recall']:.4f}"
)
print(f"F1 Score: {test_metrics['f1']:.4f}")


label_model = best_model.stages[0]
labels = label_model.labels

decode_expression = F.create_map(
    *[
        value
        for index, label in enumerate(labels)
        for value in (
            F.lit(float(index)),
            F.lit(label)
        )
    ]
)

final_predictions = test_predictions.withColumn(
    "predicted_class",
    decode_expression[F.col("prediction")]
).withColumn(
    "actual_class",
    F.col("performance_class")
).withColumn(
    "is_correct",
    F.col("actual_class") == F.col("predicted_class")
).withColumn(
    "model_name",
    F.lit(best_model_name)
).withColumn(
    "model_version",
    F.lit(MODEL_VERSION)
)

print("\n========SAMPLE PREDICTIONS========")
final_predictions.select(
    "item_id",
    "item_name",
    "actual_class",
    "predicted_class",
    "is_correct"
).show(50, truncate=False)


confusion_matrix = final_predictions.groupBy(
    "actual_class",
    "predicted_class"
).count().orderBy(
    "actual_class",
    "predicted_class"
)

print("\n========CONFUSION MATRIX========")
confusion_matrix.show(truncate=False)


results_df.write.mode("overwrite").parquet(
    f"{output_folder}/menu_classification_model_metrics"
)

final_predictions.select(
    "item_id",
    "item_name",
    "actual_class",
    "predicted_class",
    "is_correct",
    "probability",
    "model_name",
    "model_version"
).write.mode("overwrite").parquet(
    f"{output_folder}/menu_classification_predictions"
)

confusion_matrix.write.mode("overwrite").parquet(
    f"{output_folder}/menu_classification_confusion_matrix"
)

best_model.write().overwrite().save(
    f"{model_folder}/{MODEL_VERSION}"
)

print(
    f"\nBest model saved to "
    f"{model_folder}/{MODEL_VERSION}"
)
print(
    "Spark MLlib menu classification "
    "completed successfully."
)

spark.stop()
