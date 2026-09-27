import os
import pandas as pd
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.classification import DecisionTreeClassifier
from sklearn.model_selection import StratifiedKFold
from sklearn.pipeline import Pipeline as SklearnPipeline
from sklearn.preprocessing import StandardScaler as SklearnStandardScaler
from sklearn.ensemble import RandomForestClassifier

input_folder = "analytics_data"
output_folder = "ml_data/comparison"

SPARK_MODEL_VERSION = "spark_menu_classification_v1"
PYTHON_MODEL_VERSION = "python_menu_classification_v1"
COMPARISON_VERSION = "dual_pipeline_menu_classification_v1"
RANDOM_STATE = 42
N_FOLDS = 5

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

target_column = "performance_class"

os.makedirs(output_folder, exist_ok=True)

spark = (
    SparkSession.builder
    .appName("DineIQ Dual Pipeline Comparison")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n========DUAL PIPELINE DATASET========")

menu_classification = spark.read.parquet(
    f"{input_folder}/menu_classification"
)

required_columns = [
    "item_id",
    "item_name",
    target_column,
    *feature_columns
]

comparison_data = (
    menu_classification
    .select(*required_columns)
    .dropna()
)

print(f"Total Records: {comparison_data.count()}")

print("\n========CREATE SHARED OOF FOLDS========")

class_window = Window.partitionBy(
    target_column
).orderBy(
    F.rand(seed=RANDOM_STATE)
)

class_count_window = Window.partitionBy(
    target_column
)

fold_data = (
    comparison_data
    .withColumn(
        "_class_row",
        F.row_number().over(class_window)
    )
    .withColumn(
        "_class_count",
        F.count("*").over(class_count_window)
    )
    .withColumn(
        "fold_id",
        (
            (
                F.col("_class_row") - 1
            ) % N_FOLDS
        ).cast("int")
    )
    .drop(
        "_class_row",
        "_class_count"
    )
    .cache()
)

fold_counts = (
    fold_data
    .groupBy("fold_id")
    .count()
    .orderBy("fold_id")
)

fold_counts.show()

fold_assignment = (
    fold_data
    .select(
        "item_id",
        "fold_id"
    )
    .toPandas()
)

python_data = (
    comparison_data
    .toPandas()
    .merge(
        fold_assignment,
        on="item_id",
        how="inner"
    )
    .sort_values("item_id")
    .reset_index(drop=True)
)

print("\n========SPARK OOF PREDICTIONS========")

spark_predictions = []

for fold_id in range(N_FOLDS):
    print(
        f"Spark Fold {fold_id + 1}/{N_FOLDS}"
    )

    train_data = (
        fold_data
        .filter(
            F.col("fold_id") != fold_id
        )
        .drop("fold_id")
    )

    test_data = (
        fold_data
        .filter(
            F.col("fold_id") == fold_id
        )
        .drop("fold_id")
    )

    label_indexer = StringIndexer(
        inputCol=target_column,
        outputCol="label",
        handleInvalid="error"
    )

    assembler = VectorAssembler(
        inputCols=feature_columns,
        outputCol="unscaled_features"
    )

    scaler = StandardScaler(
        inputCol="unscaled_features",
        outputCol="features",
        withStd=True,
        withMean=True
    )

    classifier = DecisionTreeClassifier(
        labelCol="label",
        featuresCol="features",
        maxDepth=5,
        seed=RANDOM_STATE
    )

    pipeline = Pipeline(
        stages=[
            label_indexer,
            assembler,
            scaler,
            classifier
        ]
    )

    model = pipeline.fit(train_data)

    label_model = model.stages[0]
    labels = label_model.labels

    prediction_udf = F.udf(
        lambda value: labels[int(value)],
        "string"
    )

    probability_udf = F.udf(
        lambda vector: float(max(vector)),
        "double"
    )

    fold_predictions = (
        model.transform(test_data)
        .select(
            "item_id",
            "item_name",
            F.col(target_column).alias(
                "actual_class"
            ),
            prediction_udf(
                F.col("prediction")
            ).alias(
                "spark_prediction"
            ),
            probability_udf(
                F.col("probability")
            ).alias(
                "spark_probability"
            )
        )
        .toPandas()
    )

    fold_predictions["fold_id"] = fold_id

    spark_predictions.append(
        fold_predictions
    )

spark_oof = (
    pd.concat(
        spark_predictions,
        ignore_index=True
    )
    .sort_values("item_id")
    .reset_index(drop=True)
)

print(
    f"Spark OOF Predictions: "
    f"{len(spark_oof)}"
)

print("\n========PYTHON OOF PREDICTIONS========")

python_predictions = []

for fold_id in range(N_FOLDS):
    print(
        f"Python Fold {fold_id + 1}/{N_FOLDS}"
    )

    train_data = python_data[
        python_data["fold_id"] != fold_id
    ].copy()

    test_data = python_data[
        python_data["fold_id"] == fold_id
    ].copy()

    X_train = train_data[
        feature_columns
    ]

    y_train = train_data[
        target_column
    ]

    X_test = test_data[
        feature_columns
    ]

    model = SklearnPipeline(
        [
            (
                "scaler",
                SklearnStandardScaler()
            ),
            (
                "classifier",
                RandomForestClassifier(
                    n_estimators=100,
                    max_depth=6,
                    random_state=RANDOM_STATE,
                    n_jobs=-1
                )
            )
        ]
    )

    model.fit(
        X_train,
        y_train
    )

    predicted_classes = model.predict(
        X_test
    )

    probabilities = model.predict_proba(
        X_test
    )

    fold_predictions = test_data[
        [
            "item_id"
        ]
    ].copy()

    fold_predictions[
        "python_prediction"
    ] = predicted_classes

    fold_predictions[
        "python_probability"
    ] = probabilities.max(
        axis=1
    )

    fold_predictions[
        "fold_id"
    ] = fold_id

    python_predictions.append(
        fold_predictions
    )

python_oof = (
    pd.concat(
        python_predictions,
        ignore_index=True
    )
    .sort_values("item_id")
    .reset_index(drop=True)
)

print(
    f"Python OOF Predictions: "
    f"{len(python_oof)}"
)

print("\n========BUILD COMPARISON========")

comparison = (
    spark_oof
    .merge(
        python_oof,
        on=[
            "item_id",
            "fold_id"
        ],
        how="inner"
    )
)

comparison[
    "match_status"
] = comparison.apply(
    lambda row:
        "Match"
        if row["spark_prediction"] ==
        row["python_prediction"]
        else "Mismatch",
    axis=1
)

comparison[
    "probability_difference"
] = (
    comparison[
        "spark_probability"
    ] -
    comparison[
        "python_probability"
    ]
).abs()

comparison[
    "spark_correct"
] = (
    comparison[
        "spark_prediction"
    ] ==
    comparison[
        "actual_class"
    ]
)

comparison[
    "python_correct"
] = (
    comparison[
        "python_prediction"
    ] ==
    comparison[
        "actual_class"
    ]
)

def consistency_status(row):
    if row["match_status"] == "Match":
        if row["spark_correct"]:
            return "Consistent Correct"
        return "Consistent Incorrect"

    if row["spark_correct"] and not row["python_correct"]:
        return "Spark Correct / Python Incorrect"

    if row["python_correct"] and not row["spark_correct"]:
        return "Python Correct / Spark Incorrect"

    return "Both Disagree With Actual"

def disagreement_explanation(row):
    if row["match_status"] == "Match":
        if row["spark_correct"]:
            return (
                "Both independently trained models "
                "agree with the actual class."
            )
        return (
            "Both independently trained models "
            "agree with each other but not the actual class."
        )

    if row["spark_correct"] and not row["python_correct"]:
        return (
            "Spark Decision Tree matches the actual class; "
            "Python Random Forest selected a different class."
        )

    if row["python_correct"] and not row["spark_correct"]:
        return (
            "Python Random Forest matches the actual class; "
            "Spark Decision Tree selected a different class."
        )

    return (
        "The independently trained models disagree with "
        "each other and neither matches the actual class."
    )

comparison[
    "final_consistency_status"
] = comparison.apply(
    consistency_status,
    axis=1
)

comparison[
    "disagreement_explanation"
] = comparison.apply(
    disagreement_explanation,
    axis=1
)

comparison[
    "spark_model_version"
] = SPARK_MODEL_VERSION

comparison[
    "python_model_version"
] = PYTHON_MODEL_VERSION

comparison[
    "comparison_version"
] = COMPARISON_VERSION

comparison = comparison[
    [
        "item_id",
        "item_name",
        "fold_id",
        "actual_class",
        "spark_prediction",
        "python_prediction",
        "match_status",
        "spark_probability",
        "python_probability",
        "probability_difference",
        "spark_correct",
        "python_correct",
        "final_consistency_status",
        "disagreement_explanation",
        "spark_model_version",
        "python_model_version",
        "comparison_version"
    ]
].sort_values(
    "item_id"
).reset_index(
    drop=True
)

total_records = len(
    comparison
)

matches = int(
    (
        comparison[
            "match_status"
        ] == "Match"
    ).sum()
)

mismatches = (
    total_records -
    matches
)

agreement_percentage = (
    matches /
    total_records *
    100
)

spark_accuracy = (
    comparison[
        "spark_correct"
    ].mean() *
    100
)

python_accuracy = (
    comparison[
        "python_correct"
    ].mean() *
    100
)

summary = pd.DataFrame(
    [
        {
            "comparison_version":
                COMPARISON_VERSION,
            "total_unseen_cases":
                total_records,
            "matches":
                matches,
            "mismatches":
                mismatches,
            "agreement_percentage":
                agreement_percentage,
            "spark_oof_accuracy":
                spark_accuracy,
            "python_oof_accuracy":
                python_accuracy,
            "average_probability_difference":
                comparison[
                    "probability_difference"
                ].mean(),
            "spark_model_version":
                SPARK_MODEL_VERSION,
            "python_model_version":
                PYTHON_MODEL_VERSION
        }
    ]
)

print("\n========DUAL PIPELINE SUMMARY========")
print(
    f"Unseen Cases: {total_records}"
)
print(
    f"Matches: {matches}"
)
print(
    f"Mismatches: {mismatches}"
)
print(
    f"Overall Agreement: "
    f"{agreement_percentage:.2f}%"
)
print(
    f"Spark OOF Accuracy: "
    f"{spark_accuracy:.2f}%"
)
print(
    f"Python OOF Accuracy: "
    f"{python_accuracy:.2f}%"
)
print(
    "Average Probability Difference: "
    f"{comparison['probability_difference'].mean():.4f}"
)

print("\n========SAMPLE COMPARISON========")
print(
    comparison[
        [
            "item_id",
            "item_name",
            "actual_class",
            "spark_prediction",
            "python_prediction",
            "match_status",
            "final_consistency_status"
        ]
    ].head(20).to_string(
        index=False
    )
)

print("\n========MAJOR DISAGREEMENTS========")

major_disagreements = (
    comparison[
        comparison[
            "match_status"
        ] == "Mismatch"
    ]
    .sort_values(
        "probability_difference",
        ascending=False
    )
    .head(20)
)

if len(major_disagreements) > 0:
    print(
        major_disagreements[
            [
                "item_id",
                "item_name",
                "actual_class",
                "spark_prediction",
                "python_prediction",
                "spark_probability",
                "python_probability",
                "final_consistency_status"
            ]
        ].to_string(
            index=False
        )
    )
else:
    print(
        "No disagreements found."
    )

comparison.to_parquet(
    f"{output_folder}/"
    "dual_pipeline_menu_comparison.parquet",
    index=False
)

summary.to_parquet(
    f"{output_folder}/"
    "dual_pipeline_comparison_summary.parquet",
    index=False
)

comparison.to_csv(
    f"{output_folder}/"
    "dual_pipeline_menu_comparison.csv",
    index=False
)

summary.to_csv(
    f"{output_folder}/"
    "dual_pipeline_comparison_summary.csv",
    index=False
)

fold_assignment.to_parquet(
    f"{output_folder}/"
    "dual_pipeline_fold_assignments.parquet",
    index=False
)

print(
    "\nDual-pipeline comparison completed "
    "successfully."
)

spark.stop()
