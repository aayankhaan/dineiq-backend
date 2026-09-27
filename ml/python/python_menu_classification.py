import os
import json
import joblib
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix
from config.settings import ANALYTICS_DATA_FOLDER, ML_DATA_FOLDER, MODEL_FOLDER

input_folder = ANALYTICS_DATA_FOLDER
output_folder = f"{ML_DATA_FOLDER}/python"
model_folder = MODEL_FOLDER

MODEL_VERSION = "python_menu_classification_v1"
RANDOM_STATE = 42

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

required_columns = [
    "item_id",
    "item_name",
    "performance_class",
    *feature_columns
]

os.makedirs(output_folder, exist_ok=True)
os.makedirs(model_folder, exist_ok=True)

menu_classification = pd.read_parquet(
    f"{input_folder}/menu_classification"
)

ml_data = menu_classification[
    required_columns
].dropna().copy()

print("\n========ML DATASET========")
print(f"Total Records: {len(ml_data)}")
print(
    ml_data["performance_class"]
    .value_counts()
    .to_string()
)

X = ml_data[feature_columns]
y = ml_data["performance_class"]

X_train, X_temp, y_train, y_temp = train_test_split(
    X,
    y,
    test_size=0.30,
    random_state=RANDOM_STATE,
    stratify=y
)

X_validation, X_test, y_validation, y_test = train_test_split(
    X_temp,
    y_temp,
    test_size=0.50,
    random_state=RANDOM_STATE,
    stratify=y_temp
)

print("\n========DATA SPLIT========")
print(f"Training Records: {len(X_train)}")
print(f"Validation Records: {len(X_validation)}")
print(f"Testing Records: {len(X_test)}")

models = {
    "Logistic Regression": Pipeline([
        (
            "scaler",
            StandardScaler()
        ),
        (
            "classifier",
            LogisticRegression(
                max_iter=100,
                C=10.0,
                solver="lbfgs",
                random_state=RANDOM_STATE
            )
        )
    ]),
    "Decision Tree": Pipeline([
        (
            "scaler",
            StandardScaler()
        ),
        (
            "classifier",
            DecisionTreeClassifier(
                max_depth=5,
                random_state=RANDOM_STATE
            )
        )
    ]),
    "Random Forest": Pipeline([
        (
            "scaler",
            StandardScaler()
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
    ])
}

def calculate_metrics(actual, predicted):
    return {
        "accuracy": accuracy_score(actual, predicted),
        "weighted_precision": precision_score(
            actual,
            predicted,
            average="weighted",
            zero_division=0
        ),
        "weighted_recall": recall_score(
            actual,
            predicted,
            average="weighted",
            zero_division=0
        ),
        "f1": f1_score(
            actual,
            predicted,
            average="weighted",
            zero_division=0
        )
    }

model_results = []
trained_models = {}

for model_name, model in models.items():
    print(f"\n========TRAINING {model_name.upper()}========")

    model.fit(
        X_train,
        y_train
    )

    train_predictions = model.predict(
        X_train
    )
    validation_predictions = model.predict(
        X_validation
    )

    train_metrics = calculate_metrics(
        y_train,
        train_predictions
    )
    validation_metrics = calculate_metrics(
        y_validation,
        validation_predictions
    )

    model_results.append({
        "model_name": model_name,
        "train_accuracy": train_metrics["accuracy"],
        "train_weighted_precision": train_metrics["weighted_precision"],
        "train_weighted_recall": train_metrics["weighted_recall"],
        "train_f1": train_metrics["f1"],
        "validation_accuracy": validation_metrics["accuracy"],
        "validation_weighted_precision": validation_metrics["weighted_precision"],
        "validation_weighted_recall": validation_metrics["weighted_recall"],
        "validation_f1": validation_metrics["f1"]
    })

    trained_models[model_name] = model

    print("Training Results:")
    print(f"Accuracy: {train_metrics['accuracy']:.4f}")
    print(
        f"Weighted Precision: "
        f"{train_metrics['weighted_precision']:.4f}"
    )
    print(
        f"Weighted Recall: "
        f"{train_metrics['weighted_recall']:.4f}"
    )
    print(f"F1 Score: {train_metrics['f1']:.4f}")

    print("Validation Results:")
    print(f"Accuracy: {validation_metrics['accuracy']:.4f}")
    print(
        f"Weighted Precision: "
        f"{validation_metrics['weighted_precision']:.4f}"
    )
    print(
        f"Weighted Recall: "
        f"{validation_metrics['weighted_recall']:.4f}"
    )
    print(f"F1 Score: {validation_metrics['f1']:.4f}")

results_df = pd.DataFrame(
    model_results
).sort_values(
    "validation_f1",
    ascending=False
).reset_index(drop=True)

print("\n========MODEL COMPARISON========")
print(results_df.to_string(index=False))

best_result = results_df.iloc[0]
best_model_name = best_result["model_name"]
best_model = trained_models[best_model_name]

print(
    f"\nSelected Model: {best_model_name} "
    f"(Validation F1: {best_result['validation_f1']:.4f})"
)

test_predictions = best_model.predict(
    X_test
)

test_probabilities = best_model.predict_proba(
    X_test
)

test_metrics = calculate_metrics(
    y_test,
    test_predictions
)

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

test_rows = ml_data.loc[
    X_test.index,
    ["item_id", "item_name", "performance_class"]
].copy()

test_rows["predicted_class"] = test_predictions
test_rows["is_correct"] = (
    test_rows["performance_class"] ==
    test_rows["predicted_class"]
)
test_rows["probability"] = test_probabilities.max(axis=1)
test_rows["model_name"] = best_model_name
test_rows["model_version"] = MODEL_VERSION

test_rows = test_rows.rename(
    columns={
        "performance_class": "actual_class"
    }
)

print("\n========SAMPLE PREDICTIONS========")
print(
    test_rows[
        [
            "item_id",
            "item_name",
            "actual_class",
            "predicted_class",
            "is_correct"
        ]
    ].head(50).to_string(index=False)
)

labels = sorted(
    ml_data["performance_class"].unique()
)

matrix = confusion_matrix(
    y_test,
    test_predictions,
    labels=labels
)

confusion_rows = []

for actual_index, actual_class in enumerate(labels):
    for predicted_index, predicted_class in enumerate(labels):
        confusion_rows.append({
            "actual_class": actual_class,
            "predicted_class": predicted_class,
            "count": int(
                matrix[
                    actual_index,
                    predicted_index
                ]
            )
        })

confusion_df = pd.DataFrame(
    confusion_rows
)

print("\n========CONFUSION MATRIX========")
print(
    confusion_df[
        confusion_df["count"] > 0
    ].to_string(index=False)
)

test_result = {
    "model_name": best_model_name,
    "model_version": MODEL_VERSION,
    "test_accuracy": test_metrics["accuracy"],
    "test_weighted_precision": test_metrics["weighted_precision"],
    "test_weighted_recall": test_metrics["weighted_recall"],
    "test_f1": test_metrics["f1"]
}

results_df.to_parquet(
    f"{output_folder}/menu_classification_model_metrics.parquet",
    index=False
)

pd.DataFrame(
    [test_result]
).to_parquet(
    f"{output_folder}/menu_classification_test_metrics.parquet",
    index=False
)

test_rows[
    [
        "item_id",
        "item_name",
        "actual_class",
        "predicted_class",
        "is_correct",
        "probability",
        "model_name",
        "model_version"
    ]
].to_parquet(
    f"{output_folder}/menu_classification_predictions.parquet",
    index=False
)

confusion_df.to_parquet(
    f"{output_folder}/menu_classification_confusion_matrix.parquet",
    index=False
)

model_metadata = {
    "model_version": MODEL_VERSION,
    "selected_model": best_model_name,
    "feature_columns": feature_columns,
    "target_column": "performance_class",
    "random_state": RANDOM_STATE,
    "training_records": len(X_train),
    "validation_records": len(X_validation),
    "testing_records": len(X_test),
    "algorithms_tested": list(models.keys()),
    "hyperparameters": {
        "Logistic Regression": {
            "max_iter": 100,
            "C": 10.0,
            "solver": "lbfgs"
        },
        "Decision Tree": {
            "max_depth": 5
        },
        "Random Forest": {
            "n_estimators": 100,
            "max_depth": 6
        }
    },
    "test_metrics": {
        key: float(value)
        for key, value in test_metrics.items()
    }
}

with open(
    f"{model_folder}/{MODEL_VERSION}_metadata.json",
    "w",
    encoding="utf-8"
) as file:
    json.dump(
        model_metadata,
        file,
        indent=4
    )

joblib.dump(
    best_model,
    f"{model_folder}/{MODEL_VERSION}.joblib"
)

print(
    f"\nBest model saved to "
    f"{model_folder}/{MODEL_VERSION}.joblib"
)
print(
    "Python Data Science menu classification "
    "completed successfully."
)
