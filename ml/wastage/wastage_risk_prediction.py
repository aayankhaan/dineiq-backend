import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array

from config.settings import (
    INTEGRATED_DATA_FOLDER,
    ANALYTICS_DATA_FOLDER,
    ML_DATA_FOLDER,
    MODEL_FOLDER
)


spark = SparkSession.builder \
    .appName("DineIQ Wastage Risk Prediction") \
    .getOrCreate()


integrated_folder = INTEGRATED_DATA_FOLDER
wastage_analysis_folder = f"{ANALYTICS_DATA_FOLDER}/wastage"
output_folder = f"{ML_DATA_FOLDER}/wastage_risk"

MODEL_VERSION = "wastage_risk_v2"
DEFAULT_VALIDATION_DAYS = 31
DEFAULT_TEST_DAYS = 31
HIGH_WASTAGE_QUANTILE = 0.60
CLASS_WEIGHT_STRENGTH = 0.50

THRESHOLD_CANDIDATES = [
    0.10,
    0.15,
    0.20,
    0.25,
    0.30,
    0.35,
    0.40,
    0.45,
    0.50,
    0.55,
    0.60,
    0.65,
    0.70,
    0.75,
    0.80
]


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Train and evaluate a chronological "
            "DineIQ wastage-risk classifier."
        )
    )

    parser.add_argument(
        "--validation-days",
        type=int,
        default=DEFAULT_VALIDATION_DAYS
    )

    parser.add_argument(
        "--test-days",
        type=int,
        default=DEFAULT_TEST_DAYS
    )

    return parser.parse_args()


def add_class_weights(frame):
    counts = {
        float(row["label"]): int(row["count"])
        for row in frame.groupBy(
            "label"
        ).count().collect()
    }

    negative_count = counts.get(
        0.0,
        0
    )

    positive_count = counts.get(
        1.0,
        0
    )

    total_count = (
        negative_count +
        positive_count
    )

    if (
        negative_count == 0 or
        positive_count == 0
    ):
        raise ValueError(
            "Both wastage-risk classes are required "
            "to train the classifier."
        )

    balanced_negative_weight = (
        total_count /
        (2.0 * negative_count)
    )

    balanced_positive_weight = (
        total_count /
        (2.0 * positive_count)
    )

    negative_weight = (
        1.0 +
        CLASS_WEIGHT_STRENGTH *
        (
            balanced_negative_weight -
            1.0
        )
    )

    positive_weight = (
        1.0 +
        CLASS_WEIGHT_STRENGTH *
        (
            balanced_positive_weight -
            1.0
        )
    )

    weighted = frame.withColumn(
        "class_weight",
        F.when(
            F.col("label") == 1.0,
            F.lit(
                positive_weight
            )
        ).otherwise(
            F.lit(
                negative_weight
            )
        )
    )

    return (
        weighted,
        negative_count,
        positive_count,
        negative_weight,
        positive_weight
    )


def threshold_metrics(
    scored,
    threshold
):
    evaluated = scored.withColumn(
        "risk_prediction",
        F.when(
            F.col(
                "risk_probability"
            ) >= F.lit(
                threshold
            ),
            F.lit(1.0)
        ).otherwise(
            F.lit(0.0)
        )
    )

    counts = evaluated.agg(
        F.sum(
            F.when(
                (
                    F.col("label") == 1.0
                ) &
                (
                    F.col(
                        "risk_prediction"
                    ) == 1.0
                ),
                1
            ).otherwise(0)
        ).alias("tp"),
        F.sum(
            F.when(
                (
                    F.col("label") == 0.0
                ) &
                (
                    F.col(
                        "risk_prediction"
                    ) == 0.0
                ),
                1
            ).otherwise(0)
        ).alias("tn"),
        F.sum(
            F.when(
                (
                    F.col("label") == 0.0
                ) &
                (
                    F.col(
                        "risk_prediction"
                    ) == 1.0
                ),
                1
            ).otherwise(0)
        ).alias("fp"),
        F.sum(
            F.when(
                (
                    F.col("label") == 1.0
                ) &
                (
                    F.col(
                        "risk_prediction"
                    ) == 0.0
                ),
                1
            ).otherwise(0)
        ).alias("fn")
    ).first()

    tp = int(
        counts["tp"]
    )

    tn = int(
        counts["tn"]
    )

    fp = int(
        counts["fp"]
    )

    fn = int(
        counts["fn"]
    )

    total = (
        tp +
        tn +
        fp +
        fn
    )

    accuracy = (
        (tp + tn) / total
        if total > 0
        else 0.0
    )

    precision = (
        tp / (tp + fp)
        if (tp + fp) > 0
        else 0.0
    )

    recall = (
        tp / (tp + fn)
        if (tp + fn) > 0
        else 0.0
    )

    specificity = (
        tn / (tn + fp)
        if (tn + fp) > 0
        else 0.0
    )

    balanced_accuracy = (
        recall +
        specificity
    ) / 2.0

    positive_f1 = (
        2 * precision * recall /
        (precision + recall)
        if (precision + recall) > 0
        else 0.0
    )

    negative_precision = (
        tn / (tn + fn)
        if (tn + fn) > 0
        else 0.0
    )

    negative_recall = (
        tn / (tn + fp)
        if (tn + fp) > 0
        else 0.0
    )

    negative_f1 = (
        2 *
        negative_precision *
        negative_recall /
        (
            negative_precision +
            negative_recall
        )
        if (
            negative_precision +
            negative_recall
        ) > 0
        else 0.0
    )

    macro_f1 = (
        positive_f1 +
        negative_f1
    ) / 2.0

    return {
        "threshold": float(
            threshold
        ),
        "accuracy": float(
            accuracy
        ),
        "precision": float(
            precision
        ),
        "recall": float(
            recall
        ),
        "specificity": float(
            specificity
        ),
        "balanced_accuracy": float(
            balanced_accuracy
        ),
        "positive_f1": float(
            positive_f1
        ),
        "negative_f1": float(
            negative_f1
        ),
        "macro_f1": float(
            macro_f1
        ),
        "true_positive": tp,
        "true_negative": tn,
        "false_positive": fp,
        "false_negative": fn
    }


args = parse_args()

if args.validation_days <= 0:
    raise ValueError(
        "Validation days must be greater than zero."
    )

if args.test_days <= 0:
    raise ValueError(
        "Test days must be greater than zero."
    )


item_daily_wastage = spark.read.parquet(
    f"{wastage_analysis_folder}/item_daily_wastage"
)

recipes = spark.read.parquet(
    f"{integrated_folder}/recipes"
)

inventory = spark.read.parquet(
    f"{integrated_folder}/inventory"
)



date_range = item_daily_wastage.agg(
    F.min(
        "analysis_date"
    ).alias(
        "historical_start"
    ),
    F.max(
        "analysis_date"
    ).alias(
        "historical_end"
    )
).first()


historical_start = date_range[
    "historical_start"
]

historical_end = date_range[
    "historical_end"
]


test_end = historical_end

test_start = spark.sql(
    f"""
    SELECT date_sub(
        DATE('{test_end}'),
        {args.test_days - 1}
    ) AS value
    """
).first()["value"]


validation_end = spark.sql(
    f"""
    SELECT date_sub(
        DATE('{test_start}'),
        1
    ) AS value
    """
).first()["value"]


validation_start = spark.sql(
    f"""
    SELECT date_sub(
        DATE('{validation_end}'),
        {args.validation_days - 1}
    ) AS value
    """
).first()["value"]


training_end = spark.sql(
    f"""
    SELECT date_sub(
        DATE('{validation_start}'),
        1
    ) AS value
    """
).first()["value"]



series_master = item_daily_wastage.groupBy(
    "restaurant_id",
    "item_id"
).agg(
    F.min(
        "analysis_date"
    ).alias(
        "first_activity_date"
    ),
    F.first(
        "restaurant_name",
        ignorenulls=True
    ).alias(
        "restaurant_name"
    ),
    F.first(
        "restaurant_city",
        ignorenulls=True
    ).alias(
        "restaurant_city"
    ),
    F.first(
        "restaurant_area",
        ignorenulls=True
    ).alias(
        "restaurant_area"
    ),
    F.first(
        "item_name",
        ignorenulls=True
    ).alias(
        "item_name"
    ),
    F.first(
        "category_id",
        ignorenulls=True
    ).alias(
        "category_id"
    ),
    F.first(
        "category_name",
        ignorenulls=True
    ).alias(
        "category_name"
    )
)


calendar = spark.range(
    1
).select(
    F.explode(
        F.sequence(
            F.lit(
                historical_start
            ),
            F.lit(
                historical_end
            ),
            F.expr(
                "interval 1 day"
            )
        )
    ).alias(
        "analysis_date"
    )
)


daily_grid = series_master.crossJoin(
    calendar
).filter(
    F.col(
        "analysis_date"
    ) >=
    F.col(
        "first_activity_date"
    )
)


daily_values = item_daily_wastage.select(
    "analysis_date",
    "restaurant_id",
    "item_id",
    F.col(
        "demand_quantity"
    ).cast(
        "double"
    ).alias(
        "demand_quantity"
    ),
    F.col(
        "estimated_wastage_cost"
    ).cast(
        "double"
    ).alias(
        "estimated_wastage_cost"
    ),
    F.col(
        "estimated_preparation_quantity"
    ).cast(
        "double"
    ).alias(
        "estimated_preparation_quantity"
    ),
    F.col(
        "promotion_share_pct"
    ).cast(
        "double"
    ).alias(
        "promotion_share_pct"
    )
)


daily_grid = daily_grid.join(
    daily_values,
    [
        "analysis_date",
        "restaurant_id",
        "item_id"
    ],
    "left"
).fillna(
    {
        "demand_quantity": 0.0,
        "estimated_wastage_cost": 0.0,
        "estimated_preparation_quantity": 0.0,
        "promotion_share_pct": 0.0
    }
)



series_recipes = series_master.select(
    "restaurant_id",
    "item_id"
).join(
    recipes.select(
        F.col(
            "menu_item_id"
        ).alias(
            "item_id"
        ),
        "ingredient_id"
    ),
    "item_id",
    "inner"
)


item_inventory_batches = series_recipes.alias(
    "sr"
).join(
    inventory.alias(
        "inv"
    ),
    (
        F.col(
            "sr.restaurant_id"
        ) ==
        F.col(
            "inv.restaurant_id"
        )
    ) &
    (
        F.col(
            "sr.ingredient_id"
        ) ==
        F.col(
            "inv.ingredient_id"
        )
    ),
    "inner"
).select(
    F.col(
        "sr.restaurant_id"
    ).alias(
        "restaurant_id"
    ),
    F.col(
        "sr.item_id"
    ).alias(
        "item_id"
    ),
    F.col(
        "inv.inventory_id"
    ).alias(
        "inventory_id"
    ),
    F.col(
        "inv.quantity_received"
    ).cast(
        "double"
    ).alias(
        "quantity_received"
    ),
    F.col(
        "inv.received_date"
    ).alias(
        "received_date"
    ),
    F.col(
        "inv.expiry_date"
    ).alias(
        "expiry_date"
    )
).filter(
    F.col(
        "quantity_received"
    ) > 0
)


daily_inventory_context = item_inventory_batches.withColumn(
    "analysis_date",
    F.explode(
        F.sequence(
            F.col(
                "received_date"
            ),
            F.col(
                "expiry_date"
            ),
            F.expr(
                "interval 1 day"
            )
        )
    )
).filter(
    (
        F.col(
            "analysis_date"
        ) >=
        F.lit(
            historical_start
        )
    ) &
    (
        F.col(
            "analysis_date"
        ) <=
        F.lit(
            historical_end
        )
    )
).withColumn(
    "days_to_expiry",
    F.datediff(
        "expiry_date",
        "analysis_date"
    )
).groupBy(
    "analysis_date",
    "restaurant_id",
    "item_id"
).agg(
    F.countDistinct(
        "inventory_id"
    ).alias(
        "active_inventory_batches"
    ),
    F.sum(
        "quantity_received"
    ).alias(
        "active_inventory_received_quantity"
    ),
    F.avg(
        "days_to_expiry"
    ).alias(
        "average_days_to_expiry"
    ),
    F.avg(
        F.when(
            F.col(
                "days_to_expiry"
            ) <= 7,
            1.0
        ).otherwise(
            0.0
        )
    ).alias(
        "expiring_within_7_days_ratio"
    ),
    F.sum(
        F.when(
            F.col(
                "days_to_expiry"
            ) <= 7,
            F.col(
                "quantity_received"
            )
        ).otherwise(
            F.lit(0.0)
        )
    ).alias(
        "expiring_within_7_days_received_quantity"
    )
)



daily_grid = daily_grid.join(
    daily_inventory_context,
    [
        "analysis_date",
        "restaurant_id",
        "item_id"
    ],
    "left"
).fillna(
    {
        "active_inventory_batches": 0,
        "active_inventory_received_quantity": 0.0,
        "average_days_to_expiry": 0.0,
        "expiring_within_7_days_ratio": 0.0,
        "expiring_within_7_days_received_quantity": 0.0
    }
)



series_window = Window.partitionBy(
    "restaurant_id",
    "item_id"
).orderBy(
    "analysis_date"
)


rolling_7_window = series_window.rowsBetween(
    -7,
    -1
)


rolling_28_window = series_window.rowsBetween(
    -28,
    -1
)


history_window = series_window.rowsBetween(
    Window.unboundedPreceding,
    -1
)


risk_data = daily_grid.withColumn(
    "lag_1_demand",
    F.lag(
        "demand_quantity",
        1
    ).over(
        series_window
    )
).withColumn(
    "lag_7_demand",
    F.lag(
        "demand_quantity",
        7
    ).over(
        series_window
    )
).withColumn(
    "rolling_7_demand",
    F.avg(
        "demand_quantity"
    ).over(
        rolling_7_window
    )
).withColumn(
    "rolling_28_demand",
    F.avg(
        "demand_quantity"
    ).over(
        rolling_28_window
    )
).withColumn(
    "historical_average_demand",
    F.avg(
        "demand_quantity"
    ).over(
        history_window
    )
).withColumn(
    "rolling_7_demand_std",
    F.stddev_pop(
        "demand_quantity"
    ).over(
        rolling_7_window
    )
).withColumn(
    "rolling_28_demand_std",
    F.stddev_pop(
        "demand_quantity"
    ).over(
        rolling_28_window
    )
).withColumn(
    "lag_1_wastage_cost",
    F.lag(
        "estimated_wastage_cost",
        1
    ).over(
        series_window
    )
).withColumn(
    "lag_7_wastage_cost",
    F.lag(
        "estimated_wastage_cost",
        7
    ).over(
        series_window
    )
).withColumn(
    "rolling_7_wastage_cost",
    F.avg(
        "estimated_wastage_cost"
    ).over(
        rolling_7_window
    )
).withColumn(
    "rolling_28_wastage_cost",
    F.avg(
        "estimated_wastage_cost"
    ).over(
        rolling_28_window
    )
).withColumn(
    "historical_average_wastage_cost",
    F.avg(
        "estimated_wastage_cost"
    ).over(
        history_window
    )
).withColumn(
    "historical_positive_wastage_cost",
    F.avg(
        F.when(
            F.col(
                "estimated_wastage_cost"
            ) > 0,
            F.col(
                "estimated_wastage_cost"
            )
        )
    ).over(
        history_window
    )
).withColumn(
    "lag_1_wastage_flag",
    F.when(
        F.coalesce(
            F.col(
                "lag_1_wastage_cost"
            ),
            F.lit(0.0)
        ) > 0,
        1.0
    ).otherwise(
        0.0
    )
).withColumn(
    "rolling_7_wastage_days",
    F.sum(
        F.when(
            F.col(
                "estimated_wastage_cost"
            ) > 0,
            1.0
        ).otherwise(
            0.0
        )
    ).over(
        rolling_7_window
    )
).withColumn(
    "rolling_28_wastage_days",
    F.sum(
        F.when(
            F.col(
                "estimated_wastage_cost"
            ) > 0,
            1.0
        ).otherwise(
            0.0
        )
    ).over(
        rolling_28_window
    )
).withColumn(
    "historical_wastage_rate",
    F.avg(
        F.when(
            F.col(
                "estimated_wastage_cost"
            ) > 0,
            1.0
        ).otherwise(
            0.0
        )
    ).over(
        history_window
    )
).withColumn(
    "lag_1_promotion_share_pct",
    F.lag(
        "promotion_share_pct",
        1
    ).over(
        series_window
    )
).withColumn(
    "rolling_7_promotion_share_pct",
    F.avg(
        "promotion_share_pct"
    ).over(
        rolling_7_window
    )
).withColumn(
    "lag_1_preparation_quantity",
    F.lag(
        "estimated_preparation_quantity",
        1
    ).over(
        series_window
    )
).withColumn(
    "rolling_7_preparation_quantity",
    F.avg(
        "estimated_preparation_quantity"
    ).over(
        rolling_7_window
    )
).withColumn(
    "menu_popularity_28d",
    F.coalesce(
        F.col(
            "rolling_28_demand"
        ),
        F.lit(0.0)
    )
).withColumn(
    "forecast_demand_proxy",
    (
        F.coalesce(
            F.col(
                "lag_7_demand"
            ),
            F.lit(0.0)
        ) +
        F.coalesce(
            F.col(
                "rolling_7_demand"
            ),
            F.lit(0.0)
        )
    ) / 2.0
).withColumn(
    "target_date",
    F.date_add(
        "analysis_date",
        1
    )
).withColumn(
    "target_day_of_week_number",
    F.dayofweek(
        "target_date"
    )
).withColumn(
    "target_month_number",
    F.month(
        "target_date"
    )
).withColumn(
    "target_is_weekend",
    F.when(
        F.col(
            "target_day_of_week_number"
        ).isin(
            1,
            6,
            7
        ),
        1.0
    ).otherwise(
        0.0
    )
).withColumn(
    "target_season",
    F.when(
        F.col(
            "target_month_number"
        ).isin(
            12,
            1,
            2
        ),
        "Winter"
    ).when(
        F.col(
            "target_month_number"
        ).isin(
            3,
            4,
            5
        ),
        "Spring"
    ).when(
        F.col(
            "target_month_number"
        ).isin(
            6,
            7,
            8
        ),
        "Summer"
    ).otherwise(
        "Autumn"
    )
).withColumn(
    "next_wastage_cost",
    F.lead(
        "estimated_wastage_cost",
        1
    ).over(
        series_window
    )
).withColumn(
    "history_days",
    F.datediff(
        "analysis_date",
        "first_activity_date"
    )
)


risk_data = risk_data.filter(
    F.col(
        "next_wastage_cost"
    ).isNotNull() &
    (
        F.col(
            "history_days"
        ) >= 28
    )
)


training_positive_costs = risk_data.filter(
    (
        F.col(
            "target_date"
        ) <=
        F.lit(
            training_end
        )
    ) &
    (
        F.col(
            "next_wastage_cost"
        ) > 0
    )
)


high_wastage_cost_threshold = training_positive_costs.approxQuantile(
    "next_wastage_cost",
    [
        HIGH_WASTAGE_QUANTILE
    ],
    0.001
)[0]


if high_wastage_cost_threshold <= 0:
    raise ValueError(
        "Training-derived high-wastage cost threshold must be positive."
    )


risk_data = risk_data.withColumn(
    "label",
    F.when(
        F.col(
            "next_wastage_cost"
        ) >=
        F.lit(
            high_wastage_cost_threshold
        ),
        1.0
    ).otherwise(
        0.0
    )
)


fill_zero_columns = [
    "lag_1_demand",
    "lag_7_demand",
    "rolling_7_demand",
    "rolling_28_demand",
    "historical_average_demand",
    "rolling_7_demand_std",
    "rolling_28_demand_std",
    "lag_1_wastage_cost",
    "lag_7_wastage_cost",
    "rolling_7_wastage_cost",
    "rolling_28_wastage_cost",
    "historical_average_wastage_cost",
    "historical_positive_wastage_cost",
    "lag_1_wastage_flag",
    "rolling_7_wastage_days",
    "rolling_28_wastage_days",
    "historical_wastage_rate",
    "lag_1_promotion_share_pct",
    "rolling_7_promotion_share_pct",
    "lag_1_preparation_quantity",
    "rolling_7_preparation_quantity",
    "menu_popularity_28d",
    "forecast_demand_proxy"
]


risk_data = risk_data.fillna(
    0.0,
    subset=fill_zero_columns
).withColumn(
    "restaurant_id_string",
    F.col(
        "restaurant_id"
    ).cast(
        "string"
    )
).withColumn(
    "category_id_string",
    F.col(
        "category_id"
    ).cast(
        "string"
    )
)


train_data = risk_data.filter(
    F.col(
        "target_date"
    ) <=
    F.lit(
        training_end
    )
)


validation_data = risk_data.filter(
    (
        F.col(
            "target_date"
        ) >=
        F.lit(
            validation_start
        )
    ) &
    (
        F.col(
            "target_date"
        ) <=
        F.lit(
            validation_end
        )
    )
)


test_data = risk_data.filter(
    (
        F.col(
            "target_date"
        ) >=
        F.lit(
            test_start
        )
    ) &
    (
        F.col(
            "target_date"
        ) <=
        F.lit(
            test_end
        )
    )
)


train_data, train_negative_count, train_positive_count, train_negative_weight, train_positive_weight = add_class_weights(
    train_data
)


validation_data = validation_data.withColumn(
    "class_weight",
    F.lit(
        1.0
    )
)




numeric_feature_columns = [
    "target_day_of_week_number",
    "target_month_number",
    "target_is_weekend",
    "lag_1_demand",
    "lag_7_demand",
    "rolling_7_demand",
    "rolling_28_demand",
    "historical_average_demand",
    "rolling_7_demand_std",
    "rolling_28_demand_std",
    "lag_1_wastage_cost",
    "lag_7_wastage_cost",
    "rolling_7_wastage_cost",
    "rolling_28_wastage_cost",
    "historical_average_wastage_cost",
    "historical_positive_wastage_cost",
    "lag_1_wastage_flag",
    "rolling_7_wastage_days",
    "rolling_28_wastage_days",
    "historical_wastage_rate",
    "lag_1_promotion_share_pct",
    "rolling_7_promotion_share_pct",
    "lag_1_preparation_quantity",
    "rolling_7_preparation_quantity",
    "menu_popularity_28d",
    "forecast_demand_proxy",
    "active_inventory_batches",
    "active_inventory_received_quantity",
    "average_days_to_expiry",
    "expiring_within_7_days_ratio",
    "expiring_within_7_days_received_quantity"
]


restaurant_indexer = StringIndexer(
    inputCol="restaurant_id_string",
    outputCol="restaurant_index",
    handleInvalid="keep"
)


category_indexer = StringIndexer(
    inputCol="category_id_string",
    outputCol="category_index",
    handleInvalid="keep"
)


season_indexer = StringIndexer(
    inputCol="target_season",
    outputCol="season_index",
    handleInvalid="keep"
)


encoder = OneHotEncoder(
    inputCols=[
        "restaurant_index",
        "category_index",
        "season_index"
    ],
    outputCols=[
        "restaurant_vector",
        "category_vector",
        "season_vector"
    ],
    handleInvalid="keep"
)


assembler = VectorAssembler(
    inputCols=[
        *numeric_feature_columns,
        "restaurant_vector",
        "category_vector",
        "season_vector"
    ],
    outputCol="features",
    handleInvalid="keep"
)


classifier = RandomForestClassifier(
    featuresCol="features",
    labelCol="label",
    weightCol="class_weight",
    predictionCol="model_prediction",
    probabilityCol="probability",
    rawPredictionCol="rawPrediction",
    numTrees=60,
    maxDepth=9,
    minInstancesPerNode=8,
    featureSubsetStrategy="sqrt",
    seed=42
)


pipeline = Pipeline(
    stages=[
        restaurant_indexer,
        category_indexer,
        season_indexer,
        encoder,
        assembler,
        classifier
    ]
)


print("\\n========WASTAGE RISK SETUP========")
print(
    f"Historical Period: "
    f"{historical_start} to {historical_end}"
)
print(
    f"Training Target Period Ends: "
    f"{training_end}"
)
print(
    f"Validation Target Period: "
    f"{validation_start} to {validation_end}"
)
print(
    f"Test Target Period: "
    f"{test_start} to {test_end}"
)
print(
    "Prediction Target: "
    "High-cost wastage risk on the next day"
)
print(
    f"High-Wastage Cost Threshold: "
    f"{high_wastage_cost_threshold:.2f}"
)
print(
    f"Threshold Source: "
    f"{HIGH_WASTAGE_QUANTILE:.0%} quantile of positive "
    f"training-period wastage costs"
)
print(
    "Random Split Used: No"
)
print(
    "Future Outcome Features Used: No"
)
print(
    f"Training Positive Rows: "
    f"{train_positive_count:,}"
)
print(
    f"Training Negative Rows: "
    f"{train_negative_count:,}"
)



training_model = pipeline.fit(
    train_data
)


validation_scored = training_model.transform(
    validation_data
).withColumn(
    "risk_probability",
    vector_to_array(
        "probability"
    )[1]
)


validation_threshold_rows = [
    threshold_metrics(
        validation_scored,
        threshold
    )
    for threshold in
    THRESHOLD_CANDIDATES
]


validation_threshold_rows.sort(
    key=lambda row: (
        row[
            "macro_f1"
        ],
        row[
            "balanced_accuracy"
        ],
        row[
            "accuracy"
        ]
    ),
    reverse=True
)


selected_threshold = validation_threshold_rows[
    0
]["threshold"]


validation_threshold_metrics = spark.createDataFrame(
    validation_threshold_rows
).orderBy(
    "threshold"
)


print("\\n========VALIDATION THRESHOLD SELECTION========")
validation_threshold_metrics.select(
    "threshold",
    F.round(
        "accuracy",
        4
    ).alias(
        "accuracy"
    ),
    F.round(
        "precision",
        4
    ).alias(
        "precision"
    ),
    F.round(
        "recall",
        4
    ).alias(
        "recall"
    ),
    F.round(
        "balanced_accuracy",
        4
    ).alias(
        "balanced_accuracy"
    ),
    F.round(
        "positive_f1",
        4
    ).alias(
        "positive_f1"
    ),
    F.round(
        "macro_f1",
        4
    ).alias(
        "macro_f1"
    )
).show(
    truncate=False
)


print(
    f"Selected Probability Threshold: "
    f"{selected_threshold:.2f}"
)




train_validation_data = risk_data.filter(
    F.col(
        "target_date"
    ) <=
    F.lit(
        validation_end
    )
)


train_validation_data, final_negative_count, final_positive_count, final_negative_weight, final_positive_weight = add_class_weights(
    train_validation_data
)


final_model = pipeline.fit(
    train_validation_data
)


test_scored = final_model.transform(
    test_data.withColumn(
        "class_weight",
        F.lit(
            1.0
        )
    )
).withColumn(
    "risk_probability",
    vector_to_array(
        "probability"
    )[1]
).withColumn(
    "risk_prediction",
    F.when(
        F.col(
            "risk_probability"
        ) >=
        F.lit(
            selected_threshold
        ),
        F.lit(1.0)
    ).otherwise(
        F.lit(0.0)
    )
)


test_metric_values = threshold_metrics(
    test_scored,
    selected_threshold
)


auc_evaluator = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)


pr_evaluator = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderPR"
)


test_auc = float(
    auc_evaluator.evaluate(
        test_scored
    )
)


test_auprc = float(
    pr_evaluator.evaluate(
        test_scored
    )
)


test_total = (
    test_metric_values[
        "true_positive"
    ] +
    test_metric_values[
        "true_negative"
    ] +
    test_metric_values[
        "false_positive"
    ] +
    test_metric_values[
        "false_negative"
    ]
)


test_positive = (
    test_metric_values[
        "true_positive"
    ] +
    test_metric_values[
        "false_negative"
    ]
)


test_negative = (
    test_metric_values[
        "true_negative"
    ] +
    test_metric_values[
        "false_positive"
    ]
)


majority_baseline_accuracy = (
    max(
        test_positive,
        test_negative
    ) /
    test_total
    if test_total > 0
    else 0.0
)


majority_precision = majority_baseline_accuracy


majority_class_f1 = (
    (
        2.0 *
        majority_precision
    ) /
    (
        majority_precision +
        1.0
    )
    if majority_precision > 0
    else 0.0
)


majority_baseline_macro_f1 = (
    majority_class_f1 /
    2.0
)


macro_f1_improvement_over_baseline = (
    test_metric_values[
        "macro_f1"
    ] -
    majority_baseline_macro_f1
)


model_metrics = spark.createDataFrame([
    (
        "test",
        int(
            test_total
        ),
        int(
            test_positive
        ),
        int(
            test_negative
        ),
        float(
            high_wastage_cost_threshold
        ),
        float(
            selected_threshold
        ),
        float(
            test_metric_values[
                "accuracy"
            ]
        ),
        float(
            test_metric_values[
                "precision"
            ]
        ),
        float(
            test_metric_values[
                "recall"
            ]
        ),
        float(
            test_metric_values[
                "specificity"
            ]
        ),
        float(
            test_metric_values[
                "balanced_accuracy"
            ]
        ),
        float(
            test_metric_values[
                "positive_f1"
            ]
        ),
        float(
            test_metric_values[
                "negative_f1"
            ]
        ),
        float(
            test_metric_values[
                "macro_f1"
            ]
        ),
        float(
            test_auc
        ),
        float(
            test_auprc
        ),
        float(
            majority_baseline_accuracy
        ),
        float(
            majority_baseline_macro_f1
        ),
        float(
            macro_f1_improvement_over_baseline
        ),
        int(
            test_metric_values[
                "true_positive"
            ]
        ),
        int(
            test_metric_values[
                "true_negative"
            ]
        ),
        int(
            test_metric_values[
                "false_positive"
            ]
        ),
        int(
            test_metric_values[
                "false_negative"
            ]
        ),
        MODEL_VERSION
    )
], [
    "split",
    "row_count",
    "positive_actual_rows",
    "negative_actual_rows",
    "high_wastage_cost_threshold",
    "selected_probability_threshold",
    "accuracy",
    "precision",
    "recall",
    "specificity",
    "balanced_accuracy",
    "positive_f1",
    "negative_f1",
    "macro_f1",
    "auc_roc",
    "auc_pr",
    "majority_baseline_accuracy",
    "majority_baseline_macro_f1",
    "macro_f1_improvement_over_baseline",
    "true_positive",
    "true_negative",
    "false_positive",
    "false_negative",
    "model_version"
])


test_predictions = test_scored.select(
    F.col(
        "analysis_date"
    ).alias(
        "feature_date"
    ),
    "target_date",
    "restaurant_id",
    "restaurant_name",
    "restaurant_city",
    "restaurant_area",
    "item_id",
    "item_name",
    "category_id",
    "category_name",
    "next_wastage_cost",
    F.lit(
        high_wastage_cost_threshold
    ).alias(
        "high_wastage_cost_threshold"
    ),
    F.col(
        "label"
    ).cast(
        "integer"
    ).alias(
        "actual_high_wastage_risk"
    ),
    F.col(
        "risk_prediction"
    ).cast(
        "integer"
    ).alias(
        "predicted_high_wastage_risk"
    ),
    F.round(
        "risk_probability",
        6
    ).alias(
        "risk_probability"
    ),
    "forecast_demand_proxy",
    "historical_average_demand",
    "historical_average_wastage_cost",
    "lag_1_wastage_cost",
    "rolling_7_wastage_cost",
    "menu_popularity_28d",
    "lag_1_promotion_share_pct",
    "lag_1_preparation_quantity",
    "active_inventory_batches",
    "active_inventory_received_quantity",
    "average_days_to_expiry",
    "expiring_within_7_days_ratio",
    "expiring_within_7_days_received_quantity"
).withColumn(
    "risk_level",
    F.when(
        F.col(
            "predicted_high_wastage_risk"
        ) == 1,
        "High Risk"
    ).otherwise(
        "Low Risk"
    )
)


high_risk_predictions = test_predictions.filter(
    F.col(
        "predicted_high_wastage_risk"
    ) == 1
).orderBy(
    F.desc(
        "risk_probability"
    )
)


item_risk_summary = test_predictions.groupBy(
    "item_id",
    "item_name",
    "category_id",
    "category_name"
).agg(
    F.count(
        "*"
    ).alias(
        "test_records"
    ),
    F.sum(
        "actual_high_wastage_risk"
    ).alias(
        "actual_high_risk_records"
    ),
    F.sum(
        "predicted_high_wastage_risk"
    ).alias(
        "predicted_high_risk_records"
    ),
    F.round(
        F.avg(
            "risk_probability"
        ),
        4
    ).alias(
        "average_risk_probability"
    ),
    F.round(
        F.max(
            "risk_probability"
        ),
        4
    ).alias(
        "maximum_risk_probability"
    ),
    F.round(
        F.sum(
            "next_wastage_cost"
        ),
        2
    ).alias(
        "actual_wastage_cost"
    )
).orderBy(
    F.desc(
        "average_risk_probability"
    )
)


period_risk_summary = test_predictions.groupBy(
    "target_date"
).agg(
    F.count(
        "*"
    ).alias(
        "evaluated_item_locations"
    ),
    F.sum(
        "actual_high_wastage_risk"
    ).alias(
        "actual_high_risk_count"
    ),
    F.sum(
        "predicted_high_wastage_risk"
    ).alias(
        "predicted_high_risk_count"
    ),
    F.round(
        F.avg(
            "risk_probability"
        ),
        4
    ).alias(
        "average_risk_probability"
    ),
    F.round(
        F.sum(
            "next_wastage_cost"
        ),
        2
    ).alias(
        "actual_wastage_cost"
    )
).orderBy(
    "target_date"
)



model_feature_columns = [
    "feature_date",
    "target_date",
    "restaurant_id",
    "item_id",
    "category_id",
    "target_season",
    "label",
    "next_wastage_cost",
    F.lit(
        high_wastage_cost_threshold
    ).alias(
        "high_wastage_cost_threshold"
    ),
    *numeric_feature_columns
]


risk_model_data = risk_data.select(
    F.col(
        "analysis_date"
    ).alias(
        "feature_date"
    ),
    "target_date",
    "restaurant_id",
    "item_id",
    "category_id",
    "target_season",
    "label",
    "next_wastage_cost",
    *numeric_feature_columns
)


metadata = spark.createDataFrame([
    (
        historical_start,
        historical_end,
        training_end,
        validation_start,
        validation_end,
        test_start,
        test_end,
        int(
            args.validation_days
        ),
        int(
            args.test_days
        ),
        False,
        False,
        "next_day_high_wastage_cost",
        "past_only_lags_rolling_history_and_receipt_time_inventory_context",
        ",".join(
            numeric_feature_columns
        ),
        ",".join(
            [
                "restaurant_id",
                "category_id",
                "target_season"
            ]
        ),
        float(
            HIGH_WASTAGE_QUANTILE
        ),
        float(
            high_wastage_cost_threshold
        ),
        float(
            CLASS_WEIGHT_STRENGTH
        ),
        float(
            selected_threshold
        ),
        int(
            train_positive_count
        ),
        int(
            train_negative_count
        ),
        float(
            train_positive_weight
        ),
        float(
            train_negative_weight
        ),
        MODEL_VERSION
    )
], [
    "historical_start_date",
    "historical_end_date",
    "training_end_date",
    "validation_start_date",
    "validation_end_date",
    "test_start_date",
    "test_end_date",
    "validation_days",
    "test_days",
    "random_split_used",
    "future_outcome_features_used",
    "prediction_target",
    "feature_policy",
    "numeric_features",
    "categorical_features",
    "high_wastage_quantile",
    "high_wastage_cost_threshold",
    "class_weight_strength",
    "selected_probability_threshold",
    "training_positive_rows",
    "training_negative_rows",
    "training_positive_weight",
    "training_negative_weight",
    "model_version"
])

print("\\n========WASTAGE RISK TEST METRICS========")
model_metrics.select(
    "row_count",
    "positive_actual_rows",
    "negative_actual_rows",
    F.round(
        "accuracy",
        4
    ).alias(
        "accuracy"
    ),
    F.round(
        "precision",
        4
    ).alias(
        "precision"
    ),
    F.round(
        "recall",
        4
    ).alias(
        "recall"
    ),
    F.round(
        "balanced_accuracy",
        4
    ).alias(
        "balanced_accuracy"
    ),
    F.round(
        "positive_f1",
        4
    ).alias(
        "positive_f1"
    ),
    F.round(
        "macro_f1",
        4
    ).alias(
        "macro_f1"
    ),
    F.round(
        "auc_roc",
        4
    ).alias(
        "auc_roc"
    ),
    F.round(
        "auc_pr",
        4
    ).alias(
        "auc_pr"
    ),
    F.round(
        "majority_baseline_accuracy",
        4
    ).alias(
        "majority_baseline_accuracy"
    ),
    F.round(
        "majority_baseline_macro_f1",
        4
    ).alias(
        "majority_baseline_macro_f1"
    ),
    F.round(
        "macro_f1_improvement_over_baseline",
        4
    ).alias(
        "macro_f1_improvement_over_baseline"
    ),
    "true_positive",
    "true_negative",
    "false_positive",
    "false_negative"
).show(
    truncate=False
)


print("\\n========HIGHEST-RISK MENU ITEMS========")
item_risk_summary.show(
    20,
    truncate=False
)


print("\\n========HIGHEST-RISK ITEM PERIODS========")
high_risk_predictions.select(
    "target_date",
    "restaurant_name",
    "item_name",
    "risk_probability",
    "actual_high_wastage_risk",
    "next_wastage_cost"
).show(
    20,
    truncate=False
)



risk_model_data.write.mode(
    "overwrite"
).parquet(
    f"{output_folder}/risk_model_data"
)


validation_threshold_metrics.write.mode(
    "overwrite"
).parquet(
    f"{output_folder}/validation_threshold_metrics"
)


test_predictions.write.mode(
    "overwrite"
).parquet(
    f"{output_folder}/test_predictions"
)


high_risk_predictions.write.mode(
    "overwrite"
).parquet(
    f"{output_folder}/high_risk_predictions"
)


item_risk_summary.write.mode(
    "overwrite"
).parquet(
    f"{output_folder}/item_risk_summary"
)


period_risk_summary.write.mode(
    "overwrite"
).parquet(
    f"{output_folder}/period_risk_summary"
)


model_metrics.write.mode(
    "overwrite"
).parquet(
    f"{output_folder}/model_metrics"
)


metadata.write.mode(
    "overwrite"
).parquet(
    f"{output_folder}/risk_metadata"
)


Path(
    MODEL_FOLDER
).mkdir(
    parents=True,
    exist_ok=True
)


final_model.write().overwrite().save(
    f"{MODEL_FOLDER}/{MODEL_VERSION}"
)


print("\\nWastage risk prediction completed successfully.")


spark.stop()
