from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.settings import ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("DineIQ Recommendation Evidence") \
    .getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "8")


recommendations = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/recommendations/recommendations"
)

output_folder = f"{ANALYTICS_DATA_FOLDER}/recommendation_evidence"


evidence = recommendations.select(
    "recommendation_id",
    "recommendation_type",
    "source_analysis",
    "recommendation_scope",
    "entity_key",
    "restaurant_id",
    "restaurant_name",
    "item_id",
    "item_name",
    "promotion_id",
    "customer_segment",
    "recommended_action",
    "impact_value",
    "impact_unit",
    F.posexplode(
        F.split(
            F.col("evidence_summary"),
            r"\s+\|\s+"
        )
    ).alias(
        "evidence_order_zero",
        "evidence_text"
    )
).withColumn(
    "evidence_order",
    F.col("evidence_order_zero") + 1
).drop(
    "evidence_order_zero"
).withColumn(
    "evidence_label",
    F.trim(
        F.substring_index(
            F.col("evidence_text"),
            ":",
            1
        )
    )
).withColumn(
    "evidence_value_text",
    F.trim(
        F.regexp_replace(
            F.col("evidence_text"),
            r"^[^:]+:\s*",
            ""
        )
    )
).withColumn(
    "evidence_numeric_value",
    F.when(
        F.regexp_extract(
            F.col("evidence_value_text"),
            r"-?\d+(?:\.\d+)?",
            0
        ) != "",
        F.regexp_extract(
            F.col("evidence_value_text"),
            r"-?\d+(?:\.\d+)?",
            0
        ).cast("double")
    )
)


evidence = evidence.withColumn(
    "evidence_category",
    F.when(
        F.lower("evidence_label").contains("margin") |
        F.lower("evidence_label").contains("profit"),
        "Profitability"
    ).when(
        F.lower("evidence_label").contains("wastage"),
        "Wastage"
    ).when(
        F.lower("evidence_label").contains("rating"),
        "Rating"
    ).when(
        F.lower("evidence_label").contains("sensitivity") |
        F.lower("evidence_label").contains("price"),
        "Pricing"
    ).when(
        F.lower("evidence_label").contains("lift") |
        F.lower("evidence_label").contains("support") |
        F.lower("evidence_label").contains("confidence") |
        F.lower("evidence_label").contains("pair"),
        "Association"
    ).when(
        F.lower("evidence_label").contains("forecast") |
        F.lower("evidence_label").contains("demand"),
        "Demand Forecast"
    ).when(
        F.lower("evidence_label").contains("customer") |
        F.lower("evidence_label").contains("recency"),
        "Customer"
    ).when(
        F.lower("evidence_label").contains("promotion") |
        F.lower("evidence_label").contains("kpi"),
        "Promotion"
    ).when(
        F.lower("evidence_label").contains("anomaly") |
        F.lower("evidence_label").contains("event"),
        "Anomaly"
    ).when(
        F.lower("evidence_label").contains("trend"),
        "Trend"
    ).when(
        F.lower("evidence_label").contains("class") |
        F.lower("evidence_label").contains("severity"),
        "Classification"
    ).when(
        F.lower("evidence_label").contains("revenue"),
        "Sales"
    ).otherwise(
        "General"
    )
).withColumn(
    "evidence_unit",
    F.when(
        F.col("evidence_value_text").contains("%"),
        "percent"
    ).when(
        F.lower("evidence_value_text").contains("per 1,000 orders"),
        "events_per_1000_orders"
    ).when(
        F.lower("evidence_value_text").contains("days"),
        "days"
    ).when(
        F.lower("evidence_value_text").contains("units"),
        "units"
    ).when(
        F.lower("evidence_label").contains("orders"),
        "orders"
    ).when(
        F.lower("evidence_label").contains("event"),
        "events"
    ).when(
        F.lower("evidence_label").contains("rating"),
        "rating"
    ).when(
        F.lower("evidence_label").contains("score"),
        "score"
    ).otherwise(
        F.lit(None).cast("string")
    )
).withColumn(
    "evidence_available",
    ~(
        F.col("evidence_value_text").isNull() |
        (
            F.trim(
                F.col("evidence_value_text")
            ) == ""
        ) |
        (
            F.upper(
                F.trim(
                    F.col("evidence_value_text")
                )
            ) == "N/A"
        )
    )
).withColumn(
    "evidence_statement",
    F.concat(
        F.col("evidence_label"),
        F.lit(": "),
        F.col("evidence_value_text")
    )
)


evidence_counts = evidence.groupBy(
    "recommendation_id"
).agg(
    F.count("*").alias("evidence_count"),
    F.sum(
        F.col("evidence_available").cast("int")
    ).alias("available_evidence_count")
)


recommendation_evidence = evidence.join(
    evidence_counts,
    "recommendation_id",
    "inner"
).withColumn(
    "evidence_complete",
    (
        F.col("evidence_count") >= 2
    ) &
    (
        F.col("available_evidence_count") >= 2
    )
).select(
    "recommendation_id",
    "recommendation_type",
    "source_analysis",
    "recommendation_scope",
    "entity_key",
    "restaurant_id",
    "restaurant_name",
    "item_id",
    "item_name",
    "promotion_id",
    "customer_segment",
    "recommended_action",
    "evidence_order",
    "evidence_category",
    "evidence_label",
    "evidence_value_text",
    "evidence_numeric_value",
    "evidence_unit",
    "evidence_available",
    "evidence_statement",
    "evidence_count",
    "available_evidence_count",
    "evidence_complete",
    "impact_value",
    "impact_unit"
)


type_window = Window.partitionBy()


evidence_coverage_summary = recommendation_evidence.groupBy(
    "recommendation_type",
    "source_analysis"
).agg(
    F.countDistinct(
        "recommendation_id"
    ).alias("recommendation_count"),
    F.count(
        "*"
    ).alias("evidence_point_count"),
    F.sum(
        F.col("evidence_available").cast("int")
    ).alias("available_evidence_points"),
    F.sum(
        F.when(
            ~F.col("evidence_available"),
            1
        ).otherwise(0)
    ).alias("unavailable_evidence_points"),
    F.round(
        F.avg("evidence_count"),
        2
    ).alias("average_evidence_points_per_recommendation"),
    F.sum(
        F.when(
            F.col("evidence_complete"),
            1
        ).otherwise(0)
    ).alias("complete_evidence_rows")
).withColumn(
    "evidence_availability_pct",
    F.when(
        F.col("evidence_point_count") > 0,
        F.round(
            (
                F.col("available_evidence_points") /
                F.col("evidence_point_count")
            ) * 100,
            2
        )
    ).otherwise(
        F.lit(0.0)
    )
).withColumn(
    "recommendation_share_pct",
    F.round(
        (
            F.col("recommendation_count") /
            F.sum("recommendation_count").over(
                type_window
            )
        ) * 100,
        2
    )
)


print("\n========RECOMMENDATION EVIDENCE COVERAGE========")
evidence_coverage_summary.orderBy(
    F.desc("recommendation_count"),
    F.asc("recommendation_type")
).show(
    truncate=False
)


print("\n========SAMPLE RECOMMENDATION EVIDENCE========")
recommendation_evidence.orderBy(
    "recommendation_type",
    "recommendation_id",
    "evidence_order"
).show(
    80,
    truncate=False
)


recommendation_evidence.write.mode(
    "overwrite"
).parquet(
    f"{output_folder}/recommendation_evidence"
)


evidence_coverage_summary.write.mode(
    "overwrite"
).parquet(
    f"{output_folder}/evidence_coverage_summary"
)


print("\nRecommendation evidence generation completed successfully.")


spark.stop()
