from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.settings import ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("DineIQ Recommendation Priority") \
    .getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "8")


recommendations = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/recommendations/recommendations"
)

evidence = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/recommendation_evidence/recommendation_evidence"
)

output_folder = f"{ANALYTICS_DATA_FOLDER}/recommendation_priority"


evidence_status = evidence.groupBy(
    "recommendation_id"
).agg(
    F.max(
        F.col("evidence_count")
    ).alias("evidence_count"),
    F.max(
        F.col("available_evidence_count")
    ).alias("available_evidence_count"),
    F.min(
        F.col("evidence_complete").cast("int")
    ).cast("boolean").alias("evidence_complete")
)


type_window = Window.partitionBy(
    "recommendation_type"
)

rank_window = Window.partitionBy(
    "recommendation_type"
).orderBy(
    F.desc("impact_value")
)


priority = recommendations.join(
    evidence_status,
    "recommendation_id",
    "inner"
).withColumn(
    "recommendation_type_count",
    F.count("*").over(
        type_window
    )
).withColumn(
    "priority_rank_within_type",
    F.rank().over(
        rank_window
    )
).withColumn(
    "impact_percentile",
    F.when(
        F.col("recommendation_type_count") == 1,
        F.lit(1.0)
    ).otherwise(
        F.round(
            F.lit(1.0) -
            (
                (
                    F.col("priority_rank_within_type") -
                    F.lit(1)
                ) /
                (
                    F.col("recommendation_type_count") -
                    F.lit(1)
                )
            ),
            6
        )
    )
).withColumn(
    "priority_score",
    F.round(
        F.col("impact_percentile") * 100,
        2
    )
).withColumn(
    "priority",
    F.when(
        F.col("impact_percentile") >= 0.90,
        "Critical"
    ).when(
        F.col("impact_percentile") >= 0.65,
        "High"
    ).when(
        F.col("impact_percentile") >= 0.35,
        "Medium"
    ).otherwise(
        "Low"
    )
).withColumn(
    "impact_basis",
    F.when(
        F.col("impact_unit") ==
        "contribution_margin",
        "Contribution margin opportunity"
    ).when(
        F.col("impact_unit") ==
        "wastage_percentage",
        "Wastage reduction opportunity"
    ).when(
        F.col("impact_unit") ==
        "item_revenue",
        "Revenue exposure of persistent low performance"
    ).when(
        F.col("impact_unit") ==
        "association_lift",
        "Strength of purchase association"
    ).when(
        F.col("impact_unit") ==
        "price_sensitivity_score",
        "Historical price sensitivity"
    ).when(
        F.col("impact_unit") ==
        "promotion_revenue",
        "Revenue exposed to ineffective promotion"
    ).when(
        F.col("impact_unit") ==
        "anomaly_events_per_1000_orders",
        "Concentration of high-severity anomaly events"
    ).when(
        F.col("impact_unit") ==
        "historical_customer_value",
        "Historical value of customers at churn risk"
    ).when(
        F.col("impact_unit") ==
        "forecast_demand_units",
        "Forecast demand requiring operational readiness"
    ).otherwise(
        "Recommendation-specific business impact"
    )
).withColumn(
    "priority_method",
    F.lit(
        "Relative business impact within the same recommendation type"
    )
).withColumn(
    "priority_reason",
    F.concat(
        F.lit("Priority is based on "),
        F.col("impact_basis"),
        F.lit(". Impact value "),
        F.round(
            F.col("impact_value"),
            2
        ).cast("string"),
        F.lit(" ranks "),
        F.col("priority_rank_within_type").cast("string"),
        F.lit(" of "),
        F.col("recommendation_type_count").cast("string"),
        F.lit(" within "),
        F.col("recommendation_type"),
        F.lit(" recommendations, giving an impact percentile of "),
        F.round(
            F.col("priority_score"),
            2
        ).cast("string"),
        F.lit("%.")
    )
)


prioritized_recommendations = priority.select(
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
    "evidence_summary",
    "evidence_count",
    "available_evidence_count",
    "evidence_complete",
    "impact_value",
    "impact_unit",
    "impact_basis",
    "recommendation_type_count",
    "priority_rank_within_type",
    "impact_percentile",
    "priority_score",
    "priority",
    "priority_method",
    "priority_reason"
)


priority_order = F.when(
    F.col("priority") == "Critical",
    1
).when(
    F.col("priority") == "High",
    2
).when(
    F.col("priority") == "Medium",
    3
).otherwise(
    4
)


priority_summary = prioritized_recommendations.groupBy(
    "priority"
).agg(
    F.count("*").alias("recommendation_count"),
    F.round(
        F.avg("priority_score"),
        2
    ).alias("average_priority_score"),
    F.round(
        F.avg("impact_percentile") * 100,
        2
    ).alias("average_impact_percentile_pct"),
    F.countDistinct(
        "recommendation_type"
    ).alias("recommendation_types_represented")
).withColumn(
    "recommendation_percentage",
    F.round(
        (
            F.col("recommendation_count") /
            F.sum("recommendation_count").over(
                Window.partitionBy()
            )
        ) * 100,
        2
    )
).orderBy(
    priority_order
)


print("\n========RECOMMENDATION PRIORITY SUMMARY========")
priority_summary.show(
    truncate=False
)


print("\n========TOP PRIORITIZED RECOMMENDATIONS========")
prioritized_recommendations.orderBy(
    priority_order,
    F.desc("priority_score"),
    F.desc("impact_value"),
    F.asc("recommendation_id")
).show(
    60,
    truncate=False
)


prioritized_recommendations.write.mode(
    "overwrite"
).parquet(
    f"{output_folder}/prioritized_recommendations"
)


priority_summary.write.mode(
    "overwrite"
).parquet(
    f"{output_folder}/priority_summary"
)


print("\nRecommendation priority assignment completed successfully.")


spark.stop()
