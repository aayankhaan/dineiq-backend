from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.settings import ANALYTICS_DATA_FOLDER, ML_DATA_FOLDER


spark = SparkSession.builder \
    .appName("DineIQ Recommendation Engine") \
    .getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "8")


location_menu = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/location_menu_performance/location_menu_performance"
)

slow_moving = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/slow_moving_dishes/slow_moving_dishes"
)

price_sensitivity = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/price_sensitivity/item_price_sensitivity"
)

bundle_recommendations = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/bundle_recommendations/recommendations"
)

time_period_forecast = spark.read.parquet(
    f"{ML_DATA_FOLDER}/forecasting/time_period_forecast"
)

churn_risk = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/churn_risk/customer_churn_risk"
)

promotion_effectiveness = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/promotion_effectiveness/promotion_effectiveness"
)

sales_anomalies = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/sales_anomalies/sales_anomaly_events"
)

location_comparison = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/multi_location_intelligence/location_comparison"
)


output_folder = f"{ANALYTICS_DATA_FOLDER}/recommendations"


def standardize(frame):
    return frame.select(
        F.col("recommendation_type").cast("string"),
        F.col("source_analysis").cast("string"),
        F.col("recommendation_scope").cast("string"),
        F.col("entity_key").cast("string"),
        F.col("restaurant_id").cast("int"),
        F.col("restaurant_name").cast("string"),
        F.col("item_id").cast("int"),
        F.col("item_name").cast("string"),
        F.col("promotion_id").cast("int"),
        F.col("customer_segment").cast("string"),
        F.col("recommended_action").cast("string"),
        F.col("evidence_summary").cast("string"),
        F.col("impact_value").cast("double"),
        F.col("impact_unit").cast("string")
    )


hidden_window = Window.orderBy(
    F.desc("contribution_margin"),
    F.desc("profit_percentage"),
    F.asc("restaurant_id"),
    F.asc("item_id")
)


hidden_opportunities = location_menu.filter(
    (
        F.col("performance_class") ==
        "Hidden Opportunity"
    ) &
    (
        F.col("contribution_margin") > 0
    ) &
    (
        F.col("contribution_margin") >=
        F.col("location_margin_median")
    )
).withColumn(
    "selection_rank",
    F.row_number().over(hidden_window)
).filter(
    F.col("selection_rank") <= 100
).select(
    F.lit("Promote Hidden Opportunity").alias("recommendation_type"),
    F.lit("Location-Specific Menu Performance").alias("source_analysis"),
    F.lit("Location Menu Item").alias("recommendation_scope"),
    F.concat_ws(
        ":",
        F.col("restaurant_id").cast("string"),
        F.col("item_id").cast("string")
    ).alias("entity_key"),
    "restaurant_id",
    "restaurant_name",
    "item_id",
    "item_name",
    F.lit(None).cast("int").alias("promotion_id"),
    F.lit(None).cast("string").alias("customer_segment"),
    F.concat(
        F.lit("Promote "),
        F.col("item_name"),
        F.lit(" at "),
        F.col("restaurant_name"),
        F.lit(" to increase demand for a high-margin Hidden Opportunity.")
    ).alias("recommended_action"),
    F.concat(
        F.lit("Class: Hidden Opportunity | Contribution margin: "),
        F.round("contribution_margin", 2).cast("string"),
        F.lit(" | Profit percentage: "),
        F.round("profit_percentage", 2).cast("string"),
        F.lit("% | Rating: "),
        F.coalesce(
            F.round("average_rating", 2).cast("string"),
            F.lit("N/A")
        ),
        F.lit(" | Wastage: "),
        F.round("wastage_percentage", 2).cast("string"),
        F.lit("%")
    ).alias("evidence_summary"),
    F.col("contribution_margin").cast("double").alias("impact_value"),
    F.lit("contribution_margin").alias("impact_unit")
)


wastage_window = Window.orderBy(
    F.desc("wastage_percentage"),
    F.desc("item_revenue"),
    F.asc("restaurant_id"),
    F.asc("item_id")
)


high_wastage = location_menu.filter(
    (
        F.col("wastage_percentage") >
        F.col("location_wastage_upper")
    ) &
    (
        F.col("wastage_percentage") > 0
    )
).withColumn(
    "selection_rank",
    F.row_number().over(wastage_window)
).filter(
    F.col("selection_rank") <= 100
).select(
    F.lit("Reduce Preparation Quantity").alias("recommendation_type"),
    F.lit("Location-Specific Menu Performance").alias("source_analysis"),
    F.lit("Location Menu Item").alias("recommendation_scope"),
    F.concat_ws(
        ":",
        F.col("restaurant_id").cast("string"),
        F.col("item_id").cast("string")
    ).alias("entity_key"),
    "restaurant_id",
    "restaurant_name",
    "item_id",
    "item_name",
    F.lit(None).cast("int").alias("promotion_id"),
    F.lit(None).cast("string").alias("customer_segment"),
    F.concat(
        F.lit("Reduce preparation quantity for "),
        F.col("item_name"),
        F.lit(" at "),
        F.col("restaurant_name"),
        F.lit(" and monitor wastage before increasing production again.")
    ).alias("recommended_action"),
    F.concat(
        F.lit("Wastage rate: "),
        F.round("wastage_percentage", 2).cast("string"),
        F.lit("% | Local high-wastage threshold: "),
        F.round("location_wastage_upper", 2).cast("string"),
        F.lit("% | Item revenue: "),
        F.round("item_revenue", 2).cast("string")
    ).alias("evidence_summary"),
    F.col("wastage_percentage").cast("double").alias("impact_value"),
    F.lit("wastage_percentage").alias("impact_unit")
)


price_window = Window.orderBy(
    F.desc("price_sensitivity_score"),
    F.desc("evaluable_price_change_events"),
    F.asc("item_id")
)


pricing_reviews = price_sensitivity.filter(
    F.col("price_sensitivity_class") ==
    "Highly Price Sensitive"
).withColumn(
    "selection_rank",
    F.row_number().over(price_window)
).filter(
    F.col("selection_rank") <= 50
).select(
    F.lit("Review Price Strategy").alias("recommendation_type"),
    F.lit("Price Sensitivity").alias("source_analysis"),
    F.lit("Menu Item").alias("recommendation_scope"),
    F.col("item_id").cast("string").alias("entity_key"),
    F.lit(None).cast("int").alias("restaurant_id"),
    F.lit(None).cast("string").alias("restaurant_name"),
    "item_id",
    "item_name",
    F.lit(None).cast("int").alias("promotion_id"),
    F.lit(None).cast("string").alias("customer_segment"),
    F.concat(
        F.lit("Review pricing for "),
        F.col("item_name"),
        F.lit(" because demand has shown high historical price sensitivity.")
    ).alias("recommended_action"),
    F.concat(
        F.lit("Sensitivity class: "),
        F.col("price_sensitivity_class"),
        F.lit(" | Sensitivity score: "),
        F.round("price_sensitivity_score", 4).cast("string"),
        F.lit(" | Evaluable price changes: "),
        F.col("evaluable_price_change_events").cast("string"),
        F.lit(" | Expected-direction rate: "),
        F.round("expected_direction_rate_pct", 2).cast("string"),
        F.lit("%")
    ).alias("evidence_summary"),
    F.col("price_sensitivity_score").cast("double").alias("impact_value"),
    F.lit("price_sensitivity_score").alias("impact_unit")
)


bundle_window = Window.orderBy(
    F.desc("lift"),
    F.desc("confidence"),
    F.desc("pair_order_count"),
    F.asc("antecedent_item_id"),
    F.asc("consequent_item_id")
)


bundles = bundle_recommendations.filter(
    (
        F.col("antecedent_item_id") <
        F.col("consequent_item_id")
    ) &
    (
        F.col("lift") > 1.10
    )
).withColumn(
    "selection_rank",
    F.row_number().over(bundle_window)
).filter(
    F.col("selection_rank") <= 50
).select(
    F.lit("Bundle Frequently Purchased Items").alias("recommendation_type"),
    F.lit("Market Basket and Bundle Analysis").alias("source_analysis"),
    F.lit("Menu Item Pair").alias("recommendation_scope"),
    F.concat_ws(
        ":",
        F.col("antecedent_item_id").cast("string"),
        F.col("consequent_item_id").cast("string")
    ).alias("entity_key"),
    F.lit(None).cast("int").alias("restaurant_id"),
    F.lit(None).cast("string").alias("restaurant_name"),
    F.col("antecedent_item_id").cast("int").alias("item_id"),
    F.col("antecedent_item_name").alias("item_name"),
    F.lit(None).cast("int").alias("promotion_id"),
    F.lit(None).cast("string").alias("customer_segment"),
    F.col("recommended_action"),
    F.concat(
        F.lit("Pair orders: "),
        F.col("pair_order_count").cast("string"),
        F.lit(" | Support: "),
        F.round(F.col("support") * 100, 2).cast("string"),
        F.lit("% | Confidence: "),
        F.round(F.col("confidence") * 100, 2).cast("string"),
        F.lit("% | Lift: "),
        F.round("lift", 2).cast("string")
    ).alias("evidence_summary"),
    F.col("lift").cast("double").alias("impact_value"),
    F.lit("association_lift").alias("impact_unit")
)


persistent_low = location_menu.alias("m").join(
    slow_moving.alias("s"),
    (
        F.col("m.restaurant_id") ==
        F.col("s.restaurant_id")
    ) &
    (
        F.col("m.item_id") ==
        F.col("s.item_id")
    ),
    "inner"
).filter(
    (
        F.col("m.performance_class") ==
        "Low Performer"
    ) &
    F.col("s.slow_moving_dish")
)


low_window = Window.orderBy(
    F.desc("s.slow_signal_count"),
    F.asc("m.contribution_margin"),
    F.desc("m.item_revenue"),
    F.asc("m.restaurant_id"),
    F.asc("m.item_id")
)


low_performers = persistent_low.withColumn(
    "selection_rank",
    F.row_number().over(low_window)
).filter(
    F.col("selection_rank") <= 100
).select(
    F.lit("Review or Redesign Low Performer").alias("recommendation_type"),
    F.lit("Location Menu Performance and Slow-Moving Detection").alias("source_analysis"),
    F.lit("Location Menu Item").alias("recommendation_scope"),
    F.concat_ws(
        ":",
        F.col("m.restaurant_id").cast("string"),
        F.col("m.item_id").cast("string")
    ).alias("entity_key"),
    F.col("m.restaurant_id").alias("restaurant_id"),
    F.col("m.restaurant_name").alias("restaurant_name"),
    F.col("m.item_id").alias("item_id"),
    F.col("m.item_name").alias("item_name"),
    F.lit(None).cast("int").alias("promotion_id"),
    F.lit(None).cast("string").alias("customer_segment"),
    F.concat(
        F.lit("Review or redesign "),
        F.col("m.item_name"),
        F.lit(" at "),
        F.col("m.restaurant_name"),
        F.lit("; consider removal only if performance remains weak after corrective action.")
    ).alias("recommended_action"),
    F.concat(
        F.lit("Class: Low Performer | Slow-moving severity: "),
        F.col("s.slow_moving_severity"),
        F.lit(" | Slow signals: "),
        F.col("s.slow_signal_count").cast("string"),
        F.lit(" | Contribution margin: "),
        F.round(F.col("m.contribution_margin"), 2).cast("string"),
        F.lit(" | Sales trend: "),
        F.coalesce(
            F.round(F.col("s.sales_trend_pct"), 2).cast("string"),
            F.lit("N/A")
        ),
        F.lit("%")
    ).alias("evidence_summary"),
    F.col("m.item_revenue").cast("double").alias("impact_value"),
    F.lit("item_revenue").alias("impact_unit")
)


forecast_periods = time_period_forecast.groupBy(
    "order_hour",
    "time_period"
).agg(
    F.min("forecast_date").alias("forecast_start_date"),
    F.max("forecast_date").alias("forecast_end_date"),
    F.round(
        F.sum("predicted_demand"),
        2
    ).alias("forecast_demand")
)


forecast_window = Window.orderBy(
    F.desc("forecast_demand"),
    F.asc("order_hour")
)


peak_inventory = forecast_periods.withColumn(
    "selection_rank",
    F.row_number().over(forecast_window)
).filter(
    F.col("selection_rank") <= 3
).select(
    F.lit("Increase Stock Before Predicted Peak").alias("recommendation_type"),
    F.lit("Demand Forecasting").alias("source_analysis"),
    F.lit("Network Time Period").alias("recommendation_scope"),
    F.concat_ws(
        ":",
        F.col("order_hour").cast("string"),
        F.col("time_period")
    ).alias("entity_key"),
    F.lit(None).cast("int").alias("restaurant_id"),
    F.lit(None).cast("string").alias("restaurant_name"),
    F.lit(None).cast("int").alias("item_id"),
    F.lit(None).cast("string").alias("item_name"),
    F.lit(None).cast("int").alias("promotion_id"),
    F.lit(None).cast("string").alias("customer_segment"),
    F.concat(
        F.lit("Increase stock and operational readiness before the predicted "),
        F.col("time_period"),
        F.lit(" peak around "),
        F.col("order_hour").cast("string"),
        F.lit(":00 during the forecast window.")
    ).alias("recommended_action"),
    F.concat(
        F.lit("Forecast demand: "),
        F.round("forecast_demand", 2).cast("string"),
        F.lit(" units | Forecast window: "),
        F.col("forecast_start_date").cast("string"),
        F.lit(" to "),
        F.col("forecast_end_date").cast("string")
    ).alias("evidence_summary"),
    F.col("forecast_demand").cast("double").alias("impact_value"),
    F.lit("forecast_demand_units").alias("impact_unit")
)


segment_targets = churn_risk.filter(
    F.col("churn_risk_flag")
).withColumn(
    "segment_name",
    F.coalesce(
        F.col("customer_segment"),
        F.lit("Unsegmented Customers")
    )
).groupBy(
    "segment_name"
).agg(
    F.count("*").alias("at_risk_customers"),
    F.sum(
        F.when(
            F.col("churn_risk_level") ==
            "Critical",
            1
        ).otherwise(0)
    ).alias("critical_customers"),
    F.sum(
        F.when(
            F.col("churn_risk_level") ==
            "High",
            1
        ).otherwise(0)
    ).alias("high_customers"),
    F.round(
        F.avg("days_since_last_order"),
        2
    ).alias("average_recency_days"),
    F.round(
        F.sum("lifetime_monetary_value"),
        2
    ).alias("historical_customer_value")
).select(
    F.lit("Target At-Risk Customer Segment").alias("recommendation_type"),
    F.lit("Customer Churn Risk").alias("source_analysis"),
    F.lit("Customer Segment").alias("recommendation_scope"),
    F.col("segment_name").alias("entity_key"),
    F.lit(None).cast("int").alias("restaurant_id"),
    F.lit(None).cast("string").alias("restaurant_name"),
    F.lit(None).cast("int").alias("item_id"),
    F.lit(None).cast("string").alias("item_name"),
    F.lit(None).cast("int").alias("promotion_id"),
    F.col("segment_name").alias("customer_segment"),
    F.concat(
        F.lit("Target "),
        F.col("segment_name"),
        F.lit(" with a focused retention and re-engagement campaign.")
    ).alias("recommended_action"),
    F.concat(
        F.lit("High/Critical churn-risk customers: "),
        F.col("at_risk_customers").cast("string"),
        F.lit(" | Critical: "),
        F.col("critical_customers").cast("string"),
        F.lit(" | High: "),
        F.col("high_customers").cast("string"),
        F.lit(" | Average recency: "),
        F.col("average_recency_days").cast("string"),
        F.lit(" days")
    ).alias("evidence_summary"),
    F.col("historical_customer_value").cast("double").alias("impact_value"),
    F.lit("historical_customer_value").alias("impact_unit")
)


promotion_window = Window.orderBy(
    F.asc("positive_kpi_count"),
    F.asc_nulls_first("margin_lift_pct"),
    F.desc("promotion_revenue"),
    F.asc("promotion_id")
)


ineffective_promotions = promotion_effectiveness.filter(
    F.col("promotion_assessment") ==
    "Ineffective"
).withColumn(
    "selection_rank",
    F.row_number().over(promotion_window)
).filter(
    F.col("selection_rank") <= 50
).select(
    F.lit("Review Ineffective Promotion").alias("recommendation_type"),
    F.lit("Promotion Effectiveness").alias("source_analysis"),
    F.lit("Promotion").alias("recommendation_scope"),
    F.col("promotion_id").cast("string").alias("entity_key"),
    "restaurant_id",
    "restaurant_name",
    "item_id",
    "item_name",
    "promotion_id",
    F.lit(None).cast("string").alias("customer_segment"),
    F.concat(
        F.lit("Review or redesign promotion "),
        F.col("promotion_name"),
        F.lit(" before running a similar campaign again.")
    ).alias("recommended_action"),
    F.concat(
        F.lit("Assessment: Ineffective | Positive KPIs: "),
        F.col("positive_kpi_count").cast("string"),
        F.lit("/8 | Margin lift: "),
        F.coalesce(
            F.round("margin_lift_pct", 2).cast("string"),
            F.lit("N/A")
        ),
        F.lit("% | Wastage change: "),
        F.coalesce(
            F.round("wastage_change_pct", 2).cast("string"),
            F.lit("N/A")
        ),
        F.lit("%")
    ).alias("evidence_summary"),
    F.coalesce(
        F.col("promotion_revenue").cast("double"),
        F.lit(0.0)
    ).alias("impact_value"),
    F.lit("promotion_revenue").alias("impact_unit")
)


location_anomaly_counts = sales_anomalies.filter(
    F.col("severity").isin(
        "High",
        "Critical"
    )
).groupBy(
    "restaurant_id",
    "restaurant_name"
).agg(
    F.count("*").alias("high_critical_anomaly_events"),
    F.sum(
        F.when(
            F.col("severity") ==
            "Critical",
            1
        ).otherwise(0)
    ).alias("critical_anomaly_events"),
    F.sum(
        F.when(
            F.col("severity") ==
            "High",
            1
        ).otherwise(0)
    ).alias("high_anomaly_events")
).join(
    location_comparison.select(
        "restaurant_id",
        "total_orders"
    ),
    "restaurant_id",
    "inner"
).withColumn(
    "anomaly_events_per_1000_orders",
    F.round(
        (
            F.col("high_critical_anomaly_events") /
            F.col("total_orders")
        ) * 1000,
        4
    )
)


anomaly_quantile = location_anomaly_counts.approxQuantile(
    "anomaly_events_per_1000_orders",
    [0.75],
    0.01
)


anomaly_rate_threshold = (
    float(anomaly_quantile[0])
    if anomaly_quantile
    else 0.0
)


anomaly_window = Window.orderBy(
    F.desc("anomaly_events_per_1000_orders"),
    F.desc("critical_anomaly_events"),
    F.asc("restaurant_id")
)


anomalous_locations = location_anomaly_counts.filter(
    F.col("anomaly_events_per_1000_orders") >=
    F.lit(anomaly_rate_threshold)
).withColumn(
    "selection_rank",
    F.row_number().over(anomaly_window)
).filter(
    F.col("selection_rank") <= 5
).select(
    F.lit("Investigate Anomalous Location").alias("recommendation_type"),
    F.lit("Sales Anomaly Detection").alias("source_analysis"),
    F.lit("Restaurant Location").alias("recommendation_scope"),
    F.col("restaurant_id").cast("string").alias("entity_key"),
    "restaurant_id",
    "restaurant_name",
    F.lit(None).cast("int").alias("item_id"),
    F.lit(None).cast("string").alias("item_name"),
    F.lit(None).cast("int").alias("promotion_id"),
    F.lit(None).cast("string").alias("customer_segment"),
    F.concat(
        F.lit("Investigate unusual sales behavior at "),
        F.col("restaurant_name"),
        F.lit(" and review the underlying anomaly events.")
    ).alias("recommended_action"),
    F.concat(
        F.lit("High/Critical anomaly events: "),
        F.col("high_critical_anomaly_events").cast("string"),
        F.lit(" | Critical events: "),
        F.col("critical_anomaly_events").cast("string"),
        F.lit(" | Anomaly rate: "),
        F.col("anomaly_events_per_1000_orders").cast("string"),
        F.lit(" per 1,000 orders | Investigation threshold: "),
        F.lit(round(anomaly_rate_threshold, 4)).cast("string")
    ).alias("evidence_summary"),
    F.col("anomaly_events_per_1000_orders").cast("double").alias("impact_value"),
    F.lit("anomaly_events_per_1000_orders").alias("impact_unit")
)


recommendations = standardize(
    hidden_opportunities
).unionByName(
    standardize(high_wastage)
).unionByName(
    standardize(pricing_reviews)
).unionByName(
    standardize(bundles)
).unionByName(
    standardize(low_performers)
).unionByName(
    standardize(peak_inventory)
).unionByName(
    standardize(segment_targets)
).unionByName(
    standardize(ineffective_promotions)
).unionByName(
    standardize(anomalous_locations)
).withColumn(
    "recommendation_id",
    F.concat(
        F.lit("REC-"),
        F.substring(
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("recommendation_type"),
                    F.col("entity_key")
                ),
                256
            ),
            1,
            16
        )
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
    "evidence_summary",
    "impact_value",
    "impact_unit"
)


recommendation_summary = recommendations.groupBy(
    "recommendation_type",
    "source_analysis"
).agg(
    F.count("*").alias("recommendation_count"),
    F.round(
        F.avg("impact_value"),
        2
    ).alias("average_impact_value"),
    F.round(
        F.max("impact_value"),
        2
    ).alias("maximum_impact_value")
).orderBy(
    F.desc("recommendation_count"),
    F.asc("recommendation_type")
)


print("\n========RECOMMENDATION ENGINE SUMMARY========")
recommendation_summary.show(
    truncate=False
)


print("\n========SAMPLE RECOMMENDATIONS========")
recommendations.orderBy(
    "recommendation_type",
    F.desc("impact_value"),
    "recommendation_id"
).show(
    60,
    truncate=False
)


recommendations.write.mode(
    "overwrite"
).parquet(
    f"{output_folder}/recommendations"
)


recommendation_summary.write.mode(
    "overwrite"
).parquet(
    f"{output_folder}/recommendation_summary"
)


print("\nRecommendation engine completed successfully.")


spark.stop()
