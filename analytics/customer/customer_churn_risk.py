from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.settings import INTEGRATED_DATA_FOLDER, ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("DineIQ Customer Churn Risk Identification") \
    .getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "8")


transactions = spark.read.parquet(
    f"{INTEGRATED_DATA_FOLDER}/transactions"
)

customer_segments = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/customer_segmentation/customer_segments"
)

output_folder = f"{ANALYTICS_DATA_FOLDER}/churn_risk"


sales = transactions.filter(
    F.col("order_status") == "Completed"
).withColumn(
    "order_date",
    F.to_date("order_datetime")
)


order_level = sales.select(
    "customer_id",
    "order_id",
    "order_datetime",
    "order_date",
    "total_amount"
).dropDuplicates(
    ["order_id"]
)


dataset_end = order_level.agg(
    F.max("order_date").alias("dataset_end")
).first()["dataset_end"]


previous_start = F.date_sub(
    F.lit(dataset_end),
    360
)

recent_start = F.date_sub(
    F.lit(dataset_end),
    180
)


lifetime = order_level.groupBy(
    "customer_id"
).agg(
    F.min(
        "order_date"
    ).alias("first_order_date"),
    F.max(
        "order_date"
    ).alias("last_order_date"),
    F.countDistinct(
        "order_id"
    ).alias("lifetime_orders"),
    F.round(
        F.sum(
            F.col("total_amount").cast("double")
        ),
        2
    ).alias("lifetime_monetary_value")
).withColumn(
    "days_since_last_order",
    F.datediff(
        F.lit(dataset_end),
        F.col("last_order_date")
    )
)


order_dates = order_level.select(
    "customer_id",
    "order_date"
).distinct()


gap_window = Window.partitionBy(
    "customer_id"
).orderBy(
    "order_date"
)


customer_gaps = order_dates.withColumn(
    "previous_order_date",
    F.lag(
        "order_date"
    ).over(
        gap_window
    )
).withColumn(
    "gap_days",
    F.datediff(
        F.col("order_date"),
        F.col("previous_order_date")
    )
)


average_gaps = customer_gaps.groupBy(
    "customer_id"
).agg(
    F.round(
        F.avg(
            F.col("gap_days").cast("double")
        ),
        2
    ).alias("average_historical_gap_days")
)


period_orders = order_level.groupBy(
    "customer_id"
).agg(
    F.countDistinct(
        F.when(
            (
                F.col("order_date") >
                previous_start
            ) &
            (
                F.col("order_date") <=
                recent_start
            ),
            F.col("order_id")
        )
    ).alias("previous_frequency"),
    F.countDistinct(
        F.when(
            F.col("order_date") >
            recent_start,
            F.col("order_id")
        )
    ).alias("recent_frequency"),
    F.round(
        F.sum(
            F.when(
                (
                    F.col("order_date") >
                    previous_start
                ) &
                (
                    F.col("order_date") <=
                    recent_start
                ),
                F.col("total_amount").cast("double")
            ).otherwise(
                0.0
            )
        ),
        2
    ).alias("previous_monetary_value"),
    F.round(
        F.sum(
            F.when(
                F.col("order_date") >
                recent_start,
                F.col("total_amount").cast("double")
            ).otherwise(
                0.0
            )
        ),
        2
    ).alias("recent_monetary_value"),
    F.countDistinct(
        F.when(
            (
                F.col("order_date") >
                previous_start
            ) &
            (
                F.col("order_date") <=
                recent_start
            ),
            F.col("order_date")
        )
    ).alias("previous_visit_days"),
    F.countDistinct(
        F.when(
            F.col("order_date") >
            recent_start,
            F.col("order_date")
        )
    ).alias("recent_visit_days")
)


period_categories = sales.groupBy(
    "customer_id"
).agg(
    F.countDistinct(
        F.when(
            (
                F.col("order_date") >
                previous_start
            ) &
            (
                F.col("order_date") <=
                recent_start
            ),
            F.col("category_id")
        )
    ).alias("previous_category_diversity"),
    F.countDistinct(
        F.when(
            F.col("order_date") >
            recent_start,
            F.col("category_id")
        )
    ).alias("recent_category_diversity"),
    F.countDistinct(
        "category_id"
    ).alias("lifetime_category_diversity")
)


profile = lifetime.join(
    average_gaps,
    "customer_id",
    "left"
).join(
    period_orders,
    "customer_id",
    "left"
).join(
    period_categories,
    "customer_id",
    "left"
).join(
    customer_segments.select(
        "customer_id",
        "customer_segment",
        "favorite_category",
        "channel_preference",
        "promotion_sensitivity"
    ),
    "customer_id",
    "left"
).fillna(
    {
        "previous_frequency": 0,
        "recent_frequency": 0,
        "previous_monetary_value": 0.0,
        "recent_monetary_value": 0.0,
        "previous_visit_days": 0,
        "recent_visit_days": 0,
        "previous_category_diversity": 0,
        "recent_category_diversity": 0,
        "lifetime_category_diversity": 0
    }
)


profile = profile.withColumn(
    "frequency_change_pct",
    F.when(
        F.col("previous_frequency") > 0,
        F.round(
            (
                (
                    F.col("recent_frequency") -
                    F.col("previous_frequency")
                ) /
                F.col("previous_frequency")
            ) * 100,
            2
        )
    )
).withColumn(
    "monetary_change_pct",
    F.when(
        F.col("previous_monetary_value") > 0,
        F.round(
            (
                (
                    F.col("recent_monetary_value") -
                    F.col("previous_monetary_value")
                ) /
                F.col("previous_monetary_value")
            ) * 100,
            2
        )
    )
).withColumn(
    "category_diversity_change_pct",
    F.when(
        F.col("previous_category_diversity") > 0,
        F.round(
            (
                (
                    F.col("recent_category_diversity") -
                    F.col("previous_category_diversity")
                ) /
                F.col("previous_category_diversity")
            ) * 100,
            2
        )
    )
).withColumn(
    "visit_frequency_change_pct",
    F.when(
        F.col("previous_visit_days") > 0,
        F.round(
            (
                (
                    F.col("recent_visit_days") -
                    F.col("previous_visit_days")
                ) /
                F.col("previous_visit_days")
            ) * 100,
            2
        )
    )
).withColumn(
    "recency_to_average_gap_ratio",
    F.when(
        F.col("average_historical_gap_days") > 0,
        F.round(
            F.col("days_since_last_order") /
            F.col("average_historical_gap_days"),
            2
        )
    )
)


eligible = profile.filter(
    F.col("lifetime_orders") >= 2
)


def lower_quartile(column):
    values = eligible.filter(
        F.col(column).isNotNull()
    ).approxQuantile(
        column,
        [0.25],
        0.01
    )

    if values:
        return float(values[0])

    return 0.0


recency_values = eligible.approxQuantile(
    "days_since_last_order",
    [0.75],
    0.01
)

recency_threshold = (
    float(recency_values[0])
    if recency_values
    else 0.0
)

frequency_decline_threshold = lower_quartile(
    "frequency_change_pct"
)

monetary_decline_threshold = lower_quartile(
    "monetary_change_pct"
)

category_decline_threshold = lower_quartile(
    "category_diversity_change_pct"
)

visit_decline_threshold = lower_quartile(
    "visit_frequency_change_pct"
)


risk = profile.withColumn(
    "risk_eligible",
    F.col("lifetime_orders") >= 2
).withColumn(
    "increasing_recency",
    F.col("risk_eligible") &
    (
        F.col("days_since_last_order") >=
        F.lit(recency_threshold)
    ) &
    F.coalesce(
        F.col("recency_to_average_gap_ratio") >=
        F.lit(1.5),
        F.lit(False)
    )
).withColumn(
    "declining_frequency",
    F.col("risk_eligible") &
    (
        F.col("previous_frequency") >= 2
    ) &
    F.col("frequency_change_pct").isNotNull() &
    (
        F.col("frequency_change_pct") < 0
    ) &
    (
        F.col("frequency_change_pct") <=
        F.lit(frequency_decline_threshold)
    )
).withColumn(
    "declining_monetary_value",
    F.col("risk_eligible") &
    (
        F.col("previous_frequency") >= 2
    ) &
    F.col("monetary_change_pct").isNotNull() &
    (
        F.col("monetary_change_pct") < 0
    ) &
    (
        F.col("monetary_change_pct") <=
        F.lit(monetary_decline_threshold)
    )
).withColumn(
    "reduced_category_diversity",
    F.col("risk_eligible") &
    (
        F.col("previous_category_diversity") >= 2
    ) &
    F.col("category_diversity_change_pct").isNotNull() &
    (
        F.col("category_diversity_change_pct") < 0
    ) &
    (
        F.col("category_diversity_change_pct") <=
        F.lit(category_decline_threshold)
    )
).withColumn(
    "lower_visit_frequency",
    F.col("risk_eligible") &
    (
        F.col("previous_visit_days") >= 2
    ) &
    F.col("visit_frequency_change_pct").isNotNull() &
    (
        F.col("visit_frequency_change_pct") < 0
    ) &
    (
        F.col("visit_frequency_change_pct") <=
        F.lit(visit_decline_threshold)
    )
).withColumn(
    "dormant_customer",
    F.col("risk_eligible") &
    (
        F.col("recent_frequency") == 0
    ) &
    (
        F.col("days_since_last_order") > 180
    )
)


signal_columns = [
    "increasing_recency",
    "declining_frequency",
    "declining_monetary_value",
    "reduced_category_diversity",
    "lower_visit_frequency"
]


risk = risk.withColumn(
    "risk_signal_count",
    sum(
        F.col(column).cast("int")
        for column in signal_columns
    )
).withColumn(
    "churn_risk_level",
    F.when(
        ~F.col("risk_eligible"),
        "Insufficient History"
    ).when(
        F.col("risk_signal_count") >= 4,
        "Critical"
    ).when(
        (
            F.col("risk_signal_count") == 3
        ) |
        F.col("dormant_customer"),
        "High"
    ).when(
        F.col("risk_signal_count") == 2,
        "Medium"
    ).otherwise(
        "Low"
    )
).withColumn(
    "churn_risk_flag",
    F.col("churn_risk_level").isin(
        "Critical",
        "High"
    )
).withColumn(
    "risk_reason",
    F.when(
        ~F.col("risk_eligible"),
        F.lit(
            "Insufficient repeat-purchase history for trend-based churn assessment."
        )
    ).when(
        (
            F.col("risk_signal_count") == 0
        ) &
        (
            ~F.col("dormant_customer")
        ),
        F.lit(
            "No material churn-risk signals detected."
        )
    ).otherwise(
        F.concat_ws(
            "; ",
            F.when(
                F.col("increasing_recency"),
                F.lit("Recency is unusually high versus the customer's historical visit gap")
            ),
            F.when(
                F.col("declining_frequency"),
                F.lit("Recent order frequency declined materially")
            ),
            F.when(
                F.col("declining_monetary_value"),
                F.lit("Recent monetary value declined materially")
            ),
            F.when(
                F.col("reduced_category_diversity"),
                F.lit("Recent category diversity declined materially")
            ),
            F.when(
                F.col("lower_visit_frequency"),
                F.lit("Recent active visit days declined materially")
            ),
            F.when(
                F.col("dormant_customer"),
                F.lit("No completed orders in the recent 180-day period")
            )
        )
    )
).withColumn(
    "recency_threshold_days",
    F.lit(recency_threshold)
).withColumn(
    "frequency_decline_threshold_pct",
    F.lit(frequency_decline_threshold)
).withColumn(
    "monetary_decline_threshold_pct",
    F.lit(monetary_decline_threshold)
).withColumn(
    "category_decline_threshold_pct",
    F.lit(category_decline_threshold)
).withColumn(
    "visit_decline_threshold_pct",
    F.lit(visit_decline_threshold)
)


risk_summary = risk.groupBy(
    "churn_risk_level"
).agg(
    F.count(
        "*"
    ).alias("customer_count"),
    F.round(
        F.avg("days_since_last_order"),
        2
    ).alias("average_recency_days"),
    F.round(
        F.avg("previous_frequency"),
        2
    ).alias("average_previous_frequency"),
    F.round(
        F.avg("recent_frequency"),
        2
    ).alias("average_recent_frequency"),
    F.round(
        F.avg("previous_monetary_value"),
        2
    ).alias("average_previous_monetary_value"),
    F.round(
        F.avg("recent_monetary_value"),
        2
    ).alias("average_recent_monetary_value"),
    F.round(
        F.avg("risk_signal_count"),
        2
    ).alias("average_risk_signals")
).withColumn(
    "customer_percentage",
    F.round(
        (
            F.col("customer_count") /
            F.sum("customer_count").over(
                Window.partitionBy()
            )
        ) * 100,
        2
    )
)


print("\n========CHURN RISK THRESHOLDS========")
print(f"Recency 75th Percentile: {recency_threshold}")
print(f"Frequency Change 25th Percentile: {frequency_decline_threshold}")
print(f"Monetary Change 25th Percentile: {monetary_decline_threshold}")
print(f"Category Diversity Change 25th Percentile: {category_decline_threshold}")
print(f"Visit Frequency Change 25th Percentile: {visit_decline_threshold}")


print("\n========CHURN RISK SUMMARY========")
risk_summary.orderBy(
    F.when(
        F.col("churn_risk_level") == "Critical",
        1
    ).when(
        F.col("churn_risk_level") == "High",
        2
    ).when(
        F.col("churn_risk_level") == "Medium",
        3
    ).when(
        F.col("churn_risk_level") == "Low",
        4
    ).otherwise(
        5
    )
).show(
    truncate=False
)


print("\n========HIGHEST-RISK CUSTOMERS========")
risk.filter(
    F.col("churn_risk_flag")
).select(
    "customer_id",
    "customer_segment",
    "days_since_last_order",
    "previous_frequency",
    "recent_frequency",
    "frequency_change_pct",
    "previous_monetary_value",
    "recent_monetary_value",
    "monetary_change_pct",
    "previous_category_diversity",
    "recent_category_diversity",
    "category_diversity_change_pct",
    "previous_visit_days",
    "recent_visit_days",
    "visit_frequency_change_pct",
    "risk_signal_count",
    "dormant_customer",
    "churn_risk_level",
    "risk_reason"
).orderBy(
    F.desc("risk_signal_count"),
    F.desc("days_since_last_order"),
    F.asc("customer_id")
).show(
    50,
    truncate=False
)


risk.write.mode(
    "overwrite"
).parquet(
    f"{output_folder}/customer_churn_risk"
)


risk_summary.write.mode(
    "overwrite"
).parquet(
    f"{output_folder}/churn_risk_summary"
)


print("\nCustomer churn-risk identification completed successfully.")


spark.stop()
