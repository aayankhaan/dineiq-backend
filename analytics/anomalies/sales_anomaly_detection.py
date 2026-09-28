from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.settings import INTEGRATED_DATA_FOLDER, ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("DineIQ Sales Anomaly Detection") \
    .getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "8")


output_folder = f"{ANALYTICS_DATA_FOLDER}/sales_anomalies"

MIN_SALES_HISTORY_DAYS = 14
SALES_Z_THRESHOLD = 2.5
SALES_CHANGE_THRESHOLD_PCT = 50.0

MIN_DEMAND_HISTORY_WEEKS = 6
DEMAND_Z_THRESHOLD = 3.0
DEMAND_CHANGE_THRESHOLD_PCT = 75.0
MIN_EXPECTED_WEEKLY_DEMAND = 3.0
MIN_ABSOLUTE_DEMAND_CHANGE = 10.0

MIN_DISCOUNT_PCT = 20.0


transactions = spark.read.parquet(
    f"{INTEGRATED_DATA_FOLDER}/transactions"
)



completed_orders = transactions.filter(
    F.col("order_status") == "Completed"
).groupBy(
    "order_id"
).agg(
    F.first(
        "customer_id",
        ignorenulls=True
    ).alias(
        "customer_id"
    ),
    F.first(
        "restaurant_id",
        ignorenulls=True
    ).alias(
        "restaurant_id"
    ),
    F.first(
        "restaurant_name",
        ignorenulls=True
    ).alias(
        "restaurant_name"
    ),
    F.first(
        "order_datetime",
        ignorenulls=True
    ).alias(
        "order_datetime"
    ),
    F.first(
        "ordering_channel",
        ignorenulls=True
    ).alias(
        "ordering_channel"
    ),
    F.first(
        "payment_method",
        ignorenulls=True
    ).alias(
        "payment_method"
    ),
    F.max(
        F.col("order_subtotal").cast("double")
    ).alias(
        "order_subtotal"
    ),
    F.max(
        F.col("order_discount_amount").cast("double")
    ).alias(
        "order_discount_amount"
    ),
    F.max(
        F.col("total_amount").cast("double")
    ).alias(
        "order_total"
    )
).withColumn(
    "order_date",
    F.to_date("order_datetime")
).withColumn(
    "discount_pct",
    F.when(
        F.col("order_subtotal") > 0,
        F.round(
            (
                F.col("order_discount_amount") /
                F.col("order_subtotal")
            ) * 100,
            4
        )
    ).otherwise(
        F.lit(0.0)
    )
)


date_bounds = completed_orders.agg(
    F.min("order_date").alias("min_date"),
    F.max("order_date").alias("max_date")
).first()

min_date = date_bounds["min_date"]
max_date = date_bounds["max_date"]


calendar = spark.range(
    1
).select(
    F.explode(
        F.sequence(
            F.lit(min_date),
            F.lit(max_date),
            F.expr("interval 1 day")
        )
    ).alias(
        "analysis_date"
    )
)


# ==================== SALES SPIKES / DROPS ====================


daily_sales = completed_orders.groupBy(
    "restaurant_id",
    "restaurant_name",
    F.col("order_date").alias(
        "analysis_date"
    )
).agg(
    F.countDistinct(
        "order_id"
    ).alias(
        "daily_orders"
    ),
    F.round(
        F.sum("order_total"),
        2
    ).alias(
        "daily_revenue"
    )
)


restaurant_start = completed_orders.groupBy(
    "restaurant_id",
    "restaurant_name"
).agg(
    F.min(
        "order_date"
    ).alias(
        "first_sales_date"
    )
)


restaurant_days = restaurant_start.crossJoin(
    calendar
).filter(
    F.col("analysis_date") >=
    F.col("first_sales_date")
).select(
    "restaurant_id",
    "restaurant_name",
    "analysis_date"
).join(
    daily_sales,
    [
        "restaurant_id",
        "restaurant_name",
        "analysis_date"
    ],
    "left"
).fillna(
    {
        "daily_orders": 0,
        "daily_revenue": 0.0
    }
).withColumn(
    "day_number",
    F.datediff(
        "analysis_date",
        F.lit("1970-01-01")
    )
)


sales_history_window = Window.partitionBy(
    "restaurant_id"
).orderBy(
    "day_number"
).rangeBetween(
    -28,
    -1
)


daily_sales_profile = restaurant_days.withColumn(
    "history_days",
    F.count(
        "daily_revenue"
    ).over(
        sales_history_window
    )
).withColumn(
    "expected_daily_revenue",
    F.avg(
        "daily_revenue"
    ).over(
        sales_history_window
    )
).withColumn(
    "revenue_stddev",
    F.stddev_samp(
        "daily_revenue"
    ).over(
        sales_history_window
    )
).withColumn(
    "sales_change_pct",
    F.when(
        F.col("expected_daily_revenue") > 0,
        (
            (
                F.col("daily_revenue") -
                F.col("expected_daily_revenue")
            ) /
            F.col("expected_daily_revenue")
        ) * 100
    )
).withColumn(
    "sales_z_score",
    F.when(
        F.col("revenue_stddev") > 0,
        (
            F.col("daily_revenue") -
            F.col("expected_daily_revenue")
        ) /
        F.col("revenue_stddev")
    )
)


sales_spikes = daily_sales_profile.filter(
    (
        F.col("history_days") >=
        MIN_SALES_HISTORY_DAYS
    ) &
    (
        F.col("sales_change_pct") >=
        SALES_CHANGE_THRESHOLD_PCT
    ) &
    (
        F.col("sales_z_score") >=
        SALES_Z_THRESHOLD
    )
)


sales_drops = daily_sales_profile.filter(
    (
        F.col("history_days") >=
        MIN_SALES_HISTORY_DAYS
    ) &
    (
        F.col("sales_change_pct") <=
        -SALES_CHANGE_THRESHOLD_PCT
    ) &
    (
        F.col("sales_z_score") <=
        -SALES_Z_THRESHOLD
    )
)


# ==================== HIGH ORDER VALUES ====================


order_value_thresholds = completed_orders.groupBy(
    "restaurant_id"
).agg(
    F.expr(
        "percentile_approx(order_total, array(0.25, 0.75), 10000)"
    ).alias(
        "order_value_quartiles"
    )
).withColumn(
    "order_value_q1",
    F.col(
        "order_value_quartiles"
    )[0]
).withColumn(
    "order_value_q3",
    F.col(
        "order_value_quartiles"
    )[1]
).withColumn(
    "order_value_iqr",
    F.col("order_value_q3") -
    F.col("order_value_q1")
).withColumn(
    "high_order_value_threshold",
    F.col("order_value_q3") +
    (
        F.col("order_value_iqr") *
        3.0
    )
).select(
    "restaurant_id",
    "high_order_value_threshold"
)


high_order_values = completed_orders.join(
    order_value_thresholds,
    "restaurant_id",
    "inner"
).filter(
    F.col("order_total") >
    F.col("high_order_value_threshold")
).withColumn(
    "order_value_ratio",
    F.col("order_total") /
    F.col("high_order_value_threshold")
)


# ==================== UNUSUAL DISCOUNTS ====================


discounted_orders = completed_orders.filter(
    F.col("discount_pct") > 0
)


discount_thresholds = discounted_orders.groupBy(
    "restaurant_id"
).agg(
    F.count(
        "*"
    ).alias(
        "discounted_order_count"
    ),
    F.expr(
        "percentile_approx(discount_pct, array(0.25, 0.75), 10000)"
    ).alias(
        "discount_quartiles"
    )
).withColumn(
    "discount_q1",
    F.col(
        "discount_quartiles"
    )[0]
).withColumn(
    "discount_q3",
    F.col(
        "discount_quartiles"
    )[1]
).withColumn(
    "discount_iqr",
    F.col("discount_q3") -
    F.col("discount_q1")
).withColumn(
    "unusual_discount_threshold",
    F.col("discount_q3") +
    (
        F.col("discount_iqr") *
        1.5
    )
).select(
    "restaurant_id",
    "discounted_order_count",
    "unusual_discount_threshold"
)


unusual_discounts = completed_orders.join(
    discount_thresholds,
    "restaurant_id",
    "inner"
).filter(
    (
        F.col("discounted_order_count") >= 10
    ) &
    (
        F.col("discount_pct") >=
        MIN_DISCOUNT_PCT
    ) &
    (
        F.col("discount_pct") >
        F.col("unusual_discount_threshold")
    )
).withColumn(
    "discount_ratio",
    F.when(
        F.col("unusual_discount_threshold") > 0,
        F.col("discount_pct") /
        F.col("unusual_discount_threshold")
    )
)


# ==================== UNEXPECTED DEMAND ====================


weekly_demand_actual = transactions.filter(
    F.col("order_status") == "Completed"
).withColumn(
    "demand_week",
    F.to_date(
        F.date_trunc(
            "week",
            "order_datetime"
        )
    )
).groupBy(
    "restaurant_id",
    "restaurant_name",
    "item_id",
    "item_name",
    "demand_week"
).agg(
    F.sum(
        F.col("quantity").cast("double")
    ).alias(
        "weekly_demand"
    )
)


item_location_start = weekly_demand_actual.groupBy(
    "restaurant_id",
    "restaurant_name",
    "item_id",
    "item_name"
).agg(
    F.min(
        "demand_week"
    ).alias(
        "first_demand_week"
    )
)


week_calendar = calendar.select(
    F.to_date(
        F.date_trunc(
            "week",
            "analysis_date"
        )
    ).alias(
        "demand_week"
    )
).distinct()


item_location_weeks = item_location_start.crossJoin(
    week_calendar
).filter(
    F.col("demand_week") >=
    F.col("first_demand_week")
).select(
    "restaurant_id",
    "restaurant_name",
    "item_id",
    "item_name",
    "demand_week"
).join(
    weekly_demand_actual,
    [
        "restaurant_id",
        "restaurant_name",
        "item_id",
        "item_name",
        "demand_week"
    ],
    "left"
).fillna(
    {
        "weekly_demand": 0.0
    }
)


demand_history_window = Window.partitionBy(
    "restaurant_id",
    "item_id"
).orderBy(
    "demand_week"
).rowsBetween(
    -8,
    -1
)


weekly_demand_profile = item_location_weeks.withColumn(
    "history_weeks",
    F.count(
        "weekly_demand"
    ).over(
        demand_history_window
    )
).withColumn(
    "expected_weekly_demand",
    F.avg(
        "weekly_demand"
    ).over(
        demand_history_window
    )
).withColumn(
    "demand_stddev",
    F.stddev_samp(
        "weekly_demand"
    ).over(
        demand_history_window
    )
).withColumn(
    "demand_change_pct",
    F.when(
        F.col("expected_weekly_demand") > 0,
        (
            (
                F.col("weekly_demand") -
                F.col("expected_weekly_demand")
            ) /
            F.col("expected_weekly_demand")
        ) * 100
    )
).withColumn(
    "demand_z_score",
    F.when(
        F.col("demand_stddev") > 0,
        (
            F.col("weekly_demand") -
            F.col("expected_weekly_demand")
        ) /
        F.col("demand_stddev")
    )
)


unexpected_demand = weekly_demand_profile.filter(
    (
        F.col("history_weeks") >=
        MIN_DEMAND_HISTORY_WEEKS
    ) &
    (
        F.col("expected_weekly_demand") >=
        MIN_EXPECTED_WEEKLY_DEMAND
    ) &
    (
        F.abs(
            F.col("weekly_demand") -
            F.col("expected_weekly_demand")
        ) >=
        MIN_ABSOLUTE_DEMAND_CHANGE
    ) &
    (
        F.abs(
            F.col("demand_change_pct")
        ) >=
        DEMAND_CHANGE_THRESHOLD_PCT
    ) &
    (
        F.abs(
            F.col("demand_z_score")
        ) >=
        DEMAND_Z_THRESHOLD
    )
)



duplicate_groups = completed_orders.groupBy(
    "customer_id",
    "restaurant_id",
    "order_datetime",
    "ordering_channel",
    "payment_method",
    "order_subtotal",
    "order_discount_amount",
    "order_total"
).agg(
    F.countDistinct(
        "order_id"
    ).alias(
        "duplicate_group_size"
    )
).filter(
    F.col("duplicate_group_size") > 1
)


duplicate_transactions = completed_orders.join(
    duplicate_groups,
    [
        "customer_id",
        "restaurant_id",
        "order_datetime",
        "ordering_channel",
        "payment_method",
        "order_subtotal",
        "order_discount_amount",
        "order_total"
    ],
    "inner"
)


def severity_from_score(score_column):
    return F.when(
        score_column >= 4.0,
        "Critical"
    ).when(
        score_column >= 3.0,
        "High"
    ).otherwise(
        "Medium"
    )


sales_spike_events = sales_spikes.select(
    F.lit("Sudden Sales Spike").alias("anomaly_type"),
    F.col("analysis_date").alias("event_date"),
    "restaurant_id",
    "restaurant_name",
    F.lit(None).cast("int").alias("item_id"),
    F.lit(None).cast("string").alias("item_name"),
    F.lit(None).cast("int").alias("order_id"),
    F.lit(None).cast("int").alias("customer_id"),
    F.lit("Daily Revenue").alias("metric_name"),
    F.round("daily_revenue", 4).alias("actual_value"),
    F.round("expected_daily_revenue", 4).alias("expected_value"),
    F.round("sales_change_pct", 2).alias("change_pct"),
    F.round(
        F.abs("sales_z_score"),
        4
    ).alias("anomaly_score"),
    severity_from_score(
        F.abs("sales_z_score")
    ).alias("severity"),
    F.concat(
        F.lit("Daily revenue increased "),
        F.round("sales_change_pct", 2).cast("string"),
        F.lit("% versus the prior 28-day baseline.")
    ).alias("evidence")
)


sales_drop_events = sales_drops.select(
    F.lit("Sudden Sales Drop").alias("anomaly_type"),
    F.col("analysis_date").alias("event_date"),
    "restaurant_id",
    "restaurant_name",
    F.lit(None).cast("int").alias("item_id"),
    F.lit(None).cast("string").alias("item_name"),
    F.lit(None).cast("int").alias("order_id"),
    F.lit(None).cast("int").alias("customer_id"),
    F.lit("Daily Revenue").alias("metric_name"),
    F.round("daily_revenue", 4).alias("actual_value"),
    F.round("expected_daily_revenue", 4).alias("expected_value"),
    F.round("sales_change_pct", 2).alias("change_pct"),
    F.round(
        F.abs("sales_z_score"),
        4
    ).alias("anomaly_score"),
    severity_from_score(
        F.abs("sales_z_score")
    ).alias("severity"),
    F.concat(
        F.lit("Daily revenue decreased "),
        F.round(
            F.abs("sales_change_pct"),
            2
        ).cast("string"),
        F.lit("% versus the prior 28-day baseline.")
    ).alias("evidence")
)


high_order_events = high_order_values.select(
    F.lit("Abnormally High Order Value").alias("anomaly_type"),
    F.col("order_date").alias("event_date"),
    "restaurant_id",
    "restaurant_name",
    F.lit(None).cast("int").alias("item_id"),
    F.lit(None).cast("string").alias("item_name"),
    F.col("order_id").cast("int").alias("order_id"),
    F.col("customer_id").cast("int").alias("customer_id"),
    F.lit("Order Value").alias("metric_name"),
    F.round("order_total", 4).alias("actual_value"),
    F.round("high_order_value_threshold", 4).alias("expected_value"),
    F.round(
        (
            (
                F.col("order_total") -
                F.col("high_order_value_threshold")
            ) /
            F.col("high_order_value_threshold")
        ) * 100,
        2
    ).alias("change_pct"),
    F.round(
        "order_value_ratio",
        4
    ).alias("anomaly_score"),
    F.when(
        F.col("order_value_ratio") >= 2.0,
        "Critical"
    ).when(
        F.col("order_value_ratio") >= 1.5,
        "High"
    ).otherwise(
        "Medium"
    ).alias("severity"),
    F.concat(
        F.lit("Order value exceeded the restaurant extreme-outlier threshold of "),
        F.round(
            "high_order_value_threshold",
            2
        ).cast("string"),
        F.lit(".")
    ).alias("evidence")
)


discount_events = unusual_discounts.select(
    F.lit("Unusual Discount").alias("anomaly_type"),
    F.col("order_date").alias("event_date"),
    "restaurant_id",
    "restaurant_name",
    F.lit(None).cast("int").alias("item_id"),
    F.lit(None).cast("string").alias("item_name"),
    F.col("order_id").cast("int").alias("order_id"),
    F.col("customer_id").cast("int").alias("customer_id"),
    F.lit("Discount Percentage").alias("metric_name"),
    F.round("discount_pct", 4).alias("actual_value"),
    F.round("unusual_discount_threshold", 4).alias("expected_value"),
    F.round(
        F.col("discount_pct") -
        F.col("unusual_discount_threshold"),
        2
    ).alias("change_pct"),
    F.round(
        F.coalesce(
            F.col("discount_ratio"),
            F.lit(1.0)
        ),
        4
    ).alias("anomaly_score"),
    F.when(
        F.col("discount_ratio") >= 1.5,
        "Critical"
    ).when(
        F.col("discount_ratio") >= 1.25,
        "High"
    ).otherwise(
        "Medium"
    ).alias("severity"),
    F.concat(
        F.lit("Order discount was "),
        F.round("discount_pct", 2).cast("string"),
        F.lit("%, above the restaurant discount outlier threshold.")
    ).alias("evidence")
)


demand_events = unexpected_demand.select(
    F.lit("Unexpected Demand").alias("anomaly_type"),
    F.col("demand_week").alias("event_date"),
    "restaurant_id",
    "restaurant_name",
    "item_id",
    "item_name",
    F.lit(None).cast("int").alias("order_id"),
    F.lit(None).cast("int").alias("customer_id"),
    F.lit("Weekly Item Demand").alias("metric_name"),
    F.round("weekly_demand", 4).alias("actual_value"),
    F.round("expected_weekly_demand", 4).alias("expected_value"),
    F.round("demand_change_pct", 2).alias("change_pct"),
    F.round(
        F.abs("demand_z_score"),
        4
    ).alias("anomaly_score"),
    severity_from_score(
        F.abs("demand_z_score")
    ).alias("severity"),
    F.concat(
        F.lit("Weekly item demand changed "),
        F.round("demand_change_pct", 2).cast("string"),
        F.lit("% versus the prior eight-week baseline.")
    ).alias("evidence")
)


duplicate_events = duplicate_transactions.select(
    F.lit("Duplicate Transaction").alias("anomaly_type"),
    F.col("order_date").alias("event_date"),
    "restaurant_id",
    "restaurant_name",
    F.lit(None).cast("int").alias("item_id"),
    F.lit(None).cast("string").alias("item_name"),
    F.col("order_id").cast("int").alias("order_id"),
    F.col("customer_id").cast("int").alias("customer_id"),
    F.lit("Duplicate Group Size").alias("metric_name"),
    F.col("duplicate_group_size").cast("double").alias("actual_value"),
    F.lit(1.0).alias("expected_value"),
    F.round(
        (
            F.col("duplicate_group_size") -
            F.lit(1)
        ) * 100.0,
        2
    ).alias("change_pct"),
    F.col("duplicate_group_size").cast("double").alias("anomaly_score"),
    F.when(
        F.col("duplicate_group_size") >= 4,
        "Critical"
    ).when(
        F.col("duplicate_group_size") == 3,
        "High"
    ).otherwise(
        "Medium"
    ).alias("severity"),
    F.concat(
        F.lit("Found "),
        F.col("duplicate_group_size").cast("string"),
        F.lit(" orders with the same transaction signature.")
    ).alias("evidence")
)


sales_anomaly_events = sales_spike_events.unionByName(
    sales_drop_events
).unionByName(
    high_order_events
).unionByName(
    discount_events
).unionByName(
    demand_events
).unionByName(
    duplicate_events
)


anomaly_summary = sales_anomaly_events.agg(
    F.count(
        "*"
    ).alias(
        "total_anomaly_events"
    ),
    F.sum(
        F.when(
            F.col("anomaly_type") == "Sudden Sales Spike",
            1
        ).otherwise(0)
    ).alias(
        "sudden_sales_spikes"
    ),
    F.sum(
        F.when(
            F.col("anomaly_type") == "Sudden Sales Drop",
            1
        ).otherwise(0)
    ).alias(
        "sudden_sales_drops"
    ),
    F.sum(
        F.when(
            F.col("anomaly_type") == "Abnormally High Order Value",
            1
        ).otherwise(0)
    ).alias(
        "high_order_value_events"
    ),
    F.sum(
        F.when(
            F.col("anomaly_type") == "Unusual Discount",
            1
        ).otherwise(0)
    ).alias(
        "unusual_discount_events"
    ),
    F.sum(
        F.when(
            F.col("anomaly_type") == "Unexpected Demand",
            1
        ).otherwise(0)
    ).alias(
        "unexpected_demand_events"
    ),
    F.sum(
        F.when(
            F.col("anomaly_type") == "Duplicate Transaction",
            1
        ).otherwise(0)
    ).alias(
        "duplicate_transaction_events"
    ),
    F.sum(
        F.when(
            F.col("severity") == "Critical",
            1
        ).otherwise(0)
    ).alias(
        "critical_events"
    ),
    F.sum(
        F.when(
            F.col("severity") == "High",
            1
        ).otherwise(0)
    ).alias(
        "high_events"
    ),
    F.sum(
        F.when(
            F.col("severity") == "Medium",
            1
        ).otherwise(0)
    ).alias(
        "medium_events"
    ),
    F.countDistinct(
        "restaurant_id"
    ).alias(
        "affected_restaurants"
    ),
    F.countDistinct(
        "item_id"
    ).alias(
        "affected_items"
    )
)


print("\n========SALES ANOMALY SUMMARY========")
anomaly_summary.show(
    truncate=False
)


print("\n========TOP SALES ANOMALIES========")
sales_anomaly_events.orderBy(
    F.when(
        F.col("severity") == "Critical",
        1
    ).when(
        F.col("severity") == "High",
        2
    ).otherwise(
        3
    ),
    F.desc("anomaly_score")
).show(
    30,
    truncate=False
)



sales_anomaly_events.write.mode(
    "overwrite"
).parquet(
    f"{output_folder}/sales_anomaly_events"
)


anomaly_summary.write.mode(
    "overwrite"
).parquet(
    f"{output_folder}/anomaly_summary"
)


print("\nSales anomaly detection completed successfully.")


spark.stop()
