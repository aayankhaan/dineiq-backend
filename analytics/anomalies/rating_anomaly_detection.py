from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.settings import INTEGRATED_DATA_FOLDER, ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("DineIQ Rating Anomaly Detection") \
    .getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "8")

integrated_folder = INTEGRATED_DATA_FOLDER
output_folder = f"{ANALYTICS_DATA_FOLDER}/rating_anomalies"

MIN_HISTORY_WEEKS = 4
RATING_Z_THRESHOLD = 2.0
RATING_CHANGE_THRESHOLD = 0.50
MIN_RATINGS_FOR_SHIFT = 5
IDENTICAL_SHARE_THRESHOLD = 80.0
IDENTICAL_MIN_RATINGS = 10
VOLUME_Z_THRESHOLD = 2.0
VOLUME_MIN_RATINGS = 10
PURCHASE_RATIO_Z_THRESHOLD = 2.0
PURCHASE_RATIO_MULTIPLIER = 1.50
PURCHASE_RATIO_MIN_RATINGS = 5
DUPLICATE_RATING_RATIO = 1.50

ratings = spark.read.parquet(f"{integrated_folder}/ratings")
transactions = spark.read.parquet(f"{integrated_folder}/transactions")



completed = transactions.filter(
    F.col("order_status") == "Completed"
).withColumn(
    "purchase_date",
    F.to_date("order_datetime")
).withColumn(
    "purchase_week",
    F.to_date(F.date_trunc("week", "purchase_date"))
)

purchase_context = completed.groupBy(
    "order_id",
    "customer_id",
    "restaurant_id",
    "item_id"
).agg(
    F.first("item_name", ignorenulls=True).alias("item_name"),
    F.first("restaurant_name", ignorenulls=True).alias("restaurant_name"),
    F.min("purchase_date").alias("purchase_date")
)

rating_context = ratings.alias("r").join(
    purchase_context.alias("p"),
    ["order_id", "customer_id", "restaurant_id", "item_id"],
    "inner"
).select(
    "r.rating_id",
    "r.order_id",
    "r.customer_id",
    "r.restaurant_id",
    "r.item_id",
    F.col("r.rating").cast("double").alias("rating"),
    F.to_date("r.rating_date").alias("rating_date"),
    "p.item_name",
    "p.restaurant_name"
).withColumn(
    "rating_week",
    F.to_date(F.date_trunc("week", "rating_date"))
)

weekly_purchases = completed.groupBy(
    "restaurant_id",
    "item_id",
    "purchase_week"
).agg(
    F.countDistinct("order_id").alias("purchase_count")
)



weekly_base = rating_context.groupBy(
    "restaurant_id",
    "restaurant_name",
    "item_id",
    "item_name",
    "rating_week"
).agg(
    F.count("*").alias("rating_count"),
    F.countDistinct("order_id").alias("distinct_rated_purchases"),
    F.countDistinct("customer_id").alias("rating_customers"),
    F.round(F.avg("rating"), 4).alias("average_rating"),
    F.sum(F.when(F.col("rating") == 1, 1).otherwise(0)).alias("rating_1_count"),
    F.sum(F.when(F.col("rating") == 2, 1).otherwise(0)).alias("rating_2_count"),
    F.sum(F.when(F.col("rating") == 3, 1).otherwise(0)).alias("rating_3_count"),
    F.sum(F.when(F.col("rating") == 4, 1).otherwise(0)).alias("rating_4_count"),
    F.sum(F.when(F.col("rating") == 5, 1).otherwise(0)).alias("rating_5_count")
).withColumn(
    "max_identical_rating_count",
    F.greatest(
        "rating_1_count",
        "rating_2_count",
        "rating_3_count",
        "rating_4_count",
        "rating_5_count"
    )
).withColumn(
    "identical_rating_share_pct",
    F.round((F.col("max_identical_rating_count") / F.col("rating_count")) * 100, 2)
).withColumn(
    "ratings_per_rated_purchase",
    F.round(F.col("rating_count") / F.col("distinct_rated_purchases"), 4)
)

weekly_base = weekly_base.alias("r").join(
    weekly_purchases.alias("p"),
    (F.col("r.restaurant_id") == F.col("p.restaurant_id")) &
    (F.col("r.item_id") == F.col("p.item_id")) &
    (F.col("r.rating_week") == F.col("p.purchase_week")),
    "left"
).select(
    "r.*",
    F.col("p.purchase_count").alias("purchase_count")
).fillna(
    {"purchase_count": 0}
).withColumn(
    "rating_purchase_ratio",
    F.when(
        F.col("purchase_count") > 0,
        F.round(F.col("rating_count") / F.col("purchase_count"), 4)
    )
)


history_window = Window.partitionBy(
    "restaurant_id",
    "item_id"
).orderBy(
    "rating_week"
).rowsBetween(-8, -1)

weekly_profile = weekly_base.withColumn(
    "history_week_count",
    F.count("average_rating").over(history_window)
).withColumn(
    "historical_average_rating",
    F.avg("average_rating").over(history_window)
).withColumn(
    "historical_rating_stddev",
    F.stddev_samp("average_rating").over(history_window)
).withColumn(
    "historical_rating_count",
    F.avg("rating_count").over(history_window)
).withColumn(
    "historical_rating_count_stddev",
    F.stddev_samp("rating_count").over(history_window)
).withColumn(
    "historical_purchase_ratio",
    F.avg("rating_purchase_ratio").over(history_window)
).withColumn(
    "historical_purchase_ratio_stddev",
    F.stddev_samp("rating_purchase_ratio").over(history_window)
).withColumn(
    "rating_change",
    F.round(F.col("average_rating") - F.col("historical_average_rating"), 4)
).withColumn(
    "rating_z_score",
    F.when(
        F.col("historical_rating_stddev") > 0,
        F.round(F.col("rating_change") / F.col("historical_rating_stddev"), 4)
    )
).withColumn(
    "rating_volume_z_score",
    F.when(
        F.col("historical_rating_count_stddev") > 0,
        F.round(
            (F.col("rating_count") - F.col("historical_rating_count")) /
            F.col("historical_rating_count_stddev"),
            4
        )
    )
).withColumn(
    "purchase_ratio_z_score",
    F.when(
        F.col("historical_purchase_ratio_stddev") > 0,
        F.round(
            (F.col("rating_purchase_ratio") - F.col("historical_purchase_ratio")) /
            F.col("historical_purchase_ratio_stddev"),
            4
        )
    )
)


rating_anomaly_windows = weekly_profile.withColumn(
    "sudden_rating_spike",
    F.coalesce(
        (F.col("history_week_count") >= MIN_HISTORY_WEEKS) &
        (F.col("rating_change") >= RATING_CHANGE_THRESHOLD) &
        (F.col("rating_count") >= MIN_RATINGS_FOR_SHIFT) &
        (F.col("rating_z_score") >= RATING_Z_THRESHOLD),
        F.lit(False)
    )
).withColumn(
    "sudden_rating_drop",
    F.coalesce(
        (F.col("history_week_count") >= MIN_HISTORY_WEEKS) &
        (F.col("rating_change") <= -RATING_CHANGE_THRESHOLD) &
        (F.col("rating_count") >= MIN_RATINGS_FOR_SHIFT) &
        (F.col("rating_z_score") <= -RATING_Z_THRESHOLD),
        F.lit(False)
    )
).withColumn(
    "excessive_identical_ratings",
    (F.col("rating_count") >= IDENTICAL_MIN_RATINGS) &
    (F.col("identical_rating_share_pct") >= IDENTICAL_SHARE_THRESHOLD)
).withColumn(
    "high_rating_volume",
    F.coalesce(
        (F.col("history_week_count") >= MIN_HISTORY_WEEKS) &
        (F.col("rating_count") >= VOLUME_MIN_RATINGS) &
        (F.col("rating_volume_z_score") >= VOLUME_Z_THRESHOLD),
        F.lit(False)
    )
).withColumn(
    "purchase_inconsistency",
    F.coalesce(
        (
            (F.col("history_week_count") >= MIN_HISTORY_WEEKS) &
            (F.col("rating_count") >= PURCHASE_RATIO_MIN_RATINGS) &
            (F.col("purchase_ratio_z_score") >= PURCHASE_RATIO_Z_THRESHOLD) &
            (F.col("rating_purchase_ratio") >= F.col("historical_purchase_ratio") * PURCHASE_RATIO_MULTIPLIER)
        ) |
        (F.col("ratings_per_rated_purchase") >= DUPLICATE_RATING_RATIO),
        F.lit(False)
    )
).withColumn(
    "anomaly_flag_count",
    F.col("sudden_rating_spike").cast("int") +
    F.col("sudden_rating_drop").cast("int") +
    F.col("excessive_identical_ratings").cast("int") +
    F.col("high_rating_volume").cast("int") +
    F.col("purchase_inconsistency").cast("int")
).withColumn(
    "rating_anomaly_detected",
    F.col("anomaly_flag_count") > 0
).withColumn(
    "anomaly_severity",
    F.when(F.col("anomaly_flag_count") >= 3, "Critical")
    .when(F.col("anomaly_flag_count") == 2, "High")
    .when(F.col("anomaly_flag_count") == 1, "Medium")
    .otherwise("None")
).withColumn(
    "anomaly_types",
    F.concat_ws(
        ", ",
        F.when(F.col("sudden_rating_spike"), F.lit("Sudden Rating Spike")),
        F.when(F.col("sudden_rating_drop"), F.lit("Sudden Rating Drop")),
        F.when(F.col("excessive_identical_ratings"), F.lit("Excessive Identical Ratings")),
        F.when(F.col("high_rating_volume"), F.lit("High Rating Volume")),
        F.when(F.col("purchase_inconsistency"), F.lit("Ratings / Purchasing Inconsistency"))
    )
).withColumn(
    "rating_z_threshold",
    F.lit(RATING_Z_THRESHOLD)
).withColumn(
    "rating_change_threshold",
    F.lit(RATING_CHANGE_THRESHOLD)
).withColumn(
    "identical_share_threshold",
    F.lit(IDENTICAL_SHARE_THRESHOLD)
).withColumn(
    "volume_z_threshold",
    F.lit(VOLUME_Z_THRESHOLD)
).withColumn(
    "purchase_ratio_z_threshold",
    F.lit(PURCHASE_RATIO_Z_THRESHOLD)
)


anomaly_summary = rating_anomaly_windows.agg(
    F.count("*").alias("weekly_windows"),
    F.sum(F.col("rating_anomaly_detected").cast("int")).alias("anomalous_windows"),
    F.sum(F.col("sudden_rating_spike").cast("int")).alias("sudden_rating_spikes"),
    F.sum(F.col("sudden_rating_drop").cast("int")).alias("sudden_rating_drops"),
    F.sum(F.col("excessive_identical_ratings").cast("int")).alias("excessive_identical_rating_windows"),
    F.sum(F.col("high_rating_volume").cast("int")).alias("high_rating_volume_windows"),
    F.sum(F.col("purchase_inconsistency").cast("int")).alias("purchase_inconsistency_windows"),
    F.sum(F.when(F.col("anomaly_severity") == "Critical", 1).otherwise(0)).alias("critical_anomalies"),
    F.sum(F.when(F.col("anomaly_severity") == "High", 1).otherwise(0)).alias("high_anomalies"),
    F.sum(F.when(F.col("anomaly_severity") == "Medium", 1).otherwise(0)).alias("medium_anomalies")
).withColumn(
    "anomaly_rate_pct",
    F.round((F.col("anomalous_windows") / F.col("weekly_windows")) * 100, 2)
)



print("\n========RATING ANOMALY SUMMARY========")
anomaly_summary.show(truncate=False)

print("\n========TOP RATING ANOMALIES========")
rating_anomaly_windows.filter(
    F.col("rating_anomaly_detected")
).select(
    "rating_week",
    "restaurant_id",
    "restaurant_name",
    "item_id",
    "item_name",
    "anomaly_severity",
    "anomaly_flag_count",
    "anomaly_types",
    "rating_count",
    "average_rating",
    "historical_average_rating",
    "rating_change",
    "rating_z_score",
    "identical_rating_share_pct",
    "rating_volume_z_score",
    "purchase_count",
    "rating_purchase_ratio",
    "historical_purchase_ratio",
    "purchase_ratio_z_score",
    "ratings_per_rated_purchase"
).orderBy(
    F.desc("anomaly_flag_count"),
    F.desc(F.abs(F.coalesce(F.col("rating_z_score"), F.lit(0.0))))
).show(30, truncate=False)


rating_anomaly_windows.write.mode("overwrite").parquet(
    f"{output_folder}/rating_anomaly_windows"
)

anomaly_summary.write.mode("overwrite").parquet(
    f"{output_folder}/anomaly_summary"
)

print("\nRating anomaly detection completed successfully.")

spark.stop()
