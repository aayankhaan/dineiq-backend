from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("Validate DineIQ Rating Anomalies") \
    .getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")

output_folder = f"{ANALYTICS_DATA_FOLDER}/rating_anomalies"

rating_anomaly_windows = spark.read.parquet(
    f"{output_folder}/rating_anomaly_windows"
)

anomaly_summary = spark.read.parquet(
    f"{output_folder}/anomaly_summary"
)

passed = 0
failed = 0


def check(name, condition, details=""):
    global passed, failed

    if condition:
        print(f"[PASS] {name}")
        passed += 1
    else:
        suffix = f" - {details}" if details else ""
        print(f"[FAIL] {name}{suffix}")
        failed += 1


print("\n========RATING ANOMALY VALIDATION========")

check(
    "Rating anomaly windows contain data",
    rating_anomaly_windows.count() > 0
)

check(
    "Anomaly summary contains one row",
    anomaly_summary.count() == 1
)

required_flags = {
    "sudden_rating_spike",
    "sudden_rating_drop",
    "excessive_identical_ratings",
    "high_rating_volume",
    "purchase_inconsistency"
}

check(
    "All five Step 30 anomaly types are implemented",
    required_flags.issubset(set(rating_anomaly_windows.columns))
)

for column in required_flags | {"rating_anomaly_detected"}:
    check(
        f"{column} is never null",
        rating_anomaly_windows.filter(F.col(column).isNull()).count() == 0
    )

check(
    "Average ratings stay between one and five",
    rating_anomaly_windows.filter(
        (F.col("average_rating") < 1) |
        (F.col("average_rating") > 5)
    ).count() == 0
)

check(
    "Rating counts are positive",
    rating_anomaly_windows.filter(F.col("rating_count") <= 0).count() == 0
)

check(
    "Identical-rating share stays between zero and 100",
    rating_anomaly_windows.filter(
        (F.col("identical_rating_share_pct") < 0) |
        (F.col("identical_rating_share_pct") > 100)
    ).count() == 0
)

check(
    "Ratings per rated purchase is at least one",
    rating_anomaly_windows.filter(
        F.col("ratings_per_rated_purchase") < 1
    ).count() == 0
)

check(
    "Sudden spikes require positive rating change",
    rating_anomaly_windows.filter(
        F.col("sudden_rating_spike") &
        (F.col("rating_change") <= 0)
    ).count() == 0
)

check(
    "Sudden spikes require at least five ratings",
    rating_anomaly_windows.filter(
        F.col("sudden_rating_spike")
    ).filter(
        F.col("rating_count") < 5
    ).count() == 0
)


check(
    "Sudden drops require at least five ratings",
    rating_anomaly_windows.filter(
        F.col("sudden_rating_drop")
    ).filter(
        F.col("rating_count") < 5
    ).count() == 0
)

check(
    "Sudden drops require negative rating change",
    rating_anomaly_windows.filter(
        F.col("sudden_rating_drop") &
        (F.col("rating_change") >= 0)
    ).count() == 0
)

check(
    "Identical-rating anomalies meet share threshold",
    rating_anomaly_windows.filter(
        F.col("excessive_identical_ratings") &
        (F.col("identical_rating_share_pct") < F.col("identical_share_threshold"))
    ).count() == 0
)

check(
    "High-volume anomalies meet z-score threshold",
    rating_anomaly_windows.filter(
        F.col("high_rating_volume") &
        (F.col("rating_volume_z_score") < F.col("volume_z_threshold"))
    ).count() == 0
)

check(
    "Purchase inconsistencies have supporting evidence",
    rating_anomaly_windows.filter(
        F.col("purchase_inconsistency") &
        (
            F.col("purchase_ratio_z_score").isNull() |
            (F.col("purchase_ratio_z_score") < F.col("purchase_ratio_z_threshold"))
        ) &
        (F.col("ratings_per_rated_purchase") < 1.50)
    ).count() == 0
)

check(
    "Anomaly flag count equals boolean flags",
    rating_anomaly_windows.filter(
        F.col("anomaly_flag_count") !=
        (
            F.col("sudden_rating_spike").cast("int") +
            F.col("sudden_rating_drop").cast("int") +
            F.col("excessive_identical_ratings").cast("int") +
            F.col("high_rating_volume").cast("int") +
            F.col("purchase_inconsistency").cast("int")
        )
    ).count() == 0
)

check(
    "Detected flag matches anomaly count",
    rating_anomaly_windows.filter(
        F.col("rating_anomaly_detected") !=
        (F.col("anomaly_flag_count") > 0)
    ).count() == 0
)

check(
    "Anomaly count stays between zero and five",
    rating_anomaly_windows.filter(
        (F.col("anomaly_flag_count") < 0) |
        (F.col("anomaly_flag_count") > 5)
    ).count() == 0
)

check(
    "Critical severity requires at least three flags",
    rating_anomaly_windows.filter(
        (F.col("anomaly_severity") == "Critical") &
        (F.col("anomaly_flag_count") < 3)
    ).count() == 0
)

check(
    "High severity requires exactly two flags",
    rating_anomaly_windows.filter(
        (F.col("anomaly_severity") == "High") &
        (F.col("anomaly_flag_count") != 2)
    ).count() == 0
)

check(
    "Medium severity requires exactly one flag",
    rating_anomaly_windows.filter(
        (F.col("anomaly_severity") == "Medium") &
        (F.col("anomaly_flag_count") != 1)
    ).count() == 0
)

check(
    "None severity requires zero flags",
    rating_anomaly_windows.filter(
        (F.col("anomaly_severity") == "None") &
        (F.col("anomaly_flag_count") != 0)
    ).count() == 0
)

summary = anomaly_summary.first()

check(
    "Summary weekly-window count matches output",
    int(summary["weekly_windows"]) == rating_anomaly_windows.count()
)

check(
    "Summary anomalous count matches output",
    int(summary["anomalous_windows"]) == rating_anomaly_windows.filter(
        F.col("rating_anomaly_detected")
    ).count()
)

check(
    "At least one anomaly is detected",
    int(summary["anomalous_windows"]) > 0
)

total_checks = passed + failed

print(
    f"\n========VALIDATION RESULT: "
    f"{passed}/{total_checks} PASS========"
)

if failed == 0:
    print("\nRating anomaly validation PASSED.")
else:
    print("\nRating anomaly validation FAILED.")

spark.stop()
