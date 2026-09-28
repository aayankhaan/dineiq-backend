from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("Validate DineIQ Slow Moving Dishes") \
    .getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")


output_folder = f"{ANALYTICS_DATA_FOLDER}/slow_moving_dishes"

dishes = spark.read.parquet(
    f"{output_folder}/slow_moving_dishes"
)

summary = spark.read.parquet(
    f"{output_folder}/slow_moving_summary"
)


passed = 0
failed = 0


def check(name, condition):
    global passed, failed

    if condition:
        print(f"[PASS] {name}")
        passed += 1
    else:
        print(f"[FAIL] {name}")
        failed += 1


print("\n========SLOW-MOVING DISH VALIDATION========")


check(
    "Slow-moving dish output contains data",
    dishes.count() > 0
)

check(
    "Summary contains one row",
    summary.count() == 1
)


required_signals = {
    "low_sales_volume",
    "low_purchase_frequency",
    "long_purchase_gaps",
    "low_repeat_purchase",
    "high_wastage",
    "weak_profitability",
    "poor_sales_trend"
}


check(
    "All seven Step 32 factors are implemented",
    required_signals.issubset(
        set(dishes.columns)
    )
)


check(
    "Item-location pairs are unique",
    dishes.groupBy(
        "restaurant_id",
        "item_id"
    ).count().filter(
        F.col("count") > 1
    ).count() == 0
)


check(
    "Repeat purchase stays between zero and 100",
    dishes.filter(
        (
            F.col("repeat_purchase_rate_pct") < 0
        ) |
        (
            F.col("repeat_purchase_rate_pct") > 100
        )
    ).count() == 0
)


check(
    "Purchase gaps are non-negative",
    dishes.filter(
        (
            F.col("average_purchase_gap_days") < 0
        ) |
        (
            F.col("days_since_last_purchase") < 0
        )
    ).count() == 0
)


check(
    "Wastage cost is non-negative",
    dishes.filter(
        F.col("wastage_cost") < 0
    ).count() == 0
)


check(
    "Signal count equals seven boolean signals",
    dishes.filter(
        F.col("slow_signal_count") !=
        (
            F.col("low_sales_volume").cast("int") +
            F.col("low_purchase_frequency").cast("int") +
            F.col("long_purchase_gaps").cast("int") +
            F.col("low_repeat_purchase").cast("int") +
            F.col("high_wastage").cast("int") +
            F.col("weak_profitability").cast("int") +
            F.col("poor_sales_trend").cast("int")
        )
    ).count() == 0
)


check(
    "Slow movers require at least four signals",
    dishes.filter(
        F.col("slow_moving_dish")
    ).filter(
        F.col("slow_signal_count") < 4
    ).count() == 0
)


check(
    "Slow movers require low sales or low frequency",
    dishes.filter(
        F.col("slow_moving_dish")
    ).filter(
        ~(
            F.col("low_sales_volume") |
            F.col("low_purchase_frequency")
        )
    ).count() == 0
)

check(
    "Non-slow movers have None severity",
    dishes.filter(
        ~F.col("slow_moving_dish")
    ).filter(
        F.col("slow_moving_severity") != "None"
    ).count() == 0
)



check(
    "Critical severity requires at least six signals",
    dishes.filter(
        F.col("slow_moving_severity") == "Critical"
    ).filter(
        F.col("slow_signal_count") < 6
    ).count() == 0
)


check(
    "High severity requires five signals",
    dishes.filter(
        F.col("slow_moving_severity") == "High"
    ).filter(
        F.col("slow_signal_count") != 5
    ).count() == 0
)


check(
    "Medium severity requires four signals",
    dishes.filter(
        F.col("slow_moving_severity") == "Medium"
    ).filter(
        F.col("slow_signal_count") != 4
    ).count() == 0
)


row = summary.first()


check(
    "Summary pair count matches output",
    int(row["item_location_pairs"]) ==
    dishes.count()
)


check(
    "Summary slow-moving count matches output",
    int(row["slow_moving_pairs"]) ==
    dishes.filter(
        F.col("slow_moving_dish")
    ).count()
)


check(
    "Summary severity counts match slow movers",
    (
        int(row["critical_slow_movers"]) +
        int(row["high_slow_movers"]) +
        int(row["medium_slow_movers"])
    ) ==
    int(row["slow_moving_pairs"])
)


check(
    "At least one slow-moving dish is identified",
    int(row["slow_moving_pairs"]) > 0
)


total = passed + failed


print(
    f"\n========VALIDATION RESULT: "
    f"{passed}/{total} PASS========"
)


if failed == 0:
    print(
        "\nSlow-moving dish validation PASSED."
    )
else:
    print(
        "\nSlow-moving dish validation FAILED."
    )


spark.stop()
