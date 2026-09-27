from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("Validate DineIQ Peak Period Analysis") \
    .getOrCreate()


output_folder = f"{ANALYTICS_DATA_FOLDER}/peak_period"

hourly_patterns = spark.read.parquet(f"{output_folder}/hourly_patterns")
daily_patterns = spark.read.parquet(f"{output_folder}/daily_patterns")
weekend_patterns = spark.read.parquet(f"{output_folder}/weekend_patterns")
monthly_trends = spark.read.parquet(f"{output_folder}/monthly_trends")
seasonal_trends = spark.read.parquet(f"{output_folder}/seasonal_trends")
location_peaks = spark.read.parquet(f"{output_folder}/location_peaks")
channel_peaks = spark.read.parquet(f"{output_folder}/channel_peaks")


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


check("Hourly patterns contain data", hourly_patterns.count() > 0)
check("Daily patterns contain data", daily_patterns.count() > 0)
check("Weekend patterns contain data", weekend_patterns.count() > 0)
check("Monthly trends contain data", monthly_trends.count() > 0)
check("Seasonal trends contain data", seasonal_trends.count() > 0)
check("Location peaks contain data", location_peaks.count() > 0)
check("Channel peaks contain data", channel_peaks.count() > 0)

check(
    "Hourly values are valid",
    hourly_patterns.filter(
        (F.col("order_hour") < 0) |
        (F.col("order_hour") > 23)
    ).count() == 0
)

check(
    "Hourly order counts are positive",
    hourly_patterns.filter(
        F.col("order_count") <= 0
    ).count() == 0
)

check(
    "Hourly order percentages total approximately 100 percent",
    abs(
        hourly_patterns.agg(
            F.sum("order_percentage").alias("total")
        ).first()["total"] - 100
    ) <= 0.1
)


check(
    "All seven days are represented",
    daily_patterns.select(
        "day_of_week"
    ).distinct().count() == 7
)

check(
    "Daily order counts are positive",
    daily_patterns.filter(
        F.col("order_count") <= 0
    ).count() == 0
)

check(
    "Daily order percentages total approximately 100 percent",
    abs(
        daily_patterns.agg(
            F.sum("order_percentage").alias("total")
        ).first()["total"] - 100
    ) <= 0.1
)

period_types = {
    row["period_type"]
    for row in weekend_patterns.select(
        "period_type"
    ).distinct().collect()
}


check(
    "Weekend and weekday periods both exist",
    period_types == {"Weekend", "Weekday"}
)

check(
    "Weekend normalization fields are valid",
    weekend_patterns.filter(
        (F.col("days_in_period") <= 0) |
        (F.col("average_orders_per_day") <= 0)
    ).count() == 0
)

check(
    "Weekend order percentages total approximately 100 percent",
    abs(
        weekend_patterns.agg(
            F.sum("order_percentage").alias("total")
        ).first()["total"] - 100
    ) <= 0.1
)


check(
    "Monthly trends contain multiple months",
    monthly_trends.select(
        "year_month"
    ).distinct().count() > 12
)

check(
    "Monthly order counts are positive",
    monthly_trends.filter(
        F.col("order_count") <= 0
    ).count() == 0
)

check(
    "Monthly order percentages total approximately 100 percent",
    abs(
        monthly_trends.agg(
            F.sum("order_percentage").alias("total")
        ).first()["total"] - 100
    ) <= 0.1
)


seasons = {
    row["season"]
    for row in seasonal_trends.select(
        "season"
    ).distinct().collect()
}


check(
    "All four seasons are represented",
    seasons == {"Winter", "Spring", "Summer", "Autumn"}
)

check(
    "Seasonal normalization fields are valid",
    seasonal_trends.filter(
        (F.col("months_in_period") <= 0) |
        (F.col("average_orders_per_month") <= 0) |
        (F.col("average_revenue_per_month") <= 0)
    ).count() == 0
)

check(
    "Seasonal order percentages total approximately 100 percent",
    abs(
        seasonal_trends.agg(
            F.sum("order_percentage").alias("total")
        ).first()["total"] - 100
    ) <= 0.1
)


check(
    "All restaurant peak rows are unique",
    location_peaks.groupBy(
        "restaurant_id"
    ).count().filter(
        F.col("count") > 1
    ).count() == 0
)

check(
    "Location peak hours are valid",
    location_peaks.filter(
        (F.col("peak_hour") < 0) |
        (F.col("peak_hour") > 23)
    ).count() == 0
)

check(
    "Location peak metrics are positive",
    location_peaks.filter(
        (F.col("peak_hour_orders") <= 0) |
        (F.col("peak_hour_revenue") <= 0) |
        (F.col("peak_day_orders") <= 0) |
        (F.col("peak_day_revenue") <= 0)
    ).count() == 0
)



channels = {
    row["ordering_channel"]
    for row in channel_peaks.select(
        "ordering_channel"
    ).distinct().collect()
}


check(
    "All ordering channels are represented",
    channels == {
        "Website/App",
        "Third-Party Delivery",
        "Takeaway",
        "Dine-in"
    }
)

check(
    "Dine-in peak analysis exists",
    "Dine-in" in channels
)

check(
    "Delivery peak analysis exists",
    "Third-Party Delivery" in channels
)

check(
    "Channel peak hours are valid",
    channel_peaks.filter(
        (F.col("peak_hour") < 0) |
        (F.col("peak_hour") > 23)
    ).count() == 0
)

check(
    "Channel peak metrics are positive",
    channel_peaks.filter(
        (F.col("peak_hour_orders") <= 0) |
        (F.col("peak_hour_revenue") <= 0) |
        (F.col("peak_day_orders") <= 0) |
        (F.col("peak_day_revenue") <= 0)
    ).count() == 0
)


total_checks = passed + failed


print("\n========PEAK PERIOD VALIDATION========")
print(f"Passed: {passed}/{total_checks}")
print(f"Failed: {failed}/{total_checks}")


if failed == 0:
    print("\nPeak period analysis validation PASSED.")
else:
    print("\nPeak period analysis validation FAILED.")


spark.stop()
