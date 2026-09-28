from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("Validate DineIQ Sales Anomalies") \
    .getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")


output_folder = f"{ANALYTICS_DATA_FOLDER}/sales_anomalies"


events = spark.read.parquet(
    f"{output_folder}/sales_anomaly_events"
)

summary = spark.read.parquet(
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
        suffix = (
            f" - {details}"
            if details
            else ""
        )
        print(f"[FAIL] {name}{suffix}")
        failed += 1


print("\n========SALES ANOMALY VALIDATION========")




check(
    "Sales anomaly events contain data",
    events.count() > 0
)


check(
    "Anomaly summary contains exactly one row",
    summary.count() == 1
)


required_summary_columns = {
    "sudden_sales_spikes",
    "sudden_sales_drops",
    "high_order_value_events",
    "unusual_discount_events",
    "unexpected_demand_events",
    "duplicate_transaction_events"
}


check(
    "All six Step 31 anomaly types are implemented",
    required_summary_columns.issubset(
        set(summary.columns)
    )
)


allowed_types = {
    "Sudden Sales Spike",
    "Sudden Sales Drop",
    "Abnormally High Order Value",
    "Unusual Discount",
    "Unexpected Demand",
    "Duplicate Transaction"
}


check(
    "Only supported anomaly types are produced",
    events.filter(
        ~F.col("anomaly_type").isin(
            list(allowed_types)
        )
    ).count() == 0
)


required_event_columns = {
    "anomaly_type",
    "event_date",
    "restaurant_id",
    "restaurant_name",
    "metric_name",
    "actual_value",
    "expected_value",
    "change_pct",
    "anomaly_score",
    "severity",
    "evidence"
}


check(
    "Event output contains required evidence fields",
    required_event_columns.issubset(
        set(events.columns)
    )
)


check(
    "Anomaly type is never null",
    events.filter(
        F.col("anomaly_type").isNull()
    ).count() == 0
)


check(
    "Event date is never null",
    events.filter(
        F.col("event_date").isNull()
    ).count() == 0
)


check(
    "Restaurant is never null",
    events.filter(
        F.col("restaurant_id").isNull()
    ).count() == 0
)


check(
    "Anomaly score is non-negative",
    events.filter(
        F.col("anomaly_score") < 0
    ).count() == 0
)


check(
    "Severity uses valid classes",
    events.filter(
        ~F.col("severity").isin(
            "Medium",
            "High",
            "Critical"
        )
    ).count() == 0
)


check(
    "Sales spikes are positive changes",
    events.filter(
        F.col("anomaly_type") ==
        "Sudden Sales Spike"
    ).filter(
        (
            F.col("change_pct") < 50
        ) |
        (
            F.col("anomaly_score") < 2.5
        )
    ).count() == 0
)


check(
    "Sales drops are negative changes",
    events.filter(
        F.col("anomaly_type") ==
        "Sudden Sales Drop"
    ).filter(
        (
            F.col("change_pct") > -50
        ) |
        (
            F.col("anomaly_score") < 2.5
        )
    ).count() == 0
)


check(
    "High order values exceed their threshold",
    events.filter(
        F.col("anomaly_type") ==
        "Abnormally High Order Value"
    ).filter(
        F.col("actual_value") <=
        F.col("expected_value")
    ).count() == 0
)


check(
    "Unusual discounts exceed threshold and 20 percent",
    events.filter(
        F.col("anomaly_type") ==
        "Unusual Discount"
    ).filter(
        (
            F.col("actual_value") <=
            F.col("expected_value")
        ) |
        (
            F.col("actual_value") < 20
        )
    ).count() == 0
)


check(
    "Unexpected demand has material deviation",
    events.filter(
        F.col("anomaly_type") ==
        "Unexpected Demand"
    ).filter(
        (
            F.abs(
                F.col("change_pct")
            ) < 50
        ) |
        (
            F.col("anomaly_score") < 2.5
        )
    ).count() == 0
)


check(
    "Duplicate transactions require multiple matching orders",
    events.filter(
        F.col("anomaly_type") ==
        "Duplicate Transaction"
    ).filter(
        F.col("actual_value") <= 1
    ).count() == 0
)


check(
    "Order-level anomalies include order IDs",
    events.filter(
        F.col("anomaly_type").isin(
            "Abnormally High Order Value",
            "Unusual Discount",
            "Duplicate Transaction"
        )
    ).filter(
        F.col("order_id").isNull()
    ).count() == 0
)


check(
    "Unexpected demand includes item IDs",
    events.filter(
        F.col("anomaly_type") ==
        "Unexpected Demand"
    ).filter(
        F.col("item_id").isNull()
    ).count() == 0
)


row = summary.first()


check(
    "Summary total matches event output",
    int(
        row["total_anomaly_events"]
    ) ==
    events.count()
)


summary_mapping = {
    "sudden_sales_spikes": "Sudden Sales Spike",
    "sudden_sales_drops": "Sudden Sales Drop",
    "high_order_value_events": "Abnormally High Order Value",
    "unusual_discount_events": "Unusual Discount",
    "unexpected_demand_events": "Unexpected Demand",
    "duplicate_transaction_events": "Duplicate Transaction"
}


for column, anomaly_type in summary_mapping.items():
    check(
        f"{column} matches event output",
        int(
            row[column]
        ) ==
        events.filter(
            F.col("anomaly_type") ==
            anomaly_type
        ).count()
    )


check(
    "Severity totals match event total",
    (
        int(row["critical_events"]) +
        int(row["high_events"]) +
        int(row["medium_events"])
    ) ==
    int(row["total_anomaly_events"])
)


total_checks = passed + failed


print(
    f"\n========VALIDATION RESULT: "
    f"{passed}/{total_checks} PASS========"
)


if failed == 0:
    print(
        "\nSales anomaly validation PASSED."
    )
else:
    print(
        "\nSales anomaly validation FAILED."
    )


spark.stop()
