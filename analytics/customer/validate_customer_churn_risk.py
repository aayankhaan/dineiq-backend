from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("Validate DineIQ Customer Churn Risk") \
    .getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")


output_folder = f"{ANALYTICS_DATA_FOLDER}/churn_risk"


risk = spark.read.parquet(
    f"{output_folder}/customer_churn_risk"
)

summary = spark.read.parquet(
    f"{output_folder}/churn_risk_summary"
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


print("\n========CUSTOMER CHURN RISK VALIDATION========")


check(
    "Customer churn-risk output contains data",
    risk.count() > 0
)


check(
    "Churn-risk summary contains data",
    summary.count() > 0
)


check(
    "Customer IDs are unique",
    risk.select(
        "customer_id"
    ).distinct().count() ==
    risk.count()
)


required_factors = {
    "increasing_recency",
    "declining_frequency",
    "declining_monetary_value",
    "reduced_category_diversity",
    "lower_visit_frequency"
}


check(
    "All five Step 36 churn factors are implemented",
    required_factors.issubset(
        set(risk.columns)
    )
)


check(
    "Recency values are non-negative",
    risk.filter(
        F.col("days_since_last_order") < 0
    ).count() == 0
)


check(
    "Lifetime orders are positive",
    risk.filter(
        F.col("lifetime_orders") <= 0
    ).count() == 0
)


check(
    "Recent and previous frequencies are non-negative",
    risk.filter(
        (
            F.col("recent_frequency") < 0
        ) |
        (
            F.col("previous_frequency") < 0
        )
    ).count() == 0
)


check(
    "Recent and previous monetary values are non-negative",
    risk.filter(
        (
            F.col("recent_monetary_value") < 0
        ) |
        (
            F.col("previous_monetary_value") < 0
        )
    ).count() == 0
)


check(
    "Category diversity values are non-negative",
    risk.filter(
        (
            F.col("recent_category_diversity") < 0
        ) |
        (
            F.col("previous_category_diversity") < 0
        )
    ).count() == 0
)


check(
    "Visit-day values are non-negative",
    risk.filter(
        (
            F.col("recent_visit_days") < 0
        ) |
        (
            F.col("previous_visit_days") < 0
        )
    ).count() == 0
)


signal_expression = (
    F.col("increasing_recency").cast("int") +
    F.col("declining_frequency").cast("int") +
    F.col("declining_monetary_value").cast("int") +
    F.col("reduced_category_diversity").cast("int") +
    F.col("lower_visit_frequency").cast("int")
)


check(
    "Risk signal count matches the five factor flags",
    risk.filter(
        F.col("risk_signal_count") !=
        signal_expression
    ).count() == 0
)


check(
    "Customers without repeat history are marked insufficient history",
    risk.filter(
        F.col("lifetime_orders") < 2
    ).filter(
        F.col("churn_risk_level") !=
        "Insufficient History"
    ).count() == 0
)


check(
    "Eligible customers do not use insufficient-history label",
    risk.filter(
        F.col("risk_eligible")
    ).filter(
        F.col("churn_risk_level") ==
        "Insufficient History"
    ).count() == 0
)


check(
    "Critical risk requires at least four risk signals",
    risk.filter(
        F.col("churn_risk_level") ==
        "Critical"
    ).filter(
        F.col("risk_signal_count") < 4
    ).count() == 0
)


check(
    "High risk requires three signals or dormancy",
    risk.filter(
        F.col("churn_risk_level") ==
        "High"
    ).filter(
        ~(
            (
                F.col("risk_signal_count") == 3
            ) |
            F.col("dormant_customer")
        )
    ).count() == 0
)


check(
    "Medium risk requires exactly two signals",
    risk.filter(
        F.col("churn_risk_level") ==
        "Medium"
    ).filter(
        F.col("risk_signal_count") != 2
    ).count() == 0
)


check(
    "Low risk has fewer than two signals and is not dormant",
    risk.filter(
        F.col("churn_risk_level") ==
        "Low"
    ).filter(
        (
            F.col("risk_signal_count") >= 2
        ) |
        F.col("dormant_customer")
    ).count() == 0
)


check(
    "Churn-risk flag matches High and Critical levels",
    risk.filter(
        F.col("churn_risk_flag") !=
        F.col("churn_risk_level").isin(
            "High",
            "Critical"
        )
    ).count() == 0
)


check(
    "Declining frequency always reflects negative change",
    risk.filter(
        F.col("declining_frequency")
    ).filter(
        F.col("frequency_change_pct") >= 0
    ).count() == 0
)


check(
    "Declining monetary value always reflects negative change",
    risk.filter(
        F.col("declining_monetary_value")
    ).filter(
        F.col("monetary_change_pct") >= 0
    ).count() == 0
)


check(
    "Reduced category diversity always reflects negative change",
    risk.filter(
        F.col("reduced_category_diversity")
    ).filter(
        F.col("category_diversity_change_pct") >= 0
    ).count() == 0
)


check(
    "Lower visit frequency always reflects negative change",
    risk.filter(
        F.col("lower_visit_frequency")
    ).filter(
        F.col("visit_frequency_change_pct") >= 0
    ).count() == 0
)



check(
    "Declining frequency requires at least two previous-period orders",
    risk.filter(
        F.col("declining_frequency")
    ).filter(
        F.col("previous_frequency") < 2
    ).count() == 0
)


check(
    "Declining monetary value requires at least two previous-period orders",
    risk.filter(
        F.col("declining_monetary_value")
    ).filter(
        F.col("previous_frequency") < 2
    ).count() == 0
)


check(
    "Reduced category diversity requires at least two previous categories",
    risk.filter(
        F.col("reduced_category_diversity")
    ).filter(
        F.col("previous_category_diversity") < 2
    ).count() == 0
)


check(
    "Lower visit frequency requires at least two previous visit days",
    risk.filter(
        F.col("lower_visit_frequency")
    ).filter(
        F.col("previous_visit_days") < 2
    ).count() == 0
)


check(
    "Dormant customers have no recent completed orders",
    risk.filter(
        F.col("dormant_customer")
    ).filter(
        F.col("recent_frequency") != 0
    ).count() == 0
)


check(
    "Risk reasons are populated",
    risk.filter(
        F.col("risk_reason").isNull() |
        (F.trim(F.col("risk_reason")) == "")
    ).count() == 0
)


summary_count = summary.agg(
    F.sum(
        "customer_count"
    ).alias("value")
).first()["value"]


check(
    "Summary customer count reconciles",
    int(summary_count) ==
    risk.count()
)


check(
    "Summary percentages total approximately 100",
    abs(
        float(
            summary.agg(
                F.sum(
                    "customer_percentage"
                ).alias("value")
            ).first()["value"]
        ) - 100.0
    ) <= 0.05
)


check(
    "At least one High or Critical churn-risk customer is identified",
    risk.filter(
        F.col("churn_risk_flag")
    ).count() > 0
)


total = passed + failed


print(
    f"\n========VALIDATION RESULT: "
    f"{passed}/{total} PASS========"
)


if failed == 0:
    print(
        "\nCustomer churn-risk validation PASSED."
    )
else:
    print(
        "\nCustomer churn-risk validation FAILED."
    )


spark.stop()
