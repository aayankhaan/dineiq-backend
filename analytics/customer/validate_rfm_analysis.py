from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("Validate DineIQ RFM Analysis") \
    .getOrCreate()


output_folder = f"{ANALYTICS_DATA_FOLDER}/rfm_analysis"

rfm = spark.read.parquet(f"{output_folder}/customer_rfm")
summary = spark.read.parquet(f"{output_folder}/rfm_summary")
segment_summary = spark.read.parquet(f"{output_folder}/rfm_segment_summary")


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


check(
    "Customer RFM output contains data",
    rfm.count() > 0
)

check(
    "RFM summary contains data",
    summary.count() > 0
)

check(
    "RFM segment summary contains data",
    segment_summary.count() > 0
)

required_columns = [
    "customer_id",
    "recency",
    "frequency",
    "monetary_value",
    "customer_segment",
    "recency_score",
    "frequency_score",
    "monetary_score",
    "rfm_score",
    "rfm_code",
    "rfm_value_group"
]


for column in required_columns:
    check(
        f"Required column exists: {column}",
        column in rfm.columns
    )



check(
    "Customer IDs are unique",
    rfm.select("customer_id").distinct().count() == rfm.count()
)

check(
    "Customer IDs are not null",
    rfm.filter(F.col("customer_id").isNull()).count() == 0
)

check(
    "RFM values are not null",
    rfm.filter(
        F.col("recency").isNull() |
        F.col("frequency").isNull() |
        F.col("monetary_value").isNull()
    ).count() == 0
)

check(
    "RFM scores are not null",
    rfm.filter(
        F.col("recency_score").isNull() |
        F.col("frequency_score").isNull() |
        F.col("monetary_score").isNull() |
        F.col("rfm_score").isNull() |
        F.col("rfm_code").isNull()
    ).count() == 0
)


check(
    "Recency values are non-negative",
    rfm.filter(F.col("recency") < 0).count() == 0
)

check(
    "Frequency values are positive",
    rfm.filter(F.col("frequency") <= 0).count() == 0
)

check(
    "Monetary values are non-negative",
    rfm.filter(F.col("monetary_value") < 0).count() == 0
)



for score_column in [
    "recency_score",
    "frequency_score",
    "monetary_score"
]:
    check(
        f"{score_column} is between 1 and 5",
        rfm.filter(
            (F.col(score_column) < 1) |
            (F.col(score_column) > 5)
        ).count() == 0
    )


check(
    "RFM total score is between 3 and 15",
    rfm.filter(
        (F.col("rfm_score") < 3) |
        (F.col("rfm_score") > 15)
    ).count() == 0
)

check(
    "RFM total equals R + F + M scores",
    rfm.filter(
        F.col("rfm_score") != (
            F.col("recency_score") +
            F.col("frequency_score") +
            F.col("monetary_score")
        )
    ).count() == 0
)

check(
    "RFM code contains exactly three digits",
    rfm.filter(
        ~F.col("rfm_code").rlike("^[1-5]{3}$")
    ).count() == 0
)


expected_groups = {
    "Top Value",
    "High Value",
    "Medium Value",
    "Low Value"
}


actual_groups = {
    row["rfm_value_group"]
    for row in rfm.select("rfm_value_group").distinct().collect()
}


check(
    "All four RFM value groups exist",
    actual_groups == expected_groups
)

check(
    "Top Value customers have scores from 13 to 15",
    rfm.filter(
        (F.col("rfm_value_group") == "Top Value") &
        (F.col("rfm_score") < 13)
    ).count() == 0
)

check(
    "High Value customers have scores from 10 to 12",
    rfm.filter(
        (F.col("rfm_value_group") == "High Value") &
        (
            (F.col("rfm_score") < 10) |
            (F.col("rfm_score") > 12)
        )
    ).count() == 0
)

check(
    "Medium Value customers have scores from 7 to 9",
    rfm.filter(
        (F.col("rfm_value_group") == "Medium Value") &
        (
            (F.col("rfm_score") < 7) |
            (F.col("rfm_score") > 9)
        )
    ).count() == 0
)

check(
    "Low Value customers have scores from 3 to 6",
    rfm.filter(
        (F.col("rfm_value_group") == "Low Value") &
        (F.col("rfm_score") > 6)
    ).count() == 0
)


recency_direction_errors = rfm.alias("a").join(
    rfm.alias("b"),
    (
        (F.col("a.recency") < F.col("b.recency")) &
        (F.col("a.recency_score") < F.col("b.recency_score"))
    ),
    "inner"
).limit(1).count()


check(
    "Better recency does not receive a lower recency score",
    recency_direction_errors == 0
)


monetary_direction_errors = rfm.alias("a").join(
    rfm.alias("b"),
    (
        (F.col("a.monetary_value") < F.col("b.monetary_value")) &
        (F.col("a.monetary_score") > F.col("b.monetary_score"))
    ),
    "inner"
).limit(1).count()


check(
    "Higher monetary value does not receive a lower monetary score",
    monetary_direction_errors == 0
)


check(
    "RFM summary contains four value groups",
    summary.count() == 4
)

check(
    "RFM summary customer counts equal total customers",
    summary.agg(
        F.sum("customer_count").alias("total")
    ).first()["total"] == rfm.count()
)

check(
    "RFM segment summary contains all six customer segments",
    segment_summary.select(
        "customer_segment"
    ).distinct().count() == 6
)

check(
    "RFM segment summary customer counts equal total customers",
    segment_summary.agg(
        F.sum("customer_count").alias("total")
    ).first()["total"] == rfm.count()
)


total_checks = passed + failed


print("\n========RFM ANALYSIS VALIDATION========")
print(f"Passed: {passed}/{total_checks}")
print(f"Failed: {failed}/{total_checks}")


if failed == 0:
    print("\nRFM analysis validation PASSED.")
else:
    print("\nRFM analysis validation FAILED.")


spark.stop()
