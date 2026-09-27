from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("Validate DineIQ Market Basket Analysis") \
    .getOrCreate()


output_folder = f"{ANALYTICS_DATA_FOLDER}/market_basket"

pairs = spark.read.parquet(f"{output_folder}/frequent_item_pairs")
rules = spark.read.parquet(f"{output_folder}/association_rules")
suitable_rules = spark.read.parquet(f"{output_folder}/suitable_rules")
summary = spark.read.parquet(f"{output_folder}/market_basket_summary")


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


# ==================== OUTPUT VALIDATION ====================


check("Frequent item pairs contain data", pairs.count() > 0)
check("Association rules contain data", rules.count() > 0)
check("Suitable association rules contain data", suitable_rules.count() > 0)
check("Market basket summary contains one row", summary.count() == 1)


# ==================== PAIR VALIDATION ====================


pair_columns = [
    "item_a_id",
    "item_a_name",
    "item_b_id",
    "item_b_name",
    "pair_order_count",
    "support"
]


for column in pair_columns:
    check(
        f"Required pair column exists: {column}",
        column in pairs.columns
    )


check(
    "Item pairs never contain the same item twice",
    pairs.filter(F.col("item_a_id") == F.col("item_b_id")).count() == 0
)

check(
    "Item pair order counts are positive",
    pairs.filter(F.col("pair_order_count") <= 0).count() == 0
)

check(
    "Pair support is greater than 0 and at most 1",
    pairs.filter(
        (F.col("support") <= 0) |
        (F.col("support") > 1)
    ).count() == 0
)


# ==================== ASSOCIATION RULE VALIDATION ====================


rule_columns = [
    "antecedent_item_id",
    "antecedent_item_name",
    "consequent_item_id",
    "consequent_item_name",
    "pair_order_count",
    "support",
    "antecedent_order_count",
    "consequent_order_count",
    "confidence",
    "consequent_support",
    "lift"
]


for column in rule_columns:
    check(
        f"Required rule column exists: {column}",
        column in rules.columns
    )


check(
    "Association rules never recommend the same item",
    rules.filter(
        F.col("antecedent_item_id") == F.col("consequent_item_id")
    ).count() == 0
)

check(
    "Association-rule support is valid",
    rules.filter(
        (F.col("support") <= 0) |
        (F.col("support") > 1)
    ).count() == 0
)

check(
    "Association-rule confidence is valid",
    rules.filter(
        (F.col("confidence") <= 0) |
        (F.col("confidence") > 1)
    ).count() == 0
)

check(
    "Association-rule lift is positive",
    rules.filter(F.col("lift") <= 0).count() == 0
)

check(
    "Association-rule metrics contain no null values",
    rules.filter(
        F.col("support").isNull() |
        F.col("confidence").isNull() |
        F.col("lift").isNull()
    ).count() == 0
)


# ==================== METRIC CALCULATION VALIDATION ====================


support_errors = rules.filter(
    F.abs(
        F.col("support") -
        (
            F.col("pair_order_count") /
            F.lit(summary.first()["total_orders"])
        )
    ) > 0.0000001
).count()


check(
    "Support calculation is correct",
    support_errors == 0
)


confidence_errors = rules.filter(
    F.abs(
        F.col("confidence") -
        (
            F.col("pair_order_count") /
            F.col("antecedent_order_count")
        )
    ) > 0.0000001
).count()


check(
    "Confidence calculation is correct",
    confidence_errors == 0
)


lift_errors = rules.filter(
    F.abs(
        F.col("lift") -
        (
            F.col("confidence") /
            F.col("consequent_support")
        )
    ) > 0.0000001
).count()


check(
    "Lift calculation is correct",
    lift_errors == 0
)


# ==================== SUITABLE RULE VALIDATION ====================


check(
    "Suitable rules meet minimum pair-order threshold",
    suitable_rules.filter(
        F.col("pair_order_count") < 20
    ).count() == 0
)

check(
    "Suitable rules meet minimum support threshold",
    suitable_rules.filter(
        F.col("support") < 0.0002
    ).count() == 0
)

check(
    "Suitable rules meet minimum confidence threshold",
    suitable_rules.filter(
        F.col("confidence") < 0.01
    ).count() == 0
)

check(
    "Suitable rules contain association-strength labels",
    suitable_rules.filter(
        F.col("association_strength").isNull()
    ).count() == 0
)


# ==================== SUMMARY VALIDATION ====================


summary_row = summary.first()


check(
    "Summary reports positive total orders",
    summary_row["total_orders"] > 0
)

check(
    "Summary reports positive unique items",
    summary_row["unique_items"] > 0
)

check(
    "Summary pair count matches output",
    summary_row["unique_item_pairs"] == pairs.count()
)

check(
    "Summary suitable-rule count matches output",
    summary_row["suitable_association_rules"] == suitable_rules.count()
)


# ==================== FINAL RESULT ====================


total_checks = passed + failed


print("\n========MARKET BASKET VALIDATION========")
print(f"Passed: {passed}/{total_checks}")
print(f"Failed: {failed}/{total_checks}")


if failed == 0:
    print("\nMarket-basket analysis validation PASSED.")
else:
    print("\nMarket-basket analysis validation FAILED.")


spark.stop()
