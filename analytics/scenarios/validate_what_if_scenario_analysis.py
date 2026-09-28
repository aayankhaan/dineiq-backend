from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("Validate DineIQ What-If Scenario Analysis") \
    .getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")


output_folder = f"{ANALYTICS_DATA_FOLDER}/what_if_scenarios"


catalog = spark.read.parquet(
    f"{output_folder}/scenario_catalog"
)

scenarios = spark.read.parquet(
    f"{output_folder}/default_scenarios"
)

summary = spark.read.parquet(
    f"{output_folder}/scenario_summary"
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


print("\n========WHAT-IF SCENARIO VALIDATION========")


required_types = {
    "Increase Menu Price",
    "Reduce Item Price",
    "Change Discount Percentage",
    "Increase Promotion Frequency",
    "Remove Menu Item",
    "Reduce Preparation Quantity",
    "Increase Predicted Demand",
    "Change Wastage Assumptions"
}


check(
    "Scenario catalog contains data",
    catalog.count() > 0
)


check(
    "Default scenario output contains data",
    scenarios.count() > 0
)


check(
    "Scenario summary contains data",
    summary.count() > 0
)


catalog_types = {
    row["scenario_type"]
    for row in catalog.select(
        "scenario_type"
    ).distinct().collect()
}


scenario_types = {
    row["scenario_type"]
    for row in scenarios.select(
        "scenario_type"
    ).distinct().collect()
}


check(
    "All eight Step 40 scenario types exist in the catalog",
    required_types == catalog_types
)


check(
    "All eight Step 40 scenario types have a working default scenario",
    required_types == scenario_types
)


check(
    "Scenario IDs are unique",
    scenarios.select(
        "scenario_id"
    ).distinct().count() ==
    scenarios.count()
)


check(
    "Exactly one default scenario is produced for each scenario type",
    scenarios.count() == 8
)


check(
    "Scenario catalog marks every scenario as user adjustable",
    catalog.filter(
        ~F.col("user_adjustable")
    ).count() == 0
)


check(
    "Step 40 outputs are scenario inputs rather than completed impact calculations",
    scenarios.filter(
        F.col("impact_calculated")
    ).count() == 0
)


check(
    "Scenario notes clearly defer impact estimation to Step 41 where applicable",
    scenarios.filter(
        F.col("scenario_note").isNull() |
        (F.trim("scenario_note") == "")
    ).count() == 0
)


check(
    "Baseline and proposed values are populated",
    scenarios.filter(
        F.col("baseline_value").isNull() |
        F.col("proposed_value").isNull()
    ).count() == 0
)


check(
    "Increase menu price raises the selected price",
    scenarios.filter(
        F.col("scenario_type") ==
        "Increase Menu Price"
    ).filter(
        F.col("proposed_value") <=
        F.col("baseline_value")
    ).count() == 0
)


check(
    "Reduce item price lowers the selected price",
    scenarios.filter(
        F.col("scenario_type") ==
        "Reduce Item Price"
    ).filter(
        F.col("proposed_value") >=
        F.col("baseline_value")
    ).count() == 0
)


check(
    "Discount scenario stays between zero and 100 percent",
    scenarios.filter(
        F.col("scenario_type") ==
        "Change Discount Percentage"
    ).filter(
        (
            F.col("proposed_value") < 0
        ) |
        (
            F.col("proposed_value") > 100
        )
    ).count() == 0
)


check(
    "Promotion frequency scenario increases campaign-active days",
    scenarios.filter(
        F.col("scenario_type") ==
        "Increase Promotion Frequency"
    ).filter(
        F.col("proposed_value") <=
        F.col("baseline_value")
    ).count() == 0
)


remove_scenario = scenarios.filter(
    F.col("scenario_type") ==
    "Remove Menu Item"
)


check(
    "Remove menu item changes availability from active to unavailable",
    remove_scenario.filter(
        (
            F.col("baseline_value") != 1
        ) |
        (
            F.col("proposed_value") != 0
        )
    ).count() == 0
)


check(
    "Preparation scenario reduces preparation quantity",
    scenarios.filter(
        F.col("scenario_type") ==
        "Reduce Preparation Quantity"
    ).filter(
        F.col("proposed_value") >=
        F.col("baseline_value")
    ).count() == 0
)


check(
    "Predicted-demand scenario increases forecast demand",
    scenarios.filter(
        F.col("scenario_type") ==
        "Increase Predicted Demand"
    ).filter(
        F.col("proposed_value") <=
        F.col("baseline_value")
    ).count() == 0
)


check(
    "Wastage scenario changes the baseline wastage assumption",
    scenarios.filter(
        F.col("scenario_type") ==
        "Change Wastage Assumptions"
    ).filter(
        F.col("proposed_value") ==
        F.col("baseline_value")
    ).count() == 0
)


check(
    "Wastage assumptions stay between zero and 100 percent",
    scenarios.filter(
        F.col("scenario_type") ==
        "Change Wastage Assumptions"
    ).filter(
        (
            F.col("proposed_value") < 0
        ) |
        (
            F.col("proposed_value") > 100
        )
    ).count() == 0
)


check(
    "Price scenarios retain historical price-sensitivity evidence",
    scenarios.filter(
        F.col("scenario_type").isin(
            "Increase Menu Price",
            "Reduce Item Price"
        )
    ).filter(
        F.col("price_sensitivity_score").isNull() |
        F.col("price_elasticity").isNull()
    ).count() == 0
)


check(
    "Menu-item scenarios identify a real item",
    scenarios.filter(
        F.col("target_scope").isin(
            "Location Menu Item",
            "Menu Item Forecast"
        )
    ).filter(
        F.col("item_id").isNull() |
        F.col("item_name").isNull()
    ).count() == 0
)


check(
    "Location menu scenarios identify a restaurant",
    scenarios.filter(
        F.col("target_scope") ==
        "Location Menu Item"
    ).filter(
        F.col("restaurant_id").isNull() |
        F.col("restaurant_name").isNull()
    ).count() == 0
)


check(
    "Promotion scenarios identify a promotion",
    scenarios.filter(
        F.col("target_scope") ==
        "Promotion"
    ).filter(
        F.col("promotion_id").isNull() |
        F.col("promotion_name").isNull()
    ).count() == 0
)


check(
    "Source analysis is documented for every scenario",
    scenarios.filter(
        F.col("source_analysis").isNull() |
        (F.trim("source_analysis") == "")
    ).count() == 0
)


check(
    "Scenario catalog contains valid adjustable ranges",
    catalog.filter(
        F.col("minimum_change_value") >
        F.col("maximum_change_value")
    ).count() == 0
)


check(
    "Default changes fall inside their configured ranges",
    catalog.filter(
        (
            F.col("default_change_value") <
            F.col("minimum_change_value")
        ) |
        (
            F.col("default_change_value") >
            F.col("maximum_change_value")
        )
    ).count() == 0
)


check(
    "Scenario summary reconciles to eight default scenarios",
    int(
        summary.agg(
            F.sum(
                "scenario_count"
            ).alias("value")
        ).first()["value"]
    ) == 8
)


total = passed + failed


print(
    f"\n========VALIDATION RESULT: "
    f"{passed}/{total} PASS========"
)


if failed == 0:
    print(
        "\nWhat-if scenario validation PASSED."
    )
else:
    print(
        "\nWhat-if scenario validation FAILED."
    )


spark.stop()
