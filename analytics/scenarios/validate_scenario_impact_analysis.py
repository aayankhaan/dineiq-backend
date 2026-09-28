from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("Validate DineIQ Scenario Impact Analysis") \
    .getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")


scenarios = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/what_if_scenarios/default_scenarios"
)

impacts = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/scenario_impact/scenario_impacts"
)

summary = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/scenario_impact/scenario_impact_summary"
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


print("\n========SCENARIO IMPACT VALIDATION========")


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
    "Scenario impact output contains data",
    impacts.count() > 0
)


check(
    "Scenario impact summary contains data",
    summary.count() > 0
)


check(
    "Every Step 40 default scenario has a Step 41 impact estimate",
    impacts.count() ==
    scenarios.count()
)


check(
    "Scenario IDs remain unique",
    impacts.select(
        "scenario_id"
    ).distinct().count() ==
    impacts.count()
)


impact_types = {
    row["scenario_type"]
    for row in impacts.select(
        "scenario_type"
    ).distinct().collect()
}


check(
    "All eight Step 40 scenario types have impact estimates",
    impact_types == required_types
)


required_columns = {
    "baseline_revenue",
    "estimated_revenue",
    "revenue_change_pct",
    "baseline_contribution_margin",
    "estimated_contribution_margin",
    "contribution_margin_change_pct",
    "baseline_demand",
    "estimated_demand",
    "demand_change_pct",
    "baseline_wastage_pct",
    "estimated_wastage_pct",
    "wastage_change_percentage_points",
    "baseline_profitability_pct",
    "estimated_profitability_pct",
    "profitability_change_percentage_points",
    "simulation_status",
    "is_estimate",
    "estimate_method",
    "impact_statement"
}


check(
    "All required Step 41 impact fields exist",
    required_columns.issubset(
        set(impacts.columns)
    )
)


check(
    "Simulated outputs are clearly marked as estimates",
    impacts.filter(
        F.col("simulation_status") !=
        "Estimated - Not Actual"
    ).count() == 0
)


check(
    "Every scenario is marked as an estimate",
    impacts.filter(
        ~F.col("is_estimate")
    ).count() == 0
)


check(
    "Estimate methods are documented",
    impacts.filter(
        F.col("estimate_method").isNull() |
        (F.trim("estimate_method") == "")
    ).count() == 0
)


check(
    "Impact statements are populated",
    impacts.filter(
        F.col("impact_statement").isNull() |
        (F.trim("impact_statement") == "")
    ).count() == 0
)


check(
    "Revenue baselines and estimates are non-negative",
    impacts.filter(
        (F.col("baseline_revenue") < 0) |
        (F.col("estimated_revenue") < 0)
    ).count() == 0
)


check(
    "Demand baselines and estimates are non-negative",
    impacts.filter(
        (F.col("baseline_demand") < 0) |
        (F.col("estimated_demand") < 0)
    ).count() == 0
)


check(
    "Wastage rates stay between zero and 100 percent",
    impacts.filter(
        (F.col("baseline_wastage_pct") < 0) |
        (F.col("baseline_wastage_pct") > 100) |
        (F.col("estimated_wastage_pct") < 0) |
        (F.col("estimated_wastage_pct") > 100)
    ).count() == 0
)


check(
    "Wastage quantities are non-negative",
    impacts.filter(
        (F.col("baseline_wastage_units") < 0) |
        (F.col("estimated_wastage_units") < 0)
    ).count() == 0
)


check(
    "Revenue changes reconcile",
    impacts.filter(
        F.abs(
            (
                F.col("estimated_revenue") -
                F.col("baseline_revenue")
            ) -
            F.col("revenue_change")
        ) > 0.02
    ).count() == 0
)


check(
    "Contribution margin changes reconcile",
    impacts.filter(
        F.abs(
            (
                F.col("estimated_contribution_margin") -
                F.col("baseline_contribution_margin")
            ) -
            F.col("contribution_margin_change")
        ) > 0.02
    ).count() == 0
)


check(
    "Demand changes reconcile",
    impacts.filter(
        F.abs(
            (
                F.col("estimated_demand") -
                F.col("baseline_demand")
            ) -
            F.col("demand_change")
        ) > 0.02
    ).count() == 0
)


check(
    "Wastage-rate changes reconcile",
    impacts.filter(
        F.abs(
            (
                F.col("estimated_wastage_pct") -
                F.col("baseline_wastage_pct")
            ) -
            F.col("wastage_change_percentage_points")
        ) > 0.02
    ).count() == 0
)


check(
    "Profitability changes reconcile",
    impacts.filter(
        F.abs(
            (
                F.col("estimated_profitability_pct") -
                F.col("baseline_profitability_pct")
            ) -
            F.col("profitability_change_percentage_points")
        ) > 0.02
    ).count() == 0
)


price_increase = impacts.filter(
    F.col("scenario_type") ==
    "Increase Menu Price"
)


check(
    "Menu price increase models lower or unchanged demand",
    price_increase.filter(
        F.col("estimated_demand") >
        F.col("baseline_demand")
    ).count() == 0
)


check(
    "Menu price increase retains price elasticity evidence",
    price_increase.filter(
        F.col("price_elasticity").isNull()
    ).count() == 0
)


price_reduction = impacts.filter(
    F.col("scenario_type") ==
    "Reduce Item Price"
)


check(
    "Item price reduction models higher or unchanged demand",
    price_reduction.filter(
        F.col("estimated_demand") <
        F.col("baseline_demand")
    ).count() == 0
)


check(
    "Item price reduction retains price elasticity evidence",
    price_reduction.filter(
        F.col("price_elasticity").isNull()
    ).count() == 0
)


discount = impacts.filter(
    F.col("scenario_type") ==
    "Change Discount Percentage"
)


check(
    "Discount scenario retains historical promotion demand evidence",
    discount.filter(
        F.col("historical_demand_lift_pct").isNull()
    ).count() == 0
)


promotion_frequency = impacts.filter(
    F.col("scenario_type") ==
    "Increase Promotion Frequency"
)


check(
    "Higher promotion frequency increases estimated promotion demand",
    promotion_frequency.filter(
        F.col("estimated_demand") <=
        F.col("baseline_demand")
    ).count() == 0
)


remove_item = impacts.filter(
    F.col("scenario_type") ==
    "Remove Menu Item"
)


check(
    "Menu removal sets direct demand to zero",
    remove_item.filter(
        F.col("estimated_demand") != 0
    ).count() == 0
)


check(
    "Menu removal sets direct revenue to zero",
    remove_item.filter(
        F.col("estimated_revenue") != 0
    ).count() == 0
)


check(
    "Menu removal sets direct contribution margin to zero",
    remove_item.filter(
        F.col("estimated_contribution_margin") != 0
    ).count() == 0
)


check(
    "Menu removal sets direct wastage to zero",
    remove_item.filter(
        (F.col("estimated_wastage_units") != 0) |
        (F.col("estimated_wastage_pct") != 0)
    ).count() == 0
)


preparation = impacts.filter(
    F.col("scenario_type") ==
    "Reduce Preparation Quantity"
)


check(
    "Reduced preparation does not increase estimated wastage",
    preparation.filter(
        F.col("estimated_wastage_units") >
        F.col("baseline_wastage_units")
    ).count() == 0
)


forecast = impacts.filter(
    F.col("scenario_type") ==
    "Increase Predicted Demand"
)


check(
    "Increased predicted demand raises estimated demand",
    forecast.filter(
        F.col("estimated_demand") <=
        F.col("baseline_demand")
    ).count() == 0
)


check(
    "Increased predicted demand raises estimated revenue",
    forecast.filter(
        F.col("estimated_revenue") <=
        F.col("baseline_revenue")
    ).count() == 0
)


wastage = impacts.filter(
    F.col("scenario_type") ==
    "Change Wastage Assumptions"
)


check(
    "Wastage scenario uses the proposed Step 40 wastage rate",
    wastage.join(
        scenarios.select(
            "scenario_id",
            F.col("proposed_value").alias("step40_proposed_value")
        ),
        "scenario_id",
        "inner"
    ).filter(
        F.abs(
            F.col("estimated_wastage_pct") -
            F.col("step40_proposed_value")
        ) > 0.02
    ).count() == 0
)


check(
    "Reduced default wastage assumption reduces estimated wastage",
    wastage.filter(
        F.col("estimated_wastage_units") >=
        F.col("baseline_wastage_units")
    ).count() == 0
)


summary_total = summary.agg(
    F.sum(
        "scenario_count"
    ).alias("value")
).first()["value"]


check(
    "Scenario impact summary reconciles to impact rows",
    int(summary_total) ==
    impacts.count()
)


check(
    "Scenario impact summary contains all eight scenario types",
    summary.select(
        "scenario_type"
    ).distinct().count() == 8
)


total = passed + failed


print(
    f"\n========VALIDATION RESULT: "
    f"{passed}/{total} PASS========"
)


if failed == 0:
    print(
        "\nScenario impact validation PASSED."
    )
else:
    print(
        "\nScenario impact validation FAILED."
    )


spark.stop()
