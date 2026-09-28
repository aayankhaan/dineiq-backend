from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("DineIQ Scenario Impact Analysis") \
    .getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "8")


scenarios = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/what_if_scenarios/default_scenarios"
)

location_menu = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/location_menu_performance/location_menu_performance"
)

promotion_effectiveness = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/promotion_effectiveness/promotion_effectiveness"
)

output_folder = f"{ANALYTICS_DATA_FOLDER}/scenario_impact"


location_context = location_menu.select(
    "restaurant_id",
    "item_id",
    F.col("wastage_percentage").cast("double").alias("context_wastage_pct"),
    F.col("estimated_preparation_quantity").cast("double").alias("context_preparation_quantity")
)


promotion_context = promotion_effectiveness.select(
    "promotion_id",
    F.coalesce(
        F.col("demand_lift_pct").cast("double"),
        F.lit(0.0)
    ).alias("historical_demand_lift_pct")
)


base = scenarios.join(
    location_context,
    [
        "restaurant_id",
        "item_id"
    ],
    "left"
).join(
    promotion_context,
    "promotion_id",
    "left"
).withColumn(
    "effective_baseline_wastage_pct",
    F.coalesce(
        F.col("baseline_wastage_pct"),
        F.col("context_wastage_pct"),
        F.lit(0.0)
    )
).withColumn(
    "effective_baseline_preparation_quantity",
    F.coalesce(
        F.col("baseline_preparation_quantity"),
        F.col("context_preparation_quantity")
    )
).withColumn(
    "baseline_demand",
    F.when(
        F.col("scenario_type") ==
        "Increase Predicted Demand",
        F.col("baseline_forecast_demand")
    ).otherwise(
        F.col("baseline_quantity")
    ).cast("double")
).withColumn(
    "historical_unit_revenue",
    F.when(
        F.col("baseline_quantity") > 0,
        F.col("baseline_revenue") /
        F.col("baseline_quantity")
    )
).withColumn(
    "historical_unit_margin",
    F.when(
        F.col("baseline_quantity") > 0,
        F.col("baseline_contribution_margin") /
        F.col("baseline_quantity")
    )
).withColumn(
    "simulation_baseline_revenue",
    F.when(
        F.col("scenario_type") ==
        "Increase Predicted Demand",
        F.col("baseline_demand") *
        F.col("historical_unit_revenue")
    ).otherwise(
        F.col("baseline_revenue")
    )
).withColumn(
    "simulation_baseline_contribution_margin",
    F.when(
        F.col("scenario_type") ==
        "Increase Predicted Demand",
        F.col("baseline_demand") *
        F.col("historical_unit_margin")
    ).otherwise(
        F.col("baseline_contribution_margin")
    )
).withColumn(
    "baseline_unit_cost",
    F.when(
        F.col("baseline_demand") > 0,
        (
            F.col("simulation_baseline_revenue") -
            F.col("simulation_baseline_contribution_margin")
        ) /
        F.col("baseline_demand")
    )
).withColumn(
    "baseline_wastage_units",
    F.when(
        F.col("scenario_type") ==
        "Increase Predicted Demand",
        F.when(
            F.col("effective_baseline_wastage_pct") < 100,
            F.col("baseline_demand") *
            (
                F.col("effective_baseline_wastage_pct") /
                (
                    F.lit(100.0) -
                    F.col("effective_baseline_wastage_pct")
                )
            )
        ).otherwise(
            F.col("baseline_demand")
        )
    ).when(
        F.col("effective_baseline_preparation_quantity").isNotNull(),
        F.col("effective_baseline_preparation_quantity") *
        F.col("effective_baseline_wastage_pct") /
        100.0
    ).otherwise(
        F.lit(0.0)
    )
)


price_demand_multiplier = F.least(
    F.lit(2.0),
    F.greatest(
        F.lit(0.0),
        F.lit(1.0) -
        (
            F.coalesce(
                F.col("price_elasticity"),
                F.lit(0.0)
            ) *
            F.coalesce(
                F.col("change_pct"),
                F.lit(0.0)
            ) /
            100.0
        )
    )
)


discount_relative_change = F.when(
    F.col("baseline_discount_percentage") > 0,
    (
        F.col("proposed_value") -
        F.col("baseline_discount_percentage")
    ) /
    F.col("baseline_discount_percentage")
).otherwise(
    F.lit(0.0)
)


discount_demand_multiplier = F.least(
    F.lit(2.0),
    F.greatest(
        F.lit(0.0),
        F.lit(1.0) +
        (
            F.coalesce(
                F.col("historical_demand_lift_pct"),
                F.lit(0.0)
            ) /
            100.0
        ) *
        discount_relative_change
    )
)


discount_net_price_multiplier = F.when(
    F.col("baseline_discount_percentage") < 100,
    (
        F.lit(1.0) -
        F.col("proposed_value") /
        100.0
    ) /
    (
        F.lit(1.0) -
        F.col("baseline_discount_percentage") /
        100.0
    )
).otherwise(
    F.lit(1.0)
)


frequency_multiplier = F.when(
    F.col("baseline_promotion_days") > 0,
    F.col("proposed_value") /
    F.col("baseline_promotion_days")
).otherwise(
    F.lit(1.0)
)


estimated = base.withColumn(
    "estimated_demand",
    F.when(
        F.col("scenario_type").isin(
            "Increase Menu Price",
            "Reduce Item Price"
        ),
        F.col("baseline_demand") *
        price_demand_multiplier
    ).when(
        F.col("scenario_type") ==
        "Change Discount Percentage",
        F.col("baseline_demand") *
        discount_demand_multiplier
    ).when(
        F.col("scenario_type") ==
        "Increase Promotion Frequency",
        F.col("baseline_demand") *
        frequency_multiplier
    ).when(
        F.col("scenario_type") ==
        "Remove Menu Item",
        F.lit(0.0)
    ).when(
        F.col("scenario_type") ==
        "Reduce Preparation Quantity",
        F.least(
            F.col("baseline_demand"),
            F.col("proposed_value")
        )
    ).when(
        F.col("scenario_type") ==
        "Increase Predicted Demand",
        F.col("proposed_value")
    ).otherwise(
        F.col("baseline_demand")
    )
).withColumn(
    "estimated_revenue",
    F.when(
        F.col("scenario_type").isin(
            "Increase Menu Price",
            "Reduce Item Price"
        ),
        F.col("simulation_baseline_revenue") *
        F.when(
            F.col("baseline_demand") > 0,
            F.col("estimated_demand") /
            F.col("baseline_demand")
        ).otherwise(
            F.lit(0.0)
        ) *
        F.when(
            F.col("baseline_price") > 0,
            F.col("proposed_value") /
            F.col("baseline_price")
        ).otherwise(
            F.lit(1.0)
        )
    ).when(
        F.col("scenario_type") ==
        "Change Discount Percentage",
        F.col("simulation_baseline_revenue") *
        F.when(
            F.col("baseline_demand") > 0,
            F.col("estimated_demand") /
            F.col("baseline_demand")
        ).otherwise(
            F.lit(0.0)
        ) *
        discount_net_price_multiplier
    ).when(
        F.col("scenario_type") ==
        "Increase Promotion Frequency",
        F.col("simulation_baseline_revenue") *
        frequency_multiplier
    ).when(
        F.col("scenario_type") ==
        "Remove Menu Item",
        F.lit(0.0)
    ).when(
        F.col("scenario_type") ==
        "Reduce Preparation Quantity",
        F.col("simulation_baseline_revenue") *
        F.when(
            F.col("baseline_demand") > 0,
            F.col("estimated_demand") /
            F.col("baseline_demand")
        ).otherwise(
            F.lit(0.0)
        )
    ).when(
        F.col("scenario_type") ==
        "Increase Predicted Demand",
        F.col("estimated_demand") *
        F.col("historical_unit_revenue")
    ).otherwise(
        F.col("simulation_baseline_revenue")
    )
).withColumn(
    "estimated_contribution_margin",
    F.when(
        F.col("scenario_type").isin(
            "Increase Menu Price",
            "Reduce Item Price",
            "Change Discount Percentage"
        ),
        F.col("estimated_revenue") -
        (
            F.coalesce(
                F.col("baseline_unit_cost"),
                F.lit(0.0)
            ) *
            F.col("estimated_demand")
        )
    ).when(
        F.col("scenario_type") ==
        "Increase Promotion Frequency",
        F.col("simulation_baseline_contribution_margin") *
        frequency_multiplier
    ).when(
        F.col("scenario_type") ==
        "Remove Menu Item",
        F.lit(0.0)
    ).when(
        F.col("scenario_type") ==
        "Reduce Preparation Quantity",
        F.col("simulation_baseline_contribution_margin") *
        F.when(
            F.col("baseline_demand") > 0,
            F.col("estimated_demand") /
            F.col("baseline_demand")
        ).otherwise(
            F.lit(0.0)
        )
    ).when(
        F.col("scenario_type") ==
        "Increase Predicted Demand",
        F.col("estimated_demand") *
        F.col("historical_unit_margin")
    ).otherwise(
        F.col("simulation_baseline_contribution_margin")
    )
)


menu_price_waste = F.when(
    F.col("effective_baseline_preparation_quantity") > 0,
    F.least(
        F.col("effective_baseline_preparation_quantity"),
        F.greatest(
            F.lit(0.0),
            F.col("baseline_wastage_units") -
            (
                F.col("estimated_demand") -
                F.col("baseline_demand")
            )
        )
    )
).otherwise(
    F.col("baseline_wastage_units")
)


prep_waste = F.when(
    F.col("proposed_value") > 0,
    F.least(
        F.col("proposed_value"),
        F.greatest(
            F.lit(0.0),
            F.col("baseline_wastage_units") -
            (
                F.col("effective_baseline_preparation_quantity") -
                F.col("proposed_value")
            )
        )
    )
).otherwise(
    F.lit(0.0)
)


forecast_waste = F.when(
    F.col("effective_baseline_wastage_pct") < 100,
    F.col("estimated_demand") *
    (
        F.col("effective_baseline_wastage_pct") /
        (
            F.lit(100.0) -
            F.col("effective_baseline_wastage_pct")
        )
    )
).otherwise(
    F.col("estimated_demand")
)


estimated = estimated.withColumn(
    "estimated_wastage_units",
    F.when(
        F.col("scenario_type").isin(
            "Increase Menu Price",
            "Reduce Item Price"
        ),
        menu_price_waste
    ).when(
        F.col("scenario_type") ==
        "Remove Menu Item",
        F.lit(0.0)
    ).when(
        F.col("scenario_type") ==
        "Reduce Preparation Quantity",
        prep_waste
    ).when(
        F.col("scenario_type") ==
        "Increase Predicted Demand",
        forecast_waste
    ).when(
        F.col("scenario_type") ==
        "Change Wastage Assumptions",
        F.when(
            F.col("effective_baseline_preparation_quantity") > 0,
            F.col("effective_baseline_preparation_quantity") *
            F.col("proposed_value") /
            100.0
        ).otherwise(
            F.lit(0.0)
        )
    ).otherwise(
        F.col("baseline_wastage_units")
    )
).withColumn(
    "estimated_wastage_pct",
    F.when(
        F.col("scenario_type") ==
        "Remove Menu Item",
        F.lit(0.0)
    ).when(
        F.col("scenario_type") ==
        "Reduce Preparation Quantity",
        F.when(
            F.col("proposed_value") > 0,
            F.least(
                F.lit(100.0),
                F.greatest(
                    F.lit(0.0),
                    (
                        F.col("estimated_wastage_units") /
                        F.col("proposed_value")
                    ) * 100.0
                )
            )
        ).otherwise(
            F.lit(0.0)
        )
    ).when(
        F.col("scenario_type") ==
        "Increase Predicted Demand",
        F.col("effective_baseline_wastage_pct")
    ).when(
        F.col("scenario_type") ==
        "Change Wastage Assumptions",
        F.col("proposed_value")
    ).when(
        F.col("scenario_type").isin(
            "Increase Menu Price",
            "Reduce Item Price"
        ),
        F.when(
            F.col("effective_baseline_preparation_quantity") > 0,
            F.least(
                F.lit(100.0),
                F.greatest(
                    F.lit(0.0),
                    (
                        F.col("estimated_wastage_units") /
                        F.col("effective_baseline_preparation_quantity")
                    ) * 100.0
                )
            )
        ).otherwise(
            F.col("effective_baseline_wastage_pct")
        )
    ).otherwise(
        F.col("effective_baseline_wastage_pct")
    )
).withColumn(
    "simulation_baseline_profitability_pct",
    F.when(
        F.col("simulation_baseline_revenue") > 0,
        (
            F.col("simulation_baseline_contribution_margin") /
            F.col("simulation_baseline_revenue")
        ) * 100.0
    ).otherwise(
        F.lit(0.0)
    )
).withColumn(
    "estimated_profitability_pct",
    F.when(
        F.col("estimated_revenue") > 0,
        (
            F.col("estimated_contribution_margin") /
            F.col("estimated_revenue")
        ) * 100.0
    ).otherwise(
        F.lit(0.0)
    )
)


impacts = estimated.withColumn(
    "revenue_change",
    F.col("estimated_revenue") -
    F.col("simulation_baseline_revenue")
).withColumn(
    "revenue_change_pct",
    F.when(
        F.col("simulation_baseline_revenue") != 0,
        (
            F.col("revenue_change") /
            F.abs(
                F.col("simulation_baseline_revenue")
            )
        ) * 100.0
    )
).withColumn(
    "contribution_margin_change",
    F.col("estimated_contribution_margin") -
    F.col("simulation_baseline_contribution_margin")
).withColumn(
    "contribution_margin_change_pct",
    F.when(
        F.col("simulation_baseline_contribution_margin") != 0,
        (
            F.col("contribution_margin_change") /
            F.abs(
                F.col("simulation_baseline_contribution_margin")
            )
        ) * 100.0
    )
).withColumn(
    "demand_change",
    F.col("estimated_demand") -
    F.col("baseline_demand")
).withColumn(
    "demand_change_pct",
    F.when(
        F.col("baseline_demand") != 0,
        (
            F.col("demand_change") /
            F.abs(
                F.col("baseline_demand")
            )
        ) * 100.0
    )
).withColumn(
    "wastage_change_units",
    F.col("estimated_wastage_units") -
    F.col("baseline_wastage_units")
).withColumn(
    "wastage_change_pct",
    F.when(
        F.col("baseline_wastage_units") != 0,
        (
            F.col("wastage_change_units") /
            F.abs(
                F.col("baseline_wastage_units")
            )
        ) * 100.0
    )
).withColumn(
    "wastage_change_percentage_points",
    F.col("estimated_wastage_pct") -
    F.col("effective_baseline_wastage_pct")
).withColumn(
    "profitability_change_percentage_points",
    F.col("estimated_profitability_pct") -
    F.col("simulation_baseline_profitability_pct")
)


impacts = impacts.withColumn(
    "estimate_method",
    F.when(
        F.col("scenario_type").isin(
            "Increase Menu Price",
            "Reduce Item Price"
        ),
        F.lit(
            "Historical price elasticity adjusts demand; price and estimated demand determine revenue while historical unit cost estimates contribution margin."
        )
    ).when(
        F.col("scenario_type") ==
        "Change Discount Percentage",
        F.lit(
            "Historical promotion demand lift scales the demand response to the discount change; net discount and estimated demand determine revenue."
        )
    ).when(
        F.col("scenario_type") ==
        "Increase Promotion Frequency",
        F.lit(
            "Promotion orders, revenue, and contribution margin scale with the ratio of proposed campaign-active days to historical campaign-active days."
        )
    ).when(
        F.col("scenario_type") ==
        "Remove Menu Item",
        F.lit(
            "Removal sets direct demand, revenue, contribution margin, and wastage for the selected location-menu item to zero; substitution is not modeled."
        )
    ).when(
        F.col("scenario_type") ==
        "Reduce Preparation Quantity",
        F.lit(
            "Preparation is reduced before demand is constrained; the reduction first removes estimated excess preparation and wastage."
        )
    ).when(
        F.col("scenario_type") ==
        "Increase Predicted Demand",
        F.lit(
            "Forecast demand is multiplied by historical per-unit revenue and contribution margin while the historical wastage rate is held constant."
        )
    ).otherwise(
        F.lit(
            "Revenue and demand are held constant while the selected wastage-rate assumption changes estimated wastage."
        )
    )
).withColumn(
    "estimate_confidence",
    F.when(
        F.col("scenario_type").isin(
            "Increase Menu Price",
            "Reduce Item Price"
        ),
        "Medium"
    ).when(
        F.col("scenario_type") ==
        "Change Discount Percentage",
        "Medium"
    ).when(
        F.col("scenario_type") ==
        "Increase Promotion Frequency",
        "Low"
    ).when(
        F.col("scenario_type") ==
        "Remove Menu Item",
        "Medium"
    ).otherwise(
        "Medium"
    )
).withColumn(
    "simulation_status",
    F.lit("Estimated - Not Actual")
).withColumn(
    "is_estimate",
    F.lit(True)
).withColumn(
    "demand_unit",
    F.when(
        F.col("target_scope") ==
        "Promotion",
        "promotion_orders"
    ).otherwise(
        "units"
    )
).withColumn(
    "impact_statement",
    F.concat(
        F.lit("Estimated revenue change "),
        F.round(
            F.coalesce(
                F.col("revenue_change_pct"),
                F.lit(0.0)
            ),
            2
        ).cast("string"),
        F.lit("%, contribution margin change "),
        F.round(
            F.coalesce(
                F.col("contribution_margin_change_pct"),
                F.lit(0.0)
            ),
            2
        ).cast("string"),
        F.lit("%, demand change "),
        F.round(
            F.coalesce(
                F.col("demand_change_pct"),
                F.lit(0.0)
            ),
            2
        ).cast("string"),
        F.lit("%, wastage-rate change "),
        F.round(
            F.col("wastage_change_percentage_points"),
            2
        ).cast("string"),
        F.lit(" percentage points, profitability change "),
        F.round(
            F.col("profitability_change_percentage_points"),
            2
        ).cast("string"),
        F.lit(" percentage points.")
    )
)


scenario_impacts = impacts.select(
    "scenario_id",
    "scenario_type",
    "target_scope",
    "restaurant_id",
    "restaurant_name",
    "item_id",
    "item_name",
    "promotion_id",
    "promotion_name",
    "parameter_name",
    "baseline_value",
    "proposed_value",
    "change_value",
    "change_pct",
    "value_unit",
    F.round(
        "simulation_baseline_revenue",
        2
    ).alias("baseline_revenue"),
    F.round(
        "estimated_revenue",
        2
    ).alias("estimated_revenue"),
    F.round(
        "revenue_change",
        2
    ).alias("revenue_change"),
    F.round(
        "revenue_change_pct",
        2
    ).alias("revenue_change_pct"),
    F.round(
        "simulation_baseline_contribution_margin",
        2
    ).alias("baseline_contribution_margin"),
    F.round(
        "estimated_contribution_margin",
        2
    ).alias("estimated_contribution_margin"),
    F.round(
        "contribution_margin_change",
        2
    ).alias("contribution_margin_change"),
    F.round(
        "contribution_margin_change_pct",
        2
    ).alias("contribution_margin_change_pct"),
    F.round(
        "baseline_demand",
        2
    ).alias("baseline_demand"),
    F.round(
        "estimated_demand",
        2
    ).alias("estimated_demand"),
    F.round(
        "demand_change",
        2
    ).alias("demand_change"),
    F.round(
        "demand_change_pct",
        2
    ).alias("demand_change_pct"),
    "demand_unit",
    F.round(
        "baseline_wastage_units",
        2
    ).alias("baseline_wastage_units"),
    F.round(
        "estimated_wastage_units",
        2
    ).alias("estimated_wastage_units"),
    F.round(
        "wastage_change_units",
        2
    ).alias("wastage_change_units"),
    F.round(
        "wastage_change_pct",
        2
    ).alias("wastage_change_pct"),
    F.round(
        "effective_baseline_wastage_pct",
        2
    ).alias("baseline_wastage_pct"),
    F.round(
        "estimated_wastage_pct",
        2
    ).alias("estimated_wastage_pct"),
    F.round(
        "wastage_change_percentage_points",
        2
    ).alias("wastage_change_percentage_points"),
    F.round(
        "simulation_baseline_profitability_pct",
        2
    ).alias("baseline_profitability_pct"),
    F.round(
        "estimated_profitability_pct",
        2
    ).alias("estimated_profitability_pct"),
    F.round(
        "profitability_change_percentage_points",
        2
    ).alias("profitability_change_percentage_points"),
    F.round(
        "price_elasticity",
        4
    ).alias("price_elasticity"),
    F.round(
        "historical_demand_lift_pct",
        2
    ).alias("historical_demand_lift_pct"),
    "estimate_method",
    "estimate_confidence",
    "simulation_status",
    "is_estimate",
    "impact_statement"
)


scenario_impact_summary = scenario_impacts.groupBy(
    "scenario_type"
).agg(
    F.count("*").alias("scenario_count"),
    F.round(
        F.avg("revenue_change_pct"),
        2
    ).alias("average_revenue_change_pct"),
    F.round(
        F.avg("contribution_margin_change_pct"),
        2
    ).alias("average_margin_change_pct"),
    F.round(
        F.avg("demand_change_pct"),
        2
    ).alias("average_demand_change_pct"),
    F.round(
        F.avg("wastage_change_percentage_points"),
        2
    ).alias("average_wastage_change_percentage_points"),
    F.round(
        F.avg("profitability_change_percentage_points"),
        2
    ).alias("average_profitability_change_percentage_points")
).orderBy(
    "scenario_type"
)


print("\n========SCENARIO IMPACT SUMMARY========")
scenario_impact_summary.show(
    truncate=False
)


print("\n========SCENARIO IMPACT ESTIMATES========")
scenario_impacts.select(
    "scenario_id",
    "scenario_type",
    "restaurant_name",
    "item_name",
    "promotion_name",
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
    "estimate_confidence",
    "simulation_status"
).orderBy(
    "scenario_id"
).show(
    truncate=False
)


scenario_impacts.write.mode(
    "overwrite"
).parquet(
    f"{output_folder}/scenario_impacts"
)


scenario_impact_summary.write.mode(
    "overwrite"
).parquet(
    f"{output_folder}/scenario_impact_summary"
)


print("\nScenario impact analysis completed successfully.")


spark.stop()
