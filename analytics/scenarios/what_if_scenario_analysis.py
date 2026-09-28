from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.settings import (
    ANALYTICS_DATA_FOLDER,
    ML_DATA_FOLDER,
    PROCESSED_DATA_FOLDER
)


spark = SparkSession.builder \
    .appName("DineIQ What-If Scenario Analysis") \
    .getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "8")


location_menu = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/location_menu_performance/location_menu_performance"
)

price_sensitivity = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/price_sensitivity/item_price_sensitivity"
)

promotion_effectiveness = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/promotion_effectiveness/promotion_effectiveness"
)

prioritized_recommendations = spark.read.parquet(
    f"{ANALYTICS_DATA_FOLDER}/recommendation_priority/prioritized_recommendations"
)

menu_item_forecast = spark.read.parquet(
    f"{ML_DATA_FOLDER}/forecasting/menu_item_forecast"
)

restaurant_menu = spark.read.parquet(
    f"{PROCESSED_DATA_FOLDER}/restaurant_menu_items"
)

output_folder = f"{ANALYTICS_DATA_FOLDER}/what_if_scenarios"


scenario_catalog = spark.createDataFrame(
    [
        (
            "Increase Menu Price",
            "Location Menu Item",
            "menu_price",
            "relative_percent",
            "percent",
            5.0,
            1.0,
            20.0,
            "Increase a menu item's current price by a selected percentage."
        ),
        (
            "Reduce Item Price",
            "Location Menu Item",
            "menu_price",
            "relative_percent",
            "percent",
            -5.0,
            -20.0,
            -1.0,
            "Reduce a menu item's current price by a selected percentage."
        ),
        (
            "Change Discount Percentage",
            "Promotion",
            "discount_percentage",
            "percentage_points",
            "percentage_points",
            5.0,
            -20.0,
            20.0,
            "Change the percentage discount for a percentage-based promotion."
        ),
        (
            "Increase Promotion Frequency",
            "Promotion",
            "promotion_days",
            "relative_percent",
            "percent",
            25.0,
            5.0,
            100.0,
            "Increase promotion frequency using campaign-active days as the simulation baseline."
        ),
        (
            "Remove Menu Item",
            "Location Menu Item",
            "menu_availability",
            "toggle",
            "binary",
            -1.0,
            -1.0,
            -1.0,
            "Simulate removing a menu item from one restaurant location."
        ),
        (
            "Reduce Preparation Quantity",
            "Location Menu Item",
            "preparation_quantity",
            "relative_percent",
            "percent",
            -10.0,
            -50.0,
            -5.0,
            "Reduce estimated preparation quantity for a selected location-menu item."
        ),
        (
            "Increase Predicted Demand",
            "Menu Item Forecast",
            "predicted_demand",
            "relative_percent",
            "percent",
            10.0,
            1.0,
            50.0,
            "Increase forecast demand by a selected percentage."
        ),
        (
            "Change Wastage Assumptions",
            "Location Menu Item",
            "wastage_percentage",
            "relative_percent",
            "percent",
            -10.0,
            -50.0,
            50.0,
            "Change the assumed wastage rate used for scenario estimation."
        )
    ],
    [
        "scenario_type",
        "target_scope",
        "parameter_name",
        "change_mode",
        "parameter_unit",
        "default_change_value",
        "minimum_change_value",
        "maximum_change_value",
        "scenario_description"
    ]
).withColumn(
    "user_adjustable",
    F.lit(True)
).withColumn(
    "output_status",
    F.lit("Scenario Input Only")
)


current_menu = restaurant_menu.filter(
    F.col("is_available")
).select(
    "restaurant_id",
    "item_id",
    F.col("price").cast("double").alias("current_menu_price")
)


menu_base = location_menu.join(
    current_menu,
    [
        "restaurant_id",
        "item_id"
    ],
    "inner"
)


price_base = menu_base.join(
    price_sensitivity.select(
        "item_id",
        "price_sensitivity_class",
        "price_sensitivity_score",
        "median_capped_absolute_elasticity"
    ),
    "item_id",
    "inner"
)


increase_price_target = price_base.filter(
    F.col("price_sensitivity_class") ==
    "Low Price Sensitivity"
).orderBy(
    F.desc("item_revenue"),
    F.desc("contribution_margin"),
    F.asc("restaurant_id"),
    F.asc("item_id")
).limit(
    1
)


increase_price = increase_price_target.select(
    F.lit("SCN-01").alias("scenario_id"),
    F.lit("Increase Menu Price").alias("scenario_type"),
    F.lit("Location Menu Item").alias("target_scope"),
    "restaurant_id",
    "restaurant_name",
    "item_id",
    "item_name",
    F.lit(None).cast("int").alias("promotion_id"),
    F.lit(None).cast("string").alias("promotion_name"),
    F.lit("menu_price").alias("parameter_name"),
    F.col("current_menu_price").alias("baseline_value"),
    F.round(
        F.col("current_menu_price") * 1.05,
        2
    ).alias("proposed_value"),
    F.round(
        F.col("current_menu_price") * 0.05,
        2
    ).alias("change_value"),
    F.lit(5.0).alias("change_pct"),
    F.lit("currency").alias("value_unit"),
    F.col("current_menu_price").alias("baseline_price"),
    F.lit(None).cast("double").alias("baseline_discount_percentage"),
    F.lit(None).cast("double").alias("baseline_promotion_days"),
    F.col("quantity_sold").cast("double").alias("baseline_quantity"),
    F.col("item_revenue").cast("double").alias("baseline_revenue"),
    F.col("contribution_margin").cast("double").alias("baseline_contribution_margin"),
    F.col("profit_percentage").cast("double").alias("baseline_profitability_pct"),
    F.col("wastage_percentage").cast("double").alias("baseline_wastage_pct"),
    F.col("estimated_preparation_quantity").cast("double").alias("baseline_preparation_quantity"),
    F.lit(None).cast("double").alias("baseline_forecast_demand"),
    F.col("price_sensitivity_score").cast("double").alias("price_sensitivity_score"),
    F.col("median_capped_absolute_elasticity").cast("double").alias("price_elasticity"),
    F.lit("Price Sensitivity and Location Menu Performance").alias("source_analysis"),
    F.lit("Scenario input only; business impact is estimated in Step 41.").alias("scenario_note")
)


reduce_price_target = price_base.filter(
    F.col("price_sensitivity_class") ==
    "Highly Price Sensitive"
).orderBy(
    F.desc("item_revenue"),
    F.desc("price_sensitivity_score"),
    F.asc("restaurant_id"),
    F.asc("item_id")
).limit(
    1
)


reduce_price = reduce_price_target.select(
    F.lit("SCN-02").alias("scenario_id"),
    F.lit("Reduce Item Price").alias("scenario_type"),
    F.lit("Location Menu Item").alias("target_scope"),
    "restaurant_id",
    "restaurant_name",
    "item_id",
    "item_name",
    F.lit(None).cast("int").alias("promotion_id"),
    F.lit(None).cast("string").alias("promotion_name"),
    F.lit("menu_price").alias("parameter_name"),
    F.col("current_menu_price").alias("baseline_value"),
    F.round(
        F.col("current_menu_price") * 0.95,
        2
    ).alias("proposed_value"),
    F.round(
        F.col("current_menu_price") * -0.05,
        2
    ).alias("change_value"),
    F.lit(-5.0).alias("change_pct"),
    F.lit("currency").alias("value_unit"),
    F.col("current_menu_price").alias("baseline_price"),
    F.lit(None).cast("double").alias("baseline_discount_percentage"),
    F.lit(None).cast("double").alias("baseline_promotion_days"),
    F.col("quantity_sold").cast("double").alias("baseline_quantity"),
    F.col("item_revenue").cast("double").alias("baseline_revenue"),
    F.col("contribution_margin").cast("double").alias("baseline_contribution_margin"),
    F.col("profit_percentage").cast("double").alias("baseline_profitability_pct"),
    F.col("wastage_percentage").cast("double").alias("baseline_wastage_pct"),
    F.col("estimated_preparation_quantity").cast("double").alias("baseline_preparation_quantity"),
    F.lit(None).cast("double").alias("baseline_forecast_demand"),
    F.col("price_sensitivity_score").cast("double").alias("price_sensitivity_score"),
    F.col("median_capped_absolute_elasticity").cast("double").alias("price_elasticity"),
    F.lit("Price Sensitivity and Location Menu Performance").alias("source_analysis"),
    F.lit("Scenario input only; business impact is estimated in Step 41.").alias("scenario_note")
)


discount_target = promotion_effectiveness.filter(
    (
        F.col("discount_type") ==
        "Percentage"
    ) &
    (
        F.col("promotion_order_count") > 0
    )
).orderBy(
    F.desc("promotion_revenue"),
    F.desc("promotion_order_count"),
    F.asc("promotion_id")
).limit(
    1
)


discount_change = discount_target.select(
    F.lit("SCN-03").alias("scenario_id"),
    F.lit("Change Discount Percentage").alias("scenario_type"),
    F.lit("Promotion").alias("target_scope"),
    "restaurant_id",
    "restaurant_name",
    "item_id",
    "item_name",
    "promotion_id",
    "promotion_name",
    F.lit("discount_percentage").alias("parameter_name"),
    F.col("discount_value").cast("double").alias("baseline_value"),
    F.least(
        F.col("discount_value").cast("double") + F.lit(5.0),
        F.lit(100.0)
    ).alias("proposed_value"),
    F.least(
        F.lit(5.0),
        F.lit(100.0) -
        F.col("discount_value").cast("double")
    ).alias("change_value"),
    F.lit(None).cast("double").alias("change_pct"),
    F.lit("percentage_points").alias("value_unit"),
    F.lit(None).cast("double").alias("baseline_price"),
    F.col("discount_value").cast("double").alias("baseline_discount_percentage"),
    F.col("promotion_days").cast("double").alias("baseline_promotion_days"),
    F.col("promotion_order_count").cast("double").alias("baseline_quantity"),
    F.col("promotion_revenue").cast("double").alias("baseline_revenue"),
    F.col("promotion_contribution_margin").cast("double").alias("baseline_contribution_margin"),
    F.when(
        F.col("promotion_revenue") > 0,
        F.round(
            (
                F.col("promotion_contribution_margin") /
                F.col("promotion_revenue")
            ) * 100,
            2
        )
    ).alias("baseline_profitability_pct"),
    F.lit(None).cast("double").alias("baseline_wastage_pct"),
    F.lit(None).cast("double").alias("baseline_preparation_quantity"),
    F.lit(None).cast("double").alias("baseline_forecast_demand"),
    F.lit(None).cast("double").alias("price_sensitivity_score"),
    F.lit(None).cast("double").alias("price_elasticity"),
    F.lit("Promotion Effectiveness").alias("source_analysis"),
    F.lit("Scenario input only; business impact is estimated in Step 41.").alias("scenario_note")
)


frequency_target = promotion_effectiveness.filter(
    (
        F.col("promotion_order_count") > 0
    ) &
    (
        F.col("promotion_days") > 0
    )
).orderBy(
    F.desc("promotion_revenue"),
    F.desc("positive_kpi_count"),
    F.asc("promotion_id")
).limit(
    1
)


promotion_frequency = frequency_target.select(
    F.lit("SCN-04").alias("scenario_id"),
    F.lit("Increase Promotion Frequency").alias("scenario_type"),
    F.lit("Promotion").alias("target_scope"),
    "restaurant_id",
    "restaurant_name",
    "item_id",
    "item_name",
    "promotion_id",
    "promotion_name",
    F.lit("promotion_days").alias("parameter_name"),
    F.col("promotion_days").cast("double").alias("baseline_value"),
    F.ceil(
        F.col("promotion_days").cast("double") * 1.25
    ).cast("double").alias("proposed_value"),
    (
        F.ceil(
            F.col("promotion_days").cast("double") * 1.25
        ).cast("double") -
        F.col("promotion_days").cast("double")
    ).alias("change_value"),
    F.round(
        (
            (
                F.ceil(
                    F.col("promotion_days").cast("double") * 1.25
                ).cast("double") -
                F.col("promotion_days").cast("double")
            ) /
            F.col("promotion_days").cast("double")
        ) * 100,
        2
    ).alias("change_pct"),
    F.lit("days").alias("value_unit"),
    F.lit(None).cast("double").alias("baseline_price"),
    F.when(
        F.col("discount_type") ==
        "Percentage",
        F.col("discount_value").cast("double")
    ).alias("baseline_discount_percentage"),
    F.col("promotion_days").cast("double").alias("baseline_promotion_days"),
    F.col("promotion_order_count").cast("double").alias("baseline_quantity"),
    F.col("promotion_revenue").cast("double").alias("baseline_revenue"),
    F.col("promotion_contribution_margin").cast("double").alias("baseline_contribution_margin"),
    F.when(
        F.col("promotion_revenue") > 0,
        F.round(
            (
                F.col("promotion_contribution_margin") /
                F.col("promotion_revenue")
            ) * 100,
            2
        )
    ).alias("baseline_profitability_pct"),
    F.lit(None).cast("double").alias("baseline_wastage_pct"),
    F.lit(None).cast("double").alias("baseline_preparation_quantity"),
    F.lit(None).cast("double").alias("baseline_forecast_demand"),
    F.lit(None).cast("double").alias("price_sensitivity_score"),
    F.lit(None).cast("double").alias("price_elasticity"),
    F.lit("Promotion Effectiveness").alias("source_analysis"),
    F.lit("Campaign-active days are used as the controllable frequency proxy for simulation.").alias("scenario_note")
)


remove_target = prioritized_recommendations.filter(
    F.col("recommendation_type") ==
    "Review or Redesign Low Performer"
).orderBy(
    F.desc("priority_score"),
    F.desc("impact_value"),
    F.asc("recommendation_id")
).limit(
    1
).select(
    "restaurant_id",
    "item_id"
).join(
    menu_base,
    [
        "restaurant_id",
        "item_id"
    ],
    "inner"
)


remove_item = remove_target.select(
    F.lit("SCN-05").alias("scenario_id"),
    F.lit("Remove Menu Item").alias("scenario_type"),
    F.lit("Location Menu Item").alias("target_scope"),
    "restaurant_id",
    "restaurant_name",
    "item_id",
    "item_name",
    F.lit(None).cast("int").alias("promotion_id"),
    F.lit(None).cast("string").alias("promotion_name"),
    F.lit("menu_availability").alias("parameter_name"),
    F.lit(1.0).alias("baseline_value"),
    F.lit(0.0).alias("proposed_value"),
    F.lit(-1.0).alias("change_value"),
    F.lit(-100.0).alias("change_pct"),
    F.lit("binary").alias("value_unit"),
    F.col("current_menu_price").alias("baseline_price"),
    F.lit(None).cast("double").alias("baseline_discount_percentage"),
    F.lit(None).cast("double").alias("baseline_promotion_days"),
    F.col("quantity_sold").cast("double").alias("baseline_quantity"),
    F.col("item_revenue").cast("double").alias("baseline_revenue"),
    F.col("contribution_margin").cast("double").alias("baseline_contribution_margin"),
    F.col("profit_percentage").cast("double").alias("baseline_profitability_pct"),
    F.col("wastage_percentage").cast("double").alias("baseline_wastage_pct"),
    F.col("estimated_preparation_quantity").cast("double").alias("baseline_preparation_quantity"),
    F.lit(None).cast("double").alias("baseline_forecast_demand"),
    F.lit(None).cast("double").alias("price_sensitivity_score"),
    F.lit(None).cast("double").alias("price_elasticity"),
    F.lit("Recommendation Priority and Location Menu Performance").alias("source_analysis"),
    F.lit("Removal is simulated as menu availability changing from active to unavailable.").alias("scenario_note")
)


prep_target = prioritized_recommendations.filter(
    F.col("recommendation_type") ==
    "Reduce Preparation Quantity"
).orderBy(
    F.desc("priority_score"),
    F.desc("impact_value"),
    F.asc("recommendation_id")
).limit(
    1
).select(
    "restaurant_id",
    "item_id"
).join(
    menu_base,
    [
        "restaurant_id",
        "item_id"
    ],
    "inner"
).filter(
    F.col("estimated_preparation_quantity") > 0
)


reduce_preparation = prep_target.select(
    F.lit("SCN-06").alias("scenario_id"),
    F.lit("Reduce Preparation Quantity").alias("scenario_type"),
    F.lit("Location Menu Item").alias("target_scope"),
    "restaurant_id",
    "restaurant_name",
    "item_id",
    "item_name",
    F.lit(None).cast("int").alias("promotion_id"),
    F.lit(None).cast("string").alias("promotion_name"),
    F.lit("preparation_quantity").alias("parameter_name"),
    F.col("estimated_preparation_quantity").cast("double").alias("baseline_value"),
    F.round(
        F.col("estimated_preparation_quantity") * 0.90,
        2
    ).alias("proposed_value"),
    F.round(
        F.col("estimated_preparation_quantity") * -0.10,
        2
    ).alias("change_value"),
    F.lit(-10.0).alias("change_pct"),
    F.lit("servings").alias("value_unit"),
    F.col("current_menu_price").alias("baseline_price"),
    F.lit(None).cast("double").alias("baseline_discount_percentage"),
    F.lit(None).cast("double").alias("baseline_promotion_days"),
    F.col("quantity_sold").cast("double").alias("baseline_quantity"),
    F.col("item_revenue").cast("double").alias("baseline_revenue"),
    F.col("contribution_margin").cast("double").alias("baseline_contribution_margin"),
    F.col("profit_percentage").cast("double").alias("baseline_profitability_pct"),
    F.col("wastage_percentage").cast("double").alias("baseline_wastage_pct"),
    F.col("estimated_preparation_quantity").cast("double").alias("baseline_preparation_quantity"),
    F.lit(None).cast("double").alias("baseline_forecast_demand"),
    F.lit(None).cast("double").alias("price_sensitivity_score"),
    F.lit(None).cast("double").alias("price_elasticity"),
    F.lit("Recommendation Priority and Location Menu Performance").alias("source_analysis"),
    F.lit("Scenario input only; business impact is estimated in Step 41.").alias("scenario_note")
)


item_forecast_summary = menu_item_forecast.groupBy(
    "item_id",
    "item_name"
).agg(
    F.round(
        F.sum("predicted_demand"),
        2
    ).alias("forecast_demand")
)


item_economics = location_menu.groupBy(
    "item_id",
    "item_name"
).agg(
    F.sum("quantity_sold").cast("double").alias("network_quantity"),
    F.round(
        F.sum("item_revenue"),
        2
    ).alias("network_revenue"),
    F.round(
        F.sum("contribution_margin"),
        2
    ).alias("network_contribution_margin"),
    F.round(
        F.avg("profit_percentage"),
        2
    ).alias("network_profitability_pct"),
    F.round(
        F.avg("wastage_percentage"),
        2
    ).alias("network_wastage_pct"),
    F.round(
        F.sum("estimated_preparation_quantity"),
        2
    ).alias("network_preparation_quantity")
)


demand_target = item_forecast_summary.join(
    item_economics,
    [
        "item_id",
        "item_name"
    ],
    "inner"
).orderBy(
    F.desc("forecast_demand"),
    F.desc("network_revenue"),
    F.asc("item_id")
).limit(
    1
)


increase_demand = demand_target.select(
    F.lit("SCN-07").alias("scenario_id"),
    F.lit("Increase Predicted Demand").alias("scenario_type"),
    F.lit("Menu Item Forecast").alias("target_scope"),
    F.lit(None).cast("int").alias("restaurant_id"),
    F.lit(None).cast("string").alias("restaurant_name"),
    "item_id",
    "item_name",
    F.lit(None).cast("int").alias("promotion_id"),
    F.lit(None).cast("string").alias("promotion_name"),
    F.lit("predicted_demand").alias("parameter_name"),
    F.col("forecast_demand").cast("double").alias("baseline_value"),
    F.round(
        F.col("forecast_demand") * 1.10,
        2
    ).alias("proposed_value"),
    F.round(
        F.col("forecast_demand") * 0.10,
        2
    ).alias("change_value"),
    F.lit(10.0).alias("change_pct"),
    F.lit("units").alias("value_unit"),
    F.when(
        F.col("network_quantity") > 0,
        F.round(
            F.col("network_revenue") /
            F.col("network_quantity"),
            2
        )
    ).alias("baseline_price"),
    F.lit(None).cast("double").alias("baseline_discount_percentage"),
    F.lit(None).cast("double").alias("baseline_promotion_days"),
    F.col("network_quantity").alias("baseline_quantity"),
    F.col("network_revenue").alias("baseline_revenue"),
    F.col("network_contribution_margin").alias("baseline_contribution_margin"),
    F.col("network_profitability_pct").alias("baseline_profitability_pct"),
    F.col("network_wastage_pct").alias("baseline_wastage_pct"),
    F.col("network_preparation_quantity").alias("baseline_preparation_quantity"),
    F.col("forecast_demand").alias("baseline_forecast_demand"),
    F.lit(None).cast("double").alias("price_sensitivity_score"),
    F.lit(None).cast("double").alias("price_elasticity"),
    F.lit("Demand Forecasting and Location Menu Performance").alias("source_analysis"),
    F.lit("Scenario input only; business impact is estimated in Step 41.").alias("scenario_note")
)


wastage_target = menu_base.filter(
    F.col("wastage_percentage") > 0
).orderBy(
    F.desc("wastage_percentage"),
    F.desc("item_revenue"),
    F.asc("restaurant_id"),
    F.asc("item_id")
).limit(
    1
)


change_wastage = wastage_target.select(
    F.lit("SCN-08").alias("scenario_id"),
    F.lit("Change Wastage Assumptions").alias("scenario_type"),
    F.lit("Location Menu Item").alias("target_scope"),
    "restaurant_id",
    "restaurant_name",
    "item_id",
    "item_name",
    F.lit(None).cast("int").alias("promotion_id"),
    F.lit(None).cast("string").alias("promotion_name"),
    F.lit("wastage_percentage").alias("parameter_name"),
    F.col("wastage_percentage").cast("double").alias("baseline_value"),
    F.round(
        F.col("wastage_percentage") * 0.90,
        2
    ).alias("proposed_value"),
    F.round(
        F.col("wastage_percentage") * -0.10,
        2
    ).alias("change_value"),
    F.lit(-10.0).alias("change_pct"),
    F.lit("percent").alias("value_unit"),
    F.col("current_menu_price").alias("baseline_price"),
    F.lit(None).cast("double").alias("baseline_discount_percentage"),
    F.lit(None).cast("double").alias("baseline_promotion_days"),
    F.col("quantity_sold").cast("double").alias("baseline_quantity"),
    F.col("item_revenue").cast("double").alias("baseline_revenue"),
    F.col("contribution_margin").cast("double").alias("baseline_contribution_margin"),
    F.col("profit_percentage").cast("double").alias("baseline_profitability_pct"),
    F.col("wastage_percentage").cast("double").alias("baseline_wastage_pct"),
    F.col("estimated_preparation_quantity").cast("double").alias("baseline_preparation_quantity"),
    F.lit(None).cast("double").alias("baseline_forecast_demand"),
    F.lit(None).cast("double").alias("price_sensitivity_score"),
    F.lit(None).cast("double").alias("price_elasticity"),
    F.lit("Location Menu Performance").alias("source_analysis"),
    F.lit("The default example reduces the wastage assumption by 10%; users may simulate increases or decreases.").alias("scenario_note")
)


default_scenarios = increase_price.unionByName(
    reduce_price
).unionByName(
    discount_change
).unionByName(
    promotion_frequency
).unionByName(
    remove_item
).unionByName(
    reduce_preparation
).unionByName(
    increase_demand
).unionByName(
    change_wastage
).withColumn(
    "is_simulated_estimate",
    F.lit(True)
).withColumn(
    "impact_calculated",
    F.lit(False)
)


scenario_summary = default_scenarios.groupBy(
    "scenario_type",
    "target_scope",
    "parameter_name",
    "value_unit"
).agg(
    F.count("*").alias("scenario_count"),
    F.round(
        F.avg("baseline_value"),
        2
    ).alias("average_baseline_value"),
    F.round(
        F.avg("proposed_value"),
        2
    ).alias("average_proposed_value")
).orderBy(
    "scenario_id"
) if False else default_scenarios.select(
    "scenario_type",
    "target_scope",
    "parameter_name",
    "value_unit"
).groupBy(
    "scenario_type",
    "target_scope",
    "parameter_name",
    "value_unit"
).agg(
    F.count("*").alias("scenario_count")
).orderBy(
    "scenario_type"
)


print("\n========WHAT-IF SCENARIO CATALOG========")
scenario_catalog.orderBy(
    "scenario_type"
).show(
    truncate=False
)


print("\n========DEFAULT WHAT-IF SCENARIOS========")
default_scenarios.select(
    "scenario_id",
    "scenario_type",
    "target_scope",
    "restaurant_name",
    "item_name",
    "promotion_name",
    "parameter_name",
    "baseline_value",
    "proposed_value",
    "change_value",
    "change_pct",
    "value_unit",
    "source_analysis",
    "scenario_note"
).orderBy(
    "scenario_id"
).show(
    truncate=False
)


scenario_catalog.write.mode(
    "overwrite"
).parquet(
    f"{output_folder}/scenario_catalog"
)


default_scenarios.write.mode(
    "overwrite"
).parquet(
    f"{output_folder}/default_scenarios"
)


scenario_summary.write.mode(
    "overwrite"
).parquet(
    f"{output_folder}/scenario_summary"
)


print("\nWhat-if scenario analysis completed successfully.")


spark.stop()
