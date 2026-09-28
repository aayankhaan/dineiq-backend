from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import INTEGRATED_DATA_FOLDER, ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("Validate DineIQ Wastage Analysis") \
    .getOrCreate()


integrated_folder = INTEGRATED_DATA_FOLDER
output_folder = f"{ANALYTICS_DATA_FOLDER}/wastage"


wastage = spark.read.parquet(
    f"{integrated_folder}/wastage"
)

transactions = spark.read.parquet(
    f"{integrated_folder}/transactions"
)

inventory = spark.read.parquet(
    f"{integrated_folder}/inventory"
)


wastage_summary = spark.read.parquet(
    f"{output_folder}/wastage_summary"
)

wastage_item_allocation = spark.read.parquet(
    f"{output_folder}/wastage_item_allocation"
)

item_daily_wastage = spark.read.parquet(
    f"{output_folder}/item_daily_wastage"
)

menu_item_wastage = spark.read.parquet(
    f"{output_folder}/menu_item_wastage"
)

category_wastage = spark.read.parquet(
    f"{output_folder}/category_wastage"
)

location_wastage = spark.read.parquet(
    f"{output_folder}/location_wastage"
)

daily_wastage = spark.read.parquet(
    f"{output_folder}/daily_wastage"
)

day_of_week_wastage = spark.read.parquet(
    f"{output_folder}/day_of_week_wastage"
)

monthly_wastage = spark.read.parquet(
    f"{output_folder}/monthly_wastage"
)

seasonal_wastage = spark.read.parquet(
    f"{output_folder}/seasonal_wastage"
)

ingredient_wastage = spark.read.parquet(
    f"{output_folder}/ingredient_wastage"
)

reason_wastage = spark.read.parquet(
    f"{output_folder}/reason_wastage"
)

inventory_consumption_relationship = spark.read.parquet(
    f"{output_folder}/inventory_consumption_relationship"
)

inventory_consumption_summary = spark.read.parquet(
    f"{output_folder}/inventory_consumption_summary"
)

promotion_wastage = spark.read.parquet(
    f"{output_folder}/promotion_wastage"
)

wastage_relationships = spark.read.parquet(
    f"{output_folder}/wastage_relationships"
)

analysis_metadata = spark.read.parquet(
    f"{output_folder}/analysis_metadata"
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

        print(
            f"[FAIL] {name}{suffix}"
        )

        failed += 1


print("\n========WASTAGE ANALYSIS VALIDATION========")



outputs = [
    ("Wastage summary", wastage_summary),
    ("Wastage item allocation", wastage_item_allocation),
    ("Item daily wastage", item_daily_wastage),
    ("Menu item wastage", menu_item_wastage),
    ("Category wastage", category_wastage),
    ("Location wastage", location_wastage),
    ("Daily wastage", daily_wastage),
    ("Day-of-week wastage", day_of_week_wastage),
    ("Monthly wastage", monthly_wastage),
    ("Seasonal wastage", seasonal_wastage),
    ("Ingredient wastage", ingredient_wastage),
    ("Reason wastage", reason_wastage),
    ("Inventory consumption relationship", inventory_consumption_relationship),
    ("Inventory consumption summary", inventory_consumption_summary),
    ("Promotion wastage", promotion_wastage),
    ("Wastage relationships", wastage_relationships),
    ("Analysis metadata", analysis_metadata)
]


for name, frame in outputs:
    check(
        f"{name} contains data",
        frame.count() > 0
    )



required_item_columns = {
    "item_id",
    "item_name",
    "category_id",
    "category_name",
    "total_demand_quantity",
    "estimated_inventory_consumption_cost",
    "estimated_wastage_cost",
    "estimated_wasted_servings",
    "estimated_preparation_quantity",
    "promotion_quantity"
}


check(
    "Menu-item analysis contains required SRS measures",
    required_item_columns.issubset(
        set(
            menu_item_wastage.columns
        )
    )
)


check(
    "Category analysis contains category identifiers and names",
    {
        "category_id",
        "category_name"
    }.issubset(
        set(
            category_wastage.columns
        )
    )
)


check(
    "Location analysis contains restaurant identifiers and names",
    {
        "restaurant_id",
        "restaurant_name",
        "restaurant_city",
        "restaurant_area"
    }.issubset(
        set(
            location_wastage.columns
        )
    )
)


check(
    "Daily analysis contains date and demand",
    {
        "analysis_date",
        "demand_quantity",
        "estimated_wastage_cost"
    }.issubset(
        set(
            daily_wastage.columns
        )
    )
)


check(
    "Time-period analysis includes monthly trends",
    {
        "year_month",
        "total_demand_quantity",
        "estimated_wastage_cost"
    }.issubset(
        set(
            monthly_wastage.columns
        )
    )
)


check(
    "Time-period analysis includes seasons",
    {
        "season",
        "estimated_wastage_cost"
    }.issubset(
        set(
            seasonal_wastage.columns
        )
    )
)


check(
    "Inventory-consumption analysis contains required measures",
    {
        "quantity_received",
        "operational_consumption_quantity",
        "wastage_quantity",
        "adjusted_quantity_remaining",
        "operational_consumption_cost",
        "wastage_cost"
    }.issubset(
        set(
            inventory_consumption_relationship.columns
        )
    )
)


check(
    "Promotion analysis contains promotion status",
    "promotion_status" in
    promotion_wastage.columns
)


check(
    "Preparation quantity is explicitly estimated",
    "estimated_preparation_quantity" in
    item_daily_wastage.columns
)



source_wastage_count = wastage.select(
    "wastage_id"
).distinct().count()


allocated_wastage_count = wastage_item_allocation.select(
    "wastage_id"
).distinct().count()


check(
    "Every cleaned wastage event is allocated for menu analysis",
    allocated_wastage_count ==
    source_wastage_count,
    (
        f"Source: {source_wastage_count}, "
        f"allocated: {allocated_wastage_count}"
    )
)


source_item_count = transactions.filter(
    F.col("order_status") != "Cancelled"
).select(
    "item_id"
).distinct().count()


analysis_item_count = menu_item_wastage.select(
    "item_id"
).distinct().count()


check(
    "All sold menu items are covered",
    analysis_item_count ==
    source_item_count,
    (
        f"Sold items: {source_item_count}, "
        f"analyzed items: {analysis_item_count}"
    )
)


source_category_count = transactions.filter(
    F.col("order_status") != "Cancelled"
).select(
    "category_id"
).distinct().count()


analysis_category_count = category_wastage.select(
    "category_id"
).distinct().count()


check(
    "All sold menu categories are covered",
    analysis_category_count ==
    source_category_count
)


source_location_count = transactions.filter(
    F.col("order_status") != "Cancelled"
).select(
    "restaurant_id"
).distinct().count()


analysis_location_count = location_wastage.select(
    "restaurant_id"
).distinct().count()


check(
    "All active restaurant locations are covered",
    analysis_location_count ==
    source_location_count
)



allocation_share_errors = wastage_item_allocation.groupBy(
    "wastage_id"
).agg(
    F.sum(
        "allocation_share"
    ).alias(
        "share_sum"
    )
).filter(
    F.abs(
        F.col("share_sum") -
        F.lit(1.0)
    ) > 0.000001
).count()


check(
    "Each wastage event allocation sums to 100 percent",
    allocation_share_errors == 0,
    (
        f"Invalid wastage events: "
        f"{allocation_share_errors}"
    )
)


source_total_cost = wastage.agg(
    F.sum(
        F.col("cost").cast("double")
    ).alias("total")
).first()["total"]


allocated_total_cost = wastage_item_allocation.agg(
    F.sum(
        "allocated_wastage_cost"
    ).alias("total")
).first()["total"]


cost_difference = abs(
    float(source_total_cost) -
    float(allocated_total_cost)
)


cost_tolerance = max(
    0.10,
    abs(
        float(source_total_cost)
    ) * 0.000001
)


check(
    "Allocated menu-item wastage cost conserves source wastage cost",
    cost_difference <=
    cost_tolerance,
    (
        f"Difference: "
        f"{cost_difference:.4f}"
    )
)


check(
    "Allocation shares remain between zero and one",
    wastage_item_allocation.filter(
        (
            F.col("allocation_share") <= 0
        ) |
        (
            F.col("allocation_share") > 1
        )
    ).count() == 0
)




check(
    "Item-day demand is non-negative",
    item_daily_wastage.filter(
        F.col("demand_quantity") < 0
    ).count() == 0
)


check(
    "Item-day estimated wastage cost is non-negative",
    item_daily_wastage.filter(
        F.col("estimated_wastage_cost") < 0
    ).count() == 0
)


check(
    "Estimated preparation quantity is never below demand",
    item_daily_wastage.filter(
        F.col("estimated_preparation_quantity") <
        F.col("demand_quantity")
    ).count() == 0
)


check(
    "Preparation surplus percentages are valid",
    item_daily_wastage.filter(
        (
            F.col("preparation_surplus_pct") < 0
        ) |
        (
            F.col("preparation_surplus_pct") > 100
        )
    ).count() == 0
)


check(
    "Promotion share percentages are valid",
    item_daily_wastage.filter(
        (
            F.col("promotion_share_pct") < 0
        ) |
        (
            F.col("promotion_share_pct") > 100
        )
    ).count() == 0
)


promotion_statuses = {
    row["promotion_status"]
    for row in promotion_wastage.select(
        "promotion_status"
    ).distinct().collect()
}


check(
    "Promotion and non-promotion periods are both represented",
    promotion_statuses == {
        "Promotion Active",
        "No Promotion"
    },
    f"Found: {sorted(promotion_statuses)}"
)



check(
    "All seven days of week are represented",
    day_of_week_wastage.select(
        "day_of_week"
    ).distinct().count() == 7
)


check(
    "Monthly analysis contains multiple months",
    monthly_wastage.select(
        "year_month"
    ).distinct().count() > 12
)


seasons = {
    row["season"]
    for row in seasonal_wastage.select(
        "season"
    ).distinct().collect()
}


check(
    "All four seasons are represented",
    seasons == {
        "Winter",
        "Spring",
        "Summer",
        "Autumn"
    },
    f"Found: {sorted(seasons)}"
)



check(
    "Inventory relationship covers every inventory batch",
    inventory_consumption_relationship.select(
        "inventory_id"
    ).distinct().count() ==
    inventory.select(
        "inventory_id"
    ).distinct().count()
)


check(
    "Operational inventory consumption is non-negative",
    inventory_consumption_relationship.filter(
        F.col(
            "operational_consumption_quantity"
        ) < -0.000001
    ).count() == 0
)


check(
    "Adjusted remaining inventory is non-negative",
    inventory_consumption_relationship.filter(
        F.col(
            "adjusted_quantity_remaining"
        ) < -0.000001
    ).count() == 0
)


check(
    "Inventory depletion does not exceed quantity received",
    inventory_consumption_relationship.filter(
        F.col(
            "total_depleted_quantity"
        ) >
        F.col(
            "quantity_received"
        ) +
        F.lit(0.01)
    ).count() == 0
)


check(
    "Inventory wastage percentages are valid",
    inventory_consumption_relationship.filter(
        (
            F.col("wastage_pct_of_received") < 0
        ) |
        (
            F.col("wastage_pct_of_received") > 100
        )
    ).count() == 0
)



reason_values = {
    row["reason"]
    for row in reason_wastage.select(
        "reason"
    ).distinct().collect()
}


check(
    "Expected wastage reasons are represented",
    reason_values == {
        "Expired",
        "Spoiled",
        "Overproduction",
        "Damaged"
    },
    f"Found: {sorted(reason_values)}"
)


check(
    "Ingredient wastage preserves units",
    ingredient_wastage.filter(
        F.col("unit").isNull()
    ).count() == 0
)


relationship_columns = [
    "demand_wastage_cost_correlation",
    "inventory_consumption_wastage_correlation",
    "preparation_wastage_correlation",
    "promotion_wastage_correlation"
]


for column in relationship_columns:
    check(
        f"Relationship metric exists: {column}",
        column in
        wastage_relationships.columns
    )


metadata_components = {
    row["analysis_component"]
    for row in analysis_metadata.select(
        "analysis_component"
    ).distinct().collect()
}


check(
    "Menu-item wastage allocation method is documented",
    "ingredient_to_menu_item_allocation" in
    metadata_components
)


check(
    "Preparation quantity estimation method is documented",
    "preparation_quantity" in
    metadata_components
)


check(
    "Inventory consumption method is documented",
    "inventory_consumption" in
    metadata_components
)


check(
    "Time-period interpretation is documented",
    "time_period" in
    metadata_components
)


check(
    "Promotion interpretation is documented",
    "promotion" in
    metadata_components
)



total_checks = passed + failed


print(
    f"\n========VALIDATION RESULT: "
    f"{passed}/{total_checks} PASS========"
)


if failed == 0:
    print(
        "\nWastage analysis validation PASSED."
    )
else:
    print(
        "\nWastage analysis validation FAILED."
    )


spark.stop()
