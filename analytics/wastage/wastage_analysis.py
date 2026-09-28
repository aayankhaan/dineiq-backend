from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.settings import INTEGRATED_DATA_FOLDER, ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("DineIQ Wastage Analysis") \
    .getOrCreate()


integrated_folder = INTEGRATED_DATA_FOLDER
output_folder = f"{ANALYTICS_DATA_FOLDER}/wastage"


transactions = spark.read.parquet(f"{integrated_folder}/transactions")
recipes = spark.read.parquet(f"{integrated_folder}/recipes")
inventory = spark.read.parquet(f"{integrated_folder}/inventory")
wastage = spark.read.parquet(f"{integrated_folder}/wastage")
restaurant_menu = spark.read.parquet(f"{integrated_folder}/restaurant_menu")



completed_transactions = transactions.filter(
    F.col("order_status") != "Cancelled"
)


item_master = transactions.groupBy(
    "item_id",
    "item_name",
    "category_id",
    "category_name"
).agg(
    F.max(
        F.col("item_cost").cast("double")
    ).alias("item_cost")
)


location_master = transactions.select(
    "restaurant_id",
    "restaurant_name",
    "restaurant_city",
    "restaurant_area"
).dropDuplicates(
    ["restaurant_id"]
)



daily_item_demand = completed_transactions.withColumn(
    "analysis_date",
    F.to_date("order_datetime")
).groupBy(
    "analysis_date",
    "restaurant_id",
    "item_id"
).agg(
    F.sum(
        F.col("quantity").cast("double")
    ).alias("demand_quantity"),
    F.countDistinct(
        "order_id"
    ).alias("order_count"),
    F.sum(
        F.when(
            F.col("item_discount_amount") > 0,
            F.col("quantity").cast("double")
        ).otherwise(
            F.lit(0.0)
        )
    ).alias("promotion_quantity"),
    F.countDistinct(
        F.when(
            F.col("item_discount_amount") > 0,
            F.col("order_id")
        )
    ).alias("promotion_order_count"),
    F.round(
        F.sum(
            F.col("line_total").cast("double")
        ),
        2
    ).alias("sales_revenue")
)


historical_item_demand = daily_item_demand.groupBy(
    "restaurant_id",
    "item_id"
).agg(
    F.sum(
        "demand_quantity"
    ).alias("historical_demand_quantity")
)



restaurant_recipes = recipes.alias(
    "rec"
).join(
    restaurant_menu.alias(
        "rm"
    ),
    (
        F.col("rec.menu_item_id") ==
        F.col("rm.item_id")
    ) &
    (
        F.col("rm.is_available") == True
    ),
    "inner"
).join(
    item_master.alias(
        "im"
    ),
    F.col("rec.menu_item_id") ==
    F.col("im.item_id"),
    "inner"
).select(
    F.col("rm.restaurant_id").alias("restaurant_id"),
    F.col("rec.menu_item_id").alias("item_id"),
    F.col("im.item_name").alias("item_name"),
    F.col("im.category_id").alias("category_id"),
    F.col("im.category_name").alias("category_name"),
    F.col("im.item_cost").alias("item_cost"),
    F.col("rec.ingredient_id").alias("ingredient_id"),
    F.col("rec.ingredient_name").alias("ingredient_name"),
    F.col("rec.quantity_required").cast("double").alias("quantity_required"),
    F.col("rec.unit").alias("ingredient_unit")
)



wastage_base = wastage.select(
    "wastage_id",
    "restaurant_id",
    "ingredient_id",
    "inventory_id",
    "wastage_date",
    F.col("quantity").cast("double").alias("wastage_quantity"),
    "unit",
    F.col("unit_cost").cast("double").alias("unit_cost"),
    F.col("cost").cast("double").alias("wastage_cost"),
    "reason",
    "ingredient_name"
)


wastage_candidates = wastage_base.alias(
    "w"
).join(
    restaurant_recipes.alias(
        "rr"
    ),
    (
        F.col("w.restaurant_id") ==
        F.col("rr.restaurant_id")
    ) &
    (
        F.col("w.ingredient_id") ==
        F.col("rr.ingredient_id")
    ),
    "inner"
).select(
    F.col("w.wastage_id"),
    F.col("w.restaurant_id"),
    F.col("w.ingredient_id"),
    F.col("w.inventory_id"),
    F.col("w.wastage_date"),
    F.col("w.wastage_quantity"),
    F.col("w.unit"),
    F.col("w.unit_cost"),
    F.col("w.wastage_cost"),
    F.col("w.reason"),
    F.col("w.ingredient_name"),
    F.col("rr.item_id"),
    F.col("rr.item_name"),
    F.col("rr.category_id"),
    F.col("rr.category_name"),
    F.col("rr.item_cost"),
    F.col("rr.quantity_required")
)


same_day_demand = daily_item_demand.select(
    F.col("analysis_date").alias("demand_date"),
    F.col("restaurant_id").alias("demand_restaurant_id"),
    F.col("item_id").alias("demand_item_id"),
    F.col("demand_quantity").alias("same_day_demand")
)


wastage_candidates = wastage_candidates.alias(
    "wc"
).join(
    same_day_demand.alias(
        "dd"
    ),
    (
        F.col("wc.wastage_date") ==
        F.col("dd.demand_date")
    ) &
    (
        F.col("wc.restaurant_id") ==
        F.col("dd.demand_restaurant_id")
    ) &
    (
        F.col("wc.item_id") ==
        F.col("dd.demand_item_id")
    ),
    "left"
).drop(
    "demand_date",
    "demand_restaurant_id",
    "demand_item_id"
)


historical_demand_lookup = historical_item_demand.select(
    F.col("restaurant_id").alias("history_restaurant_id"),
    F.col("item_id").alias("history_item_id"),
    "historical_demand_quantity"
)


wastage_candidates = wastage_candidates.alias(
    "wc"
).join(
    historical_demand_lookup.alias(
        "hd"
    ),
    (
        F.col("wc.restaurant_id") ==
        F.col("hd.history_restaurant_id")
    ) &
    (
        F.col("wc.item_id") ==
        F.col("hd.history_item_id")
    ),
    "left"
).drop(
    "history_restaurant_id",
    "history_item_id"
)


wastage_candidates = wastage_candidates.withColumn(
    "daily_recipe_consumption_weight",
    F.coalesce(
        F.col("same_day_demand"),
        F.lit(0.0)
    ) *
    F.col("quantity_required")
).withColumn(
    "historical_recipe_consumption_weight",
    F.coalesce(
        F.col("historical_demand_quantity"),
        F.lit(0.0)
    ) *
    F.col("quantity_required")
).withColumn(
    "allocation_weight",
    F.when(
        F.col("daily_recipe_consumption_weight") > 0,
        F.col("daily_recipe_consumption_weight")
    ).when(
        F.col("historical_recipe_consumption_weight") > 0,
        F.col("historical_recipe_consumption_weight")
    ).otherwise(
        F.lit(1.0)
    )
)


allocation_window = Window.partitionBy(
    "wastage_id"
)


wastage_item_allocation = wastage_candidates.withColumn(
    "total_allocation_weight",
    F.sum(
        "allocation_weight"
    ).over(
        allocation_window
    )
).withColumn(
    "allocation_share",
    F.col("allocation_weight") /
    F.col("total_allocation_weight")
).withColumn(
    "allocated_wastage_quantity",
    F.col("wastage_quantity") *
    F.col("allocation_share")
).withColumn(
    "allocated_wastage_cost",
    F.col("wastage_cost") *
    F.col("allocation_share")
).withColumn(
    "estimated_wasted_servings",
    F.when(
        F.col("item_cost") > 0,
        F.col("allocated_wastage_cost") /
        F.col("item_cost")
    ).otherwise(
        F.lit(0.0)
    )
).select(
    "wastage_id",
    "wastage_date",
    "restaurant_id",
    "item_id",
    "item_name",
    "category_id",
    "category_name",
    "ingredient_id",
    "ingredient_name",
    F.col("unit").alias("ingredient_unit"),
    "reason",
    "quantity_required",
    "same_day_demand",
    "historical_demand_quantity",
    "allocation_weight",
    "allocation_share",
    "allocated_wastage_quantity",
    "allocated_wastage_cost",
    "estimated_wasted_servings"
)



allocated_daily_wastage = wastage_item_allocation.groupBy(
    F.col("wastage_date").alias("analysis_date"),
    "restaurant_id",
    "item_id"
).agg(
    F.sum(
        "allocated_wastage_cost"
    ).alias("estimated_wastage_cost"),
    F.sum(
        "estimated_wasted_servings"
    ).alias("estimated_wasted_servings"),
    F.countDistinct(
        "wastage_id"
    ).alias("wastage_event_count")
)


item_daily_wastage = daily_item_demand.alias(
    "d"
).join(
    allocated_daily_wastage.alias(
        "w"
    ),
    [
        "analysis_date",
        "restaurant_id",
        "item_id"
    ],
    "full"
).join(
    item_master.alias(
        "im"
    ),
    "item_id",
    "left"
).join(
    location_master.alias(
        "lm"
    ),
    "restaurant_id",
    "left"
).select(
    "analysis_date",
    "restaurant_id",
    F.col("lm.restaurant_name").alias("restaurant_name"),
    F.col("lm.restaurant_city").alias("restaurant_city"),
    F.col("lm.restaurant_area").alias("restaurant_area"),
    "item_id",
    F.col("im.item_name").alias("item_name"),
    F.col("im.category_id").alias("category_id"),
    F.col("im.category_name").alias("category_name"),
    F.col("im.item_cost").alias("item_cost"),
    F.coalesce(F.col("d.demand_quantity"), F.lit(0.0)).alias("demand_quantity"),
    F.coalesce(F.col("d.order_count"), F.lit(0)).alias("order_count"),
    F.coalesce(F.col("d.promotion_quantity"), F.lit(0.0)).alias("promotion_quantity"),
    F.coalesce(F.col("d.promotion_order_count"), F.lit(0)).alias("promotion_order_count"),
    F.coalesce(F.col("d.sales_revenue"), F.lit(0.0)).alias("sales_revenue"),
    F.coalesce(F.col("w.estimated_wastage_cost"), F.lit(0.0)).alias("estimated_wastage_cost"),
    F.coalesce(F.col("w.estimated_wasted_servings"), F.lit(0.0)).alias("estimated_wasted_servings"),
    F.coalesce(F.col("w.wastage_event_count"), F.lit(0)).alias("wastage_event_count")
).withColumn(
    "estimated_inventory_consumption_cost",
    F.col("demand_quantity") *
    F.coalesce(
        F.col("item_cost"),
        F.lit(0.0)
    )
).withColumn(
    "estimated_preparation_quantity",
    F.col("demand_quantity") +
    F.col("estimated_wasted_servings")
).withColumn(
    "preparation_surplus_quantity",
    F.col("estimated_wasted_servings")
).withColumn(
    "preparation_surplus_pct",
    F.when(
        F.col("estimated_preparation_quantity") > 0,
        F.round(
            (
                F.col("preparation_surplus_quantity") /
                F.col("estimated_preparation_quantity")
            ) * 100,
            2
        )
    ).otherwise(
        F.lit(0.0)
    )
).withColumn(
    "promotion_share_pct",
    F.when(
        F.col("demand_quantity") > 0,
        F.round(
            (
                F.col("promotion_quantity") /
                F.col("demand_quantity")
            ) * 100,
            2
        )
    ).otherwise(
        F.lit(0.0)
    )
).withColumn(
    "promotion_status",
    F.when(
        F.col("promotion_quantity") > 0,
        F.lit("Promotion Active")
    ).otherwise(
        F.lit("No Promotion")
    )
).withColumn(
    "wastage_cost_per_demand_unit",
    F.when(
        F.col("demand_quantity") > 0,
        F.round(
            F.col("estimated_wastage_cost") /
            F.col("demand_quantity"),
            4
        )
    ).otherwise(
        F.lit(0.0)
    )
).withColumn(
    "day_of_week_number",
    F.dayofweek("analysis_date")
).withColumn(
    "day_of_week",
    F.date_format("analysis_date", "EEEE")
).withColumn(
    "year_month",
    F.date_format("analysis_date", "yyyy-MM")
).withColumn(
    "month_number",
    F.month("analysis_date")
).withColumn(
    "season",
    F.when(
        F.col("month_number").isin(12, 1, 2),
        "Winter"
    ).when(
        F.col("month_number").isin(3, 4, 5),
        "Spring"
    ).when(
        F.col("month_number").isin(6, 7, 8),
        "Summer"
    ).otherwise(
        "Autumn"
    )
)



menu_item_wastage = item_daily_wastage.groupBy(
    "item_id",
    "item_name",
    "category_id",
    "category_name"
).agg(
    F.sum("demand_quantity").alias("total_demand_quantity"),
    F.sum("order_count").alias("total_orders"),
    F.round(F.sum("sales_revenue"), 2).alias("total_sales_revenue"),
    F.round(F.sum("estimated_inventory_consumption_cost"), 2).alias("estimated_inventory_consumption_cost"),
    F.round(F.sum("estimated_wastage_cost"), 2).alias("estimated_wastage_cost"),
    F.round(F.sum("estimated_wasted_servings"), 2).alias("estimated_wasted_servings"),
    F.round(F.sum("estimated_preparation_quantity"), 2).alias("estimated_preparation_quantity"),
    F.sum("promotion_quantity").alias("promotion_quantity"),
    F.countDistinct("analysis_date").alias("active_days")
).withColumn(
    "wastage_cost_per_demand_unit",
    F.when(
        F.col("total_demand_quantity") > 0,
        F.round(
            F.col("estimated_wastage_cost") /
            F.col("total_demand_quantity"),
            4
        )
    ).otherwise(
        F.lit(0.0)
    )
).withColumn(
    "preparation_surplus_pct",
    F.when(
        F.col("estimated_preparation_quantity") > 0,
        F.round(
            (
                F.col("estimated_wasted_servings") /
                F.col("estimated_preparation_quantity")
            ) * 100,
            2
        )
    ).otherwise(
        F.lit(0.0)
    )
).withColumn(
    "promotion_share_pct",
    F.when(
        F.col("total_demand_quantity") > 0,
        F.round(
            (
                F.col("promotion_quantity") /
                F.col("total_demand_quantity")
            ) * 100,
            2
        )
    ).otherwise(
        F.lit(0.0)
    )
).orderBy(
    F.desc("estimated_wastage_cost")
)



category_wastage = item_daily_wastage.groupBy(
    "category_id",
    "category_name"
).agg(
    F.sum("demand_quantity").alias("total_demand_quantity"),
    F.round(F.sum("estimated_inventory_consumption_cost"), 2).alias("estimated_inventory_consumption_cost"),
    F.round(F.sum("estimated_wastage_cost"), 2).alias("estimated_wastage_cost"),
    F.round(F.sum("estimated_wasted_servings"), 2).alias("estimated_wasted_servings"),
    F.round(F.sum("estimated_preparation_quantity"), 2).alias("estimated_preparation_quantity"),
    F.sum("promotion_quantity").alias("promotion_quantity")
).withColumn(
    "preparation_surplus_pct",
    F.when(
        F.col("estimated_preparation_quantity") > 0,
        F.round(
            (
                F.col("estimated_wasted_servings") /
                F.col("estimated_preparation_quantity")
            ) * 100,
            2
        )
    ).otherwise(
        F.lit(0.0)
    )
).withColumn(
    "wastage_cost_per_demand_unit",
    F.when(
        F.col("total_demand_quantity") > 0,
        F.round(
            F.col("estimated_wastage_cost") /
            F.col("total_demand_quantity"),
            4
        )
    ).otherwise(
        F.lit(0.0)
    )
).orderBy(
    F.desc("estimated_wastage_cost")
)



location_wastage = item_daily_wastage.groupBy(
    "restaurant_id",
    "restaurant_name",
    "restaurant_city",
    "restaurant_area"
).agg(
    F.sum("demand_quantity").alias("total_demand_quantity"),
    F.round(F.sum("sales_revenue"), 2).alias("total_sales_revenue"),
    F.round(F.sum("estimated_inventory_consumption_cost"), 2).alias("estimated_inventory_consumption_cost"),
    F.round(F.sum("estimated_wastage_cost"), 2).alias("estimated_wastage_cost"),
    F.round(F.sum("estimated_wasted_servings"), 2).alias("estimated_wasted_servings"),
    F.round(F.sum("estimated_preparation_quantity"), 2).alias("estimated_preparation_quantity")
).withColumn(
    "wastage_cost_per_demand_unit",
    F.when(
        F.col("total_demand_quantity") > 0,
        F.round(
            F.col("estimated_wastage_cost") /
            F.col("total_demand_quantity"),
            4
        )
    ).otherwise(
        F.lit(0.0)
    )
).withColumn(
    "preparation_surplus_pct",
    F.when(
        F.col("estimated_preparation_quantity") > 0,
        F.round(
            (
                F.col("estimated_wasted_servings") /
                F.col("estimated_preparation_quantity")
            ) * 100,
            2
        )
    ).otherwise(
        F.lit(0.0)
    )
).orderBy(
    F.desc("estimated_wastage_cost")
)


daily_wastage = item_daily_wastage.groupBy(
    "analysis_date"
).agg(
    F.sum("demand_quantity").alias("demand_quantity"),
    F.round(F.sum("estimated_wastage_cost"), 2).alias("estimated_wastage_cost"),
    F.round(F.sum("estimated_wasted_servings"), 2).alias("estimated_wasted_servings"),
    F.round(F.sum("estimated_preparation_quantity"), 2).alias("estimated_preparation_quantity"),
    F.sum("promotion_quantity").alias("promotion_quantity")
).orderBy(
    "analysis_date"
)


day_of_week_wastage = item_daily_wastage.groupBy(
    "day_of_week_number",
    "day_of_week"
).agg(
    F.sum("demand_quantity").alias("total_demand_quantity"),
    F.round(F.sum("estimated_wastage_cost"), 2).alias("estimated_wastage_cost"),
    F.round(F.avg("estimated_wastage_cost"), 2).alias("average_item_day_wastage_cost"),
    F.round(F.sum("estimated_preparation_quantity"), 2).alias("estimated_preparation_quantity")
).orderBy(
    "day_of_week_number"
)


monthly_wastage = item_daily_wastage.groupBy(
    "year_month"
).agg(
    F.sum("demand_quantity").alias("total_demand_quantity"),
    F.round(F.sum("estimated_wastage_cost"), 2).alias("estimated_wastage_cost"),
    F.round(F.sum("estimated_wasted_servings"), 2).alias("estimated_wasted_servings"),
    F.round(F.sum("estimated_preparation_quantity"), 2).alias("estimated_preparation_quantity"),
    F.sum("promotion_quantity").alias("promotion_quantity")
).orderBy(
    "year_month"
)


seasonal_wastage = item_daily_wastage.groupBy(
    "season"
).agg(
    F.sum("demand_quantity").alias("total_demand_quantity"),
    F.round(F.sum("estimated_wastage_cost"), 2).alias("estimated_wastage_cost"),
    F.round(F.sum("estimated_wasted_servings"), 2).alias("estimated_wasted_servings"),
    F.round(F.sum("estimated_preparation_quantity"), 2).alias("estimated_preparation_quantity")
).withColumn(
    "preparation_surplus_pct",
    F.when(
        F.col("estimated_preparation_quantity") > 0,
        F.round(
            (
                F.col("estimated_wasted_servings") /
                F.col("estimated_preparation_quantity")
            ) * 100,
            2
        )
    ).otherwise(
        F.lit(0.0)
    )
).orderBy(
    F.desc("estimated_wastage_cost")
)


ingredient_wastage = wastage_base.groupBy(
    "ingredient_id",
    "ingredient_name",
    "unit"
).agg(
    F.countDistinct("wastage_id").alias("wastage_events"),
    F.round(F.sum("wastage_quantity"), 2).alias("wastage_quantity"),
    F.round(F.sum("wastage_cost"), 2).alias("wastage_cost")
).orderBy(
    F.desc("wastage_cost")
)


reason_wastage = wastage_base.groupBy(
    "reason"
).agg(
    F.countDistinct("wastage_id").alias("wastage_events"),
    F.round(F.sum("wastage_cost"), 2).alias("wastage_cost"),
    F.round(F.avg("wastage_cost"), 2).alias("average_event_cost")
).orderBy(
    F.desc("wastage_cost")
)



wastage_by_inventory = wastage_base.groupBy(
    "inventory_id"
).agg(
    F.sum("wastage_quantity").alias("inventory_wastage_quantity"),
    F.sum("wastage_cost").alias("inventory_wastage_cost"),
    F.countDistinct("wastage_id").alias("inventory_wastage_events")
)


inventory_consumption_relationship = inventory.alias(
    "inv"
).join(
    wastage_by_inventory.alias(
        "w"
    ),
    "inventory_id",
    "left"
).select(
    "inventory_id",
    F.col("inv.restaurant_id").alias("restaurant_id"),
    F.col("inv.restaurant_name").alias("restaurant_name"),
    F.col("inv.restaurant_city").alias("restaurant_city"),
    F.col("inv.restaurant_area").alias("restaurant_area"),
    F.col("inv.ingredient_id").alias("ingredient_id"),
    F.col("inv.ingredient_name").alias("ingredient_name"),
    F.col("inv.unit").alias("unit"),
    F.col("inv.received_date").alias("received_date"),
    F.col("inv.expiry_date").alias("expiry_date"),
    F.col("inv.quantity_received").cast("double").alias("quantity_received"),
    F.col("inv.quantity_remaining").cast("double").alias("reported_quantity_remaining"),
    F.col("inv.unit_cost").cast("double").alias("unit_cost"),
    F.coalesce(F.col("w.inventory_wastage_quantity"), F.lit(0.0)).alias("wastage_quantity"),
    F.coalesce(F.col("w.inventory_wastage_cost"), F.lit(0.0)).alias("wastage_cost"),
    F.coalesce(F.col("w.inventory_wastage_events"), F.lit(0)).alias("wastage_events")
).withColumn(
    "operational_consumption_quantity",
    F.col("quantity_received") -
    F.col("reported_quantity_remaining")
).withColumn(
    "operational_consumption_cost",
    F.col("operational_consumption_quantity") *
    F.col("unit_cost")
).withColumn(
    "adjusted_quantity_remaining",
    F.greatest(
        F.col("reported_quantity_remaining") -
        F.col("wastage_quantity"),
        F.lit(0.0)
    )
).withColumn(
    "total_depleted_quantity",
    F.col("operational_consumption_quantity") +
    F.col("wastage_quantity")
).withColumn(
    "wastage_pct_of_received",
    F.when(
        F.col("quantity_received") > 0,
        F.round(
            (
                F.col("wastage_quantity") /
                F.col("quantity_received")
            ) * 100,
            2
        )
    ).otherwise(
        F.lit(0.0)
    )
).withColumn(
    "wastage_share_of_depletion_pct",
    F.when(
        F.col("total_depleted_quantity") > 0,
        F.round(
            (
                F.col("wastage_quantity") /
                F.col("total_depleted_quantity")
            ) * 100,
            2
        )
    ).otherwise(
        F.lit(0.0)
    )
)


inventory_consumption_summary = inventory_consumption_relationship.groupBy(
    "restaurant_id",
    "restaurant_name",
    "ingredient_id",
    "ingredient_name",
    "unit"
).agg(
    F.round(F.sum("quantity_received"), 2).alias("quantity_received"),
    F.round(F.sum("operational_consumption_quantity"), 2).alias("operational_consumption_quantity"),
    F.round(F.sum("wastage_quantity"), 2).alias("wastage_quantity"),
    F.round(F.sum("adjusted_quantity_remaining"), 2).alias("adjusted_quantity_remaining"),
    F.round(F.sum("operational_consumption_cost"), 2).alias("operational_consumption_cost"),
    F.round(F.sum("wastage_cost"), 2).alias("wastage_cost")
).withColumn(
    "wastage_pct_of_received",
    F.when(
        F.col("quantity_received") > 0,
        F.round(
            (
                F.col("wastage_quantity") /
                F.col("quantity_received")
            ) * 100,
            2
        )
    ).otherwise(
        F.lit(0.0)
    )
).orderBy(
    F.desc("wastage_cost")
)


promotion_wastage = item_daily_wastage.groupBy(
    "promotion_status"
).agg(
    F.count("*").alias("item_day_records"),
    F.sum("demand_quantity").alias("total_demand_quantity"),
    F.sum("promotion_quantity").alias("promotion_quantity"),
    F.round(F.sum("estimated_wastage_cost"), 2).alias("estimated_wastage_cost"),
    F.round(F.avg("estimated_wastage_cost"), 2).alias("average_item_day_wastage_cost"),
    F.round(F.sum("estimated_preparation_quantity"), 2).alias("estimated_preparation_quantity")
).withColumn(
    "preparation_surplus_pct",
    F.when(
        F.col("estimated_preparation_quantity") > 0,
        F.round(
            (
                (
                    F.col("estimated_preparation_quantity") -
                    F.col("total_demand_quantity")
                ) /
                F.col("estimated_preparation_quantity")
            ) * 100,
            2
        )
    ).otherwise(
        F.lit(0.0)
    )
).orderBy(
    "promotion_status"
)



wastage_relationships = item_daily_wastage.agg(
    F.round(
        F.corr(
            "demand_quantity",
            "estimated_wastage_cost"
        ),
        4
    ).alias("demand_wastage_cost_correlation"),
    F.round(
        F.corr(
            "estimated_inventory_consumption_cost",
            "estimated_wastage_cost"
        ),
        4
    ).alias("inventory_consumption_wastage_correlation"),
    F.round(
        F.corr(
            "estimated_preparation_quantity",
            "estimated_wastage_cost"
        ),
        4
    ).alias("preparation_wastage_correlation"),
    F.round(
        F.corr(
            "promotion_share_pct",
            "estimated_wastage_cost"
        ),
        4
    ).alias("promotion_wastage_correlation")
)



inventory_value = inventory_consumption_relationship.agg(
    F.sum(
        F.col("quantity_received") *
        F.col("unit_cost")
    ).alias("inventory_received_value"),
    F.sum(
        "operational_consumption_cost"
    ).alias("operational_consumption_cost")
)


wastage_totals = wastage_base.agg(
    F.countDistinct("wastage_id").alias("total_wastage_events"),
    F.round(F.sum("wastage_cost"), 2).alias("total_wastage_cost"),
    F.round(F.avg("wastage_cost"), 2).alias("average_wastage_event_cost")
)


wastage_summary = wastage_totals.crossJoin(
    inventory_value
).withColumn(
    "wastage_pct_of_inventory_received_value",
    F.when(
        F.col("inventory_received_value") > 0,
        F.round(
            (
                F.col("total_wastage_cost") /
                F.col("inventory_received_value")
            ) * 100,
            2
        )
    ).otherwise(
        F.lit(0.0)
    )
).select(
    "total_wastage_events",
    "total_wastage_cost",
    "average_wastage_event_cost",
    F.round(
        "inventory_received_value",
        2
    ).alias("inventory_received_value"),
    F.round(
        "operational_consumption_cost",
        2
    ).alias("operational_consumption_cost"),
    "wastage_pct_of_inventory_received_value"
)


analysis_metadata = spark.createDataFrame([
    (
        "ingredient_to_menu_item_allocation",
        "Each ingredient wastage event is allocated across restaurant menu items using same-day recipe consumption when available, otherwise historical recipe-weighted demand."
    ),
    (
        "preparation_quantity",
        "Estimated preparation quantity equals sold demand plus allocated wastage cost converted to item-cost-equivalent wasted servings."
    ),
    (
        "inventory_consumption",
        "Operational inventory consumption equals quantity received minus reported quantity remaining; wastage is tracked separately from the remaining stock."
    ),
    (
        "time_period",
        "Time-period analysis uses calendar month and season because wastage records contain a date but no time-of-day timestamp."
    ),
    (
        "promotion",
        "Promotion-active item-days are identified when sold quantity includes an item-level discount."
    )
], [
    "analysis_component",
    "method"
])


print("\n========WASTAGE OVERVIEW========")
wastage_summary.show(
    truncate=False
)


print("\n========TOP MENU ITEMS BY ESTIMATED WASTAGE COST========")
menu_item_wastage.select(
    "item_id",
    "item_name",
    "category_name",
    "total_demand_quantity",
    "estimated_wastage_cost",
    "estimated_wasted_servings",
    "estimated_preparation_quantity",
    "preparation_surplus_pct",
    "wastage_cost_per_demand_unit",
    "promotion_share_pct"
).show(
    20,
    truncate=False
)


print("\n========WASTAGE BY CATEGORY========")
category_wastage.show(
    20,
    truncate=False
)


print("\n========WASTAGE BY LOCATION========")
location_wastage.show(
    20,
    truncate=False
)


print("\n========WASTAGE BY DAY OF WEEK========")
day_of_week_wastage.show(
    7,
    truncate=False
)


print("\n========MONTHLY WASTAGE TREND========")
monthly_wastage.show(
    24,
    truncate=False
)


print("\n========SEASONAL WASTAGE========")
seasonal_wastage.show(
    4,
    truncate=False
)


print("\n========WASTAGE BY REASON========")
reason_wastage.show(
    truncate=False
)


print("\n========PROMOTION AND WASTAGE========")
promotion_wastage.show(
    truncate=False
)


print("\n========WASTAGE RELATIONSHIPS========")
wastage_relationships.show(
    truncate=False
)



outputs = {
    "wastage_summary": wastage_summary,
    "wastage_item_allocation": wastage_item_allocation,
    "item_daily_wastage": item_daily_wastage,
    "menu_item_wastage": menu_item_wastage,
    "category_wastage": category_wastage,
    "location_wastage": location_wastage,
    "daily_wastage": daily_wastage,
    "day_of_week_wastage": day_of_week_wastage,
    "monthly_wastage": monthly_wastage,
    "seasonal_wastage": seasonal_wastage,
    "ingredient_wastage": ingredient_wastage,
    "reason_wastage": reason_wastage,
    "inventory_consumption_relationship": inventory_consumption_relationship,
    "inventory_consumption_summary": inventory_consumption_summary,
    "promotion_wastage": promotion_wastage,
    "wastage_relationships": wastage_relationships,
    "analysis_metadata": analysis_metadata
}


for name, frame in outputs.items():
    frame.write.mode(
        "overwrite"
    ).parquet(
        f"{output_folder}/{name}"
    )


print("\nWastage analysis completed successfully.")


spark.stop()
