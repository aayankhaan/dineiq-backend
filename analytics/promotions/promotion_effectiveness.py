from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.settings import (
    PROCESSED_DATA_FOLDER,
    INTEGRATED_DATA_FOLDER,
    ANALYTICS_DATA_FOLDER
)


spark = SparkSession.builder \
    .appName("DineIQ Promotion Effectiveness") \
    .getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "8")


processed_folder = PROCESSED_DATA_FOLDER
integrated_folder = INTEGRATED_DATA_FOLDER
wastage_folder = f"{ANALYTICS_DATA_FOLDER}/wastage"
output_folder = f"{ANALYTICS_DATA_FOLDER}/promotion_effectiveness"


promotions = spark.read.parquet(
    f"{processed_folder}/promotions"
)

transactions = spark.read.parquet(
    f"{integrated_folder}/transactions"
)

item_daily_wastage = spark.read.parquet(
    f"{wastage_folder}/item_daily_wastage"
)


# ==================== TRANSACTION BASE ====================


transaction_base = transactions.filter(
    F.col("order_status") == "Completed"
).withColumn(
    "order_date",
    F.to_date("order_datetime")
).withColumn(
    "line_revenue",
    F.col("line_total").cast("double")
).withColumn(
    "line_cost",
    F.col("item_cost").cast("double") *
    F.col("quantity").cast("double")
).withColumn(
    "line_margin",
    F.col("line_revenue") -
    F.col("line_cost")
).withColumn(
    "line_discount",
    F.col("item_discount_amount").cast("double")
)


date_range = transaction_base.agg(
    F.min("order_date").alias("historical_start"),
    F.max("order_date").alias("historical_end")
).first()


historical_start = date_range["historical_start"]
historical_end = date_range["historical_end"]


item_master = transaction_base.groupBy(
    "item_id"
).agg(
    F.first(
        "item_name",
        ignorenulls=True
    ).alias(
        "item_name"
    ),
    F.first(
        "category_id",
        ignorenulls=True
    ).alias(
        "category_id"
    ),
    F.first(
        "category_name",
        ignorenulls=True
    ).alias(
        "category_name"
    )
)


location_master = transaction_base.groupBy(
    "restaurant_id"
).agg(
    F.first(
        "restaurant_name",
        ignorenulls=True
    ).alias(
        "restaurant_name"
    ),
    F.first(
        "restaurant_city",
        ignorenulls=True
    ).alias(
        "restaurant_city"
    ),
    F.first(
        "restaurant_area",
        ignorenulls=True
    ).alias(
        "restaurant_area"
    )
)


order_level = transaction_base.groupBy(
    "order_id",
    "customer_id",
    "restaurant_id",
    "promotion_id",
    "order_datetime",
    "order_date"
).agg(
    F.round(
        F.sum("line_revenue"),
        2
    ).alias(
        "order_revenue"
    ),
    F.round(
        F.sum("line_margin"),
        2
    ).alias(
        "order_contribution_margin"
    ),
    F.round(
        F.sum("line_discount"),
        2
    ).alias(
        "order_discount"
    ),
    F.sum(
        F.col("quantity").cast("double")
    ).alias(
        "order_item_quantity"
    )
)


# ==================== PROMOTION WINDOWS ====================


promotion_base = promotions.select(
    "promotion_id",
    F.col(
        "name"
    ).alias(
        "promotion_name"
    ),
    "discount_type",
    F.col(
        "discount_value"
    ).cast(
        "double"
    ).alias(
        "discount_value"
    ),
    F.to_date(
        "start_date"
    ).alias(
        "start_date"
    ),
    F.to_date(
        "end_date"
    ).alias(
        "end_date"
    ),
    "restaurant_id",
    "item_id",
    F.col(
        "minimum_order_value"
    ).cast(
        "double"
    ).alias(
        "minimum_order_value"
    ),
    "coupon_code"
).withColumn(
    "promotion_days",
    F.datediff(
        "end_date",
        "start_date"
    ) + 1
).withColumn(
    "pre_start_date",
    F.greatest(
        F.date_sub(
            "start_date",
            F.col("promotion_days")
        ),
        F.lit(historical_start)
    )
).withColumn(
    "pre_end_date",
    F.date_sub(
        "start_date",
        1
    )
).withColumn(
    "post_start_date",
    F.date_add(
        "end_date",
        1
    )
).withColumn(
    "post_end_date",
    F.least(
        F.date_add(
            "end_date",
            F.col("promotion_days")
        ),
        F.lit(historical_end)
    )
).withColumn(
    "pre_days",
    F.greatest(
        F.datediff(
            "pre_end_date",
            "pre_start_date"
        ) + 1,
        F.lit(0)
    )
).withColumn(
    "post_days",
    F.greatest(
        F.datediff(
            "post_end_date",
            "post_start_date"
        ) + 1,
        F.lit(0)
    )
).join(
    item_master,
    "item_id",
    "left"
).join(
    location_master,
    "restaurant_id",
    "left"
)


# ==================== TARGET ITEM PERIOD METRICS ====================


target_daily = transaction_base.groupBy(
    "order_date",
    "restaurant_id",
    "item_id"
).agg(
    F.countDistinct(
        "order_id"
    ).alias(
        "target_order_count"
    ),
    F.sum(
        F.col("quantity").cast("double")
    ).alias(
        "target_demand_quantity"
    ),
    F.round(
        F.sum("line_revenue"),
        2
    ).alias(
        "target_revenue"
    ),
    F.round(
        F.sum("line_margin"),
        2
    ).alias(
        "target_contribution_margin"
    ),
    F.countDistinct(
        "customer_id"
    ).alias(
        "target_customer_count"
    )
)


target_period_metrics = promotion_base.alias(
    "p"
).join(
    target_daily.alias(
        "d"
    ),
    (
        F.col("p.restaurant_id") ==
        F.col("d.restaurant_id")
    ) &
    (
        F.col("p.item_id") ==
        F.col("d.item_id")
    ) &
    (
        F.col("d.order_date") >=
        F.col("p.pre_start_date")
    ) &
    (
        F.col("d.order_date") <=
        F.col("p.post_end_date")
    ),
    "left"
).withColumn(
    "period",
    F.when(
        (
            F.col("d.order_date") >=
            F.col("p.pre_start_date")
        ) &
        (
            F.col("d.order_date") <=
            F.col("p.pre_end_date")
        ),
        "Pre"
    ).when(
        (
            F.col("d.order_date") >=
            F.col("p.start_date")
        ) &
        (
            F.col("d.order_date") <=
            F.col("p.end_date")
        ),
        "During"
    ).when(
        (
            F.col("d.order_date") >=
            F.col("p.post_start_date")
        ) &
        (
            F.col("d.order_date") <=
            F.col("p.post_end_date")
        ),
        "Post"
    )
).groupBy(
    F.col(
        "p.promotion_id"
    ).alias(
        "promotion_id"
    )
).agg(
    F.sum(
        F.when(
            F.col("period") == "Pre",
            F.col("d.target_order_count")
        ).otherwise(0)
    ).alias(
        "pre_target_orders"
    ),
    F.sum(
        F.when(
            F.col("period") == "During",
            F.col("d.target_order_count")
        ).otherwise(0)
    ).alias(
        "during_target_orders"
    ),
    F.sum(
        F.when(
            F.col("period") == "Post",
            F.col("d.target_order_count")
        ).otherwise(0)
    ).alias(
        "post_target_orders"
    ),
    F.sum(
        F.when(
            F.col("period") == "Pre",
            F.col("d.target_demand_quantity")
        ).otherwise(0.0)
    ).alias(
        "pre_target_demand"
    ),
    F.sum(
        F.when(
            F.col("period") == "During",
            F.col("d.target_demand_quantity")
        ).otherwise(0.0)
    ).alias(
        "during_target_demand"
    ),
    F.sum(
        F.when(
            F.col("period") == "Post",
            F.col("d.target_demand_quantity")
        ).otherwise(0.0)
    ).alias(
        "post_target_demand"
    ),
    F.sum(
        F.when(
            F.col("period") == "Pre",
            F.col("d.target_revenue")
        ).otherwise(0.0)
    ).alias(
        "pre_target_revenue"
    ),
    F.sum(
        F.when(
            F.col("period") == "During",
            F.col("d.target_revenue")
        ).otherwise(0.0)
    ).alias(
        "during_target_revenue"
    ),
    F.sum(
        F.when(
            F.col("period") == "Post",
            F.col("d.target_revenue")
        ).otherwise(0.0)
    ).alias(
        "post_target_revenue"
    ),
    F.sum(
        F.when(
            F.col("period") == "Pre",
            F.col("d.target_contribution_margin")
        ).otherwise(0.0)
    ).alias(
        "pre_target_margin"
    ),
    F.sum(
        F.when(
            F.col("period") == "During",
            F.col("d.target_contribution_margin")
        ).otherwise(0.0)
    ).alias(
        "during_target_margin"
    ),
    F.sum(
        F.when(
            F.col("period") == "Post",
            F.col("d.target_contribution_margin")
        ).otherwise(0.0)
    ).alias(
        "post_target_margin"
    )
)


# ==================== CAMPAIGN ORDER METRICS ====================


promotion_order_metrics = order_level.filter(
    F.col("promotion_id").isNotNull()
).groupBy(
    "promotion_id"
).agg(
    F.countDistinct(
        "order_id"
    ).alias(
        "promotion_order_count"
    ),
    F.countDistinct(
        "customer_id"
    ).alias(
        "promotion_customer_count"
    ),
    F.round(
        F.sum("order_revenue"),
        2
    ).alias(
        "promotion_revenue"
    ),
    F.round(
        F.sum(
            "order_contribution_margin"
        ),
        2
    ).alias(
        "promotion_contribution_margin"
    ),
    F.round(
        F.sum("order_discount"),
        2
    ).alias(
        "promotion_discount_amount"
    ),
    F.round(
        F.avg("order_revenue"),
        2
    ).alias(
        "promotion_average_order_value"
    )
)


# ==================== RESTAURANT AOV BASELINE ====================


restaurant_daily_orders = order_level.groupBy(
    "order_date",
    "restaurant_id"
).agg(
    F.countDistinct(
        "order_id"
    ).alias(
        "restaurant_order_count"
    ),
    F.sum(
        "order_revenue"
    ).alias(
        "restaurant_revenue"
    )
)


aov_period_metrics = promotion_base.alias(
    "p"
).join(
    restaurant_daily_orders.alias(
        "d"
    ),
    (
        F.col("p.restaurant_id") ==
        F.col("d.restaurant_id")
    ) &
    (
        F.col("d.order_date") >=
        F.col("p.pre_start_date")
    ) &
    (
        F.col("d.order_date") <=
        F.col("p.post_end_date")
    ),
    "left"
).withColumn(
    "period",
    F.when(
        (
            F.col("d.order_date") >=
            F.col("p.pre_start_date")
        ) &
        (
            F.col("d.order_date") <=
            F.col("p.pre_end_date")
        ),
        "Pre"
    ).when(
        (
            F.col("d.order_date") >=
            F.col("p.post_start_date")
        ) &
        (
            F.col("d.order_date") <=
            F.col("p.post_end_date")
        ),
        "Post"
    )
).groupBy(
    F.col(
        "p.promotion_id"
    ).alias(
        "promotion_id"
    )
).agg(
    F.sum(
        F.when(
            F.col("period") == "Pre",
            F.col(
                "d.restaurant_order_count"
            )
        ).otherwise(0)
    ).alias(
        "pre_restaurant_orders"
    ),
    F.sum(
        F.when(
            F.col("period") == "Pre",
            F.col(
                "d.restaurant_revenue"
            )
        ).otherwise(0.0)
    ).alias(
        "pre_restaurant_revenue"
    ),
    F.sum(
        F.when(
            F.col("period") == "Post",
            F.col(
                "d.restaurant_order_count"
            )
        ).otherwise(0)
    ).alias(
        "post_restaurant_orders"
    ),
    F.sum(
        F.when(
            F.col("period") == "Post",
            F.col(
                "d.restaurant_revenue"
            )
        ).otherwise(0.0)
    ).alias(
        "post_restaurant_revenue"
    )
).withColumn(
    "pre_restaurant_aov",
    F.when(
        F.col("pre_restaurant_orders") > 0,
        F.round(
            F.col(
                "pre_restaurant_revenue"
            ) /
            F.col(
                "pre_restaurant_orders"
            ),
            2
        )
    )
).withColumn(
    "post_restaurant_aov",
    F.when(
        F.col("post_restaurant_orders") > 0,
        F.round(
            F.col(
                "post_restaurant_revenue"
            ) /
            F.col(
                "post_restaurant_orders"
            ),
            2
        )
    )
)


# ==================== CUSTOMER ACQUISITION + REPEAT ====================


first_order_window = Window.partitionBy(
    "customer_id"
).orderBy(
    "order_datetime",
    "order_id"
)


first_customer_order = order_level.withColumn(
    "row_number",
    F.row_number().over(
        first_order_window
    )
).filter(
    F.col("row_number") == 1
).select(
    "customer_id",
    F.col(
        "order_id"
    ).alias(
        "first_order_id"
    ),
    F.col(
        "order_date"
    ).alias(
        "first_order_date"
    ),
    F.col(
        "promotion_id"
    ).alias(
        "first_order_promotion_id"
    )
)


promotion_customers = order_level.filter(
    F.col("promotion_id").isNotNull()
).groupBy(
    "promotion_id",
    "customer_id"
).agg(
    F.countDistinct(
        "order_id"
    ).alias(
        "promotion_orders_by_customer"
    ),
    F.min(
        "order_date"
    ).alias(
        "first_promotion_order_date"
    ),
    F.sum(
        "order_revenue"
    ).alias(
        "promotion_customer_revenue"
    )
).join(
    promotion_base.select(
        "promotion_id",
        "restaurant_id",
        "item_id",
        "start_date",
        "end_date",
        "post_start_date",
        "post_end_date"
    ),
    "promotion_id",
    "inner"
).join(
    first_customer_order,
    "customer_id",
    "left"
).withColumn(
    "acquired_customer",
    F.coalesce(
        (
            F.col(
                "first_order_promotion_id"
            ) ==
            F.col(
                "promotion_id"
            )
        ) &
        (
            F.col(
                "first_order_date"
            ) >=
            F.col(
                "start_date"
            )
        ) &
        (
            F.col(
                "first_order_date"
            ) <=
            F.col(
                "end_date"
            )
        ),
        F.lit(False)
    )
).withColumn(
    "repeat_during_promotion",
    F.col(
        "promotion_orders_by_customer"
    ) > 1
)


post_order_behavior = promotion_customers.alias(
    "pc"
).join(
    order_level.alias(
        "o"
    ),
    (
        F.col(
            "pc.customer_id"
        ) ==
        F.col(
            "o.customer_id"
        )
    ) &
    (
        F.col(
            "o.order_date"
        ) >=
        F.col(
            "pc.post_start_date"
        )
    ) &
    (
        F.col(
            "o.order_date"
        ) <=
        F.col(
            "pc.post_end_date"
        )
    ),
    "left"
).groupBy(
    F.col(
        "pc.promotion_id"
    ).alias(
        "promotion_id"
    ),
    F.col(
        "pc.customer_id"
    ).alias(
        "customer_id"
    )
).agg(
    F.countDistinct(
        F.col("o.order_id")
    ).alias(
        "post_promotion_orders"
    )
)


post_target_behavior = promotion_customers.alias(
    "pc"
).join(
    transaction_base.alias(
        "t"
    ),
    (
        F.col(
            "pc.customer_id"
        ) ==
        F.col(
            "t.customer_id"
        )
    ) &
    (
        F.col(
            "pc.restaurant_id"
        ) ==
        F.col(
            "t.restaurant_id"
        )
    ) &
    (
        F.col(
            "pc.item_id"
        ) ==
        F.col(
            "t.item_id"
        )
    ) &
    (
        F.col(
            "t.order_date"
        ) >=
        F.col(
            "pc.post_start_date"
        )
    ) &
    (
        F.col(
            "t.order_date"
        ) <=
        F.col(
            "pc.post_end_date"
        )
    ),
    "left"
).groupBy(
    F.col(
        "pc.promotion_id"
    ).alias(
        "promotion_id"
    ),
    F.col(
        "pc.customer_id"
    ).alias(
        "customer_id"
    )
).agg(
    F.countDistinct(
        F.col("t.order_id")
    ).alias(
        "post_target_item_orders"
    )
)


promotion_customer_behavior = promotion_customers.join(
    post_order_behavior,
    [
        "promotion_id",
        "customer_id"
    ],
    "left"
).join(
    post_target_behavior,
    [
        "promotion_id",
        "customer_id"
    ],
    "left"
).fillna(
    {
        "post_promotion_orders": 0,
        "post_target_item_orders": 0
    }
).withColumn(
    "returned_after_promotion",
    F.col(
        "post_promotion_orders"
    ) > 0
).withColumn(
    "repeated_target_item_after_promotion",
    F.col(
        "post_target_item_orders"
    ) > 0
)


customer_metrics = promotion_customer_behavior.groupBy(
    "promotion_id"
).agg(
    F.countDistinct(
        "customer_id"
    ).alias(
        "promotion_customers"
    ),
    F.sum(
        F.when(
            F.col("acquired_customer"),
            1
        ).otherwise(0)
    ).alias(
        "acquired_customers"
    ),
    F.sum(
        F.when(
            F.col("repeat_during_promotion"),
            1
        ).otherwise(0)
    ).alias(
        "repeat_purchase_customers"
    ),
    F.sum(
        F.when(
            F.col("returned_after_promotion"),
            1
        ).otherwise(0)
    ).alias(
        "post_promotion_return_customers"
    ),
    F.sum(
        F.when(
            F.col(
                "repeated_target_item_after_promotion"
            ),
            1
        ).otherwise(0)
    ).alias(
        "post_target_item_repeat_customers"
    )
).withColumn(
    "customer_acquisition_rate_pct",
    F.when(
        F.col("promotion_customers") > 0,
        F.round(
            (
                F.col("acquired_customers") /
                F.col("promotion_customers")
            ) * 100,
            2
        )
    ).otherwise(
        F.lit(0.0)
    )
).withColumn(
    "repeat_purchase_rate_pct",
    F.when(
        F.col("promotion_customers") > 0,
        F.round(
            (
                F.col(
                    "repeat_purchase_customers"
                ) /
                F.col(
                    "promotion_customers"
                )
            ) * 100,
            2
        )
    ).otherwise(
        F.lit(0.0)
    )
).withColumn(
    "post_promotion_return_rate_pct",
    F.when(
        F.col("promotion_customers") > 0,
        F.round(
            (
                F.col(
                    "post_promotion_return_customers"
                ) /
                F.col(
                    "promotion_customers"
                )
            ) * 100,
            2
        )
    ).otherwise(
        F.lit(0.0)
    )
).withColumn(
    "post_target_item_repeat_rate_pct",
    F.when(
        F.col("promotion_customers") > 0,
        F.round(
            (
                F.col(
                    "post_target_item_repeat_customers"
                ) /
                F.col(
                    "promotion_customers"
                )
            ) * 100,
            2
        )
    ).otherwise(
        F.lit(0.0)
    )
)


# ==================== WASTAGE ====================


wastage_period_metrics = promotion_base.alias(
    "p"
).join(
    item_daily_wastage.alias(
        "w"
    ),
    (
        F.col("p.restaurant_id") ==
        F.col("w.restaurant_id")
    ) &
    (
        F.col("p.item_id") ==
        F.col("w.item_id")
    ) &
    (
        F.col("w.analysis_date") >=
        F.col("p.pre_start_date")
    ) &
    (
        F.col("w.analysis_date") <=
        F.col("p.post_end_date")
    ),
    "left"
).withColumn(
    "period",
    F.when(
        (
            F.col("w.analysis_date") >=
            F.col("p.pre_start_date")
        ) &
        (
            F.col("w.analysis_date") <=
            F.col("p.pre_end_date")
        ),
        "Pre"
    ).when(
        (
            F.col("w.analysis_date") >=
            F.col("p.start_date")
        ) &
        (
            F.col("w.analysis_date") <=
            F.col("p.end_date")
        ),
        "During"
    ).when(
        (
            F.col("w.analysis_date") >=
            F.col("p.post_start_date")
        ) &
        (
            F.col("w.analysis_date") <=
            F.col("p.post_end_date")
        ),
        "Post"
    )
).groupBy(
    F.col(
        "p.promotion_id"
    ).alias(
        "promotion_id"
    )
).agg(
    F.sum(
        F.when(
            F.col("period") == "Pre",
            F.col(
                "w.estimated_wastage_cost"
            )
        ).otherwise(0.0)
    ).alias(
        "pre_estimated_wastage_cost"
    ),
    F.sum(
        F.when(
            F.col("period") == "During",
            F.col(
                "w.estimated_wastage_cost"
            )
        ).otherwise(0.0)
    ).alias(
        "during_estimated_wastage_cost"
    ),
    F.sum(
        F.when(
            F.col("period") == "Post",
            F.col(
                "w.estimated_wastage_cost"
            )
        ).otherwise(0.0)
    ).alias(
        "post_estimated_wastage_cost"
    )
)


# ==================== COMBINED EFFECTIVENESS ====================


promotion_effectiveness = promotion_base.join(
    target_period_metrics,
    "promotion_id",
    "left"
).join(
    promotion_order_metrics,
    "promotion_id",
    "left"
).join(
    aov_period_metrics,
    "promotion_id",
    "left"
).join(
    customer_metrics,
    "promotion_id",
    "left"
).join(
    wastage_period_metrics,
    "promotion_id",
    "left"
).fillna(
    {
        "pre_target_orders": 0,
        "during_target_orders": 0,
        "post_target_orders": 0,
        "pre_target_demand": 0.0,
        "during_target_demand": 0.0,
        "post_target_demand": 0.0,
        "pre_target_revenue": 0.0,
        "during_target_revenue": 0.0,
        "post_target_revenue": 0.0,
        "pre_target_margin": 0.0,
        "during_target_margin": 0.0,
        "post_target_margin": 0.0,
        "promotion_order_count": 0,
        "promotion_customer_count": 0,
        "promotion_revenue": 0.0,
        "promotion_contribution_margin": 0.0,
        "promotion_discount_amount": 0.0,
        "promotion_customers": 0,
        "acquired_customers": 0,
        "repeat_purchase_customers": 0,
        "post_promotion_return_customers": 0,
        "post_target_item_repeat_customers": 0,
        "customer_acquisition_rate_pct": 0.0,
        "repeat_purchase_rate_pct": 0.0,
        "post_promotion_return_rate_pct": 0.0,
        "post_target_item_repeat_rate_pct": 0.0,
        "pre_estimated_wastage_cost": 0.0,
        "during_estimated_wastage_cost": 0.0,
        "post_estimated_wastage_cost": 0.0
    }
).withColumn(
    "pre_daily_demand",
    F.when(
        F.col("pre_days") > 0,
        F.round(
            F.col("pre_target_demand") /
            F.col("pre_days"),
            4
        )
    )
).withColumn(
    "during_daily_demand",
    F.when(
        F.col("promotion_days") > 0,
        F.round(
            F.col("during_target_demand") /
            F.col("promotion_days"),
            4
        )
    )
).withColumn(
    "post_daily_demand",
    F.when(
        F.col("post_days") > 0,
        F.round(
            F.col("post_target_demand") /
            F.col("post_days"),
            4
        )
    )
).withColumn(
    "pre_daily_revenue",
    F.when(
        F.col("pre_days") > 0,
        F.col("pre_target_revenue") /
        F.col("pre_days")
    )
).withColumn(
    "during_daily_revenue",
    F.when(
        F.col("promotion_days") > 0,
        F.col("during_target_revenue") /
        F.col("promotion_days")
    )
).withColumn(
    "pre_daily_margin",
    F.when(
        F.col("pre_days") > 0,
        F.col("pre_target_margin") /
        F.col("pre_days")
    )
).withColumn(
    "during_daily_margin",
    F.when(
        F.col("promotion_days") > 0,
        F.col("during_target_margin") /
        F.col("promotion_days")
    )
).withColumn(
    "pre_daily_wastage_cost",
    F.when(
        F.col("pre_days") > 0,
        F.col(
            "pre_estimated_wastage_cost"
        ) /
        F.col("pre_days")
    )
).withColumn(
    "during_daily_wastage_cost",
    F.when(
        F.col("promotion_days") > 0,
        F.col(
            "during_estimated_wastage_cost"
        ) /
        F.col("promotion_days")
    )
).withColumn(
    "demand_lift_pct",
    F.when(
        F.col("pre_daily_demand") > 0,
        F.round(
            (
                (
                    F.col("during_daily_demand") -
                    F.col("pre_daily_demand")
                ) /
                F.col("pre_daily_demand")
            ) * 100,
            2
        )
    )
).withColumn(
    "revenue_lift_pct",
    F.when(
        F.col("pre_daily_revenue") > 0,
        F.round(
            (
                (
                    F.col("during_daily_revenue") -
                    F.col("pre_daily_revenue")
                ) /
                F.col("pre_daily_revenue")
            ) * 100,
            2
        )
    )
).withColumn(
    "margin_lift_pct",
    F.when(
        F.abs(
            F.col("pre_daily_margin")
        ) > 0,
        F.round(
            (
                (
                    F.col("during_daily_margin") -
                    F.col("pre_daily_margin")
                ) /
                F.abs(
                    F.col("pre_daily_margin")
                )
            ) * 100,
            2
        )
    )
).withColumn(
    "aov_change_pct",
    F.when(
        F.col("pre_restaurant_aov") > 0,
        F.round(
            (
                (
                    F.col(
                        "promotion_average_order_value"
                    ) -
                    F.col(
                        "pre_restaurant_aov"
                    )
                ) /
                F.col(
                    "pre_restaurant_aov"
                )
            ) * 100,
            2
        )
    )
).withColumn(
    "wastage_change_pct",
    F.when(
        F.col("pre_daily_wastage_cost") > 0,
        F.round(
            (
                (
                    F.col(
                        "during_daily_wastage_cost"
                    ) -
                    F.col(
                        "pre_daily_wastage_cost"
                    )
                ) /
                F.col(
                    "pre_daily_wastage_cost"
                )
            ) * 100,
            2
        )
    ).when(
        (
            F.col(
                "pre_daily_wastage_cost"
            ) == 0
        ) &
        (
            F.col(
                "during_daily_wastage_cost"
            ) > 0
        ),
        F.lit(100.0)
    ).otherwise(
        F.lit(0.0)
    )
).withColumn(
    "post_demand_vs_pre_pct",
    F.when(
        F.col("pre_daily_demand") > 0,
        F.round(
            (
                (
                    F.col("post_daily_demand") -
                    F.col("pre_daily_demand")
                ) /
                F.col("pre_daily_demand")
            ) * 100,
            2
        )
    )
).withColumn(
    "promotion_used",
    F.col(
        "promotion_order_count"
    ) > 0
)


# ==================== MULTI-KPI EFFECTIVENESS ====================


promotion_effectiveness = promotion_effectiveness.withColumn(
    "positive_kpi_count",
    F.when(
        F.coalesce(
            F.col("demand_lift_pct"),
            F.lit(0.0)
        ) > 0,
        1
    ).otherwise(0) +
    F.when(
        F.coalesce(
            F.col("revenue_lift_pct"),
            F.lit(0.0)
        ) > 0,
        1
    ).otherwise(0) +
    F.when(
        F.coalesce(
            F.col("margin_lift_pct"),
            F.lit(0.0)
        ) > 0,
        1
    ).otherwise(0) +
    F.when(
        F.coalesce(
            F.col("aov_change_pct"),
            F.lit(0.0)
        ) > 0,
        1
    ).otherwise(0) +
    F.when(
        F.col(
            "acquired_customers"
        ) > 0,
        1
    ).otherwise(0) +
    F.when(
        F.col(
            "post_promotion_return_customers"
        ) > 0,
        1
    ).otherwise(0) +
    F.when(
        F.col(
            "post_target_item_repeat_customers"
        ) > 0,
        1
    ).otherwise(0) +
    F.when(
        F.col(
            "wastage_change_pct"
        ) <= 0,
        1
    ).otherwise(0)
).withColumn(
    "promotion_assessment",
    F.when(
        ~F.col("promotion_used"),
        "No Usage"
    ).when(
        (
            F.col("positive_kpi_count") >= 5
        ) &
        (
            F.coalesce(
                F.col("margin_lift_pct"),
                F.lit(-999.0)
            ) > 0
        ) &
        (
            F.col("wastage_change_pct") <= 0
        ),
        "Effective"
    ).when(
        F.col("positive_kpi_count") >= 3,
        "Mixed"
    ).otherwise(
        "Ineffective"
    )
).withColumn(
    "assessment_reason",
    F.concat_ws(
        " | ",
        F.concat(
            F.lit("Positive KPIs: "),
            F.col(
                "positive_kpi_count"
            ).cast("string"),
            F.lit("/8")
        ),
        F.concat(
            F.lit("Margin lift: "),
            F.coalesce(
                F.col(
                    "margin_lift_pct"
                ).cast("string"),
                F.lit("N/A")
            ),
            F.lit("%")
        ),
        F.concat(
            F.lit("Acquired customers: "),
            F.col(
                "acquired_customers"
            ).cast("string")
        ),
        F.concat(
            F.lit("Post-promo return rate: "),
            F.col(
                "post_promotion_return_rate_pct"
            ).cast("string"),
            F.lit("%")
        ),
        F.concat(
            F.lit("Wastage change: "),
            F.col(
                "wastage_change_pct"
            ).cast("string"),
            F.lit("%")
        )
    )
)


# ==================== SUMMARY ====================


effectiveness_summary = promotion_effectiveness.groupBy(
    "promotion_assessment"
).agg(
    F.count(
        "*"
    ).alias(
        "promotion_count"
    ),
    F.sum(
        "promotion_order_count"
    ).alias(
        "promotion_orders"
    ),
    F.round(
        F.sum(
            "promotion_revenue"
        ),
        2
    ).alias(
        "promotion_revenue"
    ),
    F.round(
        F.sum(
            "promotion_contribution_margin"
        ),
        2
    ).alias(
        "promotion_contribution_margin"
    ),
    F.sum(
        "acquired_customers"
    ).alias(
        "acquired_customers"
    ),
    F.round(
        F.avg(
            "promotion_average_order_value"
        ),
        2
    ).alias(
        "average_order_value"
    ),
    F.round(
        F.avg(
            "post_promotion_return_rate_pct"
        ),
        2
    ).alias(
        "average_post_promotion_return_rate_pct"
    ),
    F.round(
        F.avg(
            "wastage_change_pct"
        ),
        2
    ).alias(
        "average_wastage_change_pct"
    )
).orderBy(
    F.desc(
        "promotion_count"
    )
)


analysis_metadata = spark.createDataFrame([
    (
        "comparison_windows",
        "Each promotion is compared with equal-length pre-promotion and post-promotion windows, clipped to the available transaction history."
    ),
    (
        "order_volume",
        "Promotion order volume counts distinct completed orders that actually carry the promotion ID."
    ),
    (
        "revenue_margin",
        "Promotion revenue and contribution margin are calculated from completed promotion-order line totals and menu-item costs."
    ),
    (
        "customer_acquisition",
        "An acquired customer is a customer whose first completed order in the dataset used the promotion."
    ),
    (
        "repeat_purchase",
        "Repeat behavior includes multiple orders during the campaign, any return order in the post-promotion window, and repeat purchase of the promoted item after the campaign."
    ),
    (
        "average_order_value",
        "Promotion AOV uses net item revenue per completed promotion order and is compared with the restaurant pre-promotion AOV."
    ),
    (
        "wastage",
        "Wastage uses Step 23 estimated menu-item wastage cost for the promoted item and location, normalized per day across pre, during, and post periods."
    ),
    (
        "effectiveness",
        "Effectiveness uses eight business indicators. A promotion cannot be classified Effective from sales growth alone: positive contribution-margin lift and non-increasing wastage are mandatory in addition to at least five positive indicators."
    ),
    (
        "causality",
        "Pre/during/post comparisons are descriptive historical evidence and do not prove that the promotion alone caused the observed changes."
    )
], [
    "analysis_component",
    "method"
])


# ==================== RESULTS ====================


print("\n========PROMOTION EFFECTIVENESS SUMMARY========")
effectiveness_summary.show(
    truncate=False
)


print("\n========TOP PROMOTION RESULTS========")
promotion_effectiveness.select(
    "promotion_id",
    "promotion_name",
    "restaurant_name",
    "item_name",
    "discount_type",
    "discount_value",
    "promotion_days",
    "promotion_order_count",
    "promotion_customer_count",
    "promotion_revenue",
    "promotion_contribution_margin",
    "promotion_average_order_value",
    "demand_lift_pct",
    "revenue_lift_pct",
    "margin_lift_pct",
    "aov_change_pct",
    "acquired_customers",
    "repeat_purchase_rate_pct",
    "post_promotion_return_rate_pct",
    "post_target_item_repeat_rate_pct",
    "wastage_change_pct",
    "post_demand_vs_pre_pct",
    "positive_kpi_count",
    "promotion_assessment"
).orderBy(
    F.desc(
        "positive_kpi_count"
    ),
    F.desc(
        "promotion_contribution_margin"
    )
).show(
    30,
    truncate=False
)


# ==================== SAVE OUTPUTS ====================


outputs = {
    "promotion_effectiveness": promotion_effectiveness,
    "promotion_customer_behavior": promotion_customer_behavior,
    "effectiveness_summary": effectiveness_summary,
    "analysis_metadata": analysis_metadata
}


for name, frame in outputs.items():
    frame.write.mode(
        "overwrite"
    ).parquet(
        f"{output_folder}/{name}"
    )


print("\nPromotion effectiveness analysis completed successfully.")


spark.stop()
