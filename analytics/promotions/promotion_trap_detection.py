from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.settings import INTEGRATED_DATA_FOLDER, ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("DineIQ Promotion Trap Detection") \
    .getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "8")


integrated_folder = INTEGRATED_DATA_FOLDER
promotion_folder = f"{ANALYTICS_DATA_FOLDER}/promotion_effectiveness"
output_folder = f"{ANALYTICS_DATA_FOLDER}/promotion_traps"

CUSTOMER_MARGIN_COLLAPSE_PCT = -20.0
DEPENDENCY_POST_DEMAND_PCT = -20.0
DEPENDENCY_REPEAT_RATE_PCT = 10.0
CANNIBALIZATION_DEMAND_DROP_PCT = -20.0


transactions = spark.read.parquet(
    f"{integrated_folder}/transactions"
)

promotion_effectiveness = spark.read.parquet(
    f"{promotion_folder}/promotion_effectiveness"
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
    "line_margin",
    F.col("line_total").cast("double") -
    (
        F.col("item_cost").cast("double") *
        F.col("quantity").cast("double")
    )
)


item_master = transaction_base.groupBy(
    "item_id"
).agg(
    F.first(
        "item_name",
        ignorenulls=True
    ).alias(
        "candidate_item_name"
    ),
    F.first(
        "category_name",
        ignorenulls=True
    ).alias(
        "candidate_category_name"
    )
)


# ==================== CUSTOMER + MARGIN TRAP EVIDENCE ====================


target_customer_metrics = promotion_effectiveness.alias(
    "p"
).join(
    transaction_base.alias(
        "t"
    ),
    (
        F.col("p.restaurant_id") ==
        F.col("t.restaurant_id")
    ) &
    (
        F.col("p.item_id") ==
        F.col("t.item_id")
    ) &
    (
        F.col("t.order_date") >=
        F.col("p.pre_start_date")
    ) &
    (
        F.col("t.order_date") <=
        F.col("p.end_date")
    ),
    "left"
).withColumn(
    "period",
    F.when(
        (
            F.col("t.order_date") >=
            F.col("p.pre_start_date")
        ) &
        (
            F.col("t.order_date") <=
            F.col("p.pre_end_date")
        ),
        "Pre"
    ).when(
        (
            F.col("t.order_date") >=
            F.col("p.start_date")
        ) &
        (
            F.col("t.order_date") <=
            F.col("p.end_date")
        ),
        "During"
    )
).groupBy(
    F.col(
        "p.promotion_id"
    ).alias(
        "promotion_id"
    )
).agg(
    F.countDistinct(
        F.when(
            F.col("period") == "Pre",
            F.col("t.customer_id")
        )
    ).alias(
        "pre_target_customers"
    ),
    F.countDistinct(
        F.when(
            F.col("period") == "During",
            F.col("t.customer_id")
        )
    ).alias(
        "during_target_customers"
    )
)


promotion_base = promotion_effectiveness.join(
    target_customer_metrics,
    "promotion_id",
    "left"
).fillna(
    {
        "pre_target_customers": 0,
        "during_target_customers": 0
    }
).withColumn(
    "pre_average_margin_per_target_order",
    F.when(
        F.col("pre_target_orders") > 0,
        F.round(
            F.col("pre_target_margin") /
            F.col("pre_target_orders"),
            4
        )
    )
).withColumn(
    "during_average_margin_per_target_order",
    F.when(
        F.col("during_target_orders") > 0,
        F.round(
            F.col("during_target_margin") /
            F.col("during_target_orders"),
            4
        )
    )
).withColumn(
    "average_margin_per_order_change_pct",
    F.when(
        F.col(
            "pre_average_margin_per_target_order"
        ) > 0,
        F.round(
            (
                (
                    F.col(
                        "during_average_margin_per_target_order"
                    ) -
                    F.col(
                        "pre_average_margin_per_target_order"
                    )
                ) /
                F.col(
                    "pre_average_margin_per_target_order"
                )
            ) * 100,
            2
        )
    )
).withColumn(
    "target_customer_change",
    F.col("during_target_customers") -
    F.col("pre_target_customers")
)


# ==================== CANNIBALIZATION EVIDENCE ====================


other_item_period_metrics = promotion_base.alias(
    "p"
).join(
    transaction_base.alias(
        "t"
    ),
    (
        F.col("p.restaurant_id") ==
        F.col("t.restaurant_id")
    ) &
    (
        F.col("p.item_id") !=
        F.col("t.item_id")
    ) &
    (
        F.col("t.order_date") >=
        F.col("p.pre_start_date")
    ) &
    (
        F.col("t.order_date") <=
        F.col("p.end_date")
    ),
    "inner"
).withColumn(
    "period",
    F.when(
        (
            F.col("t.order_date") >=
            F.col("p.pre_start_date")
        ) &
        (
            F.col("t.order_date") <=
            F.col("p.pre_end_date")
        ),
        "Pre"
    ).when(
        (
            F.col("t.order_date") >=
            F.col("p.start_date")
        ) &
        (
            F.col("t.order_date") <=
            F.col("p.end_date")
        ),
        "During"
    )
).groupBy(
    F.col(
        "p.promotion_id"
    ).alias(
        "promotion_id"
    ),
    F.col(
        "t.item_id"
    ).alias(
        "candidate_item_id"
    ),
    F.col(
        "t.category_id"
    ).alias(
        "candidate_category_id"
    )
).agg(
    F.first(
        F.col("p.category_id")
    ).alias(
        "promoted_category_id"
    ),
    F.first(
        F.col("p.pre_days")
    ).alias(
        "pre_days"
    ),
    F.first(
        F.col("p.promotion_days")
    ).alias(
        "promotion_days"
    ),
    F.first(
        F.col("p.during_daily_demand")
    ).alias(
        "promoted_during_daily_demand"
    ),
    F.first(
        F.col("p.pre_daily_demand")
    ).alias(
        "promoted_pre_daily_demand"
    ),
    F.first(
        F.col("p.demand_lift_pct")
    ).alias(
        "promoted_demand_lift_pct"
    ),
    F.first(
        F.col("p.during_target_margin")
    ).alias(
        "promoted_during_margin"
    ),
    F.first(
        F.col("p.during_target_demand")
    ).alias(
        "promoted_during_demand"
    ),
    F.sum(
        F.when(
            F.col("period") == "Pre",
            F.col("t.quantity").cast("double")
        ).otherwise(0.0)
    ).alias(
        "candidate_pre_demand"
    ),
    F.sum(
        F.when(
            F.col("period") == "During",
            F.col("t.quantity").cast("double")
        ).otherwise(0.0)
    ).alias(
        "candidate_during_demand"
    ),
    F.sum(
        F.when(
            F.col("period") == "Pre",
            F.col("t.line_margin")
        ).otherwise(0.0)
    ).alias(
        "candidate_pre_margin"
    ),
    F.sum(
        F.when(
            F.col("period") == "During",
            F.col("t.line_margin")
        ).otherwise(0.0)
    ).alias(
        "candidate_during_margin"
    )
).withColumn(
    "candidate_pre_daily_demand",
    F.when(
        F.col("pre_days") > 0,
        F.col("candidate_pre_demand") /
        F.col("pre_days")
    )
).withColumn(
    "candidate_during_daily_demand",
    F.when(
        F.col("promotion_days") > 0,
        F.col("candidate_during_demand") /
        F.col("promotion_days")
    )
).withColumn(
    "candidate_demand_change_pct",
    F.when(
        F.col("candidate_pre_daily_demand") > 0,
        F.round(
            (
                (
                    F.col("candidate_during_daily_demand") -
                    F.col("candidate_pre_daily_demand")
                ) /
                F.col("candidate_pre_daily_demand")
            ) * 100,
            2
        )
    )
).withColumn(
    "candidate_pre_margin_per_unit",
    F.when(
        F.col("candidate_pre_demand") > 0,
        F.col("candidate_pre_margin") /
        F.col("candidate_pre_demand")
    )
).withColumn(
    "promoted_during_margin_per_unit",
    F.when(
        F.col("promoted_during_demand") > 0,
        F.col("promoted_during_margin") /
        F.col("promoted_during_demand")
    )
).withColumn(
    "candidate_pre_daily_margin",
    F.when(
        F.col("pre_days") > 0,
        F.col("candidate_pre_margin") /
        F.col("pre_days")
    )
).withColumn(
    "candidate_during_daily_margin",
    F.when(
        F.col("promotion_days") > 0,
        F.col("candidate_during_margin") /
        F.col("promotion_days")
    )
).withColumn(
    "estimated_lost_daily_margin",
    F.col("candidate_pre_daily_margin") -
    F.col("candidate_during_daily_margin")
).withColumn(
    "candidate_margin_advantage",
    F.col("candidate_pre_margin_per_unit") -
    F.col("promoted_during_margin_per_unit")
).join(
    item_master,
    F.col("candidate_item_id") ==
    F.col("item_id"),
    "left"
).drop(
    "item_id"
)


cannibalization_candidates = other_item_period_metrics.filter(
    (
        F.col("promoted_demand_lift_pct") > 0
    ) &
    (
        F.col("candidate_category_id") ==
        F.col("promoted_category_id")
    ) &
    (
        F.col("candidate_pre_demand") >= 3
    ) &
    (
        F.col("candidate_demand_change_pct") <=
        F.lit(CANNIBALIZATION_DEMAND_DROP_PCT)
    ) &
    (
        F.col("candidate_margin_advantage") > 0
    ) &
    (
        F.col("estimated_lost_daily_margin") > 0
    )
)


cannibalization_window = Window.partitionBy(
    "promotion_id"
).orderBy(
    F.desc(
        "estimated_lost_daily_margin"
    ),
    F.asc(
        "candidate_item_id"
    )
)


top_cannibalization = cannibalization_candidates.withColumn(
    "row_number",
    F.row_number().over(
        cannibalization_window
    )
).filter(
    F.col("row_number") == 1
).select(
    "promotion_id",
    "candidate_item_id",
    "candidate_item_name",
    "candidate_category_id",
    "candidate_category_name",
    "promoted_category_id",
    F.round(
        "candidate_pre_daily_demand",
        4
    ).alias(
        "candidate_pre_daily_demand"
    ),
    F.round(
        "candidate_during_daily_demand",
        4
    ).alias(
        "candidate_during_daily_demand"
    ),
    "candidate_demand_change_pct",
    F.round(
        "candidate_pre_margin_per_unit",
        4
    ).alias(
        "candidate_pre_margin_per_unit"
    ),
    F.round(
        "promoted_during_margin_per_unit",
        4
    ).alias(
        "promoted_during_margin_per_unit"
    ),
    F.round(
        "candidate_margin_advantage",
        4
    ).alias(
        "candidate_margin_advantage"
    ),
    F.round(
        "estimated_lost_daily_margin",
        4
    ).alias(
        "estimated_lost_daily_margin"
    )
)


# ==================== TRAP FLAGS ====================


promotion_traps = promotion_base.join(
    top_cannibalization,
    "promotion_id",
    "left"
).withColumn(
    "sales_profit_trap",
    F.coalesce(
        F.col("promotion_used") &
        (
            (
                F.col("demand_lift_pct") > 0
            ) |
            (
                F.col("revenue_lift_pct") > 0
            )
        ) &
        (
            F.col("margin_lift_pct") < 0
        ),
        F.lit(False)
    )
).withColumn(
    "customer_margin_trap",
    F.coalesce(
        F.col("promotion_used") &
        (
            F.col("during_target_customers") >
            F.col("pre_target_customers")
        ) &
        (
            F.col(
                "average_margin_per_order_change_pct"
            ) <=
            F.lit(
                CUSTOMER_MARGIN_COLLAPSE_PCT
            )
        ),
        F.lit(False)
    )
).withColumn(
    "wastage_trap",
    F.coalesce(
        F.col("promotion_used") &
        (
            F.col("during_daily_wastage_cost") >
            F.col("pre_daily_wastage_cost")
        ),
        F.lit(False)
    )
).withColumn(
    "discount_dependency_trap",
    F.coalesce(
        F.col("promotion_used") &
        (
            F.col("promotion_order_count") >= 5
        ) &
        (
            F.col("post_days") >= 3
        ) &
        (
            F.col("during_daily_demand") >
            F.col("pre_daily_demand")
        ) &
        (
            F.col("post_demand_vs_pre_pct") <=
            F.lit(
                DEPENDENCY_POST_DEMAND_PCT
            )
        ) &
        (
            F.col(
                "post_target_item_repeat_rate_pct"
            ) <=
            F.lit(
                DEPENDENCY_REPEAT_RATE_PCT
            )
        ),
        F.lit(False)
    )
).withColumn(
    "cannibalization_trap",
    F.coalesce(
        F.col("promotion_used") &
        F.col("candidate_item_id").isNotNull(),
        F.lit(False)
    )
).withColumn(
    "trap_count",
    F.col("sales_profit_trap").cast("int") +
    F.col("customer_margin_trap").cast("int") +
    F.col("wastage_trap").cast("int") +
    F.col("discount_dependency_trap").cast("int") +
    F.col("cannibalization_trap").cast("int")
).withColumn(
    "promotion_trap_detected",
    F.col("trap_count") > 0
).withColumn(
    "trap_severity",
    F.when(
        F.col("trap_count") >= 3,
        "Critical"
    ).when(
        F.col("trap_count") == 2,
        "High"
    ).when(
        F.col("trap_count") == 1,
        "Medium"
    ).otherwise(
        "None"
    )
).withColumn(
    "trap_types",
    F.concat_ws(
        ", ",
        F.when(
            F.col("sales_profit_trap"),
            F.lit("Sales Increase / Profit Decrease")
        ),
        F.when(
            F.col("customer_margin_trap"),
            F.lit("Customer Growth / Margin Collapse")
        ),
        F.when(
            F.col("wastage_trap"),
            F.lit("Wastage Increase")
        ),
        F.when(
            F.col("discount_dependency_trap"),
            F.lit("Discount Dependency")
        ),
        F.when(
            F.col("cannibalization_trap"),
            F.lit("Cannibalization")
        )
    )
).withColumn(
    "customer_margin_collapse_threshold_pct",
    F.lit(
        CUSTOMER_MARGIN_COLLAPSE_PCT
    )
).withColumn(
    "dependency_post_demand_threshold_pct",
    F.lit(
        DEPENDENCY_POST_DEMAND_PCT
    )
).withColumn(
    "dependency_repeat_rate_threshold_pct",
    F.lit(
        DEPENDENCY_REPEAT_RATE_PCT
    )
).withColumn(
    "cannibalization_demand_drop_threshold_pct",
    F.lit(
        CANNIBALIZATION_DEMAND_DROP_PCT
    )
)


# ==================== SUMMARY ====================


trap_summary = promotion_traps.agg(
    F.count(
        "*"
    ).alias(
        "total_promotions"
    ),
    F.sum(
        F.col(
            "promotion_trap_detected"
        ).cast("int")
    ).alias(
        "promotions_with_traps"
    ),
    F.sum(
        F.col(
            "sales_profit_trap"
        ).cast("int")
    ).alias(
        "sales_profit_traps"
    ),
    F.sum(
        F.col(
            "customer_margin_trap"
        ).cast("int")
    ).alias(
        "customer_margin_traps"
    ),
    F.sum(
        F.col(
            "wastage_trap"
        ).cast("int")
    ).alias(
        "wastage_traps"
    ),
    F.sum(
        F.col(
            "discount_dependency_trap"
        ).cast("int")
    ).alias(
        "discount_dependency_traps"
    ),
    F.sum(
        F.col(
            "cannibalization_trap"
        ).cast("int")
    ).alias(
        "cannibalization_traps"
    ),
    F.sum(
        F.when(
            F.col("trap_severity") == "Critical",
            1
        ).otherwise(0)
    ).alias(
        "critical_traps"
    ),
    F.sum(
        F.when(
            F.col("trap_severity") == "High",
            1
        ).otherwise(0)
    ).alias(
        "high_traps"
    ),
    F.sum(
        F.when(
            F.col("trap_severity") == "Medium",
            1
        ).otherwise(0)
    ).alias(
        "medium_traps"
    )
).withColumn(
    "trap_rate_pct",
    F.round(
        (
            F.col("promotions_with_traps") /
            F.col("total_promotions")
        ) * 100,
        2
    )
)


analysis_metadata = spark.createDataFrame([
    (
        "sales_profit_trap",
        "Flags a used promotion when demand or revenue increases but contribution-margin performance decreases."
    ),
    (
        "customer_margin_trap",
        f"Flags a used promotion when promoted-item customer count increases while average contribution margin per target-item order falls by at least {abs(CUSTOMER_MARGIN_COLLAPSE_PCT):.0f}%."
    ),
    (
        "wastage_trap",
        "Flags a used promotion when estimated daily promoted-item wastage cost is higher during the campaign than before it."
    ),
    (
        "discount_dependency_trap",
        f"Flags a used promotion with at least five orders when campaign demand exceeds pre-promotion demand, post-promotion demand falls at least {abs(DEPENDENCY_POST_DEMAND_PCT):.0f}% below the pre-promotion level, and no more than {DEPENDENCY_REPEAT_RATE_PCT:.0f}% of promotion customers repurchase the promoted item afterward."
    ),
    (
        "cannibalization_trap",
        f"Flags a used promotion when promoted-item demand rises while another item in the same menu category at the same restaurant had at least 3 pre-promotion units sold, loses at least {abs(CANNIBALIZATION_DEMAND_DROP_PCT):.0f}% daily demand, loses daily margin, and had higher pre-promotion margin per unit than the promoted item earned during the campaign."
    ),
    (
        "severity",
        "Trap severity is Medium for one detected trap, High for two, Critical for three or more, and None when no trap is detected."
    ),
    (
        "interpretation",
        "Promotion-trap flags are descriptive warning signals for investigation. They do not by themselves prove that the promotion caused every observed KPI change."
    )
], [
    "analysis_component",
    "method"
])


# ==================== RESULTS ====================


print("\n========PROMOTION TRAP SUMMARY========")
trap_summary.show(
    truncate=False
)


print("\n========TOP PROMOTION TRAPS========")
promotion_traps.filter(
    F.col("promotion_trap_detected")
).select(
    "promotion_id",
    "promotion_name",
    "restaurant_name",
    "item_name",
    "promotion_assessment",
    "trap_severity",
    "trap_count",
    "trap_types",
    "demand_lift_pct",
    "revenue_lift_pct",
    "margin_lift_pct",
    "pre_target_customers",
    "during_target_customers",
    "average_margin_per_order_change_pct",
    "wastage_change_pct",
    "post_demand_vs_pre_pct",
    "post_target_item_repeat_rate_pct",
    "candidate_item_name",
    "candidate_demand_change_pct",
    "candidate_margin_advantage",
    "estimated_lost_daily_margin"
).orderBy(
    F.desc("trap_count"),
    F.desc(
        "promotion_revenue"
    )
).show(
    30,
    truncate=False
)


# ==================== SAVE OUTPUTS ====================


outputs = {
    "promotion_traps": promotion_traps,
    "cannibalization_candidates": cannibalization_candidates,
    "trap_summary": trap_summary,
    "analysis_metadata": analysis_metadata
}


for name, frame in outputs.items():
    frame.write.mode(
        "overwrite"
    ).parquet(
        f"{output_folder}/{name}"
    )


print("\nPromotion trap detection completed successfully.")


spark.stop()
