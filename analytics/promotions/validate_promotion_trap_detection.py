from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("Validate DineIQ Promotion Traps") \
    .getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")


promotion_folder = f"{ANALYTICS_DATA_FOLDER}/promotion_effectiveness"
output_folder = f"{ANALYTICS_DATA_FOLDER}/promotion_traps"


promotion_effectiveness = spark.read.parquet(
    f"{promotion_folder}/promotion_effectiveness"
)

promotion_traps = spark.read.parquet(
    f"{output_folder}/promotion_traps"
)

cannibalization_candidates = spark.read.parquet(
    f"{output_folder}/cannibalization_candidates"
)

trap_summary = spark.read.parquet(
    f"{output_folder}/trap_summary"
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


print("\n========PROMOTION TRAP VALIDATION========")


for name, frame in [
    ("Promotion traps", promotion_traps),
    ("Cannibalization candidates", cannibalization_candidates),
    ("Trap summary", trap_summary),
    ("Analysis metadata", analysis_metadata)
]:
    check(
        f"{name} contains data",
        frame.count() > 0
    )


source_count = promotion_effectiveness.select(
    "promotion_id"
).distinct().count()


trap_count = promotion_traps.select(
    "promotion_id"
).distinct().count()


check(
    "Every Step 27 promotion is evaluated for traps",
    source_count == trap_count,
    (
        f"Step 27: {source_count}, "
        f"Step 28: {trap_count}"
    )
)


check(
    "Promotion trap IDs are unique",
    promotion_traps.groupBy(
        "promotion_id"
    ).count().filter(
        F.col("count") > 1
    ).count() == 0
)


required_flags = {
    "sales_profit_trap",
    "customer_margin_trap",
    "wastage_trap",
    "discount_dependency_trap",
    "cannibalization_trap"
}


check(
    "All five required Step 28 trap types are implemented",
    required_flags.issubset(
        set(
            promotion_traps.columns
        )
    )
)


for column in required_flags | {
    "promotion_trap_detected"
}:
    check(
        f"{column} is never null",
        promotion_traps.filter(
            F.col(column).isNull()
        ).count() == 0
    )



check(
    "Trap count equals the five boolean flags",
    promotion_traps.filter(
        F.col("trap_count") !=
        (
            F.col(
                "sales_profit_trap"
            ).cast("int") +
            F.col(
                "customer_margin_trap"
            ).cast("int") +
            F.col(
                "wastage_trap"
            ).cast("int") +
            F.col(
                "discount_dependency_trap"
            ).cast("int") +
            F.col(
                "cannibalization_trap"
            ).cast("int")
        )
    ).count() == 0
)


check(
    "Detected flag matches trap count",
    promotion_traps.filter(
        F.col("promotion_trap_detected") !=
        (
            F.col("trap_count") > 0
        )
    ).count() == 0
)


check(
    "Trap count stays between zero and five",
    promotion_traps.filter(
        (
            F.col("trap_count") < 0
        ) |
        (
            F.col("trap_count") > 5
        )
    ).count() == 0
)


check(
    "Unused promotions are never flagged as traps",
    promotion_traps.filter(
        ~F.col("promotion_used")
    ).filter(
        F.col("promotion_trap_detected")
    ).count() == 0
)



check(
    "Sales-profit traps require sales growth",
    promotion_traps.filter(
        F.col("sales_profit_trap")
    ).filter(
        ~(
            (
                F.col("demand_lift_pct") > 0
            ) |
            (
                F.col("revenue_lift_pct") > 0
            )
        )
    ).count() == 0
)


check(
    "Sales-profit traps require declining margin",
    promotion_traps.filter(
        F.col("sales_profit_trap")
    ).filter(
        F.col("margin_lift_pct") >= 0
    ).count() == 0
)



margin_threshold = promotion_traps.select(
    "customer_margin_collapse_threshold_pct"
).first()[
    "customer_margin_collapse_threshold_pct"
]


check(
    "Customer-margin traps require customer growth",
    promotion_traps.filter(
        F.col("customer_margin_trap")
    ).filter(
        F.col("during_target_customers") <=
        F.col("pre_target_customers")
    ).count() == 0
)


check(
    "Customer-margin traps require material margin collapse",
    promotion_traps.filter(
        F.col("customer_margin_trap")
    ).filter(
        F.col(
            "average_margin_per_order_change_pct"
        ) >
        F.lit(
            margin_threshold
        )
    ).count() == 0
)

check(
    "Wastage traps require higher during-promotion daily wastage",
    promotion_traps.filter(
        F.col("wastage_trap")
    ).filter(
        F.col("during_daily_wastage_cost") <=
        F.col("pre_daily_wastage_cost")
    ).count() == 0
)


dependency_demand_threshold = promotion_traps.select(
    "dependency_post_demand_threshold_pct"
).first()[
    "dependency_post_demand_threshold_pct"
]


dependency_repeat_threshold = promotion_traps.select(
    "dependency_repeat_rate_threshold_pct"
).first()[
    "dependency_repeat_rate_threshold_pct"
]


check(
    "Discount-dependency traps require at least five promotion orders",
    promotion_traps.filter(
        F.col("discount_dependency_trap")
    ).filter(
        F.col("promotion_order_count") < 5
    ).count() == 0
)


check(
    "Discount-dependency traps require campaign demand above pre-promotion demand",
    promotion_traps.filter(
        F.col("discount_dependency_trap")
    ).filter(
        F.col("during_daily_demand") <=
        F.col("pre_daily_demand")
    ).count() == 0
)


check(
    "Discount-dependency traps require post-promotion demand collapse",
    promotion_traps.filter(
        F.col("discount_dependency_trap")
    ).filter(
        F.col("post_demand_vs_pre_pct") >
        F.lit(
            dependency_demand_threshold
        )
    ).count() == 0
)


check(
    "Discount-dependency traps require low promoted-item repeat rate",
    promotion_traps.filter(
        F.col("discount_dependency_trap")
    ).filter(
        F.col(
            "post_target_item_repeat_rate_pct"
        ) >
        F.lit(
            dependency_repeat_threshold
        )
    ).count() == 0
)


cannibalization_threshold = promotion_traps.select(
    "cannibalization_demand_drop_threshold_pct"
).first()[
    "cannibalization_demand_drop_threshold_pct"
]


check(
    "Cannibalization traps identify a displaced item",
    promotion_traps.filter(
        F.col("cannibalization_trap")
    ).filter(
        F.col("candidate_item_id").isNull()
    ).count() == 0
)


check(
    "Cannibalization requires same-category comparison",
    promotion_traps.filter(
        F.col("cannibalization_trap")
    ).filter(
        F.col("candidate_category_id") !=
        F.col("promoted_category_id")
    ).count() == 0
)


check(
    "Cannibalization requires meaningful pre-promotion demand",
    promotion_traps.filter(
        F.col("cannibalization_trap")
    ).filter(
        (
            F.col("candidate_pre_daily_demand") *
            F.col("pre_days")
        ) < 2.99
    ).count() == 0
)


check(
    "Cannibalization requires promoted-item demand growth",
    promotion_traps.filter(
        F.col("cannibalization_trap")
    ).filter(
        F.col("demand_lift_pct") <= 0
    ).count() == 0
)


check(
    "Cannibalization requires displaced-item demand decline",
    promotion_traps.filter(
        F.col("cannibalization_trap")
    ).filter(
        F.col("candidate_demand_change_pct") >
        F.lit(
            cannibalization_threshold
        )
    ).count() == 0
)


check(
    "Cannibalized item has higher margin per unit",
    promotion_traps.filter(
        F.col("cannibalization_trap")
    ).filter(
        F.col("candidate_margin_advantage") <= 0
    ).count() == 0
)


check(
    "Cannibalization requires estimated lost daily margin",
    promotion_traps.filter(
        F.col("cannibalization_trap")
    ).filter(
        F.col("estimated_lost_daily_margin") <= 0
    ).count() == 0
)



check(
    "Critical severity requires at least three traps",
    promotion_traps.filter(
        F.col("trap_severity") == "Critical"
    ).filter(
        F.col("trap_count") < 3
    ).count() == 0
)


check(
    "High severity requires exactly two traps",
    promotion_traps.filter(
        F.col("trap_severity") == "High"
    ).filter(
        F.col("trap_count") != 2
    ).count() == 0
)


check(
    "Medium severity requires exactly one trap",
    promotion_traps.filter(
        F.col("trap_severity") == "Medium"
    ).filter(
        F.col("trap_count") != 1
    ).count() == 0
)


check(
    "No-trap severity requires zero traps",
    promotion_traps.filter(
        F.col("trap_severity") == "None"
    ).filter(
        F.col("trap_count") != 0
    ).count() == 0
)




summary_row = trap_summary.first()


check(
    "Trap summary total matches promotion output",
    int(
        summary_row[
            "total_promotions"
        ]
    ) ==
    promotion_traps.count()
)


check(
    "Trap summary detected count matches promotion output",
    int(
        summary_row[
            "promotions_with_traps"
        ]
    ) ==
    promotion_traps.filter(
        F.col("promotion_trap_detected")
    ).count()
)


check(
    "At least one promotion trap is detected",
    int(
        summary_row[
            "promotions_with_traps"
        ]
    ) > 0
)


metadata_components = {
    row["analysis_component"]
    for row in analysis_metadata.select(
        "analysis_component"
    ).distinct().collect()
}


for component in [
    "sales_profit_trap",
    "customer_margin_trap",
    "wastage_trap",
    "discount_dependency_trap",
    "cannibalization_trap",
    "severity",
    "interpretation"
]:
    check(
        f"Methodology documented: {component}",
        component in
        metadata_components
    )




total_checks = passed + failed


print(
    f"\n========VALIDATION RESULT: "
    f"{passed}/{total_checks} PASS========"
)


if failed == 0:
    print(
        "\nPromotion trap validation PASSED."
    )
else:
    print(
        "\nPromotion trap validation FAILED."
    )


spark.stop()