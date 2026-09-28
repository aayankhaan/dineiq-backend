from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import INTEGRATED_DATA_FOLDER, ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("Validate DineIQ Multi Location Intelligence") \
    .getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")


output_folder = f"{ANALYTICS_DATA_FOLDER}/multi_location_intelligence"


locations = spark.read.parquet(
    f"{output_folder}/location_comparison"
)

summary = spark.read.parquet(
    f"{output_folder}/network_summary"
)

transactions = spark.read.parquet(
    f"{INTEGRATED_DATA_FOLDER}/transactions"
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


print("\n========MULTI-LOCATION VALIDATION========")

check(
    "Location comparison contains data",
    locations.count() > 0
)


check(
    "Network summary contains exactly one row",
    summary.count() == 1
)


required_columns = {
    "revenue",
    "profit_percentage",
    "average_order_value",
    "customer_count",
    "repeat_purchase_rate_pct",
    "estimated_wastage_cost",
    "average_rating",
    "promotion_effective_rate_pct",
    "profitable_menu_item_rate_pct"
}


check(
    "All nine Step 33 comparison dimensions are present",
    required_columns.issubset(
        set(locations.columns)
    )
)


check(
    "Each restaurant appears exactly once",
    locations.groupBy(
        "restaurant_id"
    ).count().filter(
        F.col("count") != 1
    ).count() == 0
)


source_location_count = transactions.filter(
    F.col("order_status") == "Completed"
).select(
    "restaurant_id"
).distinct().count()


check(
    "All completed-transaction locations are represented",
    locations.count() ==
    source_location_count
)



check(
    "Revenue is non-negative",
    locations.filter(
        F.col("revenue") < 0
    ).count() == 0
)


check(
    "Average order value is positive",
    locations.filter(
        F.col("average_order_value") <= 0
    ).count() == 0
)


check(
    "Customer counts are positive",
    locations.filter(
        F.col("customer_count") <= 0
    ).count() == 0
)


check(
    "Repeat customer count cannot exceed customer count",
    locations.filter(
        F.col("repeat_customers") >
        F.col("customer_count")
    ).count() == 0
)


check(
    "Repeat purchase rate stays between zero and 100",
    locations.filter(
        (
            F.col("repeat_purchase_rate_pct") < 0
        ) |
        (
            F.col("repeat_purchase_rate_pct") > 100
        )
    ).count() == 0
)


check(
    "Estimated wastage cost is non-negative",
    locations.filter(
        F.col("estimated_wastage_cost") < 0
    ).count() == 0
)


check(
    "Wastage percentage of item revenue is non-negative",
    locations.filter(
        F.col("wastage_cost_pct_of_item_revenue") < 0
    ).count() == 0
)


check(
    "Average ratings stay within one to five when ratings exist",
    locations.filter(
        F.col("rating_count") > 0
    ).filter(
        (
            F.col("average_rating") < 1
        ) |
        (
            F.col("average_rating") > 5
        )
    ).count() == 0
)


check(
    "Promotion effectiveness rate stays between zero and 100",
    locations.filter(
        F.col("promotion_effective_rate_pct").isNotNull()
    ).filter(
        (
            F.col("promotion_effective_rate_pct") < 0
        ) |
        (
            F.col("promotion_effective_rate_pct") > 100
        )
    ).count() == 0
)


check(
    "Profitable menu item rate stays between zero and 100",
    locations.filter(
        (
            F.col("profitable_menu_item_rate_pct") < 0
        ) |
        (
            F.col("profitable_menu_item_rate_pct") > 100
        )
    ).count() == 0
)



check(
    "Average order value reconciles with revenue and orders",
    locations.filter(
        F.abs(
            F.col("average_order_value") -
            (
                F.col("revenue") /
                F.col("total_orders")
            )
        ) > 0.02
    ).count() == 0
)


check(
    "Profit percentage reconciles with item revenue and margin",
    locations.filter(
        F.col("item_revenue") > 0
    ).filter(
        F.abs(
            F.col("profit_percentage") -
            (
                (
                    F.col("contribution_margin") /
                    F.col("item_revenue")
                ) * 100
            )
        ) > 0.02
    ).count() == 0
)


check(
    "Menu item counts reconcile",
    locations.filter(
        F.col("active_menu_items") !=
        (
            F.col("profitable_menu_items") +
            F.col("loss_making_menu_items") +
            F.col("break_even_menu_items")
        )
    ).count() == 0
)


check(
    "Promotion assessment counts reconcile",
    locations.filter(
        F.col("promotion_count") !=
        (
            F.col("effective_promotions") +
            F.col("mixed_promotions") +
            F.col("ineffective_promotions") +
            F.col("unused_promotions")
        )
    ).count() == 0
)


check(
    "Used promotions exclude no-usage promotions",
    locations.filter(
        F.col("used_promotion_count") !=
        (
            F.col("effective_promotions") +
            F.col("mixed_promotions") +
            F.col("ineffective_promotions")
        )
    ).count() == 0
)


check(
    "Promotion effectiveness rate reconciles",
    locations.filter(
        F.col("used_promotion_count") > 0
    ).filter(
        F.abs(
            F.col("promotion_effective_rate_pct") -
            (
                (
                    F.col("effective_promotions") /
                    F.col("used_promotion_count")
                ) * 100
            )
        ) > 0.02
    ).count() == 0
)



rank_columns = [
    "revenue_rank",
    "profitability_rank",
    "average_order_value_rank",
    "customer_count_rank",
    "repeat_purchase_rank",
    "wastage_rank",
    "rating_rank",
    "menu_performance_rank"
]


for column in rank_columns:
    check(
        f"{column} contains positive ranks",
        locations.filter(
            F.col(column).isNull() |
            (F.col(column) < 1)
        ).count() == 0
    )


check(
    "Promotion rank is present when promotion effectiveness is available",
    locations.filter(
        F.col("promotion_effective_rate_pct").isNotNull()
    ).filter(
        F.col("promotion_effectiveness_rank").isNull() |
        (F.col("promotion_effectiveness_rank") < 1)
    ).count() == 0
)




row = summary.first()


check(
    "Summary location count matches comparison output",
    int(row["location_count"]) ==
    locations.count()
)


check(
    "Summary revenue matches comparison output",
    abs(
        float(row["network_revenue"]) -
        float(
            locations.agg(
                F.sum("revenue").alias("value")
            ).first()["value"]
        )
    ) <= 0.02
)


check(
    "Summary contribution margin matches comparison output",
    abs(
        float(row["network_contribution_margin"]) -
        float(
            locations.agg(
                F.sum(
                    "contribution_margin"
                ).alias("value")
            ).first()["value"]
        )
    ) <= 0.02
)


check(
    "Summary wastage matches comparison output",
    abs(
        float(row["network_estimated_wastage_cost"]) -
        float(
            locations.agg(
                F.sum(
                    "estimated_wastage_cost"
                ).alias("value")
            ).first()["value"]
        )
    ) <= 0.02
)



total = passed + failed


print(
    f"\n========VALIDATION RESULT: "
    f"{passed}/{total} PASS========"
)


if failed == 0:
    print(
        "\nMulti-location intelligence validation PASSED."
    )
else:
    print(
        "\nMulti-location intelligence validation FAILED."
    )


spark.stop()
