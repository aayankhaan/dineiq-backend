import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.settings import INTEGRATED_DATA_FOLDER, ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("DineIQ Price Intelligence") \
    .getOrCreate()

spark.conf.set("spark.sql.ansi.enabled", "false")

integrated_folder = INTEGRATED_DATA_FOLDER
output_folder = f"{ANALYTICS_DATA_FOLDER}/price_intelligence"

DEFAULT_WINDOW_DAYS = 28


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Analyze DineIQ price relationships and "
            "before/after price-change behavior."
        )
    )

    parser.add_argument(
        "--window-days",
        type=int,
        default=DEFAULT_WINDOW_DAYS
    )

    return parser.parse_args()


args = parse_args()

if args.window_days < 7:
    raise ValueError(
        "Price-change window must be at least 7 days."
    )


transactions = spark.read.parquet(
    f"{integrated_folder}/transactions"
)

ratings = spark.read.parquet(
    f"{integrated_folder}/ratings"
)

pricing_history = spark.read.parquet(
    f"{integrated_folder}/pricing_history"
)


# ==================== TRANSACTION BASE ====================


transaction_base = transactions.filter(
    F.col("order_status") != "Cancelled"
).withColumn(
    "order_date",
    F.to_date("order_datetime")
).withColumn(
    "gross_item_value",
    F.col("unit_price").cast("double") *
    F.col("quantity").cast("double")
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
    "item_discount",
    F.col("item_discount_amount").cast("double")
)


date_range = transaction_base.agg(
    F.min("order_date").alias("start_date"),
    F.max("order_date").alias("end_date")
).first()


historical_start = date_range["start_date"]
historical_end = date_range["end_date"]


item_master = transaction_base.groupBy(
    "item_id"
).agg(
    F.first(
        "item_name",
        ignorenulls=True
    ).alias("item_name"),
    F.first(
        "category_id",
        ignorenulls=True
    ).alias("category_id"),
    F.first(
        "category_name",
        ignorenulls=True
    ).alias("category_name")
)


location_master = transaction_base.groupBy(
    "restaurant_id"
).agg(
    F.first(
        "restaurant_name",
        ignorenulls=True
    ).alias("restaurant_name"),
    F.first(
        "restaurant_city",
        ignorenulls=True
    ).alias("restaurant_city"),
    F.first(
        "restaurant_area",
        ignorenulls=True
    ).alias("restaurant_area")
)


# ==================== RATED PURCHASES ====================


rating_by_purchase = ratings.groupBy(
    "order_id",
    "restaurant_id",
    "item_id"
).agg(
    F.avg(
        F.col("rating").cast("double")
    ).alias("purchase_rating")
)


transaction_purchase_lookup = transaction_base.select(
    "order_id",
    "restaurant_id",
    "item_id",
    "order_date",
    "unit_price"
).dropDuplicates(
    [
        "order_id",
        "restaurant_id",
        "item_id"
    ]
)


rated_purchases = transaction_purchase_lookup.join(
    rating_by_purchase,
    [
        "order_id",
        "restaurant_id",
        "item_id"
    ],
    "inner"
)


# ==================== PRICE-LEVEL PERFORMANCE ====================


price_level_base = transaction_base.groupBy(
    "restaurant_id",
    "item_id",
    F.col(
        "unit_price"
    ).cast(
        "double"
    ).alias(
        "observed_price"
    )
).agg(
    F.countDistinct(
        "order_date"
    ).alias(
        "active_days"
    ),
    F.countDistinct(
        "order_id"
    ).alias(
        "order_count"
    ),
    F.sum(
        F.col("quantity").cast("double")
    ).alias(
        "demand_quantity"
    ),
    F.round(
        F.sum("line_revenue"),
        2
    ).alias(
        "revenue"
    ),
    F.round(
        F.sum("line_margin"),
        2
    ).alias(
        "contribution_margin"
    ),
    F.round(
        F.sum("item_discount"),
        2
    ).alias(
        "discount_amount"
    ),
    F.round(
        F.sum("gross_item_value"),
        2
    ).alias(
        "gross_item_value"
    ),
    F.countDistinct(
        "customer_id"
    ).alias(
        "unique_customers"
    )
).withColumn(
    "average_daily_demand",
    F.round(
        F.col("demand_quantity") /
        F.col("active_days"),
        4
    )
).withColumn(
    "revenue_per_day",
    F.round(
        F.col("revenue") /
        F.col("active_days"),
        4
    )
).withColumn(
    "margin_per_day",
    F.round(
        F.col("contribution_margin") /
        F.col("active_days"),
        4
    )
).withColumn(
    "discount_percentage",
    F.when(
        F.col("gross_item_value") > 0,
        F.round(
            (
                F.col("discount_amount") /
                F.col("gross_item_value")
            ) * 100,
            4
        )
    ).otherwise(
        F.lit(0.0)
    )
)


price_customer_orders = transaction_base.groupBy(
    "restaurant_id",
    "item_id",
    F.col(
        "unit_price"
    ).cast(
        "double"
    ).alias(
        "observed_price"
    ),
    "customer_id"
).agg(
    F.countDistinct(
        "order_id"
    ).alias(
        "customer_orders"
    )
)


price_repeat_purchase = price_customer_orders.groupBy(
    "restaurant_id",
    "item_id",
    "observed_price"
).agg(
    F.count(
        "*"
    ).alias(
        "customers_at_price"
    ),
    F.sum(
        F.when(
            F.col("customer_orders") > 1,
            1
        ).otherwise(0)
    ).alias(
        "repeat_customers_at_price"
    )
).withColumn(
    "repeat_purchase_rate",
    F.round(
        (
            F.col("repeat_customers_at_price") /
            F.col("customers_at_price")
        ) * 100,
        4
    )
)


price_rating = rated_purchases.groupBy(
    "restaurant_id",
    "item_id",
    F.col(
        "unit_price"
    ).cast(
        "double"
    ).alias(
        "observed_price"
    )
).agg(
    F.count(
        "*"
    ).alias(
        "rating_count"
    ),
    F.round(
        F.avg("purchase_rating"),
        4
    ).alias(
        "average_rating"
    )
)


price_level_performance = price_level_base.join(
    price_repeat_purchase,
    [
        "restaurant_id",
        "item_id",
        "observed_price"
    ],
    "left"
).join(
    price_rating,
    [
        "restaurant_id",
        "item_id",
        "observed_price"
    ],
    "left"
).join(
    item_master,
    "item_id",
    "left"
).join(
    location_master,
    "restaurant_id",
    "left"
).fillna(
    {
        "repeat_purchase_rate": 0.0,
        "rating_count": 0
    }
).select(
    "restaurant_id",
    "restaurant_name",
    "restaurant_city",
    "restaurant_area",
    "item_id",
    "item_name",
    "category_id",
    "category_name",
    "observed_price",
    "active_days",
    "order_count",
    "demand_quantity",
    "average_daily_demand",
    "revenue",
    "revenue_per_day",
    "contribution_margin",
    "margin_per_day",
    "discount_amount",
    "discount_percentage",
    "unique_customers",
    "repeat_purchase_rate",
    "rating_count",
    "average_rating"
)


# ==================== PRICE RELATIONSHIPS ====================


global_price_relationships = price_level_performance.agg(
    F.round(
        F.corr(
            "observed_price",
            "average_daily_demand"
        ),
        4
    ).alias(
        "price_demand_correlation"
    ),
    F.round(
        F.corr(
            "observed_price",
            "revenue_per_day"
        ),
        4
    ).alias(
        "price_revenue_correlation"
    ),
    F.round(
        F.corr(
            "observed_price",
            "margin_per_day"
        ),
        4
    ).alias(
        "price_margin_correlation"
    ),
    F.round(
        F.corr(
            "observed_price",
            "discount_percentage"
        ),
        4
    ).alias(
        "price_discount_correlation"
    ),
    F.round(
        F.corr(
            "observed_price",
            "average_rating"
        ),
        4
    ).alias(
        "price_rating_correlation"
    ),
    F.round(
        F.corr(
            "observed_price",
            "repeat_purchase_rate"
        ),
        4
    ).alias(
        "price_repeat_purchase_correlation"
    )
)


item_price_relationships = price_level_performance.groupBy(
    "item_id",
    "item_name",
    "category_id",
    "category_name"
).agg(
    F.countDistinct(
        "observed_price"
    ).alias(
        "observed_price_levels"
    ),
    F.round(
        F.min("observed_price"),
        2
    ).alias(
        "minimum_observed_price"
    ),
    F.round(
        F.max("observed_price"),
        2
    ).alias(
        "maximum_observed_price"
    ),
    F.round(
        F.corr(
            "observed_price",
            "average_daily_demand"
        ),
        4
    ).alias(
        "price_demand_correlation"
    ),
    F.round(
        F.corr(
            "observed_price",
            "revenue_per_day"
        ),
        4
    ).alias(
        "price_revenue_correlation"
    ),
    F.round(
        F.corr(
            "observed_price",
            "margin_per_day"
        ),
        4
    ).alias(
        "price_margin_correlation"
    ),
    F.round(
        F.corr(
            "observed_price",
            "discount_percentage"
        ),
        4
    ).alias(
        "price_discount_correlation"
    ),
    F.round(
        F.corr(
            "observed_price",
            "average_rating"
        ),
        4
    ).alias(
        "price_rating_correlation"
    ),
    F.round(
        F.corr(
            "observed_price",
            "repeat_purchase_rate"
        ),
        4
    ).alias(
        "price_repeat_purchase_correlation"
    )
).filter(
    F.col("observed_price_levels") >= 2
)


# ==================== PRICE-CHANGE EVENTS ====================


price_window = Window.partitionBy(
    "restaurant_id",
    "item_id"
).orderBy(
    "effective_date",
    "price_history_id"
)


price_events = pricing_history.select(
    "price_history_id",
    "restaurant_id",
    "item_id",
    F.col(
        "old_price"
    ).cast(
        "double"
    ).alias(
        "old_price"
    ),
    F.col(
        "new_price"
    ).cast(
        "double"
    ).alias(
        "new_price"
    ),
    F.to_date(
        "effective_date"
    ).alias(
        "effective_date"
    ),
    "reason"
).withColumn(
    "previous_change_date",
    F.lag(
        "effective_date"
    ).over(
        price_window
    )
).withColumn(
    "next_change_date",
    F.lead(
        "effective_date"
    ).over(
        price_window
    )
).withColumn(
    "pre_window_start",
    F.greatest(
        F.date_sub(
            "effective_date",
            args.window_days
        ),
        F.lit(
            historical_start
        )
    )
).withColumn(
    "pre_window_end",
    F.date_sub(
        "effective_date",
        1
    )
).withColumn(
    "post_window_start",
    F.col(
        "effective_date"
    )
).withColumn(
    "post_window_end",
    F.least(
        F.date_add(
            "effective_date",
            args.window_days - 1
        ),
        F.lit(
            historical_end
        )
    )
).withColumn(
    "pre_window_days",
    F.datediff(
        "pre_window_end",
        "pre_window_start"
    ) + 1
).withColumn(
    "post_window_days",
    F.datediff(
        "post_window_end",
        "post_window_start"
    ) + 1
).withColumn(
    "isolated_change",
    (
        F.col(
            "previous_change_date"
        ).isNull() |
        (
            F.datediff(
                "effective_date",
                "previous_change_date"
            ) >=
            args.window_days
        )
    ) &
    (
        F.col(
            "next_change_date"
        ).isNull() |
        (
            F.datediff(
                "next_change_date",
                "effective_date"
            ) >=
            args.window_days
        )
    )
).withColumn(
    "price_change_amount",
    F.round(
        F.col("new_price") -
        F.col("old_price"),
        2
    )
).withColumn(
    "price_change_pct",
    F.when(
        F.col("old_price") != 0,
        F.round(
            (
                (
                    F.col("new_price") -
                    F.col("old_price")
                ) /
                F.col("old_price")
            ) * 100,
            4
        )
    )
)


event_transactions = price_events.alias(
    "e"
).join(
    transaction_base.alias(
        "t"
    ),
    (
        F.col(
            "e.restaurant_id"
        ) ==
        F.col(
            "t.restaurant_id"
        )
    ) &
    (
        F.col(
            "e.item_id"
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
            "e.pre_window_start"
        )
    ) &
    (
        F.col(
            "t.order_date"
        ) <=
        F.col(
            "e.post_window_end"
        )
    ),
    "left"
).withColumn(
    "event_period",
    F.when(
        F.col(
            "t.order_date"
        ) <
        F.col(
            "e.effective_date"
        ),
        "Pre"
    ).when(
        F.col(
            "t.order_date"
        ) >=
        F.col(
            "e.effective_date"
        ),
        "Post"
    )
)


event_metrics = event_transactions.groupBy(
    F.col(
        "e.price_history_id"
    ).alias(
        "price_history_id"
    ),
    F.col(
        "e.restaurant_id"
    ).alias(
        "restaurant_id"
    ),
    F.col(
        "e.item_id"
    ).alias(
        "item_id"
    ),
    F.col(
        "e.old_price"
    ).alias(
        "old_price"
    ),
    F.col(
        "e.new_price"
    ).alias(
        "new_price"
    ),
    F.col(
        "e.effective_date"
    ).alias(
        "effective_date"
    ),
    F.col(
        "e.reason"
    ).alias(
        "reason"
    ),
    F.col(
        "e.price_change_amount"
    ).alias(
        "price_change_amount"
    ),
    F.col(
        "e.price_change_pct"
    ).alias(
        "price_change_pct"
    ),
    F.col(
        "e.pre_window_days"
    ).alias(
        "pre_window_days"
    ),
    F.col(
        "e.post_window_days"
    ).alias(
        "post_window_days"
    ),
    F.col(
        "e.isolated_change"
    ).alias(
        "isolated_change"
    )
).agg(
    F.sum(
        F.when(
            F.col("event_period") == "Pre",
            F.col("t.quantity").cast("double")
        ).otherwise(0.0)
    ).alias(
        "pre_demand_quantity"
    ),
    F.sum(
        F.when(
            F.col("event_period") == "Post",
            F.col("t.quantity").cast("double")
        ).otherwise(0.0)
    ).alias(
        "post_demand_quantity"
    ),
    F.sum(
        F.when(
            F.col("event_period") == "Pre",
            F.col("t.line_revenue")
        ).otherwise(0.0)
    ).alias(
        "pre_revenue"
    ),
    F.sum(
        F.when(
            F.col("event_period") == "Post",
            F.col("t.line_revenue")
        ).otherwise(0.0)
    ).alias(
        "post_revenue"
    ),
    F.sum(
        F.when(
            F.col("event_period") == "Pre",
            F.col("t.line_margin")
        ).otherwise(0.0)
    ).alias(
        "pre_contribution_margin"
    ),
    F.sum(
        F.when(
            F.col("event_period") == "Post",
            F.col("t.line_margin")
        ).otherwise(0.0)
    ).alias(
        "post_contribution_margin"
    ),
    F.sum(
        F.when(
            F.col("event_period") == "Pre",
            F.col("t.item_discount")
        ).otherwise(0.0)
    ).alias(
        "pre_discount_amount"
    ),
    F.sum(
        F.when(
            F.col("event_period") == "Post",
            F.col("t.item_discount")
        ).otherwise(0.0)
    ).alias(
        "post_discount_amount"
    ),
    F.sum(
        F.when(
            F.col("event_period") == "Pre",
            F.col("t.gross_item_value")
        ).otherwise(0.0)
    ).alias(
        "pre_gross_item_value"
    ),
    F.sum(
        F.when(
            F.col("event_period") == "Post",
            F.col("t.gross_item_value")
        ).otherwise(0.0)
    ).alias(
        "post_gross_item_value"
    ),
    F.avg(
        F.when(
            F.col("event_period") == "Pre",
            F.col("t.unit_price").cast("double")
        )
    ).alias(
        "pre_average_observed_price"
    ),
    F.avg(
        F.when(
            F.col("event_period") == "Post",
            F.col("t.unit_price").cast("double")
        )
    ).alias(
        "post_average_observed_price"
    )
)


event_ratings = price_events.alias(
    "e"
).join(
    rated_purchases.alias(
        "r"
    ),
    (
        F.col(
            "e.restaurant_id"
        ) ==
        F.col(
            "r.restaurant_id"
        )
    ) &
    (
        F.col(
            "e.item_id"
        ) ==
        F.col(
            "r.item_id"
        )
    ) &
    (
        F.col(
            "r.order_date"
        ) >=
        F.col(
            "e.pre_window_start"
        )
    ) &
    (
        F.col(
            "r.order_date"
        ) <=
        F.col(
            "e.post_window_end"
        )
    ),
    "left"
).groupBy(
    F.col(
        "e.price_history_id"
    ).alias(
        "price_history_id"
    )
).agg(
    F.avg(
        F.when(
            F.col("r.order_date") <
            F.col("e.effective_date"),
            F.col("r.purchase_rating")
        )
    ).alias(
        "pre_average_rating"
    ),
    F.avg(
        F.when(
            F.col("r.order_date") >=
            F.col("e.effective_date"),
            F.col("r.purchase_rating")
        )
    ).alias(
        "post_average_rating"
    ),
    F.sum(
        F.when(
            F.col("r.order_date") <
            F.col("e.effective_date"),
            1
        ).otherwise(0)
    ).alias(
        "pre_rating_count"
    ),
    F.sum(
        F.when(
            F.col("r.order_date") >=
            F.col("e.effective_date"),
            1
        ).otherwise(0)
    ).alias(
        "post_rating_count"
    )
)


event_customer_orders = event_transactions.filter(
    F.col("t.customer_id").isNotNull()
).groupBy(
    F.col(
        "e.price_history_id"
    ).alias(
        "price_history_id"
    ),
    "event_period",
    F.col(
        "t.customer_id"
    ).alias(
        "customer_id"
    )
).agg(
    F.countDistinct(
        F.col("t.order_id")
    ).alias(
        "customer_orders"
    )
)


event_repeat_purchase = event_customer_orders.groupBy(
    "price_history_id"
).agg(
    F.count(
        F.when(
            F.col("event_period") == "Pre",
            1
        )
    ).alias(
        "pre_customers"
    ),
    F.sum(
        F.when(
            (
                F.col("event_period") == "Pre"
            ) &
            (
                F.col("customer_orders") > 1
            ),
            1
        ).otherwise(0)
    ).alias(
        "pre_repeat_customers"
    ),
    F.count(
        F.when(
            F.col("event_period") == "Post",
            1
        )
    ).alias(
        "post_customers"
    ),
    F.sum(
        F.when(
            (
                F.col("event_period") == "Post"
            ) &
            (
                F.col("customer_orders") > 1
            ),
            1
        ).otherwise(0)
    ).alias(
        "post_repeat_customers"
    )
)


price_change_events = event_metrics.join(
    event_ratings,
    "price_history_id",
    "left"
).join(
    event_repeat_purchase,
    "price_history_id",
    "left"
).join(
    item_master,
    "item_id",
    "left"
).join(
    location_master,
    "restaurant_id",
    "left"
).fillna(
    {
        "pre_customers": 0,
        "pre_repeat_customers": 0,
        "post_customers": 0,
        "post_repeat_customers": 0,
        "pre_rating_count": 0,
        "post_rating_count": 0
    }
).withColumn(
    "pre_daily_demand",
    F.round(
        F.col("pre_demand_quantity") /
        F.col("pre_window_days"),
        4
    )
).withColumn(
    "post_daily_demand",
    F.round(
        F.col("post_demand_quantity") /
        F.col("post_window_days"),
        4
    )
).withColumn(
    "demand_change_pct",
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
            4
        )
    )
).withColumn(
    "absolute_demand_change_pct",
    F.abs(
        F.col("demand_change_pct")
    )
).withColumn(
    "pre_revenue_per_day",
    F.round(
        F.col("pre_revenue") /
        F.col("pre_window_days"),
        4
    )
).withColumn(
    "post_revenue_per_day",
    F.round(
        F.col("post_revenue") /
        F.col("post_window_days"),
        4
    )
).withColumn(
    "revenue_change_pct",
    F.when(
        F.col("pre_revenue_per_day") != 0,
        F.round(
            (
                (
                    F.col("post_revenue_per_day") -
                    F.col("pre_revenue_per_day")
                ) /
                F.abs(
                    F.col("pre_revenue_per_day")
                )
            ) * 100,
            4
        )
    )
).withColumn(
    "pre_margin_per_day",
    F.round(
        F.col("pre_contribution_margin") /
        F.col("pre_window_days"),
        4
    )
).withColumn(
    "post_margin_per_day",
    F.round(
        F.col("post_contribution_margin") /
        F.col("post_window_days"),
        4
    )
).withColumn(
    "margin_change_pct",
    F.when(
        F.col("pre_margin_per_day") != 0,
        F.round(
            (
                (
                    F.col("post_margin_per_day") -
                    F.col("pre_margin_per_day")
                ) /
                F.abs(
                    F.col("pre_margin_per_day")
                )
            ) * 100,
            4
        )
    )
).withColumn(
    "pre_discount_percentage",
    F.when(
        F.col("pre_gross_item_value") > 0,
        F.round(
            (
                F.col("pre_discount_amount") /
                F.col("pre_gross_item_value")
            ) * 100,
            4
        )
    ).otherwise(
        F.lit(0.0)
    )
).withColumn(
    "post_discount_percentage",
    F.when(
        F.col("post_gross_item_value") > 0,
        F.round(
            (
                F.col("post_discount_amount") /
                F.col("post_gross_item_value")
            ) * 100,
            4
        )
    ).otherwise(
        F.lit(0.0)
    )
).withColumn(
    "pre_repeat_purchase_rate",
    F.when(
        F.col("pre_customers") > 0,
        F.round(
            (
                F.col("pre_repeat_customers") /
                F.col("pre_customers")
            ) * 100,
            4
        )
    ).otherwise(
        F.lit(0.0)
    )
).withColumn(
    "post_repeat_purchase_rate",
    F.when(
        F.col("post_customers") > 0,
        F.round(
            (
                F.col("post_repeat_customers") /
                F.col("post_customers")
            ) * 100,
            4
        )
    ).otherwise(
        F.lit(0.0)
    )
).withColumn(
    "rating_change",
    F.when(
        F.col("pre_average_rating").isNotNull() &
        F.col("post_average_rating").isNotNull(),
        F.round(
            F.col("post_average_rating") -
            F.col("pre_average_rating"),
            4
        )
    )
).withColumn(
    "repeat_purchase_change_pct_points",
    F.round(
        F.col("post_repeat_purchase_rate") -
        F.col("pre_repeat_purchase_rate"),
        4
    )
).withColumn(
    "price_elasticity_proxy",
    F.when(
        F.col("price_change_pct").isNotNull() &
        (
            F.abs(
                F.col("price_change_pct")
            ) > 0.0001
        ) &
        F.col("demand_change_pct").isNotNull(),
        F.round(
            F.col("demand_change_pct") /
            F.col("price_change_pct"),
            4
        )
    )
).withColumn(
    "evaluable_change",
    F.col("isolated_change") &
    (
        F.col("pre_window_days") >= 14
    ) &
    (
        F.col("post_window_days") >= 14
    ) &
    (
        F.col("pre_demand_quantity") >= 10
    ) &
    F.col("demand_change_pct").isNotNull()
)


significance_candidates = price_change_events.filter(
    F.col("evaluable_change")
).select(
    "absolute_demand_change_pct"
).filter(
    F.col(
        "absolute_demand_change_pct"
    ).isNotNull()
)


significance_quantile = significance_candidates.approxQuantile(
    "absolute_demand_change_pct",
    [0.75],
    0.01
)


if significance_quantile:
    significant_demand_threshold = max(
        float(
            significance_quantile[0]
        ),
        10.0
    )
else:
    significant_demand_threshold = 15.0


price_change_events = price_change_events.withColumn(
    "significant_demand_threshold_pct",
    F.lit(
        significant_demand_threshold
    )
).withColumn(
    "significant_demand_change",
    F.col("evaluable_change") &
    (
        F.col("absolute_demand_change_pct") >=
        F.lit(
            significant_demand_threshold
        )
    )
).withColumn(
    "demand_change_direction",
    F.when(
        ~F.col("evaluable_change"),
        "Insufficient / Overlapping History"
    ).when(
        F.col("significant_demand_change") &
        (
            F.col("demand_change_pct") > 0
        ),
        "Significant Increase"
    ).when(
        F.col("significant_demand_change") &
        (
            F.col("demand_change_pct") < 0
        ),
        "Significant Decrease"
    ).when(
        F.col("demand_change_pct") > 0,
        "Minor Increase"
    ).when(
        F.col("demand_change_pct") < 0,
        "Minor Decrease"
    ).otherwise(
        "No Material Change"
    )
)


# ==================== SIGNIFICANT PRICE RESPONSES ====================


significant_response_items = price_change_events.filter(
    F.col(
        "significant_demand_change"
    )
).groupBy(
    "item_id",
    "item_name",
    "category_id",
    "category_name"
).agg(
    F.countDistinct(
        "price_history_id"
    ).alias(
        "significant_price_change_events"
    ),
    F.round(
        F.avg(
            "price_change_pct"
        ),
        4
    ).alias(
        "average_price_change_pct"
    ),
    F.round(
        F.avg(
            "demand_change_pct"
        ),
        4
    ).alias(
        "average_demand_change_pct"
    ),
    F.round(
        F.avg(
            "absolute_demand_change_pct"
        ),
        4
    ).alias(
        "average_absolute_demand_change_pct"
    ),
    F.round(
        F.avg(
            "price_elasticity_proxy"
        ),
        4
    ).alias(
        "average_price_elasticity_proxy"
    ),
    F.round(
        F.avg(
            "revenue_change_pct"
        ),
        4
    ).alias(
        "average_revenue_change_pct"
    ),
    F.round(
        F.avg(
            "margin_change_pct"
        ),
        4
    ).alias(
        "average_margin_change_pct"
    ),
    F.round(
        F.avg(
            "rating_change"
        ),
        4
    ).alias(
        "average_rating_change"
    ),
    F.round(
        F.avg(
            "repeat_purchase_change_pct_points"
        ),
        4
    ).alias(
        "average_repeat_purchase_change_pct_points"
    )
).orderBy(
    F.desc(
        "average_absolute_demand_change_pct"
    )
)


# ==================== SUMMARY ====================


summary_counts = price_change_events.agg(
    F.count(
        "*"
    ).alias(
        "total_price_change_events"
    ),
    F.sum(
        F.when(
            F.col("isolated_change"),
            1
        ).otherwise(0)
    ).alias(
        "isolated_price_change_events"
    ),
    F.sum(
        F.when(
            F.col("evaluable_change"),
            1
        ).otherwise(0)
    ).alias(
        "evaluable_price_change_events"
    ),
    F.sum(
        F.when(
            F.col("significant_demand_change"),
            1
        ).otherwise(0)
    ).alias(
        "significant_demand_change_events"
    )
)


price_intelligence_summary = summary_counts.withColumn(
    "significant_demand_threshold_pct",
    F.lit(
        significant_demand_threshold
    )
).withColumn(
    "analysis_window_days",
    F.lit(
        args.window_days
    )
).withColumn(
    "significant_event_rate_pct",
    F.when(
        F.col(
            "evaluable_price_change_events"
        ) > 0,
        F.round(
            (
                F.col(
                    "significant_demand_change_events"
                ) /
                F.col(
                    "evaluable_price_change_events"
                )
            ) * 100,
            2
        )
    ).otherwise(
        F.lit(0.0)
    )
)


analysis_metadata = spark.createDataFrame([
    (
        "price_relationships",
        "Relationships are measured across observed restaurant-item price levels using demand, revenue, contribution margin, discount, rating, and repeat-purchase behavior."
    ),
    (
        "price_change_windows",
        f"Each historical price change compares up to {args.window_days} days before with {args.window_days} days after the effective date."
    ),
    (
        "isolated_price_changes",
        f"A price change is isolated when no other price change for the same restaurant-item occurs within the surrounding {args.window_days}-day analysis windows."
    ),
    (
        "significant_demand_change",
        f"Significance uses the 75th percentile of absolute daily-demand change among evaluable isolated events, with a minimum threshold of 10%. Current threshold: {significant_demand_threshold:.4f}%."
    ),
    (
        "price_elasticity_proxy",
        "The elasticity proxy is percentage demand change divided by percentage price change. It is descriptive evidence, not a causal estimate."
    ),
    (
        "ratings",
        "Ratings are linked to the price paid on the rated purchase rather than the later rating submission date."
    )
], [
    "analysis_component",
    "method"
])


# ==================== RESULTS ====================


print("\n========PRICE INTELLIGENCE SUMMARY========")
price_intelligence_summary.show(
    truncate=False
)


print("\n========GLOBAL PRICE RELATIONSHIPS========")
global_price_relationships.show(
    truncate=False
)


print("\n========SIGNIFICANT DEMAND RESPONSES AFTER PRICE CHANGES========")
price_change_events.filter(
    F.col(
        "significant_demand_change"
    )
).select(
    "price_history_id",
    "restaurant_name",
    "item_name",
    "old_price",
    "new_price",
    "price_change_pct",
    "effective_date",
    "pre_daily_demand",
    "post_daily_demand",
    "demand_change_pct",
    "revenue_change_pct",
    "margin_change_pct",
    "pre_discount_percentage",
    "post_discount_percentage",
    "pre_average_rating",
    "post_average_rating",
    "pre_repeat_purchase_rate",
    "post_repeat_purchase_rate",
    "price_elasticity_proxy",
    "demand_change_direction"
).orderBy(
    F.desc(
        "absolute_demand_change_pct"
    )
).show(
    30,
    truncate=False
)


print("\n========ITEMS WITH SIGNIFICANT PRICE RESPONSE========")
significant_response_items.show(
    30,
    truncate=False
)


# ==================== SAVE OUTPUTS ====================


outputs = {
    "price_level_performance": price_level_performance,
    "global_price_relationships": global_price_relationships,
    "item_price_relationships": item_price_relationships,
    "price_change_events": price_change_events,
    "significant_response_items": significant_response_items,
    "price_intelligence_summary": price_intelligence_summary,
    "analysis_metadata": analysis_metadata
}


for name, frame in outputs.items():
    frame.write.mode(
        "overwrite"
    ).parquet(
        f"{output_folder}/{name}"
    )


print("\nPrice intelligence analysis completed successfully.")


spark.stop()
