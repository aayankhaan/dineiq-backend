from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import INTEGRATED_DATA_FOLDER, ANALYTICS_DATA_FOLDER


spark = SparkSession.builder \
    .appName("DineIQ Market Basket Analysis") \
    .getOrCreate()


output_folder = f"{ANALYTICS_DATA_FOLDER}/market_basket"


transactions = spark.read.parquet(
    f"{INTEGRATED_DATA_FOLDER}/transactions"
)



order_items = transactions.select(
    "order_id",
    "item_id",
    "item_name"
).filter(
    F.col("order_id").isNotNull() &
    F.col("item_id").isNotNull()
).dropDuplicates(
    ["order_id", "item_id"]
)


total_orders = order_items.select(
    "order_id"
).distinct().count()


item_order_counts = order_items.groupBy(
    "item_id",
    "item_name"
).agg(
    F.countDistinct("order_id").alias("item_order_count")
)



left_items = order_items.alias("a")
right_items = order_items.alias("b")


item_pairs = left_items.join(
    right_items,
    (
        (F.col("a.order_id") == F.col("b.order_id")) &
        (F.col("a.item_id") < F.col("b.item_id"))
    ),
    "inner"
).select(
    F.col("a.order_id").alias("order_id"),
    F.col("a.item_id").alias("item_a_id"),
    F.col("a.item_name").alias("item_a_name"),
    F.col("b.item_id").alias("item_b_id"),
    F.col("b.item_name").alias("item_b_name")
)


pair_counts = item_pairs.groupBy(
    "item_a_id",
    "item_a_name",
    "item_b_id",
    "item_b_name"
).agg(
    F.countDistinct("order_id").alias("pair_order_count")
)


item_a_counts = item_order_counts.select(
    F.col("item_id").alias("item_a_id"),
    F.col("item_order_count").alias("item_a_order_count")
)


item_b_counts = item_order_counts.select(
    F.col("item_id").alias("item_b_id"),
    F.col("item_order_count").alias("item_b_order_count")
)


pair_metrics = pair_counts.join(
    item_a_counts,
    "item_a_id",
    "inner"
).join(
    item_b_counts,
    "item_b_id",
    "inner"
)



pair_metrics = pair_metrics.withColumn(
    "support",
    F.col("pair_order_count") / F.lit(total_orders)
)



rules_a_to_b = pair_metrics.select(
    F.col("item_a_id").alias("antecedent_item_id"),
    F.col("item_a_name").alias("antecedent_item_name"),
    F.col("item_b_id").alias("consequent_item_id"),
    F.col("item_b_name").alias("consequent_item_name"),
    "pair_order_count",
    "support",
    F.col("item_a_order_count").alias("antecedent_order_count"),
    F.col("item_b_order_count").alias("consequent_order_count")
)


rules_b_to_a = pair_metrics.select(
    F.col("item_b_id").alias("antecedent_item_id"),
    F.col("item_b_name").alias("antecedent_item_name"),
    F.col("item_a_id").alias("consequent_item_id"),
    F.col("item_a_name").alias("consequent_item_name"),
    "pair_order_count",
    "support",
    F.col("item_b_order_count").alias("antecedent_order_count"),
    F.col("item_a_order_count").alias("consequent_order_count")
)


association_rules = rules_a_to_b.unionByName(
    rules_b_to_a
).withColumn(
    "confidence",
    F.col("pair_order_count") / F.col("antecedent_order_count")
).withColumn(
    "consequent_support",
    F.col("consequent_order_count") / F.lit(total_orders)
).withColumn(
    "lift",
    F.col("confidence") / F.col("consequent_support")
)


minimum_pair_orders = 20
minimum_support = 0.0002
minimum_confidence = 0.01


suitable_rules = association_rules.filter(
    (F.col("pair_order_count") >= minimum_pair_orders) &
    (F.col("support") >= minimum_support) &
    (F.col("confidence") >= minimum_confidence)
).withColumn(
    "association_strength",
    F.when(
        F.col("lift") > 1.10,
        "Strong Positive"
    ).when(
        F.col("lift") > 1.00,
        "Positive"
    ).when(
        F.col("lift") == 1.00,
        "Independent"
    ).otherwise(
        "Weak"
    )
)




frequent_pairs = pair_metrics.select(
    "item_a_id",
    "item_a_name",
    "item_b_id",
    "item_b_name",
    "pair_order_count",
    "support"
).orderBy(
    F.desc("support"),
    F.desc("pair_order_count")
)



market_basket_summary = spark.createDataFrame(
    [
        (
            total_orders,
            item_order_counts.count(),
            pair_metrics.count(),
            suitable_rules.count(),
            minimum_pair_orders,
            minimum_support,
            minimum_confidence
        )
    ],
    [
        "total_orders",
        "unique_items",
        "unique_item_pairs",
        "suitable_association_rules",
        "minimum_pair_orders",
        "minimum_support",
        "minimum_confidence"
    ]
)



print("\n========MARKET BASKET SUMMARY========")
market_basket_summary.show(
    truncate=False
)


print("\n========TOP FREQUENT ITEM PAIRS========")
frequent_pairs.show(
    20,
    truncate=False
)


print("\n========TOP ASSOCIATION RULES========")
suitable_rules.orderBy(
    F.desc("lift"),
    F.desc("confidence"),
    F.desc("support")
).show(
    20,
    truncate=False
)


frequent_pairs.write.mode("overwrite").parquet(
    f"{output_folder}/frequent_item_pairs"
)


association_rules.write.mode("overwrite").parquet(
    f"{output_folder}/association_rules"
)


suitable_rules.write.mode("overwrite").parquet(
    f"{output_folder}/suitable_rules"
)


market_basket_summary.write.mode("overwrite").parquet(
    f"{output_folder}/market_basket_summary"
)


print("\nMarket-basket analysis completed successfully.")


spark.stop()
