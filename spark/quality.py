from pyspark.sql import functions as F
from spark.ingest import *

def check_nulls(df):
    null_checks = []
    for column in df.columns:
        null_checks.append(F.count(F.when(F.col(column).isNull(), 1)).alias(f"{column}_nulls"))
    df.select(*null_checks).show(truncate=False)


def check_duplicates(df, column):
    duplicates = df.groupBy(column).count().filter(F.col("count") > 1)
    count = duplicates.count()
    print(f"{column}: {count} duplicate ids")
    if count > 0:
        duplicates.show()

def check_negative(df, columns):
    for column in columns:
        count = df.filter(F.col(column) < 0).count()
        print(f"{column}: {count} negative values")

def check_invalid_values(df, column, valid_values):
    invalid = df.filter(F.col(column).isNotNull() & ~F.col(column).isin(valid_values))
    count = invalid.count()
    print(f"{column}: {count} invalid values")
    if count > 0:
        invalid.select(column).distinct().show()

def check_invalid_range(df, column, minimum=None, maximum=None):
    condition = F.lit(False)
    if minimum is not None:
        condition = condition | (F.col(column) < minimum)
    if maximum is not None:
        condition = condition | (F.col(column) > maximum)
    invalid = df.filter(F.col(column).isNotNull() & condition)
    count = invalid.count()
    print(f"{column}: {count} values outside valid range")
    if count > 0:
        invalid.select(column).show()

def check_foreign_key(df, column, reference_df, reference_column):
    invalid = df.filter(F.col(column).isNotNull()).join(
        reference_df.select(F.col(reference_column).alias("_reference_id")),
        F.col(column) == F.col("_reference_id"), "left_anti"
    )
    count = invalid.count()
    print(f"{column}: {count} invalid references")
    if count > 0:
        invalid.select(column).distinct().show()

def check_cancelled_orders(df):
    count = df.filter(F.col("order_status") == "Cancelled").count()
    print(f"Cancelled transactions: {count}")

def check_incorrect_order_discounts(df):
    invalid = df.filter((F.col("discount_amount") < 0) | (F.col("discount_amount") > F.col("subtotal")))
    count = invalid.count()
    print(f"Incorrect order discounts: {count}")
    if count > 0:
        invalid.select("order_id", "subtotal", "discount_amount").show()

def check_impossible_wastage(wastage_df, inventory_df):
    invalid = wastage_df.alias("w").join(
        inventory_df.alias("i"), F.col("w.inventory_id") == F.col("i.inventory_id"), "left"
    ).filter(
        F.col("i.inventory_id").isNull() |
        (F.col("w.quantity") <= 0) |
        (F.col("w.quantity") > F.col("i.quantity_received")) |
        (F.col("w.wastage_date") < F.col("i.received_date")) |
        (F.col("w.wastage_date") > F.col("i.expiry_date"))
    )
    count = invalid.count()
    print(f"Impossible wastage records: {count}")
    if count > 0:
        invalid.select("w.wastage_id", "w.inventory_id", "w.quantity", "i.quantity_received", "w.wastage_date", "i.received_date", "i.expiry_date").show()

def check_order_dates(df):
    invalid = df.filter(
        F.col("order_datetime").isNull() |
        (F.to_date(F.col("order_datetime")) < F.lit("2025-03-01").cast("date")) |
        (F.to_date(F.col("order_datetime")) > F.lit("2026-08-31").cast("date"))
    )
    count = invalid.count()
    print(f"invalid order dates: {count}")
    if count > 0:
        invalid.select("order_id", "order_datetime").show()

def check_inconsistent_units(ingredients, inventory, wastage):
    valid_units = ["kg", "liters", "pieces"]

    print("Checking invalid ingredient units..")
    invalid_ingredients = ingredients.filter(~F.col("unit").isin(valid_units))
    print(f"Invalid ingredient units: {invalid_ingredients.count()}")

    print("Checking inventory unit consistency....")
    invalid_inventory_units = inventory.alias("inv").join(
        ingredients.alias("ing"),
        F.col("inv.ingredient_id") == F.col("ing.ingredient_id"),
        "left"
    ).filter(
        F.col("ing.ingredient_id").isNull() |
        (F.col("inv.unit") != F.col("ing.unit"))
    )

    print(f"inconsistent inventory units: {invalid_inventory_units.count()}")

    print("Checking wastage unit consistency...")
    invalid_wastage_units = wastage.alias("w").join(
        inventory.alias("inv"),
        F.col("w.inventory_id") == F.col("inv.inventory_id"),
        "left"
    ).filter(
        F.col("inv.inventory_id").isNull() |
        (F.col("w.unit") != F.col("inv.unit"))
    )

    print(f"Inconsistent wastage units: {invalid_wastage_units.count()}")
