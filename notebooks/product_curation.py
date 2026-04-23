# Databricks notebook source
# Product Curation Pipeline
# Purpose: derive product demand metrics (ADI, CV^2) and classify demand patterns
# and maintain them in a Delta table using SCD Type 2 logic.

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from helper_functions import scd2_merge

# --------------------------------------------------
# Configuration
# --------------------------------------------------

SOURCE_CATALOG = "samples"
SOURCE_SCHEMA = "tpch"
TARGET_CATALOG = "s3catalog"
TARGET_SCHEMA = "tpch_star_silver"
TARGET_TABLE = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.product_demand_scd"

# --------------------------------------------------
# Read Source Tables
# --------------------------------------------------

orders_df = spark.table(f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.orders")
lineitem_df = spark.table(f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.lineitem")
part_df = spark.table(f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.part")

# --------------------------------------------------
# Prepare Order Dates per Line Item
# --------------------------------------------------

li_with_dates_df = (
    lineitem_df.alias("l")
    .join(orders_df.alias("o"), F.col("l.l_orderkey") == F.col("o.o_orderkey"), "inner")
    .select(
        F.col("l.l_partkey").alias("product_id"),
        F.col("o.o_orderdate").alias("order_date"),
        F.col("l.l_quantity").alias("quantity")
    )
)

# --------------------------------------------------
# Compute Inter-arrival Days (for ADI)
# --------------------------------------------------

w = Window.partitionBy("product_id").orderBy("order_date")

li_with_intervals_df = (
    li_with_dates_df
    .withColumn("prev_date", F.lag("order_date").over(w))
    .withColumn("days_between", F.datediff(F.col("order_date"), F.col("prev_date")))
)

# --------------------------------------------------
# Aggregate Demand Metrics
# --------------------------------------------------

product_metrics_df = (
    li_with_intervals_df
    .groupBy("product_id")
    .agg(
        F.count(F.lit(1)).alias("demand_count"),
        F.round(F.avg("days_between"), 2).alias("adi"),  # Average Demand Interval
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.round(F.variance("quantity"), 4).alias("var_quantity")
    )
    .withColumn(
        "cv_squared",
        F.when(F.col("avg_quantity") > 0, F.col("var_quantity") / (F.col("avg_quantity") ** 2))
         .otherwise(F.lit(None))
    )
)

# --------------------------------------------------
# Demand Classification (ADI vs CV^2)
# --------------------------------------------------

classified_df = (
    product_metrics_df
    .withColumn(
        "demand_class",
        F.when((F.col("adi") <= 1.32) & (F.col("cv_squared") <= 0.49), F.lit("smooth"))
         .when((F.col("adi") > 1.32) & (F.col("cv_squared") <= 0.49), F.lit("intermittent"))
         .when((F.col("adi") <= 1.32) & (F.col("cv_squared") > 0.49), F.lit("erratic"))
         .otherwise(F.lit("lumpy"))
    )
)

# --------------------------------------------------
# Join Product Attributes
# --------------------------------------------------

product_curated_df = (
    part_df.alias("p")
    .join(classified_df.alias("m"), F.col("p.p_partkey") == F.col("m.product_id"), "left")
    .select(
        F.col("p.p_partkey").alias("product_id"),
        F.col("p.p_name").alias("product_name"),
        F.col("p.p_brand").alias("brand"),
        F.col("p.p_type").alias("type"),
        F.col("p.p_size").alias("size"),
        F.coalesce(F.col("m.demand_count"), F.lit(0)).alias("demand_count"),
        F.col("m.adi"),
        F.col("m.avg_quantity"),
        F.col("m.var_quantity"),
        F.col("m.cv_squared"),
        F.coalesce(F.col("m.demand_class"), F.lit("unknown")).alias("demand_class")
    )
)

# --------------------------------------------------
# Write Curated Output with SCD Type 2 Logic
# --------------------------------------------------

tracked_columns = [
    "product_name",
    "brand",
    "type",
    "size",
    "demand_count",
    "adi",
    "avg_quantity",
    "var_quantity",
    "cv_squared",
    "demand_class"
]

scd2_merge(
    spark=spark,
    source_df=product_curated_df,
    target_table=TARGET_TABLE,
    business_key="product_id",
    tracked_columns=tracked_columns
)

# --------------------------------------------------
# Preview Output
# --------------------------------------------------

display(
    spark.table(TARGET_TABLE)
    .orderBy(F.col("product_id"), F.col("valid_from"))
)
