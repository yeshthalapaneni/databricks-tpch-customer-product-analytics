# Databricks notebook source
# Customer Curation Pipeline
# Purpose: derive customer behavior metrics from TPC-H tables
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
TARGET_TABLE = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.customer_metrics_scd"

# --------------------------------------------------
# Read Source Tables
# --------------------------------------------------

customer_df = spark.table(f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.customer")
orders_df = spark.table(f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.orders")
lineitem_df = spark.table(f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.lineitem")

# --------------------------------------------------
# Prepare Order-Level Revenue
# --------------------------------------------------

order_revenue_df = (
    lineitem_df
    .groupBy("l_orderkey")
    .agg(
        F.sum(
            F.col("l_extendedprice") * (1 - F.col("l_discount"))
        ).alias("order_revenue")
    )
)

orders_enriched_df = (
    orders_df.alias("o")
    .join(order_revenue_df.alias("r"), F.col("o.o_orderkey") == F.col("r.l_orderkey"), "left")
    .select(
        F.col("o.o_orderkey").alias("order_id"),
        F.col("o.o_custkey").alias("customer_id"),
        F.col("o.o_orderdate").alias("order_date"),
        F.coalesce(F.col("r.order_revenue"), F.lit(0)).alias("order_revenue")
    )
)

# --------------------------------------------------
# Calculate Customer Order Intervals
# --------------------------------------------------

window_spec = Window.partitionBy("customer_id").orderBy("order_date")

customer_orders_df = (
    orders_enriched_df
    .withColumn("previous_order_date", F.lag("order_date").over(window_spec))
    .withColumn(
        "days_between_orders",
        F.datediff(F.col("order_date"), F.col("previous_order_date"))
    )
)

# --------------------------------------------------
# Build Customer Metrics
# --------------------------------------------------

customer_metrics_df = (
    customer_orders_df
    .groupBy("customer_id")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.round(F.avg("days_between_orders"), 2).alias("avg_days_between_orders"),
        F.round(F.sum("order_revenue"), 2).alias("lifetime_value"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date")
    )
    .withColumn(
        "purchase_frequency",
        F.when(F.col("total_orders") >= 15, F.lit("high"))
         .when(F.col("total_orders") >= 5, F.lit("medium"))
         .otherwise(F.lit("low"))
    )
    .withColumn(
        "order_interval_bucket",
        F.when(F.col("avg_days_between_orders").isNull(), F.lit("single_order"))
         .when(F.col("avg_days_between_orders") <= 30, F.lit("frequent"))
         .when(F.col("avg_days_between_orders") <= 90, F.lit("moderate"))
         .otherwise(F.lit("infrequent"))
    )
)

customer_curated_df = (
    customer_df.alias("c")
    .join(customer_metrics_df.alias("m"), F.col("c.c_custkey") == F.col("m.customer_id"), "left")
    .select(
        F.col("c.c_custkey").alias("customer_id"),
        F.col("c.c_name").alias("customer_name"),
        F.col("c.c_nationkey").alias("nation_key"),
        F.col("c.c_acctbal").alias("account_balance"),
        F.coalesce(F.col("m.total_orders"), F.lit(0)).alias("total_orders"),
        F.col("m.avg_days_between_orders"),
        F.coalesce(F.col("m.lifetime_value"), F.lit(0)).alias("lifetime_value"),
        F.col("m.first_order_date"),
        F.col("m.last_order_date"),
        F.coalesce(F.col("m.purchase_frequency"), F.lit("low")).alias("purchase_frequency"),
        F.coalesce(F.col("m.order_interval_bucket"), F.lit("single_order")).alias("order_interval_bucket")
    )
)

# --------------------------------------------------
# Write Curated Output with SCD Type 2 Logic
# --------------------------------------------------

tracked_columns = [
    "customer_name",
    "nation_key",
    "account_balance",
    "total_orders",
    "avg_days_between_orders",
    "lifetime_value",
    "first_order_date",
    "last_order_date",
    "purchase_frequency",
    "order_interval_bucket"
]

scd2_merge(
    spark=spark,
    source_df=customer_curated_df,
    target_table=TARGET_TABLE,
    business_key="customer_id",
    tracked_columns=tracked_columns
)

# --------------------------------------------------
# Preview Output
# --------------------------------------------------

display(
    spark.table(TARGET_TABLE)
    .orderBy(F.col("customer_id"), F.col("valid_from"))
)
