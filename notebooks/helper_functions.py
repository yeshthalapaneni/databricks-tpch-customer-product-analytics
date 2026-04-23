# Helper functions for Databricks customer and product analytics project
# Purpose: provide reusable utilities for SCD Type 2 merge operations
# and common table management steps.

from delta.tables import DeltaTable
from pyspark.sql import functions as F


def add_scd_columns(df):
    """
    Adds standard SCD Type 2 tracking columns to a DataFrame.

    Columns added:
    - valid_from: timestamp when the record becomes active
    - valid_to: timestamp when the record stops being active
    - is_current: flag to identify the active version
    """
    return (
        df.withColumn("valid_from", F.current_timestamp())
          .withColumn("valid_to", F.lit(None).cast("timestamp"))
          .withColumn("is_current", F.lit(True))
    )


def create_delta_table_if_not_exists(spark, table_name, df):
    """
    Creates the target Delta table if it does not already exist.
    The initial load writes the full dataset with SCD Type 2 columns.
    """
    if not spark.catalog.tableExists(table_name):
        initial_df = add_scd_columns(df)
        (
            initial_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(table_name)
        )
        print(f"Created table: {table_name}")
    else:
        print(f"Table already exists: {table_name}")


def scd2_merge(spark, source_df, target_table, business_key, tracked_columns):
    """
    Applies SCD Type 2 merge logic to a Delta table.

    Parameters:
    - spark: active Spark session
    - source_df: incoming transformed DataFrame
    - target_table: target Delta table name
    - business_key: primary business key column, for example customer_id or product_id
    - tracked_columns: list of columns that should trigger a new version when changed

    Logic:
    1. If target table does not exist, create it as an initial load.
    2. Compare incoming records with current active records.
    3. Expire changed current records by setting valid_to and is_current = false.
    4. Insert new rows as the latest current version.
    """

    create_delta_table_if_not_exists(spark, target_table, source_df)

    delta_table = DeltaTable.forName(spark, target_table)

    current_df = (
        spark.table(target_table)
        .filter(F.col("is_current") == True)
        .select(*([business_key] + tracked_columns))
    )

    source_alias = source_df.alias("src")
    current_alias = current_df.alias("tgt")

    join_condition = F.col(f"src.{business_key}") == F.col(f"tgt.{business_key}")

    comparison_df = source_alias.join(current_alias, join_condition, "left")

    change_condition = None
    for col_name in tracked_columns:
        condition = (
            F.coalesce(F.col(f"src.{col_name}"), F.lit("__null__"))
            != F.coalesce(F.col(f"tgt.{col_name}"), F.lit("__null__"))
        )
        change_condition = condition if change_condition is None else (change_condition | condition)

    changed_or_new_df = (
        comparison_df
        .filter(F.col(f"tgt.{business_key}").isNull() | change_condition)
        .select([F.col(f"src.{c}") for c in source_df.columns])
        .dropDuplicates([business_key])
    )

    keys_to_expire = changed_or_new_df.select(business_key).distinct()

    (
        delta_table.alias("t")
        .merge(
            keys_to_expire.alias("k"),
            f"t.{business_key} = k.{business_key} AND t.is_current = true"
        )
        .whenMatchedUpdate(set={
            "valid_to": "current_timestamp()",
            "is_current": "false"
        })
        .execute()
    )

    new_versions_df = add_scd_columns(changed_or_new_df)

    (
        new_versions_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(target_table)
    )

    print(f"SCD Type 2 merge completed for table: {target_table}")


def optimize_delta_table(spark, table_name):
    """
    Optional helper to optimize Delta table performance.
    Useful after repeated merges or large writes.
    """
    spark.sql(f"OPTIMIZE {table_name}")
    print(f"Optimized table: {table_name}")
