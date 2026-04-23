# Databricks Customer and Product Analytics Pipeline (TPC-H)

This project demonstrates how to build a scalable analytics pipeline on Databricks using the TPC-H benchmark dataset. The goal is to transform raw transactional data into meaningful customer behavior metrics and product demand insights using PySpark and Delta Lake.

## What This Project Is

This is an analytics engineering project built on Databricks that:

* Processes large-scale relational data (TPC-H dataset)
* Performs transformations using PySpark
* Generates business-level metrics for customers and products
* Stores curated outputs as Delta tables
* Maintains historical changes using SCD Type 2 logic

The project is structured to reflect how real-world data teams build pipelines that turn raw data into analytical datasets.

## Why This Project

Most data engineering projects focus only on ingestion or basic transformations. This project goes a step further by demonstrating how to:

* Work with a realistic, multi-table dataset (TPC-H)
* Build end-to-end transformations in Databricks
* Derive business-relevant metrics instead of just moving data
* Implement slowly changing dimension (SCD Type 2) logic for historical tracking
* Create reusable analytical tables for downstream use

This makes the project closer to real production scenarios where data is not only processed but also prepared for decision-making.

## Dataset: TPC-H

TPC-H is a standard benchmark dataset used to simulate real-world business data such as:

* customers
* orders
* products
* line items

It is commonly used to test data warehouse performance and analytical query capabilities.

In this project, TPC-H data is used as the source for building customer and product analytics.

## Project Overview

The pipeline processes raw TPC-H tables available in Databricks and builds two main analytical outputs:

### 1. Customer Metrics Table

This table provides insights into customer purchasing behavior, including:

* purchase frequency
* average days between orders
* customer lifetime value
* order interval segmentation (high, medium, low activity)

These metrics help in understanding how often customers buy and how valuable they are over time.

### 2. Product Demand Table

This table classifies product demand patterns using statistical measures:

* Average Demand Interval (ADI)
* Coefficient of Variation (CV²)

Based on these metrics, products are categorized into demand types such as:

* smooth
* intermittent
* erratic
* lumpy

This type of classification is commonly used in supply chain and inventory planning.

## Pipeline Flow

1. Raw data is read from TPC-H tables in Databricks
2. PySpark transformations join and aggregate data across multiple tables
3. Business metrics are derived for customers and products
4. Results are written as Delta tables in the curated layer
5. SCD Type 2 merge logic is applied to track historical changes

## Technology Stack

* Databricks
* PySpark
* Delta Lake
* SQL

## Data Modeling Approach

The project follows a simple layered approach:

* Source layer: TPC-H raw tables
* Transformation layer: PySpark-based aggregations
* Curated layer: Delta tables with business metrics

SCD Type 2 logic ensures that historical versions of records are preserved when values change.

## Key Features

* Distributed processing using Spark
* Multi-table joins and aggregations
* Business-focused metric engineering
* SCD Type 2 history tracking
* Delta Lake storage for efficient querying

## Output Tables

### Customer Metrics (Delta Table)

Contains customer-level behavioral insights and segmentation.

### Product Demand (Delta Table)

Contains product-level demand classification and statistical metrics.

## How to Run

1. Open Databricks workspace
2. Load the notebooks from the `src/` folder
3. Execute notebooks in order:

   * customer curation
   * product curation
4. Verify output Delta tables in the target schema

## What This Project Demonstrates

* Ability to work with large-scale datasets in Databricks
* Experience with PySpark transformations
* Understanding of analytical data modeling
* Implementation of SCD Type 2 logic
* Converting raw data into business insights

## Why Databricks Was Used

This project was built on Databricks because it aligns well with the type of data processing and analytics being performed.

### Distributed Processing for Analytical Workloads

The TPC-H dataset consists of multiple related tables such as customers, orders, and line items. Building customer and product analytics requires joining these tables and running aggregations at scale.

Databricks uses Apache Spark, which provides distributed processing. This allows complex joins and aggregations to run efficiently even on large datasets.

### Efficient Transformation Layer

The project performs transformation-heavy operations including:

* multi-table joins
* aggregations for metrics such as lifetime value and demand patterns
* feature engineering for analytics

Spark’s execution engine is optimized for these types of workloads, making it a good fit for transformation pipelines.

### Delta Lake for Reliable Storage

The curated outputs of this project are stored as Delta tables. Delta Lake provides:

* ACID transactions
* schema enforcement
* efficient reads and writes
* support for incremental updates

These features make the data more reliable and easier to maintain compared to plain file-based storage.

### Support for SCD Type 2 Logic

This project uses SCD Type 2 logic to maintain historical records. Delta Lake supports efficient merge operations, which are required to:

* update existing records
* insert new versions when data changes
* preserve historical values

This makes Databricks a strong choice for implementing slowly changing dimensions.

### Unified Development Environment

Databricks provides an integrated environment where PySpark and SQL can be used together. This simplifies development and allows the entire pipeline to be built, tested, and executed in one place.

## Summary

This project shows how to use Databricks and PySpark to build a practical analytics pipeline on top of a standard benchmark dataset. It focuses on generating meaningful insights from data and maintaining historical accuracy using SCD Type 2 techniques.
