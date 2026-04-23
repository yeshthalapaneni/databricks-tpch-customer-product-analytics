-- Validation Queries for Customer and Product Analytics Tables
-- Purpose: Verify correctness of transformations and SCD Type 2 behavior

------------------------------------------------------------
-- 1. Customer Metrics Validation
------------------------------------------------------------

-- Check total number of customers
SELECT COUNT(*) AS total_customers
FROM s3catalog.tpch_star_silver.customer_metrics_scd;

-- View sample customer records
SELECT *
FROM s3catalog.tpch_star_silver.customer_metrics_scd
LIMIT 10;

-- Check customers with highest lifetime value
SELECT customer_id, lifetime_value
FROM s3catalog.tpch_star_silver.customer_metrics_scd
ORDER BY lifetime_value DESC
LIMIT 10;

-- Validate purchase frequency distribution
SELECT purchase_frequency, COUNT(*) AS count
FROM s3catalog.tpch_star_silver.customer_metrics_scd
GROUP BY purchase_frequency
ORDER BY count DESC;

-- Validate average days between orders
SELECT customer_id, avg_days_between_orders
FROM s3catalog.tpch_star_silver.customer_metrics_scd
ORDER BY avg_days_between_orders DESC
LIMIT 10;

------------------------------------------------------------
-- 2. Product Demand Validation
------------------------------------------------------------

-- Check total number of products
SELECT COUNT(*) AS total_products
FROM s3catalog.tpch_star_silver.product_demand_scd;

-- View sample product records
SELECT *
FROM s3catalog.tpch_star_silver.product_demand_scd
LIMIT 10;

-- Validate demand classification distribution
SELECT demand_class, COUNT(*) AS count
FROM s3catalog.tpch_star_silver.product_demand_scd
GROUP BY demand_class
ORDER BY count DESC;

-- Check products with highest demand variability
SELECT product_id, cv_squared
FROM s3catalog.tpch_star_silver.product_demand_scd
ORDER BY cv_squared DESC
LIMIT 10;

------------------------------------------------------------
-- 3. SCD Type 2 Validation
------------------------------------------------------------

-- Check if multiple versions exist for same customer
SELECT customer_id, COUNT(*) AS version_count
FROM s3catalog.tpch_star_silver.customer_metrics_scd
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Check current active records
SELECT *
FROM s3catalog.tpch_star_silver.customer_metrics_scd
WHERE is_current = TRUE
LIMIT 10;

-- Validate historical tracking (valid_from / valid_to)
SELECT customer_id, valid_from, valid_to
FROM s3catalog.tpch_star_silver.customer_metrics_scd
ORDER BY customer_id, valid_from;

------------------------------------------------------------
-- 4. Data Quality Checks
------------------------------------------------------------

-- Check for null customer IDs
SELECT COUNT(*) AS null_customer_ids
FROM s3catalog.tpch_star_silver.customer_metrics_scd
WHERE customer_id IS NULL;

-- Check for negative values in lifetime value
SELECT *
FROM s3catalog.tpch_star_silver.customer_metrics_scd
WHERE lifetime_value < 0;

-- Check for invalid product demand classes
SELECT DISTINCT demand_class
FROM s3catalog.tpch_star_silver.product_demand_scd;
