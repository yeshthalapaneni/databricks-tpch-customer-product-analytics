-- Validation Queries
-- Purpose: verify customer and product analytics outputs,
-- check SCD Type 2 behavior, and confirm curated tables look correct.

-- ==================================================
-- Customer Metrics Validation
-- ==================================================

-- Total records in customer metrics table
SELECT COUNT(*) AS total_customer_records
FROM s3catalog.tpch_star_silver.customer_metrics_scd;

-- Current active customer records
SELECT COUNT(*) AS current_customer_records
FROM s3catalog.tpch_star_silver.customer_metrics_scd
WHERE is_current = TRUE;

-- Sample customer records
SELECT *
FROM s3catalog.tpch_star_silver.customer_metrics_scd
ORDER BY customer_id, valid_from
LIMIT 20;

-- Customers with highest lifetime value
SELECT customer_id, customer_name, lifetime_value
FROM s3catalog.tpch_star_silver.customer_metrics_scd
WHERE is_current = TRUE
ORDER BY lifetime_value DESC
LIMIT 10;

-- Purchase frequency distribution
SELECT purchase_frequency, COUNT(*) AS record_count
FROM s3catalog.tpch_star_silver.customer_metrics_scd
WHERE is_current = TRUE
GROUP BY purchase_frequency
ORDER BY record_count DESC;

-- Order interval bucket distribution
SELECT order_interval_bucket, COUNT(*) AS record_count
FROM s3catalog.tpch_star_silver.customer_metrics_scd
WHERE is_current = TRUE
GROUP BY order_interval_bucket
ORDER BY record_count DESC;

-- Customers with historical versions
SELECT customer_id, COUNT(*) AS version_count
FROM s3catalog.tpch_star_silver.customer_metrics_scd
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY version_count DESC, customer_id;

-- ==================================================
-- Product Demand Validation
-- ==================================================

-- Total records in product demand table
SELECT COUNT(*) AS total_product_records
FROM s3catalog.tpch_star_silver.product_demand_scd;

-- Current active product records
SELECT COUNT(*) AS current_product_records
FROM s3catalog.tpch_star_silver.product_demand_scd
WHERE is_current = TRUE;

-- Sample product records
SELECT *
FROM s3catalog.tpch_star_silver.product_demand_scd
ORDER BY product_id, valid_from
LIMIT 20;

-- Demand class distribution
SELECT demand_class, COUNT(*) AS record_count
FROM s3catalog.tpch_star_silver.product_demand_scd
WHERE is_current = TRUE
GROUP BY demand_class
ORDER BY record_count DESC;

-- Products with highest variability
SELECT product_id, product_name, cv_squared
FROM s3catalog.tpch_star_silver.product_demand_scd
WHERE is_current = TRUE
ORDER BY cv_squared DESC
LIMIT 10;

-- Products with historical versions
SELECT product_id, COUNT(*) AS version_count
FROM s3catalog.tpch_star_silver.product_demand_scd
GROUP BY product_id
HAVING COUNT(*) > 1
ORDER BY version_count DESC, product_id;

-- ==================================================
-- SCD Type 2 Validation
-- ==================================================

-- Check valid_from and valid_to timeline for a single customer
SELECT customer_id, customer_name, valid_from, valid_to, is_current
FROM s3catalog.tpch_star_silver.customer_metrics_scd
WHERE customer_id = 1
ORDER BY valid_from;

-- Check valid_from and valid_to timeline for a single product
SELECT product_id, product_name, valid_from, valid_to, is_current
FROM s3catalog.tpch_star_silver.product_demand_scd
WHERE product_id = 1
ORDER BY valid_from;

-- Ensure only one current record exists per customer
SELECT customer_id, COUNT(*) AS current_record_count
FROM s3catalog.tpch_star_silver.customer_metrics_scd
WHERE is_current = TRUE
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Ensure only one current record exists per product
SELECT product_id, COUNT(*) AS current_record_count
FROM s3catalog.tpch_star_silver.product_demand_scd
WHERE is_current = TRUE
GROUP BY product_id
HAVING COUNT(*) > 1;

-- ==================================================
-- Basic Data Quality Checks
-- ==================================================

-- Null customer IDs
SELECT COUNT(*) AS null_customer_ids
FROM s3catalog.tpch_star_silver.customer_metrics_scd
WHERE customer_id IS NULL;

-- Null product IDs
SELECT COUNT(*) AS null_product_ids
FROM s3catalog.tpch_star_silver.product_demand_scd
WHERE product_id IS NULL;

-- Negative lifetime values
SELECT *
FROM s3catalog.tpch_star_silver.customer_metrics_scd
WHERE lifetime_value < 0;

-- Invalid demand classes
SELECT DISTINCT demand_class
FROM s3catalog.tpch_star_silver.product_demand_scd
ORDER BY demand_class;
