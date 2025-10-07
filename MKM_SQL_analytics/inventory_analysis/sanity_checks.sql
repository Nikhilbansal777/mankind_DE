-- MKM_SQL_analytics/inventory_analysis/02_sanity_checks.sql
-- Purpose: Quick health checks on mkm_analytics.product_movement_monthly (PMM)
-- Run after 01_build_monthly_snapshot.sql

-- ================================
-- 0) Basic row & coverage checks
-- ================================
-- Total rows
SELECT COUNT(*) AS pmm_rows
FROM mkm_analytics.product_movement_monthly;

-- Distinct products covered
SELECT COUNT(DISTINCT product_id) AS distinct_products
FROM mkm_analytics.product_movement_monthly;

-- Date range present
SELECT MIN(month_start) AS min_month, MAX(month_start) AS max_month
FROM mkm_analytics.product_movement_monthly;

-- Latest month row count (should be near distinct product count; gaps indicate no sales/inventory)
WITH lm AS (
  SELECT MAX(month_start) AS m FROM mkm_analytics.product_movement_monthly
)
SELECT COUNT(*) AS rows_latest_month
FROM mkm_analytics.product_movement_monthly p
JOIN lm ON lm.m = p.month_start;

-- ========================================
-- 1) Metadata completeness (name/category)
-- ========================================
-- Missing product metadata (should be zero if products join was clean)
SELECT
  SUM(product_name IS NULL) AS null_product_name,
  SUM(category IS NULL)     AS null_category,
  SUM(brand IS NULL)        AS null_brand
FROM mkm_analytics.product_movement_monthly;

-- ============================
-- 2) Nulls & zero-value sanity
-- ============================
-- Overall null/zero rates (turnover/dio can be NULL when inventory or sales are zero)
SELECT
  COUNT(*)                                            AS total_rows,
  SUM(units_sold = 0)                                 AS zero_units_sold,
  SUM(revenue = 0)                                    AS zero_revenue,
  SUM(avg_inventory_units = 0)                        AS zero_avg_inventory,
  SUM(turnover_ratio IS NULL)                         AS null_turnover,
  SUM(dio_days IS NULL)                               AS null_dio
FROM mkm_analytics.product_movement_monthly;

-- Products with sales but zero avg inventory (possible inventory logging gaps)
SELECT product_id, product_name, month_start, units_sold, avg_inventory_units
FROM mkm_analytics.product_movement_monthly
WHERE units_sold > 0 AND (avg_inventory_units = 0 OR avg_inventory_units IS NULL)
ORDER BY month_start DESC, units_sold DESC
LIMIT 50;

-- Products with inventory but zero sales (potential slow movers / dead stock)
SELECT product_id, product_name, month_start, units_sold, avg_inventory_units
FROM mkm_analytics.product_movement_monthly
WHERE (units_sold = 0 OR units_sold IS NULL) AND avg_inventory_units > 0
ORDER BY month_start DESC, avg_inventory_units DESC
LIMIT 50;

-- ==========================================
-- 3) Distribution summaries (spot outliers)
-- ==========================================
-- Turnover and DIO distribution stats
SELECT
  ROUND(AVG(turnover_ratio), 3) AS avg_turnover,
  ROUND(STDDEV_SAMP(turnover_ratio), 3) AS std_turnover,
  ROUND(MIN(turnover_ratio), 3) AS min_turnover,
  ROUND(MAX(turnover_ratio), 3) AS max_turnover,
  ROUND(AVG(dio_days), 1) AS avg_dio,
  ROUND(STDDEV_SAMP(dio_days), 1) AS std_dio,
  ROUND(MIN(dio_days), 1) AS min_dio,
  ROUND(MAX(dio_days), 1) AS max_dio
FROM mkm_analytics.product_movement_monthly
WHERE turnover_ratio IS NOT NULL AND dio_days IS NOT NULL;

-- Top/Bottom 10 by turnover (latest month)
WITH lm AS (
  SELECT MAX(month_start) AS m FROM mkm_analytics.product_movement_monthly
)
SELECT *
FROM mkm_analytics.product_movement_monthly p
JOIN lm ON lm.m = p.month_start
ORDER BY turnover_ratio DESC
LIMIT 10;

WITH lm AS (
  SELECT MAX(month_start) AS m FROM mkm_analytics.product_movement_monthly
)
SELECT *
FROM mkm_analytics.product_movement_monthly p
JOIN lm ON lm.m = p.month_start
ORDER BY turnover_ratio ASC
LIMIT 10;

-- =============================
-- 4) Integrity / uniqueness
-- =============================
-- Ensure 1 row per (product_id, month_start)
SELECT
  SUM(cnt > 1) AS product_month_duplicates
FROM (
  SELECT product_id, month_start, COUNT(*) AS cnt
  FROM mkm_analytics.product_movement_monthly
  GROUP BY product_id, month_start
) t;

-- =============================
-- 5) Reconciliation spot-checks
-- =============================
-- Compare PMM sales vs raw order_items for the latest month (sanity)
WITH lm AS (
  SELECT DATE_FORMAT(MAX(o.order_date), '%Y-%m-01') AS m
  FROM orders o
),
raw AS (
  SELECT
    oi.product_id,
    DATE_FORMAT(o.order_date, '%Y-%m-01') AS month_start,
    SUM(oi.quantity) AS raw_units
  FROM order_items oi
  JOIN orders o ON o.order_id = oi.order_id
  GROUP BY oi.product_id, DATE_FORMAT(o.order_date, '%Y-%m-01')
)
SELECT
  p.product_id,
  p.product_name,
  p.month_start,
  p.units_sold      AS pmm_units,
  r.raw_units       AS raw_units,
  (p.units_sold - r.raw_units) AS diff_units
FROM mkm_analytics.product_movement_monthly p
JOIN lm ON lm.m = p.month_start
LEFT JOIN raw r
  ON r.product_id = p.product_id AND r.month_start = p.month_start
WHERE COALESCE(p.units_sold,0) <> COALESCE(r.raw_units,0)
ORDER BY ABS(p.units_sold - COALESCE(r.raw_units,0)) DESC
LIMIT 100;

-- ===================================
-- 6) Performance hint (optional use)
-- ===================================
-- Use EXPLAIN on a representative query you’ll run often (filter latest month by category)
EXPLAIN
SELECT product_id, product_name, category, brand, units_sold, turnover_ratio, dio_days
FROM mkm_analytics.product_movement_monthly
WHERE month_start = (SELECT MAX(month_start) FROM mkm_analytics.product_movement_monthly)
  AND category = 'Electronics';