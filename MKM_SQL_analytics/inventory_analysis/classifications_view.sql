-- MKM_SQL_analytics/inventory_analysis/03_classification_views.sql
-- Purpose: Classify products into FAST / MEDIUM / SLOW for the latest month
-- Requires: 01_build_monthly_snapshot.sql (mkm_analytics.product_movement_monthly)

-- Ensure target schema exists
CREATE SCHEMA IF NOT EXISTS mkm_analytics;

-- =========================
-- View A: Global classification
-- =========================
-- Uses turnover-based quintiles across ALL products in the latest month.
-- Higher turnover => faster class. NULL turnover (no sales or no inventory) => SLOW.

DROP VIEW IF EXISTS mkm_analytics.product_movement_latest_classified;

CREATE VIEW mkm_analytics.product_movement_latest_classified AS
WITH lm AS (
  SELECT MAX(month_start) AS month_start
  FROM mkm_analytics.product_movement_monthly
),
base AS (
  SELECT
      p.product_id,
      p.product_name,
      p.category,
      p.brand,
      p.month_start,
      p.units_sold,
      p.revenue,
      p.avg_inventory_units,
      p.turnover_ratio,
      p.dio_days
  FROM mkm_analytics.product_movement_monthly p
  JOIN lm ON lm.month_start = p.month_start
),
ranked AS (
  SELECT
      b.*,
      -- Rank only rows with non-null turnover; others get bucket 1 (slow) in final step
      CASE
        WHEN b.turnover_ratio IS NULL THEN NULL
        ELSE NTILE(5) OVER (ORDER BY b.turnover_ratio ASC)
      END AS ntile5_turnover_asc,  -- 1 = slowest … 5 = fastest
      PERCENT_RANK() OVER (ORDER BY b.turnover_ratio ASC) AS pr_turnover_asc
  FROM base b
)
SELECT
    product_id,
    product_name,
    category,
    brand,
    month_start,
    units_sold,
    revenue,
    avg_inventory_units,
    turnover_ratio,
    dio_days,
    COALESCE(ntile5_turnover_asc, 1) AS turnover_bucket_asc,
    pr_turnover_asc,
    CASE
      WHEN COALESCE(ntile5_turnover_asc, 1) IN (5,4) THEN 'FAST'
      WHEN COALESCE(ntile5_turnover_asc, 1) = 3 THEN 'MEDIUM'
      ELSE 'SLOW'
    END AS movement_class
FROM ranked;

-- =========================
-- View B: Category-relative classification
-- =========================
-- Uses tertiles WITHIN each category (helps avoid category-size bias).
-- 3 = FAST (top tercile), 2 = MEDIUM, 1 = SLOW. NULL turnover => SLOW.

DROP VIEW IF EXISTS mkm_analytics.product_movement_latest_classified_cat;

CREATE VIEW mkm_analytics.product_movement_latest_classified_cat AS
WITH lm AS (
  SELECT MAX(month_start) AS month_start
  FROM mkm_analytics.product_movement_monthly
),
base AS (
  SELECT
      p.product_id,
      p.product_name,
      p.category,
      p.brand,
      p.month_start,
      p.units_sold,
      p.revenue,
      p.avg_inventory_units,
      p.turnover_ratio,
      p.dio_days
  FROM mkm_analytics.product_movement_monthly p
  JOIN lm ON lm.month_start = p.month_start
),
ranked AS (
  SELECT
      b.*,
      CASE
        WHEN b.turnover_ratio IS NULL THEN NULL
        ELSE NTILE(3) OVER (PARTITION BY b.category ORDER BY b.turnover_ratio ASC)
      END AS ntile3_by_category_asc,  -- 1 = slowest … 3 = fastest within category
      PERCENT_RANK() OVER (PARTITION BY b.category ORDER BY b.turnover_ratio ASC) AS pr_by_category_asc
  FROM base b
)
SELECT
    product_id,
    product_name,
    category,
    brand,
    month_start,
    units_sold,
    revenue,
    avg_inventory_units,
    turnover_ratio,
    dio_days,
    COALESCE(ntile3_by_category_asc, 1) AS cat_bucket_asc,
    pr_by_category_asc,
    CASE
      WHEN COALESCE(ntile3_by_category_asc, 1) = 3 THEN 'FAST'
      WHEN COALESCE(ntile3_by_category_asc, 1) = 2 THEN 'MEDIUM'
      ELSE 'SLOW'
    END AS movement_class_cat
FROM ranked;

-- =========================
-- View C: KPI rollups (Category & Brand)
-- =========================
-- Quick tiles for BI: share of FAST products, averages, etc.

DROP VIEW IF EXISTS mkm_analytics.inventory_movement_kpis_category;
CREATE VIEW mkm_analytics.inventory_movement_kpis_category AS
WITH src AS (
  SELECT * FROM mkm_analytics.product_movement_latest_classified
)
SELECT
  category,
  COUNT(*)                                                   AS products_in_cat,
  SUM(movement_class = 'FAST')                               AS fast_count,
  ROUND(100 * SUM(movement_class = 'FAST')/COUNT(*), 1)      AS fast_pct,
  ROUND(AVG(turnover_ratio), 2)                              AS avg_turnover,
  ROUND(AVG(dio_days), 1)                                    AS avg_dio
FROM src
GROUP BY category
ORDER BY fast_pct DESC;

DROP VIEW IF EXISTS mkm_analytics.inventory_movement_kpis_brand;
CREATE VIEW mkm_analytics.inventory_movement_kpis_brand AS
WITH src AS (
  SELECT * FROM mkm_analytics.product_movement_latest_classified
)
SELECT
  brand,
  COUNT(*)                                                   AS products_in_brand,
  SUM(movement_class = 'FAST')                               AS fast_count,
  ROUND(100 * SUM(movement_class = 'FAST')/COUNT(*), 1)      AS fast_pct,
  ROUND(AVG(turnover_ratio), 2)                              AS avg_turnover,
  ROUND(AVG(dio_days), 1)                                    AS avg_dio
FROM src
GROUP BY brand
ORDER BY fast_pct DESC;