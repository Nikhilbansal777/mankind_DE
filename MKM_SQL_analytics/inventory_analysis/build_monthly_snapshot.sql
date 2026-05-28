-- MKM_SQL_analytics/inventory_analysis/build_monthly_snapshot.sql

-- Make sure target schema exists
CREATE SCHEMA IF NOT EXISTS mkm_analytics;

-- ===== Source Aggregations =====
WITH order_lines AS (
  SELECT
      oi.product_id,
      DATE_FORMAT(o.order_date, '%Y-%m-01') AS month_start,  -- first day of month
      SUM(oi.quantity) AS units_sold,
      SUM(oi.quantity * oi.unit_price) AS revenue
  FROM order_items oi
  JOIN orders o ON o.order_id = oi.order_id
  WHERE o.order_date >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)   -- last 12 months
  GROUP BY oi.product_id, DATE_FORMAT(o.order_date, '%Y-%m-01')
),
inventory_monthly AS (
  SELECT
      il.product_id,
      DATE_FORMAT(il.snapshot_date, '%Y-%m-01') AS month_start,
      AVG(il.stock_level) AS avg_inventory_units
  FROM inventory_logs il
  WHERE il.snapshot_date >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
  GROUP BY il.product_id, DATE_FORMAT(il.snapshot_date, '%Y-%m-01')
)

-- ===== Rebuild table atomically (simple approach) =====
-- Drop old table if present
DROP TABLE IF EXISTS mkm_analytics.product_movement_monthly;

-- Create fresh snapshot
CREATE TABLE mkm_analytics.product_movement_monthly AS
SELECT
    p.product_id,
    p.name      AS product_name,
    p.category,
    p.brand,
    ol.month_start,
    COALESCE(ol.units_sold, 0)           AS units_sold,
    COALESCE(ol.revenue, 0.0)            AS revenue,
    COALESCE(im.avg_inventory_units, 0)  AS avg_inventory_units,

    -- Units-based turnover: units_sold / avg units on hand
    CASE
      WHEN COALESCE(im.avg_inventory_units, 0) = 0 THEN NULL
      ELSE ol.units_sold / im.avg_inventory_units
    END AS turnover_ratio,

    -- DIO (monthly proxy): 30 / turnover
    CASE
      WHEN COALESCE(im.avg_inventory_units, 0) = 0 OR COALESCE(ol.units_sold,0) = 0 THEN NULL
      ELSE 30 / (ol.units_sold / im.avg_inventory_units)
    END AS dio_days

FROM order_lines ol
LEFT JOIN inventory_monthly im
  ON im.product_id = ol.product_id AND im.month_start = ol.month_start
JOIN products p
  ON p.product_id = ol.product_id;

-- Helpful indexes for BI
CREATE INDEX idx_pmm_month   ON mkm_analytics.product_movement_monthly (month_start);
CREATE INDEX idx_pmm_product ON mkm_analytics.product_movement_monthly (product_id);