CREATE OR REPLACE VIEW anomaly_report_aug_1_21 AS
SELECT
  ar.product_id,
  p.category_id,
  p.brand,
  p.name,

  /* product totals for the window */
  ar.units_sold_total,
  ar.returns_total,
  ROUND(ar.returns_total / NULLIF(ar.units_sold_total, 0), 3) AS eventual_return_rate,

  ar.ux_returns,
  ar.defect_returns,
  ar.logistics_returns,
  ar.other_returns,

  /* product reason shares */
  ROUND(ar.ux_returns        / NULLIF(ar.returns_total,0), 3) AS ux_share,
  ROUND(ar.defect_returns    / NULLIF(ar.returns_total,0), 3) AS defect_share,
  ROUND(ar.logistics_returns / NULLIF(ar.returns_total,0), 3) AS logistics_share,

  /* category baselines (same window) */
  ROUND(COALESCE(cat.cat_return_rate,  port.port_return_rate,  0), 3) AS base_return_rate,
  ROUND(COALESCE(cat.cat_ret_per_day,  port.port_ret_per_day,  0), 3) AS base_returns_per_day,
  ROUND(COALESCE(cat.cat_ux_share,     port.port_ux_share,     0), 3) AS base_ux_share,
  ROUND(COALESCE(cat.cat_defect_share, port.port_defect_share, 0), 3) AS base_defect_share,
  ROUND(COALESCE(cat.cat_log_share,    port.port_log_share,    0), 3) AS base_logistics_share,

  /* ---- anomaly flags vs category (fallback portfolio) ---- */
  CASE
    WHEN ar.units_sold_total >= 3
     AND (ar.returns_total / NULLIF(ar.units_sold_total, 0)) >= COALESCE(cat.cat_return_rate,  port.port_return_rate,  0) * 1.30
     AND (ar.returns_total / NULLIF(ar.units_sold_total, 0)) -   COALESCE(cat.cat_return_rate,  port.port_return_rate,  0) >= 0.02
    THEN 1 ELSE 0
  END AS flag_high_return_rate,

  CASE
    WHEN ar.returns_total >= 2
     AND (ar.returns_total / NULLIF(DATEDIFF('2025-08-21','2025-08-01') + 1, 0))
         >= COALESCE(cat.cat_ret_per_day, port.port_ret_per_day, 0) * 2.0
     AND (ar.returns_total / NULLIF(DATEDIFF('2025-08-21','2025-08-01') + 1, 0))
         -  COALESCE(cat.cat_ret_per_day, port.port_ret_per_day, 0) >= 1.0
    THEN 1 ELSE 0
  END AS flag_volume_spike,

  CASE
    WHEN ar.returns_total >= 2
     AND (
       (ar.ux_returns / NULLIF(ar.returns_total,0)) - COALESCE(cat.cat_ux_share,     port.port_ux_share,     0) >= 0.10
       OR (ar.ux_returns / NULLIF(ar.returns_total,0)) >= 0.50
     )
    THEN 1 ELSE 0
  END AS flag_ux_surge,

  CASE
    WHEN ar.returns_total >= 2
     AND (
       (ar.defect_returns / NULLIF(ar.returns_total,0)) - COALESCE(cat.cat_defect_share, port.port_defect_share, 0) >= 0.10
       OR (ar.defect_returns / NULLIF(ar.returns_total,0)) >= 0.50
     )
    THEN 1 ELSE 0
  END AS flag_defect_surge,

  CASE
    WHEN ar.returns_total >= 2
     AND (
       (ar.logistics_returns / NULLIF(ar.returns_total,0)) - COALESCE(cat.cat_log_share, port.port_log_share, 0) >= 0.10
       OR (ar.logistics_returns / NULLIF(ar.returns_total,0)) >= 0.50
     )
    THEN 1 ELSE 0
  END AS flag_logistics_surge,

  TRIM(BOTH ', ' FROM CONCAT_WS(', ',
    IF(ar.units_sold_total >= 3
       AND (ar.returns_total / NULLIF(ar.units_sold_total, 0)) >= COALESCE(cat.cat_return_rate, port.port_return_rate, 0) * 1.30
       AND (ar.returns_total / NULLIF(ar.units_sold_total, 0)) -   COALESCE(cat.cat_return_rate, port.port_return_rate, 0) >= 0.02, 'HIGH_RETURN_RATE', NULL),
    IF(ar.returns_total >= 2
       AND (ar.returns_total / NULLIF(DATEDIFF('2025-08-21','2025-08-01') + 1, 0)) >= COALESCE(cat.cat_ret_per_day, port.port_ret_per_day, 0) * 2.0
       AND (ar.returns_total / NULLIF(DATEDIFF('2025-08-21','2025-08-01') + 1, 0)) -  COALESCE(cat.cat_ret_per_day, port.port_ret_per_day, 0) >= 1.0, 'VOLUME_SPIKE', NULL),
    IF(ar.returns_total >= 2
       AND ( (ar.ux_returns        / NULLIF(ar.returns_total,0)) - COALESCE(cat.cat_ux_share,     port.port_ux_share,     0) >= 0.10
             OR (ar.ux_returns        / NULLIF(ar.returns_total,0)) >= 0.50 ), 'UX_SURGE', NULL),
    IF(ar.returns_total >= 2
       AND ( (ar.defect_returns    / NULLIF(ar.returns_total,0)) - COALESCE(cat.cat_defect_share, port.port_defect_share, 0) >= 0.10
             OR (ar.defect_returns    / NULLIF(ar.returns_total,0)) >= 0.50 ), 'DEFECT_SURGE', NULL),
    IF(ar.returns_total >= 2
       AND ( (ar.logistics_returns / NULLIF(ar.returns_total,0)) - COALESCE(cat.cat_log_share,    port.port_log_share,    0) >= 0.10
             OR (ar.logistics_returns / NULLIF(ar.returns_total,0)) >= 0.50 ), 'LOGISTICS_SURGE', NULL)
  )) AS anomaly_flags

FROM
  (
    SELECT
      oi.product_id,
      SUM(oi.quantity) AS units_sold_total,
      SUM( CASE WHEN r.id IS NOT NULL THEN 1 ELSE 0 END ) AS returns_total,
      SUM( CASE WHEN r.reason REGEXP 'COMPLEX_UI|INCOMPATIBLE|DOCUMENTATION_CONFUSING|DID_NOT_MEET_EXPECTATIONS|SETUP_DIFFICULT' THEN 1 ELSE 0 END ) AS ux_returns,
      SUM( CASE WHEN r.reason REGEXP 'DEFECT|DOA|OVERHEATING|SCREEN_ISSUE|SPEAKER_DISTORTION|FAN_NOISE|BATTERY_DRAIN' THEN 1 ELSE 0 END )            AS defect_returns,
      SUM( CASE WHEN r.reason REGEXP 'DAMAGED_IN_TRANSIT|UNDERPOWERED_SUPPLY' THEN 1 ELSE 0 END )                                                    AS logistics_returns,
      SUM( CASE WHEN r.id IS NOT NULL
                 AND r.reason NOT REGEXP 'COMPLEX_UI|INCOMPATIBLE|DOCUMENTATION_CONFUSING|DID_NOT_MEET_EXPECTATIONS|SETUP_DIFFICULT'
                 AND r.reason NOT REGEXP 'DEFECT|DOA|OVERHEATING|SCREEN_ISSUE|SPEAKER_DISTORTION|FAN_NOISE|BATTERY_DRAIN'
                 AND r.reason NOT REGEXP 'DAMAGED_IN_TRANSIT|UNDERPOWERED_SUPPLY'
                 THEN 1 ELSE 0 END ) AS other_returns
    FROM `test_orders` o
    JOIN `test_order_items` oi ON oi.order_id = o.id
    LEFT JOIN `test_returns` r
           ON r.product_id = oi.product_id
          AND r.user_id    = o.user_id
          AND DATE(r.created_at) BETWEEN '2025-08-01' AND '2025-08-21'
    WHERE DATE(o.created_at) BETWEEN '2025-08-01' AND '2025-08-21'
    GROUP BY oi.product_id
  ) AS ar

JOIN `test_products` p
  ON p.id = ar.product_id

LEFT JOIN (
  SELECT
    p.category_id,
    SUM(oi.quantity) AS cat_units_sold,
    SUM(CASE WHEN r.id IS NOT NULL THEN 1 ELSE 0 END) AS cat_returns,
    SUM(CASE WHEN r.id IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(SUM(oi.quantity),0) AS cat_return_rate,
    SUM(CASE WHEN r.reason REGEXP 'COMPLEX_UI|INCOMPATIBLE|DOCUMENTATION_CONFUSING|DID_NOT_MEET_EXPECTATIONS|SETUP_DIFFICULT' THEN 1 ELSE 0 END)
      / NULLIF(SUM(CASE WHEN r.id IS NOT NULL THEN 1 ELSE 0 END),0) AS cat_ux_share,
    SUM(CASE WHEN r.reason REGEXP 'DEFECT|DOA|OVERHEATING|SCREEN_ISSUE|SPEAKER_DISTORTION|FAN_NOISE|BATTERY_DRAIN' THEN 1 ELSE 0 END)
      / NULLIF(SUM(CASE WHEN r.id IS NOT NULL THEN 1 ELSE 0 END),0) AS cat_defect_share,
    SUM(CASE WHEN r.reason REGEXP 'DAMAGED_IN_TRANSIT|UNDERPOWERED_SUPPLY' THEN 1 ELSE 0 END)
      / NULLIF(SUM(CASE WHEN r.id IS NOT NULL THEN 1 ELSE 0 END),0) AS cat_log_share,
    SUM(CASE WHEN r.id IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(DATEDIFF('2025-08-21','2025-08-01') + 1, 0) AS cat_ret_per_day
  FROM `test_orders` o
  JOIN `test_order_items` oi ON oi.order_id = o.id
  JOIN `test_products` p     ON p.id = oi.product_id
  LEFT JOIN `test_returns` r
         ON r.product_id = oi.product_id
        AND r.user_id    = o.user_id
        AND DATE(r.created_at) BETWEEN '2025-08-01' AND '2025-08-21'
  WHERE DATE(o.created_at) BETWEEN '2025-08-01' AND '2025-08-21'
  GROUP BY p.category_id
) AS cat
  ON cat.category_id = p.category_id

CROSS JOIN (
  SELECT
    SUM(CASE WHEN r.id IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(SUM(oi.quantity),0) AS port_return_rate,
    SUM(CASE WHEN r.reason REGEXP 'COMPLEX_UI|INCOMPATIBLE|DOCUMENTATION_CONFUSING|DID_NOT_MEET_EXPECTATIONS|SETUP_DIFFICULT' THEN 1 ELSE 0 END)
      / NULLIF(SUM(CASE WHEN r.id IS NOT NULL THEN 1 ELSE 0 END),0) AS port_ux_share,
    SUM(CASE WHEN r.reason REGEXP 'DEFECT|DOA|OVERHEATING|SCREEN_ISSUE|SPEAKER_DISTORTION|FAN_NOISE|BATTERY_DRAIN' THEN 1 ELSE 0 END)
      / NULLIF(SUM(CASE WHEN r.id IS NOT NULL THEN 1 ELSE 0 END),0) AS port_defect_share,
    SUM(CASE WHEN r.reason REGEXP 'DAMAGED_IN_TRANSIT|UNDERPOWERED_SUPPLY' THEN 1 ELSE 0 END)
      / NULLIF(SUM(CASE WHEN r.id IS NOT NULL THEN 1 ELSE 0 END),0) AS port_log_share,
    SUM(CASE WHEN r.id IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(DATEDIFF('2025-08-21','2025-08-01') + 1, 0) AS port_ret_per_day
  FROM `test_orders` o
  JOIN `test_order_items` oi ON oi.order_id = o.id
  LEFT JOIN `test_returns` r
         ON r.product_id = oi.product_id
        AND r.user_id    = o.user_id
        AND DATE(r.created_at) BETWEEN '2025-08-01' AND '2025-08-21'
  WHERE DATE(o.created_at) BETWEEN '2025-08-01' AND '2025-08-21'
) AS port;