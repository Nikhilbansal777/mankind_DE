SELECT
    source.order_date,
    source.order_count AS source_order_count,
    summary.order_count AS summary_order_count,
    source.total_revenue AS source_total_revenue,
    summary.total_revenue AS summary_total_revenue,
    source.avg_order_value AS source_avg_order_value,
    summary.avg_order_value AS summary_avg_order_value,
    source.paid_order_count AS source_paid_order_count,
    summary.paid_order_count AS summary_paid_order_count,
    source.cancelled_order_count AS source_cancelled_order_count,
    summary.cancelled_order_count AS summary_cancelled_order_count
FROM (
    SELECT
        DATE(created_at) AS order_date,
        COUNT(*) AS order_count,
        ROUND(SUM(total), 2) AS total_revenue,
        ROUND(AVG(total), 2) AS avg_order_value,
        SUM(CASE WHEN payment_status = 'PAID' THEN 1 ELSE 0 END) AS paid_order_count,
        SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_order_count
    FROM orders
    GROUP BY DATE(created_at)
) source
JOIN daily_order_summary summary
    ON source.order_date = summary.order_date
WHERE
    source.order_count <> summary.order_count
    OR source.total_revenue <> summary.total_revenue
    OR source.avg_order_value <> summary.avg_order_value
    OR source.paid_order_count <> summary.paid_order_count
    OR source.cancelled_order_count <> summary.cancelled_order_count;