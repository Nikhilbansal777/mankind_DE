EXPLAIN ANALYZE
SELECT
    DATE(created_at) AS order_date,
    COUNT(*) AS order_count,
    ROUND(SUM(total), 2) AS total_revenue,
    ROUND(AVG(total), 2) AS avg_order_value,
    SUM(CASE WHEN payment_status = 'PAID' THEN 1 ELSE 0 END) AS paid_order_count,
    SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_order_count
FROM orders
GROUP BY DATE(created_at)
ORDER BY order_date;