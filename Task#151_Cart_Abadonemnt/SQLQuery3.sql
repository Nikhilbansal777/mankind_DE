USE Mankind_Matrix;
GO

-- View 1: Cart Abandonment Summary
CREATE VIEW cart_abandonment_summary AS
SELECT 
    status,
    COUNT(*) AS cart_count,
    SUM(total) AS total_value,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM cart), 2) AS percentage
FROM cart
GROUP BY status;
GO

-- View 2: Abandonment Rate KPI
CREATE VIEW cart_abandonment_rate AS
SELECT 
    COUNT(*) AS total_carts,
    SUM(CASE WHEN status = 'ABANDONED' THEN 1 ELSE 0 END) AS abandoned_carts,
    SUM(CASE WHEN status = 'CONVERTED' THEN 1 ELSE 0 END) AS converted_carts,
    ROUND(SUM(CASE WHEN status = 'ABANDONED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS abandonment_rate,
    SUM(CASE WHEN status = 'ABANDONED' THEN total ELSE 0 END) AS lost_revenue
FROM cart;
GO

-- View 3: Daily Cart Trend
CREATE VIEW cart_trend_daily AS
SELECT 
    CAST(created_at AS DATE) AS cart_date,
    COUNT(*) AS total_carts,
    SUM(CASE WHEN status = 'ABANDONED' THEN 1 ELSE 0 END) AS abandoned,
    SUM(CASE WHEN status = 'CONVERTED' THEN 1 ELSE 0 END) AS converted,
    SUM(total) AS total_value
FROM cart
GROUP BY CAST(created_at AS DATE);
GO

-- View 4: Abandoned Products (Which products are left in carts?)
CREATE VIEW abandoned_products AS
SELECT 
    ci.product_name,
    COUNT(*) AS times_abandoned,
    SUM(ci.quantity) AS total_quantity,
    SUM(ci.quantity * ci.price) AS lost_revenue
FROM cart_item ci
JOIN cart c ON c.id = ci.cart_id
WHERE c.status = 'ABANDONED'
GROUP BY ci.product_name;
GO