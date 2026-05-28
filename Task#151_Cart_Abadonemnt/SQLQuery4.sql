USE Mankind_Matrix;
GO
ALTER VIEW abandoned_products AS
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