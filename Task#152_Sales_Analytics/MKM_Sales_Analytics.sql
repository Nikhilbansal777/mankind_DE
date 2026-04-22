-- ============================================================
-- MKM SALES ANALYTICS - TASK #152
-- Top Selling Products & Categories
-- ============================================================
-- Author: Khyati Chauhan
-- Date: April 2026
-- Project: Mankind Matrix - AI & Semiconductor Platform
-- ============================================================


-- ============================================================
-- SECTION 1: DATABASE SETUP
-- ============================================================

CREATE DATABASE Mankind_Matrix;
GO

USE Mankind_Matrix;
GO


-- ============================================================
-- SECTION 2: CREATE TABLES
-- ============================================================

-- Products Table
CREATE TABLE products (
    id INT,
    brand VARCHAR(100),
    category_id INT,
    created_at VARCHAR(50),
    description VARCHAR(255),
    is_active INT,
    is_featured INT,
    model VARCHAR(100),
    name VARCHAR(100),
    sku VARCHAR(50),
    updated_at VARCHAR(50),
    average_rating VARCHAR(10)
);

-- Orders Table
CREATE TABLE orders (
    id INT,
    cart_id INT,
    created_at VARCHAR(50),
    discounts VARCHAR(10),
    notes VARCHAR(255),
    order_number VARCHAR(50),
    payment_id VARCHAR(50),
    payment_status VARCHAR(50),
    shipping_address_id INT,
    status VARCHAR(50),
    subtotal VARCHAR(20),
    tax VARCHAR(20),
    total VARCHAR(20),
    updated_at VARCHAR(50),
    user_id INT,
    shipping_value VARCHAR(20)
);

-- Order Items Table
CREATE TABLE order_items (
    id INT,
    created_at VARCHAR(50),
    product_id INT,
    product_name VARCHAR(100),
    product_price VARCHAR(20),
    quantity INT,
    subtotal VARCHAR(20),
    order_id INT
);

-- Returns Table
CREATE TABLE returns (
    id INT,
    created_at VARCHAR(50),
    product_id INT,
    reason VARCHAR(100),
    return_date VARCHAR(50),
    status VARCHAR(50),
    updated_at VARCHAR(50),
    user_id INT
);


-- ============================================================
-- SECTION 3: LOAD DATA FROM CSV FILES
-- ============================================================
-- Note: Update file paths as per your system

BULK INSERT products
FROM 'C:\Users\khyat\OneDrive\Desktop\Mankind Project\mankind_DE\Anomaly Detection\Tables\products.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n'
);

BULK INSERT orders
FROM 'C:\Users\khyat\OneDrive\Desktop\Mankind Project\mankind_DE\Anomaly Detection\Tables\orders.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n'
);

BULK INSERT order_items
FROM 'C:\Users\khyat\OneDrive\Desktop\Mankind Project\mankind_DE\Anomaly Detection\Tables\order_items.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n'
);

BULK INSERT returns
FROM 'C:\Users\khyat\OneDrive\Desktop\Mankind Project\mankind_DE\Anomaly Detection\Tables\returns.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n'
);


-- ============================================================
-- SECTION 4: VERIFY DATA LOADED
-- ============================================================

SELECT 'products' AS table_name, COUNT(*) AS row_count FROM products
UNION ALL
SELECT 'orders', COUNT(*) FROM orders
UNION ALL
SELECT 'order_items', COUNT(*) FROM order_items
UNION ALL
SELECT 'returns', COUNT(*) FROM returns;


-- ============================================================
-- SECTION 5: CREATE VIEWS FOR ANALYSIS
-- ============================================================

-- View 1: Top Selling Products
-- Aggregates revenue and units sold by product
GO
CREATE VIEW top_selling_products AS
SELECT 
    oi.product_id,
    oi.product_name,
    p.brand,
    p.category_id,
    SUM(CAST(oi.quantity AS INT)) AS units_sold,
    SUM(CAST(oi.subtotal AS DECIMAL(10,2))) AS total_revenue
FROM order_items oi
JOIN products p ON p.id = oi.product_id
JOIN orders o ON o.id = oi.order_id
WHERE o.status = 'completed'
GROUP BY oi.product_id, oi.product_name, p.brand, p.category_id;
GO


-- View 2: Top Selling Categories
-- Summarizes revenue and units sold by category
GO
CREATE VIEW top_selling_categories AS
SELECT 
    p.category_id,
    SUM(CAST(oi.quantity AS INT)) AS units_sold,
    SUM(CAST(oi.subtotal AS DECIMAL(10,2))) AS total_revenue
FROM order_items oi
JOIN products p ON p.id = oi.product_id
JOIN orders o ON o.id = oi.order_id
WHERE o.status = 'completed'
GROUP BY p.category_id;
GO


-- View 3: Daily Sales Trend
-- Tracks daily revenue, units sold, and order count
GO
CREATE VIEW sales_trend_daily AS
SELECT 
    CAST(o.created_at AS DATE) AS order_date,
    COUNT(DISTINCT o.id) AS total_orders,
    SUM(CAST(oi.quantity AS INT)) AS units_sold,
    SUM(CAST(oi.subtotal AS DECIMAL(10,2))) AS daily_revenue
FROM order_items oi
JOIN orders o ON o.id = oi.order_id
WHERE o.status = 'completed'
GROUP BY CAST(o.created_at AS DATE);
GO


-- View 4: Weekly Sales Trend
-- Aggregates sales metrics by week
GO
CREATE VIEW sales_trend_weekly AS
SELECT 
    DATEPART(WEEK, o.created_at) AS week_number,
    MIN(CAST(o.created_at AS DATE)) AS week_start,
    COUNT(DISTINCT o.id) AS total_orders,
    SUM(CAST(oi.quantity AS INT)) AS units_sold,
    SUM(CAST(oi.subtotal AS DECIMAL(10,2))) AS weekly_revenue
FROM order_items oi
JOIN orders o ON o.id = oi.order_id
WHERE o.status = 'completed'
GROUP BY DATEPART(WEEK, o.created_at);
GO


-- View 5: Product Sales Trend
-- Tracks product performance over time
GO
CREATE VIEW product_sales_trend AS
SELECT 
    CAST(o.created_at AS DATE) AS order_date,
    oi.product_name,
    p.brand,
    SUM(CAST(oi.quantity AS INT)) AS units_sold,
    SUM(CAST(oi.subtotal AS DECIMAL(10,2))) AS daily_revenue
FROM order_items oi
JOIN products p ON p.id = oi.product_id
JOIN orders o ON o.id = oi.order_id
WHERE o.status = 'completed'
GROUP BY CAST(o.created_at AS DATE), oi.product_name, p.brand;
GO


-- ============================================================
-- SECTION 6: ANALYSIS QUERIES
-- ============================================================

-- Query 1: Top Selling Products by Revenue
SELECT * FROM top_selling_products
ORDER BY total_revenue DESC;

-- Query 2: Top Selling Categories by Revenue
SELECT * FROM top_selling_categories
ORDER BY total_revenue DESC;

-- Query 3: Daily Sales Trend
SELECT * FROM sales_trend_daily
ORDER BY order_date;

-- Query 4: Weekly Sales Trend
SELECT * FROM sales_trend_weekly
ORDER BY week_number;

-- Query 5: Product Sales Trend
SELECT * FROM product_sales_trend
ORDER BY order_date, daily_revenue DESC;

-- Query 6: Summary KPIs
SELECT 
    COUNT(DISTINCT o.id) AS total_orders,
    SUM(CAST(oi.quantity AS INT)) AS total_units_sold,
    SUM(CAST(oi.subtotal AS DECIMAL(10,2))) AS total_revenue,
    ROUND(SUM(CAST(oi.subtotal AS DECIMAL(10,2))) / COUNT(DISTINCT o.id), 2) AS avg_order_value
FROM order_items oi
JOIN orders o ON o.id = oi.order_id
WHERE o.status = 'completed';


-- ============================================================
-- END OF SCRIPT
-- ============================================================
