-- ================================================
-- Table Optimization and Partitioning Script
-- Database: mankind_matrix_db
-- Author: Pavan Thotakura
-- Date: 2026-05-21
-- ================================================

USE mankind_matrix_db;

-- ================================================
-- STEP 1: OPTIMIZE EXISTING TABLES
-- ================================================

OPTIMIZE TABLE orders;
OPTIMIZE TABLE order_items;
OPTIMIZE TABLE products;
OPTIMIZE TABLE payments;
OPTIMIZE TABLE user_events;
OPTIMIZE TABLE weekly_sales;

-- ================================================
-- STEP 2: ADD INDEXES FOR FASTER QUERYING
-- ================================================

-- Index on orders created_at for date-based queries
ALTER TABLE orders ADD INDEX idx_orders_created_at (created_at);

-- Index on orders status for filtering
ALTER TABLE orders ADD INDEX idx_orders_status (status);

-- Index on orders user_id for user-based queries
ALTER TABLE orders ADD INDEX idx_orders_user_id (user_id);

-- Index on order_items for joining with orders
ALTER TABLE order_items ADD INDEX idx_order_items_order_id (order_id);

-- Index on payments for status filtering
ALTER TABLE payments ADD INDEX idx_payments_status (status);

-- Index on user_events for date-based queries
ALTER TABLE user_events ADD INDEX idx_user_events_created_at (created_at);

-- ================================================
-- STEP 3: VERIFY INDEXES WERE CREATED
-- ================================================

SHOW INDEX FROM orders;
SHOW INDEX FROM order_items;
SHOW INDEX FROM products;