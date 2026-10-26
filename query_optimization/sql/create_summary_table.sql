CREATE TABLE IF NOT EXISTS daily_order_summary (
    order_date DATE NOT NULL,
    order_count INT NOT NULL,
    total_revenue DECIMAL(14,2) NOT NULL,
    avg_order_value DECIMAL(14,2) NOT NULL,
    paid_order_count INT NOT NULL,
    cancelled_order_count INT NOT NULL,
    refreshed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (order_date)
);