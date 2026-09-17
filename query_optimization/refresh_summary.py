import os

import mysql.connector
from dotenv import load_dotenv

load_dotenv()

connection = mysql.connector.connect(
    host=os.getenv("DB_HOST"),
    port=int(os.getenv("DB_PORT")),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USERNAME"),
    password=os.getenv("DB_PASSWORD"),
)

cursor = connection.cursor()

refresh_query = """
INSERT INTO daily_order_summary (
    order_date,
    order_count,
    total_revenue,
    avg_order_value,
    paid_order_count,
    cancelled_order_count,
    refreshed_at
)
SELECT
    DATE(created_at) AS order_date,
    COUNT(*) AS order_count,
    ROUND(SUM(total), 2) AS total_revenue,
    ROUND(AVG(total), 2) AS avg_order_value,
    SUM(CASE WHEN payment_status = 'PAID' THEN 1 ELSE 0 END),
    SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END),
    CURRENT_TIMESTAMP
FROM orders
GROUP BY DATE(created_at)

ON DUPLICATE KEY UPDATE
    order_count = VALUES(order_count),
    total_revenue = VALUES(total_revenue),
    avg_order_value = VALUES(avg_order_value),
    paid_order_count = VALUES(paid_order_count),
    cancelled_order_count = VALUES(cancelled_order_count),
    refreshed_at = CURRENT_TIMESTAMP;
"""

cursor.execute(refresh_query)
connection.commit()

print("daily_order_summary refreshed successfully.")

cursor.close()
connection.close()