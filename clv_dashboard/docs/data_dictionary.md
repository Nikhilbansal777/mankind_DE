# Data Dictionary — Customer Lifetime Value Dashboard

This document defines all fields used across the data sources in the CLV Dashboard project.

---

## 1. MySQL Database — `mankind_matrix_db` (upstream source)

The CSV files below were prepared from this database. The Tableau workbook does **not** connect to MySQL directly; it reads the CSVs and the `.hyper` extract.

### Table: `customers`

| Field | Type | Description |
|-------|------|-------------|
| `customer_id` | INT | Unique identifier for each customer |
| `customer_name` | VARCHAR | Full name of the customer |
| `customer_segment` | VARCHAR | Segment classification: `High Value`, `Mid Value`, `Low Value` |
| `created_at` | DATETIME | Date the customer record was created |

### Table: `orders`

| Field | Type | Description |
|-------|------|-------------|
| `order_id` | INT | Unique identifier for each order |
| `customer_id` | INT | Foreign key linking to the `customers` table |
| `order_value` | DECIMAL | Monetary value of the order |
| `payment_status` | VARCHAR | Status of payment — use `CONFIRMED` for successful transactions |
| `order_date` | DATETIME | Date the order was placed |

> ⚠️ **Important:** The `payment_status` field uses `CONFIRMED`, not `PAID`. Always filter with `payment_status = 'CONFIRMED'` when calculating CLV.

---

## 2. CSV — `data/clv_summary.csv`

One row per customer (200 customers). Pre-aggregated synthetic data used for segment, cohort and top-customer analysis.

| Field | Type | Description |
|-------|------|-------------|
| `customer_id` | Integer | Unique customer identifier (matches `user_id` in `orders_for_tableau.csv`) |
| `customer_name` | String | Full name of the customer. **Not unique** — some different customers share a name, so always group by `customer_id` |
| `email` | String | Customer email (synthetic, `@example.com`) |
| `customer_since` | Datetime | Date the customer was acquired |
| `cohort_month` | String | Acquisition month, `YYYY-MM` (derived from `customer_since`; 2024-01 to 2024-12) |
| `total_orders` | Integer | Number of confirmed orders |
| `total_revenue` | Decimal | Sum of `total` across the customer's confirmed orders |
| `avg_order_value` | Decimal | `total_revenue / total_orders` |
| `lifespan_months` | Integer | Months from `customer_since` to the data snapshot date (customer tenure, not time between first and last order) |
| `clv` | Decimal | Customer Lifetime Value = `total_revenue` (includes tax and shipping) |
| `last_order_date` | Datetime | Date of the customer's most recent order |
| `customer_segment` | String | Segment based on `clv`. In this data, `High Value` customers have CLV above 3,000, `Mid Value` 1,000 – 3,000, and `Low Value` below 1,000 |

---

## 3. CSV — `data/orders_for_tableau.csv`

One row per order (1,015 orders, January 2024 – April 2026). Synthetic data used for the trend chart.

| Field | Type | Description |
|-------|------|-------------|
| `id` | Integer | Unique order identifier |
| `user_id` | Integer | Customer who placed the order (links to `customer_id` in `clv_summary.csv`) |
| `order_number` | String | Human-readable order number, e.g. `ORD-00001` |
| `subtotal` | Decimal | Item value before discounts, tax and shipping |
| `discounts` | Decimal | Discount applied |
| `tax` | Decimal | Tax charged (18% of `subtotal`) |
| `shipping_value` | Decimal | Shipping charged |
| `total` | Decimal | Order total = `subtotal − discounts + tax + shipping_value` |
| `status` | String | Order status (`CONFIRMED` for all rows) |
| `payment_status` | String | Payment status (`CONFIRMED` for all rows) |
| `created_at` | Datetime | Date the order was placed |
| `cohort_month` | String | Acquisition month of the customer who placed the order, `YYYY-MM` |

> This file has no `customer_segment` column, so the Customer Segment filter cannot be applied to the trend chart.

---

## 4. Fields Used in Tableau

The workbook has no custom calculated fields; CLV comes pre-calculated from `clv_summary.csv`.

| Shown as | Field and aggregation | Used in |
|----------|----------------------|---------|
| CLV | `SUM([clv])` per customer | Top Customers |
| Average CLV | `AVG([clv])` | Average CLV by Customer Segment, Cohort Analysis |
| Revenue by month | `SUM([total])` by continuous `MONTH([created_at])` | CLV Trend Over Time |
| Acquisition month | `[cohort_month]` | Cohort Analysis, Month of Creation filter |
