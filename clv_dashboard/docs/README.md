# Customer Lifetime Value Dashboard

## Project Overview

This project delivers an interactive Customer Lifetime Value (CLV) Dashboard built in Tableau, designed to help the business understand customer value across segments, track trends over time, and identify top-performing customers.

---

## Project Structure

```
clv_dashboard/
├── data/
│   ├── clv_summary.csv              # Synthetic CLV summary data (one row per customer)
│   └── orders_for_tableau.csv       # Synthetic order-level data
├── tableau/
│   ├── clv_dashboard.twbx           # Packaged Tableau workbook (open this one)
│   ├── Book1.twb                    # Unpackaged workbook (source)
│   └── orders_for_tableau.hyper     # Tableau extract of the orders data
└── docs/
    ├── README.md                     # This file
    ├── data_dictionary.md            # Field definitions for all data sources
    └── dashboard_guide.md            # User guide for the dashboard
```

---

## Data Sources

| Source | Type | Description |
|--------|------|-------------|
| `mankind_matrix_db` | MySQL (upstream) | Production database the CSV data is based on. The workbook does not connect to it directly. |
| `clv_summary.csv` | CSV (synthetic) | One row per customer with pre-calculated CLV, segment and cohort month |
| `orders_for_tableau.csv` | CSV (synthetic) | Order-level data used for the trend chart |

> **Note:** The `payment_status` field in the MySQL database uses the value `CONFIRMED` (not `PAID`) for successful transactions.

---

## Dashboard Visualizations

1. **Average CLV by Customer Segment** — Average CLV for each customer segment (High Value, Mid Value, Low Value)
2. **CLV Trend Over Time** — Order revenue by month, January 2024 to April 2026
3. **Top Customers** — Horizontal bar chart of the top 20 customers by CLV
4. **Cohort Analysis** — Heatmap of average CLV by customer segment and acquisition month

---

## Setup Instructions

### Tableau
1. Open `tableau/clv_dashboard.twbx` in Tableau Desktop, or in the free Tableau Reader to view only
2. All data is embedded in the packaged workbook, so no connection setup is needed

### MySQL (for refreshing the source data)
- **Database:** `mankind_matrix_db`
- **Key filter:** Use `payment_status = 'CONFIRMED'` for confirmed orders

---

## Branching Strategy

```
main
 └── feature/clv-dashboard
```

Changes are merged from `feature/clv-dashboard → main` through a pull request. Merging requires manager approval.

---

## Project Timeline

| Week | Focus |
|------|-------|
| Week 1 | Data foundation and CLV formula logic |
| Week 2 | Building all four visualizations |
| Week 3 | UI polish, testing, and manager presentation |
