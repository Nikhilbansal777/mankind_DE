# Executive Sales Dashboard

An interactive Tableau Public dashboard visualizing weekly sales KPIs 
for the Mankind Matrix platform, built from live MySQL data hosted on Aiven Cloud.

## Project Overview
This dashboard helps executives monitor sales performance at a glance,
including revenue trends, units sold, and top performing products.

## Data Source
- Database: Aiven Cloud MySQL
- Database Name: mankind_matrix_db
- Table: `weekly_sales`
- Fields:
  - `Product` — product name
  - `Year` — sales year
  - `Week Number` — week of the year
  - `Total Quantity` — units sold
  - `Total Revenue` — revenue in USD

## Dashboard Features
- **Total Revenue KPI** — overall revenue card
- **Total Units Sold KPI** — total quantity sold
- **Weekly Revenue Trend** — line chart showing revenue by week per product
- **Top Products by Revenue** — horizontal bar chart ranked by revenue
- **Interactive Filter Action** — click any product to filter all charts

## Tools Used
- Tableau Public (free)
- MySQL Workbench
- Aiven Cloud (MySQL hosting)

## Files
- `data/Weekly_Sales.csv` — exported sales data
- `tableau/executive_dashboard.twbx` — Tableau packaged workbook
- `screenshots/dashboard_preview.png` — dashboard preview image

## How to Open
1. Download and install Tableau Public (free) from https://public.tableau.com
2. Open `tableau/executive_dashboard.twbx`
3. Dashboard loads with all charts and filters ready
