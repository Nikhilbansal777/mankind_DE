# Dashboard Guide — Customer Lifetime Value Dashboard

This guide explains how to navigate and use the CLV Dashboard in Tableau.

---

## Accessing the Dashboard

1. Open `tableau/clv_dashboard.twbx` in Tableau Desktop (or Tableau Reader)
2. Navigate to the **CLV Dashboard** tab at the bottom of the workbook

---

## Dashboard Layout

The dashboard is divided into four visualization panels:

| Panel | Location | Chart Type |
|-------|----------|------------|
| Top Customers | Top-left | Horizontal bar chart |
| CLV Trend Over Time | Top-right | Line chart |
| Cohort Analysis | Bottom-left | Heatmap |
| Average CLV by Customer Segment | Bottom-right | Bar chart |

---

## Filters

Two filters are shown on the right-hand side of the dashboard. Each one applies to a specific chart.

### Customer Segment Filter
- Options: `All`, `High Value`, `Low Value`, `Mid Value`
- Applies to: **Average CLV by Customer Segment**
- To filter the other customer charts by segment, click a bar in that chart (see Cross-filtering below)

### Month of Creation Filter
- Options: acquisition months `2024-01` to `2024-12`
- Applies to: **CLV Trend Over Time**
- Shows only orders from customers acquired in the selected months
- Useful for isolating specific cohorts

---

## Visualization Guide

### 1. Top Customers
- Shows the **top 20 customers ranked by CLV**, one bar per customer ID
- Each row shows the customer ID and name; different customers can share a name
- Clicking a segment or a cohort cell elsewhere shows the top 20 within that selection

### 2. CLV Trend Over Time
- Displays **total order revenue by month**, January 2024 to April 2026
- X-axis: month and year
- Y-axis: total order value
- Useful for spotting growth, seasonal peaks and troughs

### 3. Cohort Analysis
- A **heatmap** showing average CLV by customer segment (rows) and acquisition month (columns)
- Darker colours indicate higher average CLV
- Cohort months shown: 2024-01 through 2024-12
- Rows: `High Value`, `Low Value`, `Mid Value`

### 4. Average CLV by Customer Segment
- A **bar chart** comparing average CLV across the three segments
- Segments: `High Value`, `Low Value`, `Mid Value`
- Useful for understanding which segment drives the most value on average

---

## Tips

- **Cross-filtering:** Clicking a segment bar in *Average CLV by Customer Segment* filters Top Customers and Cohort Analysis. Clicking a cell in *Cohort Analysis* filters the other three charts. Click the same mark again to clear.
- **Tooltips:** Hover over any data point to see exact values
- **Reset filters:** Select `(All)` in both filter panels and clear any selected marks
