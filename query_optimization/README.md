# Query Performance Optimization

## Objective

Optimize repeated analytical queries using a precomputed summary table.

MySQL does not provide native materialized views, so a summary-table approach was implemented.

## Source Table

`orders`

## Summary Table

`daily_order_summary`

The table stores daily aggregated metrics:

- order count
- total revenue
- average order value
- paid order count
- cancelled order count
- refresh timestamp

## Baseline Query

The original query aggregates the `orders` table by date.

Execution plan:

- table scan on `orders`
- aggregation using temporary table
- temporary table scan
- sort

Observed execution time:

Approximately 1.36 - 1.38 ms.

## Optimized Query

The optimized query reads directly from `daily_order_summary`.

Execution plan:

- primary-key index scan

Observed execution time:

Approximately 0.07 - 0.11 ms.

## Validation

The summary-table results were compared with the original aggregation query.

Result:

Validation successful. No mismatched rows were found.

## Refresh

Run:

```powershell
python query_optimization\refresh_summary.py