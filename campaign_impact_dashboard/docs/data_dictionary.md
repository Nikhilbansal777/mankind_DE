# Data Dictionary — Marketing Campaign Impact Dashboard

All files are in `campaign_impact_dashboard/data/`. Dates are `YYYY-MM-DD`, timestamps `YYYY-MM-DD HH:MM:SS`.

---

## 1. `product_views.csv` (fact table — one row per product view)

| Field | Type | Description |
|-------|------|-------------|
| `view_id` | INT | Unique id of the view event |
| `view_ts` | DATETIME | Timestamp of the view |
| `view_date` | DATE | Date part of `view_ts` (use for daily trends) |
| `user_id` | INT | User who viewed the product (1–5000, matches `users.id`) |
| `product_id` | INT | Product viewed → `products.product_id` |
| `category` | VARCHAR | Product category (denormalised for convenience) |
| `traffic_source` | VARCHAR | `organic_search`, `direct`, `referral`, `social`, `email`, `paid_search`, `display` |
| `campaign_id` | INT (nullable) | Campaign the view is attributed to (via UTM). Empty = organic / not attributed |
| `is_campaign_view` | INT (0/1) | 1 if `campaign_id` is set |
| `added_to_cart` | INT (0/1) | 1 if the user added the product to cart in the same session |
| `purchased` | INT (0/1) | 1 if the product was purchased (order with `payment_status = 'CONFIRMED'`) |
| `period_campaign_id` | INT (nullable) | Campaign whose window or 14-day baseline this view's category/date falls in. Used for lift — counts **all** views of the category, not just attributed ones |
| `campaign_period` | VARCHAR | `Baseline` (14 days before the campaign), `Campaign` (during), or `Other` |

## 2. `campaigns.csv` (dimension)

| Field | Type | Description |
|-------|------|-------------|
| `campaign_id` | INT | Unique campaign id |
| `campaign_name` | VARCHAR | Display name |
| `channel` | VARCHAR | `Email`, `Social`, `Influencer`, `Search`, `Display` |
| `target_category` | VARCHAR | Product category the campaign promotes |
| `start_date` / `end_date` | DATE | Campaign window (inclusive) |
| `budget_usd` | DECIMAL | Total spend |
| `duration_days` | INT | `end_date − start_date + 1` |

## 3. `products.csv` (dimension)

| Field | Type | Description |
|-------|------|-------------|
| `product_id` | INT | Unique product id |
| `product_name` | VARCHAR | Product name |
| `brand` | VARCHAR | Brand |
| `category` | VARCHAR | `Laptops`, `Smartphones`, `Audio`, `Wearables`, `Gaming`, `Accessories` |
| `price` | DECIMAL | Unit price (USD) |

## 4. `campaign_impact_summary.csv` (derived — one row per campaign)

| Field | Formula / Description |
|-------|------------------------|
| `baseline_avg_daily_views` | Views of `target_category` in the 14 days before `start_date` ÷ 14 |
| `campaign_avg_daily_views` | Views of `target_category` from `start_date` to `end_date` ÷ `duration_days` |
| `view_lift_pct` | (`campaign_avg_daily_views` ÷ `baseline_avg_daily_views` − 1) × 100 |
| `incremental_views` | (`campaign_avg_daily_views` − `baseline_avg_daily_views`) × `duration_days` |
| `attributed_views` | Count of views with this `campaign_id` |
| `attributed_add_to_carts` | Sum of `added_to_cart` for attributed views |
| `attributed_purchases` | Sum of `purchased` for attributed views |
| `attributed_revenue_usd` | Sum of `price` for attributed views that were purchased |
| `view_to_cart_rate_pct` | `attributed_add_to_carts` ÷ `attributed_views` × 100 |
| `cost_per_incremental_view_usd` | `budget_usd` ÷ `incremental_views` |
| `roas` | `attributed_revenue_usd` ÷ `budget_usd` (return on ad spend) |

---

## 5. `views_for_tableau.csv` (Tableau source)

All the columns of `product_views.csv`, plus `product_name`, `brand` and `price` from products, plus `campaign_name`, `channel` and `duration_days` from campaigns (joined on `period_campaign_id`). Because it's a single flat table, the workbook needs no joins.

---

## Relationships (if modelling the raw tables yourself)

```
products (product_id) 1 ──── * product_views (product_id)
campaigns (campaign_id) 1 ──── * product_views (campaign_id)   [left join: views without campaign stay]
campaign_impact_summary (campaign_id) 1 ──── 1 campaigns (campaign_id)
```
