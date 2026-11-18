# Dashboard Guide — Marketing Campaign Impact on Product Views

The ready-made workbook is `tableau/campaign_impact_dashboard.twbx`. Open it in Tableau Desktop and it lands on the **Campaign Impact** dashboard (1300 × 1000).

## What's in the workbook

**Data sources**
- **Campaign Views:** `views_for_tableau.csv` (one row per view)
- **Campaign Summary:** `campaign_impact_summary.csv` (one row per campaign)

**Sheets.** The individual sheets are hidden behind the dashboard. Right-click a dashboard zone and choose **Go to Sheet** to edit one.

| Sheet | What it shows | Value to check |
|---|---|---|
| KPI Total Views | All product views | 90,174 |
| KPI Campaign Share | % of views from campaign links | 7.7% |
| KPI Avg Lift | Average view lift across campaigns | 87.4% |
| KPI Incremental Views | Extra views vs baseline | 7,120 |
| KPI Cost per Incremental View | Budget ÷ incremental views | $6.11 |
| KPI ROAS | Attributed revenue ÷ budget | 6.60x |
| Daily Views Trend | Stacked area, campaign vs organic per day (Category filter at top right) | Blue spikes during each campaign window |
| Baseline vs Campaign | Baseline and in-campaign daily views per campaign | e.g. Spring Audio 67.4 → 184.7 |
| View Lift by Campaign | Lift %, coloured by channel | Spring Audio Launch 174.2% at the top, Gamer Week 16.4% at the bottom |
| Channel Performance | Attributed views per channel, colour/label = ROAS | Social 2,171 views / 11.01x; Display 110 / 0.15x |
| Top Campaign Products | Top 10 products by campaign-driven views | Dell Laptop 100 = 764 |
| Conversion Campaign vs Organic | Add-to-cart and purchase rates | Cart 11.26% vs 8.02%; purchase 4.08% vs 2.33% |

`docs/screenshots/dashboard_preview.png` shows the expected result. Tableau's default colours and fonts will look a little different.

**If Tableau asks for a data file on opening** (this happens with the unpackaged `.twb`), point it to the matching CSV in `data/`.

**Suggested polish once it's open:** set Daily Views Trend colours (Campaign = blue, Organic = light grey), and add a Filter action (Dashboard → Actions) from *View Lift by Campaign* to *Top Campaign Products*.

---

The steps below show how to build the same dashboard by hand, if you want to understand or recreate it. Part B explains how to read it.

```
┌──────────────────────────────────────────────────────────────────────────┐
│ Title                                         [Campaign ▼] [Category ▼]  │
├────────────┬────────────┬────────────┬────────────┬────────────┬─────────┤
│ Total views│ Campaign   │ Avg lift % │ Incremental│ Cost /     │ ROAS    │
│            │ views %    │            │ views      │ incr. view │         │
├────────────┴────────────┴────────────┴────────────┴────────────┴─────────┤
│ 1. Daily views trend by category, campaign days highlighted              │
├─────────────────────────────────────┬────────────────────────────────────┤
│ 2. Baseline vs campaign (lift %)    │ 3. Attributed views by channel     │
├─────────────────────────────────────┼────────────────────────────────────┤
│ 4. Top products per campaign        │ 5. View → cart → purchase funnel   │
└─────────────────────────────────────┴────────────────────────────────────┘
```

---

## Part A — Build Steps

### Step 1. Connect the data

1. **Connect → Text file →** `data/product_views.csv`.
2. Drag in `products.csv` and **relate** on `product_id = product_id`.
3. Drag in `campaigns.csv` and relate on `period_campaign_id = campaign_id`.
   *(Using `period_campaign_id` here, not `campaign_id`, lets one campaign's name label its baseline and campaign-period views together.)*
4. Drag in `campaign_impact_summary.csv` and relate to `campaigns` on `campaign_id = campaign_id`.
5. Check data types: `view_date` = Date, `view_ts` = Date & Time, `period_campaign_id` / `campaign_id` = Number (whole).
6. Rename the data source to **Campaign Impact**.

### Step 2. Calculated fields

Create these in `product_views` (right-click → Create Calculated Field):

| Name | Formula |
|---|---|
| `Views` | `COUNT([view_id])` |
| `Campaign Views` | `SUM([is_campaign_view])` |
| `Campaign View Share` | `[Campaign Views] / [Views]` (format: %) |
| `Baseline Daily Views` | `SUM(IF [campaign_period] = "Baseline" THEN 1 END) / 14` |
| `Campaign Daily Views` | `SUM(IF [campaign_period] = "Campaign" THEN 1 END) / MIN([duration_days])` |
| `View Lift %` | `[Campaign Daily Views] / [Baseline Daily Views] - 1` (format: %, 1 decimal) |
| `Incremental Views` | `([Campaign Daily Views] - [Baseline Daily Views]) * MIN([duration_days])` |
| `Add to Cart Rate` | `SUM([added_to_cart]) / COUNT([view_id])` |
| `Purchase Rate` | `SUM([purchased]) / COUNT([view_id])` |
| `Revenue` | `SUM(IF [purchased] = 1 THEN [price] END)` |
| `Is Campaign Day` | `[campaign_period] = "Campaign"` |

> `Baseline Daily Views`, `Campaign Daily Views`, `View Lift %` and `Incremental Views` only make sense **per campaign**, so always have `campaign_name` in the view (or filter to one campaign). For the KPI tiles that need a total across campaigns, use the summary-table fields below.

For KPI tiles, from `campaign_impact_summary`:

| Name | Formula |
|---|---|
| `Avg Lift % (KPI)` | `AVG([view_lift_pct]) / 100` |
| `Total Incremental Views (KPI)` | `SUM([incremental_views])` |
| `Cost per Incremental View (KPI)` | `SUM([budget_usd]) / SUM([incremental_views])` |
| `ROAS (KPI)` | `SUM([attributed_revenue_usd]) / SUM([budget_usd])` |

### Step 3. KPI tiles (6 sheets)

For each tile: drag the measure to **Text**, set the font to 28 pt bold, and put the label in the sheet title (11 pt, grey). Hide the headers.
Tiles: `Views`, `Campaign View Share`, `Avg Lift % (KPI)`, `Total Incremental Views (KPI)`, `Cost per Incremental View (KPI)` (format $0.00), `ROAS (KPI)` (format 0.00"x").

### Step 4. Sheet 1 — Daily Views Trend

- **Columns:** `view_date` (continuous, **exact date** or Day).
- **Rows:** `Views`.
- **Color:** `category`.
- **Mark:** Line.
- **Highlight campaign days:** duplicate `Views` on Rows → make it a **dual axis** → mark type **Bar** → put `Is Campaign Day` on **Color** (True = light grey, False = transparent/white). **Synchronize axes**, then right-click the bar axis → Move marks to back.
  *Alternative:* add a reference band per campaign (Analytics → Reference Band → from start to end date).
- **Tooltip:** date, category, views, `campaign_name`.
- What it shows: each category's line jumps inside its campaign window.

### Step 5. Sheet 2 — Baseline vs Campaign (Lift)

- Filter: `period_campaign_id` → exclude Null.
- **Rows:** `campaign_name` (sort descending by `View Lift %`).
- **Columns:** `Measure Values` with `Baseline Daily Views` and `Campaign Daily Views` → side-by-side bars (Color = Measure Names: grey = baseline, blue = campaign).
- **Label:** add `View Lift %` to Label on the campaign bar, or put it in a second column as text.
- What it shows: how many more daily views the target category got during each campaign.

### Step 6. Sheet 3 — Attributed Views by Channel

Build this sheet from the `campaign_impact_summary` fields (they're pre-aggregated per campaign):
- **Rows:** `channel` (from `campaign_impact_summary`).
- **Columns:** `SUM(attributed_views)`.
- **Color:** `ROAS (KPI)`, diverging palette centred at 1 (red = loses money, blue = earns).
- **Label:** `ROAS (KPI)`.
- Sort descending. The bar length shows reach and the colour shows return.

### Step 7. Sheet 4 — Top Products per Campaign

- Filter: `is_campaign_view` = 1.
- **Rows:** `campaign_name`, `product_name`.
- **Columns:** `Views`.
- Filter `product_name` → **Top 5 by Views**, computed *per pane*: use the calc `RANK(Views)` with Compute Using = `product_name`, then filter it to ≤ 5.
- **Color:** `Add to Cart Rate`.

### Step 8. Sheet 5 — Conversion Funnel

- Create the calc `Funnel Step Value`, or simply use `Measure Values` with: `Views`, `SUM(added_to_cart)`, `SUM(purchased)`.
- **Columns:** Measure Values; **Rows:** Measure Names; Mark = Bar; Label = values.
- Put `is_campaign_view` on **Color** (or Columns) to compare **campaign vs organic** traffic side by side.

### Step 9. Assemble the dashboard

1. New Dashboard → Size **Fixed 1200 × 900**.
2. Add a vertical container: title text → horizontal container of the 6 KPI sheets → Sheet 1 → 2×2 grid of Sheets 2–5.
3. Add filters: `campaign_name` (Single value dropdown + "All") and `category` (Multiple values dropdown). Apply both to **all sheets using this data source**.
4. Dashboard Actions → **Filter**: Source = Sheet 2, Target = Sheets 1, 4, 5, Run on Select. Clicking a campaign then filters the rest of the dashboard.
5. **File → Save As → Packaged Workbook (.twbx)** → `tableau/campaign_impact_dashboard.twbx`.

---

## Part B — How to Read It

| Question | Where to look | What good looks like |
|---|---|---|
| Did the campaign raise views? | Sheet 2, Lift % | Lift > 30% is a clear effect; < 20% is weak |
| Was the rise real, or just a busy week? | Sheet 1 | The targeted category jumps; the other categories stay flat |
| Which channel worked? | Sheet 3 | Long bar **and** ROAS ≥ 1 |
| Which products benefited? | Sheet 4 | A few hero products usually take most views. Check that the promoted SKUs are among them |
| Did views turn into sales? | Sheet 5 | Campaign cart rate ≥ organic cart rate |
| Was it worth the money? | KPI tiles | Lower cost per incremental view, ROAS > 1 |

### Current findings (synthetic data)

- **Spring Audio Launch** (Influencer) had the highest lift: **+174%**.
- **Laptop campaigns** had the best ROAS (~15–17x) because laptops are high-priced.
- **Gamer Week** (Display) lifted views by only **+16%**, with ROAS **0.15** — recommend cutting or reworking it.
- Campaign traffic adds to cart at ~11% vs ~8% for organic traffic.

### Caveats

- Lift is a pre/post comparison: it doesn't remove seasonality. Compare against untargeted categories on Sheet 1.
- Attributed views only count UTM-tagged clicks, and miss people who saw an ad and came back directly later.
- The data is synthetic. Replace the CSVs with real clickstream exports and re-run the script logic (`build_summary`) to refresh.
