"""
Generate synthetic data for the Marketing Campaign Impact on Product Views dashboard (Task 8).

Outputs (written to ../data/):
    products.csv               - product catalogue (id, name, brand, category, price)
    campaigns.csv              - marketing campaigns with channel, dates, target category, budget
    product_views.csv          - one row per product view event, with traffic source and campaign attribution
    campaign_impact_summary.csv - one row per campaign with baseline vs campaign views, lift % and cost metrics

The data is synthetic but follows the mankind_matrix_db conventions
(numeric ids, payment_status 'CONFIRMED' for successful orders).

Run:
    python generate_campaign_data.py
"""

from pathlib import Path

import numpy as np
import pandas as pd

SEED = 42
START_DATE = "2025-01-01"
END_DATE = "2025-06-30"
BASELINE_DAYS = 14          # days before each campaign used as the "normal" level
N_USERS = 5000
BASE_DAILY_VIEWS = 420      # organic views per day across all products

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
rng = np.random.default_rng(SEED)


# --------------------------------------------------------------------------- products
def build_products() -> pd.DataFrame:
    catalogue = {
        "Laptops":      (["Dell", "HP", "Lenovo", "Apple", "Asus"], (650, 2400)),
        "Smartphones":  (["Apple", "Samsung", "Google", "OnePlus"], (300, 1300)),
        "Audio":        (["Sony", "Bose", "JBL", "Sennheiser"], (40, 450)),
        "Wearables":    (["Apple", "Garmin", "Fitbit", "Samsung"], (80, 600)),
        "Gaming":       (["Sony", "Microsoft", "Nintendo", "Logitech"], (50, 650)),
        "Accessories":  (["Anker", "Logitech", "Belkin", "Samsung"], (15, 120)),
    }
    rows, pid = [], 1
    for category, (brands, (lo, hi)) in catalogue.items():
        for i in range(8):
            brand = brands[i % len(brands)]
            rows.append({
                "product_id": pid,
                "product_name": f"{brand} {category[:-1] if category.endswith('s') else category} {100 + i * 10}",
                "brand": brand,
                "category": category,
                "price": round(float(rng.uniform(lo, hi)), 2),
            })
            pid += 1
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- campaigns
def build_campaigns() -> pd.DataFrame:
    # Non-overlapping per target category so baseline vs campaign comparison is clean.
    # `true_lift` is the hidden effect used to generate data; it is NOT written to the CSV.
    spec = [
        (1, "New Year Laptop Sale",      "Email",      "Laptops",     "2025-01-20", "2025-02-02", 6000,  0.85),
        (2, "Valentine's Wearables",     "Social",     "Wearables",   "2025-02-05", "2025-02-14", 4500,  1.10),
        (3, "Spring Audio Launch",       "Influencer", "Audio",       "2025-03-10", "2025-03-23", 8000,  1.40),
        (4, "Smartphone Trade-In Push",  "Search",     "Smartphones", "2025-04-07", "2025-04-27", 12000, 0.60),
        (5, "Gamer Week",                "Display",    "Gaming",      "2025-05-12", "2025-05-18", 3500,  0.25),
        (6, "Summer Accessories Bundle", "Email",      "Accessories", "2025-06-02", "2025-06-15", 2500,  0.45),
        (7, "Mid-Year Laptop Clearance", "Social",     "Laptops",     "2025-06-16", "2025-06-29", 7000,  0.95),
    ]
    cols = ["campaign_id", "campaign_name", "channel", "target_category",
            "start_date", "end_date", "budget_usd", "true_lift"]
    df = pd.DataFrame(spec, columns=cols)
    df["start_date"] = pd.to_datetime(df["start_date"])
    df["end_date"] = pd.to_datetime(df["end_date"])
    df["duration_days"] = (df["end_date"] - df["start_date"]).dt.days + 1
    return df


# --------------------------------------------------------------------------- views
SOURCE_BY_CHANNEL = {"Email": "email", "Social": "social", "Influencer": "social",
                     "Search": "paid_search", "Display": "display"}
ORGANIC_SOURCES = ["organic_search", "direct", "referral", "social"]
ORGANIC_WEIGHTS = [0.45, 0.30, 0.15, 0.10]


def build_views(products: pd.DataFrame, campaigns: pd.DataFrame) -> pd.DataFrame:
    dates = pd.date_range(START_DATE, END_DATE, freq="D")
    categories = products["category"].unique()
    cat_share = dict(zip(categories, [0.22, 0.24, 0.16, 0.12, 0.14, 0.12]))
    prods_by_cat = {c: products.loc[products.category == c, "product_id"].to_numpy() for c in categories}
    # a few "hero" products get more traffic within their category
    prod_weights = {c: rng.dirichlet(np.full(len(p), 1.5)) for c, p in prods_by_cat.items()}

    frames = []
    for day in dates:
        weekday_factor = 1.15 if day.dayofweek >= 5 else 1.0
        trend_factor = 1 + 0.10 * (day - dates[0]).days / len(dates)   # slow organic growth
        for cat in categories:
            expected = BASE_DAILY_VIEWS * cat_share[cat] * weekday_factor * trend_factor
            n_org = rng.poisson(expected)

            # campaign-driven extra views (ramp up then fade within the window)
            active = campaigns[(campaigns.target_category == cat) &
                               (campaigns.start_date <= day) & (campaigns.end_date >= day)]
            n_camp, camp_row = 0, None
            if not active.empty:
                camp_row = active.iloc[0]
                pos = (day - camp_row.start_date).days / max(camp_row.duration_days - 1, 1)
                shape = 0.6 + 0.8 * np.sin(np.pi * pos)          # peaks mid-campaign
                n_camp = rng.poisson(expected * camp_row.true_lift * shape)

            for n, is_camp in ((n_org, False), (n_camp, True)):
                if n == 0:
                    continue
                pids = rng.choice(prods_by_cat[cat], size=n, p=prod_weights[cat])
                seconds = rng.integers(0, 86400, size=n)
                if is_camp:
                    src = np.full(n, SOURCE_BY_CHANNEL[camp_row.channel])
                    cid = np.full(n, camp_row.campaign_id, dtype=float)
                    cart_p, buy_p = 0.11, 0.35          # campaign traffic converts a bit better
                else:
                    src = rng.choice(ORGANIC_SOURCES, size=n, p=ORGANIC_WEIGHTS)
                    cid = np.full(n, np.nan)
                    cart_p, buy_p = 0.08, 0.30
                added = rng.random(n) < cart_p
                bought = added & (rng.random(n) < buy_p)
                frames.append(pd.DataFrame({
                    "view_ts": day + pd.to_timedelta(seconds, unit="s"),
                    "user_id": rng.integers(1, N_USERS + 1, size=n),
                    "product_id": pids,
                    "traffic_source": src,
                    "campaign_id": cid,
                    "added_to_cart": added.astype(int),
                    "purchased": bought.astype(int),
                }))

    views = pd.concat(frames, ignore_index=True).sort_values("view_ts").reset_index(drop=True)
    views.insert(0, "view_id", np.arange(1, len(views) + 1))
    views["view_date"] = views["view_ts"].dt.date
    views["campaign_id"] = views["campaign_id"].astype("Int64")
    views["is_campaign_view"] = views["campaign_id"].notna().astype(int)
    views = views.merge(products[["product_id", "category"]], on="product_id", how="left")

    # Tag each view with the campaign running (or about to run) on its category, so Tableau
    # can compare Baseline vs Campaign without a range join. Windows never overlap per category.
    views["period_campaign_id"] = pd.Series(pd.NA, index=views.index, dtype="Int64")
    views["campaign_period"] = "Other"
    vdate = pd.to_datetime(views["view_date"])
    for c in campaigns.itertuples():
        in_cat = views["category"] == c.target_category
        base = in_cat & (vdate >= c.start_date - pd.Timedelta(days=BASELINE_DAYS)) & (vdate < c.start_date)
        live = in_cat & (vdate >= c.start_date) & (vdate <= c.end_date)
        views.loc[base, ["period_campaign_id", "campaign_period"]] = [c.campaign_id, "Baseline"]
        views.loc[live, ["period_campaign_id", "campaign_period"]] = [c.campaign_id, "Campaign"]

    return views[["view_id", "view_ts", "view_date", "user_id", "product_id", "category",
                  "traffic_source", "campaign_id", "is_campaign_view", "added_to_cart", "purchased",
                  "period_campaign_id", "campaign_period"]]


# --------------------------------------------------------------------------- summary
def build_summary(views: pd.DataFrame, campaigns: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    v = views.copy()
    v["view_date"] = pd.to_datetime(v["view_date"])
    v = v.merge(products[["product_id", "price"]], on="product_id")
    rows = []
    for c in campaigns.itertuples():
        cat_views = v[v.category == c.target_category]
        base_start = c.start_date - pd.Timedelta(days=BASELINE_DAYS)
        base = cat_views[(cat_views.view_date >= base_start) & (cat_views.view_date < c.start_date)]
        during = cat_views[(cat_views.view_date >= c.start_date) & (cat_views.view_date <= c.end_date)]
        attributed = v[v.campaign_id == c.campaign_id]

        base_avg = len(base) / BASELINE_DAYS
        camp_avg = len(during) / c.duration_days
        incremental = (camp_avg - base_avg) * c.duration_days
        revenue = attributed.loc[attributed.purchased == 1, "price"].sum()
        rows.append({
            "campaign_id": c.campaign_id,
            "campaign_name": c.campaign_name,
            "channel": c.channel,
            "target_category": c.target_category,
            "start_date": c.start_date.date(),
            "end_date": c.end_date.date(),
            "duration_days": c.duration_days,
            "budget_usd": c.budget_usd,
            "baseline_avg_daily_views": round(base_avg, 1),
            "campaign_avg_daily_views": round(camp_avg, 1),
            "view_lift_pct": round((camp_avg / base_avg - 1) * 100, 1),
            "incremental_views": int(round(incremental)),
            "attributed_views": len(attributed),
            "attributed_add_to_carts": int(attributed.added_to_cart.sum()),
            "attributed_purchases": int(attributed.purchased.sum()),
            "attributed_revenue_usd": round(revenue, 2),
            "view_to_cart_rate_pct": round(attributed.added_to_cart.mean() * 100, 2),
            "cost_per_incremental_view_usd": round(c.budget_usd / incremental, 3) if incremental > 0 else None,
            "roas": round(revenue / c.budget_usd, 2),
        })
    return pd.DataFrame(rows)


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    products = build_products()
    campaigns = build_campaigns()
    views = build_views(products, campaigns)
    summary = build_summary(views, campaigns, products)

    products.to_csv(DATA_DIR / "products.csv", index=False)
    out_campaigns = campaigns.drop(columns=["true_lift"]).copy()
    out_campaigns["start_date"] = out_campaigns["start_date"].dt.date
    out_campaigns["end_date"] = out_campaigns["end_date"].dt.date
    out_campaigns.to_csv(DATA_DIR / "campaigns.csv", index=False)
    views.to_csv(DATA_DIR / "product_views.csv", index=False)
    summary.to_csv(DATA_DIR / "campaign_impact_summary.csv", index=False)

    # Flat file for Tableau: views + product + campaign (via period_campaign_id) in one table,
    # so the workbook needs no joins.
    enriched = (views
                .merge(products[["product_id", "product_name", "brand", "price"]], on="product_id", how="left")
                .merge(out_campaigns[["campaign_id", "campaign_name", "channel", "duration_days"]]
                       .rename(columns={"campaign_id": "period_campaign_id"}),
                       on="period_campaign_id", how="left"))
    enriched.to_csv(DATA_DIR / "views_for_tableau.csv", index=False)

    print(f"products: {len(products)}, campaigns: {len(campaigns)}, views: {len(views):,}")
    print(summary[["campaign_name", "baseline_avg_daily_views", "campaign_avg_daily_views",
                   "view_lift_pct", "incremental_views", "roas"]].to_string(index=False))


if __name__ == "__main__":
    main()
