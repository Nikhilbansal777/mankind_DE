"""
Build the Tableau packaged workbook (.twbx) for the Campaign Impact dashboard.

Writes:
    ../tableau/campaign_impact_dashboard.twb    (XML, points at ../data via a relative path)
    ../tableau/campaign_impact_dashboard.twbx   (packaged: .twb + CSVs, open this one)

The XML follows the format of Tableau 2026.1 (same as clv_dashboard/tableau/Book1.twb).
Run generate_campaign_data.py first so the CSVs exist.
"""

import uuid
import zipfile
from pathlib import Path
from xml.sax.saxutils import escape, quoteattr

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
OUT_DIR = ROOT / "tableau"
NAME = "campaign_impact_dashboard"

V = "federated.0campaignviews0000000000"   # views datasource
S = "federated.0campaignsummary00000000"   # campaign summary datasource


def q(s: str) -> str:
    return quoteattr(s)


def uid() -> str:
    return "{" + str(uuid.uuid4()).upper() + "}"


# --------------------------------------------------------------------------- datasources
VIEW_COLS = [  # (name, datatype, role, type, caption)
    ("view_id", "integer", "dimension", "ordinal", "View Id"),
    ("view_ts", "datetime", "dimension", "ordinal", "View Timestamp"),
    ("view_date", "date", "dimension", "ordinal", "View Date"),
    ("user_id", "integer", "dimension", "ordinal", "User Id"),
    ("product_id", "integer", "dimension", "ordinal", "Product Id"),
    ("category", "string", "dimension", "nominal", "Category"),
    ("traffic_source", "string", "dimension", "nominal", "Traffic Source"),
    ("campaign_id", "integer", "dimension", "ordinal", "Attributed Campaign Id"),
    ("is_campaign_view", "integer", "measure", "quantitative", "Is Campaign View"),
    ("added_to_cart", "integer", "measure", "quantitative", "Added To Cart"),
    ("purchased", "integer", "measure", "quantitative", "Purchased"),
    ("period_campaign_id", "integer", "dimension", "ordinal", "Period Campaign Id"),
    ("campaign_period", "string", "dimension", "nominal", "Campaign Period"),
    ("product_name", "string", "dimension", "nominal", "Product Name"),
    ("brand", "string", "dimension", "nominal", "Brand"),
    ("price", "real", "measure", "quantitative", "Price"),
    ("campaign_name", "string", "dimension", "nominal", "Campaign Name"),
    ("channel", "string", "dimension", "nominal", "Channel"),
    ("duration_days", "real", "measure", "quantitative", "Duration Days"),
]
SUMMARY_COLS = [
    ("campaign_id", "integer", "dimension", "ordinal", "Campaign Id"),
    ("campaign_name", "string", "dimension", "nominal", "Campaign Name"),
    ("channel", "string", "dimension", "nominal", "Channel"),
    ("target_category", "string", "dimension", "nominal", "Target Category"),
    ("start_date", "date", "dimension", "ordinal", "Start Date"),
    ("end_date", "date", "dimension", "ordinal", "End Date"),
    ("duration_days", "integer", "measure", "quantitative", "Duration Days"),
    ("budget_usd", "integer", "measure", "quantitative", "Budget (USD)"),
    ("baseline_avg_daily_views", "real", "measure", "quantitative", "Baseline Avg Daily Views"),
    ("campaign_avg_daily_views", "real", "measure", "quantitative", "Campaign Avg Daily Views"),
    ("view_lift_pct", "real", "measure", "quantitative", "View Lift Pct"),
    ("incremental_views", "integer", "measure", "quantitative", "Incremental Views"),
    ("attributed_views", "integer", "measure", "quantitative", "Attributed Views"),
    ("attributed_add_to_carts", "integer", "measure", "quantitative", "Attributed Add To Carts"),
    ("attributed_purchases", "integer", "measure", "quantitative", "Attributed Purchases"),
    ("attributed_revenue_usd", "real", "measure", "quantitative", "Attributed Revenue (USD)"),
    ("view_to_cart_rate_pct", "real", "measure", "quantitative", "View To Cart Rate Pct"),
    ("cost_per_incremental_view_usd", "real", "measure", "quantitative", "Cost Per Incremental View (USD)"),
    ("roas", "real", "measure", "quantitative", "ROAS"),
]

# calculated fields: key -> (internal name, caption, datatype, role, type, formula, default-format)
PCT, NUM, USD, X = "p0.0%", "n#,##0", 'c"$"#,##0.00', 'n#,##0.00"x"'
VIEW_CALCS = {
    "views":      ("Calculation_1000000000000000001", "Views", "integer", "measure", "quantitative",
                   "COUNT([view_id])", NUM),
    "share":      ("Calculation_1000000000000000002", "Campaign View Share", "real", "measure", "quantitative",
                   "SUM([is_campaign_view]) / COUNT([view_id])", PCT),
    "cart_rate":  ("Calculation_1000000000000000003", "Add to Cart Rate", "real", "measure", "quantitative",
                   "SUM([added_to_cart]) / COUNT([view_id])", "p0.00%"),
    "buy_rate":   ("Calculation_1000000000000000004", "Purchase Rate", "real", "measure", "quantitative",
                   "SUM([purchased]) / COUNT([view_id])", "p0.00%"),
    "traffic":    ("Calculation_1000000000000000005", "Traffic Type", "string", "dimension", "nominal",
                   'IF [is_campaign_view] = 1 THEN "Campaign" ELSE "Organic" END', None),
    "revenue":    ("Calculation_1000000000000000006", "Revenue", "real", "measure", "quantitative",
                   "SUM(IF [purchased] = 1 THEN [price] END)", 'c"$"#,##0'),
}
SUMMARY_CALCS = {
    "lift":       ("Calculation_2000000000000000001", "View Lift %", "real", "measure", "quantitative",
                   "SUM([campaign_avg_daily_views]) / SUM([baseline_avg_daily_views]) - 1", PCT),
    "avg_lift":   ("Calculation_2000000000000000002", "Avg Lift % (KPI)", "real", "measure", "quantitative",
                   "AVG([view_lift_pct]) / 100", PCT),
    "incr":       ("Calculation_2000000000000000003", "Total Incremental Views (KPI)", "integer", "measure",
                   "quantitative", "SUM([incremental_views])", NUM),
    "cpiv":       ("Calculation_2000000000000000004", "Cost per Incremental View (KPI)", "real", "measure",
                   "quantitative", "SUM([budget_usd]) / SUM([incremental_views])", USD),
    "roas":       ("Calculation_2000000000000000005", "ROAS (KPI)", "real", "measure", "quantitative",
                   "SUM([attributed_revenue_usd]) / SUM([budget_usd])", X),
}


def column_xml(name, dtype, role, typ, caption, formula=None, fmt=None) -> str:
    attrs = f"caption={q(caption)} datatype='{dtype}'"
    if fmt:
        attrs += f" default-format={q(fmt)}"
    attrs += f" name='[{name}]' role='{role}' type='{typ}'"
    if formula is None:
        return f"<column {attrs} />"
    return f"<column {attrs}><calculation class='tableau' formula={q(formula)} /></column>"


def all_columns(cols, calcs) -> str:
    out = [column_xml(*c[:4], c[4]) for c in cols]
    out += [column_xml(n, d, r, t, cap, f, fmt) for n, cap, d, r, t, f, fmt in calcs.values()]
    return "\n".join(out)


def datasource_xml(ds, caption, csv_name, cols, calcs, directory) -> str:
    table = csv_name.replace(".csv", "#csv")
    conn = "textscan." + ds.split(".")[1][:24]
    obj_id = csv_name + "_" + uuid.uuid5(uuid.NAMESPACE_URL, csv_name).hex.upper()
    physical = "\n".join(f"<column datatype='{d}' name='{n}' ordinal='{i}' />" for i, (n, d, *_) in enumerate(cols))
    relation = (f"<relation connection='{conn}' name='{csv_name}' table='[{table}]' type='table'>"
                f"<columns character-set='UTF-8' header='yes' locale='en_US' separator=','>{physical}</columns>"
                f"</relation>")
    return f"""
<datasource caption={q(caption)} inline='true' name='{ds}' version='18.1'>
  <connection class='federated'>
    <named-connections>
      <named-connection caption={q(csv_name.replace('.csv', ''))} name='{conn}'>
        <connection class='textscan' directory={q(directory)} filename='{csv_name}' password='' server='' />
      </named-connection>
    </named-connections>
    {relation}
  </connection>
  <aliases enabled='yes' />
  <column caption='{csv_name}' datatype='table' name='[__tableau_internal_object_id__].[{obj_id}]' role='measure' type='quantitative' />
  {all_columns(cols, calcs)}
  <layout dim-ordering='alphabetic' measure-ordering='alphabetic' show-structure='true' />
  <semantic-values><semantic-value key='[Country].[Name]' value='&quot;United States&quot;' /></semantic-values>
  <object-graph>
    <objects>
      <object caption='{csv_name}' id='{obj_id}'>
        <properties context=''>{relation}</properties>
      </object>
    </objects>
  </object-graph>
</datasource>"""


# --------------------------------------------------------------------------- field references
def ref(ds, instance):            # fully-qualified shelf reference
    return f"[{ds}].[{instance}]"


def dim(col):  return f"none:{col}:nk"
def ordd(col): return f"none:{col}:ok"
def agg(a, col): return f"{a}:{col}:qk"
def usr(calc_name): return f"usr:{calc_name}:qk"


def instance_xml(instance: str) -> str:
    deriv_map = {"none": "None", "sum": "Sum", "avg": "Avg", "usr": "User", "tdy": "Day-Trunc", "cnt": "Count"}
    pre, col, suffix = instance.split(":")
    typ = {"nk": "nominal", "ok": "ordinal", "qk": "quantitative"}[suffix]
    return f"<column-instance column='[{col}]' derivation='{deriv_map[pre]}' name='[{instance}]' pivot='key' type='{typ}' />"


# --------------------------------------------------------------------------- worksheets
def worksheet(name, ds, rows="", cols="", mark="Automatic", encodings=None, instances=(),
              filters="", slices=(), sorts="", labels=False, title=None) -> str:
    cols_xml = all_columns(VIEW_COLS, VIEW_CALCS) if ds == V else all_columns(SUMMARY_COLS, SUMMARY_CALCS)
    inst = "\n".join(instance_xml(i) for i in dict.fromkeys(instances))
    enc = ""
    if encodings:
        enc = "<encodings>" + "".join(f"<{k} column='{ref(ds, v)}' />" for k, v in encodings) + "</encodings>"
    style = ("<style><style-rule element='mark'><format attr='mark-labels-show' value='true' />"
             "<format attr='mark-labels-cull' value='true' /></style-rule></style>") if labels else "<style />"
    slices_xml = ("<slices>" + "".join(f"<column>{ref(ds, s)}</column>" for s in slices) + "</slices>") if slices else ""
    title_xml = ""
    if title:
        title_xml = (f"<layout-options><title><formatted-text><run bold='true' fontsize='11'>{escape(title)}</run>"
                     f"</formatted-text></title></layout-options>")
    caption = "Campaign Views" if ds == V else "Campaign Summary"
    return f"""
<worksheet name={q(name)}>
  {title_xml}
  <table>
    <view>
      <datasources><datasource caption='{caption}' name='{ds}' /></datasources>
      <datasource-dependencies datasource='{ds}'>
        {cols_xml}
        {inst}
      </datasource-dependencies>
      {filters}
      {sorts}
      {slices_xml}
      <aggregation value='true' />
    </view>
    {style}
    <panes>
      <pane selection-relaxation-option='selection-relaxation-allow'>
        <view><breakdown value='auto' /></view>
        <mark class='{mark}' />
        {enc}
      </pane>
    </panes>
    <rows>{rows}</rows>
    <cols>{cols}</cols>
  </table>
  <simple-id uuid='{uid()}' />
</worksheet>"""


def kpi(name, ds, calc_name, title):
    inst = usr(calc_name)
    return worksheet(name, ds, mark="Text", encodings=[("text", inst)], instances=[inst], title=title)


vc = {k: v[0] for k, v in VIEW_CALCS.items()}
sc = {k: v[0] for k, v in SUMMARY_CALCS.items()}

campaign_filter = (f"<filter class='categorical' column='{ref(V, dim(vc['traffic']))}' context='true'>"
                   f"<groupfilter function='member' level='[{dim(vc['traffic'])}]' member='&quot;Campaign&quot;' "
                   f"user:ui-domain='database' user:ui-enumeration='inclusive' user:ui-marker='enumerate' /></filter>")
category_filter = (f"<filter class='categorical' column='{ref(V, dim('category'))}'>"
                   f"<groupfilter function='level-members' level='[{dim('category')}]' "
                   f"user:ui-enumeration='all' user:ui-marker='enumerate' /></filter>")
top10_filter = (f"<filter class='categorical' column='{ref(V, dim('product_name'))}'>"
                f"<groupfilter count='10' end='top' function='end' units='records' user:ui-marker='end' "
                f"user:ui-top-by-field='true'><groupfilter direction='DESC' expression='COUNT([view_id])' "
                f"function='order' user:ui-marker='order'><groupfilter function='level-members' "
                f"level='[{dim('product_name')}]' /></groupfilter></groupfilter></filter>")


def sort(ds, dim_inst, measure_inst, shelf="rows", direction="DESC"):
    return (f"<shelf-sorts><shelf-sort-v2 dimension-to-sort='{ref(ds, dim_inst)}' direction='{direction}' "
            f"measure-to-sort-by='{ref(ds, measure_inst)}' shelf='{shelf}' /></shelf-sorts>")


SHEETS = [
    kpi("KPI Total Views", V, vc["views"], "Total Product Views"),
    kpi("KPI Campaign Share", V, vc["share"], "Views from Campaigns"),
    kpi("KPI Avg Lift", S, sc["avg_lift"], "Avg View Lift"),
    kpi("KPI Incremental Views", S, sc["incr"], "Incremental Views"),
    kpi("KPI Cost per Incremental View", S, sc["cpiv"], "Cost / Incremental View"),
    kpi("KPI ROAS", S, sc["roas"], "ROAS"),
    worksheet("Daily Views Trend", V,
              rows=ref(V, usr(vc["views"])), cols=ref(V, "tdy:view_date:qk"), mark="Area",
              encodings=[("color", dim(vc["traffic"]))],
              instances=[usr(vc["views"]), "tdy:view_date:qk", dim(vc["traffic"]), dim("category")],
              filters=category_filter, slices=[dim("category")],
              title="Daily Product Views — Campaign vs Organic Traffic"),
    worksheet("Baseline vs Campaign", S,
              rows=ref(S, dim("campaign_name")),
              cols=f"({ref(S, agg('sum', 'baseline_avg_daily_views'))} + {ref(S, agg('sum', 'campaign_avg_daily_views'))})",
              mark="Bar",
              encodings=[("text", agg("sum", "campaign_avg_daily_views"))],
              instances=[dim("campaign_name"), agg("sum", "baseline_avg_daily_views"),
                         agg("sum", "campaign_avg_daily_views"), usr(sc["lift"])],
              sorts=sort(S, dim("campaign_name"), usr(sc["lift"])), labels=True,
              title="Avg Daily Views: 14-Day Baseline vs During Campaign"),
    worksheet("View Lift by Campaign", S,
              rows=ref(S, dim("campaign_name")), cols=ref(S, usr(sc["lift"])), mark="Bar",
              encodings=[("color", dim("channel")), ("text", usr(sc["lift"]))],
              instances=[dim("campaign_name"), usr(sc["lift"]), dim("channel")],
              sorts=sort(S, dim("campaign_name"), usr(sc["lift"])), labels=True,
              title="View Lift % by Campaign (colour = channel)"),
    worksheet("Channel Performance", S,
              rows=ref(S, dim("channel")), cols=ref(S, agg("sum", "attributed_views")), mark="Bar",
              encodings=[("color", usr(sc["roas"])), ("text", usr(sc["roas"]))],
              instances=[dim("channel"), agg("sum", "attributed_views"), usr(sc["roas"])],
              sorts=sort(S, dim("channel"), agg("sum", "attributed_views")), labels=True,
              title="Attributed Views by Channel (colour/label = ROAS)"),
    worksheet("Top Campaign Products", V,
              rows=ref(V, dim("product_name")), cols=ref(V, usr(vc["views"])), mark="Bar",
              encodings=[("color", dim("campaign_name"))],
              instances=[dim("product_name"), usr(vc["views"]), dim("campaign_name"), dim(vc["traffic"])],
              filters=campaign_filter + top10_filter, slices=[dim(vc["traffic"]), dim("product_name")],
              sorts=sort(V, dim("product_name"), usr(vc["views"])),
              title="Top 10 Products by Campaign-Driven Views"),
    worksheet("Conversion Campaign vs Organic", V,
              rows=ref(V, dim(vc["traffic"])),
              cols=f"({ref(V, usr(vc['cart_rate']))} + {ref(V, usr(vc['buy_rate']))})",
              mark="Bar",
              encodings=[("color", dim(vc["traffic"]))],
              instances=[dim(vc["traffic"]), usr(vc["cart_rate"]), usr(vc["buy_rate"])],
              labels=True,
              title="Add-to-Cart & Purchase Rate: Campaign vs Organic"),
]
SHEET_NAMES = ["KPI Total Views", "KPI Campaign Share", "KPI Avg Lift", "KPI Incremental Views",
               "KPI Cost per Incremental View", "KPI ROAS", "Daily Views Trend", "Baseline vs Campaign",
               "View Lift by Campaign", "Channel Performance", "Top Campaign Products",
               "Conversion Campaign vs Organic"]


# --------------------------------------------------------------------------- dashboard
_zid = iter(range(1, 1000))


def zone(name, x, y, w, h, extra=""):
    return (f"<zone h='{h}' id='{next(_zid)}' name={q(name)} w='{w}' x='{x}' y='{y}' {extra}>"
            f"<zone-style><format attr='border-style' value='none' /><format attr='margin' value='4' /></zone-style></zone>")


def row(names, x0, y, w_total, h):
    w = w_total // len(names)
    inner = "".join(zone(n, x0 + i * w, y, w, h) for i, n in enumerate(names))
    return f"<zone h='{h}' id='{next(_zid)}' param='horz' type-v2='layout-flow' w='{w_total}' x='{x0}' y='{y}'>{inner}</zone>"


def dashboard_xml() -> str:
    x0, W = 800, 98400
    y_title, h_title = 1000, 6000
    y_kpi, h_kpi = 7000, 11000
    y_trend, h_trend = 18000, 27000
    y_r2, h_r2 = 45000, 27000
    y_r3, h_r3 = 72000, 27000
    title_w = 78000
    title = (f"<zone h='{h_title}' id='{next(_zid)}' type-v2='text' w='{title_w}' x='{x0}' y='{y_title}'>"
             "<formatted-text>"
             "<run bold='true' fontsize='18'>Marketing Campaign Impact on Product Views</run>"
             "<run fontsize='10'>&#10;Jan–Jun 2025 · lift = avg daily views of the target category during the campaign vs the 14 days before</run>"
             "</formatted-text></zone>")
    cat_filter = (f"<zone h='{h_title}' id='{next(_zid)}' mode='checkdropdown' name='Daily Views Trend' "
                  f"param='{ref(V, dim('category'))}' type-v2='filter' values='database' "
                  f"w='{W - title_w}' x='{x0 + title_w}' y='{y_title}' />")
    header = (f"<zone h='{h_title}' id='{next(_zid)}' param='horz' type-v2='layout-flow' w='{W}' x='{x0}' y='{y_title}'>"
              f"{title}{cat_filter}</zone>")
    body = (header
            + row(SHEET_NAMES[:6], x0, y_kpi, W, h_kpi)
            + zone("Daily Views Trend", x0, y_trend, W, h_trend)
            + row(["Baseline vs Campaign", "View Lift by Campaign"], x0, y_r2, W, h_r2)
            + row(["Channel Performance", "Top Campaign Products", "Conversion Campaign vs Organic"], x0, y_r3, W, h_r3))
    return f"""
<dashboards>
  <dashboard enable-sort-zone-taborder='true' name='Campaign Impact'>
    <style />
    <size maxheight='1000' maxwidth='1300' minheight='1000' minwidth='1300' />
    <datasources>
      <datasource caption='Campaign Views' name='{V}' />
      <datasource caption='Campaign Summary' name='{S}' />
    </datasources>
    <datasource-dependencies datasource='{V}'>
      <column caption='Category' datatype='string' name='[category]' role='dimension' type='nominal' />
      {instance_xml(dim('category'))}
    </datasource-dependencies>
    <zones>
      <zone h='100000' id='{next(_zid)}' type-v2='layout-basic' w='100000' x='0' y='0'>
        <zone h='98000' id='{next(_zid)}' param='vert' type-v2='layout-flow' w='{W}' x='{x0}' y='{y_title}'>
          {body}
        </zone>
      </zone>
    </zones>
    <simple-id uuid='{uid()}' />
  </dashboard>
</dashboards>"""


CARDS = ("<cards>"
         "<edge name='left'><strip size='160'><card type='pages' /><card type='filters' /><card type='marks' /></strip></edge>"
         "<edge name='top'><strip size='2147483647'><card type='columns' /></strip>"
         "<strip size='2147483647'><card type='rows' /></strip>"
         "<strip size='31'><card type='title' /></strip></edge>"
         "</cards>")


def windows_xml() -> str:
    # Worksheet windows must be (cards, viewpoint?, simple-id); dashboard: (viewpoints, active, simple-id)
    ws = "".join(f"<window class='worksheet' hidden='true' name={q(n)}>{CARDS}<simple-id uuid='{uid()}' /></window>"
                 for n in SHEET_NAMES)
    vps = "".join(f"<viewpoint name={q(n)}><zoom type='entire-view' /></viewpoint>" for n in SHEET_NAMES)
    return (f"<windows source-height='51'>{ws}"
            f"<window class='dashboard' maximized='true' name='Campaign Impact'>"
            f"<viewpoints>{vps}</viewpoints><active id='-1' /><simple-id uuid='{uid()}' /></window></windows>")


# --------------------------------------------------------------------------- assemble
def workbook_xml(data_dir: str) -> str:
    return f"""<?xml version='1.0' encoding='utf-8' ?>

<!-- build 20261.26.0410.0924                               -->
<workbook original-version='18.1' source-build='2026.1.1 (20261.26.0410.0924)' source-platform='mac' version='18.1' xmlns:user='http://www.tableausoftware.com/xml/user'>
  <document-format-change-manifest>
    <AccessibleZoneTabOrder />
    <AnimationOnByDefault />
    <IntuitiveSorting />
    <IntuitiveSorting_SP2 />
    <MarkAnimation />
    <ObjectModelEncapsulateLegacy />
    <ObjectModelTableType />
    <SchemaViewerObjectModel />
    <SheetIdentifierTracking />
    <WindowsPersistSimpleIdentifiers />
    <ZoneBackgroundTransparency />
  </document-format-change-manifest>
  <preferences>
    <preference name='ui.encoding.shelf.height' value='24' />
    <preference name='ui.shelf.height' value='26' />
  </preferences>
  <datasources>
    {datasource_xml(V, 'Campaign Views', 'views_for_tableau.csv', VIEW_COLS, VIEW_CALCS, data_dir)}
    {datasource_xml(S, 'Campaign Summary', 'campaign_impact_summary.csv', SUMMARY_COLS, SUMMARY_CALCS, data_dir)}
  </datasources>
  <worksheets>
    {''.join(SHEETS)}
  </worksheets>
  {dashboard_xml()}
  {windows_xml()}
</workbook>
"""


def main():
    OUT_DIR.mkdir(exist_ok=True)
    # .twb next to the repo's data folder (relative path)
    (OUT_DIR / f"{NAME}.twb").write_text(workbook_xml("../data"), encoding="utf-8")
    # .twbx: workbook + data packaged together (Tableau resolves Data/... inside the package)
    twbx = OUT_DIR / f"{NAME}.twbx"
    with zipfile.ZipFile(twbx, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(f"{NAME}.twb", workbook_xml("Data/data"))
        for f in ("views_for_tableau.csv", "campaign_impact_summary.csv"):
            z.write(DATA_DIR / f, f"Data/data/{f}")
    print("wrote", OUT_DIR / f"{NAME}.twb", "and", twbx)


if __name__ == "__main__":
    main()
