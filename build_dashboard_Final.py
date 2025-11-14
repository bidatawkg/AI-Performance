#!/usr/bin/env python3
# Dashboard builder - Violet classic layout + current working logic (v3)
# Changes vs v2:
# - Dark dropdown menus for Period & Market (visible options)
# - Custom chevron arrow
# - Logo icon updated to colored-bars-in-card style
# Output: Dashboard_final.html

import os, re, json, glob
import pandas as pd

def safe_df(df):
    return df if isinstance(df, pd.DataFrame) else pd.DataFrame()

BASE_DIR = os.path.dirname(__file__) if '__file__' in globals() else "."
FILES_DIR = os.path.join(BASE_DIR, "aiPerformanceFiles")
OUTPUT = os.path.join(BASE_DIR, "Dashboard_final.html")

INSIGHTS_DIR = os.path.join(BASE_DIR, "aiPerformanceText")
INSIGHTS_GLOB_EXEC = os.path.join(INSIGHTS_DIR, '*_main-executive.js')
INSIGHTS_GLOB_INSIGHTS = os.path.join(INSIGHTS_DIR, '*_main-insights.js')
os.makedirs(INSIGHTS_DIR, exist_ok=True)

def _latest_insights_js():
    """Return (basename, fullpath, content) of the most recent *_main-insights.js in aiPerformanceText/ or (None, None, None)."""
    import re, glob, os
    files = glob.glob(INSIGHTS_GLOB_INSIGHTS)
    if not files:
        return (None, None, None)
    def key(p):
        b = os.path.basename(p)
        m = re.match(r"(\d{4}-\d{2}-\d{2})_main-insights\.js$", b, re.I)
        d = m.group(1) if m else "0000-00-00"
        return (d, os.path.getmtime(p))
    files.sort(key=key)
    latest = files[-1]
    try:
        with open(latest, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()
    except Exception:
        content = None
    return (os.path.basename(latest), latest, content)



def _latest_exec_js():
    """Return (basename, fullpath, content) of the most recent *_main-executive.js in aiPerformanceText/ or (None, None, None)"""
    import re, glob, os
    try:
        files = glob.glob(os.path.join(INSIGHTS_DIR, "*_main-executive.js"))
    except Exception:
        files = []
    if not files:
        return (None, None, None)
    def key(p):
        b = os.path.basename(p)
        m = re.match(r"(\d{4}-\d{2}-\d{2})_main-executive\.js$", b, re.I)
        d = m.group(1) if m else "0000-00-00"
        return (d, os.path.getmtime(p))
    files.sort(key=key)
    latest = files[-1]
    try:
        with open(latest, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()
    except Exception:
        content = None
    return (os.path.basename(latest), latest, content)

def _latest_vvip_js():
    """Return (basename, fullpath, content) of the most recent *_VVIP-insights.js in aiPerformanceText/ or (None, None, None)."""
    import re, glob, os
    files = glob.glob(os.path.join(INSIGHTS_DIR, "*_VVIP-insights.js"))
    if not files:
        return (None, None, None)
    def key(p):
        b = os.path.basename(p)
        m = re.match(r"(\d{4}-\d{2}-\d{2})_VVIP-insights\.js$", b, re.I)
        d = m.group(1) if m else "0000-00-00"
        return (d, os.path.getmtime(p))
    files.sort(key=key)
    latest = files[-1]
    try:
        with open(latest, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()
    except Exception:
        content = None
    return (os.path.basename(latest), latest, content)
    if not files:
        return (None, None, None)
    def key(p):
        b = os.path.basename(p)
        m = re.match(r"(\d{4}-\d{2}-\d{2})_main-executive\.js$", b, re.I)
        d = m.group(1) if m else "0000-00-00"
        return (d, os.path.getmtime(p))
    files.sort(key=key)
    latest = files[-1]
    try:
        with open(latest, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()
    except Exception:
        content = None
    return (os.path.basename(latest), latest, content)
os.makedirs(FILES_DIR, exist_ok=True)

VALID_MARKETS = ["ALL","GCC","BET","AE","QA","SA","JO","BH","NZ","KW","EG","Others"]

def read_csv_safe(path):
    try:
        return pd.read_csv(path)
    except Exception:
        try:
            return pd.read_csv(path, encoding="latin-1")
        except Exception:
            return pd.DataFrame()

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    rename_map = {}
    for c in df.columns:
        lc = str(c).lower().strip()
        if lc in ["metric","kpi"]:
            rename_map[c] = "METRIC"
        elif lc in ["country","market"]:
            rename_map[c] = "Country"
        elif lc == "period":
            rename_map[c] = "Period"
        elif lc in ["value","amount","current_value","current"]:
            rename_map[c] = "VALUE"
        elif lc == "target":
            rename_map[c] = "TARGET"
        elif lc in ["diff_percentage","diff %","diffpct","diffpercent","diff_percentage_%"]:
            rename_map[c] = "DIFF_PERCENTAGE"
        elif lc == "date":
            rename_map[c] = "Date"
        elif lc in ["diff_abs","diff_absolute","difference_absolute","delta"]:
            rename_map[c] = "DIFF_ABSOLUTE"
    out = df.rename(columns=rename_map)
    out.columns = [str(c).strip() for c in out.columns]
    return out


def normalize_period(s):
    """
    Normalize any period label (old or new) to one of:
    "Yesterday", "7 days", "30 days", "MTD".
    """
    if s is None:
        return "Yesterday"
    t = str(s).strip().lower().replace('_','').replace(' ', '')
    mapping = {
        'daily': 'Yesterday',
        'yesterday': 'Yesterday',
        'weekly': '7 days',
        '7days': '7 days',
        '7day': '7 days',
        'sevenDays'.lower(): '7 days',
        '30days': '30 days',
        '30day': '30 days',
        'monthly': '30 days',
        'mtd': 'MTD'
    }
    return mapping.get(t, str(s))


def extract_date_from_filename(p: str):
    m = re.search(r"(\d{4}-\d{2}-\d{2})_", os.path.basename(p))
    return m.group(1) if m else None

def extract_country_from_filename(p: str):
    m = re.search(r"_(ALL|AE|QA|SA|JO|BH|NZ|KW|EG)\.csv$", os.path.basename(p))
    return m.group(1) if m else None

def extract_period_from_filename(p: str):
    # Accept old and new tokens inside filename and normalize
    m = re.search(r"_ai-main-(yesterday|7[_\s-]?days|30[_\s-]?days|mtd|daily|weekly|monthly)_", os.path.basename(p), re.I)
    token = m.group(1) if m else None
    return normalize_period(token) if token else "Yesterday"

def extract_metric_from_filename(p: str):
    m = re.search(
        r"_ai-main-(?:yesterday|7[_\s-]?days|30[_\s-]?days|mtd|daily|weekly|monthly)_([A-Za-z0-9]+)_(ALL|AE|QA|SA|JO|BH|NZ|KW|EG)\.csv$",
        os.path.basename(p), re.I)
    return m.group(1) if m else "UNKNOWN"

def load_main_data() -> pd.DataFrame:
    dfs = []
    for f in glob.glob(os.path.join(FILES_DIR, "*_ai-main-*_*.csv")):
        df = read_csv_safe(f)
        if df.empty:
            continue
        df = normalize_columns(df)
        if "Date" not in df.columns or df["Date"].isna().all():
            df["Date"] = extract_date_from_filename(f)
        if "Period" not in df.columns or df["Period"].isna().all():
            df["Period"] = extract_period_from_filename(f)
        if "Country" not in df.columns or df["Country"].isna().all():
            df["Country"] = extract_country_from_filename(f) or "ALL"
        if "METRIC" not in df.columns or df["METRIC"].isna().all():
            df["METRIC"] = extract_metric_from_filename(f)
        dfs.append(df)
    if not dfs:
        return pd.DataFrame(columns=["Date","Period","Country","METRIC","VALUE"])
    out = pd.concat(dfs, ignore_index=True)
    for col in ["Date","Period","Country","METRIC"]:
        if col in out.columns:
            out[col] = out[col].astype(str)
    return out

def load_summary_data() -> pd.DataFrame:
    dfs = []
    for f in glob.glob(os.path.join(FILES_DIR, "*_ai-summary-targets_*.csv")):
        df = read_csv_safe(f)
        if df.empty:
            continue
        df = normalize_columns(df)
        if "Period" not in df.columns or df["Period"].isna().all():
            df["Period"] = "Yesterday"
        if "Country" not in df.columns or df["Country"].isna().all():
            df["Country"] = extract_country_from_filename(f) or "ALL"
        if "METRIC" not in df.columns or df["METRIC"].isna().all():
            df["METRIC"] = "UNKNOWN"
        dfs.append(df)
    if not dfs:
        return pd.DataFrame(columns=["Period","Country","METRIC","VALUE","TARGET","DIFF_PERCENTAGE","DIFF_ABSOLUTE","Date"])
    out = pd.concat(dfs, ignore_index=True)
    for col in ["Date","Period","Country","METRIC"]:
        if col in out.columns:
            out[col] = out[col].astype(str)
    return out

def load_diff_data() -> pd.DataFrame:
    """
    Load files like: YYYY-MM-DD_ai-summary-differences_<MARKET>.csv
    Expected columns: Country/Market, Period, METRIC, VALUE, Previous, DIFF_ABSOLUTE, DIFF_PERCENTAGE
    """
    import glob, os
    dfs = []
    for f in glob.glob(os.path.join(FILES_DIR, "*_ai-summary-differences_*.csv")):
        df = read_csv_safe(f)
        if df is None or df.empty:
            continue
        # --- normalize columns to your canonical names ---
        df = df.rename(columns={
            'Market': 'Country', 'market': 'Country', 'MARKET': 'Country',
            'period': 'Period', 'PERIOD': 'Period',
            'metric': 'METRIC', 'Metric': 'METRIC',
            'value': 'VALUE', 'Value': 'VALUE',
            'previous': 'Previous', 'PREVIOUS': 'Previous',
            'diff_percentage': 'DIFF_PERCENTAGE', 'DIFF_%': 'DIFF_PERCENTAGE',
            'diff_absolute': 'DIFF_ABSOLUTE'
        })
        for col in ['Country','Period','METRIC','VALUE','Previous','DIFF_PERCENTAGE','DIFF_ABSOLUTE']:
            if col not in df.columns:
                df[col] = None
        df['Country'] = df['Country'].astype(str)
        df['Period']  = df['Period'].astype(str)
        df['METRIC']  = df['METRIC'].astype(str)
        dfs.append(df)
    if not dfs:
        return pd.DataFrame(columns=['Country','Period','METRIC','VALUE','Previous','DIFF_PERCENTAGE','DIFF_ABSOLUTE'])
    return pd.concat(dfs, ignore_index=True)
# ===== ai-groups helpers (for Breakdown by Segment) =====
def _iter_group_files():
    patterns = [os.path.join(FILES_DIR, "*_ai-groups-*_*.csv"),
                os.path.join(BASE_DIR, "*_ai-groups-*_*.csv")]
    seen = set()
    for pat in patterns:
        for f in glob.glob(pat):
            if f not in seen:
                seen.add(f); yield f

def extract_period_from_filename_groups(p: str):
    m = re.search(r"_ai-groups-(yesterday|7[_\s-]?days|30[_\s-]?days|mtd|daily|weekly|monthly)_", os.path.basename(p), re.I)
    token = m.group(1) if m else None
    return normalize_period(token) if token else "Yesterday"

def extract_metric_from_filename_groups(p: str):
    m = re.search(
        r"_ai-groups-(?:yesterday|7[_\s-]?days|30[_\s-]?days|mtd|daily|weekly|monthly)_([A-Za-z0-9]+)_(ALL|AE|QA|SA|JO|BH|NZ|KW|EG)\.csv$",
        os.path.basename(p), re.I)
    return m.group(1) if m else "UNKNOWN"

_SEGMENT_LABELS = {
    "FTDS":"New players", "FTD":"New players", "NEWPLAYERS":"New players", "FTDS]":"New players", "[FTDS":"New players",
    "1-3":"FTD 1-3 Months", "1–3":"FTD 1-3 Months", "[1-3]":"FTD 1-3 Months", "[1–3]":"FTD 1-3 Months",
    "4-12":"FTD 4-12 Months", "4–12":"FTD 4-12 Months", "[4-12]":"FTD 4-12 Months", "[4–12]":"FTD 4-12 Months",
    "13-24":"FTD 1-2 Years", "13–24":"FTD 1-2 Years", "[13-24]":"FTD 1-2 Years", "[13–24]":"FTD 1-2 Years",
    ">25":"FTD 2+ Years", "> 25":"FTD 2+ Years", "[>25]":"FTD 2+ Years", "[> 25]":"FTD 2+ Years", "25+":"FTD 2+ Years"
}
def _map_segment_label(x: str) -> str:
    if x is None: return "Unassigned"
    t = str(x).strip().strip("[]").replace("–","-")
    t_compact = t.replace(" ", "").upper()
    return _SEGMENT_LABELS.get(t_compact, t)

def load_group_data() -> pd.DataFrame:
    dfs = []
    for f in _iter_group_files():
        df = read_csv_safe(f)
        if df is None or df.empty:
            continue
        df = normalize_columns(df)
        if "Date" not in df.columns or df["Date"].isna().all():
            df["Date"] = extract_date_from_filename(f)
        if "Period" not in df.columns or df["Period"].isna().all():
            df["Period"] = extract_period_from_filename_groups(f)
        if "Country" not in df.columns or df["Country"].isna().all():
            df["Country"] = extract_country_from_filename(f) or "ALL"
        if "METRIC" not in df.columns or df["METRIC"].isna().all():
            df["METRIC"] = extract_metric_from_filename_groups(f)
        if "FTD_Group" not in df.columns:
            df["FTD_Group"] = "Unassigned"
        df["FTD_Group"] = df["FTD_Group"].apply(_map_segment_label)
        dfs.append(df)
    if not dfs:
        return pd.DataFrame(columns=["Date","Period","Country","METRIC","FTD_Group","VALUE"])
    out = pd.concat(dfs, ignore_index=True)
    for col in ["Date","Period","Country","METRIC","FTD_Group"]:
        if col in out.columns:
            out[col] = out[col].astype(str)
    return out

def load_vvip_table_data() -> pd.DataFrame:
    dfs = []
    patterns = [
        os.path.join(FILES_DIR, "*_ai-VVIPs-table_*.csv"),
        os.path.join(BASE_DIR, "*_ai-VVIPs-table_*.csv"),
    ]
    seen=set()
    for pat in patterns:
        for f in glob.glob(pat):
            if f in seen: continue
            seen.add(f)
            df = read_csv_safe(f)
            if df is None or df.empty: continue
            cols = {c: str(c).strip() for c in df.columns}
            df.rename(columns=cols, inplace=True)
            if "Market" not in df.columns:
                m = re.search(r"_ai-VVIPs-table_([A-Za-z]+)\.csv$", os.path.basename(f))
                df["Market"] = (m.group(1) if m else "ALL")
            df["Market"] = df["Market"].astype(str).str.upper()
            if "Period" in df.columns:
                df["Period"] = df["Period"].apply(normalize_period)
            else:
                df["Period"] = "Yesterday"
            if "PARTYID" in df.columns:
                df["PARTYID"] = df["PARTYID"].astype(str)
            dfs.append(df)
    if not dfs: return pd.DataFrame()
    out = pd.concat(dfs, ignore_index=True)
    for c in ["Market","Period"]:
        if c in out.columns:
            out[c] = out[c].astype(str)
    if "PARTYID" in out.columns:
        out["PARTYID"] = out["PARTYID"].astype(str)
    return out


def build_html(main_df: pd.DataFrame, summary_df: pd.DataFrame, diff_df: pd.DataFrame, group_df: pd.DataFrame, vvip_df: pd.DataFrame) -> str:
    period_opts = ["Yesterday","7 days","30 days","MTD"]
    market_opts = VALID_MARKETS
    metrics = sorted(main_df["METRIC"].dropna().astype(str).unique()) if not main_df.empty else ["NoData"]

    main_json = json.dumps(main_df.to_dict(orient="records"))
    summary_json = json.dumps(summary_df.to_dict(orient="records"))
    diff_json = json.dumps(diff_df.to_dict(orient="records"))
    metrics_json = json.dumps(metrics)
    group_json = json.dumps(group_df.to_dict(orient="records"))
    group_metrics = sorted(group_df['METRIC'].dropna().astype(str).unique()) if not group_df.empty else metrics
    group_metrics_json = json.dumps(group_metrics)
    vvip_json = json.dumps(vvip_df.to_dict(orient='records'))
    # --- VVIP Churn CSV -> JSON ---
    base_dir = os.path.dirname(os.path.abspath(__file__))
    churn_csv_path = os.path.join(base_dir, "aiPerformanceFiles", "2025-11-07_ai-VVIPs-Churn.csv")
    try:
        if os.path.exists(churn_csv_path):
            churn_df = pd.read_csv(churn_csv_path)
        elif os.path.exists(os.path.join(base_dir, "2025-11-07_ai-VVIPs-Churn.csv")):
            churn_df = pd.read_csv(os.path.join(base_dir, "2025-11-07_ai-VVIPs-Churn.csv"))
        elif os.path.exists("2025-11-07_ai-VVIPs-Churn.csv"):
            churn_df = pd.read_csv("2025-11-07_ai-VVIPs-Churn.csv")
        else:
            churn_df = pd.DataFrame([])
    except Exception:
        churn_df = pd.DataFrame([])
    churn_json = json.dumps(churn_df.to_dict(orient="records"))


    html = r'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>Performance Monitor</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
__INSIGHTS_TAG__
__EXEC_TAG__
__VVIP_TAG__
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800&display=swap" rel="stylesheet">
<style>
:root{
  --violet-1:#7A65F0;
  --violet-2:#9E7BFF;
  --violet-3:#B28DFF;
  --glass: rgba(255,255,255,0.06);
  --glass-2: rgba(255,255,255,0.12);
  --border: rgba(255,255,255,0.22);
  --soft-text:#EDE9FF;
  --muted:#D9D3FF;
  --dropdown-bg:#2B2B46;
  --dropdown-hover:#3A3A57;
}
*{box-sizing:border-box}
body{
  font-family:Inter,system-ui,Segoe UI,Roboto,Helvetica,Arial,sans-serif;
  margin:0;
  color:#fff;
  color-scheme: dark;
  background: radial-gradient(1200px 600px at 10% 0%, rgba(255,255,255,0.06), transparent 60%),
              linear-gradient(160deg, #6E6BEF 0%, #8E6EF2 40%, #9D79F7 70%, #6AB7F9 100%);
}
.container{max-width:1600px;margin:0 auto;padding:28px;}
.header-wrap{
  padding:22px 24px;
  background:linear-gradient(135deg, rgba(255,255,255,0.12), rgba(255,255,255,0.02));
  border:1px solid var(--border);
  border-radius:18px;
  box-shadow:0 12px 30px rgba(0,0,0,0.18);
  display:flex;align-items:center;justify-content:space-between;gap:16px;
}
.brand{display:flex;align-items:center;gap:14px;}
.logo{
  width:40px;height:40px;border-radius:12px;
  background:linear-gradient(135deg,#ff7a6f 0%, #7A65F0 52%, #66E3A1 100%);
  display:grid;place-items:center;box-shadow:0 6px 16px rgba(0,0,0,.25);
}
.logo svg{width:24px;height:24px;display:block;opacity:.98}
h1{margin:0;font-weight:800;letter-spacing:.4px;color:var(--soft-text);}
.filters{display:flex;gap:10px;align-items:center;}
.filters label{font-size:14px;color:#f5f2ff}
select{
  appearance:none;
  background:var(--glass);
  border:1px solid var(--border);
  color:#fff;border-radius:10px;padding:8px 34px 8px 12px;
  font-size:14px; backdrop-filter: blur(6px);
  /* custom chevron */
  background-image: url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="6 9 12 15 18 9"/></svg>');
  background-repeat:no-repeat; background-position:right 10px center;
}
select:focus{outline:2px solid rgba(255,255,255,0.55);box-shadow:0 0 0 3px rgba(255,255,255,0.12) inset}
/* style dropdown options where supported */
option{background-color:var(--dropdown-bg); color:#fff;}
/* firefox */
select:-moz-focusring { color: transparent; text-shadow: 0 0 0 #fff; }
@supports selector(option:checked){
  option:hover, option:checked{ background-color: var(--dropdown-hover); }
}
/* Edge/IE arrow */
select::-ms-expand{ display:none; }

.panel{max-width:1600px;margin-left:auto;margin-right:auto;
  margin-top:18px;padding:18px;border-radius:18px;
  background:linear-gradient(180deg, rgba(255,255,255,0.10), rgba(255,255,255,0.03));
  border:1px solid var(--border);
  box-shadow:0 10px 28px rgba(0,0,0,0.18);
  width: 100%;
  box-sizing: border-box;
}

.panel h2{margin:0 0 12px 0;color:#fff;font-weight:800;letter-spacing:.3px;}
.insights-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;}
.insight-box{
  padding:12px;border-radius:14px;background:var(--glass);
  border:1px dashed rgba(255,255,255,0.25);color:var(--muted);min-height:64px; display:flex;align-items:center;
}
.cards-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;}
.metric-card{
  background:linear-gradient(180deg, rgba(255,255,255,0.08), rgba(255,255,255,0.02));
  border:1px solid var(--border); border-radius:16px; padding:16px;
  position:relative; overflow:hidden;
}
.metric-card:before{
  content:""; position:absolute; inset:0; border-radius:16px;
  border-top:3px solid rgba(102, 227, 161, 0.6); pointer-events:none;
}
.metric-label{text-transform:uppercase;font-size:12px;font-weight:800;opacity:.95;letter-spacing:.6px;}
.metric-value{font-size:28px;font-weight:900;margin-top:6px;color:#fff;}
.metric-subtitle{font-size:12px;color:#F0ECFF;margin-top:2px;opacity:.9}
.metric-target{font-size:13px;color:#E7E1FF;margin-top:6px;opacity:.9}
.metric-footer{display:flex;justify-content:space-between;gap:8px;margin-top:8px;font-size:13px;color:#E7E1FF;opacity:.95}
.metric-change{display:inline-block;margin-top:8px;padding:4px 8px;border-radius:999px;font-size:12px;font-weight:800;
  background:rgba(255,255,255,.08);border:1px solid rgba(255,255,255,.2)}
.metric-changes{display:flex;justify-content:space-between;gap:8px;flex-wrap:wrap;margin-top:6px;}
.positive{color:#34D399;} .negative{color:#DC2626;}
.plot{height:380px;margin-top:8px;}
.vsplit{display:grid;grid-template-columns:2fr 1fr;gap:18px;}
.gauge{height:280px;}
/* Pills */
.pillbar{display:flex;flex-wrap:wrap;gap:10px;background:var(--glass);border:1px solid var(--border);border-radius:14px;padding:6px;width:fit-content}
.pill{padding:6px 12px;border-radius:10px;background:transparent;color:#F7F4FF;font-weight:800;font-size:13px;border:1px solid transparent;cursor:pointer;opacity:.92;user-select:none}
.pill:hover{background:rgba(255,255,255,0.12)}
.pill.active{background:rgba(255,255,255,0.18);border-color:rgba(255,255,255,0.35)}
.gauges-row{
  display:flex;
  gap:24px;
  justify-content:center;
  align-items:flex-start;
  flex-wrap:wrap;
}
.gauges-row .gauge{
  flex:1 1 520px;
  max-width:700px;
  margin:0 auto;
}
@media (max-width:1100px){
  .gauges-row .gauge{ flex-basis:100%; }
}
/* Plotly fonts override */
.js-plotly-plot .plotly .main-svg{font-family:Inter, sans-serif;}

/* --- VVIP Table Styling --- */
#vvipTableWrap {
  width: 100%;
  overflow: auto;
  max-height: 380px;
  border: 1px solid var(--border);
  border-radius: 12px;
  background: var(--glass);
}

#vvipTable {
  width: 100%;
  border-collapse: separate;
  border-spacing: 0;
  table-layout: fixed;
  min-width: unset !important;
}

#vvipTable th,
#vvipTable td {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  padding: 8px 12px;
}

#vvipTable th {
  position: sticky;
  top: 0;
  background: var(--dropdown-bg);
  z-index: 10;
}

#vvipTable th:first-child,
#vvipTable td:first-child {
  position: sticky;
  left: 0;
  background: var(--dropdown-bg);
  z-index: 5;
}

#vvipTable th:first-child {
  z-index: 15;
}

/* Set specific column widths for better fit */
#vvipTable th:nth-child(1), #vvipTable td:nth-child(1) { width: 12%; } /* PARTYID */
#vvipTable th:nth-child(2), #vvipTable td:nth-child(2) { width: 10%; } /* USERID */
#vvipTable th:nth-child(3), #vvipTable td:nth-child(3) { width: 10%; } /* Source */
#vvipTable th:nth-child(4), #vvipTable td:nth-child(4) { width: 12%; } /* Registration Date */
#vvipTable th:nth-child(5), #vvipTable td:nth-child(5) { width: 10%; } /* FTD Date */
#vvipTable th:nth-child(6), #vvipTable td:nth-child(6) { width: 10%; } /* Current */
#vvipTable th:nth-child(7), #vvipTable td:nth-child(7) { width: 10%; } /* Previous */
#vvipTable th:nth-child(8), #vvipTable td:nth-child(8) { width: 10%; } /* Diff */
#vvipTable th:nth-child(9), #vvipTable td:nth-child(9) { width: 8%; }  /* Diff (%) */

/* --- AI Insights Styling (robot style) --- */
.panel h2{display:flex;align-items:center;gap:10px;margin:0 0 14px 0;color:#fff;font-weight:800;letter-spacing:.3px;}
.icon-robot{display:inline-grid;place-items:center;width:26px;height:26px;border-radius:8px;background:linear-gradient(135deg, rgba(255,255,255,0.28), rgba(255,255,255,0.06));border:1px solid var(--border);box-shadow:0 6px 16px rgba(0,0,0,0.18)}
.icon-robot svg{width:18px;height:18px;opacity:.95}
.insights-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;}
.insight-box{
  padding:14px 16px;border-radius:16px;
  background:linear-gradient(180deg, rgba(255,255,255,0.10), rgba(255,255,255,0.03));
  border:1px solid var(--border);
  box-shadow:0 10px 24px rgba(0,0,0,0.22);
  color:var(--soft-text);
  min-height:140px;
  display:flex;flex-direction:column;gap:8px;align-items:flex-start;justify-content:flex-start;
}
.insight-label{
  display:inline-block;
  font-weight:900;font-size:12px;letter-spacing:.5px;text-transform:uppercase;
  color:#fff;
  background:rgba(255,255,255,0.16);
  border:1px solid rgba(255,255,255,0.24);
  padding:6px 10px;border-radius:999px; user-select:none;
}
.insight-text{font-size:14px;line-height:1.35;color:#fff}
.insight-text strong{font-weight:900}
@media (max-width:1100px){
  .insights-grid{grid-template-columns:repeat(2,1fr);}
}
@media (max-width:640px){
  .insights-grid{grid-template-columns:1fr;}
}
/* --- end AI Insights Styling --- */

/* === Normalize panel heights and VVIP sizing === */
#vvipInsightsPanel h2{ display:flex; align-items:center; gap:8px; line-height:1.2; margin-bottom:8px; }
#vvipInsightsPanel h2 svg{
  width: 18px;
  height: 18px;
  display: inline-block;
  vertical-align: middle;
}
#vvipInsightsPanel .insights-grid{
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  grid-auto-flow: column;
  column-gap: 12px;
  row-gap: 0;
}

#vvipInsightsPanel .insight-box{ padding: 6px 10px; min-height: 40px; }

/* Hotfix: prevent these two panels from stretching taller than others */
#vvipInsightsPanel, #vvipPanel { display: block; }
#vvipInsightsPanel > *:last-child, #vvipPanel > *:last-child { flex: unset; }

.insight-title{font-weight:600;margin:2px 0 4px 0;}
#vvipInsightsPanel .insight-label{white-space:normal;}
</style>
</head>
<body>
<div class="container">  <div class="header-wrap">
    <div class="brand">
      <div class="logo" aria-hidden="true">
        <!-- Icon: white card with grid and colored bars -->
        <svg viewBox="0 0 24 24">
          <rect x="5" y="6" width="14" height="12" rx="2.5" fill="white" opacity="0.92"/>
          <path d="M9 6 v12 M13 6 v12 M5 10 h14 M5 14 h14" stroke="#D6DBFF" stroke-width="0.8" opacity="0.85"/>
          <rect x="7" y="12" width="2.2" height="6" fill="#96F57E" rx="0.6"/>
          <rect x="11" y="9" width="2.2" height="9" fill="#FF7AB3" rx="0.6"/>
          <rect x="15" y="7" width="2.2" height="11" fill="#58B7FF" rx="0.6"/>
        </svg>
      </div>
      <h1>Performance Monitor</h1>
    </div>
    <div class="filters">
      <label>Period:
        <select id="periodSelect">__PERIOD_OPTIONS__</select>
      </label>
      <label>Market:
        <select id="marketSelect">__MARKET_OPTIONS__</select>
      </label>
    </div>
  </div>

  
  <div class="panel">
    <h2><span class="icon-robot" aria-hidden="true"><svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg"><rect x="4.5" y="6.5" width="15" height="11" rx="3" stroke="white" stroke-opacity=".9" fill="url(#rg2)"/><defs><linearGradient id="rg2" x1="0" y1="0" x2="24" y2="24"><stop offset="0" stop-color="#8FD1FF"/><stop offset="1" stop-color="#B597FF"/></linearGradient></defs><circle cx="9" cy="12" r="1.5" fill="#fff"/><circle cx="15" cy="12" r="1.5" fill="#fff"/><rect x="9" y="15.2" width="6" height="1.2" rx=".6" fill="#fff" opacity=".9"/><rect x="11.2" y="3" width="1.6" height="3" rx=".8" fill="#fff"/><circle cx="12" cy="3" r="1.2" fill="#B0FFDB"/></svg></span> AI Performance Executive Summary</h2>
    <div class="insights-grid">
      <div class="insight-box" id="exec1"><div class="insight-label">EXECUTIVE</div><div class="insight-text">[No summary]</div></div>
      <div class="insight-box" id="exec2"><div class="insight-label">EXECUTIVE</div><div class="insight-text">[No summary]</div></div>
      <div class="insight-box" id="exec3"><div class="insight-label">EXECUTIVE</div><div class="insight-text">[No summary]</div></div>
      <div class="insight-box" id="exec4"><div class="insight-label">EXECUTIVE</div><div class="insight-text">[No summary]</div></div>
    </div>
  </div>

  <div class="panel">
    <h2>Main Metrics</h2>
    <div id="cardsRow1" class="cards-grid"></div>
    <div id="cardsRow2" class="cards-grid" style="margin-top:12px;"></div>
  </div><div class="panel">
    <h2><span class="icon-robot" aria-hidden="true"><svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg"><rect x="4.5" y="6.5" width="15" height="11" rx="3" stroke="white" stroke-opacity=".9" fill="url(#rg)"/><defs><linearGradient id="rg" x1="0" y1="0" x2="24" y2="24"><stop offset="0" stop-color="#8FD1FF"/><stop offset="1" stop-color="#B597FF"/></linearGradient></defs><circle cx="9" cy="12" r="1.5" fill="#fff"/><circle cx="15" cy="12" r="1.5" fill="#fff"/><rect x="9" y="15.2" width="6" height="1.2" rx=".6" fill="#fff" opacity=".9"/><rect x="11.2" y="3" width="1.6" height="3" rx=".8" fill="#fff"/><circle cx="12" cy="3" r="1.2" fill="#B0FFDB"/></svg></span> AI Performance Insights</h2>
<div class="insights-grid">
  <div class="insight-box" id="insCritical"><div class="insight-label">CRITICAL ALERT</div><div class="insight-text">[No insight]</div></div>
  <div class="insight-box" id="insTrend"><div class="insight-label">TREND ANALYSIS</div><div class="insight-text">[No insight]</div></div>
  <div class="insight-box" id="insRetention"><div class="insight-label">RETENTION ISSUE</div><div class="insight-text">[No insight]</div></div>
  <div class="insight-box" id="insOpportunity"><div class="insight-label">OPPORTUNITY</div><div class="insight-text">[No insight]</div></div>
</div>
    </div>

  <div class="panel">
    <h2>Breakdown by Country</h2>
    <div id="metricPills" class="pillbar" role="tablist" aria-label="Metric selection"></div>
    <div id="stackedPlot" class="plot"></div>
  </div>
  <!-- Breakdown by Segment -->
  <div class="panel">
    <h2>Breakdown by Segment</h2>
    <div id="segmentMetricPills" class="pillbar" role="tablist" aria-label="Segment metric selection"></div>
    <div id="segmentPlot" class="plot"></div>
  </div>


<!--
<div class="panel">
  <h2>MTD Performance vs Target</h2>
  <div class="gauges-row">
    <div id="gaugeDeps" class="gauge"></div>
    <div id="gaugeNgr" class="gauge"></div>
  </div>
--></div>
</div>


  <!-- VVIP Insights panel (market-driven only) -->
  <div class="panel" id="vvipInsightsPanel">
    <h2><span class="icon-robot" aria-hidden="true"><svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg"><rect x="4.5" y="6.5" width="15" height="11" rx="3" stroke="white" stroke-opacity=".9" fill="url(#rg3)"/><defs><linearGradient id="rg3" x1="0" y1="0" x2="24" y2="24"><stop offset="0" stop-color="#8FD1FF"/><stop offset="1" stop-color="#B597FF"/></linearGradient></defs><circle cx="9" cy="12" r="1.5" fill="#fff"/><circle cx="15" cy="12" r="1.5" fill="#fff"/><rect x="9" y="15.2" width="6" height="1.2" rx=".6" fill="#fff" opacity=".9"/><rect x="11.2" y="3" width="1.6" height="3" rx=".8" fill="#fff"/><circle cx="12" cy="3" r="1.2" fill="#B0FFDB"/></svg></span> VVIP Insights</h2>
    <div class="insights-grid" id="vvipInsightsGrid">
      <div class="insight-box"><div class="insight-label">VVIP</div><div class="insight-text">[No insight]</div></div>
      <div class="insight-box"><div class="insight-label">VVIP</div><div class="insight-text">[No insight]</div></div>
      <div class="insight-box"><div class="insight-label">VVIP</div><div class="insight-text">[No insight]</div></div>
      <div class="insight-box"><div class="insight-label">VVIP</div><div class="insight-text">[No insight]</div></div>
    </div>
  </div>

<div class="panel" id="vvipPanel" style="padding:18px;">
  <h2>VVIP Performance</h2>
  <div id="vvipMetricPills" class="pillbar" role="tablist" aria-label="VVIP metric selection" style="margin-bottom:10px"></div>
  <div id="vvipNotice" style="display:none; padding:14px; border-radius:12px; border:1px solid var(--border); background:linear-gradient(180deg, rgba(255,255,255,0.08), rgba(255,255,255,0.02)); color:var(--soft-text);"></div>
  <div class="table-wrap" id="vvipTableWrap" style="overflow:auto; max-height:380px;">
    <table id="vvipTable" style="width:100%; border-collapse:separate; border-spacing:0;">
      <thead></thead>
      <tbody></tbody>
    </table>
  </div>
</div>


<!-- VVIP CHURN PANEL START -->
<div class="panel" id="vvipChurnPanel" style="padding:18px;">
  <h2>VVIP Churn</h2>
  <div id="vvipChurnBlurb" class="soft" style="margin-bottom:10px">
    The following VVIP players are in risk of churn, as they haven't made a deposit or a bet in the past 14 days:
  </div>
  <div id="vvipChurnNotice" style="display:none; padding:14px; border-radius:12px; background: linear-gradient(145deg, rgba(255,255,255,0.08), rgba(255,255,255,0.02)); color:var(--soft-text);"></div>
  <div class="table-wrap" id="vvipChurnTableWrap" style="overflow:auto; max-height:380px;">
    <table id="vvipChurnTable" style="width:100%; border-collapse:separate; border-spacing:0;">
      <thead></thead>
      <tbody></tbody>
    </table>
  </div>
</div>
<!-- VVIP CHURN PANEL END -->

<script>
function safeById(id){ try{ return document.getElementById(id)||null; }catch(e){ return null; } }
function hasEl(id){ return !!safeById(id); }

const MAIN_DATA=__MAIN_JSON__;
const SUMMARY_DATA=__SUMMARY_JSON__;
const METRICS=__METRICS_JSON__;
const DIFF_DATA=__DIFF_JSON__;
const GROUP_DATA=__GROUP_JSON__;
const GROUP_METRICS=__GROUP_METRICS_JSON__;
const VVIP_DATA=__VVIP_JSON__;
const VVIP_CHURN=__VVIP_CHURN_JSON__;

function renderVVIPChurn(){
  const wrap = document.getElementById('vvipChurnTableWrap');
  const thead = document.querySelector('#vvipChurnTable thead');
  const tbody = document.querySelector('#vvipChurnTable tbody');
  const notice = document.getElementById('vvipChurnNotice');
  const blurb = document.getElementById('vvipChurnBlurb');
  const mEl=document.getElementById('marketSelect');
  const sel = mEl && mEl.value ? String(mEl.value).trim().toUpperCase() : 'ALL';

  const data = Array.isArray(VVIP_CHURN) ? VVIP_CHURN : [];
  if (!wrap || !thead || !tbody) { return; }

  function getMarket(row){
    const key = Object.keys(row||{}).find(k => String(k).trim().toLowerCase()==='market');
    return key ? String(row[key]||'ALL').trim().toUpperCase() : 'ALL';
  }

  const rows = data.filter(r => getMarket(r) === sel);

  if (rows.length === 0){
    wrap.style.display='none';
    if (notice){ notice.style.display='block'; notice.textContent = `There is no VVIP player in risk of churn in market ${sel}`; }
    return;
  }
  wrap.style.display='block';
  if (notice){ notice.style.display='none'; }

  const preferred = ['PARTYID','USERID','REG_DATE','FTD_DATE','Source','Last BET','Last DEP','DEP Amount (local CC)','Last WD','WD Amount (local CC)'];
  const keySet = new Set(); rows.forEach(r=>Object.keys(r||{}).forEach(k=>keySet.add(k)));
  const all = Array.from(keySet);
  const cols = preferred.filter(c=>all.includes(c)).concat(all.filter(c=>!preferred.includes(c)));
  const colsFiltered = cols.filter(c => c !== 'Market');

  thead.innerHTML = '<tr>' + colsFiltered.map((c,i)=>{
    const sticky = i===0 ? 'position:sticky;left:0;z-index:2; backdrop-filter: blur(6px);' : '';
    const label = (c==='USERID'?'Username':c);
    return `<th style="text-align:left; padding:10px 12px; font-weight:700; border-bottom:1px solid rgba(255,255,255,0.22); position:sticky; top:0; background:rgba(0,0,0,0.25); ${sticky}">${label}</th>`;
  }).join('') + '</tr>';

  const BASE_URL = 'https://core.yyycasino.com/core/#/app/core/players/';

  tbody.innerHTML = rows.map(r=>{
    return '<tr>' + colsFiltered.map((c,i)=>{
      let val = r[c];
      if (val === null || val === undefined) val = '';
      if (c==='PARTYID' && val!==''){
        const id = String(val).replace(/[^0-9]/g,'');
        if (id){ val = `<a href="${BASE_URL}${id}/detail" target="_blank" rel="noopener noreferrer">${id}</a>`; }
      }
      const sticky = i===0 ? 'position:sticky;left:0;z-index:1; background:rgba(0,0,0,0.08);' : '';
      return `<td style="padding:8px 12px; border-bottom:1px dashed rgba(255,255,255,0.18); ${sticky}">${val}</td>`;
    }).join('') + '</tr>';
  }).join('');
}


function fmtNum(x){
  if(x===null||x===undefined||x===''||isNaN(Number(x)))return'N/A';
  const n=Number(x);
  if(Math.abs(n)>=1e9)return(n/1e9).toFixed(1)+'B';
  if(Math.abs(n)>=1e6)return(n/1e6).toFixed(1)+'M';
  if(Math.abs(n)>=1e3)return(n/1e3).toFixed(1)+'k';
  return n.toLocaleString(undefined,{maximumFractionDigits:2});
}
function unique(a){ return Array.from(new Set(a)); }
function sortDates(a){
  return a.slice().sort((x,y)=>{
    const dx=Date.parse(x), dy=Date.parse(y);
    if(!isNaN(dx)&&!isNaN(dy)) return dx-dy;
    return String(x).localeCompare(String(y));
  });
}
function norm(m){return String(m||'').replace(/[^A-Za-z]/g,'').toUpperCase();}
function titleCaseMetric(lbl){
  const map={DEPOSITS:'DEPs',DEPS:'DEPs',NGR:'NGR',RETENTION:'Retention',MARGIN:'Margin',RMPS:'RMPs',FTDS:'FTDs',STAKE:'Stake',GGR:'GGR'};
  return map[lbl]||lbl.charAt(0)+lbl.slice(1).toLowerCase();
}

// Card order
const CARD_ORDER=[
  ['DEPOSITS',['DEPOSITS','DEPS','DEP']],
  ['NGR',['NGR']],
  ['RETENTION',['RETENTION']],
  ['MARGIN',['MARGIN']],
  ['RMPS',['RMPS']],
  ['FTDS',['FTDS','FTD']],
  ['STAKE',['STAKE']],
  ['GGR',['GGR']]
];

function match(m,a){const nm=norm(m);return a.map(x=>norm(x)).includes(nm);}
function aggregateAllRows(rowsForMetric){
  const value = rowsForMetric.reduce((s,r)=>s + (Number(r['VALUE'])||0), 0);
  const target = rowsForMetric.reduce((s,r)=>s + (Number(r['TARGET'])||0), 0);
  const pcts = rowsForMetric.map(r=>Number(r['DIFF_PERCENTAGE'])).filter(v=>!isNaN(v)&&v!==0);
  const pct = pcts.length>0 ? pcts.reduce((s,v)=>s+v,0)/pcts.length : 0;
  return {VALUE:value, TARGET:target, DIFF_PERCENTAGE:pct, Country:'ALL'};
}


function pickDiff(aliases, period, market){
  const rows = DIFF_DATA.filter(r =>
    String(r['Period']||'Yesterday').toLowerCase()===String(period).toLowerCase() &&
    aliases.map(a=>a.toLowerCase()).includes(String(r['METRIC']||'').toLowerCase())
  );
  if(rows.length===0) return null;
  const exact = rows.find(rr => String(rr['Country']).toLowerCase()===String(market).toLowerCase());
  if(exact) return exact;
  const allRow = rows.find(rr => String(rr['Country']).toLowerCase()==='all');
  return allRow || rows[0];
}

function renderCards(){
  const p=document.getElementById('periodSelect').value;
  const m=document.getElementById('marketSelect').value;
  const rows=SUMMARY_DATA.filter(r=>String(r['Period']||'Yesterday').toLowerCase()===p.toLowerCase()
    && (m==='ALL' || String(r['Country']).toLowerCase()===m.toLowerCase() || String(r['Country']).toLowerCase()==='all'));
  const r1=document.getElementById('cardsRow1'), r2=document.getElementById('cardsRow2');
  r1.innerHTML=''; r2.innerHTML='';

  

const make=(label, rec, diffRec)=>{
  const v=Number(rec?.['VALUE'])||0, t=Number(rec?.['TARGET'])||0;
  const nm = norm(label);
  let unit = '';
  if (match(label, ['DEPOSITS','DEPS','DEP']) || ['NGR','STAKE','GGR'].includes(nm)) unit = '$';
  else if (['RETENTION','MARGIN'].includes(nm)) unit = '%';

  const prev = Number(diffRec?.['Previous']);
  let pctPrev = Number(diffRec?.['DIFF_PERCENTAGE']);
  // % vs Target (fallback compute if not in summary)
  let pctTarget = Number(rec?.['DIFF_PERCENTAGE']);
  if (isNaN(pctTarget)) {
    pctTarget = (t!==0) ? ((v - t) / Math.abs(t)) * 100 : NaN;
  }

  const valueText = unit==='%' ? (fmtNum(v)+unit) : (unit==='$' ? (unit+fmtNum(v)) : fmtNum(v));
  const targetText = unit==='%' ? (fmtNum(t)+unit) : (unit==='$' ? (unit+fmtNum(t)) : fmtNum(t));
  const previousTxt = isNaN(prev) ? 'N/A' : (unit==='%' ? (fmtNum(prev)+unit) : (unit==='$' ? (unit+fmtNum(prev)) : fmtNum(prev)));

  let h=`<div class='metric-card'><div class='metric-label'>${label}</div><div class='metric-value'>${valueText}</div><div class='metric-subtitle'>Current Value</div>`;

  // two percentage pills (left: vs Target, right: vs Previous)
  let row = '<div class="metric-changes">';
  // pill vs Target
  if(!isNaN(pctTarget)){
    let cT;
    if(label.toLowerCase()==='margin'){ cT=(pctTarget>=0)?'negative':'positive'; }
    else { cT=(pctTarget>=0)?'positive':'negative'; }
    const sT=pctTarget>=0?'+':'';
    row += `<div class="metric-change ${cT}">${sT}${Math.abs(pctTarget).toFixed(1)}%</div>`;
  } else {
    row += `<div class="metric-change" style="opacity:.6">N/A</div>`;
  }
  // pill vs Previous
  if(!isNaN(pctPrev)){
    let cP;
    if(label.toLowerCase()==='margin'){ cP=(pctPrev>=0)?'negative':'positive'; }
    else { cP=(pctPrev>=0)?'positive':'negative'; }
    const sP=pctPrev>=0?'+':'';
    row += `<div class="metric-change ${cP}">${sP}${Math.abs(pctPrev).toFixed(1)}%</div>`;
  } else {
    row += `<div class="metric-change" style="opacity:.6">N/A</div>`;
  }
  row += '</div>';
  h += row;

  // Footer row: Target (left) | Previous (right)
  h+=`<div class='metric-footer'><span>Target: ${targetText}</span><span>Previous: ${previousTxt}</span></div>`;
  h+='</div>';
  return h;
};

  const pick=(aliases)=>{
    const s=rows.filter(r=>match(r['METRIC'],aliases));
    if(s.length===0){
      const all=SUMMARY_DATA.filter(r=>match(r['METRIC'],aliases) && String(r['Period']||'Yesterday').toLowerCase()===p.toLowerCase());
      const exact = all.find(rr=>String(rr['Country']).toLowerCase()===m.toLowerCase());
      if(exact) return exact;
      const allRow = all.find(rr=>String(rr['Country']).toLowerCase()==='all');
      if(allRow) return allRow;
      if(m==='ALL' && all.length>0){
        const onlyCountries = all.filter(rr=>String(rr['Country']).toLowerCase()!=='all');
        if(onlyCountries.length>0) return aggregateAllRows(onlyCountries);
      }
      return null;
    }
    if(m==='ALL'){
      const allRow = s.find(rr=>String(rr['Country']).toLowerCase()==='all');
      if(allRow) return allRow;
      const nonAll = s.filter(rr=>String(rr['Country']).toLowerCase()!=='all');
      if(nonAll.length>0) return aggregateAllRows(nonAll);
      return s[0];
    }
    return s.find(rr=>String(rr['Country']).toLowerCase()===m.toLowerCase())
        || s.find(rr=>String(rr['Country']).toLowerCase()==='all')
        || s[0];
  };

  CARD_ORDER.forEach((pair,i)=>{
    const [label, aliases]=pair;
    const rec=pick(aliases);
    const diffRec = pickDiff(aliases, p, m);
    const container = (i<4)? r1 : r2;
    if(rec){ container.insertAdjacentHTML('beforeend', make(label, rec, diffRec)); }
    else { container.insertAdjacentHTML('beforeend', `<div class='metric-card'><div class='metric-label'>${label}</div><div class='metric-value'>N/A</div></div>`); }
  });
}

// Pills
// Inlined to avoid load-order errors
const METRIC_ORDER = [["DEPS",["DEPOSITS","DEPS","DEP"]],["NGR",["NGR"]],["Retention",["RETENTION"]],["RMPs",["RMPS"]],["FTDs",["FTDS","FTD"]],["GGR",["GGR"]],["STAKE",["STAKE"]],["Margin",["MARGIN"]]];
let currentMetric=null;
function titleCase(lbl){ return lbl.charAt(0) + lbl.slice(1).toLowerCase(); }
function initMetricPills(){
  const container=document.getElementById('metricPills');
  container.innerHTML='';
  const present=METRICS.map(x=>String(x));
  const pills=[];
  METRIC_ORDER.forEach(([label,aliases])=>{
    const found = present.find(m=>aliases.map(a=>a.toLowerCase()).includes(String(m).toLowerCase()));
    if(found){
      pills.push({label, key:found});
    }
  });
  if(pills.length===0){
    METRICS.forEach(m=>pills.push({label:String(m).toUpperCase(), key:m}));
  }
  pills.forEach((p,idx)=>{
    const btn=document.createElement('button');
    btn.className='pill'+(idx===0?' active':'');
    btn.setAttribute('role','tab');
    btn.setAttribute('aria-selected', idx===0 ? 'true' : 'false');
    btn.dataset.key=p.key;
    btn.textContent = p.label || titleCaseMetric(p.key);
    btn.addEventListener('click',()=>{
      currentMetric=p.key;
      for(const child of container.children){
        child.classList.remove('active');
        child.setAttribute('aria-selected','false');
      }
      btn.classList.add('active');
      btn.setAttribute('aria-selected','true');
      renderStacked();
    });
    container.appendChild(btn);
    if(idx===0){ currentMetric=p.key; }
  });
}

function renderStacked(){
  const plot=document.getElementById('stackedPlot');
  const p=document.getElementById('periodSelect').value;
  const m=document.getElementById('marketSelect').value;
  const met=currentMetric;
  const isCurrency = (match(met,['DEPOSITS','DEPS','DEP']) || ['NGR','STAKE','GGR'].includes(norm(met)));
  const isPercent = ['RETENTION','MARGIN'].includes(norm(met));
  const yPrefix = isCurrency ? '$' : '';
  const ySuffix = isPercent ? '%' : '';

  let pr=MAIN_DATA.filter(r=>String(r['Period']||'Yesterday').toLowerCase()===p.toLowerCase()
      && String(r['METRIC']||'').toLowerCase()===String(met).toLowerCase() && r['Date']);
 let dts = sortDates(unique(pr.map(r => r['Date']))), tr=[];

// --- limits requested ---
if (p === 'Yesterday') {
  // show only the most recent 60 days
  dts = dts.slice(-60);
} else if (p === '7 days') {
  // cap total points at 60 by downsampling evenly if needed
  if (dts.length > 60) {
    const step = Math.ceil(dts.length / 60);
    dts = dts.filter((_, i) => i % step === 0).slice(-60);
  }
}
// ------------------------


  
  const colors=['#54E0E9','#9B8CFF','#FFB1EC','#DB8EFF','#FFD166','#66E3A1','#A1E0FF','#B3C8FF','#6AC8FF','#F3C4FF'];

  // Helper: average % values for a date across a list of countries (for Retention fallback)
  function avgPercentFor(date, countries) {
    const vals = pr
      .filter(r => r['Date']===date && countries.includes(String(r['Country']).toUpperCase()))
      .map(r => Number(r['VALUE'])||0)
      .filter(v => !isNaN(v));
    if (!vals.length) return 0;
    return vals.reduce((a,b)=>a+b,0) / vals.length;
  }

  // Special rule: if metric is Retention and market is ALL/GCC/BET, plot ONE aggregate series.
  if (norm(met)==='RETENTION' && (m==='ALL' || m==='GCC' || m==='BET')) {
    const wanted = m; // 'ALL' | 'GCC' | 'BET'
    // Prefer aggregated rows if present
    const hasAgg = pr.some(r => String(r['Country']).toUpperCase()===wanted);
    const subsALL = unique(pr.map(r => String(r['Country']).toUpperCase()))
                      .filter(c => !['ALL','GCC','BET'].includes(c));
    const subsGCC = ['AE','SA','QA','KW','JO','BH'];
    const subsBET = ['NZ'];

    tr.push({
      x: dts,
      y: dts.map(dd => {
        if (hasAgg) {
          // Use the aggregate row(s) for the wanted market on this date
          const vals = pr
            .filter(r => r['Date']===dd && String(r['Country']).toUpperCase()===wanted)
            .map(r => Number(r['VALUE'])||0)
            .filter(v => !isNaN(v));
          return vals.length ? (vals.reduce((a,b)=>a+b,0)/vals.length) : 0;
        } else {
          // Fallback: average of sub-markets for that market group
          const subs = wanted==='ALL' ? subsALL : (wanted==='GCC' ? subsGCC : subsBET);
          return avgPercentFor(dd, subs);
        }
      }),
      name: wanted,
      type: 'bar',
      opacity: 0.95,
      marker: { color: colors[0] }
    });

  } else if(m==='ALL'){
    const cs=unique(pr.map(r=>r['Country']))
      .filter(c=>!['ALL','GCC','BET'].includes(String(c).toUpperCase()))
      .sort();
    cs.forEach((c,idx)=>{
      tr.push({
        x:dts,
        y:dts.map(dd=>{
          const rs=pr.filter(r=>r['Date']===dd && r['Country']===c);
          return rs.reduce((a,r)=>a+(Number(r['VALUE'])||0),0);
        }),
        name:c, type:'bar', opacity:0.9, marker:{color:colors[idx % colors.length]}
      });
    });
  } else if(m==='GCC'){
    const sub=['AE','SA','QA','KW','JO','BH'];
    sub.forEach((c,idx)=>{
      tr.push({
        x:dts,
        y:dts.map(dd=>{
          const rs=pr.filter(r=>r['Date']===dd && String(r['Country']).toUpperCase()===c);
          return rs.reduce((a,r)=>a+(Number(r['VALUE'])||0),0);
        }),
        name:c, type:'bar', opacity:0.9, marker:{color:colors[idx % colors.length]}
      });
    });
  } else if(m==='BET'){
    const sub=['NZ'];
    sub.forEach((c,idx)=>{
      tr.push({
        x:dts,
        y:dts.map(dd=>{
          const rs=pr.filter(r=>r['Date']===dd && String(r['Country']).toUpperCase()===c);
          return rs.reduce((a,r)=>a+(Number(r['VALUE'])||0),0);
        }),
        name:c, type:'bar', opacity:0.95, marker:{color:colors[idx % colors.length]}
      });
    });
  } else {
    tr.push({
      x:dts,
      y:dts.map(dd=>{
        const rs=pr.filter(r=>r['Date']===dd && String(r['Country']).toLowerCase()===m.toLowerCase());
        return rs.reduce((a,r)=>a+(Number(r['VALUE'])||0),0);
      }),
      name:m, type:'bar', opacity:0.95, marker:{color:colors[0]}
    });
  }

// ===== Breakdown by Segment: pills + chart =====
;(function(){
  if (window.__SEGMENT_AFTER_COUNTRY__) return; window.__SEGMENT_AFTER_COUNTRY__ = true;

  const SEGMENT_ORDER_DISPLAY=['New players','FTD 1-3 Months','FTD 4-12 Months','FTD 1-2 Years','FTD 2+ Years','Unassigned'];
  const colors=['#91A7FF','#74C0FC','#63E6BE','#FFD43B','#FFA8A8','#D0BFFF'];
  const ALIASES=[["DEPS",["DEPOSITS","DEPS","DEP"]],["NGR",["NGR"]],["Retention",["RETENTION"]],["RMPs",["RMPS"]],["FTDs",["FTDS","FTD"]],["GGR",["GGR"]],["STAKE",["STAKE"]],["Margin",["MARGIN"]]];
  function unique(a){ return Array.from(new Set(a)); }
  function sortDates(a){
    return a.slice().sort((x,y)=>{
      const dx=Date.parse(x), dy=Date.parse(y);
      if(!isNaN(dx)&&!isNaN(dy)) return dx-dy;
      return String(x).localeCompare(String(y));
    });
  }
  function titleCaseMetric(lbl){
    const map={DEPOSITS:'DEPs',DEPS:'DEPs',NGR:'NGR',RETENTION:'Retention',MARGIN:'Margin',RMPS:'RMPs',FTDS:'FTDs',STAKE:'Stake',GGR:'GGR'};
    const u=String(lbl||'').replace(/[^A-Za-z]/g,'').toUpperCase();
    return map[u]||String(lbl||'').charAt(0)+String(lbl||'').slice(1).toLowerCase();
  }

  let currentSegMetric=null;
  function initSegmentPills(){
    const container=document.getElementById('segmentMetricPills'); if(!container) return;
    container.innerHTML='';
    const gm = Array.isArray(GROUP_METRICS) ? GROUP_METRICS.map(String) : [];
    const base = gm.length ? gm : (Array.isArray(METRICS) ? METRICS.map(String) : []);
    const pills=[];
    ALIASES.forEach(([label,als])=>{
      const found = base.find(m=>als.map(a=>a.toLowerCase()).includes(String(m).toLowerCase()));
      if(found) pills.push({label, key:found});
    });
    if(pills.length===0){ base.forEach(m=>pills.push({label:String(m).toUpperCase(), key:m})); }
    pills.forEach((p,idx)=>{
      const btn=document.createElement('button');
      btn.className='pill'+(idx===0?' active':'');
      btn.setAttribute('role','tab');
      btn.setAttribute('aria-selected', idx===0 ? 'true' : 'false');
      btn.dataset.key=p.key;
      btn.textContent = p.label || titleCaseMetric(p.key);
      btn.addEventListener('click',()=>{
        currentSegMetric=p.key;
        for(const child of container.children){ child.classList.remove('active'); child.setAttribute('aria-selected','false'); }
        btn.classList.add('active'); btn.setAttribute('aria-selected','true');
        renderSegment();
      });
      container.appendChild(btn);
      if(idx===0){ currentSegMetric=p.key; }
    });
    if (!currentSegMetric && pills.length>0) currentSegMetric=pills[0].key;
  }

  function renderSegment(){
    const plot=document.getElementById('segmentPlot'); if(!plot) return;
    if(!Array.isArray(GROUP_DATA) || GROUP_DATA.length===0){
      plot.innerHTML='<p style="opacity:.7;margin:12px 0 0 6px">No segment data available for the selected filters.</p>';
      return;
    }
    const p=(document.getElementById('periodSelect')?.value||'Yesterday');
    const m=(document.getElementById('marketSelect')?.value||'ALL');
    const met=String(currentSegMetric||'').toLowerCase();
    let pr=GROUP_DATA.filter(r=>String(r['Period']||'Yesterday').toLowerCase()===p.toLowerCase()
        && String(r['METRIC']||'').toLowerCase()===met && r['Date']);
    if ((met === 'retention' || met === 'margin') && (m === 'ALL' || m === 'GCC' || m === 'BET')) {
      // Mirror Breakdown by Country logic: if market is ALL/GCC/BET for Retention,
      // show only the aggregate (ALL/GCC/BET), not the sum of individual markets.
      const wanted = m; // 'ALL' | 'GCC' | 'BET'
      const hasAgg = pr.some(r => String(r['Country']).toUpperCase() === wanted);

      if (hasAgg) {
        // Keep only the aggregate rows for the wanted market
        pr = pr.filter(r => String(r['Country']).toUpperCase() === wanted);
      } else {
        // Fallback: compute an average across the sub-markets for each date
        const subsALL = [...new Set(pr.map(r => String(r['Country']).toUpperCase()))]
          .filter(c => !['ALL','GCC','BET'].includes(c));
        const subsGCC = ['AE','SA','QA','KW','JO','BH'];
        const subsBET = ['NZ'];
        const subs = wanted === 'ALL' ? subsALL : (wanted === 'GCC' ? subsGCC : subsBET);
        const dates = [...new Set(pr.map(r => r['Date']))].sort((a,b)=>{
          const da = Date.parse(a), db = Date.parse(b);
          if(!isNaN(da)&&!isNaN(db)) return da - db;
          return String(a).localeCompare(String(b));
        });
        pr = dates.flatMap(dd => {
          const vals = pr
            .filter(r => r['Date'] === dd && subs.includes(String(r['Country']).toUpperCase()))
            .map(r => Number(r['VALUE']) || 0)
            .filter(v => !isNaN(v));
          if (!vals.length) return [];
          const sample = pr.find(r => r['Date'] === dd) || {};
          return [{ ...sample, Date: dd, Country: wanted, VALUE: vals.reduce((a,b)=>a+b,0) / vals.length }];
        });
      }
    } else if(m!=='ALL' && m!=='GCC' && m!=='BET'){
      pr = pr.filter(r => String(r['Country']).toLowerCase()===m.toLowerCase());
    }else if(m==='GCC'){
      const setG=['AE','SA','QA','KW','JO','BH']; pr = pr.filter(r => setG.includes(String(r['Country']).toUpperCase()));
    }else if(m==='BET'){
      const setB=['NZ']; pr = pr.filter(r => setB.includes(String(r['Country']).toUpperCase()));
    }
    let dts = sortDates(unique(pr.map(r => r['Date'])));
    

// --- limits requested ---
if (p === 'Yesterday') {
  // show only the most recent 60 days
  dts = dts.slice(-60);
} else if (p === '7 days') {
  // cap total points at 60 by downsampling evenly if needed
  if (dts.length > 60) {
    const step = Math.ceil(dts.length / 60);
    dts = dts.filter((_, i) => i % step === 0).slice(-60);
  }
}
// ------------------------




    const segs = ['New players','FTD 1-3 Months','FTD 4-12 Months','FTD 1-2 Years','FTD 2+ Years','Unassigned'].filter(s=>pr.some(r=>String(r['FTD_Group'])===s));
    const traces=[];
    segs.forEach((seg, idx)=>{
      traces.push({ x:dts, y:dts.map(dd=>pr.filter(r=>r['Date']===dd && String(r['FTD_Group'])===seg).reduce((a,r)=>a+(Number(r['VALUE'])||0),0)),
        name:seg, type:'bar', opacity:0.95, marker:{color:colors[idx % colors.length]} });
    });
    if(traces.length>0){
      plot.innerHTML='';
      Plotly.newPlot('segmentPlot', traces, { barmode:'stack', bargap:0.18, paper_bgcolor:'rgba(0,0,0,0)', plot_bgcolor:'rgba(0,0,0,0)', font:{color:'#F7F4FF'}, margin:{t:20,r:20,b:40,l:45}, legend:{x:1.02,xanchor:'left',y:1}, xaxis:{gridcolor:'rgba(255,255,255,0.25)'}, yaxis:{gridcolor:'rgba(255,255,255,0.25)'} }, {responsive:true, displayModeBar:false});
    } else {
      plot.innerHTML='<p style="opacity:.7;margin:12px 0 0 6px">No segment data for the chosen metric.</p>';
    }
  }

  // hook into existing init/refresh
  const oldRefresh = window.refreshAll;
  window.refreshAll = function(){
    if (typeof oldRefresh === 'function') { try{ oldRefresh(); }catch(e){} }
    try{ renderSegment(); }catch(e){}
  };
  document.addEventListener('DOMContentLoaded', function(){
    try{ renderVVIPChurn(); }catch(e){}
  try{ initSegmentPills(); }catch(e){};
    try{ renderSegment(); }catch(e){};
  });
  window.initSegmentPills = initSegmentPills;
  window.renderSegment = renderSegment;
})();

  if(tr.length>0){
    plot.innerHTML='';
      Plotly.newPlot('stackedPlot', tr, {
      barmode:'stack',
      bargap:0.18,
      paper_bgcolor:'rgba(0,0,0,0)',
      plot_bgcolor:'rgba(0,0,0,0)',
      font:{color:'#F7F4FF'},
      margin:{t:20,r:20,b:40,l:45},
      legend:{x:1.02,xanchor:'left',y:1,bgcolor:'rgba(0,0,0,0)',font:{size:12}},
      xaxis:{
        gridcolor:'rgba(255,255,255,0.25)',
        tickcolor:'rgba(255,255,255,0.65)',
        zerolinecolor:'rgba(255,255,255,0.25)'
      },
      yaxis:{
        gridcolor:'rgba(255,255,255,0.25)',
        tickcolor:'rgba(255,255,255,0.65)',
        zerolinecolor:'rgba(255,255,255,0.25)',
        tickprefix:yPrefix,
        ticksuffix:ySuffix
      }
    }, {responsive:true, displayModeBar:false});
  }else{
    document.getElementById('stackedPlot').innerHTML='<p>No data</p>';
  }
}

// === VVIP Insights (market filter only, ignores period) ===
function buildVVIPTitle(txt){
  const s = String(txt||"").toLowerCase();
  const metric = (s.includes("deposits") && "deposits") || (s.includes("ngr") && "NGR") || (s.includes("stake") && "stake") || "performance";
  const inc = s.includes(" has increased ");
  const dec = s.includes(" has decreased ");
  let when = "";
  const m7  = s.match(/\bin the last\s+7\s+days\b/);
  const m30 = s.match(/\bin the last\s+30\s+days\b/);
  const my  = s.match(/\byesterday\b/);
  const mtd = s.match(/\bMTD\b/i);
  if(m7){ when = "in the last 7 days"; }
  else if(m30){ when = "in the last 30 days"; }
  else if(my){ when = "yesterday"; }
  else if(mtd){ when = "MTD"; }
  const major = s.includes(" dramatically ") || s.includes(" considerably ");
  const prefix = major ? "Major " : "";
  const verb = inc ? "increased" : (dec ? "decreased" : "changed");
  return `${prefix}${metric} ${verb}${when ? " " + when : ""}!`;
}

function renderVVIPInsights(){
  const grid = document.getElementById('vvipInsightsGrid');
  if(!grid){ return; }
  const marketSelect = document.getElementById('marketSelect');
  const m = (marketSelect && marketSelect.value) ? marketSelect.value : 'ALL';
  const data = (window.VVIP_INSIGHTS && (window.VVIP_INSIGHTS[m] || window.VVIP_INSIGHTS[m.toUpperCase()]))
               || (window.VVIP_INSIGHTS ? window.VVIP_INSIGHTS.ALL : [])
               || [];
  const items = Array.isArray(data) ? data.slice(0,4) : [];
  grid.innerHTML = '';
  if(items.length===0){
    for(let i=0;i<4;i++){
      grid.insertAdjacentHTML('beforeend', '<div class="insight-box"><div class="insight-label">INSIGHT</div><div class="insight-text">[No insight]</div></div>');
    }
    return;
  }
  items.forEach(txt=>{
    const safe = String(txt||'[No insight]').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    const title = buildVVIPTitle(safe);
    grid.insertAdjacentHTML('beforeend', `<div class="insight-box"><div class="insight-label">${title}</div><div class="insight-text">${safe}</div></div>`);
  });
}
function gaugeCfg(v,t,tit){
  const r=(t&&t>0)?v/t:0;
  const nm=norm(tit);
  let unit='';
  if (match(tit,['DEPOSITS','DEPS','DEP']) || ['NGR','STAKE','GGR'].includes(nm)) unit='$';
  else if (['RETENTION','MARGIN'].includes(nm)) unit='%';
  const vText = unit==='%' ? (fmtNum(v)+unit) : (unit==='$' ? ('$'+fmtNum(v)) : fmtNum(v));
  const tText = unit==='%' ? (fmtNum(t)+unit) : (unit==='$' ? ('$'+fmtNum(t)) : fmtNum(t));
  return {
    type:'indicator',
    mode:'gauge+number',
    value:Math.min(r*100, 120),
    title:{text:`${tit}<br><span style='font-size:13px;color:#F0ECFF'>${vText} / ${tText} Target</span>`},
    number:{suffix:'%',valueformat:'.1f'},
    gauge:{
      axis:{range:[0,120],tickwidth:1,tickcolor:'rgba(255,255,255,0.7)'},
      bar:{color:'#66E3A1'},
      bgcolor:'rgba(0,0,0,0)',
      borderwidth:1,bordercolor:'rgba(255,255,255,0.25)',
      steps:[
        {range:[0,80],color:'rgba(239,68,68,0.28)'},
        {range:[80,100],color:'rgba(245,158,11,0.28)'},
        {range:[100,120],color:'rgba(34,197,94,0.28)'}
      ],
      threshold:{line:{color:'#ffffff',width:2},thickness:0.8,value:Math.min(r*100,120)}
    },
    domain:{x:[0,1],y:[0,1]}
  };
}

function pickMTD(aliases, m){
  const monthly = SUMMARY_DATA.filter(r=>['mtd','30 days','monthly'].includes(String(r['Period']||'').toLowerCase()) && match(r['METRIC'],aliases));
  if(monthly.length===0) return null;
  const exact = monthly.find(rr=>String(rr['Country']).toLowerCase()===m.toLowerCase());
  if(exact) return exact;
  const allRow = monthly.find(rr=>String(rr['Country']).toLowerCase()==='all');
  if(allRow) return allRow;
  if(m==='ALL'){
    const nonAll = monthly.filter(rr=>String(rr['Country']).toLowerCase()!=='all');
    if(nonAll.length>0) return aggregateAllRows(nonAll);
  }
  return monthly[0];
}

function renderGauges(){
  if(!hasEl('gaugeDeps') || !hasEl('gaugeNgr')){ return; }

  const m=document.getElementById('marketSelect').value;
  const recD=pickMTD(['DEPOSITS','DEPS','DEP'], m), recN=pickMTD(['NGR'], m);
  const vD=Number(recD?.['VALUE'])||0, tD=Number(recD?.['TARGET'])||0;
  const vN=Number(recN?.['VALUE'])||0, tN=Number(recN?.['TARGET'])||0;
  const gl={paper_bgcolor:'rgba(0,0,0,0)',plot_bgcolor:'rgba(0,0,0,0)',font:{color:'#F7F4FF'}};
  Plotly.newPlot('gaugeDeps',[gaugeCfg(vD,tD,'DEPOSITS')],gl,{displayModeBar:false,responsive:true});
  Plotly.newPlot('gaugeNgr',[gaugeCfg(vN,tN,'NGR')],gl,{displayModeBar:false,responsive:true});
}

function refreshAll(){
renderVVIPInsights();
renderCards();
  renderStacked();
  renderGauges();
  try{ renderSegment();
  try{ renderVVIPTable();
  try{ renderVVIPChurn(); }catch(e){} }catch(e){}
}catch(e){}
}


document.getElementById('periodSelect').addEventListener('change',refreshAll);
document.getElementById('marketSelect').addEventListener('change',refreshAll);
try{ document.getElementById('marketSelect').addEventListener('change', function(){ try{ renderVVIPChurn(); }catch(e){} }); }catch(e){};

(function init(){
  // metric pills
  const container=document.getElementById('metricPills');
  container.innerHTML='';
  const present=METRICS.map(x=>String(x));
  const order=[['DEPOSITS',['DEPOSITS','DEPS','DEP']],
             ['FTDS',['FTDS','FTD']],
             ['GGR',['GGR']],
             ['MARGIN',['MARGIN']],
             ['NGR',['NGR']],
             ['RETENTION',['RETENTION']],
             ['RMPS',['RMPS']],
             ['STAKE',['STAKE']]];
  const pills=[];
  order.forEach(([label,aliases])=>{
    const found = present.find(m=>aliases.map(a=>a.toLowerCase()).includes(String(m).toLowerCase()));
    if(found) pills.push({label, key:found});
  });
  if(pills.length===0){
    METRICS.forEach(m=>pills.push({label:String(m).toUpperCase(), key:m}));
  }
  pills.forEach((p,idx)=>{
    const btn=document.createElement('button');
    btn.className='pill'+(idx===0?' active':'');
    btn.setAttribute('role','tab');
    btn.setAttribute('aria-selected', idx===0 ? 'true' : 'false');
    btn.dataset.key=p.key;
    btn.textContent = p.label || titleCaseMetric(p.key);
    btn.addEventListener('click',()=>{
      currentMetric=p.key;
      for(const child of container.children){
        child.classList.remove('active');
        child.setAttribute('aria-selected','false');
      }
      btn.classList.add('active');
      btn.setAttribute('aria-selected','true');
      renderStacked();
    });
    container.appendChild(btn);
    if(idx===0){ currentMetric=p.key; }
  });
  refreshAll();
})();



let currentVvipMetric=null;
const VVIP_ALIASES=[["DEPs",["DEPOSITS","DEPS","DEP"]],["NGR",["NGR"]],["RMPs",["RMPS"]],["GGR",["GGR"]],["STAKE",["STAKE"]]];
function initVvipPills(){
  const container=document.getElementById('vvipMetricPills'); if(!container) return;
  container.innerHTML='';
  const present = Array.isArray(METRICS)? METRICS.map(String) : [];
  const pills=[];
  VVIP_ALIASES.forEach(([label,aliases])=>{
    const found = present.find(m=>aliases.map(a=>a.toLowerCase()).includes(String(m).toLowerCase()));
    if(found){ pills.push({label, key:found}); }
  });
  if(pills.length===0){ present.forEach(m=>pills.push({label:String(m).toUpperCase(), key:m})); }
  pills.forEach((p,idx)=>{
    const btn=document.createElement('button');
    btn.className='pill'+(idx===0?' active':''); 
    btn.setAttribute('role','tab');
    btn.setAttribute('aria-selected', idx===0 ? 'true' : 'false');
    btn.dataset.key=p.key;
    btn.textContent = p.label || titleCaseMetric(p.key);
    btn.addEventListener('click',()=>{
      currentVvipMetric=p.key;
      for(const child of container.children){ child.classList.remove('active'); child.setAttribute('aria-selected','false'); }
      btn.classList.add('active'); btn.setAttribute('aria-selected','true');
      renderVVIPTable();
    });
    container.appendChild(btn);
    if(idx===0){ currentVvipMetric=p.key; }
  });
}

function renderVVIPTable(){
  const mEl=document.getElementById('marketSelect'); const pEl=document.getElementById('periodSelect');
  const m = mEl? mEl.value : 'ALL';
  const p = pEl? pEl.value : 'Yesterday';
  const wrap = document.getElementById('vvipTableWrap');
  const thead = document.querySelector('#vvipTable thead');
  const tbody = document.querySelector('#vvipTable tbody');
  const notice = document.getElementById('vvipNotice');

  if(!Array.isArray(VVIP_DATA)){ 
    if(notice){ notice.style.display='block'; notice.textContent = 'No VVIP dataset found.'; }
    if(wrap){ wrap.style.display='none'; }
    return;
  }

  // Market + Period filter
  let rows = VVIP_DATA.filter(r => String(r['Market']||'').toUpperCase() === String(m).toUpperCase()
                               && String(r['Period']||'Yesterday').toLowerCase() === String(p).toLowerCase());

  // Metric filter (A): only selected metric rows; hide METRIC column
  const sel = currentVvipMetric;
  if (sel){
    rows = rows.filter(r => String(r['METRIC']||'').toLowerCase() === String(sel).toLowerCase());
  }

  
  // Always order by Current (VALUE) descending
  rows = rows.slice().sort((a,b)=>(Number(b['VALUE'])||0)-(Number(a['VALUE'])||0));

  // Precompute totals for Current and Previous
  const TOTALS = {
    VALUE: rows.reduce((s,r)=> s + (Number(r['VALUE'])||0), 0),
    PREVIOUS: rows.reduce((s,r)=> s + (Number(r['PREVIOUS'])||0), 0),
    DIFF_ABSOLUTE: rows.reduce((s,r)=> s + (Number(r['DIFF_ABSOLUTE'])||0), 0)
  };
  // Compute TOTAL Diff (%) = SUM(Diff) / Previous * 100, no decimals
  const __totalPrev = TOTALS.PREVIOUS || 0;
  const __sumDiff = TOTALS.DIFF_ABSOLUTE || 0;
  const TOTAL_DIFF_PERCENT = __totalPrev !== 0 ? Math.round((__sumDiff / Math.abs(__totalPrev)) * 100) : 0;

  if(rows.length === 0){
    if(wrap){ wrap.style.display='none'; }
    if(notice){
      notice.style.display='block';
      notice.textContent = `There is no VVIP activity in the selected period for ${m}.`;
    }
    return;
  }

  // Build column set and remove hidden ones (Market, Period, METRIC)
  const hidden = new Set(['Market','Period','METRIC']);
  const allKeys = Array.from(rows.reduce((s,r)=>{ Object.keys(r).forEach(k=>s.add(k)); return s; }, new Set()))
    .filter(k => !hidden.has(k));

  // Preferred order with PARTYID first
  const preferred = ['PARTYID','USERID','Source','REG_DATE','FTD_DATE','VALUE','PREVIOUS','DIFF_ABSOLUTE','DIFF_PERCENTAGE'];
  let cols = preferred.filter(k => allKeys.includes(k)).concat(allKeys.filter(k => !preferred.includes(k)));

  // Header label mapping (no METRIC shown)
  const DISPLAY_MAP = {
    'PARTYID':'PARTYID',
    'USERID':'USERID',
    'Source':'Source',
    'REG_DATE':'Registration Date',
    'FTD_DATE':'FTD Date',
    'VALUE':'Current',
    'PREVIOUS':'Previous',
    'DIFF_ABSOLUTE':'Diff',
    'DIFF_PERCENTAGE':'Diff (%)'
  };

  const CURRENCY_COLS = new Set(['VALUE','PREVIOUS','DIFF_ABSOLUTE']);
  thead.innerHTML = '<tr>' + 
cols.map((c,i)=>{
      const sticky = i===0 ? 'position:sticky;left:0;z-index:2; backdrop-filter: blur(6px);' : '';
      const label = DISPLAY_MAP[c] || c;
      return `<th style="text-align:left; padding:10px 12px; font-weight:800; border-bottom:1px solid var(--border); background:rgba(0,0,0,0.25); ${sticky}">${label}</th>`;
    }).join('') + '</tr>';

  let __body = rows.map(r=>{
    return '<tr>' + cols.map((c,i)=>{
      let val = r[c];
      if (val === null || val === undefined) val = '';
      if (typeof val === 'number') {
        const n = Number(val);
        if (!isNaN(n)) {
          if (Math.abs(n) >= 1e6) val = (n/1e6).toFixed(1)+'M';
          else if (Math.abs(n) >= 1e3) val = (n/1e3).toFixed(1)+'k';
          else val = n.toLocaleString();
        }
      }
// Prefix $ for currency columns (VALUE=Current, PREVIOUS=Previous)
if (CURRENCY_COLS.has(c) && val !== '' && String(r['METRIC'] || (typeof currentVvipMetric!=='undefined'?currentVvipMetric:'') || '').toUpperCase()!=='RMPS') {
  const s = String(val);
  val = s.startsWith('-') ? ('-$' + s.slice(1)) : ('$' + s);
}

// Format and color for Diff (%) column
if (c === 'DIFF_PERCENTAGE') {
  const num = Number(r[c]);
  if (!isNaN(num)) {
    const pct = Math.round(num);
    const cls = pct >= 0 ? 'positive' : 'negative';
    val = `<span class="${cls}">${pct}%</span>`;
  } else {
    // leave as NaN/blank with no percent sign
  }
}

      const sticky = i===0 ? 'position:sticky;left:0;z-index:1; background:rgba(0,0,0,0.08);' : '';
      return `<td style="padding:8px 12px; border-bottom:1px dashed rgba(255,255,255,0.18); ${sticky}">${val}</td>`;
    }).join('') + '</tr>';
  }).join('');
  function __renderRow(obj){
  const __labelKey = cols.includes('PARTYID') ? 'PARTYID' : cols[0];
  const __isTotal = String(obj[__labelKey]||'').toUpperCase()==='TOTAL';
  const __rowStyle = __isTotal ? 'font-weight:700;' : '';
  return `<tr style="${__rowStyle}">` + cols.map((c,i)=>{
      let val = obj[c];
      if (val === null || val === undefined) val = '';
      if (typeof val === 'number') {
        const n = Number(val);
        if (!isNaN(n)) {
          if (Math.abs(n) >= 1e6) val = (n/1e6).toFixed(1)+'M';
          else if (Math.abs(n) >= 1e3) val = (n/1e3).toFixed(1)+'k';
          else val = n.toLocaleString();
        }
      }
      // Prefix $ for currency columns (skip for RMPS)
      const metricForRow = String(obj['METRIC'] || (typeof currentVvipMetric!=='undefined'?currentVvipMetric:'' ) || '').toUpperCase();
      if (CURRENCY_COLS.has(c) && val !== '' && metricForRow !== 'RMPS') {
        const s = String(val);
        val = s.startsWith('-') ? ('-$' + s.slice(1)) : ('$' + s);
      }

// Format and color for Diff (%) column
if (c === 'DIFF_PERCENTAGE') {
  const num = Number(obj[c]);
  if (!isNaN(num)) {
    const pct = Math.round(num);
    const cls = pct >= 0 ? 'positive' : 'negative';
    val = `<span class="${cls}">${pct}%</span>`;
  } else {
    // leave as NaN/blank with no percent sign
  }
}

      const sticky = i===0 ? 'position:sticky;left:0;z-index:1; background:rgba(0,0,0,0.08);' : '';
      return `<td style="padding:8px 12px; border-bottom:1px dashed rgba(255,255,255,0.18); ${sticky}">${val}</td>`;
    }).join('') + '</tr>';
  }

  // Append TOTAL row
  const __totalRow = Object.fromEntries(cols.map(k=>[k,'']));
  __totalRow['VALUE'] = TOTALS.VALUE;
  __totalRow['PREVIOUS'] = TOTALS.PREVIOUS;
  __totalRow['DIFF_ABSOLUTE'] = TOTALS.DIFF_ABSOLUTE;
  __totalRow['DIFF_PERCENTAGE'] = TOTAL_DIFF_PERCENT;
  __totalRow['METRIC'] = (typeof currentVvipMetric!=='undefined'?currentVvipMetric:'');
  if (cols.includes('PARTYID')) __totalRow['PARTYID'] = 'TOTAL';
  else __totalRow[cols[0]] = 'TOTAL';
  const __totalHtml = __renderRow(__totalRow);
  tbody.innerHTML = __body + __totalHtml;


  if(notice){ notice.style.display='none'; }
  if(wrap){ wrap.style.display='block'; }
}
// Wire up


(function(){
  const oldRefresh = window.refreshAll;
  window.refreshAll = function(){
    try { if (typeof oldRefresh === 'function') oldRefresh(); } catch(e){}
    try { renderVVIPTable(); } catch(e){}
  };
  document.addEventListener('DOMContentLoaded', function(){
    try { initVvipPills(); } catch(e){}
    try { renderVVIPTable(); } catch(e){}
  });
})();

</script>

<script>
function _pickKey(obj, key){
  if(!obj) return null;
  var ks = Object.keys(obj||{});
  if(key in (obj||{})) return key;
  var lower = String(key).toLowerCase();
  for(var i=0;i<ks.length;i++){ if(String(ks[i]).toLowerCase()===lower) return ks[i]; }
  var norm = String(key).replace(/\s+/g,'').toUpperCase();
  for(var j=0;j<ks.length;j++){ if(String(ks[j]).replace(/\s+/g,'').toUpperCase()===norm) return ks[j]; }
  if(norm==='OTHERS'){ for(var k=0;k<ks.length;k++){ if(String(ks[k]).toUpperCase()==='OTHERS') return ks[k]; } }
  return null;
}
function _getBullets(mkt, per){
  var M = (window.AI_INSIGHTS||{});
  var mk = _pickKey(M, mkt);
  if(!mk) return [];
  var P = M[mk] || {};
  var pk = _pickKey(P, per);
  var obj = pk ? P[pk] : null;
  var arr = (obj && Array.isArray(obj.bullets)) ? obj.bullets : [];
  return arr.map(function(b){
    if (typeof b === 'string') {
      var m = (b.split('—')[1]||'').split(' ')[0] || 'Insight';
      return { metric: m, title: (m? m + ' anomaly' : 'Insight'), text: b };
    }
    var title = b.title || (b.metric ? (b.metric + ' anomaly') : 'Insight');
    return { metric: b.metric || 'Insight', title: title, text: String(b.text||'') };
  });
}
function _setInsight(boxId, title, text){
  var box = document.getElementById(boxId);
  if(!box) return;
  var lab = box.querySelector('.insight-label');
  var body = box.querySelector('.insight-text');
  if(!lab){ lab = document.createElement('div'); lab.className='insight-label'; box.prepend(lab); }
  if(!body){ body = document.createElement('div'); body.className='insight-text'; box.appendChild(body); }
  var s = (text==null||text===undefined||text==='') ? '[No insight]' : String(text).trim();
  var sl = s.toLowerCase().replace(/\s+/g,' ').trim();
  if (sl === 'no clear upside detected this period.' || sl === 'no: clear upside detected this period.') {
    s = 'No clear upside detected this period.';
  }
  lab.textContent = title || 'Insight';
  body.textContent = s;
}
function renderInsights(){
  var pEl=document.getElementById('periodSelect'), mEl=document.getElementById('marketSelect');
  if(!pEl||!mEl) return;
  var p=pEl.value, m=mEl.value;
  var bullets=_getBullets(m,p);
  _setInsight('insCritical',    bullets[0]?.title,    bullets[0]?.text);
  _setInsight('insTrend',       bullets[1]?.title,    bullets[1]?.text);
  _setInsight('insRetention',   bullets[2]?.title,    bullets[2]?.text);
  _setInsight('insOpportunity', bullets[3]?.title,    bullets[3]?.text);
}
document.addEventListener('DOMContentLoaded', function(){
  try{ renderInsights(); }catch(e){}
  var pEl=document.getElementById('periodSelect'), mEl=document.getElementById('marketSelect');
  if(pEl) pEl.addEventListener('change', function(){ try{ renderInsights(); }catch(e){} });
  if(mEl) mEl.addEventListener('change', function(){ try{ renderInsights(); }catch(e){} });
});
</script>


<script>
function _getExecutiveBullets(){
  var E = (window.AI_EXECUTIVE||{});
  var arr = Array.isArray(E.bullets) ? E.bullets : [];
  return arr.map(function(b){
    if (typeof b === 'string') {
      return { title: 'Executive insight', text: b };
    }
    return { title: b.title || (b.metric ? ('Executive — ' + b.metric) : 'Executive insight'), text: String(b.text||'') };
  }).slice(0,4);
}
function _setExec(boxId, title, text){
  var box=document.getElementById(boxId); if(!box) return;
  var lab = box.querySelector('.insight-label');
  var body = box.querySelector('.insight-text');
  if(!lab){ lab=document.createElement('div'); lab.className='insight-label'; box.prepend(lab); }
  if(!body){ body=document.createElement('div'); body.className='insight-text'; box.appendChild(body); }
  lab.textContent = title || 'Executive';
  body.textContent = (text && String(text).trim()) || '[No summary]';
}
function renderExecutive(){
  var bullets=_getExecutiveBullets();
  _setExec('exec1', bullets[0]?.title, bullets[0]?.text);
  _setExec('exec2', bullets[1]?.title, bullets[1]?.text);
  _setExec('exec3', bullets[2]?.title, bullets[2]?.text);
  _setExec('exec4', bullets[3]?.title, bullets[3]?.text);
}
document.addEventListener('DOMContentLoaded', function(){
  try{ renderExecutive(); }catch(e){}
});
</script>

</body></html>'''
    html = html.replace("__PERIOD_OPTIONS__", "".join(f"<option value='{p}'>{p}</option>" for p in period_opts))
    html = html.replace("__MARKET_OPTIONS__", "".join(f"<option value='{c}'>{c}</option>" for c in market_opts))
    html = html.replace("__MAIN_JSON__", main_json)
    html = html.replace("__SUMMARY_JSON__", summary_json)
    html = html.replace("__METRICS_JSON__", metrics_json)
    html = html.replace("__DIFF_JSON__", diff_json)
    html = html.replace("__GROUP_JSON__", group_json)
    html = html.replace("__GROUP_METRICS_JSON__", group_metrics_json)
    html = html.replace("__VVIP_JSON__", vvip_json)
    html = html.replace("__VVIP_CHURN_JSON__", churn_json)

    # Compute insights script tag dynamically (latest date)
    bname, fpath, content = _latest_insights_js()
    if bname:
        insights_tag = f"<script src=\"aiPerformanceText/{bname}\"></script>"
    elif content:  # shouldn't happen, but just in case
        insights_tag = "<script>" + content + "</script>"
    else:
        insights_tag = "<script>window.AI_INSIGHTS = {};</script>"
    html = html.replace("__INSIGHTS_TAG__", insights_tag)
    
    # Compute EXEC script tag dynamically (latest date)
    resE = _latest_exec_js()
    bnameE, fpathE, contentE = resE if isinstance(resE, (list, tuple)) and len(resE)==3 else (None, None, None)
    if bnameE:
        exec_tag = f"<script src=\"aiPerformanceText/{bnameE}\"></script>"
    elif contentE:
        exec_tag = "<script>" + contentE + "</script>"
    else:
        exec_tag = "<script>window.AI_EXECUTIVE = {date:null, bullets:[]};</script>"
    
    # Build VVIP tag from latest *_VVIP-insights.js (safe if missing)
    try:
        resV = _latest_vvip_js()
        bnameV, fpathV, contentV = resV if isinstance(resV, (list, tuple)) and len(resV)==3 else (None, None, None)
        if contentV:
            safeV = (contentV or '').replace('</script>', '</scr' + 'ipt>')
            vvip_tag = '<script>{}</script>'.format(safeV)
        else:
            vvip_tag = ''
    except Exception:
        vvip_tag = ''
    html = html.replace("__EXEC_TAG__", exec_tag)
    html = html.replace("__VVIP_TAG__", vvip_tag)
    return html

def main():
    main_df = load_main_data()
    summary_df = load_summary_data()
    diff_df = load_diff_data()
    group_df = safe_df(load_group_data())
    vvip_df = load_vvip_table_data()
    html = build_html(main_df, summary_df, diff_df, group_df, vvip_df)
    with open(OUTPUT, "w", encoding="utf-8") as f:
        f.write(html)
    print("Wrote:", OUTPUT)

if __name__ == "__main__":
    main()