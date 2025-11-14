DEBUG = False


import os, re, json, glob
from datetime import date
from typing import List, Dict, Any, Tuple
import pandas as pd
import math

# ---------------------------
# Configuration
# ---------------------------
BASE_DIR   = os.path.dirname(__file__) if "__file__" in globals() else "."
FILES_DIR  = os.path.join(BASE_DIR, "AI-Performance", "aiPerformanceFiles")
OUTPUT_DIR = os.path.join(BASE_DIR, "AI-Performance", "aiPerformanceText")
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(FILES_DIR,  exist_ok=True)

# Also search in mounted data if present (useful in notebooks / containers)
ALT_SEARCH_DIRS = [FILES_DIR]
try:
    if os.path.isdir("/mnt/data"):
        ALT_SEARCH_DIRS.append("/mnt/data")
except Exception:
    pass

VALID_MARKETS = ["ALL","AE","QA","SA","JO","BH","NZ","KW","EG","GCC","BET","Others","OTHERS"]
DRILLDOWN = {
    "ALL": ["AE","QA","SA","JO","BH","NZ","KW","EG","Others"],
    "GCC": ["AE","QA","SA","JO","BH","KW"],
    "BET": ["NZ"],
}

# ---------------------------
# Utils
# ---------------------------
def _fmt_num(x, decimals=0):
    try:
        v = float(x)
        if abs(v) >= 1000:
            return f"{v:,.0f}"
        return f"{v:,.{decimals}f}" if decimals>0 else f"{v:,.0f}"
    except Exception:
        return ""

def _pretty_metric(s: str) -> str:
    u = str(s or "").strip().upper()
    if u in {"DEPS","DEPOSITS","DEPOSIT"}: return "Deposits"
    if u == "FTDS": return "FTDs"
    if u == "GGR": return "GGR"
    if u == "NGR": return "NGR"
    if u == "MARGIN": return "Margin"
    if u == "RETENTION": return "Retention"
    if u == "STAKE": return "Stake"
    return str(s).strip()

def _cleanup_old_ai_text(dir_path: str, today: str):
    # files we auto-clean if their date is older than today
    patt_a1   = re.compile(r'^anomalies_(\d{4}-\d{2}-\d{2})\.json$', re.I)
    patt_js   = re.compile(r'^(\d{4}-\d{2}-\d{2})_main-insights\.(?:js|json)$', re.I)
    patt_exec = re.compile(r'^(\d{4}-\d{2}-\d{2})_main-executive\.(?:js|json)$', re.I)  # <-- define it here

    for fp in glob.glob(os.path.join(dir_path, '*')):
        base = os.path.basename(fp)
        m = patt_a1.match(base) or patt_js.match(base) or patt_exec.match(base)
        if not m:
            continue
        file_date = m.group(1)
        if file_date < today:
            try:
                os.remove(fp)
            except Exception:
                pass


# ---------------------------
# Loaders (DIFFERENCES as the main source)
# ---------------------------
def read_csvs_differences() -> pd.DataFrame:
    def normalize(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty: return pd.DataFrame()
        rename_map = {}
        for c in df.columns:
            lc = str(c).lower().strip()
            if lc in ["market","country"]: rename_map[c] = "Market"
            elif lc == "period": rename_map[c] = "Period"
            elif lc in ["metric","kpi"]: rename_map[c] = "METRIC"
            elif lc in ["value","current","current_value","amount"]: rename_map[c] = "VALUE"
            elif lc in ["previous","prev","prior"]: rename_map[c] = "Previous"
            elif lc in ["diff_absolute","diff_abs","delta"]: rename_map[c] = "DIFF_ABSOLUTE"
            elif lc in ["diff_percentage","diffpct","diff%","diff percent","diff_%"]: rename_map[c] = "DIFF_PERCENTAGE"
        out = df.rename(columns=rename_map)
        for col in ["Market","Period","METRIC","VALUE","Previous","DIFF_ABSOLUTE","DIFF_PERCENTAGE"]:
            if col not in out.columns: out[col] = None
        out["Market"]          = out["Market"].astype(str)
        out["Period"]          = out["Period"].astype(str)
        out["METRIC"]          = out["METRIC"].astype(str)
        out["VALUE"]           = pd.to_numeric(out["VALUE"], errors="coerce")
        out["Previous"]        = pd.to_numeric(out["Previous"], errors="coerce")
        out["DIFF_ABSOLUTE"]   = pd.to_numeric(out["DIFF_ABSOLUTE"], errors="coerce")
        out["DIFF_PERCENTAGE"] = pd.to_numeric(out["DIFF_PERCENTAGE"], errors="coerce")
        return out

    dfs = []
    for sd in ALT_SEARCH_DIRS:
        for f in glob.glob(os.path.join(sd, "*_ai-summary-differences_*.csv")):
            try:
                df = pd.read_csv(f)
            except Exception:
                try:
                    df = pd.read_csv(f, encoding="latin-1")
                except Exception:
                    df = pd.DataFrame()
            if not df.empty:
                dfs.append(normalize(df))
    if not dfs:
        return pd.DataFrame(columns=["Market","Period","METRIC","VALUE","Previous","DIFF_ABSOLUTE","DIFF_PERCENTAGE"])
    return pd.concat(dfs, ignore_index=True)

def read_groups_csv(market_code: str) -> pd.DataFrame:
    candidates = []
    for sd in ALT_SEARCH_DIRS:
        candidates.extend(glob.glob(os.path.join(sd, f"*ai-summary-groups_{market_code}.csv")))
    if not candidates:
        return pd.DataFrame(columns=["Period","Market","METRIC","FTD_Group","VALUE","PREVIOUS","DIFF_ABSOLUTE","DIFF_PERCENTAGE"])
    latest = sorted(candidates)[-1]
    try:
        df = pd.read_csv(latest)
    except Exception:
        df = pd.read_csv(latest, encoding="latin-1")

    rename_map = {
        "market":"Market","country":"Market","period":"Period","metric":"METRIC",
        "ftd_group":"FTD_Group","previous":"PREVIOUS","diff_absolute":"DIFF_ABSOLUTE",
        "diff_percentage":"DIFF_PERCENTAGE"
    }
    out = df.rename(columns={c: rename_map.get(c.lower(), c) for c in df.columns})
    for col in ["VALUE","PREVIOUS","DIFF_ABSOLUTE","DIFF_PERCENTAGE"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out

def markets_from_files(df: pd.DataFrame) -> List[str]:
    if df.empty:
        return [m for m in VALID_MARKETS if m != "OTHERS"]
    ms = sorted(df["Market"].dropna().astype(str).str.upper().unique().tolist())
    # Normalize "OTHERS" to "Others" in list
    ms = [("Others" if m == "OTHERS" else m) for m in ms]
    order = [m for m in ["ALL","GCC","BET","AE","QA","SA","JO","BH","KW","EG","NZ","Others"] if m in ms]
    extras = [m for m in ms if m not in order]
    return order + extras

def periods_from_df(df: pd.DataFrame) -> List[str]:
    if df.empty:
        return ["Yesterday","7 days","30 days","MTD"]
    return sorted(df["Period"].dropna().astype(str).unique().tolist())

# ---------------------------
# Anomaly detection helpers
# ---------------------------
def _select_anomalous_rows(df_sub: pd.DataFrame, top_n: int = 5, min_abs_pct: float = 25.0) -> pd.DataFrame:
    d = df_sub.copy()
    d = d[pd.notna(d["DIFF_PERCENTAGE"])]
    d["ABS__"] = d["DIFF_PERCENTAGE"].abs()
    d = d.sort_values(["ABS__","DIFF_ABSOLUTE"], ascending=[False, False])
    d = d[d["ABS__"] >= min_abs_pct]
    return d.head(top_n)

def _drilldown_ftd_group(metric: str, period: str, parent_market: str, sign: int) -> Tuple[str,str,Dict[str,float]]:
    sub_markets = DRILLDOWN.get(parent_market.upper(), [parent_market.upper()])
    best_score, best_row, best_market = None, None, None
    for m in sub_markets:
        gdf = read_groups_csv(m)
        if gdf.empty:
            continue
        block = gdf[(gdf["Period"].astype(str)==period) & (gdf["METRIC"].astype(str).str.upper()==metric.upper())].copy()
        if block.empty:
            continue
        block = block.sort_values("DIFF_PERCENTAGE", ascending=(sign<0))
        row   = block.iloc[0]
        score = abs(float(row.get("DIFF_PERCENTAGE") or 0))
        if (best_score is None) or (score > best_score):
            best_score, best_row, best_market = score, row, m
    if best_row is None:
        return (parent_market, None, {})
    fig = {
        "value":      float(best_row.get("VALUE", 0) or 0),
        "previous":   float(best_row.get("PREVIOUS", 0) or 0),
        "diff_abs":   float(best_row.get("DIFF_ABSOLUTE", 0) or 0),
        "diff_pct":   float(best_row.get("DIFF_PERCENTAGE", 0) or 0),
    }
    return (best_market, str(best_row.get("FTD_Group")), fig)

def _is_currency_metric(metric: str) -> bool:
    u = str(metric or "").upper().strip()
    return u in {"DEPS","DEPOSITS","DEPOSIT","STAKE","NGR","GGR"}

def _is_percent_metric(metric: str) -> bool:
    u = str(metric or "").upper().strip()
    return u in {"MARGIN","RETENTION"}

_SEGMENT_LONG = {
    "[FTDs]": "new players",
    "[1-3]": "the segment of users that have made their first deposit between one and three months ago",
    "[4-12]": "the segment of users that have made their first deposit between four and twelve months ago",
    "[13-24]": "the segment of users that have made their first deposit between one and two years ago",
    "[> 25]": "the segment of users that have made their first deposit more than two years ago",
}

def _normalize_segment_tag(tag: str) -> str:
    if not tag: return ""
    t = str(tag).strip()
    t = t.replace("[ >25]", "[> 25]").replace("[>25]", "[> 25]")
    if not t.startswith("["): t = f"[{t}]"
    return t

def _segment_phrase(ftd_group: str) -> str:
    tg = _normalize_segment_tag(ftd_group)
    if tg in _SEGMENT_LONG:
        return _SEGMENT_LONG[tg]
    return "the overall mix of segments"

def _fmt_with_unit(metric: str, value) -> str:
    if value is None or (isinstance(value, float) and (pd.isna(value) or not math.isfinite(value))):
        return ""
    if _is_currency_metric(metric):
        return f"${_fmt_num(value)}"
    if _is_percent_metric(metric):
        return f"{_fmt_num(value)}%"
    return _fmt_num(value)

def _location_prefix(metric: str, area_market: str) -> str:
    mname = _pretty_metric(metric)
    am = str(area_market or "").strip()
    if am.lower() == "general":
        return f"General {mname}"
    if am.upper() in {"OTHERS","OTHERS*"} or am == "Others":
        return f"In the other countries, the {mname}"
    return f"In {am}, the {mname}"

def _pct_to_text(prev, val, diff_abs, raw_pct) -> Tuple[str, bool]:
    try:
        pct = float(raw_pct) if raw_pct is not None else None
    except Exception:
        pct = None
    if pct is None or not math.isfinite(pct):
        if (prev is not None) and float(prev) == 0 and (val is not None) and float(val) != 0:
            return "∞%", (diff_abs or 0) > 0
        return "0%", (diff_abs or 0) > 0
    return f"{int(round(abs(pct)))}%", pct >= 0

def _compose_change_phrase(change_word: str, diff_abs_txt: str, pct_txt: str) -> str:
    diff_abs_txt = (diff_abs_txt or "").strip()
    if diff_abs_txt:
        return f"{change_word} {diff_abs_txt} ({pct_txt})"
    # no absolute value -> speak only in percentages
    return f"{change_word} {pct_txt}"

def _build_bullet(market: str,
                  period: str,
                  metric: str,
                  row: Dict[str, Any],
                  area_market: str,
                  ftd_group: str,
                  figs: Dict[str, float]) -> Dict[str, Any]:
    # Prefer drilled figures when available
    val      = figs.get("value",        row.get("VALUE"))
    prev     = figs.get("previous",     row.get("Previous"))
    diff_abs = figs.get("diff_abs",     row.get("DIFF_ABSOLUTE"))
    raw_pct  = figs.get("diff_pct",     row.get("DIFF_PERCENTAGE"))

    pct_txt, increased = _pct_to_text(prev, val, diff_abs, raw_pct)
    change_word = "increased" if increased else "decreased"

    title = f"{_pretty_metric(metric)} anomaly"
    diff_abs_txt = _fmt_with_unit(metric, diff_abs)
    total_txt    = _fmt_with_unit(metric, val)
    seg_phrase   = _segment_phrase(ftd_group) if ftd_group else "the overall mix of segments"
    start        = _location_prefix(metric, area_market)

    change_phrase = _compose_change_phrase(change_word, diff_abs_txt, pct_txt)

    # Build sentence avoiding empty ", to a total of ,"
    parts = [f"{start} {change_phrase}"]
    if total_txt:
        parts.append(f"to a total of {total_txt}")
    parts.append(f"mainly driven by {seg_phrase}.")
    text = ", ".join(parts[:-1]) + ", " + parts[-1]

    return {"metric": _pretty_metric(metric), "title": title, "text": text}

def _build_filler_bullet(row: pd.Series, market: str, period: str, mode: str) -> Dict[str, Any]:
    metric = str(row["METRIC"])
    val    = row.get("VALUE")
    prev   = row.get("Previous")
    diff_a = row.get("DIFF_ABSOLUTE")
    rawpct = row.get("DIFF_PERCENTAGE")
    pct_txt, increased = _pct_to_text(prev, val, diff_a, rawpct)
    mname  = _pretty_metric(metric)
    total_txt = _fmt_with_unit(metric, val)
    diff_txt  = _fmt_with_unit(metric, diff_a)
    area = "General" if market.upper() in DRILLDOWN else market
    start = _location_prefix(metric, area)
    if mode == "stability":
        title = f"{mname} stability"
        base  = f"{start} remained relatively stable"
        if total_txt:
            base += f" at {total_txt}"
        text  = f"{base} ({pct_txt} vs previous), with no major segment shifts."
    else:
        title = f"{mname} trend"
        trend_word = "increase" if increased else "decrease"
        base  = f"{start} showed a mild {trend_word}"
        if diff_txt:
            base += f" of {diff_txt}"
        text  = f"{base} ({pct_txt})"
        if total_txt:
            text += f" to {total_txt}"
        text += ", with the overall mix of segments."
    return {"metric": mname, "title": title, "text": text}

def _ensure_four_bullets(df_diff: pd.DataFrame,
                         bullets: List[Dict[str, Any]],
                         market: str, period: str) -> List[Dict[str, Any]]:
    if len(bullets) >= 4:
        return bullets[:4]

    sub = df_diff[(df_diff["Market"].str.upper()==market.upper()) & (df_diff["Period"].astype(str)==period)].copy()
    if sub.empty:
        while len(bullets) < 4:
            bullets.append({"metric":"Retention","title":"Retention stability","text":"General Retention remained relatively stable, with no major segment shifts."})
        return bullets[:4]

    used_metrics = {b.get("metric") for b in bullets}

    # Prefer adding a Retention note if missing
    ret = sub[sub["METRIC"].astype(str).str.upper()=="RETENTION"]
    if not ret.empty and "Retention" not in used_metrics and len(bullets) < 4:
        r = ret.iloc[0]
        mode = "stability" if abs(float(r.get("DIFF_PERCENTAGE") or 0)) < 10 else "trend"
        bullets.append(_build_filler_bullet(r, market, period, mode))
        used_metrics.add("Retention")

    # Fill remaining with smallest |%| first (stability), then larger as trend
    sub["ABS_"] = sub["DIFF_PERCENTAGE"].abs()
    for _, r in sub.sort_values("ABS_").iterrows():
        if len(bullets) >= 4: break
        mname = _pretty_metric(r["METRIC"])
        if mname in used_metrics: 
            continue
        abs_pct = abs(float(r.get("DIFF_PERCENTAGE") or 0))
        mode = "stability" if abs_pct < 10 else "trend"
        b = _build_filler_bullet(r, market, period, mode)
        bullets.append(b)
        used_metrics.add(mname)

    while len(bullets) < 4:
        bullets.append({"metric":"Retention","title":"Retention stability","text":"General Retention remained relatively stable, with no major segment shifts."})
    return bullets[:4]

def detect_anomalies(df_diff: pd.DataFrame,
                     market: str,
                     period: str,
                     top_n: int = 5,
                     min_abs_pct: float = 25.0) -> List[Dict[str, Any]]:
    sub = df_diff[(df_diff["Market"].str.upper()==market.upper()) & (df_diff["Period"].astype(str)==period)].copy()
    if sub.empty:
        return []
    picks = _select_anomalous_rows(sub, top_n=top_n, min_abs_pct=min_abs_pct)
    bullets: List[Dict[str, Any]] = []
    for _, r in picks.iterrows():
        metric = str(r["METRIC"])
        sign   = 1 if float(r["DIFF_PERCENTAGE"]) >= 0 else -1
        area_market, ftd_group, figs = _drilldown_ftd_group(metric, period, market, sign)

        # Composite and no group row -> use "General ..." with parent figures
        if market.upper() in DRILLDOWN and area_market.upper() == market.upper():
            area_market = "General"
            ftd_group = None
            figs = {}
        use_market = area_market if market.upper() in DRILLDOWN else market
        bullets.append(_build_bullet(market, period, metric, r, use_market, ftd_group, figs))

    return _ensure_four_bullets(df_diff, bullets, market, period)

# ---------------------------
# Executive summary (refined phrasing + CSV segment attribution + priorities)
# ---------------------------
def _exec_period_title_label(p: str) -> str:
    p = (p or "").strip()
    if p == "7 days":
        return "during the past 7 days"
    if p == "30 days":
        return "during the past 30 days"
    if p == "Yesterday":
        return "yesterday"
    return p  # e.g., "MTD"

def _exec_period_text_label(p: str) -> str:
    # Same phrasing used in the descriptive sentence
    return _exec_period_title_label(p)

def _metric_priority_name(metric: str) -> str:
    m = (metric or "").strip()
    mu = m.upper()
    if mu in {"DEPS","DEPOSIT","DEPOSITS"}:
        return "DEPS"
    if mu == "NGR":
        return "NGR"
    if mu in {"RETENTION","RET"}:
        return "RETENTION"
    return m

def _metric_priority_rank(metric: str) -> int:
    order = {"DEPS":0, "NGR":1, "RETENTION":2}
    return order.get(_metric_priority_name(metric).upper(), 99)

def _find_segment_driver_csv(market: str, metric: str, period: str, base_dir: str = FILES_DIR) -> str:
    """(Deprecated) Old file-pattern based finder kept for backward compat if needed elsewhere."""
    return ""

def _humanize_number(n: float) -> str:
    """Return a compact human-readable number like 1.2k, 3.4M with sign."""
    try:
        sign = "-" if n < 0 else ""
        n = abs(float(n))
    except Exception:
        return ""
    units = [("", 1), ("k", 1_000), ("M", 1_000_000), ("B", 1_000_000_000)]
    for suffix, div in reversed(units):
        if n >= div:
            val = n / div
            if val >= 100:
                fmt = f"{val:.0f}"
            elif val >= 10:
                fmt = f"{val:.1f}"
            else:
                fmt = f"{val:.2f}"
            return f"{sign}{fmt}{suffix}"
    return f"{sign}{n:.0f}"

def _exec_find_segment_driver(market: str, metric: str, period: str):
    """
    Find the FTD group with the largest |DIFF_PERCENTAGE| for the given (market, metric, period).
    - If `market` is composite via DRILLDOWN (e.g., ALL/GCC/BET), scan its children and return best child + segment.
    Returns a tuple: (driver_text, abs_text)
      driver_text -> '[1-3] (+18%)'  or  '[1-3] (+18%) in AE'
      abs_text    -> ', +120k' (empty string if not available)
    """
    import pandas as pd
    metric_u = str(metric or "").upper()
    root_market = str(market or "").upper()
    markets = DRILLDOWN.get(root_market, [root_market])
    best = None  # tuple: (abs_pct, pct, seg, where_market, abs_amount)
    for mk in markets:
        gdf = read_groups_csv(mk)
        if gdf is None or getattr(gdf, "empty", True):
            continue
        # Normalize columns in case of case differences
        cols = {c.lower(): c for c in gdf.columns}
        # We expect at least: Period, METRIC, DIFF_PERCENTAGE, FTD_Group
        pcol = cols.get("period", "Period")
        mcol = cols.get("metric", "METRIC")
        dpp = cols.get("diff_percentage", "DIFF_PERCENTAGE")
        sgc = cols.get("ftd_group", "FTD_Group")
        block = gdf[
            (gdf[pcol].astype(str) == period) &
            (gdf[mcol].astype(str).str.upper() == metric_u)
        ].copy()
        if block.empty or dpp not in block or sgc not in block:
            continue
        block["__ABS_PCT__"] = block[dpp].astype(float).abs()
        # Prefer absolute amount fields if available for contribution
        amount_fields = [c for c in block.columns if c.lower() in {"diff_abs","diff_value","diff_amount","diff","amount_diff"}]
        if amount_fields:
            af = amount_fields[0]
        else:
            af = None
        row = block.sort_values("__ABS_PCT__", ascending=False).iloc[0]
        seg = str(row.get(sgc) or "").strip()
        try:
            pct = float(row.get(dpp) or 0.0)
        except Exception:
            pct = 0.0
        abs_amt = None
        if af is not None:
            try:
                abs_amt = float(row.get(af))
            except Exception:
                abs_amt = None
        cand = (abs(pct), pct, seg, mk, abs_amt)
        if (best is None) or (cand[0] > best[0]):
            best = cand
    if not best:
        return ("", "")
    _, pct, seg, where_m, abs_amt = best
    # Normalize tag if helper exists
    try:
        seg = _normalize_segment_tag(seg) if seg else seg
    except Exception:
        pass
    try:
        pct_txt = f"{pct:+.0f}%"
    except Exception:
        pct_txt = ""
    is_composite = root_market in DRILLDOWN
    driver_txt = f"{seg} ({pct_txt})"
    if is_composite and where_m and where_m.upper() != root_market:
        driver_txt += f" in {where_m}"
    # Absolute contribution text
    abs_txt = ""
    if abs_amt is not None:
        h = _humanize_number(abs_amt)
        if h:
            # Keep sign inside the number (e.g., +120k) and prefix with comma for readability
            if not h.startswith("-"):
                h = f"+{h}"
            abs_txt = f", {h}"
    return (driver_txt, abs_txt)

def _exec_segment_driver_phrase(market: str, metric: str, period: str) -> str:
    drv_txt, _abs_txt = _exec_find_segment_driver(market, metric, period)
    if not drv_txt:
        return ""
    seg = drv_txt.split(" (", 1)[0].strip()
    in_suffix = ""
    if " in " in drv_txt:
        in_suffix = " " + drv_txt.split(" in ", 1)[1].strip()
    try:
        phrase = _segment_phrase(seg)
    except Exception:
        phrase = "the overall mix of segments"
    tail = f"mainly driven by {phrase}"
    if in_suffix:
        tail += f"{in_suffix}"
    return tail + "."

def _build_global_bullet(row: pd.Series) -> Dict[str, Any]:
    metric = _pretty_metric(row.get("METRIC"))
    market = str(row.get("Market") or "").strip()
    period = str(row.get("Period") or "").strip()

    val    = row.get("VALUE")
    prev   = row.get("Previous")
    diff_a = row.get("DIFF_ABSOLUTE")
    rawpct = row.get("DIFF_PERCENTAGE")

    pct_txt, increased = _pct_to_text(prev, val, diff_a, rawpct)
    change_word = "increased" if increased else "decreased"
    diff_abs_txt = _fmt_with_unit(metric, diff_a)
    total_txt    = _fmt_with_unit(metric, val)
    change_phrase = _compose_change_phrase(change_word, diff_abs_txt, pct_txt)

    period_title = _exec_period_title_label(period)
    title = f"{metric} in {market} {change_word} {period_title}!" if (market and period_title) else f"{metric} {change_word}!"

    base = f"{metric} {change_phrase}"
    if market:
        base += f" in {market}"
    period_text = _exec_period_text_label(period)
    if period_text:
        base += f" {period_text}"
    trailing = []
    if total_txt:
        trailing.append(f"to {total_txt}")
    text = base + (", " + ", ".join(trailing) if trailing else "")
    if not text.endswith("."):
        text += "."
    try:
        period = row.get("Period", period if "period" in locals() else None)
    except Exception:
        period = row.get("Period")
    try:
        market = row.get("MARKET", market if "market" in locals() else row.get("Market", market))
    except Exception:
        market = row.get("MARKET") or row.get("Market")
    tail_phrase = _exec_segment_driver_phrase(market, row.get("METRIC"), period)
    if tail_phrase:
        _tp = tail_phrase.strip().rstrip(".")
        if _tp.lower().startswith("this is mainly driven by "):
            _tp = _tp[26:]
            _tp = "mainly driven by " + _tp
        if _tp.lower().startswith("mainly driven by the segment of "):
            _tp = "mainly driven by " + _tp[33:]
        if ", to " in text and text.endswith("."):
            text = text.rstrip(".").replace(", to ", " to ")
            text += " – " + _tp + "."
        else:
            text += " – " + _tp + "."
    else:
        if ", to " in text and text.endswith("."):
            text = text.rstrip(".").replace(", to ", " to ")
            text += " – This appears driven by the overall mix of segments."
        else:
            text += " – This appears driven by the overall mix of segments."

    if isinstance(title, str):
        title = title.split(" • ", 1)[0].strip()
        if not title.endswith("!"):
            title += "!"

    return {"metric": metric, "title": title, "text": text}

def _pick_top_k_global(df_all: pd.DataFrame, k: int = 4, min_abs_pct: float = 25.0) -> List[Dict[str, Any]]:
    if df_all is None or df_all.empty:
        return [{"metric":"Performance","title":"Performance update","text":"No data available."} for _ in range(k)]

    d = df_all.copy()
    if "DIFF_PERCENTAGE" not in d.columns:
        return [{"metric":"Performance","title":"Performance update","text":"No change data found."} for _ in range(k)]
    d = d[pd.notna(d["DIFF_PERCENTAGE"])].copy()
    if d.empty:
        return [{"metric":"Performance","title":"Performance update","text":"No valid percentage data."} for _ in range(k)]

    if "DIFF_ABSOLUTE" not in d.columns:
        d["DIFF_ABSOLUTE"] = pd.to_numeric(d.get("DIFF_ABSOLUTE", 0), errors="coerce")
    d["ABS__"] = d["DIFF_PERCENTAGE"].abs()
    d = d[d["ABS__"] >= float(min_abs_pct)].copy()
    if d.empty:
        return [{"metric":"Performance","title":"Performance update","text":"No strong changes found."} for _ in range(k)]

    d["MARKET_UP"] = d["Market"].astype(str).str.upper()
    priority_markets = {"AE","QA","SA","NZ"}
    d["PRIORITY_FLAG"] = d["MARKET_UP"].isin(priority_markets).astype(int)
    d["METRIC_RANK"] = d["METRIC"].map(_metric_priority_rank)

    d = d.sort_values(["METRIC_RANK","PRIORITY_FLAG","ABS__","DIFF_ABSOLUTE"], ascending=[True, False, False, False])

    period_order = ['Yesterday', '7 days', '30 days', 'MTD']
    picks, metric_counts = [], {}

    def can_take(metric_raw: str) -> bool:
        m = _pretty_metric(metric_raw)
        return metric_counts.get(m, 0) < 2

    market_counts = {}
    for per in period_order:
        sub = d[d["Period"].astype(str) == per]
        if sub.empty:
            continue
        chosen = None
        for _, r in sub.iterrows():
            if can_take(r["METRIC"]):
                chosen = r
                break
        if chosen is not None:
            mkt_key = (str(chosen.get("Market")) or str(chosen.get("MARKET")) or "").strip()
            if market_counts.get(mkt_key, 0) < 2:
                picks.append(chosen)
                market_counts[mkt_key] = market_counts.get(mkt_key, 0) + 1
            m = _pretty_metric(chosen["METRIC"])
            metric_counts[m] = metric_counts.get(m, 0) + 1
        if len(picks) >= k:
            break

    if len(picks) < k:
        chosen_keys = {(str(r.get("Market")), str(r.get("METRIC")), str(r.get("Period")), float(r.get("VALUE")) if pd.notna(r.get("VALUE")) else None) for r in picks}
        for _, r in d.iterrows():
            key = (str(r.get("Market")), str(r.get("METRIC")), str(r.get("Period")), float(r.get("VALUE")) if pd.notna(r.get("VALUE")) else None)
            if key in chosen_keys or not can_take(r["METRIC"]):
                continue
            mkt_key = (str(r.get("Market")) or str(r.get("MARKET")) or "").strip()
            if market_counts.get(mkt_key, 0) < 2:
                picks.append(r)
                market_counts[mkt_key] = market_counts.get(mkt_key, 0) + 1
            m = _pretty_metric(r["METRIC"])
            metric_counts[m] = metric_counts.get(m, 0) + 1
            chosen_keys.add(key)
            if len(picks) >= k:
                break

    return [_build_global_bullet(r) for r in picks[:k]]

# ---------------------------
# VVIP insights over *_ai-VVIPs-table_XX.csv files
# ---------------------------
def _vvip_metric_text(m: str) -> str:
    u = (m or "").strip().upper()
    if u in {"DEPS","DEPOSIT","DEPOSITS"}: return "deposits"
    if u == "NGR": return "NGR"
    if u == "GGR": return "GGR"
    if u == "STAKE": return "stake"
    if u == "FTDS": return "FTDs"
    return (m or "").strip()

def _vvip_period_text(p: str) -> str:
    p = (p or "").strip()
    if p == "Yesterday": return "yesterday"
    if p in {"7 days","30 days","MTD"}: return f"in the last {p}"
    return f"in period '{p}'"

def _vvip_change_word(pct: float) -> str:
    try:
        return "increased" if float(pct) >= 0 else "decreased"
    except Exception:
        return "changed"

def _vvip_qualifier(pct: float) -> str:
    try:
        ap = abs(float(pct))
    except Exception:
        ap = 0.0
    if ap >= 75: return "dramatically"
    if ap >= 50: return "considerably"
    if ap >= 25: return "notably"
    return ""


def _vvip_build_sentence(row: pd.Series) -> str:
    user = str(row.get("USERID"))
    pid  = str(int(row.get("PARTYID"))) if pd.notna(row.get("PARTYID")) else ""
    period = str(row.get("Period"))
    metric = str(row.get("METRIC"))
    val    = row.get("VALUE")
    prev   = row.get("PREVIOUS", row.get("Previous"))
    diff_a = row.get("DIFF_ABSOLUTE")
    pct    = row.get("DIFF_PERCENTAGE")

    # compute absolute change if missing
    try:
        if (diff_a is None or (isinstance(diff_a, float) and math.isnan(diff_a))) and            (val is not None) and (prev is not None):
            diff_a = float(val) - float(prev)
    except Exception:
        diff_a = None

    chg    = _vvip_change_word(pct)
    metric_txt = _vvip_metric_text(metric)
    time_txt   = _vvip_period_text(period)

    # Special case: deposits drop to zero -> "has stopped depositing ... from a previous total of $X"
    metric_u = str(metric).upper()
    if metric_u in {"DEPOSIT","DEPOSITS"} and val is not None:
        try:
            if float(val) == 0 and (prev is not None) and (float(prev) > 0):
                prev_txt = _fmt_with_unit(metric, prev)
                return f"Player {user} ({pid}) has stopped depositing {time_txt} from a previous total of {prev_txt}."
        except Exception:
            pass

    # Default sentence with amount + % + total and NO qualifier
    abs_txt   = _fmt_with_unit(metric, abs(diff_a)) if diff_a is not None else ""
    pct_txt   = ""
    try:
        if pct is not None and not pd.isna(pct):
            pct_txt = f"{int(round(abs(float(pct))))}%"
    except Exception:
        pass
    total_txt = _fmt_with_unit(metric, val) if val is not None else ""

    change_bits = []
    if abs_txt:
        change_bits.append(abs_txt)
    if pct_txt:
        change_bits.append(f"({pct_txt})")
    change_phrase = (" by " + " ".join(change_bits)) if change_bits else ""
    tail_total = f" to a total of {total_txt}" if total_txt else ""

    return f"Player {user} ({pid}) has {chg} their {metric_txt} {time_txt}{change_phrase}{tail_total}."

def _load_vvip_frames() -> Dict[str, pd.DataFrame]:
    markets = {}
    patterns = [os.path.join(sd, "*_ai-VVIPs-table_*.csv") for sd in ALT_SEARCH_DIRS]
    files = []
    for patt in patterns:
        files.extend(glob.glob(patt))
    for p in files:
        m = re.search(r"_ai-VVIPs-table_([A-Z]+)\.csv$", os.path.basename(p))
        if not m:
            continue
        market = m.group(1).upper()
        try:
            df = pd.read_csv(p)
        except Exception:
            df = pd.read_csv(p, encoding="latin-1")
        markets[market] = df
    return markets

def _vvip_metric_rank(m) -> int:
    u = str(m).upper()
    if u in {"DEPS","DEPOSIT","DEPOSITS"}: return 0
    if u == "NGR": return 1
    if u == "STAKE": return 2
    return 9

def _pick_vvip_insights_for_market(df: pd.DataFrame, limit: int = 4) -> list:
    if df is None or df.empty:
        return []
    d = df.copy()
    for col in ["DIFF_PERCENTAGE","VALUE","PREVIOUS"]:
        if col in d.columns:
            d[col] = pd.to_numeric(d[col], errors="coerce")
    d = d[pd.notna(d["DIFF_PERCENTAGE"])].copy()
    if d.empty:
        return []
    period_order = ['Yesterday', '7 days', '30 days', 'MTD']
    d["__PERIOD_RANK__"] = d["Period"].apply(lambda x: period_order.index(str(x)) if str(x) in period_order else 99)
    d["__METRIC_RANK__"] = d["METRIC"].apply(_vvip_metric_rank)
    d["__ABS_PCT__"]     = d["DIFF_PERCENTAGE"].abs()
    d = d.sort_values(["__PERIOD_RANK__","__METRIC_RANK__","__ABS_PCT__"], ascending=[True, True, False])
    # Prefer Yesterday NGR (non-zero) rows first
    try:
        dy_mask = d["Period"].astype(str).str.lower() == "yesterday"
        ngr_mask = d["METRIC"].astype(str).str.upper() == "NGR"
        nz_mask = d["VALUE"].fillna(0) != 0
        dy_pref = d[dy_mask & ngr_mask & nz_mask]
        d_rest  = d[~(dy_mask & ngr_mask & nz_mask)]
        if not dy_pref.empty:
            d = pd.concat([dy_pref, d_rest], ignore_index=True)
    except Exception:
        pass
    insights, seen_players = [], set()
    # Reactivation sentence (RMP): choose a period with positive reactivations, prefer 30 days > 7 days > MTD
    try:
        r = df.copy()
        r["METRIC"] = r["METRIC"].astype(str)
        r["Period"] = r["Period"].astype(str)
        periods_pref = ["30 days","7 days","MTD"]
        added_reactivation = False
        for per in periods_pref:
            rp = r[(r["METRIC"].str.upper()=="RMP") & (r["Period"].str.lower()==per.lower())]
            if not rp.empty:
                # Player-level 0->1 or aggregated DIFF_ABSOLUTE>0
                player_cnt = int(((rp["PREVIOUS"].fillna(0) == 0) & (rp["VALUE"].fillna(0) > 0)).sum())
                agg_cnt = int(rp.get("DIFF_ABSOLUTE", pd.Series([0]*len(rp))).fillna(0).clip(lower=0).sum())
                count = max(player_cnt, agg_cnt)
                if count > 0:
                    ttxt = _vvip_period_text(per).capitalize()
                    insights.append(f"{ttxt}, {count} VVIP player{'s' if count != 1 else ''} were reactivated.")
                    added_reactivation = True
                    break
    except Exception:
        pass
    for _, r in d.iterrows():
        key = (r.get("PARTYID"), str(r.get("USERID")))
        if key in seen_players:
            continue
        insights.append(_vvip_build_sentence(r))
        seen_players.add(key)
        if len(insights) >= limit:
            break
    return insights

def write_vvip_insights_js(today: str) -> str:
    frames = _load_vvip_frames()
    out = {}
    for mkt, df in frames.items():
        out[mkt] = _pick_vvip_insights_for_market(df, limit=4)

    # delete stale VVIP insight files
    try:
        import glob, os
        for stale in glob.glob(os.path.join(OUTPUT_DIR, "*_VVIP-insights*.js")):
            try:
                os.remove(stale)
            except Exception:
                pass
    except Exception:
        pass

    out_path = os.path.join(OUTPUT_DIR, f"{today}_VVIP-insights.js")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("window.VVIP_INSIGHTS = ")
        json.dump(out, f, ensure_ascii=False, indent=2)
        f.write(";")
    return out_path

# ---------------------------
# Runner
# ---------------------------

# === VVIP insights helpers (inserted) ===
def _vvip_reactivation_counts(df):
    """
    Count reactivated VVIPs per period.
    Reactivation = METRIC == 'RMP' with PREVIOUS == 0 and VALUE == 1.
    Returns: dict(period -> distinct players count)
    """
    import pandas as pd
    if df is None or df.empty:
        return {}
    d = df.copy()
    for c in ["VALUE","PREVIOUS","Value","Previous"]:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")
    d["__MET_UP__"] = d["METRIC"].astype(str).str.upper()
    prev_col = "PREVIOUS" if "PREVIOUS" in d.columns else ("Previous" if "Previous" in d.columns else None)
    val_col  = "VALUE"    if "VALUE"    in d.columns else ("Value"    if "Value"    in d.columns else None)
    if prev_col is None or val_col is None or "Period" not in d.columns:
        return {}
    mask = (d["__MET_UP__"] == "RMP") & (d[prev_col].fillna(1) == 0) & (d[val_col].fillna(0) == 1)
    if not mask.any():
        return {}
    sub = d[mask].copy()
    def _uid(row):
        pid = row.get("PARTYID")
        if pd.notna(pid):
            try:
                return int(pid)
            except Exception:
                return str(pid)
        return str(row.get("USERID"))
    sub["__UID__"] = sub.apply(_uid, axis=1)
    return sub.groupby(sub["Period"].astype(str))["__UID__"].nunique().to_dict()

def _vvip_reactivation_sentence(period: str, count: int) -> str:
    if period == "Yesterday":
        return ""
    if period == "7 days":
        return f"In the last 7 days, {count} VVIP players were reactivated."
    if period == "30 days":
        return f"In the last 30 days, {count} VVIP players were reactivated."
    if period == "MTD":
        return f"Month-to-date, {count} VVIP players were reactivated."
    return f"In period '{period}', {count} VVIP players were reactivated."

def _pick_vvip_insights_for_market(df: pd.DataFrame, limit: int = 4) -> list:
    """
    Build exactly one insight per Period (ordered: 7 days, 30 days, Yesterday, MTD).
    Avoid repeating the same metric >2 times and the same player >2 times across the 4 insights.
    Prefer a 'reactivation' insight (RMP: 0 -> 1) for non-Yesterday periods.
    """
    import pandas as pd
    if df is None or df.empty:
        return []
    d = df.copy()
    for col in ["DIFF_PERCENTAGE","VALUE","PREVIOUS"]:
        if col in d.columns:
            d[col] = pd.to_numeric(d[col], errors="coerce")
    if "Period" not in d.columns or "METRIC" not in d.columns:
        return []
    reac = _vvip_reactivation_counts(d)
    period_order = ['Yesterday', '7 days', '30 days', 'MTD']
    def _p_rank(x):
        try:
            return period_order.index(str(x))
        except ValueError:
            return 99
    d["__PERIOD_RANK__"] = d["Period"].apply(_p_rank)
    d["__METRIC_RANK__"] = d["METRIC"].apply(_vvip_metric_rank)
    d["__ABS_PCT__"]     = d["DIFF_PERCENTAGE"].abs()
    d = d[pd.notna(d["DIFF_PERCENTAGE"])].sort_values(["__PERIOD_RANK__","__METRIC_RANK__","__ABS_PCT__"], ascending=[True,True,False])
    insights = []
    metric_counts = {}
    player_counts = {}
    def _uid(row):
        pid = row.get("PARTYID")
        if pd.notna(pid):
            try:
                return int(pid)
            except Exception:
                return str(pid)
        return str(row.get("USERID"))
    for per in period_order:
        rc = int(reac.get(per, 0)) if isinstance(reac, dict) else 0
        if per != "Yesterday" and rc > 0:
            insights.append(_vvip_reactivation_sentence(per, rc))
            continue
        block = d[d["Period"].astype(str) == per].copy()
        picked_sentence = None
        if not block.empty:
            for _, r in block.iterrows():
                met = str(r["METRIC"]).strip().upper()
                mname = _vvip_metric_text(met)
                uid = _uid(r)
                if metric_counts.get(mname, 0) >= 2:
                    continue
                if player_counts.get(uid, 0) >= 2:
                    continue
                picked_sentence = _vvip_build_sentence(r)
                metric_counts[mname] = metric_counts.get(mname, 0) + 1
                player_counts[uid] = player_counts.get(uid, 0) + 1
                break
        if picked_sentence is None:
            if per == "Yesterday":
                picked_sentence = "No notable VVIP changes yesterday."
            elif per == "7 days":
                picked_sentence = "No notable VVIP changes in the last 7 days."
            elif per == "30 days":
                picked_sentence = "No notable VVIP changes in the last 30 days."
            elif per == "MTD":
                picked_sentence = "No notable VVIP changes month-to-date."
            else:
                picked_sentence = f"No notable VVIP changes in period '{per}'."
        insights.append(picked_sentence)
    return insights[:4]
# === end VVIP helpers (inserted) ===
def run():
    df = read_csvs_differences()
    # Normalize "OTHERS" to "Others" in data for consistency
    if not df.empty:
        df["Market"] = df["Market"].str.replace("^OTHERS$", "Others", regex=True)
    mkts    = markets_from_files(df)
    periods = periods_from_df(df)
    today   = date.today().strftime("%Y-%m-%d")
    _cleanup_old_ai_text(OUTPUT_DIR, today)

    # Executive insights FIRST (global, cross-market)
    exec_bullets = _pick_top_k_global(df, k=4, min_abs_pct=25.0)
    exec_js = os.path.join(OUTPUT_DIR, f"{today}_main-executive.js")
    with open(exec_js, "w", encoding="utf-8") as f3:
        f3.write("window.AI_EXECUTIVE = ")
        json.dump({"date": today, "bullets": exec_bullets}, f3, ensure_ascii=False)
        f3.write(";")
    print(f"[OK] executive written to: {exec_js}")

    insights_map: Dict[str, Dict[str, Any]] = {}

    for market in mkts:
        insights_map.setdefault(market, {})
        for period in periods:
            if DEBUG: print(f"→ {market} / {period}")
            bullets = detect_anomalies(df, market, period, top_n=5, min_abs_pct=25.0)
            insights_map[market][period] = {"bullets": bullets}

    # outputs
    out_json = os.path.join(OUTPUT_DIR, f"anomalies_{today}.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(insights_map, f, ensure_ascii=False, indent=2)

    dated_js = os.path.join(OUTPUT_DIR, f"{today}_main-insights.js")
    with open(dated_js, "w", encoding="utf-8") as f2:
        f2.write("window.AI_INSIGHTS = ")
        json.dump(insights_map, f2, ensure_ascii=False)
        f2.write(";")
    print(f"[OK] insights written to: {dated_js}")

    # -------- NEW: VVIP insights file --------
    vvip_js = write_vvip_insights_js(today)
    print(f"[OK] VVIP insights written to: {vvip_js}")

if __name__ == "__main__":
    run()