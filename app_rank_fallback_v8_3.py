# app.py — 起漲戰情室｜戰神 v8.2 WantGoo Rank Source 版｜低請求量｜排行候選池｜Apple Pro
import io
import math
import re
import time
from datetime import datetime, timedelta, time as dtime
from collections import deque

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
import pandas as pd
try:
    import yfinance as yf
    HAS_YF = True
except Exception:
    yf = None
    HAS_YF = False
import streamlit as st

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================
# SYSTEM DIAGNOSTICS & CONSTANTS
# =========================
HTTP_TIMEOUT = (3.0, 10.0)
RANK_SOURCES = [
    {"name": "Yahoo 台股成交量排行", "url": "https://tw.stock.yahoo.com/rank/volume", "kind": "yahoo"},
    {"name": "HiStock 即時成交量排行", "url": "https://histock.tw/stock/rank.aspx?d=1&m=11&t=dt", "kind": "histock"},
    {"name": "WantGoo 成交量排行", "url": "https://www.wantgoo.com/stock/ranking/volume", "kind": "wantgoo"},
]

RANK_CACHE_TTL = 12


def diag_init():
    return {
        "meta_count": 0,
        "cand_total": 0,
        "rank_req_err": 0,
        "rank_seen": 0,
        "rank_parse_ok": 0,
        "rank_parse_fail": 0,
        "rank_rows": 0,
        "rank_source": "-",
        "rank_asof": "",
        "yf_symbols": 0,
        "yf_returned": 0,
        "yf_fail": 0,
        "other_err": 0,
        "yf_bulk_fail": 0,
        "yf_rescue_used": 0,
        "yf_parts_ok": 0,
        "yf_parts_fail": 0,
        "last_errors": deque(maxlen=10),
        "t_meta": 0.0,
        "t_rank": 0.0,
        "t_yf": 0.0,
        "t_filter": 0.0,
        "total": 0.0,
    }


def diag_err(diag, e, tag="ERR"):
    diag["last_errors"].append(f"[{tag}] {type(e).__name__}: {e}")


def get_github_headers():
    return {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/csv,text/plain,*/*",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
    }


RANK_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)


def get_rank_headers(url: str = ""):
    referer = "https://www.google.com/"
    if "yahoo.com" in url:
        referer = "https://tw.stock.yahoo.com/"
    elif "histock.tw" in url:
        referer = "https://histock.tw/"
    elif "wantgoo.com" in url:
        referer = "https://www.wantgoo.com/stock"
    return {
        "User-Agent": RANK_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": referer,
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }


def make_retry_session(base_headers=None):
    s = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    if base_headers:
        s.headers.update(base_headers)
    return s


def _is_ssl_like(e: Exception) -> bool:
    s = str(e).lower()
    if "ssl" in s or "certificate" in s or "cert" in s:
        return True
    cause = getattr(e, "__cause__", None) or getattr(e, "__context__", None)
    if cause and ("ssl" in str(cause).lower() or "certificate" in str(cause).lower()):
        return True
    return False


def safe_get(session, url, timeout, diag=None):
    try:
        return session.get(url, timeout=timeout, verify=True)
    except requests.exceptions.RequestException as e:
        if _is_ssl_like(e):
            if diag is not None:
                diag_err(diag, e, "SSL_DOWNGRADE")
            return session.get(url, timeout=timeout, verify=False)
        raise


# =========================
# DATA FETCHING
# =========================
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_text(url: str):
    s = make_retry_session(base_headers=get_github_headers())
    r = s.get(url, timeout=(3.0, 15.0), verify=True)
    r.raise_for_status()
    return r.text.replace("\r", "")


def get_stock_list():
    meta, errors = {}, []
    urls = [
        ("tse", "https://raw.githubusercontent.com/mlouielu/twstock/master/twstock/codes/twse_equities.csv"),
        ("otc", "https://raw.githubusercontent.com/mlouielu/twstock/master/twstock/codes/tpex_equities.csv"),
    ]
    for ex, url in urls:
        try:
            text = fetch_text(url)
            df = pd.read_csv(io.StringIO(text), dtype=str, engine="python", on_bad_lines="skip")
            col_map = {c.strip().lower(): c for c in df.columns}
            c_col = col_map.get("code") or df.columns[1]
            n_col = col_map.get("name") or df.columns[2]
            t_col = col_map.get("type")
            for _, row in df.iterrows():
                stype = str(row.get(t_col, "")) if t_col else ""
                if t_col and ("權證" in stype or "ETF" in stype or "ETN" in stype):
                    continue
                code = str(row[c_col]).strip()
                if len(code) == 4 and code.isdigit():
                    meta[code] = {"name": str(row[n_col]), "ex": ex}
        except Exception as e:
            if not isinstance(e, pd.errors.ParserError):
                errors.append(f"{ex} - {str(e)}")
    return meta, errors


@st.cache_data(ttl=6 * 3600, show_spinner=False)
def yf_download_daily(syms):
    if (not HAS_YF) or (not syms):
        return None
    df = yf.download(
        tickers=" ".join(syms),
        period="120d",
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        threads=True,
        progress=False,
    )
    if df is None or getattr(df, "empty", False):
        return df
    if not isinstance(df.columns, pd.MultiIndex):
        t = syms[0]
        df.columns = pd.MultiIndex.from_product([[t], df.columns])
    df = df[~df.index.duplicated(keep="last")]
    df = df.sort_index()
    return df


def _flatten_columns(cols):
    out = []
    for c in cols:
        if isinstance(c, tuple):
            text = "".join([str(x) for x in c if str(x) != "nan"])
        else:
            text = str(c)
        text = re.sub(r"\s+", "", text)
        out.append(text)
    return out


def _to_float(x):
    if pd.isna(x):
        return math.nan
    s = str(x).strip().replace(",", "").replace("％", "%")
    s = s.replace("▲", "")
    s = s.replace("△", "")
    s = s.replace("▼", "-")
    s = s.replace("−", "-")
    s = s.replace("%", "")
    s = re.sub(r"[^0-9.\-]", "", s)
    s = s.replace("--", "-")
    if s in ("", "-", ".", "-."):
        return math.nan
    try:
        return float(s)
    except Exception:
        return math.nan


def _find_col(df, keywords):
    for c in df.columns:
        txt = str(c)
        if all(k in txt for k in keywords):
            return c
    for c in df.columns:
        txt = str(c)
        if any(k in txt for k in keywords):
            return c
    return None


def _clean_name(x: str) -> str:
    s = re.sub(r"\s+", " ", str(x or "")).strip()
    s = re.sub(r"\((?:上市|上櫃)\)", "", s)
    return s.strip()


def _pick_best_table(tables, required_tokens):
    best_df = None
    best_score = -1
    for t in tables:
        cols = _flatten_columns(t.columns)
        score = sum(1 for token in required_tokens if any(token in c for c in cols))
        if score > best_score:
            best_df = t.copy()
            best_score = score
    return best_df, best_score


def _parse_yahoo_table(html: str):
    tables = pd.read_html(io.StringIO(html))
    if not tables:
        raise ValueError("找不到 Yahoo 表格")

    df, score = _pick_best_table(tables, ["股名", "股號", "股價", "漲跌", "漲跌幅", "最高", "最低", "成交量"])
    if df is None or score < 5:
        raise ValueError("Yahoo 表格結構不符預期")

    df.columns = _flatten_columns(df.columns)
    name_code_col = _find_col(df, ["股名", "股號"]) or _find_col(df, ["股名"]) or _find_col(df, ["股號"])
    price_col = _find_col(df, ["股價"])
    change_col = _find_col(df, ["漲跌"])
    pct_col = _find_col(df, ["漲跌幅"])
    high_col = _find_col(df, ["最高"])
    low_col = _find_col(df, ["最低"])
    vol_col = _find_col(df, ["成交量"])
    needed = [name_code_col, price_col, change_col, pct_col, high_col, low_col, vol_col]
    if any(c is None for c in needed):
        raise ValueError(f"Yahoo 欄位不足: {df.columns.tolist()}")

    out = df[[name_code_col, price_col, change_col, pct_col, high_col, low_col, vol_col]].copy()
    out.columns = ["name_code", "last", "chg", "chg_pct", "high", "low", "vol_lots"]
    out["code"] = out["name_code"].astype(str).str.extract(r"([0-9]{4,6}[A-Z]?)\.(?:TW|TWO)", expand=False)
    out["name"] = out["name_code"].astype(str).str.replace(r"([0-9]{4,6}[A-Z]?)\.(?:TW|TWO)", "", regex=True)
    out["name"] = out["name"].apply(_clean_name)

    for c in ["last", "chg", "chg_pct", "high", "low", "vol_lots"]:
        out[c] = out[c].apply(_to_float)

    out = out.dropna(subset=["code", "last", "high", "low", "vol_lots"])
    out = out[out["last"] > 0].copy()

    m = re.search(r"資料時間[:：]?\s*(\d{4}/\d{2}/\d{2}(?:\s+\d{2}:\d{2})?)", html)
    asof = m.group(1) if m else ""
    out["prev_close"] = math.nan
    return out[["code", "name", "last", "chg", "chg_pct", "high", "low", "vol_lots", "prev_close"]].reset_index(drop=True), asof


def _parse_histock_table(html: str):
    tables = pd.read_html(io.StringIO(html))
    if not tables:
        raise ValueError("找不到 HiStock 表格")

    df, score = _pick_best_table(tables, ["代號", "名稱", "價格", "漲跌", "漲跌幅", "最高", "最低", "昨收", "成交量"])
    if df is None or score < 6:
        raise ValueError("HiStock 表格結構不符預期")

    df.columns = _flatten_columns(df.columns)
    code_col = _find_col(df, ["代號"])
    name_col = _find_col(df, ["名稱"])
    price_col = _find_col(df, ["價格"])
    change_col = _find_col(df, ["漲跌"])
    pct_col = _find_col(df, ["漲跌幅"])
    high_col = _find_col(df, ["最高"])
    low_col = _find_col(df, ["最低"])
    prev_col = _find_col(df, ["昨收"])
    vol_col = _find_col(df, ["成交量"])
    needed = [code_col, name_col, price_col, pct_col, high_col, low_col, vol_col]
    if any(c is None for c in needed):
        raise ValueError(f"HiStock 欄位不足: {df.columns.tolist()}")

    cols = [code_col, name_col, price_col, pct_col, high_col, low_col, vol_col]
    if change_col is not None:
        cols.append(change_col)
    if prev_col is not None:
        cols.append(prev_col)
    out = df[cols].copy()
    rename_map = {
        code_col: "code", name_col: "name", price_col: "last", pct_col: "chg_pct",
        high_col: "high", low_col: "low", vol_col: "vol_lots"
    }
    if change_col is not None:
        rename_map[change_col] = "chg"
    if prev_col is not None:
        rename_map[prev_col] = "prev_close"
    out = out.rename(columns=rename_map)

    out["code"] = out["code"].astype(str).str.extract(r"([0-9]{4,6}[A-Z]?)", expand=False)
    out["name"] = out["name"].apply(_clean_name)
    for c in ["last", "chg_pct", "high", "low", "vol_lots"]:
        out[c] = out[c].apply(_to_float)
    if "chg" in out.columns:
        out["chg"] = out["chg"].apply(_to_float)
    else:
        out["chg"] = math.nan
    if "prev_close" in out.columns:
        out["prev_close"] = out["prev_close"].apply(_to_float)
    else:
        out["prev_close"] = math.nan

    need_change = out["chg"].isna() & out["prev_close"].notna() & out["last"].notna()
    out.loc[need_change, "chg"] = out.loc[need_change, "last"] - out.loc[need_change, "prev_close"]

    out = out.dropna(subset=["code", "last", "high", "low", "vol_lots"])
    out = out[out["last"] > 0].copy()

    date_m = re.search(r"(\d{2}-\d{2})\s+Top", html)
    time_m = re.search(r"本地時間[:：]\s*(\d{1,2}:\d{2})", html)
    asof = ""
    if date_m and time_m:
        asof = f"{date_m.group(1)} {time_m.group(1)}"
    elif time_m:
        asof = time_m.group(1)
    return out[["code", "name", "last", "chg", "chg_pct", "high", "low", "vol_lots", "prev_close"]].reset_index(drop=True), asof


def _parse_wantgoo_table(html: str):
    tables = pd.read_html(io.StringIO(html))
    if not tables:
        raise ValueError("找不到 WantGoo 表格")

    df, score = _pick_best_table(tables, ["代碼", "股票", "成交價", "最高", "最低", "成交量"])
    if df is None or score < 4:
        raise ValueError("WantGoo 表格結構不符預期")

    df.columns = _flatten_columns(df.columns)
    code_col = _find_col(df, ["代碼"])
    name_col = _find_col(df, ["股票"])
    price_col = _find_col(df, ["成交價"])
    change_col = _find_col(df, ["漲跌"])
    pct_col = _find_col(df, ["漲跌%"])
    high_col = _find_col(df, ["最高"])
    low_col = _find_col(df, ["最低"])
    vol_col = _find_col(df, ["成交量"])
    needed = [code_col, name_col, price_col, change_col, pct_col, high_col, low_col, vol_col]
    if any(c is None for c in needed):
        raise ValueError(f"WantGoo 欄位不足: {df.columns.tolist()}")

    out = df[[code_col, name_col, price_col, change_col, pct_col, high_col, low_col, vol_col]].copy()
    out.columns = ["code", "name", "last", "chg", "chg_pct", "high", "low", "vol_lots"]
    out["code"] = out["code"].astype(str).str.extract(r"([0-9]{4,6}[A-Z]?)", expand=False)
    out["name"] = out["name"].apply(_clean_name)
    for c in ["last", "chg", "chg_pct", "high", "low", "vol_lots"]:
        out[c] = out[c].apply(_to_float)
    out = out.dropna(subset=["code", "last", "high", "low", "vol_lots"])
    out = out[out["last"] > 0].copy()

    m = re.search(r"(\d{4}/\d{2}/\d{2})", html)
    asof = m.group(1) if m else ""
    out["prev_close"] = math.nan
    return out[["code", "name", "last", "chg", "chg_pct", "high", "low", "vol_lots", "prev_close"]].reset_index(drop=True), asof


@st.cache_data(ttl=RANK_CACHE_TTL, show_spinner=False)
def _fetch_rank_html(url: str):
    session = make_retry_session(base_headers=get_rank_headers(url))
    r = safe_get(session, url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.text


@st.cache_data(ttl=RANK_CACHE_TTL, show_spinner=False)
def fetch_rank_candidates(max_rows: int = 250):
    errors = []
    for spec in RANK_SOURCES:
        try:
            html = _fetch_rank_html(spec["url"])
            if spec["kind"] == "yahoo":
                df, asof = _parse_yahoo_table(html)
            elif spec["kind"] == "histock":
                df, asof = _parse_histock_table(html)
            else:
                df, asof = _parse_wantgoo_table(html)
            if df is not None and not df.empty:
                return df.head(max_rows), asof, spec["name"], errors
            errors.append(f"{spec['name']}: EMPTY")
        except Exception as e:
            errors.append(f"{spec['name']}: {type(e).__name__}: {e}")
    raise RuntimeError(" | ".join(errors) if errors else "所有排行來源都失敗")


# =========================
# UI / THEME
# =========================
st.set_page_config(page_title="起漲戰情室 Ultra", page_icon="⚡", layout="wide", initial_sidebar_state="collapsed")
st.markdown(
    """
<style>
    [data-testid="stAppViewContainer"], .main { background: #050505 !important; background-image: radial-gradient(circle at 15% 50%, rgba(20, 20, 20, 1), transparent 25%), radial-gradient(circle at 85% 30%, rgba(10, 25, 40, 0.8), transparent 25%) !important; color: #e2e8f0 !important; }
    .block-container { padding-top: 2rem; max-width: 1280px; }
    [data-testid="stSidebar"] { display: none !important; }
    .title { font-size: 58px; font-weight: 900; letter-spacing: -2px; background: linear-gradient(135deg, #ffffff 0%, #718096 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; text-align: center; margin-bottom: 5px; }
    .status-caption { color: #64748b; font-size: 13px; text-align: center; margin-bottom: 30px; letter-spacing: 1px;}
    .pro-card { background: linear-gradient(145deg, rgba(22, 24, 29, 0.9), rgba(13, 15, 18, 0.9)); backdrop-filter: blur(24px); border: 1px solid rgba(255, 255, 255, 0.05); border-top: 1px solid rgba(255, 255, 255, 0.1); border-radius: 20px; padding: 24px; margin-bottom: 16px; transition: all 0.4s cubic-bezier(0.2, 0.8, 0.2, 1); box-shadow: 0 10px 30px -10px rgba(0,0,0,0.5); }
    .pro-card:hover { border-color: rgba(56, 189, 248, 0.4); transform: translateY(-5px) scale(1.01); box-shadow: 0 20px 40px -10px rgba(56, 189, 248, 0.15); }
    .stock-name { font-size: 22px; font-weight: 800; color: #f8fafc; letter-spacing: 1px;}
    .price-large { font-size: 36px; font-weight: 900; color: #ffffff; font-variant-numeric: tabular-nums; text-shadow: 0 2px 10px rgba(255,255,255,0.1);}
    .tag-pro { padding: 5px 14px; border-radius: 6px; font-size: 11px; font-weight: 800; background: rgba(56, 189, 248, 0.1); color: #38bdf8; border: 1px solid rgba(56, 189, 248, 0.2); letter-spacing: 1px;}
    .fail-tag { display: inline-block; padding: 6px 12px; background: rgba(244, 63, 94, 0.05); color: #f43f5e; border-radius: 8px; margin: 4px; font-size: 12px; border: 1px solid rgba(244, 63, 94, 0.15); font-weight: 600;}
    .stButton>button { border-radius: 16px !important; background: linear-gradient(135deg, #f8fafc 0%, #cbd5e1 100%) !important; color: #0f172a !important; font-weight: 900 !important; padding: 20px !important; width: 100% !important; border: none !important; font-size: 18px !important; letter-spacing: 2px !important; box-shadow: 0 4px 15px rgba(255,255,255,0.1) !important; transition: all 0.3s ease !important; }
    .stButton>button:hover { transform: translateY(-2px); box-shadow: 0 8px 25px rgba(255,255,255,0.2) !important; }
    [data-testid="stMetric"] { background: rgba(20,20,20,0.6); padding: 15px; border-radius: 16px; border: 1px solid rgba(255,255,255,0.03); }
    [data-testid="stMetricValue"] { font-size: 32px !important; font-weight: 900 !important; color: #f1f5f9 !important; }
    [data-testid="stMetricLabel"] { font-size: 13px !important; color: #94a3b8 !important; font-weight: 600 !important; letter-spacing: 1px; }
    [data-testid="stExpander"] { background: transparent !important; border: 1px solid rgba(255,255,255,0.05) !important; border-radius: 16px !important; }
    [data-testid="stExpander"] summary { background: rgba(20,20,20,0.4) !important; border-radius: 16px !important; }
</style>
""",
    unsafe_allow_html=True,
)

# =========================
# HELPERS
# =========================
def now_taipei():
    return datetime.utcnow() + timedelta(hours=8)


def idx_date_taipei(idx):
    try:
        if getattr(idx, "tz", None) is not None:
            try:
                return idx.tz_convert("Asia/Taipei").date
            except Exception:
                return idx.tz_localize(None).date
    except Exception:
        pass
    return idx.date


def tw_tick(price):
    return 0.01 if price < 10 else 0.05 if price < 50 else 0.1 if price < 100 else 0.5 if price < 500 else 1.0 if price < 1000 else 5.0


def calc_limit_up(prev_close, limit_pct=0.10):
    raw = prev_close * (1.0 + limit_pct)
    tick = tw_tick(raw)
    n = math.floor((raw + 1e-12) / tick)
    return round(n * tick, 2 if tick < 0.1 else 1 if tick < 1 else 0)


def infer_daily_limit(pp, cp):
    l10 = calc_limit_up(pp, 0.10)
    l20 = calc_limit_up(pp, 0.20)
    tol20 = max(tw_tick(l20), l20 * 0.0005)
    if abs(cp - l20) <= tol20 and abs(cp - l20) < abs(cp - l10):
        return l20
    return l10


# =========================
# ENGINES
# =========================
def fetch_rank_snapshot(status_placeholder, diag):
    status_placeholder.update(label="📡 抓取多來源成交量排行中...", state="running")
    try:
        raw_df, asof, src_name, fetch_errors = fetch_rank_candidates(max_rows=500)
        diag["rank_asof"] = asof
        diag["rank_source"] = src_name
        for msg in fetch_errors:
            diag_err(diag, Exception(msg), "RANK_FALLBACK")
        diag["rank_seen"] = len(raw_df)
        return raw_df
    except Exception as e:
        diag["rank_req_err"] += 1
        diag_err(diag, e, "RANK_FETCH")
        return pd.DataFrame()


def build_rank_candidates(raw_df, meta_dict, now_ts, is_test, diag):
    rows = []
    diag["rank_seen"] = len(raw_df)
    diag["rank_parse_ok"] = 0
    diag["rank_parse_fail"] = 0
    diag["rank_rows"] = 0
    diag["cand_total"] = 0

    m = int((datetime.combine(now_ts.date(), now_ts.time()) - datetime.combine(now_ts.date(), dtime(9, 0))).total_seconds() // 60)
    m = max(0, min(270, m))
    dist_limit = 6.0 if is_test else (4.2 if m <= 60 else 3.0 if m <= 180 else 2.2)
    vol_limit_lots = 200 if is_test else 3000
    chg_pct_min = -0.5 if is_test else 0.3

    for _, q in raw_df.iterrows():
        c = str(q["code"]).strip()
        if c not in meta_dict:
            continue

        try:
            last = float(q["last"])
            high = float(q["high"])
            low = float(q["low"])
            vol_lots = float(q["vol_lots"])
            chg = float(q["chg"]) if pd.notna(q["chg"]) else 0.0
            chg_pct = float(q["chg_pct"]) if pd.notna(q["chg_pct"]) else 0.0

            prev_from_src = float(q["prev_close"]) if ("prev_close" in q and pd.notna(q["prev_close"])) else math.nan
            prev_close = prev_from_src if math.isfinite(prev_from_src) and prev_from_src > 0 else round(last - chg, 2)
            if prev_close <= 0:
                diag["rank_parse_fail"] += 1
                continue

            upper = calc_limit_up(prev_close, 0.10)
            dist_pct = max(0.0, ((upper - last) / upper) * 100)

            if vol_lots >= vol_limit_lots and dist_pct <= dist_limit and chg_pct >= chg_pct_min:
                rows.append({
                    "code": c,
                    "last": last,
                    "upper": upper,
                    "dist": dist_pct,
                    "vol_sh": vol_lots * 1000.0,
                    "prev_close": prev_close,
                    "high": high if high > 0 else last,
                    "low": low if low > 0 else last,
                    "chg_pct": chg_pct,
                })
                diag["rank_parse_ok"] += 1
            else:
                diag["rank_parse_fail"] += 1
        except Exception:
            diag["rank_parse_fail"] += 1

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["dist", "vol_sh"], ascending=[True, False]).drop_duplicates("code", keep="first")

    diag["rank_rows"] = len(df)
    diag["cand_total"] = len(df)
    return df


def make_snapshot_diag(meta_count, fetch_diag):
    diag = diag_init()
    diag["meta_count"] = meta_count
    diag["rank_req_err"] = fetch_diag.get("rank_req_err", 0)
    diag["rank_seen"] = fetch_diag.get("rank_seen", 0)
    diag["rank_source"] = fetch_diag.get("rank_source", "-")
    diag["rank_asof"] = fetch_diag.get("rank_asof", "")
    diag["last_errors"] = deque(fetch_diag.get("last_errors", []), maxlen=10)
    diag["t_meta"] = fetch_diag.get("t_meta", 0.0)
    diag["t_rank"] = fetch_diag.get("t_rank", 0.0)
    return diag


def recompute_from_snapshot(snapshot, is_test, use_bloodline):
    t0 = time.perf_counter()
    diag = make_snapshot_diag(snapshot["meta_count"], snapshot["fetch_diag"])
    now_ts = snapshot["ts"]

    pre_df = build_rank_candidates(snapshot["raw_rank_df"], snapshot["meta"], now_ts, is_test, diag)
    t = time.perf_counter()
    final_res, stats, yf_diag = core_filter_engine(pre_df, snapshot["meta"], now_ts, is_test, diag, use_bloodline)
    diag["t_filter"] = time.perf_counter() - t
    diag.update(yf_diag)
    diag["total"] = time.perf_counter() - t0

    return {
        "res": final_res,
        "stats": stats,
        "diag": diag,
        "ts": now_ts,
        "is_test": is_test,
        "use_bloodline": use_bloodline,
        "snapshot": snapshot,
        "instant_switch": True,
    }


def core_filter_engine(candidates_df, meta_dict, now_ts, is_test, diag, use_bloodline):
    stats = {"Total": 0, "爆量不足": [], "回落過大": [], "收盤太弱": [], "非連板標的": []}
    yf_diag = {"yf_symbols": 0, "yf_fail": 0, "other_err": 0}
    if candidates_df.empty:
        return pd.DataFrame(), stats, yf_diag

    candidates_df = candidates_df.sort_values(["dist", "vol_sh"], ascending=[True, False]).head(80)
    stats["Total"] = len(candidates_df)
    syms = [f"{c}.{'TW' if meta_dict[c]['ex'] == 'tse' else 'TWO'}" for c in candidates_df["code"]]
    yf_diag["yf_symbols"] = len(syms)

    # yfinance 若未安裝，系統降級為「排行即時候選模式」：
    # 保留排行來源的高/低/漲幅/接近漲停判斷，不做 20 日量均與連板血統。
    if not HAS_YF:
        diag_err(diag, Exception("yfinance not installed; fallback to rank-only mode"), "YF_MISSING")
        diag["t_yf"] = 0.0
        results = []
        m = int((datetime.combine(now_ts.date(), now_ts.time()) - datetime.combine(now_ts.date(), dtime(9, 0))).total_seconds() // 60)
        m = max(0, min(270, m))
        pb_lim = 0.05 if is_test else (0.015 if m <= 90 else 0.006)

        for _, r in candidates_df.iterrows():
            c, name = r["code"], meta_dict[r["code"]]["name"]
            try:
                rng = max(0.0, r["high"] - r["low"])
                if (r["high"] - r["last"]) / max(1e-9, r["high"]) > pb_lim:
                    stats["回落過大"].append(f"{c} {name}")
                    continue
                if rng > 0.1 and (r["last"] - r["low"]) / max(1e-9, rng) < (0.5 if is_test else 0.80):
                    stats["收盤太弱"].append(f"{c} {name}")
                    continue

                near_limit = abs(r["last"] - r["upper"]) <= max(tw_tick(r["upper"]), r["upper"] * 0.0005)
                high_is_limit = abs(r["high"] - r["upper"]) <= max(tw_tick(r["upper"]), r["upper"] * 0.0005)
                if near_limit and high_is_limit and abs(r["last"] - r["high"]) <= max(tw_tick(r["last"]), r["last"] * 0.0005):
                    status = "🔥 鎖價跡象"
                elif near_limit:
                    status = "🚀 漲停附近"
                else:
                    status = "⚡ 發動"

                base_lots = 200.0 if is_test else 3000.0
                vol_score = max(0.1, min(99.9, (r["vol_sh"] / 1000.0) / base_lots))
                results.append({
                    "代號": c,
                    "名稱": name,
                    "現價": r["last"],
                    "爆量": vol_score,
                    "狀態": status + "（降級）",
                    "階段": "排行候選",
                    "board_val": 0,
                    "漲幅%": r.get("chg_pct", 0.0),
                })
            except Exception as e:
                yf_diag["other_err"] += 1
                diag_err(diag, e, "FILTER_RANK_ONLY")

        res_df = pd.DataFrame(results)
        if not res_df.empty:
            res_df = res_df.sort_values(["漲幅%", "爆量"], ascending=[False, False])
        return res_df, stats, yf_diag

    t_yf_start = time.perf_counter()
    raw_daily = None

    def try_yf_parts(parts):
        res_frames = []
        for part in parts:
            if not part:
                res_frames.append(None)
                continue
            try:
                res_frames.append(yf_download_daily(part))
            except Exception as e:
                diag_err(diag, e, "YF_PART_FAIL")
                res_frames.append(None)
        return res_frames

    try:
        raw_daily = yf_download_daily(syms)
        if raw_daily is None or getattr(raw_daily, "empty", False):
            raise Exception("YF_BULK_EMPTY")
        diag["yf_rescue_used"] = 0
    except Exception as e:
        tag = "YF_BULK_EMPTY" if str(e) == "YF_BULK_EMPTY" else "YF_BULK_FAIL"
        diag_err(diag, e, tag)
        diag["yf_bulk_fail"] = diag.get("yf_bulk_fail", 0) + 1
        diag["yf_rescue_used"] = 1

        mid = max(1, len(syms) // 2)
        parts1 = [syms[:mid], syms[mid:]]
        frames1 = try_yf_parts(parts1)

        diag["yf_parts_ok"] = diag.get("yf_parts_ok", 0) + sum(1 for f in frames1 if f is not None and not getattr(f, "empty", False))
        diag["yf_parts_fail"] = diag.get("yf_parts_fail", 0) + sum(1 for f in frames1 if f is None or getattr(f, "empty", False))
        frames_ok = [f for f in frames1 if f is not None and not getattr(f, "empty", False)]

        if len(frames_ok) < 2:
            parts2 = []
            for i, f in enumerate(frames1):
                if f is None or getattr(f, "empty", False):
                    p = parts1[i]
                    if not p:
                        continue
                    if len(p) > 1:
                        m2 = len(p) // 2
                        parts2.extend([p[:m2], p[m2:]])
                    else:
                        parts2.append(p)

            if parts2:
                frames2 = try_yf_parts(parts2)
                diag["yf_parts_ok"] += sum(1 for f in frames2 if f is not None and not getattr(f, "empty", False))
                diag["yf_parts_fail"] += sum(1 for f in frames2 if f is None or getattr(f, "empty", False))
                frames_ok.extend([f for f in frames2 if f is not None and not getattr(f, "empty", False)])

        if frames_ok:
            raw_daily = pd.concat(frames_ok, axis=1)
            if raw_daily is not None and not isinstance(raw_daily.columns, pd.MultiIndex):
                fallback_t = syms[0]
                try:
                    for f in frames_ok:
                        if f is not None and isinstance(getattr(f, "columns", None), pd.MultiIndex):
                            fallback_t = f.columns.get_level_values(0)[0]
                            break
                except Exception:
                    pass
                raw_daily.columns = pd.MultiIndex.from_product([[fallback_t], raw_daily.columns])
            if isinstance(raw_daily.columns, pd.MultiIndex):
                raw_daily = raw_daily.loc[:, ~raw_daily.columns.duplicated()]
            raw_daily = raw_daily[~raw_daily.index.duplicated(keep="last")]
            raw_daily = raw_daily.sort_index()

    diag["t_yf"] = time.perf_counter() - t_yf_start
    if raw_daily is None or getattr(raw_daily, "empty", False):
        yf_diag["other_err"] += 1
        return pd.DataFrame(), stats, yf_diag

    if isinstance(raw_daily.columns, pd.MultiIndex):
        diag["yf_returned"] = int(raw_daily.columns.get_level_values(0).nunique())
    else:
        diag["yf_returned"] = 1

    results, today_date = [], now_ts.date()
    m = int((datetime.combine(now_ts.date(), now_ts.time()) - datetime.combine(now_ts.date(), dtime(9, 0))).total_seconds() // 60)
    m = max(0, min(270, m))
    frac = 0.5 if is_test else (
        0.12 if m <= 30 else
        0.12 + (0.5 - 0.12) * ((m - 30) / 90.0) if m <= 120 else
        min(1.0, 0.5 + (1.0 - 0.5) * ((m - 120) / 150.0))
    )
    pb_lim = 0.05 if is_test else (0.015 if m <= 90 else 0.006)

    for _, r in candidates_df.iterrows():
        c, name = r["code"], meta_dict[r["code"]]["name"]
        sym = f"{c}.{'TW' if meta_dict[c]['ex'] == 'tse' else 'TWO'}"
        try:
            if isinstance(raw_daily.columns, pd.MultiIndex):
                if sym not in raw_daily.columns.get_level_values(0):
                    yf_diag["yf_fail"] += 1
                    continue
                df_sym = raw_daily[sym]
            else:
                df_sym = raw_daily

            if not {"Close", "Volume"}.issubset(set(df_sym.columns)):
                yf_diag["yf_fail"] += 1
                continue

            dfD = df_sym[["Close", "Volume"]].dropna()
            if len(dfD) < 30:
                yf_diag["yf_fail"] += 1
                continue

            dates_tw = idx_date_taipei(dfD.index)
            past_df = dfD[dates_tw < today_date].copy()
            if len(past_df) < 30:
                yf_diag["yf_fail"] += 1
                continue

            vol_ma20_sh = float(past_df["Volume"].rolling(20).mean().iloc[-1])
            if (not math.isfinite(vol_ma20_sh)) or vol_ma20_sh <= 0:
                yf_diag["yf_fail"] += 1
                continue

            past_boards, past_10 = 0, past_df.tail(10)
            for i in range(len(past_10) - 1, 0, -1):
                cp, pp = float(past_10["Close"].iloc[i]), float(past_10["Close"].iloc[i - 1])
                lim = infer_daily_limit(pp, cp)
                if cp >= (lim - tw_tick(lim)):
                    past_boards += 1
                else:
                    break

            if use_bloodline and (not is_test) and past_boards < 1:
                stats["非連板標的"].append(f"{c} {name}")
                continue

            vol_ratio = r["vol_sh"] / (vol_ma20_sh * frac + 1e-9)
            if vol_ratio < (0.5 if is_test else 1.3):
                stats["爆量不足"].append(f"{c} {name}")
                continue

            rng = max(0.0, r["high"] - r["low"])
            if (r["high"] - r["last"]) / max(1e-9, r["high"]) > pb_lim:
                stats["回落過大"].append(f"{c} {name}")
                continue
            if rng > 0.1 and (r["last"] - r["low"]) / max(1e-9, rng) < (0.5 if is_test else 0.80):
                stats["收盤太弱"].append(f"{c} {name}")
                continue

            near_limit = abs(r["last"] - r["upper"]) <= max(tw_tick(r["upper"]), r["upper"] * 0.0005)
            high_is_limit = abs(r["high"] - r["upper"]) <= max(tw_tick(r["upper"]), r["upper"] * 0.0005)
            if near_limit and high_is_limit and abs(r["last"] - r["high"]) <= max(tw_tick(r["last"]), r["last"] * 0.0005):
                status = "🔥 鎖價跡象"
            elif near_limit:
                status = "🚀 漲停附近"
            else:
                status = "⚡ 發動"

            results.append({
                "代號": c,
                "名稱": name,
                "現價": r["last"],
                "爆量": vol_ratio,
                "狀態": status,
                "階段": f"連續 {past_boards + 1} 板",
                "board_val": past_boards,
                "漲幅%": r["chg_pct"],
            })
        except Exception as e:
            yf_diag["other_err"] += 1
            diag_err(diag, e, "FILTER")

    res_df = pd.DataFrame(results)
    if not res_df.empty:
        res_df = res_df.sort_values(["board_val", "爆量", "漲幅%"], ascending=[False, False, False])
    return res_df, stats, yf_diag


# =========================
# MAIN
# =========================
st.markdown('<div class="title">起漲戰情室 ULTRA</div>', unsafe_allow_html=True)
st.markdown('<div class="status-caption">排行候選池 v8.5｜切換即時套用｜多來源 fallback｜可無 YF 降級</div>', unsafe_allow_html=True)

if not HAS_YF:
    st.warning('⚠️ 目前環境未安裝 yfinance，系統已自動切換成「排行即時候選模式」。若要恢復血統/20日量均濾網，請在 requirements.txt 加入 yfinance。')
    st.caption('🛡️ 連板血統在無 yfinance 模式下不生效，因此已自動停用。')

col_cfg = st.columns([1.2, 1.2, 1])
with col_cfg[0]:
    is_test = st.toggle("🔥 寬鬆測試模式", value=False)
with col_cfg[1]:
    use_bloodline = st.toggle("🛡️ 嚴格連板血統", value=True, disabled=not HAS_YF)
with col_cfg[2]:
    st.caption("切換模式會直接套用上次快取，不重新掃描；重新掃描會抓最新網站資料")

now_time = time.time()
last_run = st.session_state.get("last_run_ts", 0)
cooldown_seconds = 15

if st.button("🚀 啟動排行縮圈掃描"):
    if now_time - last_run < cooldown_seconds:
        st.warning(f"⏳ 系統冷卻防護中，請等待 {int(cooldown_seconds - (now_time - last_run))} 秒後再執行...")
    else:
        st.session_state["last_run_ts"] = now_time
        t0, diag = time.perf_counter(), diag_init()

        with st.status("⚡ 建立安全連線與解析市場中...", expanded=True) as status:
            t = time.perf_counter()
            meta, meta_errs = get_stock_list()
            diag["t_meta"] = time.perf_counter() - t
            diag["meta_count"] = len(meta)
            for err in meta_errs:
                diag_err(diag, Exception(err), "META_ERR")
            if len(meta) < 500:
                diag_err(diag, Exception(f"清單數量異常 ({len(meta)})"), "META_SUSPECT")

            t = time.perf_counter()
            now_ts = now_taipei()
            raw_rank_df = fetch_rank_snapshot(status, diag)
            diag["t_rank"] = time.perf_counter() - t

            pre_df = build_rank_candidates(raw_rank_df, meta, now_ts, is_test, diag)

            t = time.perf_counter()
            final_res, stats, yf_diag = core_filter_engine(pre_df, meta, now_ts, is_test, diag, use_bloodline)
            diag["t_filter"] = time.perf_counter() - t
            diag.update(yf_diag)
            diag["total"] = time.perf_counter() - t0
            status.update(label="✅ 掃描完成", state="complete")

        snapshot = {
            "meta": meta,
            "meta_count": len(meta),
            "raw_rank_df": raw_rank_df,
            "ts": now_ts,
            "fetch_diag": {
                "rank_req_err": diag.get("rank_req_err", 0),
                "rank_seen": diag.get("rank_seen", 0),
                "rank_source": diag.get("rank_source", "-"),
                "rank_asof": diag.get("rank_asof", ""),
                "last_errors": list(diag.get("last_errors", [])),
                "t_meta": diag.get("t_meta", 0.0),
                "t_rank": diag.get("t_rank", 0.0),
            },
        }

        st.session_state["last_scan"] = {
            "res": final_res,
            "stats": stats,
            "diag": diag,
            "ts": now_ts,
            "is_test": is_test,
            "use_bloodline": use_bloodline,
            "snapshot": snapshot,
            "instant_switch": False,
        }
        st.rerun()

scan = st.session_state.get("last_scan")
if scan and scan.get("snapshot") and (scan.get("is_test") != is_test or scan.get("use_bloodline") != use_bloodline):
    st.session_state["last_scan"] = recompute_from_snapshot(scan["snapshot"], is_test, use_bloodline)
    st.rerun()

scan = st.session_state.get("last_scan")
if scan:
    d, res, sts, ts = scan["diag"], scan["res"], scan["stats"], scan["ts"]
    t_str = f"測試: {'ON' if scan['is_test'] else 'OFF'} | 血統: {'ON' if scan['use_bloodline'] else 'OFF'}"
    asof = f" | 排行日期：{d.get('rank_asof')}" if d.get("rank_asof") else ""
    st.markdown(
        f'<div class="status-caption">上次更新：{ts.strftime("%H:%M:%S")} | {t_str}{asof} | 系統耗時：{d["total"]:.2f}s</div>',
        unsafe_allow_html=True,
    )
    if scan.get("instant_switch"):
        st.caption("⚡ 本次為模式即時切換，直接套用上次掃描快取，未重新抓取網站。")

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("候選標的", d.get("cand_total", 0))
    m2.metric("嚴選錄取檔數", len(res))
    total_parse = d.get("rank_parse_ok", 0) + d.get("rank_parse_fail", 0)
    m3.metric("排行解析良率", f"{(d.get('rank_parse_ok', 0) / max(1, total_parse) * 100):.1f}%")
    m4.metric("系統異常阻擋", d.get("rank_req_err", 0) + d.get("yf_fail", 0) + d.get("other_err", 0))

    with st.expander("⚙️ 系統診斷與底層監控 (白盒分析)", expanded=False):
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("股票主清單", d.get("meta_count", 0))
        c2.metric("排行有效解析", d.get("rank_parse_ok", 0))
        st.caption(f"📡 資料源：{d.get('rank_source', '-')} | 候選池 {d.get('rank_rows', 0)} 檔 | Request ERR {d.get('rank_req_err', 0)}")
        c3.metric("YF 數據覆蓋", '未安裝 / 降級' if not HAS_YF else f"{d.get('yf_returned', 0)} / {d.get('yf_symbols', 0)}")
        rescue_msg = f"{'🟢 啟動' if d.get('yf_rescue_used', 0) else '⚪ 待命'} | ERR {d.get('other_err', 0)}"
        c4.metric("救援協議 / 錯誤", rescue_msg)
        if d.get("yf_rescue_used", 0):
            st.caption(f"⚠️ 細胞分裂救援：成功 {d.get('yf_parts_ok', 0)} 塊 / 失敗 {d.get('yf_parts_fail', 0)} 塊")

        st.caption(f"耗時分布：Meta {d['t_meta']:.2f}s | Rank {d['t_rank']:.2f}s | YF {d.get('t_yf', 0):.2f}s | Filter {d['t_filter']:.2f}s")
        if d.get("last_errors"):
            st.code("\n".join(d["last_errors"]))

    with st.expander("🎯 戰損與淘汰名單 (實名點名)", expanded=True):
        for reason, stocks in sts.items():
            if isinstance(stocks, list) and stocks:
                st.markdown(f"**{reason}**")
                st.markdown('<div>' + "".join([f'<span class="fail-tag">{s}</span>' for s in stocks]) + '</div>', unsafe_allow_html=True)

    if not res.empty:
        st.markdown("<br>", unsafe_allow_html=True)
        cols = st.columns(4)
        for i, r in res.iterrows():
            with cols[i % 4]:
                st.markdown(
                    f"""<div class="pro-card">
                        <div class="tag-pro">{r['階段']}</div>
                        <div class="stock-name">{r['代號']} {r['名稱']}</div>
                        <div style="height:12px;"></div>
                        <div class="price-large">{r['現價']:.2f}</div>
                        <div style="font-size:13px; color:#94a3b8; margin-top:12px; font-weight:600;">
                            {r['狀態']} | 動能 {r['爆量']:.1f}x | 漲幅 {r['漲幅%']:.2f}%
                        </div>
                    </div>""",
                    unsafe_allow_html=True,
                )
    else:
        if d.get("rank_parse_ok", 0) == 0:
            st.error("🚨 無法取得排行資料或解析失敗，請檢視診斷面板。")
        else:
            st.warning("⚠️ 掃描完畢。目前排行候選池內無標的通過嚴格濾網。")
