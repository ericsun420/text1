# app.py — 股市熱門趨勢雷達｜掃描精靈 v8.7 官方API優先版｜多來源備援｜即時切換｜歷史準確度測試
import html
import io
import math
import re
import time
from collections import deque
from datetime import datetime, timedelta, time as dtime
from typing import Dict, List, Tuple

import pandas as pd
import requests
import streamlit as st
import urllib3
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import yfinance as yf
    HAS_YF = True
except Exception:
    yf = None
    HAS_YF = False

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HTTP_TIMEOUT = (3.0, 12.0)
RANK_CACHE_TTL = 12
SNAPSHOT_CACHE_TTL = 60
YF_CACHE_TTL = 6 * 3600
CALIBRATION_LOOKBACK_DAYS = 180
CALIBRATION_SYMBOL_CAP = 16

OFFICIAL_TWSE_STOCK_DAY_ALL = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
OFFICIAL_TPEX_DAILY_CLOSE = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"

HTML_FALLBACK_SOURCES = [
    {"name": "Yahoo 台股成交量排行", "url": "https://tw.stock.yahoo.com/rank/volume", "kind": "yahoo"},
    {"name": "HiStock 即時成交量排行", "url": "https://histock.tw/stock/rank.aspx?d=1&m=11&t=dt", "kind": "histock"},
    {"name": "WantGoo 成交量排行", "url": "https://www.wantgoo.com/stock/ranking/volume", "kind": "wantgoo"},
]


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
        "source_mode": "-",
        "yf_symbols": 0,
        "yf_returned": 0,
        "yf_fail": 0,
        "other_err": 0,
        "yf_bulk_fail": 0,
        "yf_rescue_used": 0,
        "yf_parts_ok": 0,
        "yf_parts_fail": 0,
        "last_errors": deque(maxlen=12),
        "t_meta": 0.0,
        "t_rank": 0.0,
        "t_yf": 0.0,
        "t_filter": 0.0,
        "t_cal": 0.0,
        "total": 0.0,
    }


def diag_err(diag, e, tag="ERR"):
    diag["last_errors"].append(f"[{tag}] {type(e).__name__}: {e}")


def now_taipei():
    return datetime.utcnow() + timedelta(hours=8)


def tw_roc_date(dt_obj: datetime) -> str:
    return f"{dt_obj.year - 1911}/{dt_obj.month:02d}/{dt_obj.day:02d}"


def roc_to_gregorian(roc_date: str) -> str:
    try:
        y, m, d = [int(x) for x in str(roc_date).split("/")]
        return f"{y + 1911:04d}/{m:02d}/{d:02d}"
    except Exception:
        return str(roc_date)


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


def _is_ssl_like(e: Exception) -> bool:
    s = str(e).lower()
    if "ssl" in s or "certificate" in s or "cert" in s:
        return True
    cause = getattr(e, "__cause__", None) or getattr(e, "__context__", None)
    if cause and ("ssl" in str(cause).lower() or "certificate" in str(cause).lower()):
        return True
    return False


def _to_float(x):
    if pd.isna(x):
        return math.nan
    s = str(x).strip().replace(",", "").replace("％", "%")
    s = s.replace("▲", "").replace("△", "")
    s = s.replace("▼", "-")
    s = s.replace("▽", "-")
    s = s.replace("−", "-")
    s = s.replace("+", "")
    s = s.replace("--", "-")
    s = s.replace("%", "")
    s = re.sub(r"[^0-9.\-]", "", s)
    if s in ("", "-", ".", "-."):
        return math.nan
    try:
        return float(s)
    except Exception:
        return math.nan


def _clean_name(x: str) -> str:
    s = re.sub(r"\s+", " ", str(x or "")).strip()
    s = re.sub(r"\((?:上市|上櫃)\)", "", s)
    return s.strip()


def _normalize_code(x: str) -> str:
    m = re.search(r"([0-9]{4,6}[A-Z]?)", str(x or ""))
    return m.group(1) if m else ""


def _safe_pct(num, den):
    return 0.0 if den == 0 else num / den


def get_github_headers():
    return {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/csv,text/plain,*/*",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
    }


def get_browser_headers(url: str = ""):
    referer = "https://www.google.com/"
    if "yahoo.com" in url:
        referer = "https://tw.stock.yahoo.com/"
    elif "histock.tw" in url:
        referer = "https://histock.tw/"
    elif "wantgoo.com" in url:
        referer = "https://www.wantgoo.com/stock"
    elif "openapi.twse.com.tw" in url:
        referer = "https://openapi.twse.com.tw/"
    elif "tpex.org.tw" in url:
        referer = "https://www.tpex.org.tw/openapi/"
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": referer,
        "Connection": "keep-alive",
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


def safe_get(session, url, timeout=HTTP_TIMEOUT, params=None, diag=None):
    try:
        return session.get(url, timeout=timeout, params=params, verify=True)
    except requests.exceptions.RequestException as e:
        if _is_ssl_like(e):
            if diag is not None:
                diag_err(diag, e, "SSL_DOWNGRADE")
            return session.get(url, timeout=timeout, params=params, verify=False)
        raise


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_text(url: str):
    s = make_retry_session(base_headers=get_github_headers())
    r = safe_get(s, url, timeout=(3.0, 15.0))
    r.raise_for_status()
    return r.text.replace("\r", "")


@st.cache_data(ttl=6 * 3600, show_spinner=False)
def yf_download_daily(syms: Tuple[str, ...], period: str = "180d"):
    if (not HAS_YF) or (not syms):
        return None
    df = yf.download(
        tickers=" ".join(list(syms)),
        period=period,
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


@st.cache_data(ttl=6 * 3600, show_spinner=False)
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
            errors.append(f"{ex} - {type(e).__name__}: {e}")
    return meta, errors


# =========================
# OFFICIAL SOURCES (PRIORITY)
# =========================
def _first_value(row: dict, candidates: List[str], default=""):
    for key in candidates:
        if key in row and str(row.get(key, "")).strip() != "":
            return row.get(key)
    return default


def _normalize_official_rows(rows: List[dict], market: str, asof: str = "") -> pd.DataFrame:
    out = []
    for row in rows or []:
        code = _normalize_code(_first_value(row, ["Code", "股票代號", "SecuritiesCompanyCode", "證券代號", "證券代碼"]))
        name = _clean_name(_first_value(row, ["Name", "股票名稱", "CompanyName", "證券名稱"]))
        last = _to_float(_first_value(row, ["ClosingPrice", "收盤價", "Close", "收盤"] ))
        high = _to_float(_first_value(row, ["HighestPrice", "最高價", "High", "最高"]))
        low = _to_float(_first_value(row, ["LowestPrice", "最低價", "Low", "最低"]))
        open_p = _to_float(_first_value(row, ["OpeningPrice", "開盤價", "Open", "開盤"]))
        change = _to_float(_first_value(row, ["Change", "漲跌價差", "漲跌", "漲跌價"] ))
        dir_sign = str(_first_value(row, ["Dir", "漲跌(+/-)", "漲跌註記", "UpDown", "Direction"], default="")).strip()
        if math.isfinite(change) and dir_sign in ("-", "▽", "▼"):
            change = -abs(change)
        elif math.isfinite(change) and dir_sign in ("+", "△", "▲"):
            change = abs(change)
        vol_sh = _to_float(_first_value(row, ["TradeVolume", "成交股數", "成交量", "Volume"]))
        prev_close = _to_float(_first_value(row, ["YesterdayClosingPrice", "前日收盤價", "昨收", "PreviousClose"]))

        if not code or not math.isfinite(last) or last <= 0 or not math.isfinite(vol_sh) or vol_sh <= 0:
            continue
        if not math.isfinite(high) or high <= 0:
            high = max(last, open_p if math.isfinite(open_p) else last)
        if not math.isfinite(low) or low <= 0:
            low = min(last, open_p if math.isfinite(open_p) else last)
        if (not math.isfinite(prev_close) or prev_close <= 0) and math.isfinite(change):
            prev_close = round(last - change, 2)
        if (not math.isfinite(change)) and math.isfinite(prev_close) and prev_close > 0:
            change = last - prev_close
        chg_pct = _safe_pct(last - prev_close, prev_close) * 100 if math.isfinite(prev_close) and prev_close > 0 else math.nan

        out.append({
            "code": code,
            "name": name,
            "last": last,
            "chg": change if math.isfinite(change) else 0.0,
            "chg_pct": chg_pct,
            "high": high,
            "low": low,
            "vol_lots": vol_sh / 1000.0,
            "prev_close": prev_close,
            "market": market,
            "asof": asof,
        })
    return pd.DataFrame(out)


@st.cache_data(ttl=SNAPSHOT_CACHE_TTL, show_spinner=False)
def fetch_official_twse_snapshot() -> Tuple[pd.DataFrame, str]:
    s = make_retry_session(base_headers=get_browser_headers(OFFICIAL_TWSE_STOCK_DAY_ALL))
    r = safe_get(s, OFFICIAL_TWSE_STOCK_DAY_ALL, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    rows = r.json()
    df = _normalize_official_rows(rows, market="tse")
    return df, ""


@st.cache_data(ttl=SNAPSHOT_CACHE_TTL, show_spinner=False)
def fetch_official_tpex_snapshot(date_str: str) -> Tuple[pd.DataFrame, str]:
    s = make_retry_session(base_headers=get_browser_headers(OFFICIAL_TPEX_DAILY_CLOSE))
    params = {"l": "zh-tw", "d": date_str, "s": "0,asc,0"}
    r = safe_get(s, OFFICIAL_TPEX_DAILY_CLOSE, timeout=HTTP_TIMEOUT, params=params)
    r.raise_for_status()
    rows = r.json()
    df = _normalize_official_rows(rows, market="otc", asof=date_str)
    return df, date_str


@st.cache_data(ttl=SNAPSHOT_CACHE_TTL, show_spinner=False)
def fetch_official_combined_snapshot(max_rows: int | None = 300):
    errors = []
    twse_df = pd.DataFrame()
    try:
        twse_df, _ = fetch_official_twse_snapshot()
    except Exception as e:
        errors.append(f"TWSE官方: {type(e).__name__}: {e}")

    tpex_df = pd.DataFrame()
    tpex_asof = ""
    today = now_taipei().replace(hour=0, minute=0, second=0, microsecond=0)
    for i in range(0, 8):
        probe = today - timedelta(days=i)
        try:
            tpex_df, tpex_asof = fetch_official_tpex_snapshot(tw_roc_date(probe))
            if tpex_df is not None and not tpex_df.empty:
                break
        except Exception as e:
            errors.append(f"TPEX官方[{tw_roc_date(probe)}]: {type(e).__name__}: {e}")

    if (twse_df is None or twse_df.empty) and (tpex_df is None or tpex_df.empty):
        raise RuntimeError(" | ".join(errors) if errors else "官方來源皆失敗")

    frames = []
    if twse_df is not None and not twse_df.empty:
        frames.append(twse_df)
    if tpex_df is not None and not tpex_df.empty:
        frames.append(tpex_df)
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if df.empty:
        raise RuntimeError("官方來源回傳空資料")
    df = df.sort_values(["vol_lots", "chg_pct"], ascending=[False, False]).drop_duplicates("code", keep="first")
    asof = roc_to_gregorian(tpex_asof) if tpex_asof else now_taipei().strftime("%Y/%m/%d")
    if max_rows is not None:
        df = df.head(max_rows)
    return df.reset_index(drop=True), asof, "官方每日資料(TWSE+TPEX)", errors


# =========================
# HTML FALLBACK SOURCES
# =========================
def _extract_tables_with_bs4(html: str) -> List[pd.DataFrame]:
    soup = BeautifulSoup(html, "html.parser")
    tables = []
    for table in soup.find_all("table"):
        rows = []
        headers = []
        trs = table.find_all("tr")
        if not trs:
            continue
        for tr in trs:
            ths = tr.find_all(["th"])
            if ths and not headers:
                headers = [re.sub(r"\s+", "", th.get_text(" ", strip=True)) for th in ths]
                continue
            tds = tr.find_all(["td"])
            if tds:
                row = [td.get_text(" ", strip=True) for td in tds]
                rows.append(row)
        if not rows:
            continue
        width = max(len(headers), max(len(r) for r in rows))
        headers = headers[:width] + [f"col{i}" for i in range(len(headers), width)]
        normalized = [r[:width] + [""] * (width - len(r)) for r in rows]
        df = pd.DataFrame(normalized, columns=headers)
        if not df.empty:
            tables.append(df)
    return tables


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


def _pick_best_table(tables, required_tokens):
    best_df = None
    best_score = -1
    for t in tables:
        score = sum(1 for token in required_tokens if any(token in str(c) for c in t.columns))
        if score > best_score:
            best_df = t.copy()
            best_score = score
    return best_df, best_score


def _parse_yahoo_table(html: str):
    tables = _extract_tables_with_bs4(html)
    df, score = _pick_best_table(tables, ["股名", "股號", "股價", "漲跌", "漲跌幅", "最高", "最低", "成交量"])
    if df is None or score < 5:
        raise ValueError("Yahoo 表格結構不符預期")

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
    out["name"] = out["name_code"].astype(str).str.replace(r"([0-9]{4,6}[A-Z]?)\.(?:TW|TWO)", "", regex=True).apply(_clean_name)

    for c in ["last", "chg", "chg_pct", "high", "low", "vol_lots"]:
        out[c] = out[c].apply(_to_float)
    out["prev_close"] = out["last"] - out["chg"]
    out["market"] = ""
    out = out.dropna(subset=["code", "last", "high", "low", "vol_lots"])
    out = out[out["last"] > 0].copy()

    m = re.search(r"資料時間[:：]?\s*(\d{4}/\d{2}/\d{2}(?:\s+\d{2}:\d{2})?)", html)
    asof = m.group(1) if m else ""
    return out[["code", "name", "last", "chg", "chg_pct", "high", "low", "vol_lots", "prev_close", "market"]].reset_index(drop=True), asof


def _parse_histock_table(html: str):
    tables = _extract_tables_with_bs4(html)
    df, score = _pick_best_table(tables, ["代號", "名稱", "價格", "漲跌", "漲跌幅", "最高", "最低", "昨收", "成交量"])
    if df is None or score < 6:
        raise ValueError("HiStock 表格結構不符預期")

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

    out["code"] = out["code"].apply(_normalize_code)
    out["name"] = out["name"].apply(_clean_name)
    for c in ["last", "chg_pct", "high", "low", "vol_lots"]:
        out[c] = out[c].apply(_to_float)
    out["chg"] = out.get("chg", pd.Series([math.nan] * len(out))).apply(_to_float)
    out["prev_close"] = out.get("prev_close", pd.Series([math.nan] * len(out))).apply(_to_float)
    need_change = out["chg"].isna() & out["prev_close"].notna() & out["last"].notna()
    out.loc[need_change, "chg"] = out.loc[need_change, "last"] - out.loc[need_change, "prev_close"]
    out["market"] = ""
    out = out.dropna(subset=["code", "last", "high", "low", "vol_lots"])
    out = out[out["last"] > 0].copy()

    date_m = re.search(r"(\d{2}-\d{2})\s+Top", html)
    time_m = re.search(r"本地時間[:：]\s*(\d{1,2}:\d{2})", html)
    asof = f"{date_m.group(1)} {time_m.group(1)}" if date_m and time_m else (time_m.group(1) if time_m else "")
    return out[["code", "name", "last", "chg", "chg_pct", "high", "low", "vol_lots", "prev_close", "market"]].reset_index(drop=True), asof


def _parse_wantgoo_table(html: str):
    tables = _extract_tables_with_bs4(html)
    df, score = _pick_best_table(tables, ["代碼", "股票", "成交價", "最高", "最低", "成交量"])
    if df is None or score < 4:
        raise ValueError("WantGoo 表格結構不符預期")

    code_col = _find_col(df, ["代碼"])
    name_col = _find_col(df, ["股票"])
    price_col = _find_col(df, ["成交價"])
    change_col = _find_col(df, ["漲跌"])
    pct_col = _find_col(df, ["漲跌"])
    high_col = _find_col(df, ["最高"])
    low_col = _find_col(df, ["最低"])
    vol_col = _find_col(df, ["成交量"])
    needed = [code_col, name_col, price_col, change_col, high_col, low_col, vol_col]
    if any(c is None for c in needed):
        raise ValueError(f"WantGoo 欄位不足: {df.columns.tolist()}")

    out = df[[code_col, name_col, price_col, change_col, high_col, low_col, vol_col]].copy()
    out.columns = ["code", "name", "last", "chg", "high", "low", "vol_lots"]
    out["code"] = out["code"].apply(_normalize_code)
    out["name"] = out["name"].apply(_clean_name)
    for c in ["last", "chg", "high", "low", "vol_lots"]:
        out[c] = out[c].apply(_to_float)
    out["prev_close"] = out["last"] - out["chg"]
    out["chg_pct"] = ((_safe_pct(1, 1)) * 0.0)
    out["chg_pct"] = out.apply(lambda r: (_safe_pct(r["last"] - r["prev_close"], r["prev_close"]) * 100) if pd.notna(r["prev_close"]) and r["prev_close"] > 0 else math.nan, axis=1)
    out["market"] = ""
    out = out.dropna(subset=["code", "last", "high", "low", "vol_lots"])
    out = out[out["last"] > 0].copy()

    m = re.search(r"(\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2})", html)
    asof = m.group(1) if m else ""
    return out[["code", "name", "last", "chg", "chg_pct", "high", "low", "vol_lots", "prev_close", "market"]].reset_index(drop=True), asof


@st.cache_data(ttl=RANK_CACHE_TTL, show_spinner=False)
def _fetch_rank_html(url: str):
    session = make_retry_session(base_headers=get_browser_headers(url))
    r = safe_get(session, url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.text


@st.cache_data(ttl=RANK_CACHE_TTL, show_spinner=False)
def fetch_html_fallback_snapshot(max_rows: int = 300):
    errors = []
    for spec in HTML_FALLBACK_SOURCES:
        try:
            html = _fetch_rank_html(spec["url"])
            if spec["kind"] == "yahoo":
                df, asof = _parse_yahoo_table(html)
            elif spec["kind"] == "histock":
                df, asof = _parse_histock_table(html)
            else:
                df, asof = _parse_wantgoo_table(html)
            if df is not None and not df.empty:
                df = df.sort_values(["vol_lots", "chg_pct"], ascending=[False, False]).drop_duplicates("code", keep="first")
                return df.head(max_rows).reset_index(drop=True), asof, spec["name"], errors
            errors.append(f"{spec['name']}: EMPTY")
        except Exception as e:
            errors.append(f"{spec['name']}: {type(e).__name__}: {e}")
    raise RuntimeError(" | ".join(errors) if errors else "備用網頁資料全部失敗")


# =========================
# SNAPSHOT ORCHESTRATION
# =========================
def market_phase(now_ts: datetime) -> str:
    hhmm = now_ts.time()
    if hhmm < dtime(9, 0):
        return "pre"
    if hhmm <= dtime(13, 45):
        return "live"
    return "post"


def merge_meta(base_meta: Dict[str, dict], source_df: pd.DataFrame) -> Dict[str, dict]:
    meta = dict(base_meta or {})
    if source_df is None or source_df.empty:
        return meta
    for _, r in source_df.iterrows():
        code = str(r.get("code", "")).strip()
        market = str(r.get("market", "")).strip().lower()
        name = str(r.get("name", "")).strip() or code
        if not code:
            continue
        if code not in meta and market in ("tse", "otc"):
            meta[code] = {"name": name, "ex": market}
        elif code in meta and not meta[code].get("name"):
            meta[code]["name"] = name
    return meta


def enrich_market_from_meta(df: pd.DataFrame, meta_dict: Dict[str, dict]) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()
    markets = []
    names = []
    for _, r in df.iterrows():
        code = str(r.get("code", "")).strip()
        info = meta_dict.get(code, {})
        markets.append(info.get("ex", str(r.get("market", ""))))
        names.append(info.get("name", str(r.get("name", code))))
    df["market"] = markets
    df["name"] = names
    return df


def fetch_rank_snapshot(status_placeholder, diag, meta_dict):
    now_ts = now_taipei()
    phase = market_phase(now_ts)
    diag["source_mode"] = "優先使用官方資料"
    errors = []
    official_backup_df = pd.DataFrame()
    official_backup_asof = ""

    # 盤後/盤前優先用官方收盤快照；盤中先試官方，若日期明顯落後再切 HTML live。
    try:
        status_placeholder.update(label="🏛️ 正在檢查官方資料庫...", state="running")
        official_df, official_asof, official_name, official_errors = fetch_official_combined_snapshot(None)
        errors.extend(official_errors)
        official_df = enrich_market_from_meta(official_df, merge_meta(meta_dict, official_df))
        official_backup_df = official_df.copy()
        official_backup_asof = official_asof
        official_is_today = False
        if official_asof:
            try:
                official_date = pd.to_datetime(str(official_asof).replace("/", "-"), errors="coerce")
                official_is_today = pd.notna(official_date) and official_date.date() == now_ts.date()
            except Exception:
                official_is_today = False

        if phase != "live" or official_is_today:
            diag["rank_source"] = official_name
            diag["rank_asof"] = official_asof
            return official_df, official_backup_df, official_backup_asof
        errors.append(f"官方日期較舊: {official_asof}")
    except Exception as e:
        diag["rank_req_err"] += 1
        diag_err(diag, e, "OFFICIAL_FETCH")
        errors.append(f"官方資料錯誤: {type(e).__name__}: {e}")

    try:
        status_placeholder.update(label="📡 官方沒資料，啟動備用網頁抓取...", state="running")
        html_df, html_asof, html_name, html_errors = fetch_html_fallback_snapshot(320)
        errors.extend(html_errors)
        html_df = enrich_market_from_meta(html_df, meta_dict)
        if html_df is not None and not html_df.empty:
            diag["rank_source"] = html_name
            diag["rank_asof"] = html_asof
            diag["source_mode"] = "官方失敗 / 改用網頁備用資料"
            return html_df, official_backup_df, official_backup_asof
    except Exception as e:
        diag["rank_req_err"] += 1
        diag_err(diag, e, "HTML_FALLBACK")
        errors.append(f"網頁備用失敗: {type(e).__name__}: {e}")

    diag["rank_source"] = "全部失敗"
    for msg in errors[-5:]:
        diag_err(diag, Exception(msg), "RANK_CHAIN")
    return pd.DataFrame(), official_backup_df, official_backup_asof


# =========================
# CANDIDATE ENGINE
# =========================
def build_rank_candidates(raw_df, meta_dict, now_ts, is_test, diag):
    rows = []
    diag["rank_seen"] = len(raw_df)
    diag["rank_parse_ok"] = 0
    diag["rank_parse_fail"] = 0
    diag["rank_rows"] = 0
    diag["cand_total"] = 0

    m = int((datetime.combine(now_ts.date(), now_ts.time()) - datetime.combine(now_ts.date(), dtime(9, 0))).total_seconds() // 60)
    m = max(0, min(270, m))
    dist_limit = 6.0 if is_test else (4.6 if m <= 60 else 3.2 if m <= 180 else 2.4)
    vol_limit_lots = 200 if is_test else 2500
    chg_pct_min = -0.5 if is_test else 0.3

    for _, q in raw_df.iterrows():
        c = str(q.get("code", "")).strip()
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
        df = df.sort_values(["dist", "vol_sh", "chg_pct"], ascending=[True, False, False]).drop_duplicates("code", keep="first")
    diag["rank_rows"] = len(df)
    diag["cand_total"] = len(df)
    return df


def core_filter_engine(candidates_df, meta_dict, now_ts, is_test, diag, use_bloodline):
    stats = {"總共測試檔數": 0, "交易熱度不夠": [], "從高點跌落太多": [], "最後沒力氣維持高價": [], "過去沒有連續大漲紀錄": []}
    yf_diag = {"yf_symbols": 0, "yf_fail": 0, "other_err": 0}
    if candidates_df.empty:
        return pd.DataFrame(), stats, yf_diag

    candidates_df = candidates_df.sort_values(["dist", "vol_sh"], ascending=[True, False]).head(80)
    stats["總共測試檔數"] = len(candidates_df)
    syms = [f"{c}.{'TW' if meta_dict[c]['ex'] == 'tse' else 'TWO'}" for c in candidates_df["code"] if c in meta_dict]
    yf_diag["yf_symbols"] = len(syms)

    if (not HAS_YF) or (not syms):
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
                    stats["從高點跌落太多"].append(f"{c} {name}")
                    continue
                if rng > 0.1 and (r["last"] - r["low"]) / max(1e-9, rng) < (0.5 if is_test else 0.80):
                    stats["最後沒力氣維持高價"].append(f"{c} {name}")
                    continue
                near_limit = abs(r["last"] - r["upper"]) <= max(tw_tick(r["upper"]), r["upper"] * 0.0005)
                high_is_limit = abs(r["high"] - r["upper"]) <= max(tw_tick(r["upper"]), r["upper"] * 0.0005)
                status = "🔥 漲到頂點 (難買到)" if near_limit and high_is_limit else ("🚀 快要漲到最高限額了" if near_limit else "⚡ 開始衝刺")
                base_lots = 200.0 if is_test else 2500.0
                vol_score = max(0.1, min(99.9, (r["vol_sh"] / 1000.0) / base_lots))
                results.append({
                    "代號": c,
                    "名稱": name,
                    "目前價格": r["last"],
                    "交易熱度": vol_score,
                    "狀態": status + "（標準放寬）",
                    "階段": "初步入選",
                    "board_val": 0,
                    "上漲幅度%": r.get("chg_pct", 0.0),
                })
            except Exception as e:
                yf_diag["other_err"] += 1
                diag_err(diag, e, "FILTER_RANK_ONLY")
        res_df = pd.DataFrame(results)
        if not res_df.empty:
            res_df = res_df.sort_values(["上漲幅度%", "交易熱度"], ascending=[False, False])
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
                res_frames.append(yf_download_daily(tuple(part), "180d"))
            except Exception as e:
                diag_err(diag, e, "YF_PART_FAIL")
                res_frames.append(None)
        return res_frames

    try:
        raw_daily = yf_download_daily(tuple(syms), "180d")
        if raw_daily is None or getattr(raw_daily, "empty", False):
            raise Exception("YF_BULK_EMPTY")
        diag["yf_rescue_used"] = 0
    except Exception as e:
        diag_err(diag, e, "YF_BULK_FAIL")
        diag["yf_bulk_fail"] += 1
        diag["yf_rescue_used"] = 1
        mid = max(1, len(syms) // 2)
        frames1 = try_yf_parts([syms[:mid], syms[mid:]])
        diag["yf_parts_ok"] += sum(1 for f in frames1 if f is not None and not getattr(f, "empty", False))
        diag["yf_parts_fail"] += sum(1 for f in frames1 if f is None or getattr(f, "empty", False))
        frames_ok = [f for f in frames1 if f is not None and not getattr(f, "empty", False)]
        if frames_ok:
            raw_daily = pd.concat(frames_ok, axis=1)
            if not isinstance(raw_daily.columns, pd.MultiIndex):
                raw_daily.columns = pd.MultiIndex.from_product([[syms[0]], raw_daily.columns])
            raw_daily = raw_daily.loc[:, ~raw_daily.columns.duplicated()]
            raw_daily = raw_daily[~raw_daily.index.duplicated(keep="last")].sort_index()

    diag["t_yf"] = time.perf_counter() - t_yf_start
    if raw_daily is None or getattr(raw_daily, "empty", False):
        yf_diag["other_err"] += 1
        return pd.DataFrame(), stats, yf_diag

    diag["yf_returned"] = int(raw_daily.columns.get_level_values(0).nunique()) if isinstance(raw_daily.columns, pd.MultiIndex) else 1
    results, today_date = [], now_ts.date()
    m = int((datetime.combine(now_ts.date(), now_ts.time()) - datetime.combine(now_ts.date(), dtime(9, 0))).total_seconds() // 60)
    m = max(0, min(270, m))
    frac = 0.5 if is_test else (0.12 if m <= 30 else 0.12 + (0.5 - 0.12) * ((m - 30) / 90.0) if m <= 120 else min(1.0, 0.5 + (1.0 - 0.5) * ((m - 120) / 150.0)))
    pb_lim = 0.05 if is_test else (0.015 if m <= 90 else 0.006)

    for _, r in candidates_df.iterrows():
        c, name = r["code"], meta_dict[r["code"]]["name"]
        sym = f"{c}.{'TW' if meta_dict[c]['ex'] == 'tse' else 'TWO'}"
        try:
            df_sym = raw_daily[sym] if isinstance(raw_daily.columns, pd.MultiIndex) and sym in raw_daily.columns.get_level_values(0) else raw_daily
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
                stats["過去沒有連續大漲紀錄"].append(f"{c} {name}")
                continue
            vol_ratio = r["vol_sh"] / (vol_ma20_sh * frac + 1e-9)
            if vol_ratio < (0.5 if is_test else 1.3):
                stats["交易熱度不夠"].append(f"{c} {name}")
                continue
            rng = max(0.0, r["high"] - r["low"])
            if (r["high"] - r["last"]) / max(1e-9, r["high"]) > pb_lim:
                stats["從高點跌落太多"].append(f"{c} {name}")
                continue
            if rng > 0.1 and (r["last"] - r["low"]) / max(1e-9, rng) < (0.5 if is_test else 0.80):
                stats["最後沒力氣維持高價"].append(f"{c} {name}")
                continue
            near_limit = abs(r["last"] - r["upper"]) <= max(tw_tick(r["upper"]), r["upper"] * 0.0005)
            high_is_limit = abs(r["high"] - r["upper"]) <= max(tw_tick(r["upper"]), r["upper"] * 0.0005)
            status = "🔥 漲到頂點 (難買到)" if near_limit and high_is_limit and abs(r["last"] - r["high"]) <= max(tw_tick(r["last"]), r["last"] * 0.0005) else ("🚀 快要漲到最高限額了" if near_limit else "⚡ 開始衝刺")
            results.append({
                "代號": c,
                "名稱": name,
                "目前價格": r["last"],
                "交易熱度": vol_ratio,
                "狀態": status,
                "階段": f"連續 {past_boards + 1} 天大漲",
                "board_val": past_boards,
                "上漲幅度%": r["chg_pct"],
            })
        except Exception as e:
            yf_diag["other_err"] += 1
            diag_err(diag, e, "FILTER")

    res_df = pd.DataFrame(results)
    if not res_df.empty:
        res_df = res_df.sort_values(["board_val", "交易熱度", "上漲幅度%"], ascending=[False, False, False])
    return res_df, stats, yf_diag


# =========================
# CALIBRATION / QUALITY REVIEW
# =========================
def _future_max_return(series_high: pd.Series, series_close: pd.Series, horizon: int) -> pd.Series:
    future_highs = [series_high.shift(-i) for i in range(1, horizon + 1)]
    if not future_highs:
        return pd.Series(index=series_close.index, dtype=float)
    max_future = pd.concat(future_highs, axis=1).max(axis=1)
    return max_future / series_close - 1.0


@st.cache_data(ttl=YF_CACHE_TTL, show_spinner=False)
def calibrate_signal_quality(symbols: Tuple[str, ...], lookback_days: int = CALIBRATION_LOOKBACK_DAYS):
    if (not HAS_YF) or (not symbols):
        return {"status": "skip", "reason": "yfinance unavailable or no symbols"}
    raw = yf_download_daily(symbols, f"{max(lookback_days + 60, 240)}d")
    if raw is None or getattr(raw, "empty", False):
        return {"status": "empty", "reason": "no yfinance data"}

    records = []
    for sym in symbols:
        if isinstance(raw.columns, pd.MultiIndex):
            if sym not in raw.columns.get_level_values(0):
                continue
            df = raw[sym].copy()
        else:
            df = raw.copy()
        if not {"Open", "High", "Low", "Close", "Volume"}.issubset(df.columns):
            continue
        df = df[["Open", "High", "Low", "Close", "Volume"]].dropna().copy()
        if len(df) < 60:
            continue
        df = df.tail(max(lookback_days + 20, 120)).copy()
        df["prev_close"] = df["Close"].shift(1)
        df["vol_ma20"] = df["Volume"].rolling(20).mean()
        df["range"] = (df["High"] - df["Low"]).clip(lower=0)
        df["pos_in_range"] = (df["Close"] - df["Low"]) / df["range"].replace(0, math.nan)
        df["upper"] = df["prev_close"].apply(lambda x: calc_limit_up(float(x), 0.10) if pd.notna(x) and x > 0 else math.nan)
        df["tick"] = df["upper"].apply(lambda x: tw_tick(float(x)) if pd.notna(x) and x > 0 else math.nan)
        df["near_limit"] = (df["Close"] >= (df["upper"] - df["tick"] * 1.2))
        df["signal"] = (
            df["prev_close"].gt(0)
            & df["vol_ma20"].gt(0)
            & df["Volume"].ge(df["vol_ma20"] * 1.6)
            & df["near_limit"].fillna(False)
            & df["pos_in_range"].ge(0.72)
        )
        df["ret_1d"] = df["Close"].shift(-1) / df["Close"] - 1.0
        df["ret_3d"] = df["Close"].shift(-3) / df["Close"] - 1.0
        df["ret_5d"] = df["Close"].shift(-5) / df["Close"] - 1.0
        df["max_3d"] = _future_max_return(df["High"], df["Close"], 3)
        df["max_5d"] = _future_max_return(df["High"], df["Close"], 5)
        hits = df[df["signal"]].copy()
        if hits.empty:
            continue
        hits["symbol"] = sym
        records.append(hits[["symbol", "ret_1d", "ret_3d", "ret_5d", "max_3d", "max_5d"]])

    if not records:
        return {"status": "empty", "reason": "no historical signals"}

    all_hits = pd.concat(records, ignore_index=True)
    all_hits = all_hits.dropna(subset=["ret_1d", "max_3d"])
    if all_hits.empty:
        return {"status": "empty", "reason": "signals have no forward data"}

    summary = {
        "status": "ok",
        "signal_count": int(len(all_hits)),
        "symbol_count": int(all_hits["symbol"].nunique()),
        "avg_1d": float(all_hits["ret_1d"].mean() * 100),
        "avg_3d": float(all_hits["ret_3d"].mean() * 100),
        "avg_5d": float(all_hits["ret_5d"].mean() * 100),
        "avg_max_3d": float(all_hits["max_3d"].mean() * 100),
        "avg_max_5d": float(all_hits["max_5d"].mean() * 100),
        "win_3d_gt3": float((all_hits["max_3d"] >= 0.03).mean() * 100),
        "win_5d_gt5": float((all_hits["max_5d"] >= 0.05).mean() * 100),
    }
    if summary["signal_count"] >= 18 and summary["win_3d_gt3"] >= 55 and summary["avg_max_3d"] >= 4.0:
        score = 9
    elif summary["signal_count"] >= 12 and summary["win_3d_gt3"] >= 48 and summary["avg_max_3d"] >= 3.0:
        score = 8
    elif summary["signal_count"] >= 8 and summary["win_3d_gt3"] >= 42:
        score = 7
    elif summary["signal_count"] >= 5 and summary["win_3d_gt3"] >= 35:
        score = 6
    else:
        score = 5
    summary["score"] = score
    return summary


# =========================
# SNAPSHOT RECOMPUTE
# =========================
def make_snapshot_diag(meta_count, fetch_diag):
    diag = diag_init()
    diag["meta_count"] = meta_count
    for k, v in fetch_diag.items():
        if k == "last_errors":
            diag["last_errors"] = deque(v, maxlen=12)
        else:
            diag[k] = v
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


# =========================
# SINGLE SEARCH / SCORING
# =========================


def format_rank_table_html(table_df: pd.DataFrame) -> str:
    if table_df is None or getattr(table_df, "empty", False):
        return '<div class="rank-table-empty">目前沒有可顯示的資料。</div>'

    df = table_df.copy().reset_index(drop=True)
    df.insert(0, "#", [i + 1 for i in range(len(df))])

    def fmt_value(col, val):
        if pd.isna(val):
            return "—"
        if col == "目前價格":
            try:
                return f"{float(val):.2f}".rstrip("0").rstrip(".")
            except Exception:
                return html.escape(str(val))
        if col == "交易熱度":
            try:
                return f"{float(val):.2f}x"
            except Exception:
                return html.escape(str(val))
        if col == "上漲幅度%":
            try:
                return f"{float(val):.2f}%"
            except Exception:
                return html.escape(str(val))
        return html.escape(str(val))

    def cell_html(col, val):
        safe = fmt_value(col, val)
        if col == "#":
            return f'<td class="c-rank">{safe}</td>'
        if col == "代號":
            return f'<td class="c-code">{safe}</td>'
        if col == "名稱":
            return f'<td class="c-name">{safe}</td>'
        if col == "目前價格":
            return f'<td class="c-price">{safe}</td>'
        if col == "交易熱度":
            return f'<td class="c-power">{safe}</td>'
        if col == "狀態":
            cls = "is-live"
            txt = str(val)
            if "頂點" in txt or "難買" in txt or "鎖價" in txt:
                cls = "is-lock"
            elif "觀察" in txt or "盤整" in txt:
                cls = "is-watch"
            return f'<td><span class="status-pill {cls}">{html.escape(txt)}</span></td>'
        if col == "階段":
            return f'<td><span class="stage-pill">{safe}</span></td>'
        if col == "上漲幅度%":
            try:
                num = float(val)
            except Exception:
                num = 0.0
            tone = "up" if num >= 9.5 else "mid" if num >= 6 else "flat"
            return f'<td class="c-chg {tone}">{safe}</td>'
        return f'<td>{safe}</td>'

    headers = ''.join([f'<th>{html.escape(str(c))}</th>' for c in df.columns])
    rows = []
    for _, row in df.iterrows():
        cells = ''.join(cell_html(col, row[col]) for col in df.columns)
        rows.append(f'<tr>{cells}</tr>')
    body = ''.join(rows)

    return (
        '<div class="rank-table-shell">'
        '<div class="rank-table-toolbar">'
        '<div>'
        '<div class="rank-table-title">嚴選強勢股名單</div>'
        '<div class="rank-table-subtitle">簡單明瞭｜快速掃描市場焦點</div>'
        '</div>'
        f'<div class="rank-table-count">共 {len(df)} 檔</div>'
        '</div>'
        '<div class="rank-table-wrap">'
        '<table class="rank-table">'
        f'<thead><tr>{headers}</tr></thead>'
        f'<tbody>{body}</tbody>'
        '</table>'
        '</div>'
        '</div>'
    )

def _score_badge(score: int) -> str:
    if score >= 9:
        return "S級 (極度強勢)"
    if score >= 8:
        return "A級 (強烈表現)"
    if score >= 7:
        return "B級 (值得觀察)"
    if score >= 5:
        return "C級 (表現普通)"
    return "D級 (目前弱勢)"


def _resolve_search_row(query: str, snapshot: dict):
    q = str(query or "").strip()
    if not q:
        return None, []
    q_lower = q.lower()
    raw_df = snapshot.get("raw_rank_df", pd.DataFrame())
    backup_df = snapshot.get("search_backup_df", pd.DataFrame())
    meta = snapshot.get("meta", {})

    def _match_df(df: pd.DataFrame, source_label: str, stale_note: str = ""):
        if df is None or df.empty:
            return None, []
        work = df.copy()
        work["code"] = work["code"].astype(str).str.strip()
        work["name"] = work["name"].astype(str).str.strip()
        exact_code = work[work["code"] == q]
        exact_name = work[work["name"] == q]
        fuzzy = work[work["code"].str.contains(q, case=False, na=False) | work["name"].str.lower().str.contains(q_lower, na=False)]
        target = None
        if not exact_code.empty:
            target = exact_code.iloc[0].to_dict()
        elif not exact_name.empty:
            target = exact_name.iloc[0].to_dict()
        elif not fuzzy.empty:
            target = fuzzy.iloc[0].to_dict()
        matches = []
        if not fuzzy.empty:
            for _, rr in fuzzy.head(8).iterrows():
                matches.append(f"{rr['code']} {rr['name']}")
        if target is not None:
            target["search_source"] = source_label
            target["stale_note"] = stale_note
        return target, matches

    row, matches = _match_df(raw_df, "最新資料快取")
    if row is not None:
        return row, matches

    backup_asof = snapshot.get("search_backup_asof", "")
    stale_note = f"官方前一日資料 {backup_asof}" if backup_asof else "官方前一日資料"
    row, matches = _match_df(backup_df, stale_note, stale_note)
    if row is not None:
        return row, matches

    meta_matches = []
    for code, info in meta.items():
        name = str(info.get("name", code))
        if q == code or q_lower in name.lower():
            meta_matches.append(f"{code} {name}")
    return None, meta_matches[:8]


def evaluate_single_stock(row: dict, meta_dict: dict, now_ts: datetime, is_test: bool, use_bloodline: bool):
    code = str(row.get("code", "")).strip()
    name = str(meta_dict.get(code, {}).get("name", row.get("name", code)))
    market = str(meta_dict.get(code, {}).get("ex", row.get("market", "")))
    last = float(row.get("last", math.nan))
    high = float(row.get("high", math.nan))
    low = float(row.get("low", math.nan))
    vol_lots = float(row.get("vol_lots", math.nan))
    chg = float(row.get("chg", 0.0)) if pd.notna(row.get("chg", 0.0)) else 0.0
    chg_pct = float(row.get("chg_pct", 0.0)) if pd.notna(row.get("chg_pct", 0.0)) else 0.0
    prev_close = float(row.get("prev_close", math.nan)) if pd.notna(row.get("prev_close", math.nan)) else math.nan
    if (not math.isfinite(prev_close)) or prev_close <= 0:
        prev_close = round(last - chg, 2)
    if last <= 0 or prev_close <= 0:
        return {"status": "error", "message": "這檔股票目前沒有足夠資料可以評估。"}

    upper = calc_limit_up(prev_close, 0.10)
    dist_pct = max(0.0, ((upper - last) / upper) * 100)
    rng = max(0.0, high - low)
    range_pos = 1.0 if rng <= 0 else (last - low) / max(rng, 1e-9)
    pullback_pct = ((high - last) / high) * 100 if high > 0 else 0.0
    near_limit = abs(last - upper) <= max(tw_tick(upper), upper * 0.0005)
    high_is_limit = abs(high - upper) <= max(tw_tick(upper), upper * 0.0005)

    score = 5.0
    strengths, warnings, filter_flags = [], [], []

    if dist_pct <= 1.0:
        score += 1.8; strengths.append("距離今天最高限額非常近")
    elif dist_pct <= 2.0:
        score += 1.2; strengths.append("接近今天最高限額")
    elif dist_pct <= 4.0:
        score += 0.6
    else:
        score -= 0.8; warnings.append("離最高限額還有一段距離")

    if chg_pct >= 9.0:
        score += 1.6; strengths.append("漲幅接近單日極限")
    elif chg_pct >= 7.0:
        score += 1.2
    elif chg_pct >= 5.0:
        score += 0.8
    elif chg_pct >= 3.0:
        score += 0.4
    elif chg_pct < 0:
        score -= 1.2; warnings.append("目前價格比昨天還低")

    if range_pos >= 0.82:
        score += 1.0; strengths.append("價格穩在今天相對高點")
    elif range_pos >= 0.65:
        score += 0.5
    elif range_pos < (0.5 if is_test else 0.80) and rng > 0.1:
        score -= 1.1; warnings.append("目前價格掉回今天較低的位置")
        filter_flags.append("最後沒力氣維持高價")

    pb_lim = 5.0 if is_test else (1.5 if now_ts.time() <= dtime(10, 30) else 0.9)
    if pullback_pct <= pb_lim * 0.45:
        score += 0.6; strengths.append("高點掉下來的幅度很小")
    elif pullback_pct > pb_lim:
        score -= 1.0; warnings.append("從今天最高點跌落太多")
        filter_flags.append("從高點跌落太多")

    board_count = 0
    vol_ratio = math.nan
    vol_note = ""
    yf_status = "放寬標準計算"

    if HAS_YF and code in meta_dict:
        sym = f"{code}.{'TW' if market == 'tse' else 'TWO'}"
        raw = yf_download_daily((sym,), "180d")
        if raw is not None and not getattr(raw, "empty", False):
            try:
                df_sym = raw[sym] if isinstance(raw.columns, pd.MultiIndex) else raw
                dfD = df_sym[["Close", "Volume"]].dropna()
                if len(dfD) >= 30:
                    dates_tw = idx_date_taipei(dfD.index)
                    past_df = dfD[dates_tw < now_ts.date()].copy()
                    if len(past_df) >= 30:
                        vol_ma20_sh = float(past_df["Volume"].rolling(20).mean().iloc[-1])
                        if math.isfinite(vol_ma20_sh) and vol_ma20_sh > 0:
                            m = int((datetime.combine(now_ts.date(), now_ts.time()) - datetime.combine(now_ts.date(), dtime(9, 0))).total_seconds() // 60)
                            m = max(0, min(270, m))
                            frac = 0.5 if is_test else (0.12 if m <= 30 else 0.12 + (0.5 - 0.12) * ((m - 30) / 90.0) if m <= 120 else min(1.0, 0.5 + (1.0 - 0.5) * ((m - 120) / 150.0)))
                            vol_ratio = (vol_lots * 1000.0) / (vol_ma20_sh * frac + 1e-9)
                            yf_status = "有足夠歷史資料對比"
                            vol_note = f"和過去20天相比，熱度達 {vol_ratio:.2f} 倍"

                    past_10 = past_df.tail(10)
                    for i in range(len(past_10) - 1, 0, -1):
                        cp, pp = float(past_10["Close"].iloc[i]), float(past_10["Close"].iloc[i - 1])
                        lim = infer_daily_limit(pp, cp)
                        if cp >= (lim - tw_tick(lim)):
                            board_count += 1
                        else:
                            break
            except Exception:
                pass

    if not math.isfinite(vol_ratio):
        base_lots = 200.0 if is_test else 2500.0
        vol_ratio = max(0.05, vol_lots / base_lots)
        vol_note = f"以固定基準評估熱度 {vol_ratio:.2f} 倍"

    if vol_ratio >= 2.5:
        score += 1.6; strengths.append("交易量異常熱絡")
    elif vol_ratio >= 1.8:
        score += 1.1
    elif vol_ratio >= 1.3:
        score += 0.6
    elif vol_ratio < (0.5 if is_test else 1.3):
        score -= 1.2; warnings.append("交易熱度不足")
        filter_flags.append("交易熱度不夠")

    if board_count >= 2:
        score += 0.9; strengths.append("過去常有連續大漲紀錄")
    elif board_count == 1:
        score += 0.5; strengths.append("之前曾經大漲過")
    elif use_bloodline and HAS_YF and (not is_test):
        score -= 0.8; warnings.append("近期沒有連續大漲的紀錄")
        filter_flags.append("過去沒有連續大漲紀錄")

    score = int(max(0, min(10, round(score))))

    if near_limit and high_is_limit and pullback_pct <= max(0.2, pb_lim * 0.2):
        action = "表現優異，值得緊盯"
        status = "🔥 漲到頂點 (難買到)"
    elif score >= 8 and len(filter_flags) <= 1:
        action = "表現強勁，可持續觀察"
        status = "🚀 快要漲到最高限額了" if near_limit else "⚡ 強勢發動"
    elif score >= 6:
        action = "普通，需要再看看"
        status = "👀 持續觀察"
    else:
        action = "目前偏弱，不建議買進"
        status = "🧊 動能偏弱"

    return {
        "status": "ok",
        "code": code,
        "name": name,
        "market": market,
        "last": last,
        "chg_pct": chg_pct,
        "vol_lots": vol_lots,
        "prev_close": prev_close,
        "upper": upper,
        "dist_pct": dist_pct,
        "range_pos": range_pos * 100,
        "pullback_pct": pullback_pct,
        "board_count": board_count,
        "vol_ratio": vol_ratio,
        "vol_note": vol_note,
        "score": score,
        "badge": _score_badge(score),
        "action": action,
        "live_status": status,
        "strengths": strengths[:5],
        "warnings": warnings[:5],
        "filter_flags": filter_flags[:5],
        "query_source": row.get("search_source", "最新資料快取"),
        "stale_note": row.get("stale_note", ""),
        "yf_status": yf_status,
    }




@st.cache_data(ttl=RANK_CACHE_TTL, show_spinner=False)
def _fetch_single_html(url: str):
    session = make_retry_session(base_headers=get_browser_headers(url))
    r = safe_get(session, url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.text


def _extract_single_code_candidates(query: str, snapshot: dict):
    q = str(query or '').strip()
    q_lower = q.lower()
    meta = snapshot.get('meta', {}) or {}
    cands = []
    if re.fullmatch(r'[0-9]{4,6}[A-Z]?', q):
        if q in meta:
            cands.append((q, meta[q]))
    for code, info in meta.items():
        name = str(info.get('name', code))
        if q == code or q_lower == name.lower() or q_lower in name.lower():
            pair = (code, info)
            if pair not in cands:
                cands.append(pair)
    return cands[:8]


def _parse_single_wantgoo_quote(html: str, code: str, name: str, market: str):
    text = re.sub(r'\s+', ' ', BeautifulSoup(html, 'html.parser').get_text(' ', strip=True))
    pat = re.compile(
        rf"{re.escape(name)}\({re.escape(code)}\).*?(\d{{4}}-\d{{2}}-\d{{2}}).*?([0-9]+(?:\.[0-9]+)?)\s+([+\-]?[0-9]+(?:\.[0-9]+)?)\s*\(([+\-]?[0-9]+(?:\.[0-9]+)?)%\).*?高\s*([0-9]+(?:\.[0-9]+)?)\s*低\s*([0-9]+(?:\.[0-9]+)?)\s*量\s*([0-9,]+(?:\.[0-9]+)?)",
        re.I,
    )
    m = pat.search(text)
    if not m:
        raise ValueError('單獨查詢 WantGoo 解析失敗')
    asof, last, chg, chg_pct, high, low, vol = m.groups()
    last = _to_float(last); chg = _to_float(chg); chg_pct = _to_float(chg_pct)
    high = _to_float(high) or last; low = _to_float(low) or last; vol = _to_float(vol)
    return {
        'code': code, 'name': name, 'last': last, 'chg': chg, 'chg_pct': chg_pct,
        'high': high, 'low': low, 'vol_lots': vol, 'prev_close': (last - chg) if pd.notna(chg) else math.nan,
        'market': market, 'search_source': '單獨查詢 WantGoo', 'stale_note': f'單獨查詢 {asof} 的狀態'
    }


def _parse_single_yahoo_quote(html: str, code: str, name: str, market: str):
    text = re.sub(r'\s+', ' ', BeautifulSoup(html, 'html.parser').get_text(' ', strip=True))
    pat = re.compile(
        rf"{re.escape(name)}\s*{re.escape(code)}(?:\.TW|\.TWO)? .*?比較 .*?加入自選股 .*?([0-9]+(?:\.[0-9]+)?)\s*([+\-]?[0-9]+(?:\.[0-9]+)?)\s*\(([+\-]?[0-9]+(?:\.[0-9]+)?)%\)",
        re.I,
    )
    m = pat.search(text)
    if not m:
        # 退一步，只抓代碼附近第一組 價/漲跌/漲跌幅
        pat = re.compile(rf"{re.escape(code)}(?:\.TW|\.TWO)? .*?([0-9]+(?:\.[0-9]+)?)\s*([+\-]?[0-9]+(?:\.[0-9]+)?)\s*\(([+\-]?[0-9]+(?:\.[0-9]+)?)%\)", re.I)
        m = pat.search(text)
    if not m:
        raise ValueError('單獨查詢 Yahoo 解析失敗')
    last, chg, chg_pct = m.groups()
    last = _to_float(last); chg = _to_float(chg); chg_pct = _to_float(chg_pct)
    vol = math.nan
    for vp in [r'成交量\s*([0-9,]+(?:\.[0-9]+)?)', r'([0-9,]+(?:\.[0-9]+)?)\s*成交量']:
        vm = re.search(vp, text)
        if vm:
            vol = _to_float(vm.group(1)); break
    high = last; low = last
    hm = re.search(r'最高\s*([0-9]+(?:\.[0-9]+)?)', text)
    lm = re.search(r'最低\s*([0-9]+(?:\.[0-9]+)?)', text)
    if hm: high = _to_float(hm.group(1)) or last
    if lm: low = _to_float(lm.group(1)) or last
    tm = re.search(r'(?:開盤|盤中|收盤)\s*\|\s*(\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2})\s*更新', text)
    asof = tm.group(1) if tm else ''
    note = f'單獨查詢 {asof}' if asof else '單獨查詢，部分資料可能略有落差'
    return {
        'code': code, 'name': name, 'last': last, 'chg': chg, 'chg_pct': chg_pct,
        'high': high, 'low': low, 'vol_lots': vol, 'prev_close': (last - chg) if pd.notna(chg) else math.nan,
        'market': market, 'search_source': '單獨查詢 Yahoo', 'stale_note': note
    }


def fetch_single_stock_row(query: str, snapshot: dict):
    matches = []
    for code, info in _extract_single_code_candidates(query, snapshot):
        name = str(info.get('name', code)).strip() or code
        market = str(info.get('ex', '')).strip().lower()
        matches.append(f'{code} {name}')
        yahoo_suffix = 'TW' if market == 'tse' else 'TWO'
        sources = [
            ('wantgoo', f'https://www.wantgoo.com/stock/{code}/technical-chart'),
            ('yahoo', f'https://tw.stock.yahoo.com/quote/{code}.{yahoo_suffix}')
        ]
        errs = []
        for kind, url in sources:
            try:
                html = _fetch_single_html(url)
                if kind == 'wantgoo':
                    return _parse_single_wantgoo_quote(html, code, name, market), matches, errs
                return _parse_single_yahoo_quote(html, code, name, market), matches, errs
            except Exception as e:
                errs.append(f'{kind}: {type(e).__name__}: {e}')
        if errs:
            return None, matches, errs
    return None, matches, []

def recompute_single_search(search_state: dict, scan: dict, is_test: bool, use_bloodline: bool):
    query = str(search_state.get("query", "")).strip()
    if not query or not scan or not scan.get("snapshot"):
        return None
    row, matches = _resolve_search_row(query, scan["snapshot"])
    fetch_errors = []
    if row is None:
        single_row, single_matches, single_errors = fetch_single_stock_row(query, scan["snapshot"])
        matches = matches or single_matches
        fetch_errors = single_errors
        row = single_row
    if row is None:
        return {
            "status": "miss", "query": query, "matches": matches,
            "fetch_errors": fetch_errors,
            "is_test": is_test, "use_bloodline": use_bloodline, "ts": scan["ts"]
        }
    report = evaluate_single_stock(row, scan["snapshot"]["meta"], scan["ts"], is_test, use_bloodline)
    report["query"] = query
    report["matches"] = matches
    report["fetch_errors"] = fetch_errors
    report["is_test"] = is_test
    report["use_bloodline"] = use_bloodline
    report["ts"] = scan["ts"]
    return report


# =========================
# UI / THEME
# =========================
st.set_page_config(page_title="股市熱門趨勢雷達 Ultra", page_icon="⚡", layout="wide", initial_sidebar_state="collapsed")
st.markdown(
    """
<style>
:root {
  --bg: #05070b;
  --panel: rgba(13,16,22,.78);
  --panel2: rgba(18,22,30,.9);
  --line: rgba(255,255,255,.07);
  --text: #edf2f7;
  --muted: #8ea0b7;
  --accent: #61dafb;
  --accent2: #7c3aed;
  --danger: #fb7185;
  --ok: #34d399;
}
[data-testid="stAppViewContainer"], .main {
  background:
    radial-gradient(circle at 15% 15%, rgba(40,70,110,.26), transparent 22%),
    radial-gradient(circle at 85% 25%, rgba(80,25,120,.23), transparent 24%),
    linear-gradient(180deg, #04060a 0%, #05070b 55%, #020409 100%) !important;
  color: var(--text) !important;
}
.block-container { max-width: 1380px; padding-top: 1.2rem; padding-bottom: 2rem; }
[data-testid="stSidebar"] { display: none !important; }
.hero-wrap {
  border: 1px solid rgba(255,255,255,.06);
  border-radius: 28px;
  padding: 28px 28px 22px 28px;
  background: linear-gradient(145deg, rgba(15,18,25,.95), rgba(10,12,17,.78));
  box-shadow: 0 18px 50px rgba(0,0,0,.28);
  margin-bottom: 18px;
}
.title {
  font-size: 64px; font-weight: 900; letter-spacing: -2.2px; line-height: 1;
  background: linear-gradient(135deg,#ffffff 0%, #8fd3ff 35%, #b1a6ff 100%);
  -webkit-background-clip: text; -webkit-text-fill-color: transparent;
}
.subtitle { color: var(--muted); font-size: 14px; margin-top: 8px; letter-spacing: .8px; }
.hero-badges { display:flex; gap:10px; flex-wrap:wrap; margin-top:14px; }
.badge {
  display:inline-flex; align-items:center; gap:8px; padding: 8px 14px; border-radius: 999px;
  border: 1px solid rgba(255,255,255,.09); background: rgba(255,255,255,.03);
  color: #d8e3f0; font-size: 12px; font-weight: 700;
}
.badge.blue { border-color: rgba(97,218,251,.18); color:#97e7ff; }
.badge.green { border-color: rgba(52,211,153,.18); color:#7ef5c0; }
.badge.purple { border-color: rgba(168,85,247,.18); color:#d0b3ff; }
.glass {
  background: linear-gradient(145deg, rgba(18,22,30,.9), rgba(12,16,22,.78));
  border: 1px solid rgba(255,255,255,.06);
  border-radius: 22px;
  padding: 18px;
  box-shadow: 0 12px 30px rgba(0,0,0,.18);
}
.section-title { font-size: 18px; font-weight: 900; letter-spacing:.5px; margin-bottom: 10px; }
.hint { color: var(--muted); font-size: 12px; }
.pro-card {
  background: linear-gradient(155deg, rgba(17,22,30,.95), rgba(10,13,18,.82));
  border: 1px solid rgba(255,255,255,.06);
  border-radius: 24px;
  padding: 22px;
  min-height: 180px;
  box-shadow: 0 16px 40px rgba(0,0,0,.22);
}
.pro-card:hover { border-color: rgba(97,218,251,.25); transform: translateY(-1px); }
.stock-name { font-size: 21px; font-weight: 900; color: #f8fafc; letter-spacing: .4px; }
.price-large { font-size: 38px; font-weight: 900; color: #fff; margin-top: 12px; }
.tag-pro {
  display:inline-block; padding:6px 12px; border-radius:10px; font-size:11px; font-weight:800;
  background: rgba(97,218,251,.10); color: #7ddfff; border: 1px solid rgba(97,218,251,.16);
}
.fail-tag {
  display:inline-block; padding: 7px 12px; background: rgba(251,113,133,.07); color: #ff9aad;
  border-radius: 10px; margin: 4px; font-size: 12px; border: 1px solid rgba(251,113,133,.16); font-weight: 700;
}
.soft-line { height:1px; background: linear-gradient(90deg, transparent, rgba(255,255,255,.11), transparent); margin: 16px 0; }

.rank-table-shell {
  border: 1px solid rgba(255,255,255,.06);
  border-radius: 22px;
  overflow: hidden;
  background: linear-gradient(180deg, rgba(10,14,20,.98), rgba(7,10,15,.94));
  box-shadow: 0 18px 40px rgba(0,0,0,.20);
}
.rank-table-toolbar {
  display:flex; align-items:center; justify-content:space-between; gap:14px;
  padding: 16px 18px;
  border-bottom: 1px solid rgba(255,255,255,.06);
  background: linear-gradient(90deg, rgba(20,28,40,.86), rgba(14,18,26,.66));
}
.rank-table-title { color:#f8fbff; font-size:16px; font-weight:900; letter-spacing:.6px; }
.rank-table-subtitle { color:#8ea0b7; font-size:12px; margin-top:4px; }
.rank-table-count {
  color:#dce9f8; font-size:12px; font-weight:800; padding:8px 12px; border-radius:999px;
  border:1px solid rgba(97,218,251,.14); background: rgba(97,218,251,.08);
}
.rank-table-wrap { overflow-x:auto; }
.rank-table {
  width:100%; border-collapse:separate; border-spacing:0; min-width: 960px;
  background: transparent; color:#e8f0f8;
}
.rank-table thead th {
  position: sticky; top: 0; z-index: 1;
  text-align:left; font-size:12px; font-weight:800; letter-spacing:.7px; color:#90a4bc;
  padding: 14px 14px; background: rgba(12,18,26,.96); border-bottom:1px solid rgba(255,255,255,.08);
}
.rank-table tbody td {
  padding: 14px 14px; border-bottom:1px solid rgba(255,255,255,.05); font-size:14px;
  background: linear-gradient(180deg, rgba(10,13,19,.55), rgba(10,13,19,.35));
}
.rank-table tbody tr:nth-child(odd) td { background: linear-gradient(180deg, rgba(11,15,22,.72), rgba(11,15,22,.52)); }
.rank-table tbody tr:hover td {
  background: linear-gradient(90deg, rgba(21,31,46,.96), rgba(15,20,30,.92));
}
.rank-table tbody tr:last-child td { border-bottom: none; }
.rank-table .c-rank {
  width: 54px; color:#8ddcff; font-weight:900; font-size:18px;
}
.rank-table .c-code { color:#e8f0f8; font-weight:800; letter-spacing:.4px; }
.rank-table .c-name { color:#f8fbff; font-weight:800; }
.rank-table .c-price { color:#ffffff; font-weight:900; font-size:16px; }
.rank-table .c-power { color:#96f7c8; font-weight:800; }
.rank-table .c-chg { font-weight:900; }
.rank-table .c-chg.up { color:#ff8fa3; }
.rank-table .c-chg.mid { color:#ffd479; }
.rank-table .c-chg.flat { color:#dbe7f5; }
.status-pill, .stage-pill {
  display:inline-flex; align-items:center; justify-content:center;
  padding: 7px 12px; border-radius: 999px; font-size:12px; font-weight:800;
  border: 1px solid rgba(255,255,255,.08);
}
.status-pill.is-live {
  color:#ffd28b; background: rgba(245,158,11,.10); border-color: rgba(245,158,11,.20);
}
.status-pill.is-lock {
  color:#ffb4cb; background: rgba(244,63,94,.10); border-color: rgba(244,63,94,.20);
}
.status-pill.is-watch {
  color:#d7c2ff; background: rgba(124,58,237,.11); border-color: rgba(124,58,237,.22);
}
.stage-pill {
  color:#97e7ff; background: rgba(97,218,251,.08); border-color: rgba(97,218,251,.18);
}
.rank-table-empty {
  padding:18px; color:#8ea0b7; border:1px dashed rgba(255,255,255,.1); border-radius:16px;
}
.stButton>button {
  border-radius: 18px !important;
  background: linear-gradient(135deg, #f8fafc 0%, #cfe2ff 48%, #e2d8ff 100%) !important;
  color: #08111c !important;
  font-weight: 900 !important;
  padding: 18px 20px !important;
  width: 100% !important;
  border: none !important;
  font-size: 18px !important;
  letter-spacing: 1px !important;
  box-shadow: 0 10px 24px rgba(120,170,255,.18) !important;
}
.stButton>button:hover { transform: translateY(-1px); }
[data-testid="stMetric"] {
  background: linear-gradient(145deg, rgba(16,20,27,.85), rgba(10,13,18,.7));
  padding: 15px; border-radius: 18px; border: 1px solid rgba(255,255,255,.05);
}
[data-testid="stMetricLabel"] { color: #90a5bc !important; font-weight: 700 !important; }
[data-testid="stMetricValue"] { font-weight: 900 !important; color: #f8fafc !important; }
[data-testid="stExpander"] { border: 1px solid rgba(255,255,255,.06) !important; border-radius: 18px !important; overflow: hidden; }
[data-testid="stExpander"] summary { background: rgba(13,16,22,.78) !important; }
.top-note {
  padding: 12px 14px; border-radius: 14px; background: rgba(97,218,251,.08); border: 1px solid rgba(97,218,251,.14);
  color:#cfefff; font-size: 13px; font-weight: 700; margin-bottom: 12px;
}
.small-table {
  width:100%; border-collapse: collapse; font-size: 13px; color:#e8eef8;
}
.small-table td, .small-table th { padding: 8px 10px; border-bottom: 1px solid rgba(255,255,255,.06); text-align: left; }
.small-table th { color:#9fb0c7; font-weight: 800; }
</style>
""",
    unsafe_allow_html=True,
)

st.markdown(
    f'''
<div class="hero-wrap">
  <div class="title">股市熱門趨勢雷達 ULTRA</div>
  <div class="subtitle">v8.7｜優先使用官方資料｜備用網頁資料｜模式秒切換｜過去 180 天準確度測試</div>
  <div class="hero-badges">
    <span class="badge blue">🏛️ 官方優先</span>
    <span class="badge green">⚡ 快速切換不重載</span>
    <span class="badge purple">🧪 準確度回測 {CALIBRATION_LOOKBACK_DAYS} 天</span>
    <span class="badge">🛡️ {'歷史進階資料：正常' if HAS_YF else '歷史進階資料：停用'}</span>
  </div>
</div>
''',
    unsafe_allow_html=True,
)

if not HAS_YF:
    st.warning("⚠️ 目前環境尚未安裝進階歷史套件 (yfinance)，系統會使用『基本即時熱度模式』。若要恢復 過去20天平均交易熱度 / 連續大漲紀錄 / 歷史準確度測試，請安裝對應套件。")

cfg1, cfg2, cfg3 = st.columns([1.15, 1.15, 1.7])
with cfg1:
    is_test = st.toggle("🔥 寬鬆測試模式 (放寬篩選標準)", value=False)
with cfg2:
    use_bloodline = st.toggle("🛡️ 嚴格要求大漲紀錄 (只挑以前大漲過的)", value=True, disabled=not HAS_YF)
with cfg3:
    st.markdown('<div class="top-note">切換模式只會重新計算，不會重新下載資料。只有按下下方掃描按鈕，才會去網路抓最新資料。</div>', unsafe_allow_html=True)

now_time = time.time()
last_run = st.session_state.get("last_run_ts", 0)
cooldown_seconds = 12

if st.button("🚀 開始掃描全市場"):
    if now_time - last_run < cooldown_seconds:
        st.warning(f"⏳ 系統正在冷卻中，請等待 {int(cooldown_seconds - (now_time - last_run))} 秒後再執行。")
    else:
        st.session_state["last_run_ts"] = now_time
        t0, diag = time.perf_counter(), diag_init()
        with st.status("⚡ 正在連線並解讀市場狀況...", expanded=True) as status:
            t = time.perf_counter()
            base_meta, meta_errs = get_stock_list()
            diag["t_meta"] = time.perf_counter() - t
            diag["meta_count"] = len(base_meta)
            for err in meta_errs:
                diag_err(diag, Exception(err), "META_ERR")

            t = time.perf_counter()
            now_ts = now_taipei()
            raw_rank_df, search_backup_df, search_backup_asof = fetch_rank_snapshot(status, diag, base_meta)
            diag["t_rank"] = time.perf_counter() - t
            merged_meta = merge_meta(base_meta, raw_rank_df if raw_rank_df is not None else pd.DataFrame())
            merged_meta = merge_meta(merged_meta, search_backup_df if search_backup_df is not None else pd.DataFrame())
            diag["meta_count"] = len(merged_meta)

            pre_df = build_rank_candidates(raw_rank_df, merged_meta, now_ts, is_test, diag)
            t = time.perf_counter()
            final_res, stats, yf_diag = core_filter_engine(pre_df, merged_meta, now_ts, is_test, diag, use_bloodline)
            diag["t_filter"] = time.perf_counter() - t
            diag.update(yf_diag)
            diag["total"] = time.perf_counter() - t0
            status.update(label="✅ 掃描完成", state="complete")

        snapshot = {
            "meta": merged_meta,
            "meta_count": len(merged_meta),
            "raw_rank_df": raw_rank_df,
            "search_backup_df": search_backup_df,
            "search_backup_asof": search_backup_asof,
            "ts": now_ts,
            "fetch_diag": {
                "rank_req_err": diag.get("rank_req_err", 0),
                "rank_seen": diag.get("rank_seen", 0),
                "rank_source": diag.get("rank_source", "-"),
                "rank_asof": diag.get("rank_asof", ""),
                "source_mode": diag.get("source_mode", "-"),
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
        if st.session_state.get("single_search_state"):
            st.session_state["single_search_report"] = recompute_single_search(st.session_state["single_search_state"], st.session_state["last_scan"], is_test, use_bloodline)
        st.rerun()

scan = st.session_state.get("last_scan")
if scan and scan.get("snapshot") and (scan.get("is_test") != is_test or scan.get("use_bloodline") != use_bloodline):
    st.session_state["last_scan"] = recompute_from_snapshot(scan["snapshot"], is_test, use_bloodline)
    st.rerun()

scan = st.session_state.get("last_scan")
if scan:
    d, res, sts, ts = scan["diag"], scan["res"], scan["stats"], scan["ts"]
    t_str = f"測試模式: {'開啟' if scan['is_test'] else '關閉'} | 嚴格歷史紀錄: {'開啟' if scan['use_bloodline'] else '關閉'}"
    asof = f" | 快照時間：{d.get('rank_asof')}" if d.get("rank_asof") else ""
    st.markdown(
        f'<div class="hint" style="text-align:center; margin: 2px 0 18px 0;">上次更新：{ts.strftime("%H:%M:%S")} | {t_str}{asof} | 資料來源：{d.get("rank_source", "-")} | 模式：{d.get("source_mode", "-")} | 處理時間：{d.get("total", 0):.2f} 秒</div>',
        unsafe_allow_html=True,
    )
    if scan.get("instant_switch"):
        st.caption("⚡ 本次為模式即時切換，系統直接套用上次的快取重新運算，沒有花時間重新上網抓資料。")

    m1, m2, m3, m4 = st.columns(4)
    total_parse = d.get("rank_parse_ok", 0) + d.get("rank_parse_fail", 0)
    m1.metric("初步篩選檔數", d.get("cand_total", 0))
    m2.metric("嚴格過關檔數", len(res))
    m3.metric("資料解讀成功率", f"{(d.get('rank_parse_ok', 0) / max(1, total_parse) * 100):.1f}%")
    m4.metric("網路異常次數", d.get("rank_req_err", 0) + d.get("yf_fail", 0) + d.get("other_err", 0))

    st.markdown('<div class="glass" style="margin-top:16px; margin-bottom:16px;">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">🔎 單獨查詢與評估</div>', unsafe_allow_html=True)
    st.caption("只要輸入股票的代號或名稱，系統就會用相同的嚴格標準幫你打分數。")
    s1, s2 = st.columns([2.2, 1])
    with s1:
        st.text_input("請輸入代號 / 名稱", key="single_search_query", placeholder="例如：3017 或 鴻海")
    with s2:
        search_clicked = st.button("🎯 開始評估", key="single_search_button")

    if search_clicked:
        q = st.session_state.get("single_search_query", "").strip()
        st.session_state["single_search_state"] = {"query": q}
        st.session_state["single_search_report"] = recompute_single_search(st.session_state["single_search_state"], scan, is_test, use_bloodline)
        st.rerun()

    search_state = st.session_state.get("single_search_state")
    search_report = st.session_state.get("single_search_report")
    if search_state and search_report and (search_report.get("is_test") != is_test or search_report.get("use_bloodline") != use_bloodline or search_report.get("ts") != scan.get("ts")):
        st.session_state["single_search_report"] = recompute_single_search(search_state, scan, is_test, use_bloodline)
        search_report = st.session_state.get("single_search_report")

    if search_report:
        if search_report.get("status") == "ok":
            sr = search_report
            left, right = st.columns([1.15, 1.85])
            with left:
                card_html = (
                    f'<div class="pro-card" style="min-height:230px;">'
                    f'<div class="tag-pro">{sr["badge"]}</div>'
                    f'<div class="stock-name" style="margin-top:10px;">{sr["code"]} {sr["name"]}</div>'
                    f'<div class="price-large">{sr["last"]:.2f}</div>'
                    f'<div style="margin-top:10px; color:#dce9f8; font-size:16px; font-weight:800;">系統評分 {sr["score"]}/10</div>'
                    f'<div style="margin-top:8px; color:#9cb1c7; font-weight:700;">{sr["live_status"]} ｜ {sr["action"]}</div>'
                    f'<div style="margin-top:8px; color:#9cb1c7; font-size:13px;">資料來源：{sr["query_source"]}{(" ｜ " + sr["stale_note"]) if sr.get("stale_note") else ""}</div>'
                    '</div>'
                )
                st.markdown(card_html, unsafe_allow_html=True)
            with right:
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("上漲幅度", f'{sr["chg_pct"]:.2f}%')
                c2.metric("距今天限額", f'{sr["dist_pct"]:.2f}%')
                c3.metric("交易熱度", f'{sr["vol_ratio"]:.2f} 倍')
                c4.metric("歷史紀錄", f'連 {sr["board_count"]} 天大漲')
                c5, c6, c7 = st.columns(3)
                c5.metric("從高點掉下", f'{sr["pullback_pct"]:.2f}%')
                c6.metric("當天相對高低位置", f'{sr["range_pos"]:.1f}%')
                c7.metric("資料模式", sr["yf_status"])
                st.markdown("**加分亮點**")
                if sr.get("strengths"):
                    st.markdown("<div>" + "".join([f'<span class="badge green">+ {x}</span>' for x in sr["strengths"]]) + "</div>", unsafe_allow_html=True)
                else:
                    st.caption("目前沒有明顯加分的表現。")
                st.markdown("**風險 / 扣分項目**")
                if sr.get("warnings"):
                    st.markdown("<div>" + "".join([f'<span class="fail-tag">{x}</span>' for x in sr["warnings"]]) + "</div>", unsafe_allow_html=True)
                else:
                    st.caption("目前沒有明顯的扣分或危險訊號。")
                if sr.get("filter_flags"):
                    st.caption("嚴格把關淘汰原因：" + "、".join(sr["filter_flags"]))
                st.caption(sr.get("vol_note", ""))
        elif search_report.get("status") == "miss":
            st.warning("查無相關資料。目前的清單和備用資料庫裡都沒找到。")
            if search_report.get("matches"):
                st.caption("你可能想找的是：" + " ｜ ".join(search_report["matches"]))
            if search_report.get("fetch_errors"):
                st.caption("單獨查詢連線失敗：" + " | ".join(search_report["fetch_errors"][:3]))
        elif search_report.get("status") == "error":
            st.error(search_report.get("message", "這檔股票目前無法評估"))
    st.markdown('</div>', unsafe_allow_html=True)

    # Calibration panel
    if HAS_YF:
        cal_symbols = []
        if not res.empty:
            for _, rr in res.head(CALIBRATION_SYMBOL_CAP).iterrows():
                code = str(rr["代號"])
                if code in scan["snapshot"]["meta"]:
                    ex = scan["snapshot"]["meta"][code]["ex"]
                    cal_symbols.append(f"{code}.{'TW' if ex == 'tse' else 'TWO'}")
        if cal_symbols:
            t = time.perf_counter()
            cal = calibrate_signal_quality(tuple(cal_symbols), CALIBRATION_LOOKBACK_DAYS)
            d["t_cal"] = time.perf_counter() - t
            st.markdown('<div class="glass" style="margin-top:16px; margin-bottom:16px;">', unsafe_allow_html=True)
            st.markdown('<div class="section-title">🧪 篩選條件準確度測試（回測近 180 天的歷史表現）</div>', unsafe_allow_html=True)
            if cal.get("status") == "ok":
                c1, c2, c3, c4, c5 = st.columns(5)
                c1.metric("可靠度評分", f"{cal['score']}/10")
                c2.metric("歷史出現次數", cal["signal_count"])
                c3.metric("後3天平均最大漲幅", f"{cal['avg_max_3d']:.2f}%")
                c4.metric("後3天漲過3%的機率", f"{cal['win_3d_gt3']:.1f}%")
                c5.metric("後5天漲過5%的機率", f"{cal['win_5d_gt5']:.1f}%")
                st.caption("這是針對『畫面上這些強勢股』在過去 180 天相似情況下的表現統計，主要用來確認我們的篩選邏輯有沒有嚴重失準。")
            else:
                st.caption(f"因為某些原因跳過測試：{cal.get('reason', '-')}")
            st.markdown('</div>', unsafe_allow_html=True)

    with st.expander("⚙️ 系統運作狀況與工程數據 (適合進階使用者)", expanded=False):
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("市場總檔數", d.get("meta_count", 0))
        c2.metric("資料成功讀取數", d.get("rank_parse_ok", 0))
        c3.metric("進階資料庫連線", "未安裝套件" if not HAS_YF else f"{d.get('yf_returned', 0)} / {d.get('yf_symbols', 0)}")
        rescue_msg = f"{'🟢 啟動' if d.get('yf_rescue_used', 0) else '⚪ 待命'} | 錯誤 {d.get('other_err', 0)}"
        c4.metric("系統救援 / 錯誤", rescue_msg)
        st.caption(f"📡 目前來源：{d.get('rank_source', '-')} | 處理模式：{d.get('source_mode', '-')} | 初步候選名單 {d.get('rank_rows', 0)} 檔 | 連線錯誤 {d.get('rank_req_err', 0)} 次")
        st.caption(f"花費時間：基本資料 {d.get('t_meta',0):.2f} 秒 | 取得排行 {d.get('t_rank',0):.2f} 秒 | 進階資料 {d.get('t_yf',0):.2f} 秒 | 過濾分析 {d.get('t_filter',0):.2f} 秒 | 歷史測試 {d.get('t_cal',0):.2f} 秒")
        if d.get("last_errors"):
            st.code("\n".join(d["last_errors"]))

    with st.expander("🎯 被淘汰的股票與原因 (看看誰沒過關)", expanded=True):
        for reason, stocks in sts.items():
            if isinstance(stocks, list) and stocks:
                st.markdown(f"**{reason}**")
                st.markdown('<div>' + ''.join([f'<span class="fail-tag">{s}</span>' for s in stocks]) + '</div>', unsafe_allow_html=True)

    if not res.empty:
        st.markdown('<div class="soft-line"></div>', unsafe_allow_html=True)
        cols = st.columns(4)
        for i, r in res.iterrows():
            with cols[i % 4]:
                st.markdown(
                    f'''<div class="pro-card">
                        <div class="tag-pro">{r['階段']}</div>
                        <div class="stock-name">{r['代號']} {r['名稱']}</div>
                        <div class="price-large">{r['目前價格']:.2f}</div>
                        <div style="margin-top:12px; color:#9cb1c7; font-weight:700;">{r['狀態']}</div>
                        <div style="margin-top:8px; color:#d6e6f4; font-size:14px;">交易熱度 {r['交易熱度']:.1f} 倍 ｜ 上漲 {r['上漲幅度%']:.2f}%</div>
                    </div>''',
                    unsafe_allow_html=True,
                )
        table_df = res[["代號", "名稱", "目前價格", "交易熱度", "狀態", "階段", "上漲幅度%"]].copy()
        with st.expander("📋 嚴格過關名單總表", expanded=False):
            st.markdown(format_rank_table_html(table_df), unsafe_allow_html=True)
    else:
        if d.get("rank_parse_ok", 0) == 0:
            st.error("🚨 這次沒有成功抓到資料哦。可以打開「系統運作狀況」檢查一下是不是網站連線出了問題。")
        else:
            st.warning("⚠️ 掃描完畢，但在目前嚴格的條件下，市場上沒有找到符合標準的股票。")
else:
    st.info("請先點擊上方『開始掃描全市場』按鈕。系統會先找最準確的官方資料，如果盤中拿不到，會自動切換成備用網頁資料。")
