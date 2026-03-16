import html
import io
import math
import os
import re
import time
import textwrap
from copy import deepcopy
from datetime import datetime, timedelta, time as dtime, timezone
from collections import deque

import pandas as pd
import requests
import streamlit as st
import urllib3
import yfinance as yf
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ============================================================
# 基本設定
# ============================================================
APP_TITLE = "起漲戰情室 OMEGA"
APP_SUBTITLE = "v13.3 族群強化版｜首根雷達｜族群共振｜歷史模擬"
FUGLE_API_KEY = "ZWJjZDhjZWYtMjhhMi00YWI2LTliNWQtMmViYzVhMmIzODdjIGY1N2Y0MGZmLWQ1MjgtNDk1OC1iZTljLWMxOWUwODQ4Y2U2Zg=="
API_TIMEOUT = (3.0, 10.0)
PUBLIC_TIMEOUT = (3.0, 12.0)
RAW_HISTORY_DAYS = 420
DEFAULT_COOLDOWN_SECONDS = 45
DEFAULT_TOP_VOLUME = 220
DEFAULT_TOP_MOVERS = 150
DEFAULT_MIN_BOARD = 1
DEFAULT_HOLD_DAYS = 5
MAX_CANDIDATES = 320
FINAL_ENRICH_LIMIT = 24
YF_DOWNLOAD_CHUNK = 45
COLD_POOL_LIMIT = 90
DEFAULT_USE_TREND_TEMPLATE = True
DEFAULT_ACCOUNT_SIZE = 1_000_000
DEFAULT_RISK_PER_TRADE_PCT = 1.0
DEFAULT_FREE_ROLL_TRIGGER_R = 1.0


# ============================================================
# 診斷
# ============================================================
def diag_init():
    return {
        "meta_count": 0,
        "rank_count": 0,
        "candidate_count": 0,
        "final_count": 0,
        "rank_src": "None",
        "snapshot_ok": 0,
        "snapshot_fail": 0,
        "snapshot_market_ok": 0,
        "quote_enrich_ok": 0,
        "quote_enrich_fail": 0,
        "public_rank_ok": 0,
        "public_rank_fail": 0,
        "yf_symbols": 0,
        "yf_returned": 0,
        "yf_parts_ok": 0,
        "yf_parts_fail": 0,
        "yf_fail": 0,
        "feature_ok": 0,
        "feature_fail": 0,
        "other_err": 0,
        "last_errors": deque(maxlen=12),
        "t_meta": 0.0,
        "t_snapshot": 0.0,
        "t_rank": 0.0,
        "t_features": 0.0,
        "t_enrich": 0.0,
        "t_filter": 0.0,
        "t_backtest": 0.0,
        "total": 0.0,
    }


def diag_err(diag, e, tag="ERR"):
    return


# ============================================================
# HTTP / SESSION
# ============================================================
def get_base_headers():
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145 Safari/537.36",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


def make_retry_session(base_headers=None, total=2, backoff=0.7, pool=20):
    s = requests.Session()
    retry = Retry(
        total=total,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
        respect_retry_after_header=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=pool, pool_maxsize=pool)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update(get_base_headers())
    if base_headers:
        s.headers.update(base_headers)
    return s


# ============================================================
# 快取資料
# ============================================================
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_text(url: str):
    s = make_retry_session()
    r = s.get(url, timeout=PUBLIC_TIMEOUT, verify=False)
    r.raise_for_status()
    return r.text.replace("\r", "")


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
            cols = {c.strip().lower(): c for c in df.columns}
            c_col = cols.get("code") or df.columns[1]
            n_col = cols.get("name") or df.columns[2]
            t_col = cols.get("type")
            g_col = cols.get("group") or cols.get("industry") or cols.get("category")
            for _, row in df.iterrows():
                stype = str(row.get(t_col, "")) if t_col else ""
                raw_group = str(row.get(g_col, "")) if g_col else ""
                if t_col and ("ETF" in stype or "權證" in stype or "受益證券" in stype):
                    continue
                code = str(row.get(c_col, "")).strip()
                if len(code) == 4 and code.isdigit():
                    industry_raw = raw_group if raw_group and raw_group != "nan" else ""
                    stock_name = str(row.get(n_col, "")).strip()
                    industry_norm = normalize_industry(industry_raw)
                    meta[code] = {
                        "name": stock_name,
                        "ex": ex,
                        "market": "上市" if ex == "tse" else "上櫃",
                        "industry": refine_industry(code, stock_name, industry_norm),
                    }
        except Exception as e:
            errors.append(f"{ex}: {e}")
    return meta, errors


@st.cache_data(ttl=6 * 3600, show_spinner=False)
def yf_download_daily(symbols, period="420d"):
    if not symbols:
        return pd.DataFrame()
    data = yf.download(
        tickers=" ".join(symbols),
        period=period,
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        actions=False,
        threads=True,
        progress=False,
        multi_level_index=True,
        timeout=15,
    )
    if data is None or getattr(data, "empty", False):
        return pd.DataFrame()
    if not isinstance(data.columns, pd.MultiIndex):
        t = symbols[0]
        data.columns = pd.MultiIndex.from_product([[t], data.columns])
    data = data.loc[~data.index.duplicated(keep="last")]
    data = data.sort_index()
    return data


# ============================================================
# 基本工具
# ============================================================
def now_taipei():
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=8)


def idx_date_taipei(idx):
    try:
        ts = pd.Timestamp(idx)
        if getattr(ts, "tzinfo", None) is not None:
            ts = ts.tz_convert("Asia/Taipei")
        return ts.date()
    except Exception:
        try:
            if getattr(idx, "tz", None) is not None:
                return idx.tz_convert("Asia/Taipei").date()
        except Exception:
            pass
        return pd.Timestamp(idx).date()


def tw_tick(price):
    if price < 10:
        return 0.01
    if price < 50:
        return 0.05
    if price < 100:
        return 0.1
    if price < 500:
        return 0.5
    if price < 1000:
        return 1.0
    return 5.0


def calc_limit_up(prev_close, limit_pct=0.10):
    raw = float(prev_close) * (1.0 + limit_pct)
    tick = tw_tick(raw)
    n = math.floor((raw + 1e-12) / tick)
    if tick < 0.1:
        digits = 2
    elif tick < 1:
        digits = 1
    else:
        digits = 0
    return round(n * tick, digits)


def safe_float(x, default=0.0):
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default


def safe_int(x, default=0):
    try:
        if x is None or x == "":
            return default
        return int(float(x))
    except Exception:
        return default


def stable_unique(seq):
    out, seen = [], set()
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def symbol_of(code, meta_dict):
    return f"{code}.{'TW' if meta_dict[code]['ex'] == 'tse' else 'TWO'}"


def market_of(code, meta_dict):
    return meta_dict.get(code, {}).get("market", "上市")


def market_label(m):
    return m


def copy_diag(diag):
    d = deepcopy(diag)
    if not isinstance(d.get("last_errors"), deque):
        d["last_errors"] = deque(d.get("last_errors", []), maxlen=12)
    return d

def normalize_industry(raw: str) -> str:
    s = str(raw or "").strip()
    if not s or s.lower() == "nan":
        return "其他"
    if s in ["股票", "common stock", "commonstock"]:
        return "其他"
    s = s.replace("業", "") if s.endswith("業") and len(s) <= 8 else s
    alias = {
        "半導體": "半導體",
        "電子零組件": "電子零組件",
        "電子零組件業": "電子零組件",
        "電腦及週邊設備": "電腦週邊",
        "電腦及週邊": "電腦週邊",
        "電腦及週邊設備業": "電腦週邊",
        "光電": "光電",
        "光電業": "光電",
        "通信網路": "通信網路",
        "通信網路業": "通信網路",
        "電子通路": "電子通路",
        "電子通路業": "電子通路",
        "其他電子": "其他電子",
        "其他電子業": "其他電子",
        "資訊服務": "資訊服務",
        "資訊服務業": "資訊服務",
        "生技醫療": "生技醫療",
        "生技醫療業": "生技醫療",
        "電機機械": "電機機械",
        "電機機械業": "電機機械",
        "航運": "航運",
        "航運業": "航運",
        "鋼鐵": "鋼鐵",
        "鋼鐵業": "鋼鐵",
        "塑膠": "塑膠",
        "塑膠業": "塑膠",
        "建材營造": "營建",
        "建材營造業": "營建",
        "食品": "食品",
        "食品業": "食品",
        "金融保險": "金融保險",
        "金融保險業": "金融保險",
        "貿易百貨": "貿易百貨",
        "貿易百貨業": "貿易百貨",
        "油電燃氣": "油電燃氣",
        "油電燃氣業": "油電燃氣",
        "紡織纖維": "紡織纖維",
        "紡織纖維業": "紡織纖維",
        "居家生活": "居家生活",
        "居家生活業": "居家生活",
        "觀光餐旅": "觀光餐旅",
        "觀光餐旅業": "觀光餐旅",
        "綠能環保": "綠能環保",
        "綠能環保業": "綠能環保",
        "數位雲端": "數位雲端",
        "運動休閒": "運動休閒",
    }
    return alias.get(s, s)


def refine_industry(code: str, name: str, industry: str) -> str:
    code = str(code or "").strip()
    name = str(name or "").strip()
    industry = str(industry or "其他").strip() or "其他"

    code_map = {
        "2426": "LED光元件", "2499": "LED光元件", "2301": "LED光元件", "6164": "LED光元件",
        "3698": "LED光元件", "2340": "LED光元件", "6278": "LED光元件", "3061": "LED光元件",
        "2344": "記憶體", "2408": "記憶體", "2337": "記憶體", "3006": "記憶體", "4967": "記憶體",
        "8299": "記憶體", "3260": "記憶體",
        "3037": "ABF載板", "8046": "ABF載板", "3189": "ABF載板",
        "2368": "CCL銅箔基板", "2383": "高速連接", "6274": "高速連接", "6128": "高速連接",
        "3665": "高速連接", "3324": "高速連接", "6191": "PCB設備", "1815": "玻纖布",
        "1616": "電線電纜", "1609": "電線電纜", "1608": "電線電纜",
        "2409": "面板顯示", "3481": "面板顯示", "6116": "面板顯示", "4938": "面板顯示",
        "3008": "光學鏡頭", "3406": "光學鏡頭", "4976": "光學鏡頭", "3376": "光學鏡頭", "3019": "光學鏡頭",
        "2454": "IC設計", "2379": "IC設計", "3035": "IC設計", "3443": "IC設計", "3661": "IC設計",
        "5269": "IC設計", "6531": "IC設計", "6415": "IC設計", "2303": "晶圓代工", "5347": "晶圓代工",
        "6770": "晶圓代工", "2330": "晶圓代工", "3711": "封測", "2449": "封測", "6239": "封測",
        "3231": "AI伺服器", "3017": "伺服器ODM", "2382": "伺服器ODM", "2324": "伺服器ODM", "2356": "伺服器ODM",
        "6669": "散熱模組", "3015": "散熱模組", "3653": "散熱模組", "2421": "機殼機構",
        "2308": "電源供應", "3034": "電源供應", "6409": "電源管理", "4931": "網通設備", "5388": "網通設備", "3596": "網通設備",
        "2327": "被動元件", "2492": "被動元件", "3026": "功率半導體", "8261": "功率半導體", "6414": "功率半導體",
        "2049": "工業電腦", "2395": "工業電腦", "2464": "工業電腦",
    }
    if code in code_map:
        return code_map[code]

    keyword_map = [
        (["鼎元", "億光", "艾笛森", "隆達", "富采", "宏齊", "佰鴻", "光磊", "晶電", "東貝", "久元"], "LED光元件"),
        (["友達", "群創", "彩晶", "凌巨", "中光電", "誠美材"], "面板顯示"),
        (["玉晶光", "亞光", "佳能", "先進光", "大立光", "揚明光", "今國光", "華晶科"], "光學鏡頭"),
        (["南亞科", "華邦電", "旺宏", "晶豪科", "創見", "威剛", "品安", "十銓", "群聯"], "記憶體"),
        (["欣興", "景碩", "南電"], "ABF載板"),
        (["台光電", "聯茂", "台燿", "金像電", "瀚宇博", "華通", "健鼎"], "CCL銅箔基板"),
        (["富喬", "台玻", "德宏", "建榮"], "玻纖布"),
        (["嘉澤", "貿聯", "信音", "湧德", "良維", "正崴", "宣德", "優群"], "高速連接"),
        (["奇鋐", "雙鴻", "健策", "超眾", "力致", "建準", "高力"], "散熱模組"),
        (["緯創", "廣達", "英業達", "緯穎", "仁寶", "和碩", "技嘉", "微星"], "伺服器ODM"),
        (["川湖", "勤誠", "晟銘電", "營邦", "迎廣", "緯穎", "緯創"], "AI伺服器"),
        (["台積電", "聯電", "世界", "力積電", "VIS"], "晶圓代工"),
        (["聯發科", "瑞昱", "世芯", "創意", "智原", "譜瑞", "祥碩", "義隆", "聯詠", "矽力"], "IC設計"),
        (["日月光", "矽品", "京元電", "頎邦"], "封測"),
        (["台達電", "光寶科", "群電", "康舒", "全漢"], "電源供應"),
        (["台半", "漢磊", "朋程", "茂達", "富鼎", "尼克森"], "功率半導體"),
        (["智邦", "明泰", "正文", "中磊", "啟碁", "神準", "智易"], "網通設備"),
        (["國巨", "華新科", "禾伸堂", "立隆電", "凱美"], "被動元件"),
        (["AES", "順達", "新普", "加百裕", "興能高"], "電池模組"),
        (["上銀", "亞德客", "直得", "羅昇", "所羅門"], "自動化機器人"),
        (["研華", "樺漢", "新漢", "振樺電"], "工業電腦"),
        (["大亞", "華新", "合機", "華榮"], "電線電纜"),
    ]
    for keys, label in keyword_map:
        if any(k in name for k in keys):
            return label

    return industry
def get_api_key():
    key = ""
    try:
        key = st.secrets.get("FUGLE_API_KEY", "")
    except Exception:
        key = ""
    if not key:
        key = os.getenv("FUGLE_API_KEY", "")
    if not key:
        key = FUGLE_API_KEY
    return str(key).strip()


def fugle_get_json(session, path, api_key, params=None):
    url = f"https://api.fugle.tw/marketdata/v1.0/stock/{path}"
    headers = {"X-API-KEY": api_key}
    r = session.get(url, headers=headers, params=params or {}, timeout=API_TIMEOUT)
    return r


def snapshot_quotes_market(session, api_key, market, diag):
    r = fugle_get_json(session, f"snapshot/quotes/{market}", api_key, params={"type": "COMMONSTOCK"})
    if r.status_code == 200:
        diag["snapshot_market_ok"] += 1
        return r.json()
    if r.status_code in (401, 403, 429):
        raise RuntimeError(f"SNAPSHOT_{market}_{r.status_code}")
    raise RuntimeError(f"SNAPSHOT_{market}_{r.status_code}")


def enrich_quotes_for_codes(session, api_key, codes, diag):
    enriched = {}
    for code in codes:
        try:
            r = fugle_get_json(session, f"intraday/quote/{code}", api_key)
            if r.status_code != 200:
                diag["quote_enrich_fail"] += 1
                diag_err(diag, Exception(f"HTTP_{r.status_code} {code}"), "QUOTE_ENRICH")
                continue
            j = r.json()
            bids = j.get("bids", []) or []
            asks = j.get("asks", []) or []
            top_bid = bids[0] if bids else {}
            top_ask = asks[0] if asks else {}
            enriched[code] = {
                "best_bid": safe_float(top_bid.get("price"), 0.0),
                "best_bid_size": safe_int(top_bid.get("size"), 0),
                "best_ask": safe_float(top_ask.get("price"), 0.0),
                "best_ask_size": safe_int(top_ask.get("size"), 0),
            }
            diag["quote_enrich_ok"] += 1
            time.sleep(0.06)
        except Exception as e:
            diag["quote_enrich_fail"] += 1
            diag_err(diag, e, "QUOTE_ENRICH")
    return enriched


# ============================================================
# 公開榜單備援
# ============================================================
def fetch_public_ranking(diag):
    session = make_retry_session()
    ordered = []

    def push(items, limit=None):
        nonlocal ordered
        if limit is not None:
            items = items[:limit]
        ordered = stable_unique(ordered + [x for x in items if len(x) == 4 and x.isdigit()])

    got_any = False
    try:
        r = session.get("https://tw.stock.yahoo.com/rank/volume?exchange=ALL", timeout=PUBLIC_TIMEOUT, verify=True)
        r.raise_for_status()
        tks = re.findall(r"/quote/([0-9]{4})", r.text)
        if tks:
            push(tks, DEFAULT_TOP_VOLUME)
            got_any = True
            diag["public_rank_ok"] += 1
    except Exception as e:
        diag["public_rank_fail"] += 1
        diag_err(diag, e, "PUB_YAHOO_VOL")

    try:
        r = session.get("https://tw.stock.yahoo.com/rank/change-up?exchange=ALL", timeout=PUBLIC_TIMEOUT, verify=True)
        r.raise_for_status()
        tks = re.findall(r"/quote/([0-9]{4})", r.text)
        if tks:
            push(tks, DEFAULT_TOP_MOVERS)
            got_any = True
            diag["public_rank_ok"] += 1
    except Exception as e:
        diag["public_rank_fail"] += 1
        diag_err(diag, e, "PUB_YAHOO_UP")

    if len(ordered) < 40:
        try:
            r = session.get("https://www.wantgoo.com/stock/ranking/volume", timeout=PUBLIC_TIMEOUT, verify=True)
            r.raise_for_status()
            tks = re.findall(r"/stock/([0-9]{4})", r.text)
            if tks:
                push(tks, 100)
                got_any = True
                diag["public_rank_ok"] += 1
        except Exception as e:
            diag["public_rank_fail"] += 1
            diag_err(diag, e, "PUB_WANTGOO")

    if not got_any:
        raise RuntimeError("PUBLIC_RANK_ALL_FAILED")

    diag["rank_src"] = "網路公開排行榜"
    return ordered[:MAX_CANDIDATES]


# ============================================================
# 官方全市場資料優先
# ============================================================
def build_quotes_from_snapshot(snapshot_json, market, meta_dict):
    rows = []
    for item in snapshot_json.get("data", []) or []:
        code = str(item.get("symbol", "")).strip()
        if code not in meta_dict:
            continue
        last = safe_float(item.get("closePrice"), 0.0)
        high = safe_float(item.get("highPrice"), last)
        low = safe_float(item.get("lowPrice"), last)
        open_ = safe_float(item.get("openPrice"), last)
        vol = safe_int(item.get("tradeVolume"), 0)
        val = safe_float(item.get("tradeValue"), 0.0)
        chg = safe_float(item.get("change"), 0.0)
        chg_pct = safe_float(item.get("changePercent"), 0.0)
        prev_close = last - chg if last > 0 else 0.0
        if prev_close <= 0:
            continue
        upper = calc_limit_up(prev_close)
        dist_pct = max(0.0, (upper - last) / max(upper, 1e-9) * 100.0)
        rows.append(
            {
                "code": code,
                "name": meta_dict[code]["name"],
                "market": market,
                "industry": meta_dict[code].get("industry", "其他"),
                "open": open_,
                "high": high,
                "low": low,
                "last": last,
                "vol_sh": vol,
                "trade_value": val,
                "change": chg,
                "change_pct": chg_pct,
                "prev_close": prev_close,
                "upper": upper,
                "dist": dist_pct,
                "last_updated": safe_int(item.get("lastUpdated"), 0),
            }
        )
    return pd.DataFrame(rows)


def select_cold_momentum_codes(quotes_df, limit=COLD_POOL_LIMIT):
    if quotes_df is None or getattr(quotes_df, "empty", False):
        return []
    q = quotes_df.copy()
    if q.empty:
        return []
    q = q[(q["last"].astype(float) >= 8.0)]
    q = q[(q["change_pct"].astype(float) >= 0.8) & (q["change_pct"].astype(float) <= 8.5)]
    q = q[(q["dist"].astype(float) <= 6.8)]
    q = q[(q["vol_sh"].astype(float) >= 60000) & (q["vol_sh"].astype(float) <= 2500000)]
    q = q[(q["trade_value"].astype(float) >= 12000000)]
    if q.empty:
        return []
    q["cold_score"] = (
        q["change_pct"].astype(float) * 1.00
        + (6.8 - q["dist"].astype(float)).clip(lower=0.0) * 0.55
        + (q["trade_value"].astype(float).clip(lower=0.0, upper=200000000) / 200000000) * 1.20
        + ((q["vol_sh"].astype(float).between(180000, 1500000)).astype(float) * 0.55)
    )
    return q.sort_values(["cold_score", "change_pct", "dist", "trade_value"], ascending=[False, False, True, False])["code"].head(limit).tolist()


def fetch_market_snapshot_and_rank(meta_dict, api_key, diag, status_placeholder):
    t0 = time.perf_counter()
    session = make_retry_session()
    quotes_frames = []
    for market, m_label in zip(("TSE", "OTC"), ("上市", "上櫃")):
        status_placeholder.update(label=f"⚡ 讀取 {m_label} 官方資料中...", state="running")
        try:
            snap = snapshot_quotes_market(session, api_key, market, diag)
            quotes_frames.append(build_quotes_from_snapshot(snap, m_label, meta_dict))
            diag["snapshot_ok"] += 1
        except Exception as e:
            diag["snapshot_fail"] += 1
            diag_err(diag, e, f"SNAPSHOT_{market}")
    diag["t_snapshot"] = time.perf_counter() - t0

    if not quotes_frames:
        raise RuntimeError("SNAPSHOT_ALL_FAILED")

    quotes_df = pd.concat(quotes_frames, ignore_index=True)
    quotes_df = quotes_df.drop_duplicates("code", keep="first")

    vol_top = quotes_df.sort_values(["vol_sh", "trade_value"], ascending=[False, False])["code"].head(DEFAULT_TOP_VOLUME).tolist()
    mover_top = quotes_df.sort_values(["change_pct", "trade_value"], ascending=[False, False])["code"].head(DEFAULT_TOP_MOVERS).tolist()
    cold_top = select_cold_momentum_codes(quotes_df, limit=COLD_POOL_LIMIT)
    ranked_codes = stable_unique(vol_top + mover_top + cold_top)[:MAX_CANDIDATES]

    candidate_df = quotes_df[quotes_df["code"].isin(ranked_codes)].copy()
    order_map = {c: i for i, c in enumerate(ranked_codes)}
    candidate_df["rank_order"] = candidate_df["code"].map(order_map)
    candidate_df = candidate_df.sort_values(["rank_order", "dist", "vol_sh"], ascending=[True, True, False]).reset_index(drop=True)

    diag["rank_src"] = "官方全市場最新資料"
    diag["rank_count"] = len(ranked_codes)
    diag["candidate_count"] = len(candidate_df)
    diag["t_rank"] = max(diag.get("t_rank", 0.0), time.perf_counter() - t0)
    return candidate_df, ranked_codes


def fetch_candidate_rows_by_public_rank(meta_dict, api_key, diag, status_placeholder):
    t0 = time.perf_counter()
    ranked_codes = fetch_public_ranking(diag)
    session = make_retry_session()
    rows = []

    for idx, code in enumerate(ranked_codes, start=1):
        if code not in meta_dict:
            continue
        try:
            if idx <= 35:
                sleep_sec = 0.05
                stage = "⚡ 快速掃描最熱門的股票"
            elif idx <= 80:
                sleep_sec = 0.18
                stage = "🛰️ 掃描其他中段班股票"
            else:
                sleep_sec = 0.30
                stage = "🛡️ 慢慢掃描後段班股票"
            status_placeholder.update(label=f"{stage}... ({idx}/{len(ranked_codes)})", state="running")
            r = fugle_get_json(session, f"intraday/quote/{code}", api_key)
            if r.status_code != 200:
                diag["snapshot_fail"] += 1
                diag_err(diag, Exception(f"HTTP_{r.status_code} {code}"), "PUBLIC_QUOTE")
                time.sleep(min(0.6, sleep_sec + 0.1))
                continue
            j = r.json()
            ref = safe_float(j.get("referencePrice"), 0.0)
            last = safe_float(j.get("closePrice"), ref)
            high = safe_float(j.get("highPrice"), last)
            low = safe_float(j.get("lowPrice"), last)
            open_ = safe_float(j.get("openPrice"), ref)
            vol = safe_int((j.get("total") or {}).get("tradeVolume"), 0)
            bids = j.get("bids", []) or []
            asks = j.get("asks", []) or []
            best_bid = safe_float(bids[0].get("price"), 0.0) if bids else 0.0
            best_bid_size = safe_int(bids[0].get("size"), 0) if bids else 0
            best_ask = safe_float(asks[0].get("price"), 0.0) if asks else 0.0
            best_ask_size = safe_int(asks[0].get("size"), 0) if asks else 0
            if ref <= 0 or last <= 0:
                continue
            upper = calc_limit_up(ref)
            rows.append(
                {
                    "code": code,
                    "name": meta_dict[code]["name"],
                    "market": market_of(code, meta_dict),
                    "industry": meta_dict[code].get("industry", "其他"),
                    "open": open_,
                    "high": high,
                    "low": low,
                    "last": last,
                    "vol_sh": vol,
                    "trade_value": 0.0,
                    "change": last - ref,
                    "change_pct": ((last - ref) / ref * 100.0) if ref else 0.0,
                    "prev_close": ref,
                    "upper": upper,
                    "dist": max(0.0, (upper - last) / max(upper, 1e-9) * 100.0),
                    "last_updated": 0,
                    "best_bid": best_bid,
                    "best_bid_size": best_bid_size,
                    "best_ask": best_ask,
                    "best_ask_size": best_ask_size,
                    "rank_order": idx - 1,
                }
            )
            diag["snapshot_ok"] += 1
        except Exception as e:
            diag["snapshot_fail"] += 1
            diag_err(diag, e, "PUBLIC_QUOTE")
        time.sleep(sleep_sec)

    df = pd.DataFrame(rows).drop_duplicates("code", keep="first") if rows else pd.DataFrame()
    if not df.empty:
        cold_codes = set(select_cold_momentum_codes(df, limit=min(60, COLD_POOL_LIMIT)))
        df["cold_boost"] = df["code"].isin(cold_codes).astype(int)
        df = df.sort_values(["cold_boost", "rank_order", "dist", "vol_sh"], ascending=[False, True, True, False]).reset_index(drop=True)
    diag["rank_count"] = len(ranked_codes)
    diag["candidate_count"] = len(df)
    diag["t_rank"] = time.perf_counter() - t0
    return df, ranked_codes


# ============================================================
# 歷史表現預先計算
# ============================================================
def _extract_symbol_frame(raw_daily, sym):
    if raw_daily is None or getattr(raw_daily, "empty", False):
        return pd.DataFrame()
    if isinstance(raw_daily.columns, pd.MultiIndex):
        if sym not in raw_daily.columns.get_level_values(0):
            return pd.DataFrame()
        return raw_daily[sym].copy()
    return raw_daily.copy()


def _consecutive_limit_ups(past_df, tail_n=12):
    if len(past_df) < 2:
        return 0
    streak = 0
    tail = past_df.tail(tail_n)
    for i in range(len(tail) - 1, 0, -1):
        cp = safe_float(tail["Close"].iloc[i], 0.0)
        pp = safe_float(tail["Close"].iloc[i - 1], 0.0)
        if cp <= 0 or pp <= 0:
            break
        lim = calc_limit_up(pp)
        if cp >= lim - tw_tick(lim):
            streak += 1
        else:
            break
    return streak


def _build_trend_template_features(past_df):
    if past_df is None or past_df.empty:
        return {}

    close = past_df["Close"].astype(float)
    high = past_df["High"].astype(float)
    low = past_df["Low"].astype(float)

    ma50_series = close.rolling(50).mean()
    ma150_series = close.rolling(150).mean()
    ma200_series = close.rolling(200).mean()
    ma50 = safe_float(ma50_series.iloc[-1], 0.0)
    ma150 = safe_float(ma150_series.iloc[-1], 0.0)
    ma200 = safe_float(ma200_series.iloc[-1], 0.0)
    ma50_prev20 = safe_float(ma50_series.iloc[-21], ma50) if len(ma50_series) >= 21 else ma50
    ma200_prev20 = safe_float(ma200_series.iloc[-21], ma200) if len(ma200_series) >= 21 else ma200

    high_52w = safe_float(high.tail(252).max(), 0.0)
    low_52w = safe_float(low.tail(252).min(), 0.0)
    last_close = safe_float(close.iloc[-1], 0.0)
    pivot_low_10 = safe_float(low.tail(10).min(), 0.0)
    pivot_low_20 = safe_float(low.tail(20).min(), 0.0)
    atr14 = safe_float((high - low).rolling(14).mean().iloc[-1], 0.0)

    stage_checks = {
        "price_above_ma150_200": last_close > ma150 > 0 and last_close > ma200 > 0,
        "ma_alignment": ma50 > ma150 > ma200 > 0,
        "ma200_up": ma200 > 0 and ma200_prev20 > 0 and ma200 >= ma200_prev20,
        "price_above_ma50": last_close > ma50 > 0,
        "above_52w_low": low_52w > 0 and last_close >= low_52w * 1.30,
        "near_52w_high": high_52w > 0 and last_close >= high_52w * 0.75,
        "ma50_up": ma50 > 0 and ma50 >= ma50_prev20,
    }

    stage_required = [
        "price_above_ma150_200",
        "ma_alignment",
        "ma200_up",
        "price_above_ma50",
        "above_52w_low",
        "near_52w_high",
    ]
    stage_failed_map = {
        "price_above_ma150_200": "股價未站上150/200MA",
        "ma_alignment": "均線多頭排列未完成",
        "ma200_up": "200MA 尚未上彎",
        "price_above_ma50": "股價未站上50MA",
        "above_52w_low": "離52週低點不夠遠",
        "near_52w_high": "距52週高點太遠",
        "ma50_up": "50MA 尚未轉強",
    }
    failed = [stage_failed_map[k] for k, ok in stage_checks.items() if not ok]
    stage_pass = all(stage_checks[k] for k in stage_required)
    stage_score = sum(1 for ok in stage_checks.values() if ok)

    return {
        "ma50": ma50,
        "ma150": ma150,
        "ma200": ma200,
        "ma50_prev20": ma50_prev20,
        "ma200_prev20": ma200_prev20,
        "ma50_slope_pct": ((ma50 / ma50_prev20 - 1.0) * 100.0) if ma50 > 0 and ma50_prev20 > 0 else 0.0,
        "ma200_slope_pct": ((ma200 / ma200_prev20 - 1.0) * 100.0) if ma200 > 0 and ma200_prev20 > 0 else 0.0,
        "low_52w": low_52w,
        "high_52w": high_52w,
        "pct_from_52w_low": ((last_close / low_52w - 1.0) * 100.0) if last_close > 0 and low_52w > 0 else 0.0,
        "pct_from_52w_high": ((last_close / high_52w - 1.0) * 100.0) if last_close > 0 and high_52w > 0 else 0.0,
        "pivot_low_10": pivot_low_10,
        "pivot_low_20": pivot_low_20,
        "atr14": atr14,
        "trend_template_pass": stage_pass,
        "trend_template_score": stage_score,
        "trend_template_note": "通過" if stage_pass else "、".join(failed[:4]),
    }


def build_trade_management_plan(item, feat=None, account_size=DEFAULT_ACCOUNT_SIZE, risk_pct=DEFAULT_RISK_PER_TRADE_PCT, free_roll_r=DEFAULT_FREE_ROLL_TRIGGER_R):
    feat = feat or {}
    current_price = safe_float(item.get("現價", 0.0), 0.0)
    best_ask = safe_float(item.get("best_ask", 0.0), 0.0)
    entry_price = best_ask if best_ask > 0 else current_price

    ma50 = safe_float(feat.get("ma50", item.get("50MA", 0.0)), 0.0)
    pivot_low_10 = safe_float(feat.get("pivot_low_10", 0.0), 0.0)
    pivot_low_20 = safe_float(feat.get("pivot_low_20", 0.0), 0.0)
    atr14 = safe_float(feat.get("atr14", feat.get("atr20", 0.0)), 0.0)

    structural_candidates = []
    for x in [pivot_low_10, pivot_low_20]:
        if x > 0 and x < entry_price:
            structural_candidates.append(max(0.0, x - tw_tick(x)))
    if ma50 > 0 and ma50 < entry_price:
        structural_candidates.append(ma50 * 0.985)
    if atr14 > 0 and current_price > 0:
        atr_stop = current_price - atr14 * 2.0
        if 0 < atr_stop < entry_price:
            structural_candidates.append(atr_stop)

    structural_stop = max(structural_candidates) if structural_candidates else entry_price * 0.94
    hard_stop = entry_price * 0.92
    stop_price = max(structural_stop, hard_stop)
    if stop_price >= entry_price:
        stop_price = entry_price * 0.97

    risk_per_share = max(entry_price - stop_price, 0.0)
    risk_budget = max(0.0, account_size * (risk_pct / 100.0))
    shares_raw = int(risk_budget / risk_per_share) if risk_per_share > 0 else 0
    shares_lot = (shares_raw // 1000) * 1000
    free_roll_price = entry_price + risk_per_share * free_roll_r if risk_per_share > 0 else 0.0
    trail_guard = ma50
    trail_hit = current_price > 0 and ma50 > 0 and current_price < ma50
    allow_pyramid = current_price >= max(entry_price, free_roll_price) and not trail_hit if current_price > 0 else False

    return {
        "建議進場價": round(entry_price, 2) if entry_price > 0 else 0.0,
        "建議停損價": round(stop_price, 2) if stop_price > 0 else 0.0,
        "每股風險": round(risk_per_share, 2) if risk_per_share > 0 else 0.0,
        "風險資金": round(risk_budget, 2),
        "建議股數": int(max(0, shares_raw)),
        "建議張數": round(max(0, shares_lot) / 1000, 1),
        "Free Roll觸發價": round(free_roll_price, 2) if free_roll_price > 0 else 0.0,
        "50MA防守價": round(trail_guard, 2) if trail_guard > 0 else 0.0,
        "Stage2守門": "通過" if bool(feat.get("trend_template_pass", False)) else "未過",
        "加碼守則": "只准加碼獲利部位，未到 free roll 嚴禁加碼" if not allow_pyramid else "可贏家加碼，但仍不可攤平",
        "禁止攤平": "現價低於進場價一律不加碼",
        "50MA警報": "收盤跌破 50MA，偏向減碼/出場" if trail_hit else "50MA 尚未失守",
        "風控摘要": f"停損 {stop_price:.2f}｜Free Roll {free_roll_price:.2f}｜50MA {trail_guard:.2f}" if entry_price > 0 else "資料不足",
    }


def compute_feature_cache(candidate_df, meta_dict, diag, status_placeholder, period="420d"):
    t0 = time.perf_counter()
    if candidate_df.empty:
        return {}, pd.DataFrame()

    codes = [c for c in candidate_df["code"].tolist() if c in meta_dict]
    syms = [symbol_of(c, meta_dict) for c in codes]
    diag["yf_symbols"] = len(syms)

    raw_parts = []
    for i in range(0, len(syms), YF_DOWNLOAD_CHUNK):
        part = syms[i : i + YF_DOWNLOAD_CHUNK]
        status_placeholder.update(label=f"📚 正在下載過去的表現紀錄... ({min(i + len(part), len(syms))}/{len(syms)})", state="running")
        try:
            part_df = yf_download_daily(part, period=period)
            if part_df is not None and not getattr(part_df, "empty", False):
                raw_parts.append(part_df)
                diag["yf_parts_ok"] += 1
            else:
                diag["yf_parts_fail"] += 1
        except Exception as e:
            diag["yf_parts_fail"] += 1
            diag_err(diag, e, "YF_PART")

    if not raw_parts:
        diag["t_features"] = time.perf_counter() - t0
        return {}, pd.DataFrame()

    raw_daily = pd.concat(raw_parts, axis=1)
    if isinstance(raw_daily.columns, pd.MultiIndex):
        raw_daily = raw_daily.loc[:, ~raw_daily.columns.duplicated()]
        diag["yf_returned"] = int(raw_daily.columns.get_level_values(0).nunique())
    else:
        diag["yf_returned"] = 1
    raw_daily = raw_daily.loc[~raw_daily.index.duplicated(keep="last")].sort_index()

    today_date = now_taipei().date()
    features = {}

    for code in codes:
        sym = symbol_of(code, meta_dict)
        try:
            df = _extract_symbol_frame(raw_daily, sym)
            if df.empty or not {"Close", "Volume", "High", "Low", "Open"}.issubset(set(df.columns)):
                diag["feature_fail"] += 1
                continue
            df = df[["Open", "High", "Low", "Close", "Volume"]].dropna().copy()
            dates_tw = pd.Index([idx_date_taipei(x) for x in df.index])
            past_df = df[dates_tw < today_date].copy()
            if len(past_df) < 35:
                diag["feature_fail"] += 1
                continue

            close = past_df["Close"].astype(float)
            vol = past_df["Volume"].astype(float)
            high = past_df["High"].astype(float)
            low = past_df["Low"].astype(float)

            vol_ma20 = safe_float(vol.rolling(20).mean().iloc[-1], 0.0)
            high_52w = safe_float(high.tail(252).max(), 0.0)
            board_streak = _consecutive_limit_ups(past_df, tail_n=12)
            prev_close_hist = safe_float(close.iloc[-1], 0.0)
            atr20 = safe_float((high - low).rolling(20).mean().iloc[-1], 0.0)
            ret5 = safe_float((close.iloc[-1] / close.iloc[-6] - 1) * 100.0, 0.0) if len(close) >= 6 and close.iloc[-6] > 0 else 0.0
            ret10 = safe_float((close.iloc[-1] / close.iloc[-11] - 1) * 100.0, 0.0) if len(close) >= 11 and close.iloc[-11] > 0 else 0.0
            ret20 = safe_float((close.iloc[-1] / close.iloc[-21] - 1) * 100.0, 0.0) if len(close) >= 21 and close.iloc[-21] > 0 else 0.0
            range20_pct = safe_float(((high.tail(20).max() - low.tail(20).min()) / max(close.iloc[-1], 1e-9)) * 100.0, 0.0) if len(close) >= 20 else 0.0
            vol_ma5 = safe_float(vol.rolling(5).mean().iloc[-1], 0.0)
            trend_pack = _build_trend_template_features(past_df)

            features[code] = {
                "vol_ma20": vol_ma20,
                "vol_ma5": vol_ma5,
                "high_52w": high_52w,
                "board_streak": board_streak,
                "prev_close_hist": prev_close_hist,
                "atr20": atr20,
                "ret5": ret5,
                "ret10": ret10,
                "ret20": ret20,
                "range20_pct": range20_pct,
                **trend_pack,
            }
            diag["feature_ok"] += 1
        except Exception as e:
            diag["feature_fail"] += 1
            diag_err(diag, e, "FEATURE")

    diag["t_features"] = time.perf_counter() - t0
    return features, raw_daily

def intraday_progress_fraction(now_ts):
    m = int((datetime.combine(now_ts.date(), now_ts.time()) - datetime.combine(now_ts.date(), dtime(9, 0))).total_seconds() // 60)
    m = max(0, min(270, m))
    if m <= 30:
        return 0.12
    if m <= 120:
        return 0.12 + (0.50 - 0.12) * ((m - 30) / 90.0)
    return min(1.0, 0.50 + (1.00 - 0.50) * ((m - 120) / 150.0))


def get_thresholds(now_ts, is_test=False):
    m = int((datetime.combine(now_ts.date(), now_ts.time()) - datetime.combine(now_ts.date(), dtime(9, 0))).total_seconds() // 60)
    m = max(0, min(270, m))

    if is_test:
        if m <= 60:
            pullback_lim = 0.025
            dist_limit = 5.5
        elif m <= 180:
            pullback_lim = 0.018
            dist_limit = 4.6
        else:
            pullback_lim = 0.012
            dist_limit = 3.6
        return {
            "dist_limit": dist_limit,
            "vol_limit": 200_000,
            "pullback_lim": pullback_lim,
            "close_pos_min": 0.60,
            "vol_ratio_min": 0.85,
        }

    if m <= 60:
        dist_limit = 4.0
        pullback_lim = 0.018
    elif m <= 180:
        dist_limit = 3.0
        pullback_lim = 0.012
    else:
        dist_limit = 2.2
        pullback_lim = 0.008

    return {
        "dist_limit": dist_limit,
        "vol_limit": 500_000,
        "pullback_lim": pullback_lim,
        "close_pos_min": 0.72,
        "vol_ratio_min": 1.10,
    }


def score_to_star_count(signal_score, dist_pct, vol_ratio, board_streak, close_pos, proximity_52w, status_text=""):
    stars = 1
    if signal_score >= 8.8:
        stars = 5
    elif signal_score >= 7.0:
        stars = 4
    elif signal_score >= 5.4:
        stars = 3
    elif signal_score >= 4.0:
        stars = 2

    bonus = 0.0
    if dist_pct <= 0.20:
        bonus += 1.0
    elif dist_pct <= 0.50:
        bonus += 0.5

    if vol_ratio >= 3.0:
        bonus += 1.0
    elif vol_ratio >= 2.0:
        bonus += 0.5

    if board_streak >= 2:
        bonus += 1.0
    elif board_streak >= 1:
        bonus += 0.5

    if close_pos >= 0.95:
        bonus += 0.5
    elif close_pos < 0.85:
        bonus -= 0.5

    if proximity_52w >= 95:
        bonus += 0.5
    elif proximity_52w < 85:
        bonus -= 0.25

    if "最高價" in str(status_text) or "鎖" in str(status_text):
        bonus += 0.5

    stars = int(round(stars + bonus * 0.5))
    return max(1, min(5, stars))


def render_star_bar(stars):
    stars = max(1, min(5, int(stars)))
    return "★" * stars + "☆" * (5 - stars)


def compute_feature_from_history(df, today_date):
    if df is None or getattr(df, "empty", False):
        return None
    if not {"Close", "Volume", "High", "Low", "Open"}.issubset(set(df.columns)):
        return None
    df = df[["Open", "High", "Low", "Close", "Volume"]].dropna().copy()
    if df.empty:
        return None
    dates_tw = pd.Index([idx_date_taipei(x) for x in df.index])
    past_df = df[dates_tw < today_date].copy()
    if len(past_df) < 35:
        return None

    close = past_df["Close"].astype(float)
    vol = past_df["Volume"].astype(float)
    high = past_df["High"].astype(float)
    low = past_df["Low"].astype(float)
    trend_pack = _build_trend_template_features(past_df)
    return {
        "vol_ma20": safe_float(vol.rolling(20).mean().iloc[-1], 0.0),
        "high_52w": safe_float(high.tail(252).max(), 0.0),
        "board_streak": _consecutive_limit_ups(past_df, tail_n=12),
        "prev_close_hist": safe_float(close.iloc[-1], 0.0),
        "atr20": safe_float((high - low).rolling(20).mean().iloc[-1], 0.0),
        "ret5": safe_float((close.iloc[-1] / close.iloc[-6] - 1) * 100.0, 0.0) if len(close) >= 6 and close.iloc[-6] > 0 else 0.0,
        "ret10": safe_float((close.iloc[-1] / close.iloc[-11] - 1) * 100.0, 0.0) if len(close) >= 11 and close.iloc[-11] > 0 else 0.0,
        "ret20": safe_float((close.iloc[-1] / close.iloc[-21] - 1) * 100.0, 0.0) if len(close) >= 21 and close.iloc[-21] > 0 else 0.0,
        "range20_pct": safe_float(((high.tail(20).max() - low.tail(20).min()) / max(close.iloc[-1], 1e-9)) * 100.0, 0.0) if len(close) >= 20 else 0.0,
        "vol_ma5": safe_float(vol.rolling(5).mean().iloc[-1], 0.0),
        **trend_pack,
    }

def resolve_stock_query(query, meta_dict):
    q = str(query or "").strip()
    if not q:
        return None, []
    nq = re.sub(r"\s+", "", q).upper()
    if nq.isdigit() and len(nq) == 4 and nq in meta_dict:
        return nq, []

    exact_name = [code for code, info in meta_dict.items() if re.sub(r"\s+", "", str(info.get("name", ""))).upper() == nq]
    if len(exact_name) == 1:
        return exact_name[0], []

    prefix = []
    partial = []
    for code, info in meta_dict.items():
        name_norm = re.sub(r"\s+", "", str(info.get("name", ""))).upper()
        if code.startswith(nq) or name_norm.startswith(nq):
            prefix.append(code)
        elif nq in code or nq in name_norm:
            partial.append(code)
    matches = stable_unique(exact_name + prefix + partial)
    if len(matches) == 1:
        return matches[0], []
    return None, matches[:8]


def fetch_single_quote_row(session, api_key, code, meta_dict):
    r = fugle_get_json(session, f"intraday/quote/{code}", api_key)
    if r.status_code != 200:
        raise RuntimeError(f"QUOTE_{code}_{r.status_code}")
    j = r.json()
    ref = safe_float(j.get("referencePrice"), 0.0)
    last = safe_float(j.get("closePrice"), ref)
    high = safe_float(j.get("highPrice"), last)
    low = safe_float(j.get("lowPrice"), last)
    open_ = safe_float(j.get("openPrice"), ref)
    vol = safe_int((j.get("total") or {}).get("tradeVolume"), 0)
    bids = j.get("bids", []) or []
    asks = j.get("asks", []) or []
    best_bid = safe_float(bids[0].get("price"), 0.0) if bids else 0.0
    best_bid_size = safe_int(bids[0].get("size"), 0) if bids else 0
    best_ask = safe_float(asks[0].get("price"), 0.0) if asks else 0.0
    best_ask_size = safe_int(asks[0].get("size"), 0) if asks else 0
    if ref <= 0 or last <= 0:
        raise RuntimeError(f"QUOTE_{code}_EMPTY")
    upper = calc_limit_up(ref)
    return {
        "code": code,
        "name": meta_dict[code]["name"],
        "market": market_of(code, meta_dict),
        "industry": meta_dict[code].get("industry", "其他"),
        "open": open_,
        "high": high,
        "low": low,
        "last": last,
        "vol_sh": vol,
        "trade_value": 0.0,
        "change": last - ref,
        "change_pct": ((last - ref) / ref * 100.0) if ref else 0.0,
        "prev_close": ref,
        "upper": upper,
        "dist": max(0.0, (upper - last) / max(upper, 1e-9) * 100.0),
        "last_updated": 0,
        "best_bid": best_bid,
        "best_bid_size": best_bid_size,
        "best_ask": best_ask,
        "best_ask_size": best_ask_size,
        "rank_order": 9999,
    }


def evaluate_candidate_record(r, feat, now_ts, is_test, use_bloodline, only_tse, min_board, use_trend_template=DEFAULT_USE_TREND_TEMPLATE, account_size=DEFAULT_ACCOUNT_SIZE, risk_per_trade_pct=DEFAULT_RISK_PER_TRADE_PCT, free_roll_trigger_r=DEFAULT_FREE_ROLL_TRIGGER_R):
    code = r["code"]
    name = r["name"]
    market = r.get("market", "上市")
    if only_tse and market != "上市":
        return {"passed": False, "reason_key": "市場不符", "reason_text": "目前設定只看上市", "item": None}

    hist_missing = feat is None
    feat = feat or {}
    vol_ma20 = safe_float(feat.get("vol_ma20"), 0.0)
    board_streak = safe_int(feat.get("board_streak"), 0)
    high_52w = safe_float(feat.get("high_52w"), 0.0)
    ret5 = safe_float(feat.get("ret5"), 0.0)
    ret10 = safe_float(feat.get("ret10"), 0.0)
    ret20 = safe_float(feat.get("ret20"), 0.0)
    range20_pct = safe_float(feat.get("range20_pct"), 0.0)
    vol_ma5 = safe_float(feat.get("vol_ma5"), 0.0)
    ma50 = safe_float(feat.get("ma50"), 0.0)
    ma150 = safe_float(feat.get("ma150"), 0.0)
    ma200 = safe_float(feat.get("ma200"), 0.0)
    stage2_pass = bool(feat.get("trend_template_pass", False))
    stage2_score = safe_int(feat.get("trend_template_score", 0), 0)
    stage2_note = str(feat.get("trend_template_note", "資料不足"))

    if vol_ma20 <= 0:
        vol_ma20 = max(safe_float(r.get("vol_sh", 0), 0.0), 1.0)
        hist_missing = True

    if use_trend_template and (not hist_missing) and (not stage2_pass):
        return {"passed": False, "reason_key": "Stage2未過", "reason_text": stage2_note or "未通過 Stage 2 / Trend Template", "item": None}

    th = get_thresholds(now_ts, is_test=is_test)
    frac = intraday_progress_fraction(now_ts)
    base_vol = max(vol_ma20 * (1.0 if is_test else frac), 1e-9)
    vol_ratio_live = r["vol_sh"] / base_vol
    rng = max(r["high"] - r["low"], 0.0)
    pullback = (r["high"] - r["last"]) / max(r["high"], 1e-9)
    close_pos = 1.0 if rng < 1e-9 else (r["last"] - r["low"]) / max(rng, 1e-9)

    bid_price = safe_float(r.get("best_bid", 0.0), 0.0)
    bid_size = safe_int(r.get("best_bid_size", 0), 0)
    near_limit = r["last"] >= r["upper"] - tw_tick(r["upper"])
    hard_locked = near_limit and bid_price >= r["upper"] - tw_tick(r["upper"]) and bid_size >= (80000 if r["last"] < 50 else 120000 if r["last"] < 100 else 200000)
    proximity_52w = (r["last"] / max(high_52w, 1e-9) * 100.0) if high_52w > 0 else 0.0

    score = 0.0
    score += min(3.2, max(0.0, 3.2 - r["dist"] * 0.85))
    score += min(3.0, max(0.0, vol_ratio_live - 0.85))
    score += 1.1 if close_pos >= 0.92 else 0.6 if close_pos >= 0.78 else 0.2 if close_pos >= 0.62 else 0.0
    score += min(0.22, board_streak * 0.10)
    score += 0.9 if proximity_52w >= 95 else 0.5 if proximity_52w >= 88 else 0.0
    score += 0.35 if ret5 > 0 else 0.0
    score += 0.35 if ret20 > 0 else 0.0
    if stage2_pass:
        score += 1.0
    elif not hist_missing:
        score -= 0.8

    risk_flags = []
    risk_count = 0

    if hist_missing:
        score -= 0.55
        risk_count += 1
        risk_flags.append("歷史不足")

    if not stage2_pass and not hist_missing:
        risk_count += 1
        risk_flags.append(f"Stage2未過：{stage2_note}")

    if r["dist"] > th["dist_limit"]:
        score -= min(1.0, (r["dist"] - th["dist_limit"]) * 0.45)
        risk_count += 1
        risk_flags.append("離高點偏遠")

    if r["vol_sh"] < th["vol_limit"]:
        score -= 0.7
        risk_count += 1
        risk_flags.append("成交量偏低")

    if vol_ratio_live < th["vol_ratio_min"]:
        score -= min(1.0, (th["vol_ratio_min"] - vol_ratio_live) * 1.1)
        risk_count += 1
        risk_flags.append("熱度不足")

    if pullback > th["pullback_lim"]:
        score -= min(1.1, (pullback - th["pullback_lim"]) * 28)
        risk_count += 1
        risk_flags.append("回落偏大")

    if close_pos < th["close_pos_min"] and rng > max(0.1, r["last"] * 0.002):
        score -= min(0.9, (th["close_pos_min"] - close_pos) * 2.2)
        risk_count += 1
        risk_flags.append("收在偏低")

    if ma50 > 0 and r["last"] < ma50:
        score -= 0.9
        risk_count += 1
        risk_flags.append("跌回 50MA 下方")

    bloodline_note = ""
    if use_bloodline:
        if board_streak >= max(2, min_board + 1):
            score += 1.35
            bloodline_note = "｜血統強"
            risk_count = max(0, risk_count - 1)
        elif board_streak >= min_board:
            score += 0.70
            bloodline_note = "｜血統穩"
        else:
            score -= 0.80 if not is_test else 0.35
            risk_flags.append("血統偏弱")
            risk_count += 1
            bloodline_note = "｜新起漲"
    else:
        if board_streak == 0:
            score += 0.22

    if is_test:
        score += 0.85
        risk_count = max(0, risk_count - 2)
        if len(risk_flags) >= 2:
            risk_flags = risk_flags[:-2]
        elif risk_flags:
            risk_flags = risk_flags[:-1]

    signal_score = max(0.0, min(10.0, round(score, 2)))

    breakout_score = 0.0
    if vol_ratio_live >= 1.15:
        breakout_score += 1.2
    elif vol_ratio_live >= 0.95:
        breakout_score += 0.6

    if close_pos >= 0.82:
        breakout_score += 1.0
    elif close_pos >= 0.70:
        breakout_score += 0.45

    if r["dist"] <= 2.8:
        breakout_score += 1.0
    elif r["dist"] <= 4.2:
        breakout_score += 0.45

    if safe_float(r.get("change_pct", 0.0), 0.0) >= 4.0:
        breakout_score += 0.9
    elif safe_float(r.get("change_pct", 0.0), 0.0) >= 1.5:
        breakout_score += 0.45

    if proximity_52w >= 85:
        breakout_score += 0.6
    elif proximity_52w >= 78:
        breakout_score += 0.3

    if ret20 > 0 or ret5 > 0:
        breakout_score += 0.45

    if 0 < ret20 <= 12 and ret5 >= 1.2 and range20_pct <= 18:
        breakout_score += 1.15
    elif -3 <= ret20 <= 8 and ret10 >= 2.0 and range20_pct <= 14:
        breakout_score += 0.85

    if vol_ma20 > 0 and vol_ma5 / max(vol_ma20, 1e-9) >= 1.25:
        breakout_score += 0.65
    elif vol_ma20 > 0 and vol_ma5 / max(vol_ma20, 1e-9) >= 1.10:
        breakout_score += 0.30

    if stage2_pass:
        breakout_score += 0.55
    elif not hist_missing:
        breakout_score -= 0.35

    if use_bloodline:
        if board_streak >= max(2, min_board + 1):
            breakout_score += 0.55
        elif board_streak >= min_board:
            breakout_score += 0.25
        else:
            breakout_score -= 0.30 if not is_test else 0.12
    else:
        if board_streak == 0:
            breakout_score += 0.45
        elif board_streak >= 2:
            breakout_score -= 0.12

    if hist_missing:
        breakout_score -= 0.25
    if pullback > th["pullback_lim"]:
        breakout_score -= 0.35

    breakout_score = max(0.0, round(breakout_score, 2))

    status = "🔒 漲到頂買不到" if hard_locked else "🟣 快漲到最高價" if near_limit else "⚡ 強力上漲中"
    if stage2_pass:
        status = f"{status}｜Stage2合格{bloodline_note}"
    else:
        status = f"{status}｜Stage2待補強{bloodline_note}"

    star_count = score_to_star_count(
        signal_score=signal_score,
        dist_pct=r["dist"],
        vol_ratio=vol_ratio_live,
        board_streak=board_streak,
        close_pos=close_pos,
        proximity_52w=proximity_52w,
        status_text=status,
    )

    item = {
        "代號": code,
        "名稱": name,
        "市場": market,
        "產業": str(r.get("industry", "其他") or "其他"),
        "現價": r["last"],
        "漲幅%": safe_float(r.get("change_pct", 0.0), 0.0),
        "今日最高": safe_float(r.get("high", 0.0), 0.0),
        "距離最高價%": r["dist"],
        "交易熱度": vol_ratio_live,
        "今日表現分數": signal_score,
        "推薦星等": star_count,
        "推薦指數": render_star_bar(star_count),
        "狀態": status,
        "階段": f"過去連續大漲 {board_streak} 天",
        "board_val": board_streak,
        "close_pos": close_pos,
        "pullback": pullback,
        "接近一年最高價%": proximity_52w,
        "近5天表現%": ret5,
        "近20天表現%": ret20,
        "best_bid": bid_price,
        "best_bid_size": bid_size,
        "best_ask": safe_float(r.get("best_ask", 0.0), 0.0),
        "best_ask_size": safe_int(r.get("best_ask_size", 0), 0),
        "成交量": safe_int(r.get("vol_sh", 0), 0),
        "風險數": int(risk_count),
        "風險標記": "、".join(risk_flags) if risk_flags else "低風險",
        "起漲雷達分數": breakout_score,
        "突破區間分數": round((1.2 if (0 < ret20 <= 12 and ret5 >= 1.2 and range20_pct <= 18) else 0.85 if (-3 <= ret20 <= 8 and ret10 >= 2.0 and range20_pct <= 14) else 0.0), 2),
        "整理區間20日%": range20_pct,
        "近10天表現%": ret10,
        "量能抬升比": round(vol_ma5 / max(vol_ma20, 1e-9), 2) if vol_ma20 > 0 else 1.0,
        "保底補位": "",
        "Stage2模板": "通過" if stage2_pass else "未通過",
        "Stage2分數": f"{stage2_score}/7",
        "Stage2說明": stage2_note,
        "50MA": round(ma50, 2) if ma50 > 0 else 0.0,
        "150MA": round(ma150, 2) if ma150 > 0 else 0.0,
        "200MA": round(ma200, 2) if ma200 > 0 else 0.0,
    }
    item.update(build_trade_management_plan(item, feat=feat, account_size=account_size, risk_pct=risk_per_trade_pct, free_roll_r=free_roll_trigger_r))
    return {"passed": True, "reason_key": "通過", "reason_text": item["風險標記"], "item": item}

def build_history_pattern_table(df):
    if df is None or getattr(df, "empty", False):
        return pd.DataFrame()
    if not {"Open", "High", "Low", "Close", "Volume"}.issubset(set(df.columns)):
        return pd.DataFrame()

    x = df[["Open", "High", "Low", "Close", "Volume"]].dropna().copy()
    if len(x) < 40:
        return pd.DataFrame()

    x["prev_close"] = x["Close"].shift(1)
    x["vol_ma20"] = x["Volume"].rolling(20).mean()
    x["chg_pct"] = (x["Close"] / x["prev_close"] - 1.0) * 100.0
    x["range"] = (x["High"] - x["Low"]).clip(lower=1e-9)
    x["close_pos"] = (x["Close"] - x["Low"]) / x["range"]
    x["vol_ratio"] = x["Volume"] / x["vol_ma20"].replace(0, pd.NA)
    x["high_52w"] = x["High"].rolling(252, min_periods=60).max()
    x["proximity_52w"] = (x["Close"] / x["high_52w"].replace(0, pd.NA)) * 100.0

    dist_list = []
    board_list = [0] * len(x)
    vals = x.reset_index(drop=False)

    for i in range(len(vals)):
        prev_close = safe_float(vals.loc[i, "prev_close"], 0.0)
        close_now = safe_float(vals.loc[i, "Close"], 0.0)

        if prev_close > 0 and close_now > 0:
            upper = calc_limit_up(prev_close)
            dist_pct = max(0.0, (upper - close_now) / max(upper, 1e-9) * 100.0)
        else:
            dist_pct = None
        dist_list.append(dist_pct)

        if i >= 1:
            streak = 0
            j = i
            while j >= 1:
                cp = safe_float(vals.loc[j, "Close"], 0.0)
                pp = safe_float(vals.loc[j - 1, "Close"], 0.0)
                if cp > 0 and pp > 0:
                    lim = calc_limit_up(pp)
                    if cp >= lim - tw_tick(lim):
                        streak += 1
                        j -= 1
                        continue
                break
            board_list[i] = streak

    x["dist_pct"] = dist_list
    x["board_streak"] = board_list
    x = x.dropna(subset=["prev_close", "vol_ratio", "close_pos", "proximity_52w", "dist_pct"]).copy()

    if x.empty:
        return pd.DataFrame()

    return x.sort_index()


def estimate_continuation_from_history(hist_df, item, lookahead_days=5):
    empty_result = {
        "預測主句": "白話預測：歷史樣本太少，這張先不硬猜",
        "預測副句": "先以原本的分數、熱度和位置為主",
        "預估續強天數": None,
        "再衝高機率": None,
        "預測樣本數": 0,
        "預測信心": "不足",
    }

    table = build_history_pattern_table(hist_df)
    if table.empty or len(table) < 50:
        return empty_result

    if len(table) <= lookahead_days + 8:
        return empty_result

    usable = table.iloc[:-lookahead_days].copy()
    if usable.empty:
        return empty_result

    target_change = safe_float(item.get("漲幅%", 6.0), 6.0)
    target_vol = safe_float(item.get("交易熱度", 1.5), 1.5)
    target_close_pos = safe_float(item.get("close_pos", 0.85), 0.85)
    target_board = safe_int(item.get("board_val", 0), 0)
    target_prox = safe_float(item.get("接近一年最高價%", 85.0), 85.0)
    target_dist = safe_float(item.get("距離最高價%", 2.0), 2.0)

    def pick_matches(level="tight"):
        if level == "tight":
            chg_min = max(5.0, min(target_change - 2.0, 9.0))
            vol_min = max(1.3, min(target_vol * 0.70, 3.0))
            close_pos_min = max(0.78, target_close_pos - 0.08)
            prox_min = max(82.0, target_prox - 6.0)
            dist_max = max(2.5, target_dist + 0.8)
            board_min = target_board
        elif level == "mid":
            chg_min = max(4.0, min(target_change - 3.0, 8.0))
            vol_min = max(1.1, min(target_vol * 0.55, 2.5))
            close_pos_min = max(0.72, target_close_pos - 0.12)
            prox_min = max(78.0, target_prox - 10.0)
            dist_max = max(3.2, target_dist + 1.4)
            board_min = max(0, target_board - 1)
        else:
            chg_min = max(3.0, min(target_change - 4.0, 7.0))
            vol_min = max(1.0, min(target_vol * 0.45, 2.0))
            close_pos_min = 0.68
            prox_min = max(75.0, target_prox - 14.0)
            dist_max = max(4.0, target_dist + 2.0)
            board_min = 1 if target_board >= 2 else 0

        mask = (
            (usable["chg_pct"] >= chg_min)
            & (usable["vol_ratio"] >= vol_min)
            & (usable["close_pos"] >= close_pos_min)
            & (usable["proximity_52w"] >= prox_min)
            & (usable["dist_pct"] <= dist_max)
        )

        if board_min > 0:
            mask &= usable["board_streak"] >= board_min

        return usable[mask].copy()

    matched = pd.DataFrame()
    for level in ["tight", "mid", "loose"]:
        matched = pick_matches(level)
        if len(matched) >= 8:
            break

    if len(matched) < 6:
        return empty_result

    records = []
    for idx, row in matched.iterrows():
        pos = table.index.get_loc(idx)
        future = table.iloc[pos + 1 : pos + 1 + lookahead_days].copy()
        if len(future) < lookahead_days:
            continue

        signal_high = safe_float(row["High"], 0.0)
        signal_close = safe_float(row["Close"], 0.0)
        barrier = signal_high + tw_tick(signal_high) * 0.5

        last_strong_day = 0
        for offset, (_, frow) in enumerate(future.iterrows(), start=1):
            if safe_float(frow["High"], 0.0) > barrier:
                last_strong_day = offset

        keep_days = 0
        for offset, (_, frow) in enumerate(future.iterrows(), start=1):
            if safe_float(frow["Close"], 0.0) >= signal_close:
                keep_days = offset
            else:
                break

        hit3 = 1 if safe_float(future.head(3)["High"].max(), 0.0) > barrier else 0

        records.append(
            {
                "續強天數": last_strong_day,
                "守住天數": keep_days,
                "三天內再衝高": hit3,
            }
        )

    if len(records) < 6:
        return empty_result

    stat_df = pd.DataFrame(records)
    sample_n = len(stat_df)

    est_days = int(round(max(
        safe_float(stat_df["續強天數"].median(), 0.0),
        safe_float(stat_df["守住天數"].median(), 0.0),
    )))
    est_days = max(1, min(lookahead_days, est_days))

    prob3 = int(round(stat_df["三天內再衝高"].mean() * 100.0))

    if prob3 < 40 and est_days > 2:
        est_days = 2
    if prob3 >= 75 and est_days < 2:
        est_days = 2

    if sample_n >= 24:
        confidence = "高"
    elif sample_n >= 12:
        confidence = "中"
    else:
        confidence = "低"

    sub_tail = ""
    if confidence == "低":
        sub_tail = "（樣本偏少，先參考就好）"

    return {
        "預測主句": f"白話預測：大概還有 {est_days} 天續強空間",
        "預測副句": f"歷史上像今天這種走法，3 天內再衝高的機率約 {prob3}%｜樣本 {sample_n} 次｜信心：{confidence}{sub_tail}",
        "預估續強天數": est_days,
        "再衝高機率": prob3,
        "預測樣本數": sample_n,
        "預測信心": confidence,
    }


def attach_continuation_prediction(res_df, raw_daily, meta_dict):
    if res_df is None or res_df.empty:
        return res_df

    out_rows = []
    for _, row in res_df.iterrows():
        item = row.to_dict()
        code = str(item.get("代號", "")).strip()

        hist_df = pd.DataFrame()
        try:
            if code in meta_dict:
                sym = symbol_of(code, meta_dict)
                hist_df = _extract_symbol_frame(raw_daily, sym)
        except Exception:
            hist_df = pd.DataFrame()

        pred = estimate_continuation_from_history(hist_df, item)
        item.update(pred)
        out_rows.append(item)

    return pd.DataFrame(out_rows)


def evaluate_single_search(query, meta_dict, api_key, now_ts, is_test, use_bloodline, min_board, vault=None):
    code, matches = resolve_stock_query(query, meta_dict)
    if not code:
        if matches:
            return {
                "ok": False,
                "kind": "ambiguous",
                "message": "找到多個類似的目標，請輸入更完整的股票代號或名稱。",
                "matches": [{"code": c, "name": meta_dict[c]["name"], "market": market_label(meta_dict[c]["market"])} for c in matches],
            }
        return {"ok": False, "kind": "not_found", "message": "找不到這支股票，請確認代號或名稱是否正確。", "matches": []}

    row = None
    feat = None
    hist_df = pd.DataFrame()
    source = []

    if vault:
        cdf = vault.get("candidate_df")
        if cdf is not None and not getattr(cdf, "empty", False):
            hit = cdf[cdf["code"] == code]
            if not hit.empty:
                row = hit.iloc[0].to_dict()
                source.append("已下載好的資料庫")

        feat = (vault.get("feature_cache") or {}).get(code)
        if feat:
            source.append("已計算過的過去表現")

        try:
            raw_daily_vault = vault.get("raw_daily")
            if raw_daily_vault is not None and code in meta_dict:
                sym = symbol_of(code, meta_dict)
                hist_df = _extract_symbol_frame(raw_daily_vault, sym)
        except Exception:
            hist_df = pd.DataFrame()

    if row is None:
        session = make_retry_session()
        row = fetch_single_quote_row(session, api_key, code, meta_dict)
        source.append("即時查詢最新報價")

    if feat is None or hist_df.empty:
        sym = symbol_of(code, meta_dict)
        raw_daily = yf_download_daily([sym], period=f"{RAW_HISTORY_DAYS}d")
        df = _extract_symbol_frame(raw_daily, sym)
        hist_df = df.copy()
        feat = compute_feature_from_history(df, now_ts.date())
        source.append("剛下載好的歷史資料")

    assessment = evaluate_candidate_record(
        r=row,
        feat=feat,
        now_ts=now_ts,
        is_test=is_test,
        use_bloodline=use_bloodline,
        only_tse=False,
        min_board=min_board,
    )

    if assessment.get("item"):
        pred = estimate_continuation_from_history(hist_df, assessment["item"])
        assessment["item"].update(pred)
        item = assessment["item"]
        # 獨立搜尋也補上簡易族群資訊與入選理由，避免卡片資訊不完整
        if vault and isinstance(vault, dict):
            cdf = vault.get("candidate_df")
            if cdf is not None and not getattr(cdf, "empty", False) and "industry" in cdf.columns:
                ind = str(item.get("產業", "其他") or "其他")
                base = cdf.copy()
                base["_industry"] = base["industry"].fillna("其他").replace("", "其他")
                hs = base.get("high", pd.Series([0]*len(base), index=base.index)).astype(float)
                ls = base.get("low", pd.Series([0]*len(base), index=base.index)).astype(float)
                lasts = base.get("last", pd.Series([0]*len(base), index=base.index)).astype(float)
                cps = ((lasts-ls)/(hs-ls).clip(lower=1e-9)).clip(lower=0.0, upper=1.0)
                rm = (
                    (base.get("change_pct", pd.Series([0]*len(base), index=base.index)).astype(float) >= (2.8 if not is_test else 1.5)) &
                    (base.get("dist", pd.Series([99]*len(base), index=base.index)).astype(float) <= (3.8 if not is_test else 5.0)) &
                    (base.get("vol_sh", pd.Series([0]*len(base), index=base.index)).astype(float) >= (800000 if not is_test else 320000)) &
                    (cps >= (0.82 if not is_test else 0.74))
                )
                total_n = int((base["_industry"] == ind).sum()) if ind else 1
                rise_n_all = int(((base["_industry"] == ind) & rm).sum()) if ind else 0
                peer_total_n = max(0, total_n - 1)
                peer_rise_n = max(0, rise_n_all - 1) if rise_n_all > 0 else 0
                if peer_rise_n >= 1:
                    item["族群狀態"] = f"同族群跟漲 {peer_rise_n} 檔"
                elif peer_total_n >= 1:
                    item["族群狀態"] = f"同族群 {peer_total_n} 檔｜未同步"
                else:
                    item["族群狀態"] = "一支獨秀"
                item["同族群跟漲數"] = peer_rise_n
                item["族群共振分數"] = 3.4 if peer_rise_n >=4 else 2.7 if peer_rise_n==3 else 1.8 if peer_rise_n==2 else 0.8 if peer_rise_n==1 else 0.0
        item["入選理由"] = build_reason_tags(item)
        item.update(build_position_advice(item))

    return {
        "ok": True,
        "kind": "result",
        "code": code,
        "name": meta_dict[code]["name"],
        "market": market_label(meta_dict[code]["market"]),
        "assessment": assessment,
        "source": " / ".join(source),
    }



def apply_dynamic_filters(raw_df, feature_cache, now_ts, is_test, use_bloodline, only_tse, min_board, base_diag):
    diag = copy_diag(base_diag)
    stats = {"候選總數": 0}
    if raw_df is None or raw_df.empty:
        return pd.DataFrame(), stats, diag

    work = raw_df.copy()
    if only_tse:
        work = work[work["market"] == "上市"].copy()

    stats["候選總數"] = len(work)
    if work.empty:
        diag["final_count"] = 0
        return pd.DataFrame(), stats, diag

    # 先用候選池計算「同族群一起轉強」數，後面直接吃進主評分與分級
    industry_col = work["industry"].fillna("其他").replace("", "其他") if "industry" in work.columns else pd.Series(["其他"] * len(work), index=work.index)
    industry_counts = industry_col.value_counts()
    high_s = work.get("high", pd.Series([0] * len(work), index=work.index)).astype(float)
    low_s = work.get("low", pd.Series([0] * len(work), index=work.index)).astype(float)
    last_s = work.get("last", pd.Series([0] * len(work), index=work.index)).astype(float)
    rng_s = (high_s - low_s).clip(lower=1e-9)
    close_pos_s = ((last_s - low_s) / rng_s).clip(lower=0.0, upper=1.0)
    trade_value_s = work.get("trade_value", pd.Series([0] * len(work), index=work.index)).astype(float)
    rising_mask = (
        (work.get("change_pct", pd.Series([0] * len(work), index=work.index)).astype(float) >= (2.8 if not is_test else 1.5)) &
        (work.get("dist", pd.Series([99] * len(work), index=work.index)).astype(float) <= (3.8 if not is_test else 5.0)) &
        (work.get("vol_sh", pd.Series([0] * len(work), index=work.index)).astype(float) >= (800000 if not is_test else 320000)) &
        (trade_value_s >= (40000000 if not is_test else 18000000)) &
        (close_pos_s >= (0.82 if not is_test else 0.74))
    )
    industry_rising = industry_col[rising_mask].value_counts()

    out = []
    for _, r in work.iterrows():
        assessment = evaluate_candidate_record(
            r=r,
            feat=feature_cache.get(r["code"]),
            now_ts=now_ts,
            is_test=is_test,
            use_bloodline=use_bloodline,
            only_tse=only_tse,
            min_board=min_board,
        )
        if not assessment.get("passed"):
            if assessment.get("reason_key") == "資訊不足":
                diag["yf_fail"] += 1
            continue
        item = assessment.get("item")
        if item:
            ind = str(item.get("產業", "其他") or "其他")
            rise_n_all = int(industry_rising.get(ind, 0)) if len(industry_rising) else 0
            total_n = int(industry_counts.get(ind, 1)) if len(industry_counts) else 1
            peer_total_n = max(0, total_n - 1)
            peer_rise_n = max(0, rise_n_all - 1) if rise_n_all > 0 else 0
            if peer_rise_n >= 1:
                cluster_status = f"同族群跟漲 {peer_rise_n} 檔"
            elif peer_total_n >= 1:
                cluster_status = f"同族群 {peer_total_n} 檔｜未同步"
            else:
                cluster_status = "一支獨秀"
            if peer_rise_n >= 4:
                cluster_score = 3.4
            elif peer_rise_n == 3:
                cluster_score = 2.7
            elif peer_rise_n == 2:
                cluster_score = 1.8
            elif peer_rise_n == 1:
                cluster_score = 0.8
            else:
                cluster_score = 0.0
            item["同產業檔數"] = total_n
            item["同族群跟漲數"] = peer_rise_n
            item["族群狀態"] = cluster_status
            item["族群共振分數"] = cluster_score

            out.append(item)

    res = pd.DataFrame(out)
    if not res.empty:
        # 模式排序分：讓兩個開關真的會改變排序與入選名單，而不只是顯示文案不同
        if "風險數" not in res.columns:
            res["風險數"] = 0
        if "起漲雷達分數" not in res.columns:
            res["起漲雷達分數"] = 0.0

        if "族群共振分數" not in res.columns:
            res["族群共振分數"] = 0.0
        if "同族群跟漲數" not in res.columns:
            res["同族群跟漲數"] = 0
        res["模式排序分"] = (
            res["今日表現分數"].astype(float)
            + res["起漲雷達分數"].astype(float) * 0.72
            + res["族群共振分數"].astype(float) * 0.58
            - res["風險數"].astype(float) * 0.40
        )

        if use_bloodline:
            res["模式排序分"] += res["board_val"].astype(float).clip(upper=3) * 0.55
            res.loc[res["board_val"].astype(int) == 0, "模式排序分"] -= 0.55
        else:
            res.loc[res["board_val"].astype(int) == 0, "模式排序分"] += 0.25

        if is_test:
            res["模式排序分"] += 0.55
            res.loc[res["交易熱度"].astype(float) >= 0.80, "模式排序分"] += 0.18
            res.loc[res["距離最高價%"].astype(float) <= 6.8, "模式排序分"] += 0.18
            res.loc[res["風險數"].astype(int) <= 3, "模式排序分"] += 0.12

        res = res.sort_values(
            ["模式排序分", "今日表現分數", "推薦星等", "交易熱度", "board_val", "距離最高價%"],
            ascending=[False, False, False, False, False, True],
        ).reset_index(drop=True)

        score = res["今日表現分數"].astype(float)
        risk = res["風險數"].astype(int)

        if is_test:
            a_score, a_risk = 5.9, 3
            b_score, b_risk = 4.6, 5
        else:
            a_score, a_risk = 6.8, 2
            b_score, b_risk = 5.4, 4

        radar = res["起漲雷達分數"].astype(float)

        def _tier(row):
            s = float(row["今日表現分數"])
            mode_s = float(row.get("模式排序分", s))
            rsk = int(row["風險數"])
            radar_s = float(row.get("起漲雷達分數", 0.0))
            chg = float(row.get("漲幅%", 0.0))
            dist = float(row.get("距離最高價%", 99.0))
            heat = float(row.get("交易熱度", 0.0))
            cp = float(row.get("close_pos", 0.0))
            board = int(row.get("board_val", 0))
            cluster_s = float(row.get("族群共振分數", 0.0))
            rising_n = int(row.get("同族群跟漲數", 0))
            breakout_s = float(row.get("突破區間分數", 0.0))
            vol_lift = float(row.get("量能抬升比", 1.0))

            blood_a_ok = (board >= 1) or (not use_bloodline)
            blood_b_ok = (board >= 1) or (not use_bloodline) or (radar_s >= 4.4 and s >= b_score + 0.45)

            if mode_s >= (a_score + 1.1 if not is_test else a_score + 0.7) and s >= a_score and rsk <= a_risk and chg >= (3.6 if is_test else 4.0) and heat >= (0.95 if is_test else 1.15) and cp >= (0.74 if is_test else 0.78) and blood_a_ok:
                return "A級焦點"

            if (
                radar_s >= (3.45 if is_test else 3.85)
                and mode_s >= (6.4 if is_test else 6.9)
                and s >= max(4.7 if is_test else 5.0, b_score - 0.20)
                and rsk <= min(b_risk, 4 if not is_test else 5)
                and dist <= (5.0 if is_test else 4.2)
                and 0.3 <= chg <= 9.8
                and cp >= (0.68 if is_test else 0.74)
                and vol_lift >= (1.05 if is_test else 1.10)
                and breakout_s >= (0.80 if is_test else 0.95)
                and (cluster_s >= (0.9 if is_test else 1.0) or rising_n >= 1 or radar_s >= 5.0)
                and blood_b_ok
            ):
                return "B級觀察"

            # C 級要像真正候補，不要太像小 B 級；放寬模式下再稍微鬆一點
            if is_test:
                c_radar_min, c_heat_min, c_dist_max, c_chg_min = 1.8, 0.72, 7.4, 0.1
            else:
                c_radar_min, c_heat_min, c_dist_max, c_chg_min = 2.0, 0.78, 6.8, 0.2

            if radar_s >= c_radar_min and heat >= c_heat_min and dist <= c_dist_max and chg >= c_chg_min:
                return "C級候補"
            return "排除"

        res["分級"] = res.apply(_tier, axis=1)
        res["模式分級"] = res["分級"]

        # A 保底弱化：只有最高分真的夠強時才補，不強求一定要有 A
        if (res["分級"] == "A級焦點").sum() == 0 and len(res) >= 1:
            top_pick = res.sort_values(["今日表現分數", "起漲雷達分數"], ascending=[False, False]).head(1).copy()
            if not top_pick.empty:
                top_score = safe_float(top_pick["今日表現分數"].iloc[0], 0.0)
                top_radar = safe_float(top_pick["起漲雷達分數"].iloc[0], 0.0)
                top_risk = safe_int(top_pick["風險數"].iloc[0], 9)
                if top_score >= max(6.3, a_score - 0.2) and top_radar >= 3.8 and top_risk <= a_risk + 1:
                    top_idx = top_pick.index
                    res.loc[top_idx, ["分級", "模式分級", "保底補位"]] = ["A級焦點", "A級焦點", "A保底"]

        if (res["分級"] == "B級觀察").sum() == 0 and len(res) >= 1:
            reserve_pool = res[~res.index.isin(res[res["分級"] == "A級焦點"].index)].copy()
            reserve_pool = reserve_pool[
                (reserve_pool["起漲雷達分數"].astype(float) >= (4.25 if is_test else 4.55))
                & (reserve_pool["突破區間分數"].astype(float) >= (0.95 if is_test else 1.05))
                & (reserve_pool["量能抬升比"].astype(float) >= (1.08 if is_test else 1.12))
                & (reserve_pool["模式排序分"].astype(float) >= (6.2 if is_test else 6.8))
                & (
                    (reserve_pool["同族群跟漲數"].astype(int) >= 1)
                    | (reserve_pool["族群共振分數"].astype(float) >= 0.9)
                    | (reserve_pool["起漲雷達分數"].astype(float) >= 5.1)
                )
            ]
            reserve_idx = (
                reserve_pool.sort_values(["突破區間分數", "族群共振分數", "起漲雷達分數", "量能抬升比", "今日表現分數"], ascending=[False, False, False, False, False])
                .head(1)
                .index
            )
            if len(reserve_idx) > 0:
                res.loc[reserve_idx, ["分級", "模式分級", "保底補位"]] = ["B級觀察", "B級觀察", "B保底(嚴選)"]

        # C 不是垃圾桶：只留真正還有起漲味道的前 8 檔，但如果完全沒有，補少量 C 保底
        c_candidates = res[(res["分級"] == "C級候補") | (res["分級"] == "排除")].copy()
        if is_test:
            c_candidates = c_candidates[
                (c_candidates["起漲雷達分數"] >= 1.8) &
                (c_candidates["漲幅%"] >= 0.1) &
                (c_candidates["交易熱度"] >= 0.72)
            ]
        else:
            c_candidates = c_candidates[
                (c_candidates["起漲雷達分數"] >= 1.7) &
                (c_candidates["漲幅%"] >= 0.0) &
                (c_candidates["交易熱度"] >= 0.70)
            ]

        c_keep_idx = (
            c_candidates.sort_values(
                ["起漲雷達分數", "今日表現分數", "交易熱度", "距離最高價%"],
                ascending=[False, False, False, True]
            )
            .head(8)
            .index
        )

        if len(c_keep_idx) == 0:
            fallback_pool = res[~res["分級"].isin(["A級焦點", "B級觀察"])].copy()
            fallback_idx = (
                fallback_pool.sort_values(
                    ["起漲雷達分數", "今日表現分數", "交易熱度", "距離最高價%"],
                    ascending=[False, False, False, True]
                )
                .head(3)
                .index
            )
            res.loc[fallback_idx, ["分級", "模式分級", "保底補位"]] = ["C級候補", "C級候補", "C保底"]
        else:
            res.loc[res.index.isin(c_keep_idx), ["分級", "模式分級"]] = ["C級候補", "C級候補"]

        res.loc[
            (~res["分級"].isin(["A級焦點", "B級觀察", "C級候補"])),
            ["分級", "模式分級"]
        ] = ["排除", "排除"]
    else:
        res = pd.DataFrame(columns=["分級", "模式分級"])

    diag["final_count"] = len(res)
    return res, stats, diag



# ============================================================
# 歷史模擬驗證
# ============================================================
def pick_backtest_universe(raw_df, top_n=28):
    if raw_df is None or raw_df.empty:
        return []
    df = raw_df.sort_values(["change_pct", "vol_sh", "dist"], ascending=[False, False, True]).head(top_n)
    return df["code"].tolist()


def run_surrogate_backtest(raw_daily, universe_codes, meta_dict, lookback_days=126, hold_days=5, use_bloodline=True, min_board=1, is_test=False, use_trend_template=DEFAULT_USE_TREND_TEMPLATE):
    trades = []
    if raw_daily is None or getattr(raw_daily, "empty", False) or not universe_codes:
        return pd.DataFrame(), {
            "signals": 0,
            "wins": 0,
            "win_rate": 0.0,
            "avg_return": 0.0,
            "median_return": 0.0,
            "best": 0.0,
            "worst": 0.0,
        }

    for code in universe_codes:
        if code not in meta_dict:
            continue
        sym = symbol_of(code, meta_dict)
        df = _extract_symbol_frame(raw_daily, sym)
        if df.empty or not {"Open", "High", "Low", "Close", "Volume"}.issubset(set(df.columns)):
            continue
        df = df[["Open", "High", "Low", "Close", "Volume"]].dropna().copy()
        if len(df) < max(230, lookback_days + hold_days + 30):
            continue
        df = df.tail(lookback_days + hold_days + 220).copy()
        df["vol_ma20"] = df["Volume"].rolling(20).mean()
        df["prev_close"] = df["Close"].shift(1)
        df["chg_pct"] = (df["Close"] / df["prev_close"] - 1.0) * 100.0
        df["range"] = (df["High"] - df["Low"]).clip(lower=1e-9)
        df["close_pos"] = (df["Close"] - df["Low"]) / df["range"]
        df["vol_ratio"] = df["Volume"] / df["vol_ma20"]
        df["ma50"] = df["Close"].rolling(50).mean()
        df["ma150"] = df["Close"].rolling(150).mean()
        df["ma200"] = df["Close"].rolling(200).mean()
        df["ma200_prev20"] = df["ma200"].shift(20)
        df["high_52w"] = df["High"].rolling(252, min_periods=60).max()
        df["low_52w"] = df["Low"].rolling(252, min_periods=60).min()
        df["pivot_low_10"] = df["Low"].rolling(10).min()
        df["atr14"] = (df["High"] - df["Low"]).rolling(14).mean()

        board_list = [0] * len(df)
        vals = df.reset_index(drop=False)
        for i in range(1, len(vals)):
            streak = 0
            j = i
            while j >= 1:
                cp = safe_float(vals.loc[j, "Close"], 0.0)
                pp = safe_float(vals.loc[j - 1, "Close"], 0.0)
                if cp > 0 and pp > 0 and cp >= calc_limit_up(pp) - tw_tick(calc_limit_up(pp)):
                    streak += 1
                    j -= 1
                else:
                    break
            board_list[i] = streak
        df["board_streak"] = board_list

        df["dist_pct"] = 0.0
        for i in range(1, len(df)):
            prev_close = safe_float(df["Close"].iloc[i - 1], 0.0)
            close_now = safe_float(df["Close"].iloc[i], 0.0)
            if prev_close > 0 and close_now > 0:
                upper = calc_limit_up(prev_close)
                df.iloc[i, df.columns.get_loc("dist_pct")] = max(0.0, (upper - close_now) / max(upper, 1e-9) * 100.0)

        ret5 = (df["Close"] / df["Close"].shift(5) - 1.0) * 100.0
        ret10 = (df["Close"] / df["Close"].shift(10) - 1.0) * 100.0
        ret20 = (df["Close"] / df["Close"].shift(20) - 1.0) * 100.0
        range20_pct = ((df["High"].rolling(20).max() - df["Low"].rolling(20).min()) / df["Close"].replace(0, pd.NA)) * 100.0
        vol_ma5 = df["Volume"].rolling(5).mean()
        vol_lift = vol_ma5 / df["vol_ma20"].replace(0, pd.NA)

        trend_pass = (
            (df["Close"] > df["ma150"])
            & (df["Close"] > df["ma200"])
            & (df["ma50"] > df["ma150"])
            & (df["ma150"] > df["ma200"])
            & (df["Close"] > df["ma50"])
            & (df["ma200"] >= df["ma200_prev20"])
            & (df["low_52w"] > 0)
            & (df["Close"] >= df["low_52w"] * 1.30)
            & (df["high_52w"] > 0)
            & (df["Close"] >= df["high_52w"] * 0.75)
        )

        breakout_proxy = (
            (df["vol_ratio"] >= 1.15).astype(float) * 1.2
            + (df["close_pos"] >= 0.74).astype(float) * 0.9
            + (df["dist_pct"] <= 4.8).astype(float) * 0.9
            + (df["chg_pct"] >= 1.3).astype(float) * 0.7
            + (((ret20 > -3) & (ret20 <= 12) & (ret5 >= 0.8) & (range20_pct <= 18)).astype(float) * 1.2)
            + ((vol_lift >= 1.10).astype(float) * 0.6)
        )
        mode_proxy = (
            df["chg_pct"].clip(lower=0.0, upper=9.0) * 0.28
            + df["vol_ratio"].clip(lower=0.0, upper=3.5) * 1.05
            + df["close_pos"].clip(lower=0.0, upper=1.0) * 1.7
            + ((5.8 - df["dist_pct"].clip(lower=0.0, upper=5.8)) / 5.8) * 1.2
            + (breakout_proxy * 0.7)
            + ((vol_lift.fillna(1.0).clip(lower=0.8, upper=1.8) - 1.0) * 1.0)
            + (((ret20 > 0) & (ret5 > 0)).astype(float) * 0.5)
            + trend_pass.astype(float) * 0.9
        )
        if use_bloodline:
            mode_proxy += df["board_streak"].clip(upper=3) * 0.35
            mode_proxy -= (df["board_streak"] == 0).astype(float) * 0.18
        else:
            mode_proxy += (df["board_streak"] == 0).astype(float) * 0.18

        signal = (
            (mode_proxy >= (6.0 if not is_test else 5.5))
            & (breakout_proxy >= (2.2 if not is_test else 1.8))
            & (df["close_pos"] >= (0.70 if not is_test else 0.66))
            & (df["dist_pct"] <= (5.4 if not is_test else 6.4))
            & (vol_lift.fillna(1.0) >= (1.04 if not is_test else 1.00))
        )
        if use_trend_template:
            signal &= trend_pass.fillna(False)

        sig_idx = df.index[signal.fillna(False)].tolist()
        for idx in sig_idx:
            pos = df.index.get_loc(idx)
            if pos + 1 >= len(df):
                continue
            entry_pos = pos + 1
            entry_idx = df.index[entry_pos]
            entry = safe_float(df.loc[entry_idx, "Open"], 0.0)
            if entry <= 0:
                continue

            sig_ma50 = safe_float(df.loc[idx, "ma50"], 0.0)
            sig_pivot = safe_float(df.loc[idx, "pivot_low_10"], 0.0)
            sig_atr14 = safe_float(df.loc[idx, "atr14"], 0.0)
            stop_candidates = []
            if 0 < sig_pivot < entry:
                stop_candidates.append(sig_pivot - tw_tick(sig_pivot))
            if 0 < sig_ma50 < entry:
                stop_candidates.append(sig_ma50 * 0.985)
            if sig_atr14 > 0:
                atr_stop = entry - sig_atr14 * 2.0
                if 0 < atr_stop < entry:
                    stop_candidates.append(atr_stop)
            structural_stop = max(stop_candidates) if stop_candidates else entry * 0.94
            stop_price = max(structural_stop, entry * 0.92)
            if stop_price >= entry:
                stop_price = entry * 0.97
            risk_per_share = entry - stop_price
            if risk_per_share <= 0:
                continue
            free_roll_price = entry + risk_per_share * DEFAULT_FREE_ROLL_TRIGGER_R
            active_stop = stop_price
            moved_to_free_roll = False
            exit_price = 0.0
            exit_idx = entry_idx
            exit_reason = "時間到"
            final_pos = min(entry_pos + hold_days, len(df) - 1)

            for step in range(entry_pos, final_pos + 1):
                day_idx = df.index[step]
                day_open = safe_float(df.loc[day_idx, "Open"], 0.0)
                day_high = safe_float(df.loc[day_idx, "High"], 0.0)
                day_low = safe_float(df.loc[day_idx, "Low"], 0.0)
                day_close = safe_float(df.loc[day_idx, "Close"], 0.0)
                day_ma50 = safe_float(df.loc[day_idx, "ma50"], 0.0)

                if day_low <= active_stop:
                    exit_price = day_open if 0 < day_open < active_stop else active_stop
                    exit_idx = day_idx
                    exit_reason = "初始停損" if not moved_to_free_roll else "Free Roll後跌回成本"
                    break

                if (not moved_to_free_roll) and day_high >= free_roll_price:
                    moved_to_free_roll = True
                    active_stop = max(active_stop, entry)

                if moved_to_free_roll and day_ma50 > 0 and day_close < day_ma50:
                    exit_price = day_close
                    exit_idx = day_idx
                    exit_reason = "跌破50MA"
                    break

                exit_idx = day_idx
                exit_price = day_close

            if exit_price <= 0:
                exit_price = safe_float(df.loc[exit_idx, "Close"], 0.0)
            if exit_price <= 0:
                continue
            ret = (exit_price / entry - 1.0) * 100.0
            r_multiple = (exit_price - entry) / risk_per_share if risk_per_share > 0 else 0.0
            trades.append(
                {
                    "code": code,
                    "name": meta_dict[code]["name"],
                    "signal_date": str(pd.Timestamp(idx).date()),
                    "entry_date": str(pd.Timestamp(entry_idx).date()),
                    "exit_date": str(pd.Timestamp(exit_idx).date()),
                    "entry": round(entry, 2),
                    "exit": round(exit_price, 2),
                    "return_pct": round(ret, 2),
                    "board_streak": int(df.loc[idx, "board_streak"]),
                    "vol_ratio": round(safe_float(df.loc[idx, "vol_ratio"], 0.0), 2),
                    "exit_reason": exit_reason,
                    "r_multiple": round(r_multiple, 2),
                }
            )

    bt = pd.DataFrame(trades)
    if bt.empty:
        return bt, {
            "signals": 0,
            "wins": 0,
            "win_rate": 0.0,
            "avg_return": 0.0,
            "median_return": 0.0,
            "best": 0.0,
            "worst": 0.0,
        }

    wins = int((bt["return_pct"] > 0).sum())
    stats = {
        "signals": int(len(bt)),
        "wins": wins,
        "win_rate": round(wins / len(bt) * 100.0, 1),
        "avg_return": round(float(bt["return_pct"].mean()), 2),
        "median_return": round(float(bt["return_pct"].median()), 2),
        "best": round(float(bt["return_pct"].max()), 2),
        "worst": round(float(bt["return_pct"].min()), 2),
    }
    bt = bt.sort_values(["signal_date", "return_pct"], ascending=[False, False]).reset_index(drop=True)
    return bt, stats

def make_backtest_display(bt_df: pd.DataFrame):
    if bt_df is None or bt_df.empty:
        return pd.DataFrame()

    display_df = bt_df.rename(
        columns={
            "code": "股票代號",
            "name": "股票名稱",
            "signal_date": "出現機會日",
            "entry_date": "進場日",
            "exit_date": "賣出日",
            "entry": "買進價格",
            "exit": "賣出價格",
            "return_pct": "獲利報酬%",
            "board_streak": "過去大漲次數",
            "vol_ratio": "交易熱度倍數",
            "exit_reason": "出場原因",
            "r_multiple": "R倍數",
        }
    ).copy()

    keep_cols = [
        "股票代號", "股票名稱", "出現機會日", "進場日", "賣出日", "買進價格", "賣出價格",
        "獲利報酬%", "R倍數", "出場原因", "過去大漲次數", "交易熱度倍數"
    ]
    keep_cols = [c for c in keep_cols if c in display_df.columns]
    display_df = display_df[keep_cols]
    return display_df

def render_error_panel(errors):
    if not errors:
        return

    counts = {}
    order = []
    for msg in errors:
        if msg not in counts:
            counts[msg] = 0
            order.append(msg)
        counts[msg] += 1

    rows = []
    for msg in order:
        count = counts[msg]
        tag = "重複" if count > 1 else "單次"
        badge = f"<span class='log-count'>{count}x</span>" if count > 1 else ""
        rows.append(
            f"<div class='log-row'><span class='log-tag'>{tag}</span><span class='log-msg'>{html.escape(msg)}</span>{badge}</div>"
        )

    st.markdown("<div class='log-panel'>" + "".join(rows) + "</div>", unsafe_allow_html=True)


def render_backtest_table(display_df: pd.DataFrame):
    if display_df is None or display_df.empty:
        return

    headers = list(display_df.columns)
    header_html = "".join([f"<th>{html.escape(str(h))}</th>" for h in headers])

    body_rows = []
    for _, row in display_df.iterrows():
        ret = float(row["獲利報酬%"] ) if pd.notna(row["獲利報酬%"] ) else 0.0
        if ret >= 6:
            ret_class = "ret-strong"
        elif ret > 0:
            ret_class = "ret-pos"
        elif ret <= -6:
            ret_class = "ret-weak"
        elif ret < 0:
            ret_class = "ret-neg"
        else:
            ret_class = "ret-flat"

        cells = []
        for col in headers:
            val = row[col]
            cls = "num" if col in ["買進價格", "賣出價格", "過去大漲次數", "交易熱度倍數"] else ""
            if col == "獲利報酬%":
                val_html = f"<span class='ret-chip {ret_class}'>{ret:+.2f}%</span>"
                cls = "num"
            elif col in ["買進價格", "賣出價格"]:
                val_html = f"{float(val):.2f}"
            elif col == "交易熱度倍數":
                val_html = f"{float(val):.2f}x"
            else:
                val_html = html.escape(str(val))
            cells.append(f"<td class='{cls}'>{val_html}</td>")
        body_rows.append("<tr>" + "".join(cells) + "</tr>")

    table_html = f"""
    <div class='bt-wrap'>
      <div class='bt-table-scroll'>
        <table class='bt-table'>
          <thead><tr>{header_html}</tr></thead>
          <tbody>{''.join(body_rows)}</tbody>
        </table>
      </div>
    </div>
    """
    st.markdown(table_html, unsafe_allow_html=True)



def build_position_advice(item):
    signal = safe_float(item.get("今日表現分數", 0.0), 0.0)
    radar = safe_float(item.get("起漲雷達分數", 0.0), 0.0)
    heat = safe_float(item.get("交易熱度", 0.0), 0.0)
    dist = safe_float(item.get("距離最高價%", 99.0), 99.0)
    close_pos = safe_float(item.get("close_pos", 0.0), 0.0)
    pullback = safe_float(item.get("pullback", 0.0), 0.0)
    risk = safe_int(item.get("風險數", 0), 0)
    peer_rising = safe_int(item.get("同族群跟漲數", 0), 0)
    ret5 = safe_float(item.get("近5天表現%", 0.0), 0.0)
    ret20 = safe_float(item.get("近20天表現%", 0.0), 0.0)
    breakout = safe_float(item.get("突破區間分數", 0.0), 0.0)
    stage2_ok = str(item.get("Stage2模板", "未通過")) == "通過"
    ma50 = safe_float(item.get("50MA", 0.0), 0.0)
    current = safe_float(item.get("現價", 0.0), 0.0)
    entry = safe_float(item.get("建議進場價", 0.0), 0.0)
    free_roll = safe_float(item.get("Free Roll觸發價", 0.0), 0.0)
    stop_price = safe_float(item.get("建議停損價", 0.0), 0.0)

    buy = 0.0
    hold = 0.0
    sell = 0.0
    buy_reasons, hold_reasons, sell_reasons = [], [], []

    if stage2_ok:
        buy += 1.2
        hold += 1.0
        buy_reasons.append("Stage2 合格")
        hold_reasons.append("中期趨勢仍在")
    else:
        sell += 1.4
        buy -= 0.8
        sell_reasons.append("Stage2 未過")

    if breakout >= 1.0 or radar >= 4.0:
        buy += 1.5
        buy_reasons.append("整理後突破")
    elif radar >= 3.0:
        buy += 0.8
        buy_reasons.append("準發動型態")

    if heat >= 1.25:
        buy += 1.2
        hold += 0.8
        buy_reasons.append("量能持續放大")
        hold_reasons.append("熱度仍在")
    elif heat >= 1.0:
        buy += 0.6
        hold += 0.6
        buy_reasons.append("量能沒有掉")
    elif heat < 0.6:
        sell += 1.0
        hold -= 0.6
        sell_reasons.append("熱度明顯降溫")

    if dist <= 2.5:
        buy += 1.0
        hold += 0.8
        buy_reasons.append("仍貼近日高")
        hold_reasons.append("位置仍偏強")
    elif dist <= 5.0:
        hold += 0.6
    elif dist >= 8.0:
        sell += 1.0
        hold -= 0.7
        sell_reasons.append("已明顯離開高點")

    if close_pos >= 0.84:
        buy += 0.9
        hold += 0.8
        buy_reasons.append("收在高檔")
    elif close_pos >= 0.70:
        hold += 0.6
    elif close_pos < 0.55:
        sell += 1.2
        hold -= 0.8
        sell_reasons.append("收盤位置偏弱")

    if peer_rising >= 2:
        buy += 0.8
        hold += 0.8
        buy_reasons.append(f"同族群跟漲 {peer_rising} 檔")
        hold_reasons.append("族群仍有共振")
    elif peer_rising == 1:
        hold += 0.3
        hold_reasons.append("至少有同族群跟動")
    else:
        sell += 0.4
        sell_reasons.append("族群暫未同步")

    if signal >= 6.2:
        buy += 0.9
        hold += 1.1
        hold_reasons.append("整體結構仍強")
    elif signal >= 5.0:
        hold += 0.9
        hold_reasons.append("結構還沒壞")
    elif signal < 3.8:
        sell += 0.8
        sell_reasons.append("整體分數偏弱")

    if ret20 > 0 and ret5 > 0:
        hold += 0.8
        buy += 0.3
        hold_reasons.append("中短線趨勢仍正")
    elif ret5 < 0 and ret20 < 0:
        sell += 0.8
        sell_reasons.append("中短線同步轉弱")

    if risk <= 1:
        buy += 0.5
        hold += 0.7
    elif risk <= 3:
        hold += 0.4
    elif risk >= 5:
        sell += 1.4
        hold -= 1.0
        buy -= 1.0
        sell_reasons.append("風險訊號累積過多")
    elif risk >= 4:
        sell += 0.8
        sell_reasons.append("風險偏高")

    if pullback <= 0.008:
        buy += 0.5
        hold += 0.4
    elif pullback >= 0.02:
        sell += 1.0
        hold -= 0.7
        sell_reasons.append("從高點回落偏大")

    if ma50 > 0 and current < ma50:
        sell += 1.8
        hold -= 1.0
        sell_reasons.append("50MA 失守")

    if stop_price > 0 and current <= stop_price:
        sell += 2.2
        buy -= 1.2
        hold -= 1.0
        sell_reasons.append("跌破初始停損")

    if entry > 0 and current < entry:
        sell += 0.6
        buy -= 0.8
        sell_reasons.append("現價低於進場價，不可攤平")
    elif entry > 0 and free_roll > 0 and current >= free_roll:
        buy += 0.5
        hold += 1.0
        buy_reasons.append("已達 free roll，可用贏家加碼")
        hold_reasons.append("已可把停損推到成本")

    scores = {"持續買入": round(buy, 2), "續抱": round(hold, 2), "賣出": round(sell, 2)}
    if scores["賣出"] >= 4.2 and scores["賣出"] >= scores["續抱"] + 0.8 and scores["賣出"] >= scores["持續買入"] + 1.0:
        action = "賣出"
        reasons = sell_reasons
    elif scores["持續買入"] >= 4.6 and scores["持續買入"] >= scores["續抱"] + 0.6 and risk <= 3:
        action = "持續買入"
        reasons = buy_reasons
    else:
        action = "續抱"
        reasons = hold_reasons if hold_reasons else buy_reasons

    ordered = sorted(scores.values(), reverse=True)
    top = ordered[0] if ordered else 0.0
    second = ordered[1] if len(ordered) > 1 else 0.0
    gap = top - second
    if top >= 5.0 and gap >= 1.0:
        confidence = "高"
    elif top >= 4.0 and gap >= 0.45:
        confidence = "中"
    else:
        confidence = "低"

    reason_text = "｜".join(list(dict.fromkeys(reasons))[:4]) if reasons else "先看原本分數與位置"
    if action == "持續買入":
        summary = "偏向持續買入：符合 Stage2，且只准用贏家加碼。"
    elif action == "續抱":
        summary = "偏向續抱：結構尚未轉壞，守 50MA 與初始停損即可。"
    else:
        summary = "偏向賣出：50MA / 停損 / 趨勢資格已出現明顯風險。"

    return {
        "動作建議": action,
        "建議信心": confidence,
        "建議理由": reason_text,
        "建議摘要": summary,
        "加碼分": scores["持續買入"],
        "續抱分": scores["續抱"],
        "賣出分": scores["賣出"],
    }

def build_reason_tags(row):
    tags = []
    breakout = safe_float(row.get("突破區間分數", 0.0), 0.0)
    radar = safe_float(row.get("起漲雷達分數", 0.0), 0.0)
    vol_lift = safe_float(row.get("量能抬升比", 1.0), 1.0)
    cp = safe_float(row.get("close_pos", 0.0), 0.0)
    ret5 = safe_float(row.get("近5天表現%", 0.0), 0.0)
    ret20 = safe_float(row.get("近20天表現%", 0.0), 0.0)
    rising = safe_int(row.get("同族群跟漲數", 0), 0)

    if breakout >= 1.2:
        tags.append("突破整理上緣")
    elif radar >= 4.2:
        tags.append("整理後突破")
    elif radar >= 3.4:
        tags.append("準發動")

    if vol_lift >= 1.28:
        tags.append("量能明顯抬升")
    elif vol_lift >= 1.12:
        tags.append("量能轉強")

    if cp >= 0.84:
        tags.append("收在高檔")

    if ret20 > 0 and ret5 > 0:
        tags.append("趨勢翻正")

    if rising >= 3:
        tags.append(f"同族群跟漲{rising}檔")
    elif rising >= 2:
        tags.append(f"族群共振{rising}檔")
    elif rising >= 1:
        tags.append(f"有同族群跟動{rising}檔")
    else:
        tags.append("單兵觀察")

    if row.get("保底補位", ""):
        tags.append(str(row.get("保底補位")))
    return "｜".join(tags[:4]) if tags else "先看分數與位置"

def render_search_result_box(search_result):
    if not search_result:
        return
    if not search_result.get("ok"):
        kind = search_result.get("kind")
        if kind == "ambiguous":
            tags = ''.join([
                f"<span class='fail-tag'>{html.escape(m['code'])} {html.escape(m['name'])}｜{html.escape(m['market'])}</span>"
                for m in search_result.get("matches", [])
            ])
            st.markdown(
                f"<div class='search-panel'><div class='search-head'>獨立搜尋結果</div><div class='search-bad'>{html.escape(search_result.get('message', ''))}</div><div class='fail-bag'>{tags}</div></div>",
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f"<div class='search-panel'><div class='search-head'>獨立搜尋結果</div><div class='search-bad'>{html.escape(search_result.get('message', ''))}</div></div>",
                unsafe_allow_html=True,
            )
        return

    assess = search_result.get("assessment") or {}
    item = assess.get("item") or {}
    if not item:
        st.markdown(
            f"<div class='search-panel'><div class='search-head'>獨立搜尋結果</div><div class='search-bad'>{html.escape(assess.get('reason_text', '目前資料庫沒這支股票的完整數據。'))}</div></div>",
            unsafe_allow_html=True,
        )
        return

    passed = assess.get("passed", False)
    badge_cls = "search-good" if passed else "search-warn"
    badge_text = "順利通過當前條件" if passed else f"沒有通過｜{assess.get('reason_text', '表現未達標準')}"
    stage2_text = html.escape(str(item.get("Stage2模板", "未通過")))
    stage2_note = html.escape(str(item.get("Stage2說明", "")))
    risk_line = html.escape(str(item.get("風控摘要", "資料不足")))

    parts = [
        "<div class='search-panel'>",
        "<div class='search-head-row'>",
        "<div>",
        "<div class='search-head'>獨立搜尋與評分</div>",
        f"<div class='search-source'>資料來源：{html.escape(search_result.get('source', ''))}</div>",
        "</div>",
        f"<div class='{badge_cls}'>{html.escape(badge_text)}</div>",
        "</div>",
        "<div class='card search-card'>",
        f"<div class='card-stage'>{html.escape(str(item.get('階段', '')))}</div>",
        f"<div class='card-code'>{html.escape(str(item.get('代號', '')))}</div>",
        f"<div class='card-name'>{html.escape(str(item.get('名稱', '')))} ｜ {html.escape(str(item.get('市場', '')))} ｜ {html.escape(str(item.get('產業', '其他')))}</div>",
        f"<div class='card-price'>{safe_float(item.get('現價', 0.0), 0.0):.2f}</div>",
        f"<div class='card-status'>{html.escape(str(item.get('狀態', '')))}</div>",
        f"<div class='card-predict'>Stage2：{stage2_text}｜{stage2_note}</div>",
        f"<div class='card-predict-note'>{risk_line}</div>",
        f"<div class='card-predict'>{html.escape(str(item.get('預測主句', '白話預測：暫時沒有足夠資料')))}</div>",
        f"<div class='card-predict-note'>{html.escape(str(item.get('預測副句', '先以原本分數與熱度為主')))}</div>",
        f"<div class='card-predict' style='margin-top:10px;'>建議：{html.escape(str(item.get('動作建議', '續抱')))}｜信心：{html.escape(str(item.get('建議信心', '中')))}</div>",
        f"<div class='card-predict-note'>{html.escape(str(item.get('建議摘要', '先看原本分數與位置')))}</div>",
        f"<div class='soft-note'>建議理由：{html.escape(str(item.get('建議理由', '先看原本分數與位置')))}</div>",
        f"<div class='soft-note'>入選理由：{html.escape(str(item.get('入選理由', '先看分數與位置')))}</div>",
        "<div class='card-stars-wrap'>",
        f"<div class='card-stars'>{html.escape(str(item.get('推薦指數', '')))}</div>",
        f"<div class='card-stars-badge'>推薦 {int(safe_int(item.get('推薦星等', 1), 1))}/5</div>",
        "</div>",
        "<div class='card-grid'>",
        f"<div class='stat-pill'><div class='stat-k'>今日分數</div><div class='stat-v'>{safe_float(item.get('今日表現分數', 0.0), 0.0):.2f}</div></div>",
        f"<div class='stat-pill'><div class='stat-k'>交易熱度</div><div class='stat-v'>{safe_float(item.get('交易熱度', 0.0), 0.0):.2f}x</div></div>",
        f"<div class='stat-pill'><div class='stat-k'>距最高價</div><div class='stat-v'>{safe_float(item.get('距離最高價%', 0.0), 0.0):.2f}%</div></div>",
        f"<div class='stat-pill'><div class='stat-k'>Stage2</div><div class='stat-v'>{stage2_text}</div></div>",
        f"<div class='stat-pill'><div class='stat-k'>建議停損</div><div class='stat-v'>{safe_float(item.get('建議停損價', 0.0), 0.0):.2f}</div></div>",
        f"<div class='stat-pill'><div class='stat-k'>Free Roll</div><div class='stat-v'>{safe_float(item.get('Free Roll觸發價', 0.0), 0.0):.2f}</div></div>",
        f"<div class='stat-pill'><div class='stat-k'>50MA 防守</div><div class='stat-v'>{safe_float(item.get('50MA防守價', 0.0), 0.0):.2f}</div></div>",
        f"<div class='stat-pill'><div class='stat-k'>建議張數</div><div class='stat-v'>{safe_float(item.get('建議張數', 0.0), 0.0):.1f}</div></div>",
        f"<div class='stat-pill'><div class='stat-k'>族群熱度</div><div class='stat-v'>{html.escape(str(item.get('族群狀態', '單兵觀察')))}</div></div>",
        f"<div class='stat-pill'><div class='stat-k'>加碼守則</div><div class='stat-v'>{html.escape(str(item.get('加碼守則', '只准加碼贏家')))}</div></div>",
        f"<div class='stat-pill'><div class='stat-k'>近 5 天</div><div class='stat-v'>{safe_float(item.get('近5天表現%', 0.0), 0.0):+.2f}%</div></div>",
        f"<div class='stat-pill'><div class='stat-k'>近 20 天</div><div class='stat-v'>{safe_float(item.get('近20天表現%', 0.0), 0.0):+.2f}%</div></div>",
        "</div>",
        "</div>",
        "</div>",
    ]
    st.markdown(''.join(parts), unsafe_allow_html=True)

def render_stock_cards(section_df: pd.DataFrame, empty_text: str):
    if section_df is None or section_df.empty:
        st.info(empty_text)
        return

    cols = st.columns(4)
    for i, (_, row) in enumerate(section_df.iterrows()):
        stage2_text = html.escape(str(row.get('Stage2模板', '未通過')))
        stage2_note = html.escape(str(row.get('Stage2說明', '')))
        risk_line = html.escape(str(row.get('風控摘要', '資料不足')))
        with cols[i % 4]:
            st.markdown(
                f"""
<div class="card">
  <div class="card-stage">{row.get('階段', '')}</div>
  <div class="card-code">{row.get('代號', '')}</div>
  <div class="card-name">{row.get('名稱', '')} ｜ {row.get('市場', '')} ｜ {row.get('產業', '其他')}</div>
  <div class="card-price">{safe_float(row.get('現價', 0.0), 0.0):.2f}</div>
  <div class="card-status">{row.get('狀態', '')}</div>

  <div class="card-predict">Stage2：{stage2_text}｜{stage2_note}</div>
  <div class="card-predict-note">{risk_line}</div>
  <div class="card-predict">{html.escape(str(row.get('預測主句', '白話預測：暫時沒有足夠資料')))}</div>
  <div class="card-predict-note">{html.escape(str(row.get('預測副句', '先以原本分數與熱度為主')))}</div>
  <div class="soft-note">入選理由：{html.escape(str(row.get('入選理由', '先看分數與位置')))}</div>

  <div class="card-stars-wrap">
    <div class="card-stars">{row.get('推薦指數', '')}</div>
    <div class="card-stars-badge">推薦 {int(safe_int(row.get('推薦星等', 1), 1))}/5</div>
  </div>
  <div class="card-grid">
    <div class="stat-pill"><div class="stat-k">今日分數</div><div class="stat-v">{safe_float(row.get('今日表現分數', 0.0), 0.0):.2f}</div></div>
    <div class="stat-pill"><div class="stat-k">交易熱度</div><div class="stat-v">{safe_float(row.get('交易熱度', 0.0), 0.0):.2f}x</div></div>
    <div class="stat-pill"><div class="stat-k">距最高價</div><div class="stat-v">{safe_float(row.get('距離最高價%', 0.0), 0.0):.2f}%</div></div>
    <div class="stat-pill"><div class="stat-k">Stage2</div><div class="stat-v">{stage2_text}</div></div>
    <div class="stat-pill"><div class="stat-k">建議停損</div><div class="stat-v">{safe_float(row.get('建議停損價', 0.0), 0.0):.2f}</div></div>
    <div class="stat-pill"><div class="stat-k">Free Roll</div><div class="stat-v">{safe_float(row.get('Free Roll觸發價', 0.0), 0.0):.2f}</div></div>
    <div class="stat-pill"><div class="stat-k">50MA 防守</div><div class="stat-v">{safe_float(row.get('50MA防守價', 0.0), 0.0):.2f}</div></div>
    <div class="stat-pill"><div class="stat-k">建議張數</div><div class="stat-v">{safe_float(row.get('建議張數', 0.0), 0.0):.1f}</div></div>
    <div class="stat-pill"><div class="stat-k">加碼守則</div><div class="stat-v">{html.escape(str(row.get('加碼守則', '只准加碼贏家')))}</div></div>
    <div class="stat-pill"><div class="stat-k">近 5 天</div><div class="stat-v">{safe_float(row.get('近5天表現%', 0.0), 0.0):+.2f}%</div></div>
    <div class="stat-pill"><div class="stat-k">近 20 天</div><div class="stat-v">{safe_float(row.get('近20天表現%', 0.0), 0.0):+.2f}%</div></div>
    <div class="stat-pill"><div class="stat-k">族群熱度</div><div class="stat-v">{row.get('族群狀態', '單兵觀察')}</div></div>
  </div>
</div>
""",
                unsafe_allow_html=True,
            )
