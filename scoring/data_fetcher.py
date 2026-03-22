"""
Phase 1 — Data Collection Layer
Fetches market and fundamental data from Yahoo Finance (no API key required).
Uses a two-source strategy: v7/quote (no crumb) + v10/quoteSummary (with crumb).
Falls back gracefully when the crumb flow is unavailable.
"""

import time
import threading
import numpy as np
import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
YAHOO_Q1 = "https://query1.finance.yahoo.com"
YAHOO_Q2 = "https://query2.finance.yahoo.com"

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_BASE_HEADERS = {
    "User-Agent": _UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

# ---------------------------------------------------------------------------
# Session + crumb (cached per process)
# ---------------------------------------------------------------------------
_sess_lock = threading.Lock()
_session: requests.Session | None = None
_crumb: str = ""
_crumb_ts: float = 0.0
_CRUMB_TTL = 1800  # re-obtain crumb after 30 min


def _build_session_and_crumb() -> tuple[requests.Session, str]:
    """
    Build a fresh session with cookies + crumb.
    Mirrors yfinance's two-strategy approach:
      1. fc.yahoo.com  — lightweight A3 cookie, no consent page
      2. finance.yahoo.com — fallback, may serve GDPR consent redirect
    Never holds _sess_lock.
    """
    s = requests.Session()
    s.headers.update(_BASE_HEADERS)

    # Strategy 1: fc.yahoo.com (preferred — no consent pages)
    cookie_ok = False
    try:
        r = s.get("https://fc.yahoo.com", timeout=(5, 10), allow_redirects=True)
        if r.status_code == 200:
            cookie_ok = True
    except Exception:
        pass

    # Strategy 2: finance.yahoo.com fallback
    if not cookie_ok:
        for seed_url in ["https://finance.yahoo.com", "https://www.yahoo.com"]:
            try:
                s.get(seed_url, timeout=(5, 10), allow_redirects=True)
                cookie_ok = True
                break
            except Exception:
                continue

    # Try both query bases for the crumb
    crumb = ""
    for base in [YAHOO_Q2, YAHOO_Q1]:
        try:
            r = s.get(f"{base}/v1/test/getcrumb", timeout=(5, 10))
            text = r.text.strip()
            if r.status_code == 200 and text not in ("", "null"):
                crumb = text
                break
        except Exception:
            continue

    return s, crumb


def _get_session_and_crumb() -> tuple[requests.Session, str]:
    """Return (session, crumb), rebuilding only if expired. Lock is held briefly."""
    global _session, _crumb, _crumb_ts
    with _sess_lock:
        now = time.time()
        if _session is not None and _crumb and (now - _crumb_ts) < _CRUMB_TTL:
            return _session, _crumb
        # Snapshot current values in case we need to fall back
        existing = (_session, _crumb)

    # Build outside the lock so we don't block other threads
    s, crumb = _build_session_and_crumb()

    with _sess_lock:
        # Only store if newer than what another thread may have set meanwhile
        if (time.time() - _crumb_ts) >= _CRUMB_TTL or not _crumb:
            _session = s
            _crumb = crumb
            _crumb_ts = time.time()
        return _session, _crumb


def _reset_session():
    global _session, _crumb, _crumb_ts
    with _sess_lock:
        _session = None
        _crumb = ""
        _crumb_ts = 0.0


def prewarm():
    """Pre-initialise the session in a background thread at startup."""
    def _do():
        try:
            _get_session_and_crumb()
        except Exception:
            pass
    threading.Thread(target=_do, daemon=True).start()


# ---------------------------------------------------------------------------
# In-process result cache (5 min TTL)
# ---------------------------------------------------------------------------
_cache: dict = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 300

# Global throttle: space Yahoo Finance calls by at least 2 s
_yf_lock = threading.Lock()
_last_call: float = 0.0
_MIN_GAP = 2.0


def _throttle():
    global _last_call
    with _yf_lock:
        gap = _MIN_GAP - (time.time() - _last_call)
        if gap > 0:
            time.sleep(gap)
        _last_call = time.time()


# ---------------------------------------------------------------------------
# Low-level fetchers
# ---------------------------------------------------------------------------

SUMMARY_MODULES = [
    "summaryDetail", "defaultKeyStatistics", "financialData",
    "incomeStatementHistory", "balanceSheetHistory",
    "cashflowStatementHistory", "price", "assetProfile",
]


def _fetch_quote_v7(ticker: str, session: requests.Session) -> tuple[dict, str]:
    """v7/finance/quote — returns (data_dict, error_str)."""
    fields = (
        "longName,shortName,regularMarketPrice,regularMarketChangePercent,"
        "marketCap,trailingPE,forwardPE,priceToBook,trailingEps,"
        "fiftyTwoWeekHigh,fiftyTwoWeekLow,fiftyDayAverage,twoHundredDayAverage,"
        "dividendYield,beta,volume,averageVolume,sharesOutstanding,floatShares,"
        "shortRatio,bookValue,pegRatio,currency,sector,industry"
    )
    last_err = "no attempt"
    for base in [YAHOO_Q1, YAHOO_Q2]:
        try:
            r = session.get(
                f"{base}/v7/finance/quote",
                params={"symbols": ticker, "fields": fields},
                timeout=15,
            )
            if r.status_code == 200:
                body = r.json()
                result = (body.get("quoteResponse") or {}).get("result") or []
                if result:
                    return result[0], None
                last_err = f"HTTP 200 but empty result"
            else:
                last_err = f"HTTP {r.status_code}"
        except Exception as e:
            last_err = str(e)
    return {}, last_err


def _fetch_quote_summary(ticker: str, session: requests.Session, crumb: str) -> tuple[dict, str | None]:
    """v10/finance/quoteSummary — full fundamental data, requires crumb."""
    url = f"{YAHOO_Q1}/v10/finance/quoteSummary/{ticker}"
    params = {"modules": ",".join(SUMMARY_MODULES)}
    if crumb:
        params["crumb"] = crumb
    for attempt in range(2):
        try:
            r = session.get(url, params=params, timeout=20)
            if r.status_code == 401 and attempt == 0:
                _reset_session()
                session, crumb = _get_session_and_crumb()
                if crumb:
                    params["crumb"] = crumb
                continue
            if r.status_code != 200:
                return {}, f"HTTP {r.status_code}"
            body = r.json()
            qs = body.get("quoteSummary", {})
            if qs.get("error"):
                return {}, str(qs["error"])
            results = qs.get("result") or []
            return (results[0] if results else {}), None
        except Exception as e:
            return {}, str(e)
    return {}, "Auth failed after retry"


def _fetch_chart(ticker: str, session: requests.Session) -> list[tuple[int, float]]:
    """v8/finance/chart — price history, rarely rate-limited."""
    for base in [YAHOO_Q1, YAHOO_Q2]:
        try:
            r = session.get(
                f"{base}/v8/finance/chart/{ticker}",
                params={"interval": "1d", "range": "1y", "includeAdjustedClose": "true"},
                timeout=15,
            )
            if r.status_code != 200:
                continue
            chart = r.json().get("chart", {})
            res = (chart.get("result") or [{}])[0]
            ts = res.get("timestamp", [])
            ac = (res.get("indicators", {}).get("adjclose") or [{}])[0].get("adjclose", [])
            pairs = [(t, c) for t, c in zip(ts, ac) if c is not None]
            if pairs:
                return pairs
        except Exception:
            continue
    return []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _raw(obj, *keys, default=None):
    cur = obj
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k, default)
        if cur is None:
            return default
    if isinstance(cur, dict) and "raw" in cur:
        return cur["raw"]
    return cur


def _safe(val, default=None):
    if val is None:
        return default
    try:
        f = float(val)
        return default if f != f else f   # NaN check
    except (TypeError, ValueError):
        return default


def _pct(val):
    v = _safe(val)
    if v is None:
        return None
    return v if abs(v) <= 1.5 else v / 100.0


# ---------------------------------------------------------------------------
# Main extraction entry-point
# ---------------------------------------------------------------------------

def extract_raw_metrics(ticker: str):
    """
    Returns (metrics_dict, quality_dict, error_list).
    """
    # Cache hit?
    with _cache_lock:
        entry = _cache.get(ticker)
        if entry and (time.time() - entry[0]) < _CACHE_TTL:
            return entry[1], entry[2], entry[3]

    _throttle()

    session, crumb = _get_session_and_crumb()
    metrics: dict = {}
    errors: list = []

    # Sequential fetches — requests.Session is NOT thread-safe
    v7, v7_err = _fetch_quote_v7(ticker, session)
    qs, qs_err = _fetch_quote_summary(ticker, session, crumb)
    price_hist = _fetch_chart(ticker, session)

    if v7_err:
        errors.append(f"v7/quote: {v7_err}")
    if qs_err:
        errors.append(f"quoteSummary: {qs_err}")

    # Require at least a price from either source to proceed
    price = _safe(v7.get("regularMarketPrice"))
    if price is None:
        price = _safe(_raw(qs.get("price", {}), "regularMarketPrice"))
    if price is None and not qs:
        detail = f"v7_err={v7_err}, qs_err={qs_err}"
        return None, None, [f"No price data for '{ticker}'. {detail}"]

    # Shorthand sections from quoteSummary
    sd  = qs.get("summaryDetail",              {}) or {}
    ks  = qs.get("defaultKeyStatistics",       {}) or {}
    fd  = qs.get("financialData",              {}) or {}
    pr  = qs.get("price",                      {}) or {}
    ap  = qs.get("assetProfile",               {}) or {}
    inc = (qs.get("incomeStatementHistory",    {}) or {}).get("incomeStatementHistory", [])
    bs  = (qs.get("balanceSheetHistory",       {}) or {}).get("balanceSheetStatements", [])
    cf  = (qs.get("cashflowStatementHistory",  {}) or {}).get("cashflowStatements",     [])

    # ── Identity ─────────────────────────────────────────────────────────────
    metrics["ticker"]   = ticker.upper()
    metrics["name"]     = (v7.get("longName") or v7.get("shortName")
                           or _raw(pr, "longName") or ticker.upper())
    metrics["sector"]   = v7.get("sector")   or _raw(ap, "sector")   or "Unknown"
    metrics["industry"] = v7.get("industry") or _raw(ap, "industry") or "Unknown"
    metrics["currency"] = v7.get("currency") or _raw(pr, "currency") or "USD"

    # ── Price & market ───────────────────────────────────────────────────────
    metrics["current_price"] = price or _safe(_raw(pr, "regularMarketPrice"))
    metrics["market_cap"]    = _safe(v7.get("marketCap")) or _safe(_raw(pr, "marketCap"))
    cp_chg = v7.get("regularMarketChangePercent")
    if cp_chg is not None:
        metrics["change_pct"] = _safe(cp_chg)

    # ── Valuation ────────────────────────────────────────────────────────────
    metrics["pe_ratio"]      = _safe(v7.get("trailingPE")) or _safe(_raw(sd, "trailingPE"))
    metrics["forward_pe"]    = _safe(v7.get("forwardPE"))  or _safe(_raw(ks, "forwardPE"))
    metrics["price_to_book"] = _safe(v7.get("priceToBook"))or _safe(_raw(ks, "priceToBook"))
    metrics["ev_to_ebitda"]  = _safe(_raw(ks, "enterpriseToEbitda"))
    metrics["ev_to_revenue"] = _safe(_raw(ks, "enterpriseToRevenue"))
    metrics["dividend_yield"]= _pct(v7.get("dividendYield") or _raw(sd, "dividendYield"))
    metrics["peg_ratio"]     = _safe(v7.get("pegRatio"))   or _safe(_raw(ks, "pegRatio"))

    # ── Growth ───────────────────────────────────────────────────────────────
    metrics["revenue_growth"]  = _pct(_raw(fd, "revenueGrowth"))
    metrics["earnings_growth"] = _pct(_raw(fd, "earningsGrowth"))

    if len(inc) >= 2:
        r0 = _raw(inc[0], "totalRevenue");  r1 = _raw(inc[1], "totalRevenue")
        n0 = _raw(inc[0], "netIncome");     n1 = _raw(inc[1], "netIncome")
        if r0 and r1 and r1 != 0:
            metrics["revenue_growth_yoy"]    = (r0 - r1) / abs(r1)
        if n0 and n1 and n1 != 0:
            metrics["net_income_growth_yoy"] = (n0 - n1) / abs(n1)
        metrics["total_revenue"] = r0
        metrics["net_income"]    = n0
        metrics["gross_profit"]  = _raw(inc[0], "grossProfit")
        metrics["ebit"]          = _raw(inc[0], "ebit")
        metrics["interest_expense"] = _raw(inc[0], "interestExpense")

    # ── Profitability ────────────────────────────────────────────────────────
    metrics["roe"]              = _pct(_raw(fd, "returnOnEquity"))
    metrics["roa"]              = _pct(_raw(fd, "returnOnAssets"))
    metrics["gross_margins"]    = _pct(_raw(fd, "grossMargins"))
    metrics["operating_margins"]= _pct(_raw(fd, "operatingMargins"))
    metrics["profit_margins"]   = _pct(_raw(fd, "profitMargins"))
    metrics["ebitda_margins"]   = _pct(_raw(fd, "ebitdaMargins"))

    # ── Balance sheet ────────────────────────────────────────────────────────
    if bs:
        b = bs[0]
        metrics["total_assets"]             = _raw(b, "totalAssets")
        metrics["total_liabilities"]        = _raw(b, "totalLiab")
        metrics["stockholder_equity"]       = _raw(b, "totalStockholderEquity")
        metrics["cash"]                     = _raw(b, "cash")
        metrics["total_current_assets"]     = _raw(b, "totalCurrentAssets")
        metrics["total_current_liabilities"]= _raw(b, "totalCurrentLiabilities")
        metrics["long_term_debt"]           = _raw(b, "longTermDebt")
        metrics["short_term_debt"]          = _raw(b, "shortLongTermDebt")
        eq  = metrics.get("stockholder_equity")
        ltd = metrics.get("long_term_debt") or 0
        std = metrics.get("short_term_debt") or 0
        if eq and eq != 0:
            metrics["debt_to_equity"] = (ltd + std) / abs(eq)

    # ── Cash flow ────────────────────────────────────────────────────────────
    if cf:
        c = cf[0]
        ocf  = _raw(c, "totalCashFromOperatingActivities")
        capx = _raw(c, "capitalExpenditures")
        metrics["operating_cash_flow"] = ocf
        metrics["capex"]               = capx
        if ocf is not None and capx is not None:
            metrics["free_cash_flow"] = ocf - abs(capx)

    # ── Liquidity ────────────────────────────────────────────────────────────
    metrics["current_ratio"]     = _safe(_raw(fd, "currentRatio"))
    metrics["quick_ratio"]       = _safe(_raw(fd, "quickRatio"))
    metrics["debt_to_equity_fd"] = _safe(_raw(fd, "debtToEquity"))

    # ── Market / risk ────────────────────────────────────────────────────────
    metrics["beta"]               = _safe(v7.get("beta"))    or _safe(_raw(sd, "beta"))
    metrics["volume"]             = _safe(v7.get("volume"))  or _safe(_raw(sd, "volume"))
    metrics["avg_volume"]         = _safe(v7.get("averageVolume")) or _safe(_raw(sd, "averageVolume"))
    metrics["shares_outstanding"] = _safe(v7.get("sharesOutstanding")) or _safe(_raw(ks, "sharesOutstanding"))
    metrics["float_shares"]       = _safe(v7.get("floatShares"))
    metrics["short_ratio"]        = _safe(v7.get("shortRatio"))  or _safe(_raw(ks, "shortRatio"))
    metrics["52w_high"]           = _safe(v7.get("fiftyTwoWeekHigh"))  or _safe(_raw(sd, "fiftyTwoWeekHigh"))
    metrics["52w_low"]            = _safe(v7.get("fiftyTwoWeekLow"))   or _safe(_raw(sd, "fiftyTwoWeekLow"))
    metrics["50d_avg"]            = _safe(v7.get("fiftyDayAverage"))   or _safe(_raw(sd, "fiftyDayAverage"))
    metrics["200d_avg"]           = _safe(v7.get("twoHundredDayAverage")) or _safe(_raw(sd, "twoHundredDayAverage"))
    metrics["52w_change"]         = _pct(_raw(ks, "fiftyTwoWeekChange"))

    # ── Derived ratios ───────────────────────────────────────────────────────
    ebit = metrics.get("ebit")
    iexp = metrics.get("interest_expense")
    if ebit and iexp:
        iabs = abs(iexp)
        if iabs > 0:
            metrics["interest_coverage"] = ebit / iabs

    cp   = metrics.get("current_price")
    ma50 = metrics.get("50d_avg")
    ma200= metrics.get("200d_avg")
    hi52 = metrics.get("52w_high")
    lo52 = metrics.get("52w_low")

    if cp and ma50  and ma50  > 0: metrics["price_vs_50d"]  = (cp - ma50)  / ma50
    if cp and ma200 and ma200 > 0: metrics["price_vs_200d"] = (cp - ma200) / ma200
    if cp and hi52 and lo52:
        rng = hi52 - lo52
        if rng > 0:
            metrics["52w_position"] = (cp - lo52) / rng

    # ── Historical volatility & momentum ─────────────────────────────────────
    if len(price_hist) > 20:
        closes = [p[1] for p in price_hist]
        rets = [np.log(closes[i] / closes[i-1])
                for i in range(1, len(closes))
                if closes[i-1] and closes[i-1] > 0]
        if rets:
            metrics["historical_volatility"] = float(np.std(rets) * np.sqrt(252))
        if len(closes) >= 63  and closes[-63]  > 0: metrics["momentum_3m"]  = (closes[-1] - closes[-63])  / closes[-63]
        if len(closes) >= 126 and closes[-126] > 0: metrics["momentum_6m"]  = (closes[-1] - closes[-126]) / closes[-126]
        if len(closes) >= 252 and closes[-252] > 0: metrics["momentum_12m"] = (closes[-1] - closes[-252]) / closes[-252]

    # ── Data quality ─────────────────────────────────────────────────────────
    core = ["current_price", "pe_ratio", "market_cap", "roe", "profit_margins",
            "current_ratio", "beta", "52w_change"]
    present = sum(1 for f in core if metrics.get(f) is not None)
    quality = {
        "completeness":      round(present / len(core), 2),
        "has_price":         metrics.get("current_price") is not None,
        "has_fundamental":   any(metrics.get(f) is not None for f in ["roe", "pe_ratio"]),
        "has_balance_sheet": metrics.get("total_assets") is not None,
        "field_count":       sum(1 for v in metrics.values() if v is not None),
    }

    with _cache_lock:
        _cache[ticker] = (time.time(), metrics, quality, errors)

    return metrics, quality, errors
