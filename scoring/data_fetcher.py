"""
Phase 1 — Data Collection Layer
Fetches all required market and fundamental data from Yahoo Finance (no API key required).
Uses cookie + crumb authentication to access v10 quoteSummary endpoints.
"""

import requests
import numpy as np
import threading
import concurrent.futures

YAHOO_BASE  = "https://query1.finance.yahoo.com"
YAHOO_BASE2 = "https://query2.finance.yahoo.com"

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# ---------------------------------------------------------------------------
# Session + crumb management (lazy initialisation, cached per process)
# ---------------------------------------------------------------------------
_session_lock = threading.Lock()
_shared_session = None
_crumb = None


def _get_session_and_crumb():
    """
    Returns (requests.Session, crumb_str).
    Initialises once per process by:
      1. GET https://fc.yahoo.com  (sets A3 cookie)
      2. GET /v1/test/getcrumb     (returns crumb string)
    """
    global _shared_session, _crumb
    with _session_lock:
        if _shared_session is not None and _crumb:
            return _shared_session, _crumb

        s = requests.Session()
        s.headers.update({
            "User-Agent": _UA,
            "Accept":     "*/*",
            "Accept-Language": "en-US,en;q=0.9",
        })
        try:
            s.get("https://fc.yahoo.com", timeout=10)
        except Exception:
            pass  # cookie may already be set; continue

        cr_r = s.get(
            f"{YAHOO_BASE2}/v1/test/getcrumb",
            timeout=10,
        )
        if cr_r.status_code == 200 and cr_r.text.strip():
            _crumb = cr_r.text.strip()
        else:
            _crumb = ""   # fallback: proceed without crumb

        _shared_session = s
        return s, _crumb


def prewarm():
    """Call once at server startup so the first real request doesn't pay init cost."""
    threading.Thread(target=_get_session_and_crumb, daemon=True).start()

SUMMARY_MODULES = [
    "summaryDetail",
    "defaultKeyStatistics",
    "financialData",
    "incomeStatementHistory",
    "balanceSheetHistory",
    "cashflowStatementHistory",
    "price",
    "assetProfile",
]


# ---------------------------------------------------------------------------
# Low-level fetchers
# ---------------------------------------------------------------------------

def fetch_quote_summary(ticker: str):
    """Return (data_dict, error_str) from Yahoo Finance quoteSummary."""
    session, crumb = _get_session_and_crumb()
    url    = f"{YAHOO_BASE}/v10/finance/quoteSummary/{ticker}"
    params = {"modules": ",".join(SUMMARY_MODULES)}
    if crumb:
        params["crumb"] = crumb
    try:
        r = session.get(url, params=params, timeout=15)
        r.raise_for_status()
        body = r.json()
        qs = body.get("quoteSummary", {})
        if qs.get("error"):
            return None, f"Yahoo error: {qs['error']}"
        result = qs.get("result") or []
        if not result:
            return None, "No result returned by Yahoo Finance"
        return result[0], None
    except requests.HTTPError as e:
        return None, f"HTTP {e.response.status_code} for {ticker}"
    except Exception as e:
        return None, str(e)


def fetch_price_history(ticker: str, period: str = "1y"):
    """Return (list_of_(timestamp,close), error_str)."""
    session, crumb = _get_session_and_crumb()
    url    = f"{YAHOO_BASE}/v8/finance/chart/{ticker}"
    params = {"interval": "1d", "range": period, "includeAdjustedClose": "true"}
    if crumb:
        params["crumb"] = crumb
    try:
        r = session.get(url, params=params, timeout=15)
        r.raise_for_status()
        body = r.json()
        chart = body.get("chart", {})
        if chart.get("error"):
            return None, str(chart["error"])
        results = chart.get("result") or []
        if not results:
            return None, "Empty chart result"
        res = results[0]
        timestamps = res.get("timestamp", [])
        adj_close_list = res.get("indicators", {}).get("adjclose", [])
        closes = adj_close_list[0].get("adjclose", []) if adj_close_list else []
        pairs = [(t, c) for t, c in zip(timestamps, closes) if c is not None]
        if not pairs:
            return None, "No valid close prices"
        return pairs, None
    except Exception as e:
        return None, str(e)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _raw(obj, *keys, default=None):
    """Safely navigate nested dicts; unwrap Yahoo's {'raw': X, 'fmt': Y} wrappers."""
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


def _pct(val):
    """Yahoo sometimes stores 0.15 for 15% and sometimes 15. Normalise to decimal."""
    if val is None:
        return None
    return val if abs(val) <= 1.5 else val / 100.0


# ---------------------------------------------------------------------------
# Main extraction entry-point
# ---------------------------------------------------------------------------

def extract_raw_metrics(ticker: str):
    """
    Returns (metrics_dict, quality_dict, error_list).
    metrics_dict  — all numerical/string values needed for scoring
    quality_dict  — data-quality metadata
    error_list    — non-fatal warnings
    """
    metrics = {}
    errors  = []

    # ── Fetch fundamental data + price history in parallel ───────────────────
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        f_fund  = pool.submit(fetch_quote_summary, ticker)
        f_price = pool.submit(fetch_price_history, ticker)
        fund, err   = f_fund.result()
        price_hist, perr = f_price.result()

    if err:
        return None, None, [f"Quote summary failed: {err}"]
    if perr:
        errors.append(f"Price history: {perr}")
        price_hist = []

    # Shorthand sections
    sd  = fund.get("summaryDetail",              {}) or {}
    ks  = fund.get("defaultKeyStatistics",       {}) or {}
    fd  = fund.get("financialData",              {}) or {}
    pr  = fund.get("price",                      {}) or {}
    ap  = fund.get("assetProfile",               {}) or {}
    inc = (fund.get("incomeStatementHistory",    {}) or {}).get("incomeStatementHistory", [])
    bs  = (fund.get("balanceSheetHistory",       {}) or {}).get("balanceSheetStatements", [])
    cf  = (fund.get("cashflowStatementHistory",  {}) or {}).get("cashflowStatements",     [])

    # ── Identity ─────────────────────────────────────────────────────────────
    metrics["ticker"]   = ticker.upper()
    metrics["name"]     = _raw(pr, "longName") or _raw(pr, "shortName") or ticker.upper()
    metrics["sector"]   = _raw(ap, "sector")   or "Unknown"
    metrics["industry"] = _raw(ap, "industry") or "Unknown"
    metrics["currency"] = _raw(pr, "currency") or "USD"

    # ── Price & market ───────────────────────────────────────────────────────
    metrics["current_price"] = _raw(pr, "regularMarketPrice")
    metrics["market_cap"]    = _raw(pr, "marketCap")
    metrics["change_pct"]    = _raw(pr, "regularMarketChangePercent")

    # ── Valuation ────────────────────────────────────────────────────────────
    metrics["pe_ratio"]      = _raw(sd, "trailingPE")
    metrics["forward_pe"]    = _raw(ks, "forwardPE")
    metrics["price_to_book"] = _raw(ks, "priceToBook")
    metrics["ev_to_ebitda"]  = _raw(ks, "enterpriseToEbitda")
    metrics["ev_to_revenue"] = _raw(ks, "enterpriseToRevenue")
    metrics["dividend_yield"]= _pct(_raw(sd, "dividendYield"))
    metrics["peg_ratio"]     = _raw(ks, "pegRatio")

    # ── Growth (from financialData) ──────────────────────────────────────────
    metrics["revenue_growth"]  = _pct(_raw(fd, "revenueGrowth"))
    metrics["earnings_growth"] = _pct(_raw(fd, "earningsGrowth"))

    # ── Growth from income-statement history ─────────────────────────────────
    if len(inc) >= 2:
        r0 = _raw(inc[0], "totalRevenue");  r1 = _raw(inc[1], "totalRevenue")
        n0 = _raw(inc[0], "netIncome");     n1 = _raw(inc[1], "netIncome")
        if r0 and r1 and r1 != 0:
            metrics["revenue_growth_yoy"]    = (r0 - r1) / abs(r1)
        if n0 and n1 and n1 != 0:
            metrics["net_income_growth_yoy"] = (n0 - n1) / abs(n1)
        metrics["total_revenue"]    = r0
        metrics["net_income"]       = n0
        metrics["gross_profit"]     = _raw(inc[0], "grossProfit")
        metrics["ebit"]             = _raw(inc[0], "ebit")
        metrics["interest_expense"] = _raw(inc[0], "interestExpense")

    # ── Profitability ────────────────────────────────────────────────────────
    metrics["roe"]             = _pct(_raw(fd, "returnOnEquity"))
    metrics["roa"]             = _pct(_raw(fd, "returnOnAssets"))
    metrics["gross_margins"]   = _pct(_raw(fd, "grossMargins"))
    metrics["operating_margins"]= _pct(_raw(fd, "operatingMargins"))
    metrics["profit_margins"]  = _pct(_raw(fd, "profitMargins"))
    metrics["ebitda_margins"]  = _pct(_raw(fd, "ebitdaMargins"))

    # ── Balance sheet ────────────────────────────────────────────────────────
    if bs:
        b = bs[0]
        metrics["total_assets"]            = _raw(b, "totalAssets")
        metrics["total_liabilities"]       = _raw(b, "totalLiab")
        metrics["stockholder_equity"]      = _raw(b, "totalStockholderEquity")
        metrics["cash"]                    = _raw(b, "cash")
        metrics["total_current_assets"]    = _raw(b, "totalCurrentAssets")
        metrics["total_current_liabilities"]= _raw(b, "totalCurrentLiabilities")
        metrics["long_term_debt"]          = _raw(b, "longTermDebt")
        metrics["short_term_debt"]         = _raw(b, "shortLongTermDebt")
        eq = metrics.get("stockholder_equity")
        ltd= metrics.get("long_term_debt") or 0
        std= metrics.get("short_term_debt") or 0
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

    # ── Liquidity (from financialData) ──────────────────────────────────────
    metrics["current_ratio"]      = _raw(fd, "currentRatio")
    metrics["quick_ratio"]        = _raw(fd, "quickRatio")
    metrics["debt_to_equity_fd"]  = _raw(fd, "debtToEquity")   # can be × 100

    # ── Market risk metrics ──────────────────────────────────────────────────
    metrics["beta"]              = _raw(sd, "beta")
    metrics["volume"]            = _raw(sd, "volume")
    metrics["avg_volume"]        = _raw(sd, "averageVolume")
    metrics["shares_outstanding"]= _raw(ks, "sharesOutstanding")
    metrics["float_shares"]      = _raw(ks, "floatShares")
    metrics["short_ratio"]       = _raw(ks, "shortRatio")
    metrics["52w_high"]          = _raw(sd, "fiftyTwoWeekHigh")
    metrics["52w_low"]           = _raw(sd, "fiftyTwoWeekLow")
    metrics["50d_avg"]           = _raw(sd, "fiftyDayAverage")
    metrics["200d_avg"]          = _raw(sd, "twoHundredDayAverage")
    metrics["52w_change"]        = _pct(_raw(ks, "fiftyTwoWeekChange"))

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

    # ── Historical volatility & momentum from price history ─────────────────
    if len(price_hist) > 20:
        closes = [p[1] for p in price_hist]
        rets   = []
        for i in range(1, len(closes)):
            if closes[i - 1] and closes[i - 1] > 0:
                rets.append(np.log(closes[i] / closes[i - 1]))
        if rets:
            metrics["historical_volatility"] = float(np.std(rets) * np.sqrt(252))
        if len(closes) >= 63  and closes[-63]  > 0: metrics["momentum_3m"]  = (closes[-1] - closes[-63])  / closes[-63]
        if len(closes) >= 126 and closes[-126] > 0: metrics["momentum_6m"]  = (closes[-1] - closes[-126]) / closes[-126]
        if len(closes) >= 252 and closes[-252] > 0: metrics["momentum_12m"] = (closes[-1] - closes[-252]) / closes[-252]

    # ── Data quality ─────────────────────────────────────────────────────────
    core = ["current_price","pe_ratio","market_cap","roe","profit_margins",
            "current_ratio","beta","52w_change"]
    present = sum(1 for f in core if metrics.get(f) is not None)
    quality = {
        "completeness":      round(present / len(core), 2),
        "has_price":         metrics.get("current_price") is not None,
        "has_fundamental":   any(metrics.get(f) is not None for f in ["roe","pe_ratio"]),
        "has_balance_sheet": metrics.get("total_assets") is not None,
        "field_count":       sum(1 for v in metrics.values() if v is not None),
    }

    return metrics, quality, errors
