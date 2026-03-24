"""
Phase 1 — Data Collection Layer
Fetches market and fundamental data from Yahoo Finance via yfinance.
yfinance handles Yahoo's CSRF/consent cookie flow that plain requests cannot.
No API key required.
"""

import time
import threading

# ---------------------------------------------------------------------------
# In-process result cache (5 min TTL per ticker)
# ---------------------------------------------------------------------------
_cache: dict = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 300  # seconds

# Throttle: one Yahoo Finance call at a time, minimum 3 s gap
_yf_lock = threading.Lock()
_last_call: float = 0.0
_MIN_GAP = 3.0

# curl_cffi Chrome session — bypasses Yahoo's TLS-fingerprint bot detection
_cf_session = None
_cf_lock = threading.Lock()


def _get_cf_session():
    """Return a curl_cffi Session impersonating Chrome, or None if unavailable."""
    global _cf_session
    with _cf_lock:
        if _cf_session is not None:
            return _cf_session if _cf_session is not False else None
        try:
            from curl_cffi import requests as cffi
            _cf_session = cffi.Session(impersonate="chrome110")
        except Exception:
            _cf_session = False  # mark unavailable so we don't retry
        return _cf_session if _cf_session is not False else None


def prewarm():
    """Initialise the curl_cffi Chrome session at startup in the background."""
    threading.Thread(target=_get_cf_session, daemon=True).start()


def _throttle():
    global _last_call
    with _yf_lock:
        gap = _MIN_GAP - (time.time() - _last_call)
        if gap > 0:
            time.sleep(gap)
        _last_call = time.time()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe(val, default=None):
    if val is None:
        return default
    try:
        f = float(val)
        return default if f != f else f
    except (TypeError, ValueError):
        return default


def _pct(val):
    v = _safe(val)
    if v is None:
        return None
    return v if abs(v) <= 1.5 else v / 100.0


def _row(df, *names):
    """Safely get the first row matching any name from a yfinance DataFrame."""
    if df is None or df.empty:
        return None
    for name in names:
        if name in df.index:
            try:
                vals = [v for v in df.loc[name] if v is not None and str(v) != "nan"]
                return vals if vals else None
            except Exception:
                continue
    return None


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

    # Import yfinance here so it only loads pandas when actually needed.
    import yfinance as yf

    metrics: dict = {}
    errors: list = []
    info: dict = {}
    t = None

    # Use curl_cffi Chrome session to bypass Yahoo's TLS-fingerprint bot detection.
    # Datacenter IPs (e.g. Render) are blocked by Yahoo without this.
    session = _get_cf_session()

    # Retry up to 3× — total max wait ~30s to stay well within gunicorn timeout
    for attempt in range(3):
        try:
            t = yf.Ticker(ticker, session=session) if session else yf.Ticker(ticker)
            info = t.info or {}
            if info.get("regularMarketPrice") or info.get("currentPrice"):
                break
            if attempt < 2:
                time.sleep(5)   # 5s, 10s between empty-response retries
        except Exception as e:
            msg = str(e)
            if any(k in msg for k in ("Too Many Requests", "Rate limited", "429")):
                if attempt < 2:
                    wait = 8 * (attempt + 1)   # 8s, 16s
                    time.sleep(wait)
                    continue
                return None, None, ["Yahoo Finance is temporarily rate-limiting this server. Please wait 1–2 minutes and try again."]
            return None, None, [f"yfinance error: {msg}"]

    if not info or (not info.get("regularMarketPrice") and not info.get("currentPrice")):
        return None, None, [f"No data returned for '{ticker}'. Check the symbol."]

    # ── Identity ─────────────────────────────────────────────────────────────
    metrics["ticker"]   = ticker.upper()
    metrics["name"]     = info.get("longName") or info.get("shortName") or ticker.upper()
    metrics["sector"]   = info.get("sector")   or "Unknown"
    metrics["industry"] = info.get("industry") or "Unknown"
    metrics["currency"] = info.get("currency") or "USD"

    # ── Price & market ───────────────────────────────────────────────────────
    metrics["current_price"] = _safe(info.get("regularMarketPrice") or info.get("currentPrice"))
    metrics["market_cap"]    = _safe(info.get("marketCap"))
    cp_chg = info.get("regularMarketChangePercent")
    if cp_chg is not None:
        metrics["change_pct"] = _safe(cp_chg)

    # ── Valuation ────────────────────────────────────────────────────────────
    metrics["pe_ratio"]      = _safe(info.get("trailingPE"))
    metrics["forward_pe"]    = _safe(info.get("forwardPE"))
    metrics["price_to_book"] = _safe(info.get("priceToBook"))
    metrics["ev_to_ebitda"]  = _safe(info.get("enterpriseToEbitda"))
    metrics["ev_to_revenue"] = _safe(info.get("enterpriseToRevenue"))
    metrics["dividend_yield"]= _pct(info.get("dividendYield"))
    metrics["peg_ratio"]     = _safe(info.get("pegRatio"))

    # ── Growth ───────────────────────────────────────────────────────────────
    metrics["revenue_growth"]  = _pct(info.get("revenueGrowth"))
    metrics["earnings_growth"] = _pct(info.get("earningsGrowth"))

    # ── Profitability ────────────────────────────────────────────────────────
    metrics["roe"]              = _pct(info.get("returnOnEquity"))
    metrics["roa"]              = _pct(info.get("returnOnAssets"))
    metrics["gross_margins"]    = _pct(info.get("grossMargins"))
    metrics["operating_margins"]= _pct(info.get("operatingMargins"))
    metrics["profit_margins"]   = _pct(info.get("profitMargins"))
    metrics["ebitda_margins"]   = _pct(info.get("ebitdaMargins"))

    # ── Liquidity ────────────────────────────────────────────────────────────
    metrics["current_ratio"]    = _safe(info.get("currentRatio"))
    metrics["quick_ratio"]      = _safe(info.get("quickRatio"))
    de = _safe(info.get("debtToEquity"))
    if de is not None:
        metrics["debt_to_equity_fd"] = de / 100.0 if abs(de) > 10 else de

    # ── Market / risk ────────────────────────────────────────────────────────
    metrics["beta"]               = _safe(info.get("beta"))
    metrics["volume"]             = _safe(info.get("volume") or info.get("regularMarketVolume"))
    metrics["avg_volume"]         = _safe(info.get("averageVolume"))
    metrics["shares_outstanding"] = _safe(info.get("sharesOutstanding"))
    metrics["float_shares"]       = _safe(info.get("floatShares"))
    metrics["short_ratio"]        = _safe(info.get("shortRatio"))
    metrics["52w_high"]           = _safe(info.get("fiftyTwoWeekHigh"))
    metrics["52w_low"]            = _safe(info.get("fiftyTwoWeekLow"))
    metrics["50d_avg"]            = _safe(info.get("fiftyDayAverage"))
    metrics["200d_avg"]           = _safe(info.get("twoHundredDayAverage"))
    metrics["52w_change"]         = _pct(info.get("52WeekChange"))

    # ── Income statement ─────────────────────────────────────────────────────
    try:
        if t is not None:
            fin = t.financials
            if fin is not None and not fin.empty:
                rev  = _row(fin, "Total Revenue")
                ni   = _row(fin, "Net Income")
                gp   = _row(fin, "Gross Profit")
                ebit = _row(fin, "EBIT", "Operating Income")
                ie   = _row(fin, "Interest Expense")
                if rev:
                    metrics["total_revenue"] = _safe(rev[0])
                    if len(rev) >= 2 and rev[1]:
                        metrics["revenue_growth_yoy"] = (rev[0] - rev[1]) / abs(rev[1])
                if ni:
                    metrics["net_income"] = _safe(ni[0])
                    if len(ni) >= 2 and ni[1]:
                        metrics["net_income_growth_yoy"] = (ni[0] - ni[1]) / abs(ni[1])
                if gp:   metrics["gross_profit"]      = _safe(gp[0])
                if ebit: metrics["ebit"]              = _safe(ebit[0])
                if ie:   metrics["interest_expense"]  = _safe(ie[0])
    except Exception as e:
        errors.append(f"financials: {e}")

    # ── Balance sheet ─────────────────────────────────────────────────────────
    try:
        if t is not None:
            bs = t.balance_sheet
            if bs is not None and not bs.empty:
                def bval(*names):
                    row = _row(bs, *names)
                    return _safe(row[0]) if row else None
                metrics["total_assets"]             = bval("Total Assets")
                metrics["total_liabilities"]        = bval("Total Liabilities Net Minority Interest", "Total Liab")
                metrics["stockholder_equity"]       = bval("Stockholders Equity", "Total Stockholder Equity")
                metrics["cash"]                     = bval("Cash And Cash Equivalents", "Cash")
                metrics["total_current_assets"]     = bval("Current Assets", "Total Current Assets")
                metrics["total_current_liabilities"]= bval("Current Liabilities", "Total Current Liabilities")
                metrics["long_term_debt"]           = bval("Long Term Debt")
                metrics["short_term_debt"]          = bval("Current Debt", "Short Long Term Debt")
                eq  = metrics.get("stockholder_equity")
                ltd = metrics.get("long_term_debt") or 0
                std = metrics.get("short_term_debt") or 0
                if eq and eq != 0:
                    metrics["debt_to_equity"] = (ltd + std) / abs(eq)
    except Exception as e:
        errors.append(f"balance_sheet: {e}")

    # ── Cash flow ─────────────────────────────────────────────────────────────
    try:
        if t is not None:
            cf = t.cashflow
            if cf is not None and not cf.empty:
                def cfval(*names):
                    row = _row(cf, *names)
                    return _safe(row[0]) if row else None
                ocf  = cfval("Operating Cash Flow", "Total Cash From Operating Activities")
                capx = cfval("Capital Expenditure", "Capital Expenditures")
                metrics["operating_cash_flow"] = ocf
                metrics["capex"] = capx
                if ocf is not None and capx is not None:
                    metrics["free_cash_flow"] = ocf - abs(capx)
    except Exception as e:
        errors.append(f"cashflow: {e}")

    # ── Derived ratios ────────────────────────────────────────────────────────
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

    # ── Price history: momentum & volatility ──────────────────────────────────
    try:
        if t is not None:
            hist = t.history(period="1y")
            if hist is not None and len(hist) > 20:
                import numpy as np  # lazy: only loaded when history is computed
                closes = hist["Close"].tolist()
                rets = [float(np.log(closes[i] / closes[i-1]))
                        for i in range(1, len(closes))
                        if closes[i-1] and closes[i-1] > 0]
                if rets:
                    metrics["historical_volatility"] = float(np.std(rets) * np.sqrt(252))
                if len(closes) >= 63  and closes[-63]  > 0: metrics["momentum_3m"]  = (closes[-1] - closes[-63])  / closes[-63]
                if len(closes) >= 126 and closes[-126] > 0: metrics["momentum_6m"]  = (closes[-1] - closes[-126]) / closes[-126]
                if len(closes) >= 252 and closes[-252] > 0: metrics["momentum_12m"] = (closes[-1] - closes[-252]) / closes[-252]
    except Exception as e:
        errors.append(f"history: {e}")

    # ── Data quality ──────────────────────────────────────────────────────────
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
