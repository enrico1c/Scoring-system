"""
Phase 1 — Data Collection Layer
Fetches all required market and fundamental data from Yahoo Finance via yfinance.
No API key required; yfinance handles authentication internally.
"""

import time
import threading
import numpy as np
import yfinance as yf

# Simple in-process cache: ticker -> (timestamp, metrics, quality, errors)
_cache = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 300  # seconds — reuse data for 5 minutes

# Global throttle: only one Yahoo Finance call at a time, min 2s between calls
_yf_lock = threading.Lock()
_last_yf_call = 0.0
_MIN_INTERVAL = 2.0  # seconds between calls


def prewarm():
    """No-op kept for API compatibility."""
    pass


def _cached_fetch(ticker: str):
    """Return cached result if still fresh, otherwise None."""
    with _cache_lock:
        entry = _cache.get(ticker)
        if entry and (time.time() - entry[0]) < _CACHE_TTL:
            return entry[1], entry[2], entry[3]
    return None


def _store_cache(ticker: str, metrics, quality, errors):
    with _cache_lock:
        _cache[ticker] = (time.time(), metrics, quality, errors)


def _safe(val, default=None):
    """Return val if it is a finite number, else default."""
    if val is None:
        return default
    try:
        f = float(val)
        if f != f:   # NaN check
            return default
        return f
    except (TypeError, ValueError):
        return default


def _pct(val):
    """Normalise to decimal fraction (Yahoo sometimes stores 15 instead of 0.15)."""
    v = _safe(val)
    if v is None:
        return None
    return v if abs(v) <= 1.5 else v / 100.0


def _row(df, *names):
    """Safely extract the first matching row from a yfinance DataFrame."""
    if df is None or df.empty:
        return None
    for name in names:
        if name in df.index:
            row = df.loc[name]
            vals = [v for v in row if v is not None and str(v) != "nan"]
            return vals if vals else None
    return None


# ---------------------------------------------------------------------------
# Main extraction entry-point
# ---------------------------------------------------------------------------

def extract_raw_metrics(ticker: str):
    """
    Returns (metrics_dict, quality_dict, error_list).
    """
    # Return cached result if still fresh
    cached = _cached_fetch(ticker)
    if cached:
        return cached

    metrics = {}
    errors  = []

    # Throttle + retry: only one Yahoo Finance call at a time, with backoff
    global _last_yf_call
    info = {}
    t = None
    for attempt in range(3):
        with _yf_lock:
            wait = _MIN_INTERVAL - (time.time() - _last_yf_call)
            if wait > 0:
                time.sleep(wait)
            _last_yf_call = time.time()
        try:
            t = yf.Ticker(ticker)
            info = t.info or {}
            if info.get("regularMarketPrice") or info.get("currentPrice"):
                break  # got valid data
            if attempt < 2:
                time.sleep(5)
        except Exception as e:
            msg = str(e)
            if "Too Many Requests" in msg or "Rate limited" in msg or "429" in msg:
                if attempt < 2:
                    time.sleep(10 * (attempt + 1))
                    continue
            return None, None, [f"yfinance fetch failed: {msg}"]

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
    raw_chg = info.get("regularMarketChangePercent")
    if raw_chg is not None:
        v = _safe(raw_chg)
        # yfinance returns this as a decimal (e.g. 0.012 = 1.2%)
        metrics["change_pct"] = v

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
        # yfinance returns this as a percentage (e.g. 150 = 1.5x) — normalise
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

    # ── Income statement ────────────────────────────────────────────────────
    try:
        fin = t.financials  # columns = dates (newest first)
        if fin is not None and not fin.empty:
            rev_row = _row(fin, "Total Revenue")
            ni_row  = _row(fin, "Net Income")
            gp_row  = _row(fin, "Gross Profit")
            ebit_row= _row(fin, "EBIT", "Operating Income")
            ie_row  = _row(fin, "Interest Expense")

            if rev_row and len(rev_row) >= 1:
                metrics["total_revenue"] = _safe(rev_row[0])
            if rev_row and len(rev_row) >= 2 and rev_row[1] and rev_row[1] != 0:
                metrics["revenue_growth_yoy"] = (rev_row[0] - rev_row[1]) / abs(rev_row[1])
            if ni_row and len(ni_row) >= 1:
                metrics["net_income"] = _safe(ni_row[0])
            if ni_row and len(ni_row) >= 2 and ni_row[1] and ni_row[1] != 0:
                metrics["net_income_growth_yoy"] = (ni_row[0] - ni_row[1]) / abs(ni_row[1])
            if gp_row:
                metrics["gross_profit"] = _safe(gp_row[0])
            if ebit_row:
                metrics["ebit"] = _safe(ebit_row[0])
            if ie_row:
                metrics["interest_expense"] = _safe(ie_row[0])
    except Exception as e:
        errors.append(f"Income statement: {e}")

    # ── Balance sheet ────────────────────────────────────────────────────────
    try:
        bs = t.balance_sheet
        if bs is not None and not bs.empty:
            def bval(name, *alts):
                row = _row(bs, name, *alts)
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
        errors.append(f"Balance sheet: {e}")

    # ── Cash flow ────────────────────────────────────────────────────────────
    try:
        cf = t.cashflow
        if cf is not None and not cf.empty:
            def cfval(name, *alts):
                row = _row(cf, name, *alts)
                return _safe(row[0]) if row else None

            ocf  = cfval("Operating Cash Flow", "Total Cash From Operating Activities")
            capx = cfval("Capital Expenditure", "Capital Expenditures")
            metrics["operating_cash_flow"] = ocf
            metrics["capex"] = capx
            if ocf is not None and capx is not None:
                metrics["free_cash_flow"] = ocf - abs(capx)
    except Exception as e:
        errors.append(f"Cash flow: {e}")

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

    # ── Price history: momentum & volatility ─────────────────────────────────
    try:
        hist = t.history(period="1y")
        if hist is not None and len(hist) > 20:
            closes = hist["Close"].tolist()
            rets = []
            for i in range(1, len(closes)):
                if closes[i-1] and closes[i-1] > 0:
                    rets.append(float(np.log(closes[i] / closes[i-1])))
            if rets:
                metrics["historical_volatility"] = float(np.std(rets) * np.sqrt(252))
            if len(closes) >= 63  and closes[-63]  > 0: metrics["momentum_3m"]  = (closes[-1] - closes[-63])  / closes[-63]
            if len(closes) >= 126 and closes[-126] > 0: metrics["momentum_6m"]  = (closes[-1] - closes[-126]) / closes[-126]
            if len(closes) >= 252 and closes[-252] > 0: metrics["momentum_12m"] = (closes[-1] - closes[-252]) / closes[-252]
    except Exception as e:
        errors.append(f"Price history: {e}")

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

    _store_cache(ticker, metrics, quality, errors)
    return metrics, quality, errors
