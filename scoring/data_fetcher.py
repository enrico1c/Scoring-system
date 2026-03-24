"""
Phase 1 — Data Collection Layer
Fetches market and fundamental data from Financial Modeling Prep (FMP) API.
FMP works reliably from datacenter IPs — no bot detection or IP blocks.

Free API key (250 req/day): https://site.financialmodelingprep.com/register
Set FMP_API_KEY as an environment variable in Render settings.
"""

import os
import json
import math
import statistics
import threading
import time
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError

# ---------------------------------------------------------------------------
# In-process result cache (5 min TTL per ticker)
# ---------------------------------------------------------------------------
_cache: dict = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 300  # seconds

FMP_BASE  = "https://financialmodelingprep.com/stable"   # new stable API (2024+)
FMP_V3    = "https://financialmodelingprep.com/api/v3"    # legacy fallback


def _fmp_key() -> str:
    return os.environ.get("FMP_API_KEY", "").strip()


def prewarm():
    """No-op — FMP uses plain HTTPS, no session warming needed."""
    pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe(val, default=None):
    if val is None:
        return default
    try:
        f = float(val)
        return default if (f != f) else f  # reject NaN
    except (TypeError, ValueError):
        return default


def _get(path: str, params: dict = None):
    """GET a FMP endpoint and return parsed JSON. Raises on error."""
    key = _fmp_key()
    qs = {"apikey": key}
    if params:
        qs.update(params)
    url = f"{FMP_BASE}{path}?{urlencode(qs)}"
    req = Request(url, headers={
        "User-Agent": "Mozilla/5.0 (compatible; StockScorer/1.0)",
        "Accept": "application/json",
    })
    try:
        with urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except HTTPError as e:
        try:
            body = e.read().decode("utf-8", errors="replace")[:300]
        except Exception:
            body = ""
        raise RuntimeError(f"FMP HTTP {e.code}: {body or e.reason}")
    except URLError as e:
        raise RuntimeError(f"Network error reaching FMP: {e.reason}")

    # FMP returns {"Error Message": "..."} on auth failure
    if isinstance(data, dict) and data.get("Error Message"):
        raise RuntimeError(f"FMP API error: {data['Error Message']}")

    return data


# ---------------------------------------------------------------------------
# Main extraction entry-point
# ---------------------------------------------------------------------------

def extract_raw_metrics(ticker: str):
    """
    Returns (metrics_dict, quality_dict, error_list).
    Fetches all FMP endpoints in parallel for speed.
    """
    # Cache hit?
    with _cache_lock:
        entry = _cache.get(ticker)
        if entry and (time.time() - entry[0]) < _CACHE_TTL:
            return entry[1], entry[2], entry[3]

    key = _fmp_key()
    if not key:
        return None, None, [
            "FMP_API_KEY is not configured on this server. "
            "Get a free key at https://site.financialmodelingprep.com/register "
            "and add it as FMP_API_KEY in your Render service Environment settings."
        ]

    metrics: dict = {}
    errors: list = []
    results: dict = {}

    # ── Parallel fetch of all FMP endpoints ─────────────────────────────────
    def fetch(name, path, params=None):
        try:
            results[name] = _get(path, params)
        except Exception as e:
            errors.append(f"{name}: {e}")

    tasks = [
        ("profile",  "/profile",                 {"symbol": ticker}),
        ("ratios",   "/ratios-ttm",              {"symbol": ticker}),
        ("income",   "/income-statement",        {"symbol": ticker, "limit": "2", "period": "annual"}),
        ("balance",  "/balance-sheet-statement", {"symbol": ticker, "limit": "1", "period": "annual"}),
        ("cashflow", "/cash-flow-statement",     {"symbol": ticker, "limit": "1", "period": "annual"}),
        ("history",  "/historical-price-eod/full", {"symbol": ticker, "serietype": "line", "timeseries": "365"}),
    ]
    threads = [threading.Thread(target=fetch, args=t, daemon=True) for t in tasks]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=25)

    # ── Profile ──────────────────────────────────────────────────────────────
    raw_profile = results.get("profile", [])
    profile = raw_profile[0] if isinstance(raw_profile, list) and raw_profile else {}

    if not profile or not profile.get("price"):
        msg = (f"No data returned for '{ticker}'. Check the symbol."
               if not errors else
               f"No data for '{ticker}'. Errors: {'; '.join(errors)}")
        return None, None, [msg]

    metrics["ticker"]   = ticker.upper()
    metrics["name"]     = profile.get("companyName") or ticker.upper()
    metrics["sector"]   = profile.get("sector")   or "Unknown"
    metrics["industry"] = profile.get("industry") or "Unknown"
    metrics["currency"] = profile.get("currency") or "USD"

    cp = _safe(profile.get("price"))
    metrics["current_price"] = cp
    metrics["market_cap"]    = _safe(profile.get("marketCap") or profile.get("mktCap"))
    metrics["beta"]          = _safe(profile.get("beta"))
    metrics["avg_volume"]    = _safe(profile.get("volAvg"))
    metrics["volume"]        = _safe(profile.get("volAvg"))

    # 52w range — FMP returns "low-high" string e.g. "164.08-199.62"
    rng_str = str(profile.get("range") or "")
    if "-" in rng_str:
        parts = rng_str.split("-")
        if len(parts) == 2:
            metrics["52w_low"]  = _safe(parts[0].strip())
            metrics["52w_high"] = _safe(parts[1].strip())

    # Daily change % — FMP `changes` is absolute $ change
    ch = _safe(profile.get("changes"))
    if ch is not None and cp and cp > 0:
        prev = cp - ch
        metrics["change_pct"] = ch / prev if prev != 0 else 0.0

    # ── Ratios TTM ───────────────────────────────────────────────────────────
    raw_r = results.get("ratios", [])
    ratios = raw_r[0] if isinstance(raw_r, list) and raw_r else {}

    def rat(*keys):
        for k in keys:
            v = _safe(ratios.get(k))
            if v is not None:
                return v
        return None

    metrics["pe_ratio"]         = rat("peRatioTTM")
    metrics["forward_pe"]       = rat("peRatioTTM")   # FMP TTM-only; reuse trailing
    metrics["price_to_book"]    = rat("priceToBookRatioTTM")
    metrics["ev_to_ebitda"]     = rat("enterpriseValueMultipleTTM")
    metrics["ev_to_revenue"]    = rat("priceToSalesRatioTTM")
    metrics["peg_ratio"]        = rat("pegRatioTTM", "priceEarningsToGrowthRatioTTM")
    # FMP has a known typo: dividendYielTTM (missing final 'd')
    metrics["dividend_yield"]   = rat("dividendYielTTM", "dividendYieldTTM", "dividendYieldPercentageTTM")
    metrics["roe"]              = rat("returnOnEquityTTM")
    metrics["roa"]              = rat("returnOnAssetsTTM")
    metrics["gross_margins"]    = rat("grossProfitMarginTTM")
    metrics["operating_margins"]= rat("operatingProfitMarginTTM")
    metrics["profit_margins"]   = rat("netProfitMarginTTM")
    metrics["ebitda_margins"]   = rat("ebitdaMarginTTM", "netProfitMarginTTM")
    metrics["current_ratio"]    = rat("currentRatioTTM")
    metrics["quick_ratio"]      = rat("quickRatioTTM")
    metrics["interest_coverage"]= rat("interestCoverageTTM")

    de = rat("debtEquityRatioTTM")
    if de is not None:
        metrics["debt_to_equity_fd"] = de

    # ── Income statement ─────────────────────────────────────────────────────
    inc_list = results.get("income", [])
    if isinstance(inc_list, list) and inc_list:
        inc0 = inc_list[0]
        inc1 = inc_list[1] if len(inc_list) > 1 else {}

        metrics["total_revenue"]   = _safe(inc0.get("revenue"))
        metrics["gross_profit"]    = _safe(inc0.get("grossProfit"))
        metrics["net_income"]      = _safe(inc0.get("netIncome"))
        metrics["ebit"]            = _safe(inc0.get("operatingIncome") or inc0.get("ebit"))
        metrics["interest_expense"]= _safe(inc0.get("interestExpense"))

        r0 = _safe(inc0.get("revenue"))
        r1 = _safe(inc1.get("revenue")) if inc1 else None
        if r0 and r1 and r1 != 0:
            metrics["revenue_growth_yoy"] = (r0 - r1) / abs(r1)
            metrics["revenue_growth"]     = metrics["revenue_growth_yoy"]

        n0 = _safe(inc0.get("netIncome"))
        n1 = _safe(inc1.get("netIncome")) if inc1 else None
        if n0 and n1 and n1 != 0:
            metrics["net_income_growth_yoy"] = (n0 - n1) / abs(n1)
            metrics["earnings_growth"]       = metrics["net_income_growth_yoy"]

    # ── Balance sheet ─────────────────────────────────────────────────────────
    bs_list = results.get("balance", [])
    if isinstance(bs_list, list) and bs_list:
        bs = bs_list[0]
        metrics["total_assets"]              = _safe(bs.get("totalAssets"))
        metrics["total_liabilities"]         = _safe(bs.get("totalLiabilities") or
                                                     bs.get("totalLiabilitiesAndTotalEquity"))
        metrics["stockholder_equity"]        = _safe(bs.get("totalStockholdersEquity") or
                                                     bs.get("stockholdersEquity"))
        metrics["cash"]                      = _safe(bs.get("cashAndCashEquivalents") or
                                                     bs.get("cash"))
        metrics["total_current_assets"]      = _safe(bs.get("totalCurrentAssets"))
        metrics["total_current_liabilities"] = _safe(bs.get("totalCurrentLiabilities"))
        metrics["long_term_debt"]            = _safe(bs.get("longTermDebt"))
        metrics["short_term_debt"]           = _safe(bs.get("shortTermDebt") or
                                                     bs.get("shortTermBorrowings") or
                                                     bs.get("currentPortionOfLongTermDebt"))

        eq  = metrics.get("stockholder_equity")
        ltd = metrics.get("long_term_debt") or 0
        std = metrics.get("short_term_debt") or 0
        if eq and eq != 0:
            metrics["debt_to_equity"] = (ltd + std) / abs(eq)

    # ── Cash flow ─────────────────────────────────────────────────────────────
    cf_list = results.get("cashflow", [])
    if isinstance(cf_list, list) and cf_list:
        cf = cf_list[0]
        ocf  = _safe(cf.get("operatingCashFlow"))
        capx = _safe(cf.get("capitalExpenditure"))
        metrics["operating_cash_flow"] = ocf
        metrics["capex"]               = capx
        if ocf is not None and capx is not None:
            metrics["free_cash_flow"] = ocf - abs(capx)

    # ── Price history: momentum & volatility ──────────────────────────────────
    hist_data   = results.get("history", {})
    # stable API returns {"symbol":..., "historical":[...]} or a plain list
    if isinstance(hist_data, dict):
        hist_prices = hist_data.get("historical", [])
    elif isinstance(hist_data, list):
        hist_prices = hist_data
    else:
        hist_prices = []

    if hist_prices and len(hist_prices) > 20:
        hist_prices = list(reversed(hist_prices))   # oldest first
        closes = [float(p["close"]) for p in hist_prices
                  if p.get("close") and float(p["close"]) > 0]

        if len(closes) > 1:
            rets = [math.log(closes[i] / closes[i - 1])
                    for i in range(1, len(closes)) if closes[i - 1] > 0]
            if len(rets) > 1:
                metrics["historical_volatility"] = statistics.stdev(rets) * math.sqrt(252)

            n = len(closes)
            if n >= 63:  metrics["momentum_3m"]  = (closes[-1] - closes[-63])  / closes[-63]
            if n >= 126: metrics["momentum_6m"]  = (closes[-1] - closes[-126]) / closes[-126]
            if n >= 252: metrics["momentum_12m"] = (closes[-1] - closes[-252]) / closes[-252]
            if closes[0] > 0:
                metrics["52w_change"] = (closes[-1] - closes[0]) / closes[0]
            if n >= 50:  metrics["50d_avg"]  = sum(closes[-50:])  / 50
            if n >= 200: metrics["200d_avg"] = sum(closes[-200:]) / 200

        ma50  = metrics.get("50d_avg")
        ma200 = metrics.get("200d_avg")
        if cp and ma50  and ma50  > 0: metrics["price_vs_50d"]  = (cp - ma50)  / ma50
        if cp and ma200 and ma200 > 0: metrics["price_vs_200d"] = (cp - ma200) / ma200

        hi52 = metrics.get("52w_high")
        lo52 = metrics.get("52w_low")
        if cp and hi52 and lo52:
            r = hi52 - lo52
            if r > 0:
                metrics["52w_position"] = (cp - lo52) / r

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
