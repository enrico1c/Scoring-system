"""
Phase 2 — Scoring Engine
Implements the Universal Scoring Formula from the institutional framework.

Architecture: raw variables → normalization → dimension scores → weighted composite
Formula:  FinalScore = α·[Σ_d W_{d,c,r,t} · (Σ_j ω_j · Z_j) · Q_d] + (1−α)·RA
"""

# ---------------------------------------------------------------------------
# Interpolation helper — maps a raw value to 0-100 using piecewise linear map
# ---------------------------------------------------------------------------

def _interp(value, breakpoints):
    """
    breakpoints: list of (raw_value, score_0_to_100) sorted ascending by raw_value.
    Linear interpolation; clamped at extremes.
    """
    if value is None:
        return None
    pts = sorted(breakpoints, key=lambda p: p[0])
    if value <= pts[0][0]:
        return float(pts[0][1])
    if value >= pts[-1][0]:
        return float(pts[-1][1])
    for i in range(len(pts) - 1):
        x0, s0 = pts[i]
        x1, s1 = pts[i + 1]
        if x0 <= value <= x1:
            t = (value - x0) / (x1 - x0)
            return float(s0 + t * (s1 - s0))
    return 50.0


def _avg(scores):
    valid = [s for s in scores if s is not None]
    return sum(valid) / len(valid) if valid else None


# ---------------------------------------------------------------------------
# Rating labels
# ---------------------------------------------------------------------------

def get_assessment(score):
    if   score >= 85: return "Exceptional"
    elif score >= 75: return "Strong"
    elif score >= 65: return "Good"
    elif score >= 55: return "Moderate"
    elif score >= 45: return "Fair"
    elif score >= 35: return "Weak"
    else:             return "Poor"


def get_rating(score):
    if   score >= 90: return {"label": "Exceptional",     "class": "rating-exceptional",     "stars": 5}
    elif score >= 80: return {"label": "Very Attractive",  "class": "rating-very-attractive",  "stars": 4}
    elif score >= 70: return {"label": "Attractive",       "class": "rating-attractive",        "stars": 4}
    elif score >= 60: return {"label": "Neutral",          "class": "rating-neutral",           "stars": 3}
    elif score >= 50: return {"label": "Weak",             "class": "rating-weak",              "stars": 2}
    else:             return {"label": "Unattractive",     "class": "rating-unattractive",      "stars": 1}


# ---------------------------------------------------------------------------
# Dimension scorers  (each returns a dict with score, confidence, details)
# ---------------------------------------------------------------------------

def _dim_result(scores_map, details):
    """Build standardised dimension result from {name: score_or_None} and details dict."""
    valid = [v for v in scores_map.values() if v is not None]
    if not valid:
        return {"score": 50.0, "confidence": 0.1, "details": details, "assessment": "Insufficient data"}
    final = _avg(valid)
    conf  = min(1.0, len(valid) / max(1, len(scores_map)))
    return {"score": round(final, 1), "confidence": round(conf, 2),
            "details": details, "assessment": get_assessment(final)}


# 1 ── Valuation / Carry ──────────────────────────────────────────────────────

def score_valuation(m):
    details = {}
    scores  = {}

    pe = m.get("pe_ratio") or m.get("forward_pe")
    if pe and pe > 0:
        s = _interp(pe, [(3,85),(8,88),(12,83),(18,72),(25,55),(35,38),(50,22),(80,10)])
        scores["pe"] = s
        details["pe_ratio"] = {"value": round(pe,2), "score": round(s,1),
                               "label": "P/E Ratio", "note": "lower = cheaper"}

    pb = m.get("price_to_book")
    if pb and pb > 0:
        s = _interp(pb, [(0.3,90),(1.0,80),(2.0,65),(3.0,50),(5.0,35),(8.0,20),(15,10)])
        scores["pb"] = s
        details["price_to_book"] = {"value": round(pb,2), "score": round(s,1),
                                    "label": "Price / Book", "note": "lower = more value"}

    ev_eb = m.get("ev_to_ebitda")
    if ev_eb and 0 < ev_eb < 200:
        s = _interp(ev_eb, [(3,90),(7,80),(12,65),(18,50),(25,35),(35,20),(50,10)])
        scores["ev_ebitda"] = s
        details["ev_ebitda"] = {"value": round(ev_eb,2), "score": round(s,1),
                                "label": "EV / EBITDA", "note": "lower = cheaper enterprise"}

    dy = m.get("dividend_yield")
    if dy is not None and dy >= 0:
        pct = dy * 100
        s = _interp(pct, [(0,50),(1.5,60),(2.5,72),(3.5,80),(5.0,76),(7.0,62),(10,48),(15,32)])
        scores["div_yield"] = s
        details["dividend_yield"] = {"value": round(pct,2), "score": round(s,1),
                                     "label": "Dividend Yield %", "note": "2-5% optimal range"}

    peg = m.get("peg_ratio")
    if peg and 0 < peg < 10:
        s = _interp(peg, [(0.3,90),(0.7,82),(1.0,75),(1.5,60),(2.0,45),(3.0,28),(5.0,14)])
        scores["peg"] = s
        details["peg_ratio"] = {"value": round(peg,2), "score": round(s,1),
                                "label": "PEG Ratio", "note": "<1.0 = growth underpriced"}

    return _dim_result(scores, details)


# 2 ── Growth / Adoption ──────────────────────────────────────────────────────

def score_growth(m):
    details = {}
    scores  = {}

    rg = m.get("revenue_growth") or m.get("revenue_growth_yoy")
    if rg is not None:
        pct = rg * 100
        s = _interp(pct, [(-30,5),(-10,18),(-3,32),(0,44),(5,56),(10,66),(15,75),(22,83),(35,90),(50,95)])
        scores["rev_growth"] = s
        details["revenue_growth"] = {"value": round(pct,2), "score": round(s,1),
                                     "label": "Revenue Growth %", "note": "higher = stronger demand"}

    eg = m.get("earnings_growth") or m.get("net_income_growth_yoy")
    if eg is not None:
        pct = eg * 100
        s = _interp(pct, [(-30,5),(-10,18),(0,38),(5,52),(10,63),(20,75),(30,85),(50,92)])
        scores["earn_growth"] = s
        details["earnings_growth"] = {"value": round(pct,2), "score": round(s,1),
                                      "label": "Earnings Growth %", "note": "operating leverage indicator"}

    m3 = m.get("momentum_3m")
    if m3 is not None:
        pct = m3 * 100
        s = _interp(pct, [(-40,10),(-20,25),(-5,40),(0,50),(5,60),(12,68),(25,78),(40,87)])
        scores["mom_3m"] = s * 0.6 + 20   # dampened: momentum is secondary in growth dim
        details["momentum_3m"] = {"value": round(pct,2), "score": round(s,1),
                                  "label": "3-Month Price Chg %", "note": "proxy for near-term expectations"}

    return _dim_result(scores, details)


# 3 ── Profitability / Quality ────────────────────────────────────────────────

def score_profitability(m):
    details = {}
    scores  = {}

    for key, label, bp in [
        ("roe",             "Return on Equity %",   [(-20,5),(-5,15),(0,25),(5,40),(10,55),(15,68),(20,78),(25,86),(30,92),(40,96)]),
        ("roa",             "Return on Assets %",   [(-10,5),(0,30),(3,48),(6,63),(10,76),(15,87),(20,94)]),
        ("gross_margins",   "Gross Margin %",       [(0,15),(10,28),(20,42),(30,55),(40,65),(50,73),(60,81),(70,88),(80,93)]),
        ("operating_margins","Operating Margin %",  [(-10,5),(0,25),(5,43),(10,58),(15,68),(20,78),(25,85),(35,92)]),
        ("profit_margins",  "Net Profit Margin %",  [(-10,5),(0,28),(3,43),(7,56),(10,66),(15,75),(20,83),(30,91)]),
    ]:
        val = m.get(key)
        if val is not None:
            pct = val * 100 if abs(val) < 2 else val
            s = _interp(pct, bp)
            scores[key] = s
            details[key] = {"value": round(pct,2), "score": round(s,1),
                            "label": label, "note": "higher = more efficient"}

    return _dim_result(scores, details)


# 4 ── Market Performance ─────────────────────────────────────────────────────

def score_market_performance(m):
    details = {}
    scores  = {}

    w52 = m.get("52w_change")
    if w52 is not None:
        pct = w52 * 100
        s = _interp(pct, [(-60,5),(-30,15),(-15,28),(-5,40),(0,50),(10,62),(20,72),(30,80),(50,88),(80,93)])
        scores["w52"] = s
        details["52w_change"] = {"value": round(pct,2), "score": round(s,1),
                                 "label": "52-Week Return %", "note": "annual price performance"}

    pv50 = m.get("price_vs_50d")
    if pv50 is not None:
        pct = pv50 * 100
        s = _interp(pct, [(-20,15),(-10,28),(-5,38),(0,50),(3,60),(7,70),(12,78),(20,85)])
        scores["pv50"] = s
        details["price_vs_50d"] = {"value": round(pct,2), "score": round(s,1),
                                   "label": "Price vs 50-Day MA %", "note": "short-term trend"}

    pv200 = m.get("price_vs_200d")
    if pv200 is not None:
        pct = pv200 * 100
        s = _interp(pct, [(-30,10),(-15,25),(-5,38),(0,50),(5,62),(10,72),(20,82),(35,90)])
        scores["pv200"] = s
        details["price_vs_200d"] = {"value": round(pct,2), "score": round(s,1),
                                    "label": "Price vs 200-Day MA %", "note": "long-term trend"}

    pos = m.get("52w_position")
    if pos is not None:
        pct = pos * 100
        s = _interp(pct, [(0,22),(10,32),(25,42),(45,54),(60,64),(75,73),(88,81),(100,87)])
        scores["range_pos"] = s
        details["52w_position"] = {"value": round(pct,1), "score": round(s,1),
                                   "label": "52-Week Range Position %", "note": "100% = at yearly high"}

    m6 = m.get("momentum_6m")
    if m6 is not None:
        pct = m6 * 100
        s = _interp(pct, [(-40,8),(-20,22),(-10,35),(0,50),(5,60),(10,68),(20,77),(35,86),(50,92)])
        scores["mom6"] = s
        details["momentum_6m"] = {"value": round(pct,2), "score": round(s,1),
                                  "label": "6-Month Momentum %", "note": "intermediate trend signal"}

    return _dim_result(scores, details)


# 5 ── Risk Profile ───────────────────────────────────────────────────────────

def score_risk(m):
    details = {}
    scores  = {}

    beta = m.get("beta")
    if beta is not None:
        ab = abs(beta)
        s = _interp(ab, [(0.0,55),(0.3,62),(0.6,70),(0.8,76),(1.0,73),(1.2,66),(1.5,53),(2.0,36),(2.5,22),(3.0,12)])
        if beta < 0:
            s = min(85, s + 8)
        scores["beta"] = s
        details["beta"] = {"value": round(beta,3), "score": round(s,1),
                           "label": "Beta", "note": "0.6-1.2 is moderate risk"}

    hv = m.get("historical_volatility")
    if hv is not None:
        pct = hv * 100
        s = _interp(pct, [(5,88),(10,82),(15,75),(20,65),(25,55),(30,44),(40,32),(50,22),(70,12)])
        scores["vol"] = s
        details["volatility"] = {"value": round(pct,2), "score": round(s,1),
                                 "label": "Annual Volatility %", "note": "lower = steadier"}

    sr = m.get("short_ratio")
    if sr is not None and sr >= 0:
        s = _interp(sr, [(0,76),(1,72),(2,68),(3,63),(5,55),(8,44),(10,35),(15,22)])
        scores["short"] = s
        details["short_ratio"] = {"value": round(sr,2), "score": round(s,1),
                                  "label": "Short Ratio (days)", "note": "high = heavy short interest"}

    return _dim_result(scores, details)


# 6 ── Liquidity ──────────────────────────────────────────────────────────────

def score_liquidity(m):
    details = {}
    scores  = {}

    cr = m.get("current_ratio")
    if cr is not None and cr >= 0:
        s = _interp(cr, [(0.3,10),(0.7,25),(0.9,38),(1.0,45),(1.2,58),(1.5,72),(2.0,82),(2.5,80),(3.5,72),(5.0,60)])
        scores["cr"] = s
        details["current_ratio"] = {"value": round(cr,2), "score": round(s,1),
                                    "label": "Current Ratio", "note": "1.5-2.5 is optimal"}

    qr = m.get("quick_ratio")
    if qr is not None and qr >= 0:
        s = _interp(qr, [(0.2,10),(0.5,25),(0.7,40),(0.9,52),(1.0,62),(1.3,72),(1.8,80),(2.5,78)])
        scores["qr"] = s
        details["quick_ratio"] = {"value": round(qr,2), "score": round(s,1),
                                  "label": "Quick Ratio", "note": ">1.0 avoids inventory dependency"}

    avg_vol = m.get("avg_volume")
    flt     = m.get("float_shares")
    if avg_vol and flt and flt > 0:
        vol_pct = (avg_vol / flt) * 100
        s = _interp(vol_pct, [(0.01,20),(0.05,35),(0.1,48),(0.2,60),(0.5,72),(1.0,82),(2.0,88),(5.0,92)])
        scores["mkt_liq"] = s
        details["market_liquidity"] = {"value": round(vol_pct,4), "score": round(s,1),
                                       "label": "Daily Vol % of Float", "note": "higher = easier to trade"}
    elif avg_vol:
        vol_m = avg_vol / 1_000_000
        s = _interp(vol_m, [(0.01,20),(0.1,35),(0.5,48),(1.0,58),(5.0,70),(10.0,80),(50.0,88),(100.0,92)])
        scores["mkt_liq"] = s
        details["avg_volume_m"] = {"value": round(vol_m,2), "score": round(s,1),
                                   "label": "Avg Volume (M shares)", "note": "higher = liquid market"}

    return _dim_result(scores, details)


# 7 ── Structural Strength ────────────────────────────────────────────────────

def score_structural(m):
    details = {}
    scores  = {}

    de = m.get("debt_to_equity_fd") or m.get("debt_to_equity")
    if de is not None and de >= 0:
        if de > 10:
            de = de / 100.0          # Yahoo sometimes expresses as percentage
        s = _interp(de, [(0,88),(0.1,85),(0.3,80),(0.5,74),(0.8,65),(1.0,58),(1.5,48),(2.0,38),(3.0,25),(5.0,12)])
        scores["de"] = s
        details["debt_equity"] = {"value": round(de,3), "score": round(s,1),
                                  "label": "Debt / Equity", "note": "lower = less leverage risk"}

    rev    = m.get("total_revenue")
    assets = m.get("total_assets")
    if rev and assets and assets > 0:
        at = rev / assets
        s  = _interp(at, [(0.05,20),(0.2,35),(0.4,50),(0.7,62),(1.0,72),(1.5,80),(2.0,85),(3.0,88)])
        scores["at"] = s
        details["asset_turnover"] = {"value": round(at,3), "score": round(s,1),
                                     "label": "Asset Turnover", "note": "revenue per $ of assets"}

    fcf  = m.get("free_cash_flow")
    mcap = m.get("market_cap")
    if fcf is not None and mcap and mcap > 0:
        fcf_y = (fcf / mcap) * 100
        s = _interp(fcf_y, [(-5,10),(0,30),(2,48),(4,62),(6,73),(8,81),(10,87),(15,92)])
        scores["fcf"] = s
        details["fcf_yield"] = {"value": round(fcf_y,2), "score": round(s,1),
                                "label": "FCF Yield %", "note": "cash generation vs market value"}

    return _dim_result(scores, details)


# 8 ── Credit Quality ─────────────────────────────────────────────────────────

def score_credit(m):
    details = {}
    scores  = {}

    ic = m.get("interest_coverage")
    if ic is not None:
        s = 5.0 if ic < 0 else _interp(ic, [(0.5,8),(1.0,18),(1.5,30),(2.0,42),(3.0,55),(5.0,68),(7.0,78),(10.0,87),(15.0,93)])
        scores["ic"] = s
        details["interest_coverage"] = {"value": round(ic,2), "score": round(s,1),
                                        "label": "Interest Coverage (×)", "note": ">3× considered safe"}

    ltd  = m.get("long_term_debt")  or 0
    std  = m.get("short_term_debt") or 0
    ta   = m.get("total_assets")
    if ta and ta > 0 and (ltd + std) >= 0:
        da = (ltd + std) / ta
        s  = _interp(da, [(0,90),(0.1,84),(0.2,76),(0.3,66),(0.4,54),(0.5,43),(0.6,32),(0.7,22),(0.8,12)])
        scores["da"] = s
        details["debt_to_assets"] = {"value": round(da,3), "score": round(s,1),
                                     "label": "Debt / Assets", "note": "lower = safer balance sheet"}

    ocf = m.get("operating_cash_flow")
    if ocf and (ltd + std) > 0:
        od = ocf / (ltd + std)
        s  = _interp(od, [(-0.1,5),(0,20),(0.1,35),(0.2,50),(0.3,62),(0.5,72),(0.7,81),(1.0,88)])
        scores["od"] = s
        details["ocf_to_debt"] = {"value": round(od,3), "score": round(s,1),
                                  "label": "OCF / Total Debt", "note": ">0.3 indicates repayment capacity"}

    return _dim_result(scores, details)


# 9 ── Macro Alignment ────────────────────────────────────────────────────────

def score_macro_alignment(m):
    details = {}
    scores  = {}

    pv50  = m.get("price_vs_50d")
    pv200 = m.get("price_vs_200d")
    if pv50 is not None and pv200 is not None:
        combined = (pv50 + pv200) * 50
        if pv50 > 0 and pv200 > 0:
            s = min(92, 72 + combined * 0.3)
        elif pv200 > 0:
            s = 56
        elif pv50 > 0:
            s = 50
        else:
            s = max(18, 44 + combined)
        scores["trend"] = s
        details["trend_alignment"] = {"value": round(combined,2), "score": round(s,1),
                                      "label": "Trend vs MAs", "note": "above 50d+200d = bullish"}

    beta = m.get("beta")
    if beta is not None:
        ab = abs(beta)
        s  = _interp(ab, [(0.1,60),(0.5,70),(0.8,78),(1.0,75),(1.3,65),(1.7,50),(2.5,35)])
        scores["beta_macro"] = s
        details["macro_sensitivity"] = {"value": round(beta,3), "score": round(s,1),
                                        "label": "Beta (macro sensitivity)", "note": "moderate beta suits most regimes"}

    m12 = m.get("momentum_12m") or m.get("52w_change")
    if m12 is not None:
        pct = m12 * 100
        s = _interp(pct, [(-50,10),(-20,25),(-5,42),(0,52),(10,62),(20,70),(35,78),(50,85)])
        scores["m12"] = s
        details["annual_momentum"] = {"value": round(pct,2), "score": round(s,1),
                                      "label": "12-Month Momentum %", "note": "positive = macro tailwind"}

    return _dim_result(scores, details)


# 10 ── Factor Attractiveness ─────────────────────────────────────────────────

def score_factor_attractiveness(m):
    details = {}
    scores  = {}

    # Value factor (PE + PB + PEG combined)
    val_parts = []
    pe = m.get("pe_ratio") or m.get("forward_pe")
    pb = m.get("price_to_book")
    pg = m.get("peg_ratio")
    if pe and pe > 0:  val_parts.append(_interp(pe, [(5,90),(15,75),(20,60),(30,45),(50,25)]))
    if pb and pb > 0:  val_parts.append(_interp(pb, [(0.5,90),(1.5,75),(3.0,55),(5.0,35),(10,15)]))
    if pg and 0<pg<10: val_parts.append(_interp(pg, [(0.5,90),(1.0,75),(1.5,58),(2.0,42),(3.0,25)]))
    if val_parts:
        s = _avg(val_parts)
        scores["value_factor"] = s
        details["value_factor"] = {"value": round(s,1), "score": round(s,1),
                                   "label": "Value Factor", "note": "composite PE/PB/PEG attractiveness"}

    # Quality factor (ROE + profit margins)
    q_parts = []
    roe = m.get("roe")
    pm  = m.get("profit_margins")
    if roe is not None:
        r = roe * 100 if abs(roe) < 2 else roe
        q_parts.append(_interp(r, [(0,25),(10,55),(20,75),(30,88)]))
    if pm is not None:
        p = pm * 100 if abs(pm) < 2 else pm
        q_parts.append(_interp(p, [(0,25),(5,50),(15,72),(25,88)]))
    if q_parts:
        s = _avg(q_parts)
        scores["quality_factor"] = s
        details["quality_factor"] = {"value": round(s,1), "score": round(s,1),
                                     "label": "Quality Factor", "note": "high ROE + margins = durable moat"}

    # Momentum factor
    mom_parts = []
    m6  = m.get("momentum_6m")
    m12 = m.get("momentum_12m") or m.get("52w_change")
    if m6  is not None: mom_parts.append(_interp(m6*100,  [(-30,10),(0,50),(15,70),(30,85)]))
    if m12 is not None: mom_parts.append(_interp(m12*100, [(-30,10),(0,50),(20,70),(40,85)]))
    if mom_parts:
        s = _avg(mom_parts)
        scores["momentum_factor"] = s
        details["momentum_factor"] = {"value": round(s,1), "score": round(s,1),
                                      "label": "Momentum Factor", "note": "sustained price trend signal"}

    return _dim_result(scores, details)


# ---------------------------------------------------------------------------
# Dimension registry
# ---------------------------------------------------------------------------

DIMENSION_WEIGHTS = {
    "valuation":            0.15,
    "growth":               0.15,
    "profitability":        0.12,
    "market_performance":   0.10,
    "risk":                 0.10,
    "liquidity":            0.08,
    "structural":           0.10,
    "credit":               0.10,
    "macro_alignment":      0.05,
    "factor_attractiveness":0.05,
}

DIMENSION_LABELS = {
    "valuation":            "Valuation / Carry",
    "growth":               "Growth / Adoption",
    "profitability":        "Profitability / Quality",
    "market_performance":   "Market Performance",
    "risk":                 "Risk Profile",
    "liquidity":            "Liquidity",
    "structural":           "Structural Strength",
    "credit":               "Credit Quality",
    "macro_alignment":      "Macro Alignment",
    "factor_attractiveness":"Factor Attractiveness",
}

SCORERS = {
    "valuation":            score_valuation,
    "growth":               score_growth,
    "profitability":        score_profitability,
    "market_performance":   score_market_performance,
    "risk":                 score_risk,
    "liquidity":            score_liquidity,
    "structural":           score_structural,
    "credit":               score_credit,
    "macro_alignment":      score_macro_alignment,
    "factor_attractiveness":score_factor_attractiveness,
}


# ---------------------------------------------------------------------------
# Master scoring function
# ---------------------------------------------------------------------------

def compute_full_score(metrics: dict) -> dict:
    """
    Returns {final_score, rating, dimensions}.
    Implements quality-adjusted weighted average:
      W_eff = W_base × Q_confidence
    """
    dimension_results = {}

    for dim, fn in SCORERS.items():
        try:
            res = fn(metrics)
        except Exception as e:
            res = {"score": 50.0, "confidence": 0.0, "details": {},
                   "assessment": "Scoring error", "error": str(e)}
        res["label"]  = DIMENSION_LABELS[dim]
        res["weight"] = DIMENSION_WEIGHTS[dim]
        dimension_results[dim] = res

    # Quality-adjusted weighted average  (Q_d embedded in confidence)
    weighted_sum  = 0.0
    total_weight  = 0.0
    for res in dimension_results.values():
        eff_w = res["weight"] * res["confidence"]
        weighted_sum  += res["score"] * eff_w
        total_weight  += eff_w

    raw = weighted_sum / total_weight if total_weight > 0 else 50.0

    # Relative attractiveness overlay (simple: compare to neutral 50)
    alpha_c  = 0.90                          # weight on universal score vs RA
    ra       = raw                           # simplified: RA = self (single-asset mode)
    final    = alpha_c * raw + (1 - alpha_c) * ra

    final = max(0.0, min(100.0, final))

    return {
        "final_score": round(final, 1),
        "rating":      get_rating(final),
        "dimensions":  dimension_results,
    }
