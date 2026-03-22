"""
Phase 3 — Analysis Text Generator
Produces institutional-quality written commentary for every scoring dimension
and an overall executive summary.
"""

from scoring.scorer import DIMENSION_LABELS


# ---------------------------------------------------------------------------
# Per-dimension analysts
# ---------------------------------------------------------------------------

def _analyze_valuation(dim, m):
    s   = dim["score"]
    det = dim.get("details", {})
    out = []

    pe_d = det.get("pe_ratio")
    if pe_d:
        v = pe_d["value"]
        if v < 10:
            out.append(f"The P/E of {v}x represents deep-value territory — either a genuine bargain or a signal of structural headwinds requiring investigation.")
        elif v < 18:
            out.append(f"P/E of {v}x sits at or below long-run market averages, indicating fair to attractive fundamental pricing.")
        elif v < 30:
            out.append(f"P/E of {v}x is elevated relative to historical norms; continued earnings delivery is necessary to sustain this multiple.")
        else:
            out.append(f"At a P/E of {v}x the market is pricing in considerable future growth — any earnings shortfall risks sharp multiple compression.")

    pb_d = det.get("price_to_book")
    if pb_d:
        v = pb_d["value"]
        if v < 1.0:
            out.append(f"Price-to-Book of {v}x means shares trade below book value — a classic value signal, though sector context matters.")
        elif v < 3.0:
            out.append(f"P/B of {v}x reflects reasonable asset pricing for the current business quality.")
        else:
            out.append(f"P/B of {v}x implies a heavy intangible or goodwill premium; verify whether intellectual capital justifies the premium.")

    dy_d = det.get("dividend_yield")
    if dy_d:
        v = dy_d["value"]
        if v > 0.5:
            out.append(f"A dividend yield of {v:.2f}% adds a tangible income component, cushioning total return in flat or declining markets.")
        else:
            out.append("Minimal dividend signals a reinvestment-first capital allocation strategy — evaluate growth trajectory accordingly.")

    peg_d = det.get("peg_ratio")
    if peg_d:
        v = peg_d["value"]
        if v < 1.0:
            out.append(f"PEG of {v:.2f}x suggests the stock is cheap relative to its growth rate — a key buy-side screening signal.")
        elif v < 2.0:
            out.append(f"PEG of {v:.2f}x implies growth is being priced at a fair premium.")
        else:
            out.append(f"PEG of {v:.2f}x indicates the growth premium is rich — execution must remain flawless to justify this valuation.")

    if s >= 75:
        out.append("Overall, the valuation profile compares favourably to market benchmarks and presents an attractive entry point.")
    elif s >= 55:
        out.append("Valuation is fair; neither a compelling value nor significantly overpriced by broad market standards.")
    else:
        out.append("Valuation multiples are stretched — limited margin of safety at current prices relative to peers and history.")

    return " ".join(out) if out else "Insufficient valuation data to form a view."


def _analyze_growth(dim, m):
    s   = dim["score"]
    det = dim.get("details", {})
    out = []

    rg = det.get("revenue_growth")
    if rg:
        v = rg["value"]
        if v > 20:
            out.append(f"Revenue expanding at {v:.1f}% year-over-year places this company firmly in the top tier of growth stocks — evidence of strong market demand or share capture.")
        elif v > 8:
            out.append(f"Revenue growth of {v:.1f}% exceeds nominal GDP expansion, reflecting healthy organic momentum.")
        elif v > 0:
            out.append(f"Revenue growing at {v:.1f}% — modest but positive; management must demonstrate the pipeline to sustain or accelerate this pace.")
        else:
            out.append(f"Revenue declined {abs(v):.1f}% year-over-year, raising concerns about competitive positioning, pricing power, or macro-induced volume pressure.")

    eg = det.get("earnings_growth")
    if eg:
        v = eg["value"]
        if v > 20:
            out.append(f"Earnings growth of {v:.1f}% demonstrates strong operating leverage — revenue gains are translating efficiently into bottom-line expansion.")
        elif v > 0:
            out.append(f"Earnings growing at {v:.1f}% — constructive, though investors will monitor whether margin improvements can sustain this trajectory.")
        else:
            out.append(f"Earnings contracted {abs(v):.1f}%, which warrants scrutiny of whether cost pressures, pricing deterioration, or one-time items are to blame.")

    if s >= 75:
        out.append("The growth profile is well above peer averages and represents a material catalyst for price appreciation and multiple expansion.")
    elif s >= 55:
        out.append("Moderate growth profile; execution discipline will be key to maintaining current investor expectations and multiples.")
    else:
        out.append("Growth headwinds are visible. Investors should assess whether the deceleration is cyclical (temporary) or structural (persistent) before committing capital.")

    return " ".join(out) if out else "Insufficient growth data to form a view."


def _analyze_profitability(dim, m):
    s   = dim["score"]
    det = dim.get("details", {})
    out = []

    roe_d = det.get("roe")
    if roe_d:
        v = roe_d["value"]
        if v > 20:
            out.append(f"ROE of {v:.1f}% is exceptional — a hallmark of durable competitive advantages and disciplined capital deployment that earns well above cost of equity.")
        elif v > 10:
            out.append(f"ROE of {v:.1f}% is above market average, indicating capable management of the shareholder equity base.")
        elif v > 0:
            out.append(f"ROE of {v:.1f}% is positive but below the threshold typically associated with high-quality compounders.")
        else:
            out.append(f"Negative ROE ({v:.1f}%) means the business is eroding equity value — a critical red flag absent a clear restructuring catalyst.")

    gm = det.get("gross_margins")
    if gm:
        v = gm["value"]
        if v > 50:
            out.append(f"Gross margin of {v:.1f}% signals powerful pricing power and/or an asset-light model with minimal direct cost sensitivity.")
        elif v > 25:
            out.append(f"Gross margin of {v:.1f}% reflects a reasonably profitable core business with room for operating leverage.")
        else:
            out.append(f"Thin gross margin of {v:.1f}% is characteristic of capital-intensive or highly competitive industries where cost discipline is paramount.")

    om = det.get("operating_margins")
    if om:
        v = om["value"]
        if v > 20:
            out.append(f"Operating margin of {v:.1f}% reflects outstanding cost control and scalability — top-decile efficiency among most industry cohorts.")
        elif v > 8:
            out.append(f"Operating margin of {v:.1f}% is solid, providing a comfortable buffer for R&D investment and unexpected headwinds.")
        else:
            out.append(f"Operating margin of {v:.1f}% leaves limited room for error; any revenue disappointment could rapidly pressure earnings.")

    if s >= 75:
        out.append("The profitability profile positions this company as a high-quality operator worthy of a quality premium in the multiple.")
    elif s >= 55:
        out.append("Profitability metrics are acceptable — neither a drag nor a material differentiator versus the broader market.")
    else:
        out.append("Profitability is below par; focus points are margin recovery, cost structure rationalisation, and pricing strategy reassessment.")

    return " ".join(out) if out else "Insufficient profitability data to form a view."


def _analyze_market_performance(dim, m):
    s   = dim["score"]
    det = dim.get("details", {})
    out = []

    w52 = det.get("52w_change")
    if w52:
        v = w52["value"]
        if v > 30:
            out.append(f"A 52-week gain of {v:.1f}% demonstrates strong price discovery and sustained institutional buying interest.")
        elif v > 5:
            out.append(f"Price appreciation of {v:.1f}% over 12 months reflects positive investor sentiment, outperforming a flat cash return.")
        elif v > -10:
            out.append(f"The 52-week return of {v:.1f}% reflects a sideways consolidation phase — awaiting a catalyst for the next directional move.")
        else:
            out.append(f"A 52-week decline of {abs(v):.1f}% reflects either meaningful fundamental deterioration or a market overreaction — distinguish between the two before acting.")

    ma50 = det.get("price_vs_50d")
    ma200= det.get("price_vs_200d")
    if ma50 and ma200:
        a50  = ma50["value"]  > 0
        a200 = ma200["value"] > 0
        if a50 and a200:
            out.append("Trading above both the 50-day and 200-day moving averages represents a classic bullish technical configuration — trend-followers will see this as a green light.")
        elif a200:
            out.append("Price remains above the 200-day MA (long-term support) but has dipped below the 50-day — a tactical pullback within a structural uptrend.")
        elif a50:
            out.append("Short-term price action is constructive (above 50-day) but the longer-term 200-day trend remains impaired — watch for confirmation of a sustained reversal.")
        else:
            out.append("Trading below both moving averages is technically bearish; a sustained recovery above the 200-day would be required to signal trend rehabilitation.")

    if s >= 75:
        out.append("Price dynamics are decidedly positive — momentum and trend indicators both support the bulls.")
    elif s >= 55:
        out.append("Market performance is mixed, with some positive technical signals offset by lagging metrics.")
    else:
        out.append("Price performance has been weak across multiple timeframes; risk discipline and position sizing should reflect the absence of technical tailwinds.")

    return " ".join(out) if out else "Insufficient market performance data to form a view."


def _analyze_risk(dim, m):
    s   = dim["score"]
    det = dim.get("details", {})
    out = []

    beta_d = det.get("beta")
    if beta_d:
        v = beta_d["value"]
        if abs(v) < 0.5:
            out.append(f"Beta of {v:.2f} indicates low correlation to market moves — the stock provides natural diversification and capital preservation in volatile environments.")
        elif abs(v) <= 1.2:
            out.append(f"Beta of {v:.2f} means the stock broadly tracks the market; systematic (market) risk is the primary risk driver.")
        else:
            out.append(f"Beta of {v:.2f} amplifies both bull and bear market moves significantly — position sizing must account for this elevated systematic exposure.")

    vol_d = det.get("volatility")
    if vol_d:
        v = vol_d["value"]
        if v < 15:
            out.append(f"Annualised historical volatility of {v:.1f}% is low — consistent with stable, large-cap defensive equities.")
        elif v < 25:
            out.append(f"Volatility of {v:.1f}% annualised is moderate, typical of mainstream large-cap equities.")
        elif v < 40:
            out.append(f"Elevated volatility of {v:.1f}% warrants careful position sizing and use of stop-loss disciplines.")
        else:
            out.append(f"High annualised volatility of {v:.1f}% is characteristic of speculative names; options-based strategies may be warranted for risk-adjusted exposure.")

    sr_d = det.get("short_ratio")
    if sr_d:
        v = sr_d["value"]
        if v > 8:
            out.append(f"Short ratio of {v:.1f} days reflects heavy bearish positioning — this creates a potential short-squeeze risk on positive news, but also signals meaningful negative fundamental sentiment.")
        elif v > 4:
            out.append(f"Short ratio of {v:.1f} days is above average — monitor for catalyst-driven cover rallies.")
        else:
            out.append(f"Low short ratio of {v:.1f} days indicates market participants are not materially betting against the stock.")

    if s >= 70:
        out.append("The overall risk profile is conservative; suitable for risk-aware portfolios seeking capital preservation alongside upside participation.")
    elif s >= 50:
        out.append("Risk is broadly at market levels — standard position sizing and monitoring protocols apply.")
    else:
        out.append("Elevated risk indicators are present across multiple metrics; prudent risk management calls for reduced position size or protective hedges.")

    return " ".join(out) if out else "Insufficient risk data to form a view."


def _analyze_liquidity(dim, m):
    s   = dim["score"]
    det = dim.get("details", {})
    out = []

    cr_d = det.get("current_ratio")
    if cr_d:
        v = cr_d["value"]
        if v >= 1.5:
            out.append(f"Current ratio of {v:.2f}× confirms the company can comfortably absorb short-term obligations without straining operations.")
        elif v >= 1.0:
            out.append(f"Current ratio of {v:.2f}× is adequate but provides a thin buffer — any unexpected cash demand could create near-term pressure.")
        else:
            out.append(f"Current ratio of {v:.2f}× is a liquidity caution signal; the company may need to draw on credit facilities or accelerate receivables collection to meet near-term commitments.")

    qr_d = det.get("quick_ratio")
    if qr_d:
        v = qr_d["value"]
        if v >= 1.0:
            out.append(f"Quick ratio of {v:.2f}× indicates liquid assets (cash + receivables) alone cover current liabilities — no inventory liquidation required in a stress scenario.")
        else:
            out.append(f"Quick ratio of {v:.2f}× means the company depends on inventory conversion for short-term liquidity; sector norms should be considered when interpreting this.")

    vol_d = det.get("avg_volume_m") or det.get("market_liquidity")
    if vol_d:
        sentiment = "strong" if s > 62 else "moderate" if s > 45 else "limited"
        out.append(f"Market trading liquidity is {sentiment}, which is relevant for institutional investors considering position sizing and execution risk.")

    if s >= 70:
        out.append("A strong liquidity position provides financial flexibility and resilience, even in an adverse operating or macroeconomic environment.")
    elif s >= 50:
        out.append("Liquidity is adequate under base-case conditions but could face strain in a prolonged revenue shortfall scenario.")
    else:
        out.append("Liquidity stress indicators merit close monitoring; near-term cash generation and any upcoming debt maturities are key watchpoints.")

    return " ".join(out) if out else "Insufficient liquidity data to form a view."


def _analyze_structural(dim, m):
    s   = dim["score"]
    det = dim.get("details", {})
    out = []

    de_d = det.get("debt_equity")
    if de_d:
        v = de_d["value"]
        if v < 0.3:
            out.append(f"Debt-to-equity of {v:.2f}× represents a fortress balance sheet — minimal financial risk, preserving optionality for acquisitions or buybacks without balance sheet stress.")
        elif v < 1.0:
            out.append(f"Debt-to-equity of {v:.2f}× reflects conservative use of financial leverage — manageable across most interest-rate environments.")
        elif v < 2.0:
            out.append(f"Debt-to-equity of {v:.2f}× is moderately elevated; rising rates or a cash flow shortfall could increase refinancing risk over time.")
        else:
            out.append(f"High debt-to-equity of {v:.2f}× represents a leveraged capital structure. Investors should scrutinise debt maturity profiles, covenant headroom, and free cash flow generation capacity.")

    at_d = det.get("asset_turnover")
    if at_d:
        v = at_d["value"]
        if v > 1.0:
            out.append(f"Asset turnover of {v:.2f}× demonstrates efficient monetisation of the asset base — every dollar of assets generates {v:.2f} dollars in revenue.")
        else:
            out.append(f"Asset turnover of {v:.2f}× is characteristic of capital-intensive businesses; return-on-asset improvement requires either margin expansion or asset rationalisation.")

    fcf_d = det.get("fcf_yield")
    if fcf_d:
        v = fcf_d["value"]
        if v > 5:
            out.append(f"Free cash flow yield of {v:.1f}% is above market average — the company is generating real cash well in excess of what valuations imply, supporting dividends, buybacks, or debt reduction.")
        elif v > 0:
            out.append(f"Positive FCF yield of {v:.1f}% confirms cash generation, even if modest relative to the market capitalisation.")
        else:
            out.append(f"Negative FCF yield ({v:.1f}%) indicates the business is consuming more cash than it generates — capital requirements must be financed externally or from existing cash reserves.")

    if s >= 70:
        out.append("Structural fundamentals are robust; the balance sheet can absorb economic disruptions and positions management for value-accretive capital allocation.")
    elif s >= 50:
        out.append("Balance sheet structure is sound but leverage levels deserve ongoing monitoring, particularly in a rising-rate environment.")
    else:
        out.append("Structural risk is present — elevated leverage, thin asset efficiency, or poor FCF generation may limit future strategic options and financial flexibility.")

    return " ".join(out) if out else "Insufficient structural data to form a view."


def _analyze_credit(dim, m):
    s   = dim["score"]
    det = dim.get("details", {})
    out = []

    ic_d = det.get("interest_coverage")
    if ic_d:
        v = ic_d["value"]
        if v > 10:
            out.append(f"Interest coverage of {v:.1f}× is excellent — debt service costs represent a very small fraction of operating income, with no near-term concern.")
        elif v > 3:
            out.append(f"Interest coverage of {v:.1f}× is healthy; earnings comfortably service interest obligations with meaningful headroom for adverse scenarios.")
        elif v > 1.5:
            out.append(f"Interest coverage of {v:.1f}× is thin — a moderate earnings decline could stress interest payments and potentially trigger covenant reviews.")
        else:
            out.append(f"Critical: interest coverage of {v:.1f}× signals the company is barely generating enough operating income to service its debt. Default risk is elevated without rapid improvement.")

    da_d = det.get("debt_to_assets")
    if da_d:
        v = da_d["value"]
        if v < 0.2:
            out.append(f"Debt represents only {v*100:.1f}% of total assets — an exceptionally conservative balance sheet that should carry investment-grade equivalent credit characteristics.")
        elif v < 0.5:
            out.append(f"Debt-to-assets of {v:.2f} is within the range typically associated with investment-grade credit quality.")
        else:
            out.append(f"Debt-to-assets of {v:.2f} indicates a highly leveraged balance sheet; credit risk is above market average and refinancing conditions will be critical.")

    od_d = det.get("ocf_to_debt")
    if od_d:
        v = od_d["value"]
        if v > 0.5:
            out.append(f"Operating cash flow covers {v*100:.0f}% of total debt annually — the company could theoretically repay all debt within two years from cash operations alone.")
        elif v > 0.2:
            out.append(f"Cash flow covers {v*100:.0f}% of debt per year — adequate but implying multi-year deleveraging timelines.")
        else:
            out.append(f"Cash flow covers only {v*100:.0f}% of debt annually — significant reliance on external financing or asset sales would be required for material deleveraging.")

    if s >= 70:
        out.append("Credit quality is strong — the company presents low default risk and should access capital markets on favourable terms.")
    elif s >= 50:
        out.append("Credit profile is adequate but not investment-grade quality across all metrics; monitor for any covenant or rating-related triggers.")
    else:
        out.append("Credit risk flags are material — refinancing risk, covenant breaches, or rating downgrades are plausible scenarios that should be factored into the investment thesis.")

    return " ".join(out) if out else "Insufficient credit data to form a view."


def _analyze_macro_alignment(dim, m):
    s   = dim["score"]
    det = dim.get("details", {})
    out = []

    tr_d = det.get("trend_alignment")
    if tr_d:
        ts = tr_d["score"]
        if ts > 70:
            out.append("The stock is trading in constructive alignment with both primary moving averages, consistent with a favourable macro tailwind and trend-following capital flows.")
        elif ts > 50:
            out.append("Price action shows mixed technical signals relative to moving averages — macro positioning is directionally uncertain.")
        else:
            out.append("The stock is not aligned with current macro momentum; caution is warranted until price recovers above key moving averages.")

    ms_d = det.get("macro_sensitivity")
    if ms_d:
        v = ms_d["value"]
        if v < 0.5:
            out.append(f"Beta of {v:.2f} implies strong defensive characteristics — this stock may act as a portfolio anchor in risk-off macro environments.")
        elif v <= 1.3:
            out.append(f"Beta of {v:.2f} reflects balanced macro exposure — neither purely cyclical nor purely defensive, allowing participation across macro regimes.")
        else:
            out.append(f"Beta of {v:.2f} makes this a distinctly cyclical name — most suited to expansionary macro regimes and liquidity-driven bull markets.")

    am_d = det.get("annual_momentum")
    if am_d:
        v = am_d["value"]
        if v > 20:
            out.append(f"12-month momentum of +{v:.1f}% reflects positive macro alignment and sustained institutional buying interest over the course of the year.")
        elif v > 0:
            out.append(f"Positive 12-month momentum ({v:.1f}%) suggests the stock has been a mild beneficiary of prevailing macro conditions.")
        else:
            out.append(f"Negative 12-month performance ({v:.1f}%) indicates macro headwinds have dominated, or that company-specific negatives have outweighed supportive market conditions.")

    sector = m.get("sector", "Unknown")
    if sector != "Unknown":
        out.append(f"Sector: {sector}. Regime-specific factor weights should reflect the cyclicality and macro sensitivity inherent to this sector classification.")

    if s >= 70:
        out.append("Overall macro alignment is supportive — the combination of trend, beta, and momentum suggests the current environment is additive to the investment case.")
    elif s >= 50:
        out.append("Macro alignment is neutral; the stock is neither a clear beneficiary nor a clear victim of current macro dynamics.")
    else:
        out.append("Macro alignment is unfavourable — the current regime appears to be working against this name's risk/return characteristics.")

    return " ".join(out) if out else "Insufficient macro alignment data to form a view."


def _analyze_factor_attractiveness(dim, m):
    s   = dim["score"]
    det = dim.get("details", {})
    out = []

    val_d = det.get("value_factor")
    if val_d:
        v = val_d["score"]
        if v > 70:
            out.append("The value factor screens favourably — the stock ranks well on PE, PB, and PEG relative to market benchmarks, identifying it as a potential value opportunity.")
        elif v > 50:
            out.append("Value metrics are moderate — the stock is neither a deep-value candidate nor significantly stretched on fundamental pricing.")
        else:
            out.append("Value factor scores are below average — current pricing already bakes in significant growth or quality expectations that must materialise.")

    q_d = det.get("quality_factor")
    if q_d:
        v = q_d["score"]
        if v > 70:
            out.append("The quality factor screens strongly — high ROE and durable margins indicate a business model with real competitive advantages worth paying up for.")
        elif v > 50:
            out.append("Quality metrics are in the acceptable range — the business generates adequate returns but lacks the exceptional profitability that commands a quality premium.")
        else:
            out.append("Quality factor is weak — below-average profitability and returns metrics suggest the business model lacks a durable economic moat.")

    mom_d = det.get("momentum_factor")
    if mom_d:
        v = mom_d["score"]
        if v > 70:
            out.append("Momentum factor is positive — medium-term price trends signal continued institutional allocation and a reinforcing cycle of investor interest.")
        elif v > 50:
            out.append("Momentum is neutral — neither a clear positive nor negative signal from recent price trends alone.")
        else:
            out.append("Momentum factor is negative — price trend deterioration suggests systematic sellers have been active; a catalyst would be needed to reverse this signal.")

    if s >= 70:
        out.append("Multi-factor attractiveness is high — this stock scores well simultaneously on value, quality, and momentum, a rare combination historically associated with superior risk-adjusted returns.")
    elif s >= 50:
        out.append("Factor attractiveness is mixed; certain factors are compelling while others are neutral or a drag on the composite score.")
    else:
        out.append("Multi-factor analysis yields a below-average score — few factors align positively at current prices, which typically implies a lower expected return relative to risk taken.")

    return " ".join(out) if out else "Insufficient factor data to form a view."


# ---------------------------------------------------------------------------
# Dispatch table
# ---------------------------------------------------------------------------

_ANALYZERS = {
    "valuation":             _analyze_valuation,
    "growth":                _analyze_growth,
    "profitability":         _analyze_profitability,
    "market_performance":    _analyze_market_performance,
    "risk":                  _analyze_risk,
    "liquidity":             _analyze_liquidity,
    "structural":            _analyze_structural,
    "credit":                _analyze_credit,
    "macro_alignment":       _analyze_macro_alignment,
    "factor_attractiveness": _analyze_factor_attractiveness,
}


# ---------------------------------------------------------------------------
# Master entry-point
# ---------------------------------------------------------------------------

def generate_full_analysis(score_result: dict, metrics: dict) -> dict:
    """
    Returns a dict with keys = dimension names + '_summary'.
    Each value is a paragraph of institutional-quality text.
    """
    analyses = {}

    for dim, res in score_result["dimensions"].items():
        fn = _ANALYZERS.get(dim)
        if fn:
            try:
                analyses[dim] = fn(res, metrics)
            except Exception as e:
                analyses[dim] = f"Analysis unavailable: {e}"

    # Executive summary
    final  = score_result["final_score"]
    rating = score_result["rating"]["label"]
    name   = metrics.get("name", metrics.get("ticker", "This asset"))
    ticker = metrics.get("ticker", "")
    sector = metrics.get("sector", "Unknown")

    dim_scores  = {d: r["score"] for d, r in score_result["dimensions"].items()}
    sorted_dims = sorted(dim_scores.items(), key=lambda x: x[1], reverse=True)

    best_dim  = DIMENSION_LABELS.get(sorted_dims[0][0],  sorted_dims[0][0])
    worst_dim = DIMENSION_LABELS.get(sorted_dims[-1][0], sorted_dims[-1][0])
    best_score  = sorted_dims[0][1]
    worst_score = sorted_dims[-1][1]

    summary = (
        f"{name} ({ticker}) receives a Universal Composite Score of {final}/100, "
        f"corresponding to a rating of \"{rating}\". "
        f"The asset operates in the {sector} sector. "
        f"Its standout strength is {best_dim} ({best_score:.1f}/100), "
        f"while the area most in need of improvement is {worst_dim} ({worst_score:.1f}/100). "
    )

    if final >= 80:
        summary += (
            "The broad-based strength across multiple analytical dimensions makes this a high-conviction "
            "candidate for growth, quality, and momentum-oriented mandates alike."
        )
    elif final >= 65:
        summary += (
            "The asset presents a balanced risk-reward profile with selective areas of genuine strength "
            "that may appeal to sector-specific or factor-driven investment strategies."
        )
    elif final >= 50:
        summary += (
            "Several dimensions require improvement before this represents a compelling risk-adjusted "
            "opportunity; current positioning is neutral pending catalysts."
        )
    else:
        summary += (
            "Material concerns span multiple analytical dimensions. Thorough due diligence, conservative "
            "position sizing, and a clearly defined catalyst are advised before establishing or adding exposure."
        )

    analyses["_summary"] = summary
    return analyses
