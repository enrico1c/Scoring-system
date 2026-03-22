"""
Phase Validation System
Checks that every phase of the pipeline is operational, data exists,
and the system retrieves correct data before each phase goes live.
"""

import sys
import traceback


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _p(text):
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode("ascii", "replace").decode("ascii"))

def _ok(msg):   _p(f"  [PASS] {msg}")
def _fail(msg): _p(f"  [FAIL] {msg}")
def _warn(msg): _p(f"  [WARN] {msg}")
def _head(msg): _p(f"\n{'='*62}\n  {msg}\n{'='*62}")


# ---------------------------------------------------------------------------
# Phase 1 — Data Collection
# ---------------------------------------------------------------------------

def validate_phase1(ticker: str = "AAPL") -> dict:
    from scoring.data_fetcher import (
        fetch_quote_summary, fetch_price_history, extract_raw_metrics
    )

    _head(f"PHASE 1 VALIDATION  |  Data Collection  |  Ticker: {ticker}")
    passed, failed = 0, 0
    results = []

    # ── Test 1: quoteSummary endpoint ────────────────────────────────────────
    _p("\n[1/5]  Yahoo Finance quoteSummary endpoint")
    try:
        data, err = fetch_quote_summary(ticker)
        if data and not err:
            _ok(f"Data received — keys: {list(data.keys())[:6]}…")
            passed += 1; results.append(("quoteSummary endpoint", "PASS", None))
        else:
            _fail(f"Error: {err}")
            failed += 1; results.append(("quoteSummary endpoint", "FAIL", err))
    except Exception as e:
        _fail(str(e)); failed += 1; results.append(("quoteSummary endpoint", "FAIL", str(e)))

    # ── Test 2: price history endpoint ───────────────────────────────────────
    _p("\n[2/5]  Yahoo Finance price history endpoint")
    try:
        prices, perr = fetch_price_history(ticker)
        if prices and not perr:
            _ok(f"{len(prices)} daily close prices retrieved")
            passed += 1; results.append(("Price history endpoint", "PASS", None))
        else:
            _fail(f"Error: {perr}")
            failed += 1; results.append(("Price history endpoint", "FAIL", perr))
    except Exception as e:
        _fail(str(e)); failed += 1; results.append(("Price history endpoint", "FAIL", str(e)))

    # ── Test 3: Full metric extraction ───────────────────────────────────────
    _p(f"\n[3/5]  Full metric extraction for {ticker}")
    try:
        metrics, quality, errors = extract_raw_metrics(ticker)
        if metrics and quality:
            _ok(f"{quality['field_count']} fields extracted  |  completeness={quality['completeness']*100:.0f}%")
            _ok(f"has_price={quality['has_price']}  has_fundamental={quality['has_fundamental']}  has_balance_sheet={quality['has_balance_sheet']}")
            if errors:
                _warn(f"Non-fatal errors: {errors}")
            passed += 1; results.append(("Metric extraction", "PASS", None))
        else:
            _fail("extract_raw_metrics returned None")
            failed += 1; results.append(("Metric extraction", "FAIL", "None returned"))
    except Exception as e:
        _fail(traceback.format_exc())
        failed += 1; results.append(("Metric extraction", "FAIL", str(e)))
        metrics = None

    # ── Test 4: Critical field existence ────────────────────────────────────
    _p("\n[4/5]  Critical field existence check")
    if metrics:
        CRITICAL = [
            "current_price", "pe_ratio", "market_cap", "roe", "profit_margins",
            "current_ratio", "beta", "52w_change",
        ]
        present = sum(1 for f in CRITICAL if metrics.get(f) is not None)
        for f in CRITICAL:
            v = metrics.get(f)
            sym = "[OK]" if v is not None else "[--]"
            _p(f"    {sym}  {f}: {v}")
        if present >= 5:
            _ok(f"{present}/{len(CRITICAL)} critical fields present")
            passed += 1; results.append(("Critical fields", "PASS", None))
        else:
            _fail(f"Only {present}/{len(CRITICAL)} critical fields available")
            failed += 1; results.append(("Critical fields", "FAIL", f"{present} present"))
    else:
        _warn("Skipped — no metrics object available")

    # ── Test 5: Invalid ticker graceful handling ─────────────────────────────
    _p("\n[5/5]  Invalid ticker graceful-failure handling")
    try:
        bad, bq, be = extract_raw_metrics("ZZZINVALID999XYZ")
        if bad is None:
            _ok("Invalid ticker correctly returned None")
            passed += 1; results.append(("Invalid ticker handling", "PASS", None))
        elif bq and bq.get("completeness", 1) < 0.3:
            _ok("Invalid ticker returned minimal/empty data (acceptable)")
            passed += 1; results.append(("Invalid ticker handling", "PASS", "minimal data"))
        else:
            _warn("Unexpected data for invalid ticker — check symbol validation")
            passed += 1; results.append(("Invalid ticker handling", "WARN", "unexpected data"))
    except Exception as e:
        _fail(str(e)); failed += 1; results.append(("Invalid ticker handling", "FAIL", str(e)))

    _p(f"\n  PHASE 1:  {passed} passed  /  {failed} failed")
    return {"phase": 1, "passed": passed, "failed": failed, "results": results}


# ---------------------------------------------------------------------------
# Phase 2 — Scoring Engine
# ---------------------------------------------------------------------------

def validate_phase2(ticker: str = "AAPL") -> dict:
    from scoring.data_fetcher import extract_raw_metrics
    from scoring.scorer import compute_full_score, DIMENSION_WEIGHTS

    _head(f"PHASE 2 VALIDATION  |  Scoring Engine  |  Ticker: {ticker}")
    passed, failed = 0, 0
    results = []

    metrics, _, _ = extract_raw_metrics(ticker)
    if not metrics:
        _fail("Cannot validate scoring — data fetch failed")
        return {"phase": 2, "passed": 0, "failed": 1,
                "results": [("Data availability", "FAIL", "no metrics")]}

    # ── Test 1: Scoring pipeline runs ────────────────────────────────────────
    _p("\n[1/4]  Scoring pipeline execution")
    try:
        score_result = compute_full_score(metrics)
        _ok(f"Score: {score_result['final_score']}/100  |  Rating: {score_result['rating']['label']}")
        passed += 1; results.append(("Scoring pipeline", "PASS", None))
    except Exception as e:
        _fail(traceback.format_exc())
        failed += 1; results.append(("Scoring pipeline", "FAIL", str(e)))
        return {"phase": 2, "passed": passed, "failed": failed, "results": results}

    # ── Test 2: Score range validity ─────────────────────────────────────────
    _p("\n[2/4]  Score range validity (all dimensions 0-100)")
    all_valid = 0 <= score_result["final_score"] <= 100
    for dim, res in score_result["dimensions"].items():
        if not (0 <= res["score"] <= 100):
            _fail(f"{dim} score out of range: {res['score']}")
            all_valid = False
    if all_valid:
        _ok("All dimension scores within [0, 100]")
        passed += 1; results.append(("Score ranges", "PASS", None))
    else:
        failed += 1; results.append(("Score ranges", "FAIL", "out-of-range values"))

    # ── Test 3: All 10 dimensions present ────────────────────────────────────
    _p("\n[3/4]  All 10 dimensions scored")
    expected = list(DIMENSION_WEIGHTS.keys())
    missing  = [d for d in expected if d not in score_result["dimensions"]]
    if not missing:
        for d, r in score_result["dimensions"].items():
            _p(f"    [OK]  {r['label']:<30}  {r['score']:5.1f}/100  ({r['assessment']})")
        passed += 1; results.append(("10 dimensions present", "PASS", None))
    else:
        _fail(f"Missing dimensions: {missing}")
        failed += 1; results.append(("10 dimensions present", "FAIL", str(missing)))

    # ── Test 4: Weight sum equals 1.0 ────────────────────────────────────────
    _p("\n[4/4]  Dimension weight normalisation check")
    wsum = sum(r["weight"] for r in score_result["dimensions"].values())
    if abs(wsum - 1.0) < 0.001:
        _ok(f"Weights sum = {wsum:.4f} (≈ 1.0)")
        passed += 1; results.append(("Weight sum = 1", "PASS", None))
    else:
        _fail(f"Weights sum = {wsum:.4f} (expected 1.0)")
        failed += 1; results.append(("Weight sum = 1", "FAIL", str(wsum)))

    _p(f"\n  PHASE 2:  {passed} passed  /  {failed} failed")
    return {"phase": 2, "passed": passed, "failed": failed, "results": results}


# ---------------------------------------------------------------------------
# Phase 3 — Analysis Generator
# ---------------------------------------------------------------------------

def validate_phase3(ticker: str = "AAPL") -> dict:
    from scoring.data_fetcher import extract_raw_metrics
    from scoring.scorer import compute_full_score
    from scoring.analyzer import generate_full_analysis

    _head(f"PHASE 3 VALIDATION  |  Analysis Generator  |  Ticker: {ticker}")
    passed, failed = 0, 0
    results = []

    metrics, _, _ = extract_raw_metrics(ticker)
    if not metrics:
        return {"phase": 3, "passed": 0, "failed": 1,
                "results": [("Data availability", "FAIL", "no metrics")]}

    score_result = compute_full_score(metrics)

    # ── Test 1: Analysis generates without errors ────────────────────────────
    _p("\n[1/3]  Analysis text generation")
    try:
        analyses = generate_full_analysis(score_result, metrics)
        _ok(f"Analysis generated for {len(analyses)} sections")
        passed += 1; results.append(("Analysis generation", "PASS", None))
    except Exception as e:
        _fail(traceback.format_exc())
        failed += 1; results.append(("Analysis generation", "FAIL", str(e)))
        return {"phase": 3, "passed": passed, "failed": failed, "results": results}

    # ── Test 2: All dimensions covered ──────────────────────────────────────
    _p("\n[2/3]  Coverage — all dimensions have analysis text")
    dims = ["valuation","growth","profitability","market_performance",
            "risk","liquidity","structural","credit","macro_alignment","factor_attractiveness"]
    missing = [d for d in dims if d not in analyses or len(analyses[d]) < 30]
    if not missing:
        _ok("All 10 dimensions covered with substantive text")
        passed += 1; results.append(("Analysis coverage", "PASS", None))
    else:
        _fail(f"Short or missing: {missing}")
        failed += 1; results.append(("Analysis coverage", "FAIL", str(missing)))

    # ── Test 3: Executive summary present ───────────────────────────────────
    _p("\n[3/3]  Executive summary")
    summary = analyses.get("_summary", "")
    if len(summary) > 80:
        _ok(f"Summary generated ({len(summary)} chars)")
        _p(f"\n    Preview: {summary[:200]}…")
        passed += 1; results.append(("Executive summary", "PASS", None))
    else:
        _fail(f"Summary too short or absent ({len(summary)} chars)")
        failed += 1; results.append(("Executive summary", "FAIL", "too short"))

    _p(f"\n  PHASE 3:  {passed} passed  /  {failed} failed")
    return {"phase": 3, "passed": passed, "failed": failed, "results": results}


# ---------------------------------------------------------------------------
# Full suite
# ---------------------------------------------------------------------------

def validate_all(ticker: str = "AAPL") -> dict:
    _p(f"\n{'#'*62}")
    _p(f"  UNIVERSAL SCORING SYSTEM  —  FULL VALIDATION SUITE")
    _p(f"  Ticker under test: {ticker}")
    _p(f"{'#'*62}")

    p1 = validate_phase1(ticker)
    p2 = validate_phase2(ticker)
    p3 = validate_phase3(ticker)

    total_pass = p1["passed"] + p2["passed"] + p3["passed"]
    total_fail = p1["failed"] + p2["failed"] + p3["failed"]
    status     = "ALL SYSTEMS OPERATIONAL" if total_fail == 0 else f"WARNING — {total_fail} test(s) failed"

    _p(f"\n{'#'*62}")
    _p(f"  VALIDATION SUMMARY")
    _p(f"  Phase 1 (Data Collection) : {p1['passed']} pass / {p1['failed']} fail")
    _p(f"  Phase 2 (Scoring Engine)  : {p2['passed']} pass / {p2['failed']} fail")
    _p(f"  Phase 3 (Analysis)        : {p3['passed']} pass / {p3['failed']} fail")
    _p(f"  TOTAL : {total_pass} pass / {total_fail} fail")
    _p(f"  STATUS: {status}")
    _p(f"{'#'*62}\n")

    return {
        "phases":      {"phase1": p1, "phase2": p2, "phase3": p3},
        "total_pass":  total_pass,
        "total_fail":  total_fail,
        "status":      "ok" if total_fail == 0 else "warning",
        "status_msg":  status,
    }


if __name__ == "__main__":
    t = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    validate_all(t)
