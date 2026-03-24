"""
Universal Multi-Asset Quantitative Rating System
Flask backend — serves the frontend and exposes the analysis API.
"""

import re
import time
import os

from flask import Flask, jsonify, request, send_file, abort
from flask_cors import CORS

from scoring.data_fetcher import extract_raw_metrics, prewarm
from scoring.scorer       import compute_full_score
from scoring.analyzer     import generate_full_analysis

app = Flask(__name__, static_folder="static", static_url_path="/static")
CORS(app, origins=["https://enrico1c.github.io", "http://localhost:5000", "http://127.0.0.1:5000", "http://localhost:3000"])

# Warm up Yahoo Finance session in the background so the first request is fast
prewarm()

_TICKER_RE = re.compile(r"^[A-Z0-9\.\-\^]{1,12}$")


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return send_file("index.html")


@app.route("/api/analyze/<ticker>", methods=["GET"])
def analyze(ticker: str):
    """Main analysis endpoint — returns full scoring result as JSON."""
    ticker = ticker.upper().strip()

    if not _TICKER_RE.match(ticker):
        return jsonify({"error": "Invalid ticker symbol format."}), 400

    t0 = time.time()

    metrics, quality, fetch_errors = extract_raw_metrics(ticker)

    if not metrics:
        # Detect rate-limit specifically so the UI can show a helpful message
        rate_limited = any("rate limit" in (e or "").lower() or "too many" in (e or "").lower()
                           for e in (fetch_errors or []))
        if rate_limited:
            return jsonify({
                "error": "Yahoo Finance is temporarily rate-limiting this server. "
                         "Please wait 1–2 minutes and try again.",
                "details": fetch_errors,
            }), 429
        return jsonify({
            "error": f"Could not retrieve data for '{ticker}'. "
                     "Verify the symbol is correct and listed on a Yahoo Finance-supported exchange.",
            "details": fetch_errors,
        }), 404

    score_result = compute_full_score(metrics)
    analyses     = generate_full_analysis(score_result, metrics)

    elapsed = round(time.time() - t0, 2)

    def _fmt_cap(val):
        if val is None:
            return None
        if val >= 1e12:
            return f"${val/1e12:.2f}T"
        if val >= 1e9:
            return f"${val/1e9:.2f}B"
        if val >= 1e6:
            return f"${val/1e6:.2f}M"
        return f"${val:,.0f}"

    return jsonify({
        # ── Identity
        "ticker":       ticker,
        "name":         metrics.get("name", ticker),
        "sector":       metrics.get("sector", "Unknown"),
        "industry":     metrics.get("industry", "Unknown"),
        "currency":     metrics.get("currency", "USD"),
        # ── Price snapshot
        "current_price":    metrics.get("current_price"),
        "market_cap":       metrics.get("market_cap"),
        "market_cap_fmt":   _fmt_cap(metrics.get("market_cap")),
        "change_pct":       metrics.get("change_pct"),
        # ── Scores
        "final_score":   score_result["final_score"],
        "rating":        score_result["rating"],
        "dimensions":    score_result["dimensions"],
        # ── Analysis text
        "analyses":      analyses,
        # ── Meta
        "data_quality":     quality,
        "fetch_errors":     fetch_errors,
        "processing_time_s": elapsed,
    })


@app.route("/api/validate", methods=["GET"])
def validate():
    """Run full validation suite and return results as JSON (for the admin panel)."""
    from scoring.validator import validate_all
    ticker = request.args.get("ticker", "AAPL").upper().strip()
    if not _TICKER_RE.match(ticker):
        return jsonify({"error": "Invalid ticker"}), 400
    return jsonify(validate_all(ticker))


@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "version": "1.0",
                    "description": "Universal Multi-Asset Quantitative Rating System"})


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=False, host="0.0.0.0", port=port)
