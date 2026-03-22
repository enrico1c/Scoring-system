/* =========================================================
   Universal Asset Rating System  —  Frontend Logic
   Classic 2010-style JavaScript (no frameworks)
   ========================================================= */

/* ── Utility helpers ─────────────────────────────────────── */

function el(id) { return document.getElementById(id); }
function show(id) { var e = el(id); if (e) e.style.display = "block"; }
function hide(id) { var e = el(id); if (e) e.style.display = "none"; }

function esc(str) {
    if (str === null || str === undefined) return "—";
    return String(str)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;");
}

function fmtNum(v, dp) {
    if (v === null || v === undefined) return "—";
    dp = (dp === undefined) ? 2 : dp;
    return parseFloat(v).toFixed(dp);
}

function fmtPct(v, dp) {
    if (v === null || v === undefined) return "—";
    return fmtNum(v, dp === undefined ? 2 : dp) + "%";
}

function scoreColor(s) {
    if (s >= 85) return "score-high";
    if (s >= 55) return "score-mid";
    return "score-low";
}

function fillClass(s) {
    if (s >= 85) return "fill-exceptional";
    if (s >= 75) return "fill-strong";
    if (s >= 65) return "fill-good";
    if (s >= 55) return "fill-moderate";
    if (s >= 45) return "fill-fair";
    if (s >= 35) return "fill-weak";
    return "fill-poor";
}

function assessClass(a) {
    return "assess-" + a.toLowerCase().replace(/\s+/g, "-");
}

function stars(n, max) {
    max = max || 5;
    var out = "";
    for (var i = 1; i <= max; i++) {
        out += (i <= n) ? "&#9733;" : '<span class="star-empty">&#9733;</span>';
    }
    return out;
}

function makeXHR(url, callback) {
    var xhr;
    if (window.XMLHttpRequest) {
        xhr = new XMLHttpRequest();
    } else {
        xhr = new ActiveXObject("Microsoft.XMLHTTP"); // IE6
    }
    xhr.open("GET", url, true);
    xhr.onreadystatechange = function () {
        if (xhr.readyState === 4) {
            callback(xhr.status, xhr.responseText);
        }
    };
    xhr.send();
    return xhr;
}


/* ── Main analyze flow ──────────────────────────────────── */

var currentXHR = null;

function doAnalyze() {
    var ticker = el("ticker-input").value.trim().toUpperCase();
    if (!ticker) {
        alert("Please enter a ticker symbol.");
        return;
    }

    // Reset UI
    hide("results-area");
    hide("error-box");
    hide("validation-panel");
    el("error-box").innerHTML = "";
    show("loading-overlay");
    el("analyze-btn").disabled = true;
    el("loading-text").innerHTML = "Fetching market data for <b>" + esc(ticker) + "</b>…";

    if (currentXHR) { currentXHR.abort(); }

    currentXHR = makeXHR("/api/analyze/" + encodeURIComponent(ticker), function(status, text) {
        el("analyze-btn").disabled = false;
        hide("loading-overlay");

        if (status === 0) {
            showError("Cannot reach the analysis server. Make sure the app is running on port 5000 and visit http://localhost:5000 (not the file directly).");
            return;
        }
        if (status !== 200) {
            var errMsg = "Server error (HTTP " + status + ").";
            try {
                var j = JSON.parse(text);
                if (j.error) errMsg = j.error;
            } catch(e) {}
            showError(errMsg);
            return;
        }

        var data;
        try {
            data = JSON.parse(text);
        } catch(e) {
            showError("Failed to parse server response.");
            return;
        }

        if (data.error) {
            showError(data.error);
            return;
        }

        renderResults(data);
    });
}

function showError(msg) {
    var box = el("error-box");
    box.innerHTML = "<strong>Error:</strong> " + esc(msg);
    show("error-box");
}


/* ── Render full results ────────────────────────────────── */

function renderResults(d) {
    renderCompanyCard(d);
    renderScorePanel(d);
    renderDimensionBars(d);
    renderDetailCards(d);
    show("results-area");
    // Scroll to results
    el("results-area").scrollIntoView({ behavior: "smooth", block: "start" });
}


function renderCompanyCard(d) {
    var chg    = d.change_pct;
    var chgCls = (chg >= 0) ? "change-pos" : "change-neg";
    var chgSgn = (chg >= 0) ? "+" : "";
    var chgStr = (chg !== null && chg !== undefined) ? chgSgn + fmtNum(chg * 100, 2) + "%" : "";

    var price = (d.current_price !== null && d.current_price !== undefined)
        ? d.currency + " " + fmtNum(d.current_price, 2)
        : "—";

    el("co-name").innerHTML     = esc(d.name);
    el("co-ticker").innerHTML   = esc(d.ticker);
    el("co-sector").innerHTML   = esc(d.sector);
    el("co-industry").innerHTML = esc(d.industry);
    el("co-price").innerHTML    = esc(price);
    el("co-change").className   = chgCls;
    el("co-change").innerHTML   = esc(chgStr);
    el("co-mcap").innerHTML     = d.market_cap_fmt ? "Mkt Cap: " + esc(d.market_cap_fmt) : "";
}


function renderScorePanel(d) {
    var sc   = d.final_score;
    var rat  = d.rating;

    el("big-score").innerHTML    = fmtNum(sc, 1);
    el("rating-badge").innerHTML = esc(rat.label);
    el("rating-badge").className = "rating-badge " + (rat.class || "rating-neutral");
    el("stars-display").innerHTML= stars(rat.stars, 5);
    el("summary-text").innerHTML = esc(d.analyses._summary || "");

    // Score colour
    var scEl = el("big-score");
    scEl.style.color = sc >= 70 ? "#006600" : sc >= 50 ? "#885500" : "#cc0000";
}


function renderDimensionBars(d) {
    var container = el("dim-bars-container");
    container.innerHTML = "";

    var dims = d.dimensions;
    var order = [
        "valuation","growth","profitability","market_performance",
        "risk","liquidity","structural","credit","macro_alignment","factor_attractiveness"
    ];

    for (var i = 0; i < order.length; i++) {
        var key = order[i];
        var dim = dims[key];
        if (!dim) continue;

        var sc   = dim.score || 0;
        var label= dim.label || key;
        var ass  = dim.assessment || "";

        var row  = document.createElement("div");
        row.className  = "dim-row";
        row.setAttribute("data-dim", key);
        row.onclick    = makeDimClickHandler(key);

        row.innerHTML =
            '<div class="dim-label-cell">' + esc(label) + '</div>' +
            '<div class="dim-bar-cell">' +
              '<div class="dim-bar-track">' +
                '<div class="dim-bar-fill ' + fillClass(sc) + '" style="width:0%" data-target="' + sc + '">' +
                  '<span class="dim-bar-fill-text">' + fmtNum(sc,1) + '</span>' +
                '</div>' +
              '</div>' +
            '</div>' +
            '<div class="dim-score-cell">' + fmtNum(sc,1) + '</div>' +
            '<div class="dim-assessment">' +
              '<span class="assessment-badge ' + assessClass(ass) + '">' + esc(ass) + '</span>' +
            '</div>';

        container.appendChild(row);
    }

    // Animate bars after DOM insertion
    setTimeout(function () {
        var fills = container.querySelectorAll(".dim-bar-fill[data-target]");
        for (var j = 0; j < fills.length; j++) {
            fills[j].style.width = fills[j].getAttribute("data-target") + "%";
        }
    }, 80);
}

function makeDimClickHandler(key) {
    return function () { toggleDetailCard(key); };
}

function toggleDetailCard(key) {
    var card = el("card-" + key);
    if (!card) return;
    var isActive = card.classList.contains("active");
    // Close all
    var cards = document.querySelectorAll(".detail-card");
    for (var i = 0; i < cards.length; i++) {
        cards[i].classList.remove("active");
        var tog = cards[i].querySelector(".detail-card-toggle");
        if (tog) tog.innerHTML = "+";
    }
    if (!isActive) {
        card.classList.add("active");
        var tog2 = card.querySelector(".detail-card-toggle");
        if (tog2) tog2.innerHTML = "−";
        card.scrollIntoView({ behavior: "smooth", block: "nearest" });
    }
}


function renderDetailCards(d) {
    var container = el("detail-cards-container");
    container.innerHTML = "";

    var dims  = d.dimensions;
    var anals = d.analyses;
    var order = [
        "valuation","growth","profitability","market_performance",
        "risk","liquidity","structural","credit","macro_alignment","factor_attractiveness"
    ];

    for (var i = 0; i < order.length; i++) {
        var key = order[i];
        var dim = dims[key];
        if (!dim) continue;

        var sc   = dim.score || 0;
        var label= dim.label || key;
        var ass  = dim.assessment || "";
        var ana  = anals[key] || "No analysis available.";
        var dets = dim.details || {};

        var card = document.createElement("div");
        card.className = "detail-card";
        card.id        = "card-" + key;

        // Build metrics table rows
        var metricsRows = "";
        var detKeys = Object.keys(dets);
        for (var j = 0; j < detKeys.length; j++) {
            var mk  = detKeys[j];
            var det = dets[mk];
            if (!det) continue;
            var v   = det.value;
            var vs  = (v !== null && v !== undefined) ? fmtNum(v, 2) : "—";
            var ds  = det.score || 0;
            var dsc = scoreColor(ds);
            var note= det.note || "";
            var minW= Math.max(1, Math.round(ds));
            metricsRows +=
                '<tr>' +
                  '<td class="metric-name">' + esc(det.label || mk) + '</td>' +
                  '<td class="metric-value">' + esc(vs) + '</td>' +
                  '<td class="mini-bar-cell"><div class="mini-bar-track">' +
                    '<div class="mini-bar-fill" style="width:' + minW + '%"></div>' +
                  '</div></td>' +
                  '<td class="score-cell ' + dsc + '">' + fmtNum(ds,1) + '</td>' +
                  '<td class="metric-note">' + esc(note) + '</td>' +
                '</tr>';
        }

        var metricsSection = "";
        if (metricsRows) {
            metricsSection =
                '<table class="metrics-table">' +
                  '<thead><tr>' +
                    '<th>Metric</th><th>Value</th><th>Score Bar</th><th>Score</th><th>Note</th>' +
                  '</tr></thead>' +
                  '<tbody>' + metricsRows + '</tbody>' +
                '</table>';
        }

        // Weight badge
        var wPct = dim.weight ? Math.round(dim.weight * 100) : 0;

        card.innerHTML =
            '<div class="detail-card-header" onclick="toggleDetailCard(\'' + key + '\')">' +
              '<div class="detail-card-title">&#9658;&nbsp;' + esc(label) +
                '&nbsp;<span style="font-size:10px;font-weight:normal;color:#667788;">(weight: ' + wPct + '%)</span></div>' +
              '<div class="detail-card-score">' +
                '<span class="assessment-badge ' + assessClass(ass) + '">' + esc(ass) + '</span>' +
              '</div>' +
              '<div class="detail-card-score" style="width:60px">' + fmtNum(sc,1) + ' / 100</div>' +
              '<div class="detail-card-toggle">+</div>' +
            '</div>' +
            '<div class="detail-card-body">' +
              '<div class="detail-analysis">' + esc(ana) + '</div>' +
              metricsSection +
            '</div>';

        container.appendChild(card);
    }
}


/* ── Expand / collapse all ──────────────────────────────── */

function expandAll() {
    var cards = document.querySelectorAll(".detail-card");
    for (var i = 0; i < cards.length; i++) {
        cards[i].classList.add("active");
        var tog = cards[i].querySelector(".detail-card-toggle");
        if (tog) tog.innerHTML = "−";
    }
}

function collapseAll() {
    var cards = document.querySelectorAll(".detail-card");
    for (var i = 0; i < cards.length; i++) {
        cards[i].classList.remove("active");
        var tog = cards[i].querySelector(".detail-card-toggle");
        if (tog) tog.innerHTML = "+";
    }
}


/* ── Validation panel ───────────────────────────────────── */

function runValidation() {
    var ticker = el("ticker-input").value.trim().toUpperCase() || "AAPL";
    show("validation-panel");
    el("validation-output").innerHTML = '<span class="val-section">Running validation suite for ' + esc(ticker) + '…</span>\n';

    makeXHR("/api/validate?ticker=" + encodeURIComponent(ticker), function(status, text) {
        if (status !== 200) {
            el("validation-output").innerHTML += '<span class="val-fail">Server returned HTTP ' + status + '</span>';
            return;
        }
        var data;
        try { data = JSON.parse(text); } catch(e) {
            el("validation-output").innerHTML += '<span class="val-fail">Parse error: ' + esc(String(e)) + '</span>';
            return;
        }
        renderValidation(data);
    });
}

function renderValidation(data) {
    var out = "";

    var phaseNames = { phase1: "Phase 1 — Data Collection", phase2: "Phase 2 — Scoring Engine", phase3: "Phase 3 — Analysis" };
    var phases = data.phases || {};

    for (var pk in phaseNames) {
        var ph = phases[pk];
        if (!ph) continue;
        out += '<span class="val-section">\n' + phaseNames[pk] + '\n</span>';
        var res = ph.results || [];
        for (var i = 0; i < res.length; i++) {
            var r = res[i];
            var cls = r[1] === "PASS" ? "val-pass" : r[1] === "FAIL" ? "val-fail" : "val-warn";
            out += '  <span class="' + cls + '">[' + r[1] + ']</span> ' + esc(r[0]);
            if (r[2]) out += ' — ' + esc(r[2]);
            out += '\n';
        }
        out += '  Passed: ' + ph.passed + '  Failed: ' + ph.failed + '\n\n';
    }

    var statusCls = data.status === "ok" ? "val-pass" : "val-fail";
    out += '<span class="' + statusCls + '">\n▶ ' + esc(data.status_msg || data.status) +
           '  (' + data.total_pass + ' pass / ' + data.total_fail + ' fail)\n</span>';

    el("validation-output").innerHTML = out;
}


/* ── Keyboard support ───────────────────────────────────── */

document.addEventListener("DOMContentLoaded", function () {
    var inp = el("ticker-input");
    if (inp) {
        inp.addEventListener("keydown", function(e) {
            if ((e.key || e.keyCode) === "Enter" || e.keyCode === 13) {
                doAnalyze();
            }
        });
        inp.focus();
    }
});
