from __future__ import annotations


def render_dashboard_html() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Polymarket BTC Engine Dashboard</title>
  <style>
    :root {
      --bg: #0f172a;
      --panel: #111827;
      --panel-2: #1f2937;
      --text: #e5e7eb;
      --muted: #9ca3af;
      --ok: #10b981;
      --warn: #f59e0b;
      --bad: #ef4444;
      --accent: #22d3ee;
      --line: #334155;
      --shadow: 0 14px 28px rgba(2, 6, 23, 0.45);
    }

    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      color: var(--text);
      background:
        radial-gradient(1200px 600px at -10% -20%, #0b3a5c 0%, transparent 55%),
        radial-gradient(900px 500px at 100% 0%, #1a2346 0%, transparent 50%),
        var(--bg);
      min-height: 100vh;
    }

    .container {
      max-width: 1240px;
      margin: 0 auto;
      padding: 20px 18px 40px;
    }

    .header {
      display: flex;
      flex-wrap: wrap;
      justify-content: space-between;
      align-items: center;
      gap: 10px;
      margin-bottom: 16px;
    }

    h1 {
      margin: 0;
      font-size: 1.2rem;
      letter-spacing: 0.02em;
      font-weight: 700;
    }

    .meta {
      font-size: 0.88rem;
      color: var(--muted);
    }

    .grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
    }

    .panel {
      background: linear-gradient(160deg, rgba(17,24,39,0.98), rgba(15,23,42,0.96));
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 14px;
      box-shadow: var(--shadow);
      overflow: hidden;
    }

    .stat-label {
      color: var(--muted);
      font-size: 0.8rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      margin-bottom: 8px;
    }

    .stat-value {
      font-size: 1.25rem;
      font-weight: 700;
      line-height: 1.2;
    }

    .span-2 { grid-column: span 2; }
    .span-4 { grid-column: span 4; }

    .status-pill {
      display: inline-block;
      border-radius: 999px;
      padding: 4px 10px;
      font-size: 0.78rem;
      font-weight: 600;
      letter-spacing: 0.04em;
      border: 1px solid transparent;
      margin-top: 2px;
    }
    .ok { color: #34d399; border-color: rgba(52, 211, 153, 0.45); background: rgba(16, 185, 129, 0.14); }
    .warn { color: #fbbf24; border-color: rgba(251, 191, 36, 0.45); background: rgba(245, 158, 11, 0.14); }
    .bad { color: #f87171; border-color: rgba(248, 113, 113, 0.45); background: rgba(239, 68, 68, 0.14); }

    .table-wrap {
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 10px;
      margin-top: 8px;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 650px;
      font-size: 0.86rem;
    }

    th, td {
      padding: 9px 10px;
      border-bottom: 1px solid rgba(51, 65, 85, 0.7);
      white-space: nowrap;
      text-align: left;
    }

    th {
      color: #94a3b8;
      font-weight: 600;
      font-size: 0.78rem;
      letter-spacing: 0.06em;
      text-transform: uppercase;
      background: rgba(30, 41, 59, 0.55);
      position: sticky;
      top: 0;
    }

    tr:hover td { background: rgba(56, 189, 248, 0.06); }

    .foot {
      margin-top: 12px;
      color: var(--muted);
      font-size: 0.8rem;
    }

    .error-box {
      margin-top: 10px;
      font-size: 0.85rem;
      color: #fecaca;
      background: rgba(127, 29, 29, 0.35);
      border: 1px solid rgba(248, 113, 113, 0.35);
      border-radius: 10px;
      padding: 10px;
      display: none;
    }

    @media (max-width: 1080px) {
      .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .span-2, .span-4 { grid-column: span 2; }
    }

    @media (max-width: 640px) {
      .grid { grid-template-columns: 1fr; }
      .span-2, .span-4 { grid-column: span 1; }
    }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h1>Polymarket BTC 5m Engine Dashboard</h1>
      <div class="meta">Auto-refresh every 8s</div>
    </div>

    <div class="grid">
      <div class="panel">
        <div class="stat-label">Service Status</div>
        <div id="svc_status" class="status-pill warn">loading</div>
      </div>
      <div class="panel">
        <div class="stat-label">DB</div>
        <div id="db_status" class="status-pill warn">loading</div>
      </div>
      <div class="panel">
        <div class="stat-label">Runtime Mode</div>
        <div id="runtime_mode" class="stat-value">-</div>
      </div>
      <div class="panel">
        <div class="stat-label">Last Observation</div>
        <div id="last_obs" class="stat-value">-</div>
      </div>

      <div class="panel">
        <div class="stat-label">Total Trades</div>
        <div id="total_trades" class="stat-value">0</div>
      </div>
      <div class="panel">
        <div class="stat-label">EV</div>
        <div id="ev" class="stat-value">0</div>
      </div>
      <div class="panel">
        <div class="stat-label">Total Profit</div>
        <div id="profit" class="stat-value">0</div>
      </div>
      <div class="panel">
        <div class="stat-label">Total Loss</div>
        <div id="loss" class="stat-value">0</div>
      </div>

      <div class="panel">
        <div class="stat-label">Rule Accuracy</div>
        <div id="rule_acc" class="stat-value">0%</div>
      </div>
      <div class="panel">
        <div class="stat-label">ML Accuracy</div>
        <div id="ml_acc" class="stat-value">0%</div>
      </div>
      <div class="panel">
        <div class="stat-label">Meta Accuracy</div>
        <div id="meta_acc" class="stat-value">0%</div>
      </div>
      <div class="panel">
        <div class="stat-label">Max Drawdown</div>
        <div id="max_dd" class="stat-value">0</div>
      </div>

      <div class="panel span-2">
        <div class="stat-label">Recent Observations</div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Timestamp</th>
                <th>Price</th>
                <th>Rule</th>
                <th>ML</th>
                <th>Final</th>
                <th>Prob ML</th>
                <th>Resolved</th>
                <th>Outcome</th>
              </tr>
            </thead>
            <tbody id="recent_body"></tbody>
          </table>
        </div>
      </div>

      <div class="panel span-2">
        <div class="stat-label">Bucket Performance</div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Day</th>
                <th>Price Bucket</th>
                <th>Time Bucket</th>
                <th>BTC Gap Bucket</th>
                <th>Count</th>
                <th>Wins</th>
                <th>Losses</th>
                <th>Win Rate</th>
              </tr>
            </thead>
            <tbody id="bucket_body"></tbody>
          </table>
        </div>
      </div>
    </div>

    <div id="error_box" class="error-box"></div>
    <div id="footer" class="foot">Last refresh: never</div>
  </div>

  <script>
    const el = (id) => document.getElementById(id);

    function fmtPct(v) {
      if (v === null || v === undefined || Number.isNaN(v)) return "0%";
      return (Number(v) * 100).toFixed(2) + "%";
    }
    function fmtNum(v) {
      if (v === null || v === undefined || Number.isNaN(Number(v))) return "0";
      return Number(v).toFixed(4);
    }
    function fmtTs(v) {
      if (!v) return "-";
      try { return new Date(v).toLocaleString(); } catch { return String(v); }
    }
    function boolTxt(v) {
      if (v === true) return "true";
      if (v === false) return "false";
      return "-";
    }
    function setPill(id, txt, level) {
      const node = el(id);
      node.textContent = txt;
      node.className = "status-pill " + level;
    }

    async function getJson(path) {
      const res = await fetch(path, { cache: "no-store" });
      if (!res.ok) throw new Error(path + " -> HTTP " + res.status);
      return res.json();
    }

    async function refresh() {
      const errorBox = el("error_box");
      try {
        const [health, stats, comparison, recent, buckets] = await Promise.all([
          getJson("/health"),
          getJson("/stats"),
          getJson("/comparison"),
          getJson("/recent"),
          getJson("/buckets")
        ]);

        setPill("svc_status", health.status || "unknown", health.status === "ok" ? "ok" : "bad");
        setPill("db_status", health.db_ok ? "connected" : "disconnected", health.db_ok ? "ok" : "bad");
        el("runtime_mode").textContent = health.runtime?.mode || "-";
        el("last_obs").textContent = fmtTs(health.runtime?.last_observation_at);

        el("total_trades").textContent = String(stats.total_trades ?? 0);
        el("ev").textContent = fmtNum(stats.ev);
        el("profit").textContent = fmtNum(stats.total_profit);
        el("loss").textContent = fmtNum(stats.total_loss);
        el("max_dd").textContent = fmtNum(stats.max_drawdown);

        el("rule_acc").textContent = fmtPct(comparison.rule_accuracy ?? stats.rule_accuracy);
        el("ml_acc").textContent = fmtPct(comparison.ml_accuracy ?? stats.ml_accuracy);
        el("meta_acc").textContent = fmtPct(stats.meta_accuracy);

        const recentRows = (recent || []).slice(0, 30).map((r) => `
          <tr>
            <td>${fmtTs(r.timestamp)}</td>
            <td>${fmtNum(r.price)}</td>
            <td>${boolTxt(r.decision_rule)}</td>
            <td>${boolTxt(r.decision_ml)}</td>
            <td>${boolTxt(r.final_decision)}</td>
            <td>${fmtNum(r.prob_ml)}</td>
            <td>${boolTxt(r.resolved)}</td>
            <td>${boolTxt(r.outcome)}</td>
          </tr>
        `).join("");
        el("recent_body").innerHTML = recentRows || '<tr><td colspan="8">No observations yet</td></tr>';

        const bucketRows = (buckets || []).slice(0, 40).map((b) => `
          <tr>
            <td>${b.bucket_day || "-"}</td>
            <td>${b.price_bucket || "-"}</td>
            <td>${b.time_bucket || "-"}</td>
            <td>${b.btc_gap_bucket || "-"}</td>
            <td>${b.count ?? 0}</td>
            <td>${b.wins ?? 0}</td>
            <td>${b.losses ?? 0}</td>
            <td>${fmtPct(b.win_rate)}</td>
          </tr>
        `).join("");
        el("bucket_body").innerHTML = bucketRows || '<tr><td colspan="8">No bucket stats yet</td></tr>';

        errorBox.style.display = "none";
      } catch (err) {
        errorBox.textContent = "Dashboard refresh failed: " + err.message;
        errorBox.style.display = "block";
      } finally {
        el("footer").textContent = "Last refresh: " + new Date().toLocaleString();
      }
    }

    refresh();
    setInterval(refresh, 8000);
  </script>
</body>
</html>
"""
