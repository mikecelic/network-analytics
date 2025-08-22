#!/usr/bin/env python3
# net_analytics_webui_standalone_v1_5.py
#
# Changes vs v1.4:
# - Remembers hop toggle selections across auto-refresh & reload (localStorage).
# - Remembers which speedtest series are hidden via legend toggles (localStorage).
# - Default auto-refresh is now **5 minutes** (300s). Hard minimum still 60s.
# - Adds server-side **ping bucketing** to massively reduce payload for long ranges.
#   Frontend automatically requests a bucket size based on the selected hours.
#
# Endpoints:
#   GET /api/pings?start=<ms>&end=<ms>&bucket_ms=<int>   (if bucket_ms>0, returns aggregated rows)
#       Raw rows:      [ts_ms, target, tag, rtt_ms, success]
#       Aggregated:    [bucket_ts_ms,   tag, avg_rtt_ms, success_count, total_count]
#   GET /api/speedtests?start=<ms>&end=<ms>
#   GET /api/latest_traceroute
#
# Run:
#   HOST=0.0.0.0 PORT=8088 LOG_DIR=/path DEFAULT_WINDOW_HOURS=24 \
#   python3 net_analytics_webui_standalone_v1_5.py

import os
import json
import sqlite3
from urllib.parse import urlparse, parse_qs
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timezone

LOG_DIR = os.environ.get("LOG_DIR", "./net_analytics_log")
DB_PATH = os.environ.get("DB_PATH", os.path.join(LOG_DIR, "net_analytics.db"))
HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "8088"))
DEFAULT_WINDOW_HOURS = int(os.environ.get("DEFAULT_WINDOW_HOURS", "24"))

def get_conn():
    if not os.path.exists(DB_PATH):
        return None
    return sqlite3.connect(DB_PATH, timeout=15)

def query_pings_raw(start_ms, end_ms):
    conn = get_conn()
    if conn is None:
        return []
    cur = conn.cursor()
    cur.execute("""
        SELECT ts_ms, target, tag, rtt_ms, success
        FROM pings
        WHERE ts_ms BETWEEN ? AND ?
        ORDER BY ts_ms ASC
    """, (start_ms, end_ms))
    rows = cur.fetchall()
    conn.close()
    return rows

def query_pings_bucketed(start_ms, end_ms, bucket_ms):
    conn = get_conn()
    if conn is None:
        return []
    cur = conn.cursor()
    # bucket = floor(ts_ms / bucket_ms) * bucket_ms
    # avg RTT over successful pings only, plus success & total counts for loss/weighting
    cur.execute(f"""
        SELECT ((ts_ms / ?) * ?) AS bucket_ts,
               tag,
               AVG(CASE WHEN success=1 THEN rtt_ms END) AS avg_rtt,
               SUM(success) AS success_count,
               COUNT(*) AS total_count
        FROM pings
        WHERE ts_ms BETWEEN ? AND ?
        GROUP BY bucket_ts, tag
        ORDER BY bucket_ts ASC
    """, (bucket_ms, bucket_ms, start_ms, end_ms))
    rows = cur.fetchall()
    conn.close()
    return rows

def query_speedtests(start_ms, end_ms):
    conn = get_conn()
    if conn is None:
        return []
    cur = conn.cursor()
    cur.execute("""
        SELECT ts_ms, tool, server_id, server_name, ping_ms, download_mbps, upload_mbps, jitter_ms
        FROM speedtests
        WHERE ts_ms BETWEEN ? AND ?
        ORDER BY ts_ms ASC
    """, (start_ms, end_ms))
    rows = cur.fetchall()
    conn.close()
    return rows

def query_last_traces():
    conn = get_conn()
    if conn is None:
        return []
    cur = conn.cursor()
    cur.execute("SELECT MAX(ts_ms) FROM traceroutes")
    row = cur.fetchone()
    if not row or row[0] is None:
        conn.close()
        return []
    max_ts = row[0]
    cur.execute("""
        SELECT ts_ms, dest, hop, ip
        FROM traceroutes
        WHERE ts_ms = ?
        ORDER BY hop ASC
    """, (max_ts,))
    rows = cur.fetchall()
    conn.close()
    return rows

INDEX_HTML = r"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Net Analytics Dashboard</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns"></script>
  <style>
    :root {
      color-scheme: light dark;
      --card: #ffffff;
      --text: #111827;
      --muted: #6b7280;
      --border: #e5e7eb;
      --bg: #f9fafb;
    }
    @media (prefers-color-scheme: dark) {
      :root {
        --card: #111827;
        --text: #e5e7eb;
        --muted: #9ca3af;
        --border: #1f2937;
        --bg: #0b0f17;
      }
    }
    * { box-sizing: border-box; }
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 20px; background: var(--bg); color: var(--text); }
    .container { max-width: 1100px; margin: 0 auto; }
    .grid { display: grid; grid-template-columns: 1fr; gap: 20px; }
    .card { background: var(--card); border: 1px solid var(--border); border-radius: 16px; padding: 16px; box-shadow: 0 1px 2px rgba(0,0,0,0.08); }
    .row { display: flex; gap: 12px; align-items: center; flex-wrap: wrap; }
    .controls input { padding: 6px 8px; border: 1px solid var(--border); border-radius: 8px; background: transparent; color: var(--text); }
    .controls button { padding: 6px 10px; border: 1px solid var(--border); border-radius: 8px; background: transparent; color: var(--text); cursor: pointer; }
    h1 { margin: 0 0 16px 0; }
    h2 { margin: 0 0 8px 0; font-size: 18px; }
    .muted { color: var(--muted); font-size: 12px; }
    .tiles { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; margin: 12px 0 20px; }
    .tile { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 12px; }
    .tile .big { font-size: 22px; font-weight: 600; }
    .chart-wrap { height: 240px; position: relative; }
    canvas.chart { width: 100% !important; height: 100% !important; display: block; }
    .toggles { display: flex; flex-wrap: wrap; gap: 12px; margin: 8px 0 4px; }
    .toggles label { display: inline-flex; align-items: center; gap: 6px; font-size: 12px; color: var(--muted); }
  </style>
</head>
<body>
<div class="container">
  <h1>Network Analytics</h1>
  <div class="row controls">
    <label>Time window (hours): <input id="hours" type="number" value="{{HOURS}}" min="1" max="1440"></label>
    <label>Auto-refresh (s): <input id="refresh" type="number" value="300" min="60" max="86400" step="30"></label>
    <button onclick="applyAndReload()">Apply</button>
  </div>

  <div class="tiles">
    <div class="tile">
      <div class="muted">Avg dest latency</div>
      <div id="avgDest" class="big">—</div>
    </div>
    <div class="tile">
      <div class="muted">Dest packet loss</div>
      <div id="lossDest" class="big">—</div>
    </div>
    <div class="tile">
      <div class="muted">Last speedtest</div>
      <div id="lastSpeed" class="big">—</div>
      <div id="avgSpeedWindows" class="muted" style="margin-top:6px;">15m: — / — • 1h: — / — • 24h: — / —</div>
    </div>
  </div>

  <div class="grid">
    <div class="card">
      <h2>Latency (ms): hops &amp; destination</h2>
      <div class="muted">hop1, hop2, hop3, and dest ({{DEST}}). Failed pings omitted.</div>
      <div class="toggles">
        <label><input type="checkbox" id="tHop1"> hop1</label>
        <label><input type="checkbox" id="tHop2"> hop2</label>
        <label><input type="checkbox" id="tHop3"> hop3</label>
        <label><input type="checkbox" id="tDest"> dest</label>
      </div>
      <div class="chart-wrap"><canvas id="pingChart" class="chart"></canvas></div>
    </div>

    <div class="card">
      <h2>Speedtests (Mbps)</h2>
      <div class="muted">Download/Upload per server.</div>
      <div class="chart-wrap"><canvas id="speedChart" class="chart"></canvas></div>
    </div>

    <div class="card">
      <h2>Latest Traceroute</h2>
      <div id="traceBox" class="muted">loading...</div>
    </div>
  </div>
</div>

<script>
let pingChart, speedChart;
const MAX_POINTS_PER_SERIES = 3000; // client-side thinning
const HOP_KEY = 'netdash_hop_toggles_v1';
const SPEED_HIDDEN_KEY = 'netdash_speed_hidden_v1';

function msAgo(hours) { return Date.now() - (hours*3600*1000); }

// ---- Storage helpers ----
function saveHopToggles(val){ localStorage.setItem(HOP_KEY, JSON.stringify(val)); }
function loadHopToggles(){
  const d = {hop1:true, hop2:true, hop3:true, dest:true};
  try { return Object.assign(d, JSON.parse(localStorage.getItem(HOP_KEY)||'{}')); } catch { return d; }
}
function saveSpeedHidden(map){ localStorage.setItem(SPEED_HIDDEN_KEY, JSON.stringify(map)); }
function loadSpeedHidden(){
  try { return JSON.parse(localStorage.getItem(SPEED_HIDDEN_KEY) || '{}'); } catch { return {}; }
}

// pick a bucket size in milliseconds based on hours selected
function pickBucketMs(hours){
  if (hours <= 6) return 15000;      // 15s
  if (hours <= 24) return 30000;     // 30s
  if (hours <= 72) return 60000;     // 60s
  if (hours <= 168) return 120000;   // 2m
  if (hours <= 504) return 300000;   // 5m
  return 600000;                      // 10m for multi-week views
}

async function fetchJSON(url) {
  const r = await fetch(url);
  if (!r.ok) return [];
  return r.json();
}

function thin(points, maxPoints) {
  if (!Array.isArray(points) || points.length <= maxPoints) return points;
  const stride = Math.ceil(points.length / maxPoints);
  const out = [];
  for (let i = 0; i < points.length; i += stride) out.push(points[i]);
  return out;
}

// Build series from bucketed rows: [bucket_ts, tag, avg_rtt, success_count, total_count]
function seriesFromBucketed(rows) {
  const s = {hop1: [], hop2: [], hop3: [], dest: []};
  rows.forEach(r => {
    const ts = r[0], tag = r[1], avg = r[2];
    if (avg != null && s[tag]) s[tag].push({x: ts, y: avg});
  });
  for (const k of Object.keys(s)) s[k] = thin(s[k], MAX_POINTS_PER_SERIES);
  return s;
}

function destStatsFromBucketed(rows){
  const dest = rows.filter(r => r[1] === 'dest');
  let succ = 0, total = 0, wsum = 0;
  dest.forEach(r => {
    const avg = r[2], sc = r[3]||0, tc = r[4]||0;
    succ += sc; total += tc;
    if (avg != null && sc>0) wsum += avg * sc;
  });
  const lossPct = total ? (100*(total - succ)/total) : 0;
  const avgrtt = succ ? (wsum / succ) : null;
  return {avg: avgrtt, lossPct};
}

function buildLineDataset(label, data, hidden=false) {
  return { label, data, hidden, parsing: false, borderWidth: 1, pointRadius: 0, tension: 0.2 };
}

async function loadPings(hours) {
  const start = msAgo(hours);
  const bucket = pickBucketMs(hours);
  const rows = await fetchJSON(`/api/pings?start=${start}&end=${Date.now()}&bucket_ms=${bucket}`);

  const hopToggles = loadHopToggles();
  // set checkbox UI to stored state (once at start or if changed externally)
  document.getElementById('tHop1').checked = hopToggles.hop1;
  document.getElementById('tHop2').checked = hopToggles.hop2;
  document.getElementById('tHop3').checked = hopToggles.hop3;
  document.getElementById('tDest').checked = hopToggles.dest;

  const s = seriesFromBucketed(rows);
  const cfg = {
    type: 'line',
    data: {
      datasets: [
        buildLineDataset('hop1', s.hop1, !hopToggles.hop1),
        buildLineDataset('hop2', s.hop2, !hopToggles.hop2),
        buildLineDataset('hop3', s.hop3, !hopToggles.hop3),
        buildLineDataset('dest', s.dest, !hopToggles.dest),
      ]
    },
    options: {
      scales: {
        x: { type: 'time', time: { tooltipFormat: 'Pp' } },
        y: { title: { display: true, text: 'ms' } }
      },
      plugins: {
        legend: { display: true, position: 'bottom' },
        decimation: { enabled: true, algorithm: 'lttb', threshold: 2500 }
      },
      interaction: { mode: 'nearest', intersect: false },
      maintainAspectRatio: false,
      responsive: true
    }
  };
  if (pingChart) pingChart.destroy();
  pingChart = new Chart(document.getElementById('pingChart').getContext('2d'), cfg);

  const stats = destStatsFromBucketed(rows);
  document.getElementById('avgDest').textContent = (stats.avg!=null)? stats.avg.toFixed(1)+' ms' : '—';
  document.getElementById('lossDest').textContent = stats.lossPct.toFixed(1)+' %';
}

// ---- Speedtests per server with persistent legend visibility ----
function groupByServer(rows) {
  const map = new Map(); // server_id => {name, down:[{x,y}], up:[{x,y}]}
  rows.forEach(r => {
    const ts = r[0];
    const sid = (r[2] || "unknown").toString();
    const sname = (r[3] || "unknown").toString();
    const down = r[5]; const up = r[6];
    if (!map.has(sid)) map.set(sid, { name: sname, down: [], up: [] });
    if (typeof down === 'number') map.get(sid).down.push({x: ts, y: down});
    if (typeof up === 'number')   map.get(sid).up.push({x: ts, y: up});
  });
  for (const v of map.values()) {
    v.down = thin(v.down, MAX_POINTS_PER_SERIES);
    v.up   = thin(v.up,   MAX_POINTS_PER_SERIES);
  }
  return map;
}

async function loadSpeed(hours) {
  const start = msAgo(hours);
  const rows = await fetchJSON(`/api/speedtests?start=${start}&end=${Date.now()}`);
  const perServer = groupByServer(rows);

  const hiddenMap = loadSpeedHidden();
  const datasets = [];
  perServer.forEach((v, sid) => {
    const labelDown = `${v.name || sid} (${sid}) ↓`;
    const labelUp   = `${v.name || sid} (${sid}) ↑`;
    datasets.push(buildLineDataset(labelDown, v.down, !!hiddenMap[labelDown]));
    datasets.push(buildLineDataset(labelUp,   v.up,   !!hiddenMap[labelUp]));
  });

  const cfg = {
    type: 'line',
    data: { datasets },
    options: {
      scales: {
        x: { type: 'time', time: { tooltipFormat: 'Pp' } },
        y: { title: { display: true, text: 'Mbps' } }
      },
      plugins: {
        legend: {
          display: true,
          position: 'bottom',
          onClick: (e, legendItem, legend) => {
            const index = legendItem.datasetIndex;
            const ci = legend.chart;
            const ds = ci.data.datasets[index];
            ds.hidden = !ds.hidden;
            const map = loadSpeedHidden();
            map[ds.label] = ds.hidden;
            saveSpeedHidden(map);
            ci.update();
          }
        },
        decimation: { enabled: true, algorithm: 'lttb', threshold: 2500 },
        tooltip: { callbacks: { afterTitle: items => {
          if (!items?.length) return '';
          const ts = items[0].raw.x;
          const row = rows.find(r => r[0] === ts) || null;
          return row ? ` ${row[2]} ${row[3]}` : '';
        } } }
      },
      interaction: { mode: 'nearest', intersect: false },
      maintainAspectRatio: false,
      responsive: true
    }
  };
  if (speedChart) speedChart.destroy();
  speedChart = new Chart(document.getElementById('speedChart').getContext('2d'), cfg);

  if (rows.length) {
    const last = rows[rows.length-1];
    document.getElementById('lastSpeed').textContent = `${last[5]?.toFixed(1) ?? '—'}↓ / ${last[6]?.toFixed(1) ?? '—'}↑ Mbps`;
    const w15  = avgWindow(rows, 15*60*1000);
    const w1h  = avgWindow(rows, 60*60*1000);
    const w24h = avgWindow(rows, 24*60*60*1000);
    const fmt = v => (v==null ? '—' : v.toFixed(1));
    document.getElementById('avgSpeedWindows').textContent =
      `15m: ${fmt(w15.down)}↓ / ${fmt(w15.up)}↑ • 1h: ${fmt(w1h.down)}↓ / ${fmt(w1h.up)}↑ • 24h: ${fmt(w24h.down)}↓ / ${fmt(w24h.up)}↑`;
  } else {
    document.getElementById('lastSpeed').textContent = '—';
    document.getElementById('avgSpeedWindows').textContent = '15m: — / — • 1h: — / — • 24h: — / —';
  }
}

function avgWindow(rows, windowMs) {
  const cutoff = Date.now() - windowMs;
  const win = rows.filter(r => r[0] >= cutoff);
  const downs = win.map(r => r[5]).filter(v => typeof v === 'number');
  const ups   = win.map(r => r[6]).filter(v => typeof v === 'number');
  const avg = arr => arr.length ? (arr.reduce((a,b)=>a+b,0)/arr.length) : null;
  return {down: avg(downs), up: avg(ups)};
}

async function loadTrace() {
  const rows = await fetchJSON('/api/latest_traceroute');
  const box = document.getElementById('traceBox');
  if (!rows.length) { box.textContent = 'No traceroute data found.'; return; }
  const ts = rows[0][0];
  let html = '<div><b>' + new Date(ts).toLocaleString() + '</b></div><ol>';
  rows.forEach(r => { html += `<li>${r[3] || '*'}</li>`; });
  html += '</ol>';
  box.innerHTML = html;
}

async function reloadAll() {
  const hrs = Math.max(1, Number(document.getElementById('hours').value || 24));
  await Promise.all([loadPings(hrs), loadSpeed(hrs), loadTrace()]);
}

let _loopTimer = null;
function scheduleLoop() {
  if (_loopTimer) clearTimeout(_loopTimer);
  const sec = Math.max(60, Number(document.getElementById('refresh').value || 300));
  _loopTimer = setTimeout(loop, sec * 1000);
}

function applyAndReload() {
  reloadAll();
  scheduleLoop();
}

async function loop() {
  await reloadAll();
  scheduleLoop();
}

document.addEventListener('DOMContentLoaded', () => {
  // initialize hop toggles from storage
  const hops = loadHopToggles();
  document.getElementById('tHop1').checked = hops.hop1;
  document.getElementById('tHop2').checked = hops.hop2;
  document.getElementById('tHop3').checked = hops.hop3;
  document.getElementById('tDest').checked = hops.dest;
  ['tHop1','tHop2','tHop3','tDest'].forEach(id => {
    document.getElementById(id).addEventListener('change', () => {
      const val = {
        hop1: document.getElementById('tHop1').checked,
        hop2: document.getElementById('tHop2').checked,
        hop3: document.getElementById('tHop3').checked,
        dest: document.getElementById('tDest').checked,
      };
      saveHopToggles(val);
      loadPings(Math.max(1, Number(document.getElementById('hours').value || 24)));
    });
  });

  scheduleLoop();
});
</script>
</body>
</html>
""".replace("{{HOURS}}", str(DEFAULT_WINDOW_HOURS)).replace("{{DEST}}", os.environ.get("DEST_HOST", "8.8.8.8"))

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path in ("/", "/index.html"):
            self._send_html(INDEX_HTML); return

        if parsed.path == "/api/pings":
            qs = parse_qs(parsed.query or "")
            start = int(qs.get("start", [0])[0])
            end = int(qs.get("end", [int(datetime.now(tz=timezone.utc).timestamp()*1000)])[0])
            bucket_ms = int(qs.get("bucket_ms", [0])[0])
            if bucket_ms and bucket_ms > 0:
                self._send_json(query_pings_bucketed(start, end, bucket_ms)); return
            else:
                self._send_json(query_pings_raw(start, end)); return

        if parsed.path == "/api/speedtests":
            qs = parse_qs(parsed.query or "")
            start = int(qs.get("start", [0])[0])
            end = int(qs.get("end", [int(datetime.now(tz=timezone.utc).timestamp()*1000)])[0])
            self._send_json(query_speedtests(start, end)); return

        if parsed.path == "/api/latest_traceroute":
            self._send_json(query_last_traces()); return

        self.send_response(404); self.end_headers(); self.wfile.write(b"not found")

    def _send_html(self, html):
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(html.encode("utf-8"))

    def _send_json(self, obj):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(obj).encode("utf-8"))

def main():
    print(f"[INFO] Serving dashboard on http://{HOST}:{PORT}  (DB={DB_PATH})")
    httpd = HTTPServer((HOST, PORT), Handler)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    print("[INFO] Stopped.")

if __name__ == "__main__":
    main()
