#!/usr/bin/env python3
# net_analytics_collector_v1_2.py
# Collect ping latency to first 3 traceroute hops + dest, and run scheduled speedtests.
# Logs to SQLite + hourly CSVs for long-term analysis.
#
# Changelog v1.2:
# - LOG_DIR default now "./net_analytics_log" (overridable via env).
# - Clarified (and kept) strictly sequential speedtest execution; no parallel runs in this process.
#
# Usage:
#   chmod +x net_analytics_collector_v1_2.py
#   ./net_analytics_collector_v1_2.py
#
# Stop with Ctrl+C. Graceful shutdown is handled.

import os
import sys
import re
import csv
import time
import json
import shlex
import signal
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path

# =====================
# CONFIG (env can override)
# =====================
LOG_DIR = os.environ.get("LOG_DIR", "./net_analytics_log")
DB_PATH = os.environ.get("DB_PATH", os.path.join(LOG_DIR, "net_analytics.db"))

# Destination to traceroute/ping "out to the internet"
DEST_HOST = os.environ.get("DEST_HOST", "8.8.8.8")

# Intervals (seconds)
PING_INTERVAL_SEC = int(os.environ.get("PING_INTERVAL_SEC", "3"))
TRACEROUTE_REFRESH_SEC = int(os.environ.get("TRACEROUTE_REFRESH_SEC", "300"))   # 5 min
SPEEDTEST_INTERVAL_SEC = int(os.environ.get("SPEEDTEST_INTERVAL_SEC", "1800"))  # 30 min

# Speedtest selection
# Choose tool: "ookla", "speedtest-cli", or "auto" (auto picks whichever is available)
SPEEDTEST_TOOL = os.environ.get("SPEEDTEST_TOOL", "auto")

# If you want to pin to specific servers, set IDs here (strings). Otherwise, leave empty and AUTO_SELECT = True.
SPEEDTEST_SERVER_IDS = [s for s in os.environ.get("SPEEDTEST_SERVER_IDS", "").split(",") if s.strip()]

# Auto-select closest servers when SPEEDTEST_SERVER_IDS is empty. (True/False)
AUTO_SELECT_SPEEDTEST_SERVERS = os.environ.get("AUTO_SELECT_SPEEDTEST_SERVERS", "true").lower() in ("1","true","yes")
# How many servers to use if auto-selecting
AUTO_NUM_SERVERS = int(os.environ.get("AUTO_NUM_SERVERS", "2"))

# CSV rollover: separate folder per hour with standardized filenames
CSV_SUBDIR_FORMAT = "%Y%m%d_%H"  # folder like 20250820_11
CSV_PINGS_NAME = "pings.csv"
CSV_TRACES_NAME = "traceroutes.csv"
CSV_SPEEDTESTS_NAME = "speedtests.csv"

# Verbose logging to stdout
VERBOSE = os.environ.get("VERBOSE", "1") == "1"

# =====================
# Helpers
# =====================
_shutdown = False
def _sig_handler(signum, frame):
    global _shutdown
    _log(f"[INFO] Received signal {signum}, shutting down...")
    _shutdown = True

def _log(msg):
    if VERBOSE:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{ts}] {msg}", flush=True)

def _now_iso():
    return datetime.now(timezone.utc).isoformat()

def _epoch_ms():
    return int(time.time() * 1000)

def _ensure_dirs():
    Path(LOG_DIR).mkdir(parents=True, exist_ok=True)

# =====================
# SQLite storage
# =====================
def init_db(path):
    _ensure_dirs()
    conn = sqlite3.connect(path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pings (
            ts_ms INTEGER NOT NULL,
            target TEXT NOT NULL,
            tag TEXT NOT NULL,      -- hop1/hop2/hop3/dest
            rtt_ms REAL,            -- NULL if lost
            success INTEGER NOT NULL CHECK (success IN (0,1))
        );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pings_ts ON pings(ts_ms);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pings_target ON pings(target);")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS traceroutes (
            ts_ms INTEGER NOT NULL,
            dest TEXT NOT NULL,
            hop INTEGER NOT NULL,
            ip TEXT
        );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_traces_ts ON traceroutes(ts_ms);")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS speedtests (
            ts_ms INTEGER NOT NULL,
            tool TEXT NOT NULL,                 -- 'ookla' or 'speedtest-cli'
            server_id TEXT,
            server_name TEXT,
            ping_ms REAL,
            download_mbps REAL,
            upload_mbps REAL,
            jitter_ms REAL
        );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_speed_ts ON speedtests(ts_ms);")
    conn.commit()
    return conn

# =====================
# CSV writers per-hour
# =====================
class HourlyCSV:
    def __init__(self, base_dir):
        self.base_dir = Path(base_dir)
        self.current_hour_dir = None
        self.files = {}

    def _hour_dir(self, dt=None):
        if dt is None:
            dt = datetime.now()
        sub = dt.strftime(CSV_SUBDIR_FORMAT)
        return self.base_dir / sub

    def _ensure_open(self, name, header):
        hour_dir = self._hour_dir()
        if self.current_hour_dir != hour_dir:
            self._rotate()
            hour_dir.mkdir(parents=True, exist_ok=True)
            self.current_hour_dir = hour_dir
            self.files = {}

        if name not in self.files:
            fpath = hour_dir / name
            f = open(fpath, "a", newline="")
            writer = csv.writer(f)
            if f.tell() == 0:
                writer.writerow(header)
            self.files[name] = (f, writer)

        return self.files[name][1]

    def write_ping(self, ts_ms, target, tag, rtt_ms, success):
        writer = self._ensure_open(CSV_PINGS_NAME, ["ts_ms", "target", "tag", "rtt_ms", "success"])
        writer.writerow([ts_ms, target, tag, "" if rtt_ms is None else rtt_ms, success])

    def write_trace(self, ts_ms, dest, hop, ip):
        writer = self._ensure_open(CSV_TRACES_NAME, ["ts_ms", "dest", "hop", "ip"])
        writer.writerow([ts_ms, dest, hop, ip or ""])

    def write_speedtest(self, ts_ms, tool, server_id, server_name, ping_ms, download_mbps, upload_mbps, jitter_ms):
        writer = self._ensure_open(CSV_SPEEDTESTS_NAME, ["ts_ms","tool","server_id","server_name","ping_ms","download_mbps","upload_mbps","jitter_ms"])
        writer.writerow([ts_ms, tool, server_id or "", server_name or "", ping_ms or "", download_mbps or "", upload_mbps or "", jitter_ms or ""])

    def _rotate(self):
        for f, _ in self.files.values():
            try:
                f.flush()
                f.close()
            except Exception:
                pass
        self.files = {}

    def close(self):
        self._rotate()

# =====================
# Command helpers
# =====================
def which(cmd):
    try:
        out = subprocess.run(["which", cmd], capture_output=True, text=True)
        return out.returncode == 0
    except Exception:
        return False

def run_cmd(cmd, timeout_sec=30):
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_sec)
        return res.returncode, res.stdout, res.stderr
    except subprocess.TimeoutExpired:
        return 124, "", "timeout"

# =====================
# Traceroute
# =====================
IP_RE = re.compile(r"(\d{1,3}(?:\.\d{1,3}){3})")

def do_traceroute(dest):
    if which("traceroute"):
        cmd = ["traceroute", "-n", "-w", "2", "-q", "1", dest]
        _log(f"[TRACE] {' '.join(cmd)}")
        rc, out, err = run_cmd(cmd, timeout_sec=30)
        hops = []
        if rc == 0 and out:
            for line in out.splitlines()[1:]:
                m = IP_RE.search(line)
                if m:
                    hops.append(m.group(1))
                if len(hops) >= 3:
                    break
        return hops

    elif which("mtr"):
        cmd = ["mtr", "-n", "-r", "-c", "1", dest]
        _log(f"[TRACE] {' '.join(cmd)}")
        rc, out, err = run_cmd(cmd, timeout_sec=30)
        hops = []
        if rc == 0 and out:
            for line in out.splitlines():
                m = IP_RE.search(line)
                if m:
                    hops.append(m.group(1))
                if len(hops) >= 3:
                    break
        return hops

    else:
        _log("[WARN] Neither 'traceroute' nor 'mtr' found. Install one of them.")
        return []

# =====================
# Ping
# =====================
def do_ping(ip):
    cmd = ["ping", "-n", "-c", "1", "-W", "1", ip]
    rc, out, err = run_cmd(cmd, timeout_sec=5)
    if rc == 0 and "time=" in out:
        m = re.search(r"time[=<]\s*([\d\.]+)\s*ms", out)
        if m:
            return True, float(m.group(1))
    return False, None

# =====================
# Speedtest
# =====================
def which_speedtest_tool():
    tool = SPEEDTEST_TOOL
    if tool == "auto":
        if which("speedtest"):
            return "ookla"
        elif which("speedtest-cli"):
            return "speedtest-cli"
        else:
            return None
    elif tool == "ookla":
        return "ookla" if which("speedtest") else None
    elif tool == "speedtest-cli":
        return "speedtest-cli" if which("speedtest-cli") else None
    return None

def auto_select_servers(tool, count):
    servers = []
    if tool == "ookla":
        rc, out, err = run_cmd(["speedtest", "-L"], timeout_sec=20)
        if rc == 0:
            for line in out.splitlines():
                m = re.match(r"\s*(\d+)\)\s*(.+)", line.strip())
                if m:
                    servers.append({"id": m.group(1), "name": m.group(2)})
                    if len(servers) >= count:
                        break
    elif tool == "speedtest-cli":
        rc, out, err = run_cmd(["speedtest-cli", "--list"], timeout_sec=25)
        if rc == 0:
            for line in out.splitlines()[:200]:
                m = re.match(r"\s*(\d+)\)\s*(.+)", line.strip())
                if m:
                    servers.append({"id": m.group(1), "name": m.group(2)})
                    if len(servers) >= count:
                        break
    return servers

def run_speedtest(tool, server_id=None):
    # NOTE: Called synchronously; this process never runs speedtests in parallel.
    if tool == "ookla":
        base = ["speedtest", "--format=json"]
        if server_id:
            base += ["--server-id", str(server_id)]
        rc, out, err = run_cmd(base, timeout_sec=120)
        if rc == 0 and out.strip():
            try:
                data = json.loads(out)
                ping_ms = (data.get("ping") or {}).get("latency")
                jitter_ms = (data.get("ping") or {}).get("jitter")
                down_bps = (data.get("download") or {}).get("bandwidth")
                up_bps = (data.get("upload") or {}).get("bandwidth")
                server_name = ((data.get("server") or {}).get("name") or "") + " - " + ((data.get("server") or {}).get("location") or "")
                download_mbps = (down_bps * 8 / 1e6) if down_bps else None
                upload_mbps = (up_bps * 8 / 1e6) if up_bps else None
                return {
                    "tool": "ookla",
                    "server_id": str((data.get("server") or {}).get("id") or server_id or ""),
                    "server_name": server_name.strip(" -"),
                    "ping_ms": ping_ms,
                    "jitter_ms": jitter_ms,
                    "download_mbps": download_mbps,
                    "upload_mbps": upload_mbps,
                }
            except Exception as e:
                return {"error": f"json_parse_error: {e}"}
        return {"error": f"rc={rc} err={err.strip()}"}

    elif tool == "speedtest-cli":
        base = ["speedtest-cli", "--json"]
        if server_id:
            base += ["--server", str(server_id)]
        rc, out, err = run_cmd(base, timeout_sec=180)
        if rc == 0 and out.strip():
            try:
                data = json.loads(out)
                ping_ms = data.get("ping")
                download_mbps = (data.get("download") or 0) / 1e6
                upload_mbps = (data.get("upload") or 0) / 1e6
                server = data.get("server") or {}
                server_id = str(server.get("id") or server_id or "")
                server_name = f"{server.get('sponsor','')} - {server.get('name','')}".strip(" -")
                return {
                    "tool": "speedtest-cli",
                    "server_id": server_id,
                    "server_name": server_name,
                    "ping_ms": ping_ms,
                    "jitter_ms": None,
                    "download_mbps": download_mbps,
                    "upload_mbps": upload_mbps,
                }
            except Exception as e:
                return {"error": f"json_parse_error: {e}"}
        return {"error": f"rc={rc} err={err.strip()}"}

    else:
        return {"error": "No speedtest tool available"}

# =====================
# Main loop
# =====================
def main():
    global _shutdown
    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    _ensure_dirs()

    conn = init_db(DB_PATH)
    cur = conn.cursor()
    csvw = HourlyCSV(LOG_DIR)

    last_trace_ts = 0
    last_speedtest_ts = 0
    hop_ips = []

    tool = which_speedtest_tool()
    if not tool:
        _log("[WARN] No speedtest tool found in PATH ('speedtest' or 'speedtest-cli'). Speedtests will be skipped.")

    servers = []
    if tool and SPEEDTEST_SERVER_IDS:
        servers = [{"id": sid.strip(), "name": ""} for sid in SPEEDTEST_SERVER_IDS]
    elif tool and AUTO_SELECT_SPEEDTEST_SERVERS:
        servers = auto_select_servers(tool, AUTO_NUM_SERVERS)
        if not servers:
            _log("[WARN] Could not auto-select speedtest servers; will use tool's default when running.")

    _log(f"[START] Logging to DB: {DB_PATH}  CSV dir: {LOG_DIR}")
    _log(f"[CONFIG] DEST_HOST={DEST_HOST} PING_INTERVAL_SEC={PING_INTERVAL_SEC} TRACEROUTE_REFRESH_SEC={TRACEROUTE_REFRESH_SEC} SPEEDTEST_INTERVAL_SEC={SPEEDTEST_INTERVAL_SEC}")
    if tool:
        _log(f"[CONFIG] SPEEDTEST_TOOL={tool} servers={','.join([s['id'] for s in servers]) or '(default)'}")
        _log("[INFO] Speedtests execute sequentially in this process; no parallel runs.")

    while not _shutdown:
        now = time.time()

        if now - last_trace_ts >= TRACEROUTE_REFRESH_SEC or not hop_ips:
            hops = do_traceroute(DEST_HOST)
            ts_ms = _epoch_ms()
            hop_ips = hops[:3]
            _log(f"[TRACE] Hops: {hop_ips or '(none)'}")
            for i, ip in enumerate(hop_ips, start=1):
                cur.execute("INSERT INTO traceroutes (ts_ms, dest, hop, ip) VALUES (?, ?, ?, ?)", (ts_ms, str(DEST_HOST), i, ip))
                csvw.write_trace(ts_ms, str(DEST_HOST), i, ip)
            conn.commit()
            last_trace_ts = now

        targets = []
        for idx, ip in enumerate(hop_ips, start=1):
            targets.append((ip, f"hop{idx}"))
        targets.append((DEST_HOST, "dest"))

        for ip, tag in targets:
            ts_ms = _epoch_ms()
            ok, rtt = do_ping(ip)
            cur.execute("INSERT INTO pings (ts_ms, target, tag, rtt_ms, success) VALUES (?, ?, ?, ?, ?)", (ts_ms, ip, tag, rtt if ok else None, 1 if ok else 0))
            csvw.write_ping(ts_ms, ip, tag, rtt if ok else None, 1 if ok else 0)
            if ok:
                _log(f"[PING] {tag} {ip} {rtt:.2f} ms")
            else:
                _log(f"[PING] {tag} {ip} LOST")

            if _shutdown:
                break

        conn.commit()

        # Speedtests: strictly sequential. If multiple servers configured, run them one-by-one.
        if tool and (now - last_speedtest_ts >= SPEEDTEST_INTERVAL_SEC):
            if servers:
                for s in servers:
                    ts_ms = _epoch_ms()
                    res = run_speedtest(tool, s.get("id"))
                    if "error" in res:
                        _log(f"[SPEEDTEST] error with server {s.get('id')}: {res['error']}")
                    else:
                        _log(f"[SPEEDTEST] {res['server_id']} {res['server_name']} down={res['download_mbps']:.2f} Mbps up={res['upload_mbps']:.2f} Mbps ping={res['ping_ms']:.1f} ms")
                        cur.execute("""INSERT INTO speedtests (ts_ms, tool, server_id, server_name, ping_ms, download_mbps, upload_mbps, jitter_ms)
                                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                                    (ts_ms, res.get("tool"), res.get("server_id"), res.get("server_name"), res.get("ping_ms"), res.get("download_mbps"), res.get("upload_mbps"), res.get("jitter_ms")))
                        csvw.write_speedtest(ts_ms, res.get("tool"), res.get("server_id"), res.get("server_name"), res.get("ping_ms"), res.get("download_mbps"), res.get("upload_mbps"), res.get("jitter_ms"))
                    conn.commit()
                    if _shutdown:
                        break
            else:
                ts_ms = _epoch_ms()
                res = run_speedtest(tool, None)
                if "error" in res:
                    _log(f"[SPEEDTEST] error: {res['error']}")
                else:
                    _log(f"[SPEEDTEST] {res['server_id']} {res['server_name']} down={res['download_mbps']:.2f} Mbps up={res['upload_mbps']:.2f} Mbps ping={res['ping_ms']:.1f} ms")
                    cur.execute("""INSERT INTO speedtests (ts_ms, tool, server_id, server_name, ping_ms, download_mbps, upload_mbps, jitter_ms)
                                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                                (ts_ms, res.get("tool"), res.get("server_id"), res.get("server_name"), res.get("ping_ms"), res.get("download_mbps"), res.get("upload_mbps"), res.get("jitter_ms")))
                    csvw.write_speedtest(ts_ms, res.get("tool"), res.get("server_id"), res.get("server_name"), res.get("ping_ms"), res.get("download_mbps"), res.get("upload_mbps"), res.get("jitter_ms"))
                conn.commit()
            last_speedtest_ts = now

        for _ in range(PING_INTERVAL_SEC * 10):
            if _shutdown:
                break
            time.sleep(0.1)

    try:
        conn.commit()
        conn.close()
    except Exception:
        pass
    csvw.close()
    _log("[EXIT] Collector stopped.")

if __name__ == "__main__":
    main()
