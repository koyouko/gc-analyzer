"""
SQLite time-series store for fleet GC + host (SAR) metrics.

Tables:
  instances    : the component inventory (one row per JVM).
  metrics      : hourly rollups per instance — the GC history that powers
                 30-day trends, "right now" health, and "last hour" alerting.
  host_metrics : hourly rollups of OS-level activity (CPU/mem/swap/disk/net)
                 for the *host* each instance runs on — same instance_id, same
                 time grid, so it joins trivially against `metrics` for
                 GC<->host correlation (see correlate.py) and cluster-wide
                 scaling analysis (see scaling_advisor.py).

In production, a periodic collection job parses each node's GC log and calls
`record_metric()` once per interval, and (independently) parses sar/sadf
output and calls `record_host_metric()`; the dashboard reads aggregates back
out. For the demo, seed/seed_history.py and seed/seed_sar_history.py populate
30 days of correlated rows.

Health and tuning advice reuse gcanalyzer.analyzer / gcanalyzer.sar_analyzer so
a node is judged the same way whether the numbers come from a freshly parsed
log/report or from the store.
"""

from __future__ import annotations

import json
import os
import sqlite3
import statistics
import time
from contextlib import contextmanager

from . import analyzer, sar_analyzer

DB_PATH = os.environ.get(
    "GC_DB", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "gc_history.db")
)

# Last-hour alert thresholds — defaults from analyzer SLOs; override via env.
def _alert_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw not in (None, "") else float(default)


PAUSE_ALERT_MS = _alert_env("GC_ALERT_PAUSE_MS", analyzer.ALERT_PAUSE_CRITICAL_MS)
P99_PAUSE_ALERT_MS = _alert_env("GC_ALERT_P99_MS", analyzer.ALERT_P99_WARNING_MS)
HEAP_ALERT_PCT = _alert_env("GC_ALERT_HEAP_PCT", analyzer.ALERT_HEAP_WARNING_PCT)
STORM_TIME_IN_GC_PCT = _alert_env("GC_ALERT_STORM_TIG_PCT", analyzer.ALERT_STORM_TIME_IN_GC_PCT)
STORM_BASELINE_MULT = _alert_env("GC_ALERT_STORM_MULT", analyzer.ALERT_STORM_BASELINE_MULT)
GC_FREQ_ALERT_MIN = _alert_env("GC_ALERT_GC_FREQ_MIN", analyzer.ALERT_GC_FREQ_MIN)
GC_FREQ_BASELINE_MULT = _alert_env("GC_ALERT_GC_FREQ_MULT", analyzer.ALERT_GC_FREQ_BASELINE_MULT)
RECENT_WINDOW_S = int(_alert_env("GC_ALERT_WINDOW_S", analyzer.ALERT_RECENT_WINDOW_S))


SCHEMA = """
CREATE TABLE IF NOT EXISTS instances (
    id          TEXT PRIMARY KEY,
    region      TEXT, env TEXT, cluster TEXT,
    grp         TEXT, role TEXT, idx INTEGER,
    heap_max_mb INTEGER, collector TEXT,
    node_id TEXT
);
CREATE TABLE IF NOT EXISTS metrics (
    ts             INTEGER,
    instance_id    TEXT,
    heap_used_mb   REAL,   -- avg post-GC live set in the interval
    heap_max_mb    REAL,
    heap_after_pct REAL,
    pause_avg_ms   REAL,
    pause_p99_ms   REAL,
    pause_max_ms   REAL,
    full_gc_count  INTEGER,
    young_count    INTEGER,
    gc_per_min     REAL,
    time_in_gc_pct REAL,
    throughput_pct REAL,
    PRIMARY KEY (instance_id, ts)
);
CREATE INDEX IF NOT EXISTS idx_metrics_inst_ts ON metrics(instance_id, ts);
CREATE TABLE IF NOT EXISTS collector_state (
    instance_id TEXT,
    file_path   TEXT,
    inode       INTEGER,
    offset      INTEGER,
    updated_ts  INTEGER,
    PRIMARY KEY (instance_id, file_path)
);
CREATE TABLE IF NOT EXISTS host_metrics (
    ts                INTEGER,
    instance_id       TEXT,
    cpu_user_pct      REAL,
    cpu_system_pct    REAL,
    cpu_iowait_pct    REAL,
    cpu_busy_pct      REAL,
    load1             REAL,
    load5             REAL,
    runq_sz           REAL,
    cswch_per_s       REAL,
    mem_used_pct      REAL,
    mem_cached_mb     REAL,
    swap_used_pct     REAL,
    disk_util_pct_max REAL,
    disk_await_ms_max REAL,
    disk_tps          REAL,
    net_util_pct_max  REAL,
    net_rx_kbs        REAL,
    net_tx_kbs        REAL,
    top_disks_json    TEXT,
    top_nics_json     TEXT,
    PRIMARY KEY (instance_id, ts)
);
CREATE INDEX IF NOT EXISTS idx_host_metrics_inst_ts ON host_metrics(instance_id, ts);
CREATE TABLE IF NOT EXISTS sar_collector_state (
    instance_id TEXT,
    last_ts     INTEGER,
    updated_ts  INTEGER,
    PRIMARY KEY (instance_id)
);
"""


@contextmanager
def connect(db_path: str = None):
    conn = sqlite3.connect(db_path or DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def _ensure_columns(c) -> None:
    cols = {r[1] for r in c.execute('PRAGMA table_info(instances)')}
    if 'node_id' not in cols:
        c.execute('ALTER TABLE instances ADD COLUMN node_id TEXT')


def init_db(db_path: str = None) -> None:
    with connect(db_path) as c:
        c.executescript(SCHEMA)
        _ensure_columns(c)


def upsert_instance(c, inst, collector="G1") -> None:
    c.execute(
        "INSERT OR REPLACE INTO instances(id,region,env,cluster,grp,role,idx,heap_max_mb,collector,node_id)"
        " VALUES(?,?,?,?,?,?,?,?,?,?)",
        (inst.id, inst.region, inst.env, inst.cluster, inst.group, inst.role,
         inst.index, inst.heap_max_mb, collector, getattr(inst, 'node_id', None) or ''),
    )


def record_metric(c, instance_id: str, ts: int, m: dict) -> None:
    c.execute(
        "INSERT OR REPLACE INTO metrics(ts,instance_id,heap_used_mb,heap_max_mb,heap_after_pct,"
        "pause_avg_ms,pause_p99_ms,pause_max_ms,full_gc_count,young_count,gc_per_min,"
        "time_in_gc_pct,throughput_pct) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (ts, instance_id, m["heap_used_mb"], m["heap_max_mb"], m["heap_after_pct"],
         m["pause_avg_ms"], m["pause_p99_ms"], m["pause_max_ms"], m["full_gc_count"],
         m["young_count"], m["gc_per_min"], m["time_in_gc_pct"], m["throughput_pct"]),
    )


# --------------------------------------------------------------------------- #
# Host (SAR) metrics — same instance_id / ts grid as `metrics`, so the two
# tables join directly for correlation and scaling analysis.
# --------------------------------------------------------------------------- #
def record_host_metric(c, instance_id: str, ts: int, m: dict) -> None:
    c.execute(
        "INSERT OR REPLACE INTO host_metrics(ts,instance_id,cpu_user_pct,cpu_system_pct,"
        "cpu_iowait_pct,cpu_busy_pct,load1,load5,runq_sz,cswch_per_s,mem_used_pct,mem_cached_mb,"
        "swap_used_pct,disk_util_pct_max,disk_await_ms_max,disk_tps,net_util_pct_max,"
        "net_rx_kbs,net_tx_kbs,top_disks_json,top_nics_json) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            ts, instance_id,
            m.get("cpu_user_pct_avg", 0.0), m.get("cpu_system_pct_avg", 0.0),
            m.get("cpu_iowait_pct_avg", 0.0), m.get("cpu_busy_pct_avg", 0.0),
            m.get("load1_avg", 0.0), m.get("load5_avg", 0.0),
            m.get("runq_sz_avg", 0.0), m.get("cswch_per_s_avg", 0.0),
            m.get("mem_used_pct_avg", 0.0), m.get("mem_cached_mb_avg", 0.0),
            m.get("swap_used_pct_max", 0.0),
            m.get("disk_util_pct_max", 0.0), m.get("disk_await_ms_max", 0.0), m.get("disk_tps_avg", 0.0),
            m.get("net_util_pct_max", 0.0), m.get("net_rx_kbs_avg", 0.0), m.get("net_tx_kbs_avg", 0.0),
            json.dumps(m.get("top_disks", [])), json.dumps(m.get("top_nics", [])),
        ),
    )


def host_window_rows(c, instance_id: str, since_ts: int, until_ts: int = None) -> list[dict]:
    q = "SELECT * FROM host_metrics WHERE instance_id=? AND ts>=?"
    args = [instance_id, since_ts]
    if until_ts is not None:
        q += " AND ts<=?"
        args.append(until_ts)
    q += " ORDER BY ts ASC"
    return [dict(r) for r in c.execute(q, args)]


def host_latest_row(c, instance_id: str, now: int = None) -> dict | None:
    q = "SELECT * FROM host_metrics WHERE instance_id=?"
    args = [instance_id]
    if now is not None:
        q += " AND ts<=?"
        args.append(now)
    q += " ORDER BY ts DESC LIMIT 1"
    r = c.execute(q, args).fetchone()
    return dict(r) if r else None


def _host_metrics_dict_from_window(rows: list[dict]) -> dict:
    """Reconstruct a sar_analyzer-compatible metrics dict from stored rows
    (averaging the per-interval averages, taking the max of the maxes)."""
    if not rows:
        return {"sample_count": 0}
    try:
        top_disks = json.loads(rows[-1].get("top_disks_json") or "[]")
    except (TypeError, ValueError):
        top_disks = []
    try:
        top_nics = json.loads(rows[-1].get("top_nics_json") or "[]")
    except (TypeError, ValueError):
        top_nics = []
    return {
        "sample_count": len(rows),
        "span_seconds": (rows[-1]["ts"] - rows[0]["ts"]) if len(rows) > 1 else 0,
        "cpu_user_pct_avg": round(statistics.fmean(r["cpu_user_pct"] for r in rows), 2),
        "cpu_system_pct_avg": round(statistics.fmean(r["cpu_system_pct"] for r in rows), 2),
        "cpu_iowait_pct_avg": round(statistics.fmean(r["cpu_iowait_pct"] for r in rows), 2),
        "cpu_iowait_pct_max": round(max(r["cpu_iowait_pct"] for r in rows), 2),
        "cpu_busy_pct_avg": round(statistics.fmean(r["cpu_busy_pct"] for r in rows), 2),
        "cpu_busy_pct_max": round(max(r["cpu_busy_pct"] for r in rows), 2),
        "load1_avg": round(statistics.fmean(r["load1"] for r in rows), 2),
        "load1_max": round(max(r["load1"] for r in rows), 2),
        "load5_avg": round(statistics.fmean(r["load5"] for r in rows), 2),
        "runq_sz_avg": round(statistics.fmean(r["runq_sz"] for r in rows), 2),
        "cswch_per_s_avg": round(statistics.fmean(r["cswch_per_s"] for r in rows), 2),
        "mem_used_pct_avg": round(statistics.fmean(r["mem_used_pct"] for r in rows), 2),
        "mem_used_pct_max": round(max(r["mem_used_pct"] for r in rows), 2),
        "mem_cached_mb_avg": round(statistics.fmean(r["mem_cached_mb"] for r in rows), 2),
        "swap_used_pct_avg": round(statistics.fmean(r["swap_used_pct"] for r in rows), 2),
        "swap_used_pct_max": round(max(r["swap_used_pct"] for r in rows), 2),
        "disk_util_pct_max": round(max(r["disk_util_pct_max"] for r in rows), 2),
        "disk_await_ms_max": round(max(r["disk_await_ms_max"] for r in rows), 2),
        "disk_tps_avg": round(statistics.fmean(r["disk_tps"] for r in rows), 2),
        "net_util_pct_max": round(max(r["net_util_pct_max"] for r in rows), 2),
        "net_rx_kbs_avg": round(statistics.fmean(r["net_rx_kbs"] for r in rows), 2),
        "net_tx_kbs_avg": round(statistics.fmean(r["net_tx_kbs"] for r in rows), 2),
        "net_tx_kbs_max": round(max(r["net_tx_kbs"] for r in rows), 2),
        "top_disks": top_disks,
        "top_nics": top_nics,
    }


def current_host_snapshot(c, instance_id: str, now: int = None) -> dict | None:
    inst = get_instance(c, instance_id)
    if not inst:
        return None
    now = now or now_ts(c)
    last = host_latest_row(c, instance_id, now)
    if not last:
        return {"instance": inst, "metrics": {}, "health": None, "findings": None}
    day = host_window_rows(c, instance_id, now - 86400, now)
    if not day:
        day = [last]
    metrics = _host_metrics_dict_from_window(day)
    health = sar_analyzer.score_health(metrics)
    findings = sar_analyzer.derive_findings(metrics)
    return {"instance": inst, "latest": last, "metrics": metrics, "health": health, "findings": findings}


def host_trends(c, instance_id: str, days: int = 30, now: int = None) -> dict:
    now = now or now_ts(c)
    since = now - days * 86400
    query = """
        SELECT
            (ts / 86400) * 86400 AS day,
            AVG(cpu_busy_pct) AS cpu_busy_avg, MAX(cpu_busy_pct) AS cpu_busy_max,
            AVG(cpu_iowait_pct) AS iowait_avg, MAX(cpu_iowait_pct) AS iowait_max,
            AVG(mem_used_pct) AS mem_avg, MAX(mem_used_pct) AS mem_max,
            MAX(swap_used_pct) AS swap_max,
            MAX(disk_util_pct_max) AS disk_util_max, MAX(disk_await_ms_max) AS disk_await_max,
            MAX(net_util_pct_max) AS net_util_max,
            AVG(net_tx_kbs) AS net_tx_avg, AVG(load1) AS load1_avg
        FROM host_metrics
        WHERE instance_id = ? AND ts >= ? AND ts <= ?
        GROUP BY day
        ORDER BY day ASC
    """
    rows = c.execute(query, (instance_id, since, now)).fetchall()
    series = []
    for r in rows:
        series.append({
            "t": r["day"],
            "cpu_busy_avg": round(r["cpu_busy_avg"], 1) if r["cpu_busy_avg"] is not None else 0.0,
            "cpu_busy_max": round(r["cpu_busy_max"], 1) if r["cpu_busy_max"] is not None else 0.0,
            "iowait_avg": round(r["iowait_avg"], 1) if r["iowait_avg"] is not None else 0.0,
            "iowait_max": round(r["iowait_max"], 1) if r["iowait_max"] is not None else 0.0,
            "mem_avg": round(r["mem_avg"], 1) if r["mem_avg"] is not None else 0.0,
            "mem_max": round(r["mem_max"], 1) if r["mem_max"] is not None else 0.0,
            "swap_max": round(r["swap_max"], 2) if r["swap_max"] is not None else 0.0,
            "disk_util_max": round(r["disk_util_max"], 1) if r["disk_util_max"] is not None else 0.0,
            "disk_await_max": round(r["disk_await_max"], 1) if r["disk_await_max"] is not None else 0.0,
            "net_util_max": round(r["net_util_max"], 1) if r["net_util_max"] is not None else 0.0,
            "net_tx_avg": round(r["net_tx_avg"], 1) if r["net_tx_avg"] is not None else 0.0,
            "load1_avg": round(r["load1_avg"], 2) if r["load1_avg"] is not None else 0.0,
        })
    return {"instance_id": instance_id, "days": days, "series": series}


def host_range_series(c, instance_id: str, since: int, until: int, bucket_s: int) -> dict:
    rows = host_window_rows(c, instance_id, since, until)
    buckets: dict[int, list[dict]] = {}
    for r in rows:
        b = (r["ts"] // bucket_s) * bucket_s
        buckets.setdefault(b, []).append(r)
    series = []
    for b in sorted(buckets):
        rs = buckets[b]
        series.append({
            "t": b,
            "cpu_busy_avg": round(statistics.fmean(r["cpu_busy_pct"] for r in rs), 1),
            "cpu_busy_max": round(max(r["cpu_busy_pct"] for r in rs), 1),
            "iowait_avg": round(statistics.fmean(r["cpu_iowait_pct"] for r in rs), 1),
            "mem_avg": round(statistics.fmean(r["mem_used_pct"] for r in rs), 1),
            "mem_max": round(max(r["mem_used_pct"] for r in rs), 1),
            "swap_max": round(max(r["swap_used_pct"] for r in rs), 2),
            "disk_util_max": round(max(r["disk_util_pct_max"] for r in rs), 1),
            "disk_await_max": round(max(r["disk_await_ms_max"] for r in rs), 1),
            "net_util_max": round(max(r["net_util_pct_max"] for r in rs), 1),
            "net_tx_avg": round(statistics.fmean(r["net_tx_kbs"] for r in rs), 1),
            "load1_avg": round(statistics.fmean(r["load1"] for r in rs), 2),
        })
    return {"instance_id": instance_id, "bucket_s": bucket_s, "series": series}


def get_sar_state(c, instance_id: str) -> int | None:
    """Last-ingested sar sample timestamp (dedup point — sar/sadf reports are
    re-queryable, not append-only files, so incremental collection just means
    'skip samples we've already recorded')."""
    r = c.execute("SELECT last_ts FROM sar_collector_state WHERE instance_id=?", (instance_id,)).fetchone()
    return int(r["last_ts"]) if r and r["last_ts"] is not None else None


def set_sar_state(c, instance_id: str, last_ts: int, updated_ts: int) -> None:
    c.execute(
        "INSERT OR REPLACE INTO sar_collector_state(instance_id,last_ts,updated_ts) VALUES(?,?,?)",
        (instance_id, last_ts, updated_ts),
    )


# --------------------------------------------------------------------------- #
# Reads
# --------------------------------------------------------------------------- #
def list_instances(c) -> list[dict]:
    return [dict(r) for r in c.execute("SELECT * FROM instances ORDER BY id")]


def get_instance(c, instance_id: str) -> dict | None:
    r = c.execute("SELECT * FROM instances WHERE id=?", (instance_id,)).fetchone()
    return dict(r) if r else None


def latest_row(c, instance_id: str, now: int = None) -> dict | None:
    q = "SELECT * FROM metrics WHERE instance_id=?"
    args = [instance_id]
    if now is not None:
        q += " AND ts<=?"
        args.append(now)
    q += " ORDER BY ts DESC LIMIT 1"
    r = c.execute(q, args).fetchone()
    return dict(r) if r else None


def window_rows(c, instance_id: str, since_ts: int, until_ts: int = None) -> list[dict]:
    q = "SELECT * FROM metrics WHERE instance_id=? AND ts>=?"
    args = [instance_id, since_ts]
    if until_ts is not None:
        q += " AND ts<=?"
        args.append(until_ts)
    q += " ORDER BY ts ASC"
    return [dict(r) for r in c.execute(q, args)]


def now_ts(c) -> int:
    """The most recent timestamp in the store (so demo + live both work)."""
    r = c.execute("SELECT MAX(ts) AS m FROM metrics").fetchone()
    return int(r["m"]) if r and r["m"] else int(time.time())


# --------------------------------------------------------------------------- #
# Derived: current health, last-hour alerts, findings, trends
# --------------------------------------------------------------------------- #
def _metrics_dict_from_window(rows: list[dict], heap_max: float) -> dict:
    """Build an analyzer-compatible metrics dict from recent rows (24h)."""
    if not rows:
        return {}
    after_pcts = [r["heap_after_pct"] for r in rows]
    return {
        "throughput_pct": round(statistics.fmean(r["throughput_pct"] for r in rows), 3),
        "pct_time_in_gc": round(statistics.fmean(r["time_in_gc_pct"] for r in rows), 3),
        "avg_pause_ms": round(statistics.fmean(r["pause_avg_ms"] for r in rows), 2),
        "p99_pause_ms": round(max(r["pause_p99_ms"] for r in rows), 2),
        "max_pause_ms": round(max(r["pause_max_ms"] for r in rows), 2),
        "full_count": int(sum(r["full_gc_count"] for r in rows)),
        "young_count": int(sum(r["young_count"] for r in rows)),
        "mixed_count": 0,
        "gc_per_min": round(statistics.fmean(r["gc_per_min"] for r in rows), 2),
        "heap_max_mb": round(heap_max, 1),
        "avg_heap_after_pct": round(statistics.fmean(after_pcts), 1),
        "peak_heap_after_pct": round(max(after_pcts), 1),
        "avg_heap_after_mb": round(statistics.fmean(r["heap_used_mb"] for r in rows), 1),
        "promotion_trend_pct": _promotion_trend(after_pcts),
    }


def _promotion_trend(after_pcts: list[float]) -> float:
    if len(after_pcts) < 2:
        return 0.0
    climbs = sum(1 for a, b in zip(after_pcts, after_pcts[1:]) if b > a)
    return round(climbs / (len(after_pcts) - 1) * 100.0, 1)


def current_snapshot(c, instance_id: str, now: int = None) -> dict | None:
    inst = get_instance(c, instance_id)
    if not inst:
        return None
    now = now or now_ts(c)
    last = latest_row(c, instance_id, now)
    if not last:
        return {"instance": inst, "metrics": {}, "health": None, "alerts": [], "findings": None}

    day = window_rows(c, instance_id, now - 86400, now)
    if not day and last:
        day = [last]
    observed_heap = max((r["heap_max_mb"] for r in day if r.get("heap_max_mb")), default=0)
    effective_heap = max(inst["heap_max_mb"] or 0, observed_heap)
    metrics = _metrics_dict_from_window(day, effective_heap)
    health = analyzer.score_health(metrics)
    findings = analyzer.derive_findings(metrics, inst["collector"])
    alerts = evaluate_alerts(c, instance_id, now)
    return {
        "instance": inst,
        "latest": last,
        "metrics": metrics,
        "health": health,
        "alerts": alerts,
        "findings": findings,
    }


def _heap_alert_pct(role: str) -> float:
    return analyzer.ALERT_HEAP_BY_ROLE.get(role or "", HEAP_ALERT_PCT)


def evaluate_alerts(c, instance_id: str, now: int = None) -> list[dict]:
    """Issues observed in the last hour, color-coded."""
    now = now or now_ts(c)
    recent = window_rows(c, instance_id, now - RECENT_WINDOW_S, now)
    if not recent:
        return []
    inst = get_instance(c, instance_id)
    role = (inst or {}).get("role") or "broker"
    heap_thresh = _heap_alert_pct(role)

    alerts = []
    full = sum(r["full_gc_count"] for r in recent)
    max_pause = max(r["pause_max_ms"] for r in recent)
    max_p99 = max(r["pause_p99_ms"] for r in recent)
    peak_heap = max(r["heap_after_pct"] for r in recent)
    recent_tig = max(r["time_in_gc_pct"] for r in recent)
    recent_freq = max(r["gc_per_min"] for r in recent)

    # Baseline = median over the 30 days BEFORE the last 2 hours.
    base_rows = window_rows(c, instance_id, now - 30 * 86400, now - 2 * 3600)
    base_tig = statistics.median([r["time_in_gc_pct"] for r in base_rows]) if base_rows else 1.0
    base_freq = statistics.median([r["gc_per_min"] for r in base_rows]) if base_rows else 5.0

    if full > 0:
        alerts.append({"type": "full_gc", "severity": "critical",
                       "msg": f"{full} Full GC(s) in the last hour"})
    if max_pause > PAUSE_ALERT_MS:
        alerts.append({"type": "long_pause", "severity": "critical",
                       "msg": f"Stop-the-world pause {max_pause:.0f}ms (> {PAUSE_ALERT_MS:.0f}ms)"})
    elif max_p99 > P99_PAUSE_ALERT_MS:
        alerts.append({"type": "tail_pause", "severity": "warning",
                       "msg": f"p99 pause {max_p99:.0f}ms exceeds {P99_PAUSE_ALERT_MS:.0f}ms G1 target"})
    if peak_heap > heap_thresh:
        alerts.append({"type": "heap_pressure", "severity": "warning",
                       "msg": f"Heap live set at {peak_heap:.0f}% (> {heap_thresh:.0f}% for {role})"})
    storm_thresh = max(STORM_TIME_IN_GC_PCT, base_tig * STORM_BASELINE_MULT)
    if recent_tig > storm_thresh:
        alerts.append({"type": "gc_storm", "severity": "warning",
                       "msg": f"Time-in-GC {recent_tig:.1f}% vs {base_tig:.1f}% baseline (>{storm_thresh:.1f}%)"})
    elif recent_freq > base_freq * GC_FREQ_BASELINE_MULT and recent_freq > GC_FREQ_ALERT_MIN:
        alerts.append({"type": "gc_freq", "severity": "warning",
                       "msg": f"GC frequency {recent_freq:.0f}/min vs {base_freq:.0f}/min baseline"})
    return alerts


def trends(c, instance_id: str, days: int = 30, now: int = None) -> dict:
    """Daily-aggregated series for the per-instance trend charts."""
    now = now or now_ts(c)
    since = now - days * 86400
    query = """
        SELECT
            (ts / 86400) * 86400 AS day,
            AVG(heap_used_mb) AS heap_used_avg,
            MAX(heap_used_mb) AS heap_used_max,
            AVG(heap_after_pct) AS heap_after_pct_avg,
            MAX(pause_p99_ms) AS pause_p99_max,
            MAX(pause_max_ms) AS pause_max,
            SUM(full_gc_count) AS full_gc,
            AVG(time_in_gc_pct) AS time_in_gc_avg,
            AVG(throughput_pct) AS throughput_avg,
            MAX(heap_max_mb) AS heap_max_mb
        FROM metrics
        WHERE instance_id = ? AND ts >= ? AND ts <= ?
        GROUP BY day
        ORDER BY day ASC
    """
    rows = c.execute(query, (instance_id, since, now)).fetchall()

    series = []
    for r in rows:
        series.append({
            "t": r["day"],
            "heap_used_avg": round(r["heap_used_avg"], 1) if r["heap_used_avg"] is not None else 0.0,
            "heap_used_max": round(r["heap_used_max"], 1) if r["heap_used_max"] is not None else 0.0,
            "heap_after_pct_avg": round(r["heap_after_pct_avg"], 1) if r["heap_after_pct_avg"] is not None else 0.0,
            "pause_p99_max": round(r["pause_p99_max"], 1) if r["pause_p99_max"] is not None else 0.0,
            "pause_max": round(r["pause_max"], 1) if r["pause_max"] is not None else 0.0,
            "full_gc": int(r["full_gc"]) if r["full_gc"] is not None else 0,
            "time_in_gc_avg": round(r["time_in_gc_avg"], 2) if r["time_in_gc_avg"] is not None else 0.0,
            "throughput_avg": round(r["throughput_avg"], 2) if r["throughput_avg"] is not None else 0.0,
        })
    heap_max = rows[0]["heap_max_mb"] if rows else None
    return {"instance_id": instance_id, "days": days, "heap_max_mb": heap_max, "series": series}


def hourly_series(c, instance_id: str, hours: int = 48, now: int = None) -> list[dict]:
    """Fine-grained recent series (for the most-recent-window chart)."""
    now = now or now_ts(c)
    rows = window_rows(c, instance_id, now - hours * 3600, now)
    return [{"t": r["ts"], "heap_used_mb": r["heap_used_mb"], "heap_after_pct": r["heap_after_pct"],
             "pause_max_ms": r["pause_max_ms"], "time_in_gc_pct": r["time_in_gc_pct"],
             "full_gc": r["full_gc_count"]} for r in rows]


def range_series(c, instance_id: str, since: int, until: int, bucket_s: int) -> dict:
    """Series aggregated into buckets of `bucket_s` seconds over [since, until].

    Same row shape as trends() so the dashboard charts render any time range
    (1h .. 2y) by just changing the bucket size.
    """
    rows = window_rows(c, instance_id, since, until)
    buckets: dict[int, list[dict]] = {}
    for r in rows:
        b = (r["ts"] // bucket_s) * bucket_s
        buckets.setdefault(b, []).append(r)
    series = []
    for b in sorted(buckets):
        rs = buckets[b]
        series.append({
            "t": b,
            "heap_used_avg": round(statistics.fmean(r["heap_used_mb"] for r in rs), 1),
            "heap_used_max": round(max(r["heap_used_mb"] for r in rs), 1),
            "heap_after_pct_avg": round(statistics.fmean(r["heap_after_pct"] for r in rs), 1),
            "pause_p99_max": round(max(r["pause_p99_ms"] for r in rs), 1),
            "pause_max": round(max(r["pause_max_ms"] for r in rs), 1),
            "full_gc": int(sum(r["full_gc_count"] for r in rs)),
            "time_in_gc_avg": round(statistics.fmean(r["time_in_gc_pct"] for r in rs), 2),
            "throughput_avg": round(statistics.fmean(r["throughput_pct"] for r in rs), 2),
        })
    heap_max = rows[0]["heap_max_mb"] if rows else None
    return {"instance_id": instance_id, "bucket_s": bucket_s, "heap_max_mb": heap_max, "series": series}


# --------------------------------------------------------------------------- #
# Incremental-collection offsets (the scheduler reads each GC log from where it
# left off): one row per (instance, file) tracking inode + byte offset.
# --------------------------------------------------------------------------- #
def get_offsets(c, instance_id: str) -> dict:
    rows = c.execute(
        "SELECT file_path, inode, offset FROM collector_state WHERE instance_id=?", (instance_id,)
    )
    return {r["file_path"]: {"inode": r["inode"], "offset": r["offset"]} for r in rows}


def set_offset(c, instance_id: str, file_path: str, inode: int, offset: int, ts: int) -> None:
    c.execute(
        "INSERT OR REPLACE INTO collector_state(instance_id,file_path,inode,offset,updated_ts)"
        " VALUES(?,?,?,?,?)",
        (instance_id, file_path, inode, offset, ts),
    )


# --------------------------------------------------------------------------- #
# Admin cluster management + retention
# --------------------------------------------------------------------------- #
def delete_instance(c, instance_id: str) -> None:
    c.execute("DELETE FROM metrics WHERE instance_id=?", (instance_id,))
    c.execute("DELETE FROM collector_state WHERE instance_id=?", (instance_id,))
    c.execute("DELETE FROM host_metrics WHERE instance_id=?", (instance_id,))
    c.execute("DELETE FROM sar_collector_state WHERE instance_id=?", (instance_id,))
    c.execute("DELETE FROM instances WHERE id=?", (instance_id,))


def migrate_instance(c, old_id: str, new_id: str) -> None:
    if old_id == new_id:
        return
    c.execute("UPDATE OR IGNORE metrics SET instance_id=? WHERE instance_id=?", (new_id, old_id))
    c.execute("DELETE FROM metrics WHERE instance_id=?", (old_id,))
    c.execute("UPDATE OR IGNORE collector_state SET instance_id=? WHERE instance_id=?", (new_id, old_id))
    c.execute("DELETE FROM collector_state WHERE instance_id=?", (old_id,))
    c.execute("UPDATE OR IGNORE host_metrics SET instance_id=? WHERE instance_id=?", (new_id, old_id))
    c.execute("DELETE FROM host_metrics WHERE instance_id=?", (old_id,))
    c.execute("UPDATE OR IGNORE sar_collector_state SET instance_id=? WHERE instance_id=?", (new_id, old_id))
    c.execute("DELETE FROM sar_collector_state WHERE instance_id=?", (old_id,))
    c.execute("DELETE FROM instances WHERE id=?", (old_id,))


def delete_cluster(c, cluster: str) -> int:
    """Remove a cluster's instances, metrics, and collector offsets. Returns
    the number of instances removed."""
    ids = [r["id"] for r in c.execute("SELECT id FROM instances WHERE cluster=?", (cluster,))]
    for iid in ids:
        delete_instance(c, iid)

    return len(ids)


def delete_clusters(c, names: set[str] | list[str]) -> int:
    removed = 0
    for name in set(names):
        removed += delete_cluster(c, name)
    return removed


def prune_orphan_clusters(c, onboarded: set[str]) -> int:
    """Drop instance rows whose cluster name is not declared in any clusters/*.yaml."""
    removed = 0
    for row in c.execute("SELECT DISTINCT cluster FROM instances"):
        cl = row["cluster"]
        if cl not in onboarded:
            removed += delete_cluster(c, cl)
    return removed


def prune_before(c, before_ts: int) -> int:
    """Delete metric + host_metric rows older than before_ts (retention).
    Returns total rows deleted across both tables."""
    cur = c.execute("DELETE FROM metrics WHERE ts < ?", (before_ts,))
    deleted = cur.rowcount
    cur2 = c.execute("DELETE FROM host_metrics WHERE ts < ?", (before_ts,))
    deleted += cur2.rowcount
    return deleted
