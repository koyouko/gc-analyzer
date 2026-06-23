"""
SQLite time-series store for fleet GC metrics.

Two tables:
  instances : the component inventory (one row per JVM).
  metrics   : hourly rollups per instance — the history that powers 30-day
              trends, "right now" health, and "last hour" alerting.

In production, a periodic collection job parses each node's GC log and calls
`record_metric()` once per interval; the dashboard reads aggregates back out.
For the demo, seed/seed_history.py populates 30 days of rows.

Health and tuning advice reuse gcanalyzer.analyzer so a node is judged the same
way whether the numbers come from a freshly parsed log or from the store.
"""

from __future__ import annotations

import os
import sqlite3
import statistics
import time
from contextlib import contextmanager

from . import analyzer

DB_PATH = os.environ.get(
    "GC_DB", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "gc_history.db")
)

# Last-hour alert thresholds (tunable to your SLOs).
PAUSE_ALERT_MS = 500.0
HEAP_ALERT_PCT = 85.0
STORM_TIME_IN_GC_PCT = 5.0     # absolute floor for a "GC storm"
STORM_BASELINE_MULT = 3.0      # ... or 3x the node's own baseline
RECENT_WINDOW_S = 3600         # "last hour"


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
    metrics = _metrics_dict_from_window(day, inst["heap_max_mb"])
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


def evaluate_alerts(c, instance_id: str, now: int = None) -> list[dict]:
    """Issues observed in the last hour, color-coded."""
    now = now or now_ts(c)
    recent = window_rows(c, instance_id, now - RECENT_WINDOW_S, now)
    if not recent:
        return []
    alerts = []
    full = sum(r["full_gc_count"] for r in recent)
    max_pause = max(r["pause_max_ms"] for r in recent)
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
    if peak_heap > HEAP_ALERT_PCT:
        alerts.append({"type": "heap_pressure", "severity": "warning",
                       "msg": f"Heap live set at {peak_heap:.0f}% (> {HEAP_ALERT_PCT:.0f}%)"})
    if recent_tig > max(STORM_TIME_IN_GC_PCT, base_tig * STORM_BASELINE_MULT):
        alerts.append({"type": "gc_storm", "severity": "warning",
                       "msg": f"Time-in-GC {recent_tig:.1f}% vs {base_tig:.1f}% baseline"})
    elif recent_freq > base_freq * 2 and recent_freq > 20:
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
def delete_cluster(c, cluster: str) -> int:
    """Remove a cluster's instances, metrics, and collector offsets. Returns
    the number of instances removed."""
    ids = [r["id"] for r in c.execute("SELECT id FROM instances WHERE cluster=?", (cluster,))]
    for iid in ids:
        c.execute("DELETE FROM metrics WHERE instance_id=?", (iid,))
        c.execute("DELETE FROM collector_state WHERE instance_id=?", (iid,))
    c.execute("DELETE FROM instances WHERE cluster=?", (cluster,))
    return len(ids)


def prune_before(c, before_ts: int) -> int:
    """Delete metric rows older than before_ts (retention). Returns rows deleted."""
    cur = c.execute("DELETE FROM metrics WHERE ts < ?", (before_ts,))
    return cur.rowcount
