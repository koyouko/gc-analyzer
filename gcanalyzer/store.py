"""
SQLite time-series store for fleet GC + host (SAR) metrics.

Tables:
  instances    : the component inventory (one row per JVM).
  metrics      : hourly rollups per instance — the GC history that powers
                 30-day trends, "right now" health, and "last hour" alerting.
  host_metrics : fixed-minute rollups of OS-level activity (CPU/mem/swap/disk/net)
                 for the *host* each instance runs on — same instance_id, same
                 UTC time basis, so it can be rebucketed against `metrics` for
                 GC<->host correlation (see correlate.py) and cluster-wide
                 scaling analysis (see scaling_advisor.py).
  sar_samples  : one sparse normalized record per instance/sample timestamp,
                 used only to recompute changed rollups or boundary queries.
  gc_collection_quality : timestamped collection outcomes, separate from measured
                          metrics so rejected records cannot imply fresh health.

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
from dataclasses import asdict

from . import analyzer, sar_analyzer, sar_parser

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
CREATE TABLE IF NOT EXISTS gc_collection_quality (
    instance_id TEXT NOT NULL,
    ts INTEGER NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('complete', 'partial', 'unknown')),
    malformed_count INTEGER NOT NULL DEFAULT 0,
    unsupported_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (instance_id, ts)
);
CREATE TABLE IF NOT EXISTS host_metrics (
    ts                INTEGER,
    instance_id       TEXT,
    cpu_user_pct      REAL,
    cpu_system_pct    REAL,
    cpu_iowait_pct    REAL,
    cpu_busy_pct      REAL,
    cpu_steal_pct     REAL,
    load1             REAL,
    load5             REAL,
    load15            REAL,
    runq_sz           REAL,
    plist_sz          REAL,
    blocked           REAL,
    proc_per_s        REAL,
    cswch_per_s       REAL,
    mem_used_pct      REAL,
    mem_avail_mb      REAL,
    mem_cached_mb     REAL,
    mem_commit_pct    REAL,
    swap_used_pct     REAL,
    pgpgin_kbs        REAL,
    pgpgout_kbs       REAL,
    fault_per_s       REAL,
    majflt_per_s      REAL,
    pswpin_per_s      REAL,
    pswpout_per_s     REAL,
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
CREATE TABLE IF NOT EXISTS sar_samples (
    instance_id TEXT NOT NULL,
    ts INTEGER NOT NULL,
    sample_json TEXT NOT NULL,
    PRIMARY KEY (instance_id, ts)
);
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


# Columns added to host_metrics after the original release (RHEL 8/9 full
# SAR metric set) — migrated in on init for DBs created before the change.
_HOST_METRIC_EXTRA_COLS = [
    "cpu_steal_pct", "load15", "plist_sz", "blocked", "proc_per_s",
    "mem_avail_mb", "mem_commit_pct",
    "pgpgin_kbs", "pgpgout_kbs", "fault_per_s", "majflt_per_s",
    "pswpin_per_s", "pswpout_per_s",
]

_HOST_ROLLUP_COLS = {
    "sample_count": "INTEGER", "duration_seconds": "REAL", "bucket_seconds": "INTEGER",
    "first_observed_at": "INTEGER", "last_observed_at": "INTEGER", "metric_stats_json": "TEXT",
}

_HOST_COLUMNS = (
    "cpu_user_pct", "cpu_system_pct", "cpu_iowait_pct", "cpu_busy_pct", "cpu_steal_pct",
    "load1", "load5", "load15", "runq_sz", "plist_sz", "blocked", "proc_per_s", "cswch_per_s",
    "mem_used_pct", "mem_avail_mb", "mem_cached_mb", "mem_commit_pct", "swap_used_pct",
    "pgpgin_kbs", "pgpgout_kbs", "fault_per_s", "majflt_per_s", "pswpin_per_s", "pswpout_per_s",
    "disk_util_pct_max", "disk_await_ms_max", "disk_tps", "net_util_pct_max", "net_rx_kbs", "net_tx_kbs",
)


def _ensure_columns(c) -> None:
    cols = {r[1] for r in c.execute('PRAGMA table_info(instances)')}
    if 'node_id' not in cols:
        c.execute('ALTER TABLE instances ADD COLUMN node_id TEXT')
    host_cols = {r[1] for r in c.execute('PRAGMA table_info(host_metrics)')}
    for col in _HOST_METRIC_EXTRA_COLS:
        if col not in host_cols:
            c.execute(f'ALTER TABLE host_metrics ADD COLUMN {col} REAL')
    for col, kind in _HOST_ROLLUP_COLS.items():
        if col not in host_cols:
            c.execute(f'ALTER TABLE host_metrics ADD COLUMN {col} {kind}')


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
    values = _host_metric_row(instance_id, ts, m)
    c.execute(f"INSERT OR REPLACE INTO host_metrics({','.join(values)}) "
              f"VALUES({','.join('?' for _ in values)})", list(values.values()))


def _host_metric_row(instance_id: str, ts: int, m: dict) -> dict:
    values = {"ts": ts, "instance_id": instance_id}
    for col in _HOST_COLUMNS:
        metric = col if col.endswith("_max") else f"{col}_avg"
        if col == "swap_used_pct":
            metric = "swap_used_pct_max"
        value = m.get(metric)
        values[col] = value if sar_analyzer._numeric(value) else None
    values.update({col: m.get(col) for col in _HOST_ROLLUP_COLS if col != "metric_stats_json"})
    stats = {col: stats for col, stats in (m.get("metric_stats") or {}).items() if stats.get("count")}
    values.update(top_disks_json=json.dumps(m.get("top_disks", [])),
                  top_nics_json=json.dumps(m.get("top_nics", [])),
                  metric_stats_json=json.dumps(stats) if stats else None)
    return values


def record_sar_samples(c, instance_id: str, samples: list[sar_parser.SarSample]) -> tuple[int, set[int]]:
    """Store each normalized sample once, merging complementary exports by timestamp.

    No report blobs or sample lists are copied into rollups. A watermark is only
    progress telemetry, never a reason to discard an unseen late observation.
    The caller holds the write transaction through bucket recomputation.
    """
    new_count, affected = 0, set()
    for sample in samples:
        incoming = {k: v for k, v in asdict(sample).items()
                    if k != "ts" and v is not None and v != []}
        for field in ("disks", "nics"):
            if field in incoming:
                incoming[field] = [{k: v for k, v in item.items() if v is not None}
                                   for item in incoming[field]]
        old = c.execute("SELECT sample_json FROM sar_samples WHERE instance_id=? AND ts=?",
                        (instance_id, sample.ts)).fetchone()
        merged = json.loads(old[0]) if old else {}
        for field, value in incoming.items():
            if field in ("disks", "nics"):
                key = "dev" if field == "disks" else "iface"
                devices = {item[key]: item for item in merged.get(field, [])}
                for item in value:
                    devices.setdefault(item[key], {}).update(item)
                merged[field] = [devices[name] for name in sorted(devices)]
            else:
                merged[field] = value
        payload = json.dumps(merged, sort_keys=True, separators=(",", ":"), allow_nan=False)
        if old and payload == old[0]:
            continue
        c.execute("INSERT INTO sar_samples(instance_id,ts,sample_json) VALUES(?,?,?) "
                  "ON CONFLICT(instance_id,ts) DO UPDATE SET sample_json=excluded.sample_json",
                  (instance_id, sample.ts, payload))
        new_count += int(old is None)
        affected.add(sample.ts // sar_analyzer.HOST_BUCKET_SECONDS * sar_analyzer.HOST_BUCKET_SECONDS)
    return new_count, affected


def recompute_host_buckets(c, instance_id: str, buckets: set[int]) -> None:
    for ts in sorted(buckets):
        rows = c.execute("SELECT ts,sample_json FROM sar_samples WHERE instance_id=? AND ts>=? AND ts<? ORDER BY ts",
                         (instance_id, ts, ts + sar_analyzer.HOST_BUCKET_SECONDS)).fetchall()
        samples = [sar_parser.SarSample(ts=r["ts"], **json.loads(r["sample_json"])) for r in rows]
        if not samples:
            continue
        m = sar_analyzer._rollup(samples)
        m["bucket_seconds"] = sar_analyzer.HOST_BUCKET_SECONDS
        record_host_metric(c, instance_id, ts, m)


def host_window_rows(c, instance_id: str, since_ts: int, until_ts: int = None) -> list[dict]:
    q = "SELECT * FROM host_metrics WHERE instance_id=? AND ts>=?"
    args = [instance_id, since_ts // sar_analyzer.HOST_BUCKET_SECONDS * sar_analyzer.HOST_BUCKET_SECONDS]
    if until_ts is not None:
        q += " AND ts<=?"
        args.append(until_ts)
    q += " ORDER BY ts ASC"
    return [bounded for r in c.execute(q, args)
            if (bounded := _bounded_host_row(c, dict(r), since_ts, until_ts)) is not None]


def _bounded_host_row(c, row: dict, since: int | None, until: int | None) -> dict | None:
    start = row.get("first_observed_at")
    end = row.get("last_observed_at")
    if ((since is None or (start if start is not None else row["ts"]) >= since)
            and (until is None or (end if end is not None else row["ts"]) <= until)):
        return row
    # Only boundary buckets need their samples reread. Legacy/direct rollups
    # keep the original inclusive bucket-timestamp query convention.
    raw = []
    if row.get("bucket_seconds") == sar_analyzer.HOST_BUCKET_SECONDS:
        raw = c.execute("SELECT ts,sample_json FROM sar_samples WHERE instance_id=? AND ts>=? AND ts<? ORDER BY ts",
                        (row["instance_id"], row["ts"], row["ts"] + sar_analyzer.HOST_BUCKET_SECONDS)).fetchall()
    if not raw:
        return row if (since is None or row["ts"] >= since) and (until is None or row["ts"] <= until) else None
    samples = [sar_parser.SarSample(ts=r["ts"], **json.loads(r["sample_json"])) for r in raw
               if (since is None or r["ts"] >= since) and (until is None or r["ts"] <= until)]
    if not samples:
        return None
    m = sar_analyzer._rollup(samples)
    m["bucket_seconds"] = sar_analyzer.HOST_BUCKET_SECONDS
    return _host_metric_row(row["instance_id"], row["ts"], m)


def host_latest_row(c, instance_id: str, now: int = None) -> dict | None:
    q = "SELECT * FROM host_metrics WHERE instance_id=?"
    args = [instance_id]
    if now is not None:
        q += " AND ts<=?"
        args.append(now)
    q += " ORDER BY ts DESC"
    for r in c.execute(q, args):
        bounded = _bounded_host_row(c, dict(r), None, now)
        if bounded is not None:
            return bounded
    return None


def _host_stats(row: dict) -> dict:
    if "_stats" in row:
        return row["_stats"]
    try:
        value = json.loads(row.get("metric_stats_json") or "{}")
        return value if isinstance(value, dict) else {}
    except (TypeError, ValueError):
        return {}


def _host_aggregate(rows, col, operation="avg", digits=2):
    observed = [(r, _host_stats(r).get(col, {})) for r in rows if sar_analyzer._numeric(r.get(col))]
    if not observed:
        return None
    if operation == "max":
        return round(max(stats.get("max") if stats.get("max") is not None else r[col]
                         for r, stats in observed), digits)
    timed = (all(r.get("duration_seconds") is not None for r in rows)
             and all(stats.get("weight") for _, stats in observed))
    total, weight = 0, 0
    for r, stats in observed:
        w = stats.get("weight") if timed else stats.get("count", r.get("sample_count") or 1)
        subtotal = stats.get("sum" if timed else "sample_sum")
        total += subtotal if subtotal is not None else r[col] * w
        weight += w
    return round(total / weight, digits) if weight else None


def _host_metrics_dict_from_window(rows: list[dict]) -> dict:
    """Combine durations/counts and peaks; unknown legacy weights count as one row."""
    if not rows:
        return {"sample_count": 0}
    rows = [{**r, "_stats": _host_stats(r)} for r in rows]
    try:
        top_disks = json.loads(rows[-1].get("top_disks_json") or "[]")
    except (TypeError, ValueError):
        top_disks = []
    try:
        top_nics = json.loads(rows[-1].get("top_nics_json") or "[]")
    except (TypeError, ValueError):
        top_nics = []
    def _favg(col: str) -> float | None:
        return _host_aggregate(rows, col)

    def _fmax(col: str) -> float | None:
        return _host_aggregate(rows, col, "max")

    first = min(r.get("first_observed_at") if r.get("first_observed_at") is not None else r["ts"] for r in rows)
    last = max(r.get("last_observed_at") if r.get("last_observed_at") is not None else r["ts"] for r in rows)
    return {
        "sample_count": sum(r.get("sample_count") if r.get("sample_count") is not None else 1 for r in rows),
        "duration_seconds": sum(r["duration_seconds"] for r in rows)
        if all(r.get("duration_seconds") is not None for r in rows) else None,
        "first_observed_at": first, "last_observed_at": last,
        "metric_stats": {col: {"count": sum(_host_stats(r).get(col, {}).get(
            "count", (r.get("sample_count") or 1) if sar_analyzer._numeric(r.get(col)) else 0) for r in rows)}
            for col in _HOST_COLUMNS},
        "span_seconds": last - first,
        "cpu_user_pct_avg": _favg("cpu_user_pct"),
        "cpu_system_pct_avg": _favg("cpu_system_pct"),
        "cpu_iowait_pct_avg": _favg("cpu_iowait_pct"),
        "cpu_iowait_pct_max": _fmax("cpu_iowait_pct"),
        "cpu_busy_pct_avg": _favg("cpu_busy_pct"),
        "cpu_busy_pct_max": _fmax("cpu_busy_pct"),
        "cpu_steal_pct_avg": _favg("cpu_steal_pct"),
        "cpu_steal_pct_max": _fmax("cpu_steal_pct"),
        "load1_avg": _favg("load1"),
        "load1_max": _fmax("load1"),
        "load5_avg": _favg("load5"),
        "load15_avg": _favg("load15"),
        "runq_sz_avg": _favg("runq_sz"),
        "plist_sz_avg": _favg("plist_sz"),
        "blocked_avg": _favg("blocked"),
        "proc_per_s_avg": _favg("proc_per_s"),
        "cswch_per_s_avg": _favg("cswch_per_s"),
        "mem_used_pct_avg": _favg("mem_used_pct"),
        "mem_used_pct_max": _fmax("mem_used_pct"),
        "mem_avail_mb_avg": _favg("mem_avail_mb"),
        "mem_cached_mb_avg": _favg("mem_cached_mb"),
        "mem_commit_pct_avg": _favg("mem_commit_pct"),
        "swap_used_pct_avg": _favg("swap_used_pct"),
        "swap_used_pct_max": _fmax("swap_used_pct"),
        "pgpgin_kbs_avg": _favg("pgpgin_kbs"),
        "pgpgout_kbs_avg": _favg("pgpgout_kbs"),
        "fault_per_s_avg": _favg("fault_per_s"),
        "majflt_per_s_avg": _favg("majflt_per_s"),
        "majflt_per_s_max": _fmax("majflt_per_s"),
        "pswpin_per_s_avg": _favg("pswpin_per_s"),
        "pswpout_per_s_avg": _favg("pswpout_per_s"),
        "pswpout_per_s_max": _fmax("pswpout_per_s"),
        "disk_util_pct_max": _fmax("disk_util_pct_max"),
        "disk_await_ms_max": _fmax("disk_await_ms_max"),
        "disk_tps_avg": _favg("disk_tps"),
        "net_util_pct_max": _fmax("net_util_pct_max"),
        "net_rx_kbs_avg": _favg("net_rx_kbs"),
        "net_tx_kbs_avg": _favg("net_tx_kbs"),
        "net_tx_kbs_max": _fmax("net_tx_kbs"),
        "top_disks": top_disks,
        "top_nics": top_nics,
    }


def current_host_snapshot(c, instance_id: str, now: int = None) -> dict | None:
    inst = get_instance(c, instance_id)
    if not inst:
        return None
    now = now if now is not None else now_ts(c)
    last = host_latest_row(c, instance_id, now)
    quality = source_quality(last, now, sar_analyzer.HEALTH_METRICS, "SAR_SOURCE_MAX_AGE_S")
    if quality["state"] in ("missing", "stale"):
        return {"instance": inst, "latest": last, "metrics": {}, "health": _unknown_health(),
                "findings": _empty_findings(), "quality": quality}
    day = host_window_rows(c, instance_id, now - 86400, now)
    metrics = _host_metrics_dict_from_window(day)
    health = sar_analyzer.score_health(metrics)
    if quality["state"] == "partial" or health["status"] in ("partial", "unknown"):
        quality["state"] = "partial"
        health.update(score=None, grade="?", status="partial")
    findings = sar_analyzer.derive_findings(metrics)
    return {"instance": inst, "latest": last, "metrics": metrics, "health": health,
            "findings": findings, "quality": quality}


def host_trends(c, instance_id: str, days: int = 30, now: int = None) -> dict:
    now = now if now is not None else now_ts(c)
    series = host_range_series(c, instance_id, now - days * 86400, now, 86400)["series"]
    return {"instance_id": instance_id, "days": days, "series": series}


def host_range_series(c, instance_id: str, since: int, until: int, bucket_s: int) -> dict:
    if bucket_s <= 0:
        raise ValueError("bucket_s must be positive")
    rows = [{**r, "_stats": _host_stats(r)} for r in host_window_rows(c, instance_id, since, until)]
    buckets: dict[int, list[dict]] = {}
    for r in rows:
        b = (r["ts"] // bucket_s) * bucket_s
        buckets.setdefault(b, []).append(r)
    def _bavg(rs, col, digits=1):
        return _host_aggregate(rs, col, digits=digits)

    def _bmax(rs, col, digits=1):
        return _host_aggregate(rs, col, "max", digits)

    series = []
    for b in sorted(buckets):
        rs = buckets[b]
        series.append({
            "t": b,
            "cpu_busy_avg": _bavg(rs, "cpu_busy_pct"), "cpu_busy_max": _bmax(rs, "cpu_busy_pct"),
            "cpu_user_avg": _bavg(rs, "cpu_user_pct"), "cpu_system_avg": _bavg(rs, "cpu_system_pct"),
            "cpu_steal_avg": _bavg(rs, "cpu_steal_pct", 2),
            "iowait_avg": _bavg(rs, "cpu_iowait_pct"),
            "iowait_max": _bmax(rs, "cpu_iowait_pct"),
            "mem_avg": _bavg(rs, "mem_used_pct"), "mem_max": _bmax(rs, "mem_used_pct"),
            "mem_avail_avg": _bavg(rs, "mem_avail_mb"),
            "mem_cached_avg": _bavg(rs, "mem_cached_mb"), "mem_commit_avg": _bavg(rs, "mem_commit_pct"),
            "swap_max": _bmax(rs, "swap_used_pct", 2),
            "pgpgin_avg": _bavg(rs, "pgpgin_kbs"), "pgpgout_avg": _bavg(rs, "pgpgout_kbs"),
            "fault_avg": _bavg(rs, "fault_per_s"), "majflt_max": _bmax(rs, "majflt_per_s", 2),
            "pswpin_avg": _bavg(rs, "pswpin_per_s", 2), "pswpout_max": _bmax(rs, "pswpout_per_s", 2),
            "disk_util_max": _bmax(rs, "disk_util_pct_max"),
            "disk_await_max": _bmax(rs, "disk_await_ms_max"),
            "disk_tps_avg": _bavg(rs, "disk_tps"),
            "net_util_max": _bmax(rs, "net_util_pct_max"),
            "net_tx_avg": _bavg(rs, "net_tx_kbs"), "net_rx_avg": _bavg(rs, "net_rx_kbs"),
            "load1_avg": _bavg(rs, "load1", 2), "load5_avg": _bavg(rs, "load5", 2),
            "load15_avg": _bavg(rs, "load15", 2),
            "runq_avg": _bavg(rs, "runq_sz", 2), "blocked_avg": _bavg(rs, "blocked", 2),
            "cswch_avg": _bavg(rs, "cswch_per_s"), "proc_avg": _bavg(rs, "proc_per_s", 2),
        })
    return {"instance_id": instance_id, "bucket_s": bucket_s, "series": series}


def get_sar_state(c, instance_id: str) -> int | None:
    """Latest observed sample timestamp, for progress reporting only."""
    r = c.execute("SELECT last_ts FROM sar_collector_state WHERE instance_id=?", (instance_id,)).fetchone()
    return int(r["last_ts"]) if r and r["last_ts"] is not None else None


def set_sar_state(c, instance_id: str, last_ts: int, updated_ts: int) -> None:
    c.execute(
        "INSERT INTO sar_collector_state(instance_id,last_ts,updated_ts) VALUES(?,?,?) "
        "ON CONFLICT(instance_id) DO UPDATE SET last_ts=MAX(COALESCE(last_ts,excluded.last_ts),excluded.last_ts), "
        "updated_ts=excluded.updated_ts",
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
    """Live uses wall clock; explicitly enabled demos follow the latest source."""
    if os.environ.get("GC_DEMO_MODE") == "1":
        r = c.execute("SELECT MAX(t) AS m FROM (SELECT MAX(ts) AS t FROM metrics UNION ALL "
                      "SELECT MAX(COALESCE(last_observed_at,ts)) AS t FROM host_metrics UNION ALL "
                      "SELECT MAX(ts) AS t FROM gc_collection_quality)").fetchone()
        if r and r["m"] is not None:
            return int(r["m"])
    return int(time.time())


GC_HEALTH_METRICS = ("heap_after_pct", "pause_avg_ms", "pause_p99_ms", "pause_max_ms",
                     "full_gc_count", "young_count", "gc_per_min", "time_in_gc_pct", "throughput_pct")


def source_quality(last: dict | None, now: int, required_metrics, max_age_env="GC_SOURCE_MAX_AGE_S") -> dict:
    """Observation freshness is independent of retained historical rollups."""
    last = last or {}
    observed = last.get("last_observed_at")
    if observed is None:
        observed = last.get("ts")
    age = max(0, now - observed) if observed is not None else None
    available = [col for col in required_metrics if sar_analyzer._numeric(last.get(col))]
    missing = [col for col in required_metrics if col not in available]
    max_age = max(0, _alert_env(max_age_env, 7200))
    if not last or not available:
        state = "missing"
    elif age is not None and age > max_age:
        state = "stale"
    else:
        stats = _host_stats(last)
        incomplete = any(stats.get(col, {}).get("count", last.get("sample_count") or 1)
                         < (last.get("sample_count") or 1) for col in required_metrics)
        state = "partial" if missing or incomplete else "fresh"
    return {"state": state, "last_observed_at": observed, "age_seconds": age,
            "available_metrics": available, "missing_metrics": missing}


def _unknown_health():
    return {"grade": "?", "status": "unknown", "score": None, "reasons": []}


def _empty_findings():
    return {"pros": [], "cons": [], "recommendations": []}


def _aggregate(rows, col, operation="avg", digits=2):
    values = [r[col] for r in rows if sar_analyzer._numeric(r.get(col))]
    if not values:
        return None
    value = {"avg": statistics.fmean, "max": max, "sum": sum}[operation](values)
    return round(value, digits)


# --------------------------------------------------------------------------- #
# Derived: current health, last-hour alerts, findings, trends
# --------------------------------------------------------------------------- #
def _metrics_dict_from_window(rows: list[dict], heap_max: float) -> dict:
    """Build an analyzer-compatible metrics dict from recent rows (24h)."""
    if not rows:
        return {}
    after_pcts = [r["heap_after_pct"] for r in rows if sar_analyzer._numeric(r.get("heap_after_pct"))]
    return {
        "throughput_pct": _aggregate(rows, "throughput_pct", digits=3),
        "pct_time_in_gc": _aggregate(rows, "time_in_gc_pct", digits=3),
        "avg_pause_ms": _aggregate(rows, "pause_avg_ms"),
        "p99_pause_ms": _aggregate(rows, "pause_p99_ms", "max"),
        "max_pause_ms": _aggregate(rows, "pause_max_ms", "max"),
        "full_count": _aggregate(rows, "full_gc_count", "sum", 0),
        "young_count": _aggregate(rows, "young_count", "sum", 0),
        "mixed_count": 0,
        "gc_per_min": _aggregate(rows, "gc_per_min"),
        "heap_max_mb": round(heap_max, 1) if heap_max is not None else None,
        "avg_heap_after_pct": _aggregate(rows, "heap_after_pct", digits=1),
        "peak_heap_after_pct": _aggregate(rows, "heap_after_pct", "max", 1),
        "avg_heap_after_mb": _aggregate(rows, "heap_used_mb", digits=1),
        "promotion_trend_pct": _promotion_trend(after_pcts) if after_pcts else None,
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
    now = now if now is not None else now_ts(c)
    last = latest_row(c, instance_id, now)
    quality = source_quality(last, now, GC_HEALTH_METRICS)
    collection = latest_gc_collection_quality(c, instance_id, now)
    if collection:
        quality["collection"] = collection
        # Old metrics remain available as 'latest', but cannot represent current
        # GC health after an incomplete collection, even when their age is fresh.
        if collection["state"] != "complete" and quality["state"] not in ("missing", "stale"):
            quality["state"] = "unknown"
    if quality["state"] in ("missing", "stale", "unknown"):
        alerts = evaluate_alerts(c, instance_id, now) if quality["state"] == "unknown" else []
        return {"instance": inst, "latest": last, "metrics": {}, "health": _unknown_health(),
                "alerts": alerts, "findings": _empty_findings(), "quality": quality}

    day = window_rows(c, instance_id, now - 86400, now)
    observed_heap = max((r["heap_max_mb"] for r in day if r.get("heap_max_mb")), default=0)
    effective_heap = max(inst["heap_max_mb"] or 0, observed_heap)
    metrics = _metrics_dict_from_window(day, effective_heap)
    if quality["state"] == "partial" or any(v is None for v in metrics.values()):
        quality["state"] = "partial"
        health, findings = _unknown_health(), _empty_findings()
    else:
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
        "quality": quality,
    }


def _heap_alert_pct(role: str) -> float:
    return analyzer.ALERT_HEAP_BY_ROLE.get(role or "", HEAP_ALERT_PCT)


def evaluate_alerts(c, instance_id: str, now: int = None) -> list[dict]:
    """Issues observed in the last hour, color-coded."""
    now = now if now is not None else now_ts(c)
    recent = window_rows(c, instance_id, now - RECENT_WINDOW_S, now)
    if not recent:
        return []
    inst = get_instance(c, instance_id)
    role = (inst or {}).get("role") or "broker"
    heap_thresh = _heap_alert_pct(role)

    alerts = []
    full = _aggregate(recent, "full_gc_count", "sum", 0)
    max_pause = _aggregate(recent, "pause_max_ms", "max")
    max_p99 = _aggregate(recent, "pause_p99_ms", "max")
    peak_heap = _aggregate(recent, "heap_after_pct", "max")
    recent_tig = _aggregate(recent, "time_in_gc_pct", "max")
    recent_freq = _aggregate(recent, "gc_per_min", "max")

    # Baseline = median over the 30 days BEFORE the last 2 hours.
    base_rows = window_rows(c, instance_id, now - 30 * 86400, now - 2 * 3600)
    tig_values = [r["time_in_gc_pct"] for r in base_rows if sar_analyzer._numeric(r.get("time_in_gc_pct"))]
    freq_values = [r["gc_per_min"] for r in base_rows if sar_analyzer._numeric(r.get("gc_per_min"))]
    base_tig = statistics.median(tig_values) if tig_values else 1.0
    base_freq = statistics.median(freq_values) if freq_values else 5.0

    if full is not None and full > 0:
        alerts.append({"type": "full_gc", "severity": "critical",
                       "msg": f"{full} Full GC(s) in the last hour"})
    if max_pause is not None and max_pause > PAUSE_ALERT_MS:
        alerts.append({"type": "long_pause", "severity": "critical",
                       "msg": f"Stop-the-world pause {max_pause:.0f}ms (> {PAUSE_ALERT_MS:.0f}ms)"})
    elif max_p99 is not None and max_p99 > P99_PAUSE_ALERT_MS:
        alerts.append({"type": "tail_pause", "severity": "warning",
                       "msg": f"p99 pause {max_p99:.0f}ms exceeds {P99_PAUSE_ALERT_MS:.0f}ms G1 target"})
    if peak_heap is not None and peak_heap > heap_thresh:
        alerts.append({"type": "heap_pressure", "severity": "warning",
                       "msg": f"Heap live set at {peak_heap:.0f}% (> {heap_thresh:.0f}% for {role})"})
    storm_thresh = max(STORM_TIME_IN_GC_PCT, base_tig * STORM_BASELINE_MULT)
    if recent_tig is not None and recent_tig > storm_thresh:
        alerts.append({"type": "gc_storm", "severity": "warning",
                       "msg": f"Time-in-GC {recent_tig:.1f}% vs {base_tig:.1f}% baseline (>{storm_thresh:.1f}%)"})
    elif recent_freq is not None and recent_freq > base_freq * GC_FREQ_BASELINE_MULT and recent_freq > GC_FREQ_ALERT_MIN:
        alerts.append({"type": "gc_freq", "severity": "warning",
                       "msg": f"GC frequency {recent_freq:.0f}/min vs {base_freq:.0f}/min baseline"})
    return alerts


def trends(c, instance_id: str, days: int = 30, now: int = None) -> dict:
    """Daily-aggregated series for the per-instance trend charts."""
    now = now if now is not None else now_ts(c)
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
            "heap_used_avg": round(r["heap_used_avg"], 1) if r["heap_used_avg"] is not None else None,
            "heap_used_max": round(r["heap_used_max"], 1) if r["heap_used_max"] is not None else None,
            "heap_after_pct_avg": round(r["heap_after_pct_avg"], 1) if r["heap_after_pct_avg"] is not None else None,
            "pause_p99_max": round(r["pause_p99_max"], 1) if r["pause_p99_max"] is not None else None,
            "pause_max": round(r["pause_max"], 1) if r["pause_max"] is not None else None,
            "full_gc": int(r["full_gc"]) if r["full_gc"] is not None else None,
            "time_in_gc_avg": round(r["time_in_gc_avg"], 2) if r["time_in_gc_avg"] is not None else None,
            "throughput_avg": round(r["throughput_avg"], 2) if r["throughput_avg"] is not None else None,
        })
    heap_max = rows[0]["heap_max_mb"] if rows else None
    return {"instance_id": instance_id, "days": days, "heap_max_mb": heap_max, "series": series}


def hourly_series(c, instance_id: str, hours: int = 48, now: int = None) -> list[dict]:
    """Fine-grained recent series (for the most-recent-window chart)."""
    now = now if now is not None else now_ts(c)
    rows = window_rows(c, instance_id, now - hours * 3600, now)
    return [{"t": r["ts"], "heap_used_mb": r["heap_used_mb"], "heap_after_pct": r["heap_after_pct"],
             "pause_max_ms": r["pause_max_ms"], "time_in_gc_pct": r["time_in_gc_pct"],
             "full_gc": r["full_gc_count"]} for r in rows]


def range_series(c, instance_id: str, since: int, until: int, bucket_s: int) -> dict:
    """Series aggregated into buckets of `bucket_s` seconds over [since, until].

    Same row shape as trends() so the dashboard charts render any time range
    (1h .. 2y) by just changing the bucket size.
    """
    if bucket_s <= 0:
        raise ValueError("bucket_s must be positive")
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
            "heap_used_avg": _aggregate(rs, "heap_used_mb", digits=1),
            "heap_used_max": _aggregate(rs, "heap_used_mb", "max", 1),
            "heap_after_pct_avg": _aggregate(rs, "heap_after_pct", digits=1),
            "pause_p99_max": _aggregate(rs, "pause_p99_ms", "max", 1),
            "pause_max": _aggregate(rs, "pause_max_ms", "max", 1),
            "full_gc": _aggregate(rs, "full_gc_count", "sum", 0),
            "time_in_gc_avg": _aggregate(rs, "time_in_gc_pct"),
            "throughput_avg": _aggregate(rs, "throughput_pct"),
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


def record_gc_collection_quality(c, instance_id: str, ts: int, state: str,
                                 malformed_count: int = 0, unsupported_count: int = 0) -> None:
    """Record one compact outcome per collection time inside the caller's transaction."""
    c.execute(
        "INSERT OR REPLACE INTO gc_collection_quality"
        "(instance_id,ts,state,malformed_count,unsupported_count) VALUES(?,?,?,?,?)",
        (instance_id, ts, state, malformed_count, unsupported_count),
    )


def latest_gc_collection_quality(c, instance_id: str, now: int = None) -> dict | None:
    """Return the outcome known at replay time, not a later failure or recovery."""
    now = now if now is not None else now_ts(c)
    row = c.execute(
        "SELECT ts,state,malformed_count,unsupported_count FROM gc_collection_quality "
        "WHERE instance_id=? AND ts<=? ORDER BY ts DESC LIMIT 1", (instance_id, now),
    ).fetchone()
    return dict(row) if row else None


# --------------------------------------------------------------------------- #
# Admin cluster management + retention
# --------------------------------------------------------------------------- #
def delete_instance(c, instance_id: str) -> None:
    c.execute("DELETE FROM metrics WHERE instance_id=?", (instance_id,))
    c.execute("DELETE FROM gc_collection_quality WHERE instance_id=?", (instance_id,))
    c.execute("DELETE FROM collector_state WHERE instance_id=?", (instance_id,))
    c.execute("DELETE FROM host_metrics WHERE instance_id=?", (instance_id,))
    c.execute("DELETE FROM sar_collector_state WHERE instance_id=?", (instance_id,))
    c.execute("DELETE FROM sar_samples WHERE instance_id=?", (instance_id,))
    c.execute("DELETE FROM instances WHERE id=?", (instance_id,))


def migrate_instance(c, old_id: str, new_id: str) -> None:
    if old_id == new_id:
        return
    # Identity collisions must not discard evidence of a failed collection.
    c.execute(
        "INSERT INTO gc_collection_quality(instance_id,ts,state,malformed_count,unsupported_count) "
        "SELECT ?,ts,state,malformed_count,unsupported_count FROM gc_collection_quality WHERE instance_id=? "
        "ON CONFLICT(instance_id,ts) DO UPDATE SET state=CASE "
        "WHEN gc_collection_quality.state='unknown' OR excluded.state='unknown' THEN 'unknown' "
        "WHEN gc_collection_quality.state='partial' OR excluded.state='partial' THEN 'partial' ELSE 'complete' END, "
        "malformed_count=MAX(gc_collection_quality.malformed_count,excluded.malformed_count), "
        "unsupported_count=MAX(gc_collection_quality.unsupported_count,excluded.unsupported_count)",
        (new_id, old_id),
    )
    c.execute("DELETE FROM gc_collection_quality WHERE instance_id=?", (old_id,))
    samples = [sar_parser.SarSample(ts=r["ts"], **json.loads(r["sample_json"])) for r in c.execute(
        "SELECT ts,sample_json FROM sar_samples WHERE instance_id=? ORDER BY ts", (old_id,))]
    _, affected = record_sar_samples(c, new_id, samples)
    c.execute("DELETE FROM sar_samples WHERE instance_id=?", (old_id,))
    c.execute("UPDATE OR IGNORE metrics SET instance_id=? WHERE instance_id=?", (new_id, old_id))
    c.execute("DELETE FROM metrics WHERE instance_id=?", (old_id,))
    c.execute("UPDATE OR IGNORE collector_state SET instance_id=? WHERE instance_id=?", (new_id, old_id))
    c.execute("DELETE FROM collector_state WHERE instance_id=?", (old_id,))
    c.execute("UPDATE OR IGNORE host_metrics SET instance_id=? WHERE instance_id=?", (new_id, old_id))
    c.execute("DELETE FROM host_metrics WHERE instance_id=?", (old_id,))
    recompute_host_buckets(c, new_id, affected)
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
    c.execute("DELETE FROM gc_collection_quality WHERE ts < ?", (before_ts,))
    # Drop the same whole buckets as host_metrics so a later partial replay
    # cannot resurrect an incomplete rollup from unpruned sample fragments.
    cutoff = ((before_ts + sar_analyzer.HOST_BUCKET_SECONDS - 1)
              // sar_analyzer.HOST_BUCKET_SECONDS * sar_analyzer.HOST_BUCKET_SECONDS)
    c.execute("DELETE FROM sar_samples WHERE ts < ?", (cutoff,))
    return deleted
