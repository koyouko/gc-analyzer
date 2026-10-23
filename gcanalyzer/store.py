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
CREATE TABLE IF NOT EXISTS kafka_metrics (
    ts                              INTEGER,
    instance_id                     TEXT,
    under_replicated_partitions     REAL,
    offline_partitions_count        REAL,
    active_controller_count         REAL,
    isr_shrinks_per_sec             REAL,
    isr_expands_per_sec             REAL,
    unclean_leader_elections_per_sec REAL,
    leader_election_rate_ms         REAL,
    messages_in_per_sec             REAL,
    bytes_in_per_sec                REAL,
    bytes_out_per_sec               REAL,
    request_handler_avg_idle_pct    REAL,
    network_processor_avg_idle_pct  REAL,
    produce_latency_p99_ms          REAL,
    fetch_consumer_latency_p99_ms   REAL,
    fetch_follower_latency_p99_ms   REAL,
    request_queue_size              REAL,
    response_queue_size             REAL,
    partition_count                 REAL,
    leader_count                    REAL,
    log_flush_latency_ms            REAL,
    zk_avg_latency_ms               REAL,
    zk_max_latency_ms               REAL,
    zk_outstanding_requests         REAL,
    zk_alive_connections            REAL,
    zk_pending_syncs                REAL,
    zk_znode_count                  REAL,
    zk_watch_count                  REAL,
    zk_fsync_warning_count          REAL,
    zk_session_expire_count         REAL,
    zk_server_state                 TEXT,
    PRIMARY KEY (instance_id, ts)
);
CREATE INDEX IF NOT EXISTS idx_kafka_metrics_inst_ts ON kafka_metrics(instance_id, ts);
CREATE TABLE IF NOT EXISTS kafka_log_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id TEXT NOT NULL,
    ts          INTEGER NOT NULL,
    level       TEXT NOT NULL,
    category    TEXT NOT NULL,
    component   TEXT NOT NULL,
    summary     TEXT NOT NULL,
    raw_line    TEXT,
    partition   TEXT,
    duration_ms REAL
);
CREATE INDEX IF NOT EXISTS idx_kafka_logs_inst_ts ON kafka_log_events(instance_id, ts);
CREATE TABLE IF NOT EXISTS ml_models (
    instance_id   TEXT NOT NULL,
    model_type    TEXT NOT NULL,
    trained_at    INTEGER NOT NULL,
    sample_count  INTEGER NOT NULL,
    contamination REAL NOT NULL,
    anomaly_rate  REAL NOT NULL,
    params_json   TEXT NOT NULL,
    PRIMARY KEY (instance_id, model_type)
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
    ml_cols = {r[1] for r in c.execute('PRAGMA table_info(ml_models)')}
    if ml_cols and 'instance_id' not in ml_cols:
        c.execute('DROP TABLE ml_models')
        c.execute(
            """CREATE TABLE IF NOT EXISTS ml_models (
                instance_id   TEXT NOT NULL,
                model_type    TEXT NOT NULL,
                trained_at    INTEGER NOT NULL,
                sample_count  INTEGER NOT NULL,
                contamination REAL NOT NULL,
                anomaly_rate  REAL NOT NULL,
                params_json   TEXT NOT NULL,
                PRIMARY KEY (instance_id, model_type)
            )"""
        )


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
                      "SELECT MAX(ts) AS t FROM kafka_metrics UNION ALL "
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
    c.execute("DELETE FROM kafka_metrics WHERE instance_id=?", (instance_id,))
    c.execute("DELETE FROM kafka_log_events WHERE instance_id=?", (instance_id,))
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
    c.execute("DELETE FROM kafka_metrics WHERE ts < ?", (before_ts,))
    c.execute("DELETE FROM kafka_log_events WHERE ts < ?", (before_ts,))
    return deleted


# --------------------------------------------------------------------------- #
# Kafka 2.8.1 & ZooKeeper Metrics + Log Events Store & Real-Time Alerter
# --------------------------------------------------------------------------- #
_KAFKA_METRIC_COLS = (
    "under_replicated_partitions", "offline_partitions_count", "active_controller_count",
    "isr_shrinks_per_sec", "isr_expands_per_sec", "unclean_leader_elections_per_sec",
    "leader_election_rate_ms", "messages_in_per_sec", "bytes_in_per_sec", "bytes_out_per_sec",
    "request_handler_avg_idle_pct", "network_processor_avg_idle_pct",
    "produce_latency_p99_ms", "fetch_consumer_latency_p99_ms", "fetch_follower_latency_p99_ms",
    "request_queue_size", "response_queue_size", "partition_count", "leader_count",
    "log_flush_latency_ms", "zk_avg_latency_ms", "zk_max_latency_ms",
    "zk_outstanding_requests", "zk_alive_connections", "zk_pending_syncs",
    "zk_znode_count", "zk_watch_count", "zk_fsync_warning_count",
    "zk_session_expire_count", "zk_server_state",
)


def record_kafka_metric(c, instance_id: str, ts: int, m: dict) -> None:
    cols = ["ts", "instance_id"] + list(_KAFKA_METRIC_COLS)
    vals = [ts, instance_id] + [m.get(col) for col in _KAFKA_METRIC_COLS]
    placeholders = ",".join("?" for _ in cols)
    c.execute(f"INSERT OR REPLACE INTO kafka_metrics({','.join(cols)}) VALUES({placeholders})", vals)


def record_kafka_log_events(c, instance_id: str, events: list, default_ts: int | None = None) -> int:
    written = 0
    for ev in events:
        d = ev if isinstance(ev, dict) else asdict(ev)
        c.execute(
            "INSERT INTO kafka_log_events(instance_id,ts,level,category,component,summary,raw_line,partition,duration_ms)"
            " VALUES(?,?,?,?,?,?,?,?,?)",
            (
                instance_id,
                int(d.get("ts") or default_ts or time.time()),
                d.get("level", "WARN"),
                d.get("category", "general"),
                d.get("component", "broker"),
                d.get("summary", ""),
                d.get("raw_line", ""),
                d.get("partition"),
                d.get("duration_ms"),
            ),
        )
        written += 1
    return written


def kafka_latest_row(c, instance_id: str, now: int = None) -> dict | None:
    q = "SELECT * FROM kafka_metrics WHERE instance_id=?"
    args = [instance_id]
    if now is not None:
        q += " AND ts<=?"
        args.append(now)
    q += " ORDER BY ts DESC LIMIT 1"
    r = c.execute(q, args).fetchone()
    return dict(r) if r else None


def kafka_window_rows(c, instance_id: str, since_ts: int, until_ts: int = None) -> list[dict]:
    q = "SELECT * FROM kafka_metrics WHERE instance_id=? AND ts>=?"
    args = [instance_id, since_ts]
    if until_ts is not None:
        q += " AND ts<=?"
        args.append(until_ts)
    q += " ORDER BY ts ASC"
    return [dict(r) for r in c.execute(q, args)]


def kafka_log_events_window(c, instance_id: str | None = None, cluster: str | None = None, since_ts: int = 0, limit: int = 120) -> list[dict]:
    if instance_id:
        rows = c.execute(
            "SELECT * FROM kafka_log_events WHERE instance_id=? AND ts>=? ORDER BY ts DESC LIMIT ?",
            (instance_id, since_ts, limit),
        ).fetchall()
        return [dict(r) for r in rows]
    if cluster:
        rows = c.execute(
            "SELECT e.* FROM kafka_log_events e JOIN instances i ON e.instance_id=i.id "
            "WHERE i.cluster=? AND e.ts>=? ORDER BY e.ts DESC LIMIT ?",
            (cluster, since_ts, limit),
        ).fetchall()
        return [dict(r) for r in rows]
    rows = c.execute(
        "SELECT * FROM kafka_log_events WHERE ts>=? ORDER BY ts DESC LIMIT ?",
        (since_ts, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def kafka_range_series(c, instance_id: str, since: int, until: int, bucket_s: int) -> dict:
    if bucket_s <= 0:
        raise ValueError("bucket_s must be positive")
    rows = kafka_window_rows(c, instance_id, since, until)
    buckets: dict[int, list[dict]] = {}
    for r in rows:
        b = (r["ts"] // bucket_s) * bucket_s
        buckets.setdefault(b, []).append(r)

    series = []
    for b in sorted(buckets):
        rs = buckets[b]
        series.append({
            "t": b,
            "urp_max": _aggregate(rs, "under_replicated_partitions", "max", 0),
            "offline_max": _aggregate(rs, "offline_partitions_count", "max", 0),
            "isr_shrink_avg": _aggregate(rs, "isr_shrinks_per_sec", "avg", 2),
            "isr_expand_avg": _aggregate(rs, "isr_expands_per_sec", "avg", 2),
            "messages_in_avg": _aggregate(rs, "messages_in_per_sec", "avg", 1),
            "bytes_in_avg": _aggregate(rs, "bytes_in_per_sec", "avg", 1),
            "bytes_out_avg": _aggregate(rs, "bytes_out_per_sec", "avg", 1),
            "req_idle_avg": _aggregate(rs, "request_handler_avg_idle_pct", "avg", 1),
            "net_idle_avg": _aggregate(rs, "network_processor_avg_idle_pct", "avg", 1),
            "produce_p99_max": _aggregate(rs, "produce_latency_p99_ms", "max", 1),
            "fetch_consumer_p99_max": _aggregate(rs, "fetch_consumer_latency_p99_ms", "max", 1),
            "fetch_follower_p99_max": _aggregate(rs, "fetch_follower_latency_p99_ms", "max", 1),
            "req_queue_max": _aggregate(rs, "request_queue_size", "max", 0),
            "zk_latency_avg": _aggregate(rs, "zk_avg_latency_ms", "avg", 1),
            "zk_latency_max": _aggregate(rs, "zk_max_latency_ms", "max", 1),
            "zk_outstanding_max": _aggregate(rs, "zk_outstanding_requests", "max", 0),
            "zk_fsync_sum": _aggregate(rs, "zk_fsync_warning_count", "sum", 0),
            "zk_session_expire_sum": _aggregate(rs, "zk_session_expire_count", "sum", 0),
        })
    return {"instance_id": instance_id, "bucket_s": bucket_s, "series": series}


def current_kafka_snapshot(c, instance_id: str, now: int = None) -> dict | None:
    from . import kafka_analyzer

    inst = get_instance(c, instance_id)
    if not inst:
        return None
    now = now if now is not None else now_ts(c)
    last = kafka_latest_row(c, instance_id, now)
    day = kafka_window_rows(c, instance_id, now - 86400, now)
    logs = kafka_log_events_window(c, instance_id=instance_id, since_ts=now - 86400, limit=80)
    log_counts: dict[str, int] = {}
    for ev in logs:
        cat = ev.get("category", "general")
        log_counts[cat] = log_counts.get(cat, 0) + 1

    if not last and not day:
        return {
            "instance": inst,
            "latest": None,
            "metrics": {},
            "health": _unknown_health(),
            "findings": {"pros": [], "cons": [], "recommendations": [], "root_causes": []},
            "log_events": logs,
            "log_counts": log_counts,
            "quality": {"state": "missing", "last_observed_at": None},
        }

    m = dict(last or (day[-1] if day else {}))
    if day:
        m["urp_24h_max"] = _aggregate(day, "under_replicated_partitions", "max", 0)
        m["offline_24h_max"] = _aggregate(day, "offline_partitions_count", "max", 0)
        m["produce_p99_24h_max"] = _aggregate(day, "produce_latency_p99_ms", "max", 1)
        m["zk_fsync_24h_sum"] = _aggregate(day, "zk_fsync_warning_count", "sum", 0)
        m["zk_session_expire_24h_sum"] = _aggregate(day, "zk_session_expire_count", "sum", 0)

    gc_snap = current_snapshot(c, instance_id, now) or {}
    host_snap = current_host_snapshot(c, instance_id, now) or {}
    health = kafka_analyzer.score_kafka_health(m, role=inst["role"], log_counts=log_counts)
    findings = kafka_analyzer.derive_kafka_findings(
        m,
        role=inst["role"],
        gc_metrics=gc_snap.get("metrics") or {},
        host_metrics=host_snap.get("metrics") or {},
        log_counts=log_counts,
    )
    age = max(0, now - m.get("ts", now))
    return {
        "instance": inst,
        "latest": last,
        "metrics": m,
        "health": health,
        "findings": findings,
        "log_events": logs,
        "log_counts": log_counts,
        "quality": {"state": "fresh" if age <= 7200 else "stale", "last_observed_at": m.get("ts"), "age_seconds": age},
    }


def cluster_kafka_overview(c, cluster: str, now: int = None, range_s: int = 86400, bucket_s: int = 3600) -> dict | None:
    """Build a complete Kafka 2.8.1 + ZooKeeper cluster analytics view."""
    from . import prometheus_settings

    now = now if now is not None else now_ts(c)
    insts = [i for i in list_instances(c) if i["cluster"] == cluster]
    if not insts:
        return None

    brokers = []
    zookeepers = []
    other_nodes = []

    total_urp = 0
    total_offline = 0
    active_controllers = 0
    controller_broker = None
    total_msg_in = 0.0
    total_bytes_in = 0.0
    total_bytes_out = 0.0
    total_partitions = 0
    total_leaders = 0
    worst_produce_p99 = 0.0
    worst_fetch_p99 = 0.0
    min_req_idle = 100.0
    min_net_idle = 100.0
    zk_avg_lat_vals = []
    zk_max_lat_worst = 0.0
    zk_outstanding_total = 0
    zk_fsync_total = 0
    zk_sess_expire_total = 0
    zk_leader_node = None
    all_root_causes = []

    # Time-series rollup across cluster
    cluster_buckets: dict[int, dict] = {}
    since = now - range_s

    for inst in insts:
        snap = current_kafka_snapshot(c, inst["id"], now) or {}
        km = snap.get("metrics") or {}
        kh = snap.get("health") or _unknown_health()
        kf = snap.get("findings") or {}
        gc_snap = current_snapshot(c, inst["id"], now) or {}
        host_snap = current_host_snapshot(c, inst["id"], now) or {}
        hm = host_snap.get("metrics") or {}
        gm = gc_snap.get("metrics") or {}

        for rc in kf.get("root_causes") or []:
            all_root_causes.append({"instance_id": inst["id"], "node_id": inst.get("node_id") or inst["id"], "role": inst["role"], **rc})

        node_entry = {
            "id": inst["id"],
            "node_id": inst.get("node_id") or inst["id"],
            "role": inst["role"],
            "kafka_health": kh,
            "gc_grade": (gc_snap.get("health") or {}).get("grade", "?"),
            "host_grade": (host_snap.get("health") or {}).get("grade", "?"),
            "cpu_busy_pct": hm.get("cpu_busy_pct_avg"),
            "mem_used_pct": hm.get("mem_used_pct_avg"),
            "disk_await_ms": hm.get("disk_await_ms_max"),
            "max_pause_ms": gm.get("max_pause_ms"),
            "metrics": km,
            "log_counts": snap.get("log_counts") or {},
        }

        if inst["role"] == "broker":
            urp = int(km.get("under_replicated_partitions") or 0)
            off = int(km.get("offline_partitions_count") or 0)
            ctrl = int(km.get("active_controller_count") or 0)
            total_urp += urp
            total_offline += off
            active_controllers += ctrl
            if ctrl == 1:
                controller_broker = inst.get("node_id") or inst["id"]
            total_msg_in += float(km.get("messages_in_per_sec") or 0.0)
            total_bytes_in += float(km.get("bytes_in_per_sec") or 0.0)
            total_bytes_out += float(km.get("bytes_out_per_sec") or 0.0)
            total_partitions += int(km.get("partition_count") or 0)
            total_leaders += int(km.get("leader_count") or 0)
            worst_produce_p99 = max(worst_produce_p99, float(km.get("produce_latency_p99_ms") or 0.0))
            worst_fetch_p99 = max(worst_fetch_p99, float(km.get("fetch_consumer_latency_p99_ms") or 0.0))
            if km.get("request_handler_avg_idle_pct") is not None:
                min_req_idle = min(min_req_idle, float(km["request_handler_avg_idle_pct"]))
            if km.get("network_processor_avg_idle_pct") is not None:
                min_net_idle = min(min_net_idle, float(km["network_processor_avg_idle_pct"]))
            brokers.append(node_entry)
        elif inst["role"] == "zookeeper":
            if km.get("zk_avg_latency_ms") is not None:
                zk_avg_lat_vals.append(float(km["zk_avg_latency_ms"]))
            zk_max_lat_worst = max(zk_max_lat_worst, float(km.get("zk_max_latency_ms") or 0.0))
            zk_outstanding_total += int(km.get("zk_outstanding_requests") or 0)
            zk_fsync_total += int(km.get("zk_fsync_warning_count") or 0) + (snap.get("log_counts") or {}).get("zk_fsync_stall", 0)
            zk_sess_expire_total += int(km.get("zk_session_expire_count") or 0) + (snap.get("log_counts") or {}).get("zk_session_expired", 0)
            if km.get("zk_server_state") == "leader":
                zk_leader_node = inst.get("node_id") or inst["id"]
            zookeepers.append(node_entry)
        else:
            other_nodes.append(node_entry)

        # Accumulate time-series
        ser = kafka_range_series(c, inst["id"], since, now, bucket_s)["series"]
        for pt in ser:
            t = pt["t"]
            b = cluster_buckets.setdefault(t, {
                "t": t, "urp": 0, "offline": 0, "isr_shrink": 0.0, "isr_expand": 0.0,
                "messages_in": 0.0, "bytes_in": 0.0, "bytes_out": 0.0,
                "produce_p99": 0.0, "fetch_p99": 0.0, "req_idle": [], "net_idle": [],
                "zk_latency_avg": [], "zk_latency_max": 0.0, "zk_outstanding": 0, "zk_fsync": 0,
            })
            if inst["role"] == "broker":
                b["urp"] += int(pt.get("urp_max") or 0)
                b["offline"] += int(pt.get("offline_max") or 0)
                b["isr_shrink"] = round(b["isr_shrink"] + float(pt.get("isr_shrink_avg") or 0.0), 3)
                b["isr_expand"] = round(b["isr_expand"] + float(pt.get("isr_expand_avg") or 0.0), 3)
                b["messages_in"] = round(b["messages_in"] + float(pt.get("messages_in_avg") or 0.0), 1)
                b["bytes_in"] = round(b["bytes_in"] + float(pt.get("bytes_in_avg") or 0.0), 1)
                b["bytes_out"] = round(b["bytes_out"] + float(pt.get("bytes_out_avg") or 0.0), 1)
                b["produce_p99"] = max(b["produce_p99"], float(pt.get("produce_p99_max") or 0.0))
                b["fetch_p99"] = max(b["fetch_p99"], float(pt.get("fetch_consumer_p99_max") or 0.0))
                if pt.get("req_idle_avg") is not None:
                    b["req_idle"].append(float(pt["req_idle_avg"]))
                if pt.get("net_idle_avg") is not None:
                    b["net_idle"].append(float(pt["net_idle_avg"]))
            elif inst["role"] == "zookeeper":
                if pt.get("zk_latency_avg") is not None:
                    b["zk_latency_avg"].append(float(pt["zk_latency_avg"]))
                b["zk_latency_max"] = max(b["zk_latency_max"], float(pt.get("zk_latency_max") or 0.0))
                b["zk_outstanding"] += int(pt.get("zk_outstanding_max") or 0)
                b["zk_fsync"] += int(pt.get("zk_fsync_sum") or 0)

    series = []
    for t in sorted(cluster_buckets):
        b = cluster_buckets[t]
        series.append({
            "t": t,
            "urp": b["urp"],
            "offline": b["offline"],
            "isr_shrink": b["isr_shrink"],
            "isr_expand": b["isr_expand"],
            "messages_in": b["messages_in"],
            "bytes_in_mbs": round(b["bytes_in"] / (1024 * 1024), 2),
            "bytes_out_mbs": round(b["bytes_out"] / (1024 * 1024), 2),
            "produce_p99": round(b["produce_p99"], 1),
            "fetch_p99": round(b["fetch_p99"], 1),
            "req_idle_min": round(min(b["req_idle"]), 1) if b["req_idle"] else None,
            "net_idle_min": round(min(b["net_idle"]), 1) if b["net_idle"] else None,
            "zk_latency_avg": round(statistics.fmean(b["zk_latency_avg"]), 2) if b["zk_latency_avg"] else None,
            "zk_latency_max": round(b["zk_latency_max"], 1),
            "zk_outstanding": b["zk_outstanding"],
            "zk_fsync": b["zk_fsync"],
        })

    # Compute leader skew across brokers
    leader_counts = [int((b["metrics"].get("leader_count") or 0)) for b in brokers if b["metrics"].get("leader_count") is not None]
    leader_skew_pct = 0.0
    if len(leader_counts) >= 2 and statistics.fmean(leader_counts) > 0:
        leader_skew_pct = round((max(leader_counts) - min(leader_counts)) / statistics.fmean(leader_counts) * 100.0, 1)

    prom_info = {"configured": False, "base_url": "", "status": "not_configured"}
    try:
        desc = prometheus_settings.describe()
        prom_info = {
            "configured": desc.get("configured", False),
            "base_url": (desc.get("config") or {}).get("base_url", ""),
            "status": (desc.get("connection") or {}).get("status", "not_tested"),
        }
    except Exception:
        pass

    logs = kafka_log_events_window(c, cluster=cluster, since_ts=now - 86400, limit=100)
    consensus_mode = "ZooKeeper (Kafka 2.8.1)" if zookeepers else "KRaft"

    overall_status = "ok"
    if total_offline > 0 or total_urp > 0 or active_controllers != 1 or zk_fsync_total > 0:
        overall_status = "critical"
    elif min_req_idle < 35.0 or worst_produce_p99 >= 120.0 or zk_sess_expire_total > 0:
        overall_status = "watch"

    return {
        "cluster": cluster,
        "region": insts[0]["region"],
        "env": insts[0]["env"],
        "kafka_version": "2.8.1",
        "consensus_mode": consensus_mode,
        "status": overall_status,
        "now": now,
        "prometheus": prom_info,
        "summary": {
            "under_replicated_partitions": total_urp,
            "total_urp": total_urp,
            "offline_partitions_count": total_offline,
            "active_controller_count": active_controllers,
            "controller_broker": controller_broker or "broker-1",
            "messages_in_per_sec": round(total_msg_in, 1),
            "bytes_in_mb_sec": round(total_bytes_in / (1024 * 1024), 2),
            "bytes_out_mb_sec": round(total_bytes_out / (1024 * 1024), 2),
            "total_partitions": total_partitions,
            "total_leaders": total_leaders,
            "leader_skew_pct": leader_skew_pct,
            "worst_produce_p99_ms": round(worst_produce_p99, 1),
            "worst_fetch_p99_ms": round(worst_fetch_p99, 1),
            "min_request_handler_idle_pct": round(min_req_idle, 1) if min_req_idle <= 100 else None,
            "min_network_processor_idle_pct": round(min_net_idle, 1) if min_net_idle <= 100 else None,
            "zk_quorum_size": len(zookeepers),
            "zk_leader": zk_leader_node or ("zookeeper-1" if zookeepers else None),
            "zk_avg_latency_ms": round(statistics.fmean(zk_avg_lat_vals), 2) if zk_avg_lat_vals else None,
            "zk_max_latency_ms": round(zk_max_lat_worst, 1) if zookeepers else None,
            "zk_outstanding_requests": zk_outstanding_total,
            "zk_fsync_warnings_24h": zk_fsync_total,
            "zk_session_expires_24h": zk_sess_expire_total,
        },
        "brokers": brokers,
        "zookeepers": zookeepers,
        "other_nodes": other_nodes,
        "root_causes": all_root_causes,
        "log_events": logs,
        "series": series,
    }


def evaluate_realtime_alerts(c, now: int = None, cluster: str | None = None) -> dict:
    """Real-time active alerter across Host CPU/Memory/Swap/Disk, Kafka 2.8.1 URP/ZK, and JVM GC."""
    now = now if now is not None else now_ts(c)
    insts = list_instances(c)
    if cluster:
        insts = [i for i in insts if i["cluster"] == cluster]
    alerts = []

    for inst in insts:
        iid = inst["id"]
        node_label = inst.get("node_id") or iid
        cluster = inst["cluster"]
        role = inst["role"]

        # 1. Host CPU / Memory / Swap / Disk Damage & Saturation Checks (Immediate Real-Time)
        h_last = host_latest_row(c, iid, now)
        if h_last and (now - h_last.get("ts", 0)) <= 7200:
            cpu_busy = h_last.get("cpu_busy_pct")
            iowait = h_last.get("cpu_iowait_pct")
            mem_used = h_last.get("mem_used_pct")
            swap_used = h_last.get("swap_used_pct")
            pswpout = h_last.get("pswpout_per_s")
            majflt = h_last.get("majflt_per_s")
            disk_util = h_last.get("disk_util_pct_max")
            disk_await = h_last.get("disk_await_ms_max")

            if sar_analyzer._numeric(cpu_busy):
                if cpu_busy >= 90.0:
                    alerts.append({
                        "id": f"host-cpu-crit-{iid}",
                        "ts": h_last["ts"],
                        "layer": "host",
                        "category": "cpu_damage",
                        "severity": "critical",
                        "cluster": cluster,
                        "instance_id": iid,
                        "node_id": node_label,
                        "role": role,
                        "metric": "cpu_busy_pct",
                        "value": round(cpu_busy, 1),
                        "unit": "%",
                        "title": f"Host CPU Critical Saturation ({cpu_busy:.1f}%)",
                        "msg": f"Host {node_label} ({cluster}) CPU is critically saturated at {cpu_busy:.1f}% (>= 90% threshold) — JVM threads & Kafka request handlers starved.",
                    })
                elif cpu_busy >= 75.0:
                    alerts.append({
                        "id": f"host-cpu-warn-{iid}",
                        "ts": h_last["ts"],
                        "layer": "host",
                        "category": "cpu_pressure",
                        "severity": "warning",
                        "cluster": cluster,
                        "instance_id": iid,
                        "node_id": node_label,
                        "role": role,
                        "metric": "cpu_busy_pct",
                        "value": round(cpu_busy, 1),
                        "unit": "%",
                        "title": f"Host CPU High Load ({cpu_busy:.1f}%)",
                        "msg": f"Host {node_label} ({cluster}) CPU busy is at {cpu_busy:.1f}% (> 75% warning threshold).",
                    })

            if sar_analyzer._numeric(mem_used):
                if mem_used >= 92.0 or (sar_analyzer._numeric(pswpout) and pswpout > 0.5) or (sar_analyzer._numeric(swap_used) and swap_used >= 2.0):
                    alerts.append({
                        "id": f"host-mem-crit-{iid}",
                        "ts": h_last["ts"],
                        "layer": "host",
                        "category": "memory_damage",
                        "severity": "critical",
                        "cluster": cluster,
                        "instance_id": iid,
                        "node_id": node_label,
                        "role": role,
                        "metric": "mem_used_pct",
                        "value": round(mem_used, 1),
                        "unit": "%",
                        "title": f"Host Memory / Swap Emergency ({mem_used:.1f}% RAM, {swap_used or 0:.1f}% Swap)",
                        "msg": f"Host {node_label} ({cluster}) is under severe memory pressure ({mem_used:.1f}% RAM used, swap {swap_used or 0:.1f}%, swap-out {pswpout or 0:.1f} pg/s, majflt {majflt or 0:.0f}/s).",
                    })
                elif mem_used >= 85.0 or (sar_analyzer._numeric(swap_used) and swap_used >= 0.5):
                    alerts.append({
                        "id": f"host-mem-warn-{iid}",
                        "ts": h_last["ts"],
                        "layer": "host",
                        "category": "memory_pressure",
                        "severity": "warning",
                        "cluster": cluster,
                        "instance_id": iid,
                        "node_id": node_label,
                        "role": role,
                        "metric": "mem_used_pct",
                        "value": round(mem_used, 1),
                        "unit": "%",
                        "title": f"Host Memory Pressure ({mem_used:.1f}%)",
                        "msg": f"Host {node_label} ({cluster}) memory usage at {mem_used:.1f}% (swap {swap_used or 0:.1f}%) — OS page cache shrinking.",
                    })

            if (sar_analyzer._numeric(disk_await) and disk_await >= 35.0) or (sar_analyzer._numeric(iowait) and iowait >= 15.0) or (sar_analyzer._numeric(disk_util) and disk_util >= 90.0):
                sev = "critical" if (disk_await or 0) >= 50.0 or (iowait or 0) >= 20.0 else "warning"
                alerts.append({
                    "id": f"host-disk-{iid}",
                    "ts": h_last["ts"],
                    "layer": "host",
                    "category": "disk_io_damage",
                    "severity": sev,
                    "cluster": cluster,
                    "instance_id": iid,
                    "node_id": node_label,
                    "role": role,
                    "metric": "disk_await_ms_max",
                    "value": round(disk_await or 0, 1),
                    "unit": "ms",
                    "title": f"Host Storage I/O Stall ({disk_await or 0:.0f} ms await, {iowait or 0:.1f}% iowait)",
                    "msg": f"Host {node_label} ({cluster}) storage subsystem bottleneck: disk await {disk_await or 0:.1f} ms, disk util {disk_util or 0:.0f}%, CPU iowait {iowait or 0:.1f}%.",
                })

        # 2. Kafka 2.8.1 Broker & ZooKeeper Real-Time Alerts
        k_last = kafka_latest_row(c, iid, now)
        if k_last and (now - k_last.get("ts", 0)) <= 7200:
            urp = k_last.get("under_replicated_partitions") or 0
            offline = k_last.get("offline_partitions_count") or 0
            req_idle = k_last.get("request_handler_avg_idle_pct")
            prod_p99 = k_last.get("produce_latency_p99_ms")
            zk_fsync = k_last.get("zk_fsync_warning_count") or 0
            zk_lat = k_last.get("zk_avg_latency_ms")
            zk_sess = k_last.get("zk_session_expire_count") or 0

            if offline > 0:
                alerts.append({
                    "id": f"kafka-offline-{iid}",
                    "ts": k_last["ts"],
                    "layer": "kafka",
                    "category": "offline_partitions",
                    "severity": "critical",
                    "cluster": cluster,
                    "instance_id": iid,
                    "node_id": node_label,
                    "role": role,
                    "metric": "offline_partitions_count",
                    "value": int(offline),
                    "unit": "partitions",
                    "title": f"Kafka Offline Partitions ({int(offline)})",
                    "msg": f"Broker {node_label} ({cluster}) reports {int(offline)} Offline Partition(s) — clients cannot produce/consume to affected partitions!",
                })
            if urp > 0:
                alerts.append({
                    "id": f"kafka-urp-{iid}",
                    "ts": k_last["ts"],
                    "layer": "kafka",
                    "category": "urp",
                    "severity": "critical",
                    "cluster": cluster,
                    "instance_id": iid,
                    "node_id": node_label,
                    "role": role,
                    "metric": "under_replicated_partitions",
                    "value": int(urp),
                    "unit": "URPs",
                    "title": f"Kafka Under-Replicated Partitions (URP = {int(urp)})",
                    "msg": f"Broker {node_label} ({cluster}) has {int(urp)} Under-Replicated Partition(s) — ISR shrunk due to replica lag or GC/host stall.",
                })
            if sar_analyzer._numeric(req_idle) and req_idle < 25.0:
                alerts.append({
                    "id": f"kafka-threads-{iid}",
                    "ts": k_last["ts"],
                    "layer": "kafka",
                    "category": "thread_starvation",
                    "severity": "critical" if req_idle < 15.0 else "warning",
                    "cluster": cluster,
                    "instance_id": iid,
                    "node_id": node_label,
                    "role": role,
                    "metric": "request_handler_avg_idle_pct",
                    "value": round(req_idle, 1),
                    "unit": "% idle",
                    "title": f"Kafka I/O Request Handler Pool Starved ({req_idle:.1f}% idle)",
                    "msg": f"Broker {node_label} ({cluster}) RequestHandlerAvgIdlePercent dropped to {req_idle:.1f}% (Produce p99: {prod_p99 or 0:.0f} ms).",
                })
            if role == "zookeeper" and (zk_fsync > 0 or (sar_analyzer._numeric(zk_lat) and zk_lat >= 25.0) or zk_sess > 0):
                sev = "critical" if zk_fsync > 0 or (zk_lat or 0) >= 50.0 or zk_sess >= 2 else "warning"
                alerts.append({
                    "id": f"zk-quorum-{iid}",
                    "ts": k_last["ts"],
                    "layer": "zookeeper",
                    "category": "zk_quorum_stall",
                    "severity": sev,
                    "cluster": cluster,
                    "instance_id": iid,
                    "node_id": node_label,
                    "role": role,
                    "metric": "zk_avg_latency_ms",
                    "value": round(zk_lat or 0, 1),
                    "unit": "ms",
                    "title": f"ZooKeeper WAL fsync / Session Alert ({int(zk_fsync)} fsync stalls, {int(zk_sess)} expired)",
                    "msg": f"ZooKeeper {node_label} ({cluster}) avg latency {zk_lat or 0:.1f} ms, {int(zk_fsync)} WAL fsync stall warning(s), {int(zk_sess)} session expiration(s).",
                })

        # 3. JVM GC Real-Time Alerts
        for ga in evaluate_alerts(c, iid, now):
            alerts.append({
                "id": f"gc-{ga['type']}-{iid}",
                "ts": now,
                "layer": "jvm",
                "category": ga["type"],
                "severity": ga["severity"],
                "cluster": cluster,
                "instance_id": iid,
                "node_id": node_label,
                "role": role,
                "metric": ga["type"],
                "value": None,
                "unit": "",
                "title": f"JVM GC Alert: {ga['type'].replace('_', ' ').title()}",
                "msg": f"{node_label} ({cluster}): {ga['msg']}",
            })

    alerts.sort(key=lambda a: (0 if a["severity"] == "critical" else 1, -int(a.get("ts") or 0)))
    crit_count = sum(1 for a in alerts if a["severity"] == "critical")
    warn_count = sum(1 for a in alerts if a["severity"] == "warning")
    by_layer = {
        "host": sum(1 for a in alerts if a["layer"] == "host"),
        "kafka": sum(1 for a in alerts if a["layer"] == "kafka"),
        "zookeeper": sum(1 for a in alerts if a["layer"] == "zookeeper"),
        "jvm": sum(1 for a in alerts if a["layer"] == "jvm"),
    }
    return {
        "now": now,
        "status": "critical" if crit_count > 0 else "warning" if warn_count > 0 else "ok",
        "critical_count": crit_count,
        "warning_count": warn_count,
        "total_count": len(alerts),
        "by_layer": by_layer,
        "alerts": alerts,
    }

