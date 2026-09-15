"""
Host resource analysis engine.

Turns a ParsedSar (sar_parser.py) into the OS-level numbers an operator cares
about when deciding whether a Kafka node's JVM problems are actually a *host*
problem: CPU saturation, iowait, memory/swap pressure, disk and NIC busy-ness,
run-queue depth. Mirrors analyzer.py's shape (metrics / health / findings /
timeline) deliberately so the dashboard and store.py can treat GC health and
host health the same way.

This module never looks at GC data — correlate.py is where GC and host
metrics are joined. Keeping them separate means host health stays meaningful
even for nodes where GC log collection isn't configured.
"""

from __future__ import annotations

import statistics
import math

from .sar_parser import ParsedSar, SarSample


CPU_BUSY_WARN_PCT = 75.0
CPU_BUSY_CRIT_PCT = 90.0
IOWAIT_WARN_PCT = 8.0
IOWAIT_CRIT_PCT = 20.0
MEM_WARN_PCT = 85.0
MEM_CRIT_PCT = 95.0
SWAP_WARN_PCT = 1.0          # any meaningful swap activity is a red flag for a Kafka broker
SWAP_CRIT_PCT = 5.0
DISK_UTIL_WARN_PCT = 70.0
DISK_UTIL_CRIT_PCT = 90.0
DISK_AWAIT_WARN_MS = 20.0
DISK_AWAIT_CRIT_MS = 50.0
NET_UTIL_WARN_PCT = 70.0
NET_UTIL_CRIT_PCT = 90.0

# These independent pressure signals are required for an overall host grade.
HEALTH_METRICS = ("cpu_busy_pct", "cpu_iowait_pct", "mem_used_pct", "swap_used_pct",
                  "disk_util_pct_max", "disk_await_ms_max", "net_util_pct_max")
HOST_BUCKET_SECONDS = 60


def _numeric(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _avg(vals: list[float]) -> float | None:
    vals = [v for v in vals if _numeric(v)]
    return round(statistics.fmean(vals), 2) if vals else None


def _max(vals: list[float]) -> float | None:
    vals = [v for v in vals if _numeric(v)]
    return round(max(vals), 2) if vals else None


def _sum(vals) -> float | None:
    vals = [v for v in vals if _numeric(v)]
    return sum(vals) if vals else None


def _top_devices(samples: list[SarSample], key: str, field: str, util_field: str, n: int = 3) -> list[dict]:
    """Aggregate per-device stats across samples, return the busiest `n` by avg util."""
    agg: dict[str, dict] = {}
    for s in samples:
        for item in getattr(s, field):
            name = item.get(key, "?")
            a = agg.setdefault(name, {key: name, "_util": [], "_extra": {}})
            a["_util"].append(item.get(util_field))
            for k, v in item.items():
                if k in (key, util_field):
                    continue
                a["_extra"].setdefault(k, []).append(v)

    rows = []
    for name, a in agg.items():
        row = {key: name, "util_pct_avg": _avg(a["_util"]), "util_pct_max": _max(a["_util"])}
        for k, vals in a["_extra"].items():
            row[f"{k}_avg"] = _avg(vals)
        rows.append(row)
    rows.sort(key=lambda r: r["util_pct_avg"] if r["util_pct_avg"] is not None else -1, reverse=True)
    return rows[:n]


def analyze(parsed: ParsedSar) -> dict:
    samples = parsed.samples
    metrics = _rollup(samples)
    health = _health_score(metrics)
    findings = _findings(metrics)
    timeline = _timeline(samples)

    return {
        "node_id": parsed.node_id,
        "source_format": parsed.source_format,
        "hostname": parsed.hostname,
        "warnings": parsed.warnings,
        "metrics": metrics,
        "health": health,
        "findings": findings,
        "timeline": timeline,
        "quality": metric_quality(metrics),
    }


def _rollup(samples: list[SarSample]) -> dict:
    if not samples:
        return {"sample_count": 0}

    samples = sorted(samples, key=lambda s: s.ts)
    columns = (
        "cpu_user_pct", "cpu_system_pct", "cpu_iowait_pct", "cpu_steal_pct",
        "load1", "load5", "load15", "runq_sz", "plist_sz", "blocked", "proc_per_s",
        "cswch_per_s", "mem_used_pct", "mem_avail_mb", "mem_cached_mb", "mem_commit_pct",
        "swap_used_pct", "pgpgin_kbs", "pgpgout_kbs", "fault_per_s", "majflt_per_s",
        "pswpin_per_s", "pswpout_per_s",
    )
    values = {col: [getattr(s, col) for s in samples] for col in columns}
    values["cpu_busy_pct"] = [100 - s.cpu_idle_pct if _numeric(s.cpu_idle_pct) else None for s in samples]
    for col, field, measure, aggregate in (
        ("disk_util_pct_max", "disks", "util_pct", _max),
        ("disk_await_ms_max", "disks", "await_ms", _max),
        ("disk_tps", "disks", "tps", _sum),
        ("net_util_pct_max", "nics", "util_pct", _max),
        ("net_rx_kbs", "nics", "rx_kbs", _sum),
        ("net_tx_kbs", "nics", "tx_kbs", _sum),
    ):
        values[col] = [aggregate([d.get(measure) for d in getattr(s, field)]) for s in samples]

    intervals = [s.interval_seconds for s in samples]
    # If duration is unavailable, use sample weights consistently for the bucket.
    known_duration = all(_numeric(d) and d > 0 for d in intervals)
    weights = intervals if known_duration else [1] * len(samples)
    m = {
        "sample_count": len(samples), "span_seconds": samples[-1].ts - samples[0].ts,
        "first_observed_at": samples[0].ts, "last_observed_at": samples[-1].ts,
        "duration_seconds": sum(intervals) if known_duration else None,
        "metric_stats": {},
        "top_disks": _top_devices(samples, "dev", "disks", "util_pct"),
        "top_nics": _top_devices(samples, "iface", "nics", "util_pct"),
    }
    for col, vals in values.items():
        observed = [(v, w) for v, w in zip(vals, weights) if _numeric(v)]
        weight = sum(w for _, w in observed)
        total = sum(v * w for v, w in observed)
        peak = _max(vals)
        avg = round(total / weight, 2) if weight else None
        m["metric_stats"][col] = {"sum": total, "weight": weight, "count": len(observed),
                                    "max": peak, "sample_sum": _sum(vals)}
        if col.endswith("_max"):
            m[col] = peak
        else:
            m[f"{col}_avg"] = avg
            m[f"{col}_max"] = peak
    return m


def metric_quality(m: dict) -> dict:
    available = [col for col in HEALTH_METRICS
                 if _numeric(m.get(col if col.endswith("_max") else f"{col}_max"))]
    missing = [col for col in HEALTH_METRICS if col not in available]
    counts = m.get("metric_stats") or {}
    incomplete = any(counts.get(col, {}).get("count", m.get("sample_count", 0)) < m.get("sample_count", 0)
                     for col in HEALTH_METRICS)
    state = "missing" if not available else "partial" if missing or incomplete else "fresh"
    return {"state": state, "last_observed_at": m.get("last_observed_at"), "age_seconds": None,
            "available_metrics": available, "missing_metrics": missing}


def _above(m, key, threshold):
    return _numeric(m.get(key)) and m[key] > threshold


def _display(value, suffix=""):
    return f"{value:.0f}{suffix}" if _numeric(value) else "unknown"


def _health_score(m: dict) -> dict:
    if not m or m.get("sample_count", 0) == 0:
        return {"score": None, "grade": "?", "status": "unknown", "reasons": []}

    score = 100.0
    reasons = []

    if _above(m, "cpu_busy_pct_max", CPU_BUSY_CRIT_PCT):
        score -= 25
        reasons.append(f"CPU busy peaked at {m['cpu_busy_pct_max']:.0f}% (> {CPU_BUSY_CRIT_PCT:.0f}%) — host is CPU saturated")
    elif _above(m, "cpu_busy_pct_avg", CPU_BUSY_WARN_PCT):
        score -= 12
        reasons.append(f"CPU busy averaging {m['cpu_busy_pct_avg']:.0f}% — limited headroom")

    if _above(m, "cpu_iowait_pct_max", IOWAIT_CRIT_PCT):
        score -= 20
        reasons.append(f"iowait peaked at {m['cpu_iowait_pct_max']:.0f}% — storage layer is blocking the CPU")
    elif _above(m, "cpu_iowait_pct_avg", IOWAIT_WARN_PCT):
        score -= 10
        reasons.append(f"iowait averaging {m['cpu_iowait_pct_avg']:.0f}% — disk is a contributing bottleneck")

    if _above(m, "mem_used_pct_max", MEM_CRIT_PCT):
        score -= 15
        reasons.append(f"Memory used peaked at {m['mem_used_pct_max']:.0f}% — little headroom for page cache Kafka relies on")
    elif _above(m, "mem_used_pct_avg", MEM_WARN_PCT):
        score -= 7
        reasons.append(f"Memory used averaging {m['mem_used_pct_avg']:.0f}%")

    if _above(m, "swap_used_pct_max", SWAP_CRIT_PCT):
        score -= 20
        reasons.append(f"Swap in use (peak {m['swap_used_pct_max']:.1f}%) — JVM pages may be swapped, causing GC/IO stalls")
    elif _above(m, "swap_used_pct_max", SWAP_WARN_PCT):
        score -= 8
        reasons.append(f"Swap touched (peak {m['swap_used_pct_max']:.1f}%) — watch for vm.swappiness misconfiguration")

    if _above(m, "disk_util_pct_max", DISK_UTIL_CRIT_PCT) or _above(m, "disk_await_ms_max", DISK_AWAIT_CRIT_MS):
        score -= 15
        reasons.append(f"Disk util peaked {_display(m.get('disk_util_pct_max'), '%')} / await "
                       f"{_display(m.get('disk_await_ms_max'), 'ms')} - storage pressure observed")
    elif _above(m, "disk_util_pct_max", DISK_UTIL_WARN_PCT) or _above(m, "disk_await_ms_max", DISK_AWAIT_WARN_MS):
        score -= 7
        reasons.append(f"Disk util peaked {_display(m.get('disk_util_pct_max'), '%')} / await "
                       f"{_display(m.get('disk_await_ms_max'), 'ms')}")

    if _above(m, "net_util_pct_max", NET_UTIL_CRIT_PCT):
        score -= 10
        reasons.append(f"NIC util peaked {m['net_util_pct_max']:.0f}% — network is a likely throughput ceiling")
    elif _above(m, "net_util_pct_max", NET_UTIL_WARN_PCT):
        score -= 5
        reasons.append(f"NIC util peaked {m['net_util_pct_max']:.0f}%")

    quality = metric_quality(m)
    if quality["state"] != "fresh":
        return {"score": None, "grade": "?",
                "status": "unknown" if quality["state"] == "missing" else "partial", "reasons": reasons}
    score = max(0.0, round(score, 1))
    if score >= 90:
        grade, status = "A", "healthy"
    elif score >= 75:
        grade, status = "B", "good"
    elif score >= 60:
        grade, status = "C", "watch"
    elif score >= 40:
        grade, status = "D", "at risk"
    else:
        grade, status = "F", "critical"

    return {"score": score, "grade": grade, "status": status, "reasons": reasons}


def _findings(m: dict) -> dict:
    pros, cons, recs = [], [], []
    if not m or m.get("sample_count", 0) == 0:
        return {"pros": pros, "cons": cons, "recommendations": recs}

    if _numeric(m.get("cpu_busy_pct_avg")) and m["cpu_busy_pct_avg"] <= CPU_BUSY_WARN_PCT:
        pros.append(f"CPU headroom is comfortable (avg {m['cpu_busy_pct_avg']:.0f}% busy).")
    elif _above(m, "cpu_busy_pct_avg", CPU_BUSY_WARN_PCT):
        cons.append(f"CPU busy averages {m['cpu_busy_pct_avg']:.0f}% (peak {m['cpu_busy_pct_max']:.0f}%).")
        recs.append("If GC pauses correlate with these CPU peaks, the JVM is being starved by OS-level "
                     "contention — check for noisy neighbors, rebalance partitions/leaders across brokers, "
                     "or add CPU (vertical) / more brokers (horizontal) depending on whether load is cluster-wide.")

    if _above(m, "cpu_iowait_pct_avg", IOWAIT_WARN_PCT):
        cons.append(f"iowait averages {m['cpu_iowait_pct_avg']:.1f}% — the CPU is regularly blocked on storage.")
        recs.append("Investigate disk queueing (await/%util by device). On a Kafka broker this often means "
                     "log segments are outrunning disk throughput — consider faster storage (NVMe), more "
                     "disks/JBOD, or spreading partitions across more brokers.")
    elif _numeric(m.get("cpu_iowait_pct_avg")):
        pros.append("iowait is low — storage isn't blocking the CPU.")

    if _above(m, "swap_used_pct_max", SWAP_WARN_PCT):
        cons.append(f"Swap was touched (peak {m['swap_used_pct_max']:.1f}%).")
        recs.append("Disable or minimize swap on Kafka hosts (vm.swappiness=1, or swapoff) — swapped JVM "
                     "pages cause GC pauses and replica fetch stalls that look like 'GC problems' but are "
                     "really memory pressure at the OS level.")
    elif _numeric(m.get("swap_used_pct_max")):
        pros.append("No meaningful swap activity.")

    if _above(m, "disk_util_pct_max", DISK_UTIL_WARN_PCT) or _above(m, "disk_await_ms_max", DISK_AWAIT_WARN_MS):
        busiest = m["top_disks"][0]["dev"] if m.get("top_disks") else "?"
        cons.append(f"Disk '{busiest}' peaked at {_display(m.get('disk_util_pct_max'), '%')} util / "
                     f"{_display(m.get('disk_await_ms_max'), 'ms')} await.")
        recs.append(f"'{busiest}' is the busiest device — verify it's not shared with other workloads and "
                     f"that Kafka log dirs are spread across multiple disks (log.dirs).")

    if _above(m, "net_util_pct_max", NET_UTIL_WARN_PCT):
        busiest_if = m["top_nics"][0]["iface"] if m.get("top_nics") else "?"
        cons.append(f"NIC '{busiest_if}' peaked at {m['net_util_pct_max']:.0f}% utilization.")
        recs.append("Network is approaching its ceiling — consider a faster NIC, multiple NICs/bonding, or "
                     "horizontally adding brokers so replication/produce/fetch traffic spreads across more links.")

    # Paging / swap *activity* (RHEL `sar -B` / `sar -W`) — early-warning
    # signals that occupancy numbers hide.
    if _above(m, "pswpout_per_s_max", 0.5):
        cons.append(f"Active swap-out observed (peak {m['pswpout_per_s_max']:.1f} pages/s) — the kernel is "
                     f"evicting anonymous memory under pressure.")
        recs.append("Swap-out on a Kafka host is an emergency-grade memory signal (worse than mere swap "
                     "occupancy): find the memory hog, reduce heap/off-heap footprint, or add RAM — and keep "
                     "vm.swappiness=1.")
    if _above(m, "majflt_per_s_max", 20.0):
        cons.append(f"Major page faults peaked at {m['majflt_per_s_max']:.0f}/s — code or data pages are being "
                     f"re-read from disk.")
        recs.append("Sustained major faults mean the page cache is being reclaimed faster than Kafka can use "
                     "it — check for memory-hungry co-tenants and watch consumer fetch latency during these windows.")

    if not recs and metric_quality(m)["state"] == "fresh":
        recs.append("No host-level resource pressure detected — this node's headroom looks healthy.")

    return {"pros": pros, "cons": cons, "recommendations": recs}


def _timeline(samples: list[SarSample]) -> list[dict]:
    pts = [
        {
            "t": s.ts,
            "cpu_busy_pct": round(100.0 - s.cpu_idle_pct, 2) if _numeric(s.cpu_idle_pct) else None,
            "cpu_iowait_pct": s.cpu_iowait_pct,
            "mem_used_pct": s.mem_used_pct,
            "swap_used_pct": s.swap_used_pct,
            "disk_util_pct": _max([d.get("util_pct") for d in s.disks]),
            "net_util_pct": _max([n.get("util_pct") for n in s.nics]),
            "load1": s.load1,
        }
        for s in samples
    ]
    if len(pts) > 1500:
        step = len(pts) // 1500 + 1
        pts = pts[::step]
    return pts


def auto_bucket_s(samples: list[SarSample]) -> int:
    if len(samples) < 2:
        return 3600
    span = samples[-1].ts - samples[0].ts
    if span <= 3600:
        return 60
    if span <= 6 * 3600:
        return 300
    if span <= 86400:
        return 900
    if span <= 7 * 86400:
        return 3600
    if span <= 90 * 86400:
        return 6 * 3600
    return 86400


def bucket_metrics(parsed: ParsedSar, bucket_s: int | None = None) -> list[tuple[int, dict]]:
    """Roll parsed sar samples into time buckets for backfill / trend charts."""
    samples = parsed.samples
    if not samples:
        return []
    if bucket_s is None:
        bucket_s = auto_bucket_s(samples)
    if bucket_s <= 0:
        raise ValueError("bucket_s must be positive")

    buckets: dict[int, list[SarSample]] = {}
    for s in samples:
        b = (s.ts // bucket_s) * bucket_s
        buckets.setdefault(b, []).append(s)

    out: list[tuple[int, dict]] = []
    for bts in sorted(buckets):
        m = _rollup(buckets[bts])
        m["bucket_seconds"] = bucket_s
        out.append((bts, m))
    return out


# --------------------------------------------------------------------------- #
# Public helpers reused by the history store, so the same scoring/advice
# applies whether metrics come from freshly parsed sar output or from an
# already-aggregated window of host_metrics rows (see store.py).
# --------------------------------------------------------------------------- #
def score_health(metrics: dict) -> dict:
    return _health_score(metrics)


def derive_findings(metrics: dict) -> dict:
    return _findings(metrics)
