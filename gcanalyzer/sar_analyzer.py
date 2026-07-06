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


def _avg(vals: list[float]) -> float:
    return round(statistics.fmean(vals), 2) if vals else 0.0


def _max(vals: list[float]) -> float:
    return round(max(vals), 2) if vals else 0.0


def _top_devices(samples: list[SarSample], key: str, field: str, util_field: str, n: int = 3) -> list[dict]:
    """Aggregate per-device stats across samples, return the busiest `n` by avg util."""
    agg: dict[str, dict] = {}
    for s in samples:
        for item in getattr(s, field):
            name = item.get(key, "?")
            a = agg.setdefault(name, {key: name, "_util": [], "_extra": {}})
            a["_util"].append(item.get(util_field, 0.0) or 0.0)
            for k, v in item.items():
                if k in (key, util_field):
                    continue
                a["_extra"].setdefault(k, []).append(v if isinstance(v, (int, float)) else 0.0)

    rows = []
    for name, a in agg.items():
        row = {key: name, "util_pct_avg": _avg(a["_util"]), "util_pct_max": _max(a["_util"])}
        for k, vals in a["_extra"].items():
            row[f"{k}_avg"] = _avg(vals)
        rows.append(row)
    rows.sort(key=lambda r: r["util_pct_avg"], reverse=True)
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
    }


def _rollup(samples: list[SarSample]) -> dict:
    if not samples:
        return {"sample_count": 0}

    span = (samples[-1].ts - samples[0].ts) if len(samples) > 1 else 0
    cpu_busy = [100.0 - s.cpu_idle_pct for s in samples]

    m = {
        "sample_count": len(samples),
        "span_seconds": span,
        "cpu_user_pct_avg": _avg([s.cpu_user_pct for s in samples]),
        "cpu_system_pct_avg": _avg([s.cpu_system_pct for s in samples]),
        "cpu_iowait_pct_avg": _avg([s.cpu_iowait_pct for s in samples]),
        "cpu_iowait_pct_max": _max([s.cpu_iowait_pct for s in samples]),
        "cpu_busy_pct_avg": _avg(cpu_busy),
        "cpu_busy_pct_max": _max(cpu_busy),
        "load1_avg": _avg([s.load1 for s in samples]),
        "load1_max": _max([s.load1 for s in samples]),
        "load5_avg": _avg([s.load5 for s in samples]),
        "runq_sz_avg": _avg([s.runq_sz for s in samples]),
        "plist_sz_avg": _avg([s.plist_sz for s in samples]),
        "cswch_per_s_avg": _avg([s.cswch_per_s for s in samples]),
        "mem_used_pct_avg": _avg([s.mem_used_pct for s in samples]),
        "mem_used_pct_max": _max([s.mem_used_pct for s in samples]),
        "mem_cached_mb_avg": _avg([s.mem_cached_mb for s in samples]),
        "swap_used_pct_avg": _avg([s.swap_used_pct for s in samples]),
        "swap_used_pct_max": _max([s.swap_used_pct for s in samples]),
        "disk_util_pct_max": _max([d.get("util_pct", 0.0) for s in samples for d in s.disks]),
        "disk_await_ms_max": _max([d.get("await_ms", 0.0) for s in samples for d in s.disks]),
        "disk_tps_avg": _avg([sum(d.get("tps", 0.0) for d in s.disks) for s in samples]),
        "net_util_pct_max": _max([n.get("util_pct", 0.0) for s in samples for n in s.nics]),
        "net_rx_kbs_avg": _avg([sum(n.get("rx_kbs", 0.0) for n in s.nics) for s in samples]),
        "net_tx_kbs_avg": _avg([sum(n.get("tx_kbs", 0.0) for n in s.nics) for s in samples]),
        "net_tx_kbs_max": _max([sum(n.get("tx_kbs", 0.0) for n in s.nics) for s in samples]),
        "top_disks": _top_devices(samples, "dev", "disks", "util_pct"),
        "top_nics": _top_devices(samples, "iface", "nics", "util_pct"),
    }
    return m


def _health_score(m: dict) -> dict:
    if not m or m.get("sample_count", 0) == 0:
        return {"score": None, "grade": None, "status": "no_data", "reasons": []}

    score = 100.0
    reasons = []

    if m["cpu_busy_pct_max"] > CPU_BUSY_CRIT_PCT:
        score -= 25
        reasons.append(f"CPU busy peaked at {m['cpu_busy_pct_max']:.0f}% (> {CPU_BUSY_CRIT_PCT:.0f}%) — host is CPU saturated")
    elif m["cpu_busy_pct_avg"] > CPU_BUSY_WARN_PCT:
        score -= 12
        reasons.append(f"CPU busy averaging {m['cpu_busy_pct_avg']:.0f}% — limited headroom")

    if m["cpu_iowait_pct_max"] > IOWAIT_CRIT_PCT:
        score -= 20
        reasons.append(f"iowait peaked at {m['cpu_iowait_pct_max']:.0f}% — storage layer is blocking the CPU")
    elif m["cpu_iowait_pct_avg"] > IOWAIT_WARN_PCT:
        score -= 10
        reasons.append(f"iowait averaging {m['cpu_iowait_pct_avg']:.0f}% — disk is a contributing bottleneck")

    if m["mem_used_pct_max"] > MEM_CRIT_PCT:
        score -= 15
        reasons.append(f"Memory used peaked at {m['mem_used_pct_max']:.0f}% — little headroom for page cache Kafka relies on")
    elif m["mem_used_pct_avg"] > MEM_WARN_PCT:
        score -= 7
        reasons.append(f"Memory used averaging {m['mem_used_pct_avg']:.0f}%")

    if m["swap_used_pct_max"] > SWAP_CRIT_PCT:
        score -= 20
        reasons.append(f"Swap in use (peak {m['swap_used_pct_max']:.1f}%) — JVM pages may be swapped, causing GC/IO stalls")
    elif m["swap_used_pct_max"] > SWAP_WARN_PCT:
        score -= 8
        reasons.append(f"Swap touched (peak {m['swap_used_pct_max']:.1f}%) — watch for vm.swappiness misconfiguration")

    if m["disk_util_pct_max"] > DISK_UTIL_CRIT_PCT or m["disk_await_ms_max"] > DISK_AWAIT_CRIT_MS:
        score -= 15
        reasons.append(f"Disk util peaked {m['disk_util_pct_max']:.0f}% / await {m['disk_await_ms_max']:.0f}ms — storage is saturated")
    elif m["disk_util_pct_max"] > DISK_UTIL_WARN_PCT or m["disk_await_ms_max"] > DISK_AWAIT_WARN_MS:
        score -= 7
        reasons.append(f"Disk util peaked {m['disk_util_pct_max']:.0f}% / await {m['disk_await_ms_max']:.0f}ms")

    if m["net_util_pct_max"] > NET_UTIL_CRIT_PCT:
        score -= 10
        reasons.append(f"NIC util peaked {m['net_util_pct_max']:.0f}% — network is a likely throughput ceiling")
    elif m["net_util_pct_max"] > NET_UTIL_WARN_PCT:
        score -= 5
        reasons.append(f"NIC util peaked {m['net_util_pct_max']:.0f}%")

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

    if m["cpu_busy_pct_avg"] <= CPU_BUSY_WARN_PCT:
        pros.append(f"CPU headroom is comfortable (avg {m['cpu_busy_pct_avg']:.0f}% busy).")
    else:
        cons.append(f"CPU busy averages {m['cpu_busy_pct_avg']:.0f}% (peak {m['cpu_busy_pct_max']:.0f}%).")
        recs.append("If GC pauses correlate with these CPU peaks, the JVM is being starved by OS-level "
                     "contention — check for noisy neighbors, rebalance partitions/leaders across brokers, "
                     "or add CPU (vertical) / more brokers (horizontal) depending on whether load is cluster-wide.")

    if m["cpu_iowait_pct_avg"] > IOWAIT_WARN_PCT:
        cons.append(f"iowait averages {m['cpu_iowait_pct_avg']:.1f}% — the CPU is regularly blocked on storage.")
        recs.append("Investigate disk queueing (await/%util by device). On a Kafka broker this often means "
                     "log segments are outrunning disk throughput — consider faster storage (NVMe), more "
                     "disks/JBOD, or spreading partitions across more brokers.")
    else:
        pros.append("iowait is low — storage isn't blocking the CPU.")

    if m["swap_used_pct_max"] > SWAP_WARN_PCT:
        cons.append(f"Swap was touched (peak {m['swap_used_pct_max']:.1f}%).")
        recs.append("Disable or minimize swap on Kafka hosts (vm.swappiness=1, or swapoff) — swapped JVM "
                     "pages cause GC pauses and replica fetch stalls that look like 'GC problems' but are "
                     "really memory pressure at the OS level.")
    else:
        pros.append("No meaningful swap activity.")

    if m["disk_util_pct_max"] > DISK_UTIL_WARN_PCT or m["disk_await_ms_max"] > DISK_AWAIT_WARN_MS:
        busiest = m["top_disks"][0]["dev"] if m.get("top_disks") else "?"
        cons.append(f"Disk '{busiest}' peaked at {m['disk_util_pct_max']:.0f}% util / "
                     f"{m['disk_await_ms_max']:.0f}ms await.")
        recs.append(f"'{busiest}' is the busiest device — verify it's not shared with other workloads and "
                     f"that Kafka log dirs are spread across multiple disks (log.dirs).")

    if m["net_util_pct_max"] > NET_UTIL_WARN_PCT:
        busiest_if = m["top_nics"][0]["iface"] if m.get("top_nics") else "?"
        cons.append(f"NIC '{busiest_if}' peaked at {m['net_util_pct_max']:.0f}% utilization.")
        recs.append("Network is approaching its ceiling — consider a faster NIC, multiple NICs/bonding, or "
                     "horizontally adding brokers so replication/produce/fetch traffic spreads across more links.")

    if not recs:
        recs.append("No host-level resource pressure detected — this node's headroom looks healthy.")

    return {"pros": pros, "cons": cons, "recommendations": recs}


def _timeline(samples: list[SarSample]) -> list[dict]:
    pts = [
        {
            "t": s.ts,
            "cpu_busy_pct": round(100.0 - s.cpu_idle_pct, 2),
            "cpu_iowait_pct": s.cpu_iowait_pct,
            "mem_used_pct": s.mem_used_pct,
            "swap_used_pct": s.swap_used_pct,
            "disk_util_pct": max((d.get("util_pct", 0.0) for d in s.disks), default=0.0),
            "net_util_pct": max((n.get("util_pct", 0.0) for n in s.nics), default=0.0),
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

    buckets: dict[int, list[SarSample]] = {}
    for s in samples:
        b = (s.ts // bucket_s) * bucket_s
        buckets.setdefault(b, []).append(s)

    out: list[tuple[int, dict]] = []
    for bts in sorted(buckets):
        out.append((bts, _rollup(buckets[bts])))
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
