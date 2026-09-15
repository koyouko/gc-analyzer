"""
Vertical vs horizontal scaling advisor.

Summarizes observed resource pressure and cross-node skew. GC and host metrics
alone cannot select a Kafka scaling direction: process ownership, workload,
leader distribution and application impact are not measured here. Missing or
unreliable sources prevent a cluster-wide headroom verdict.

This module is read-only and advisory — like analyzer.py's findings, it
recommends, it never acts.
"""

from __future__ import annotations

import statistics

from . import sar_analyzer, store
from .correlate import _finite_number

SKEW_CV_HIGH = 0.30          # coefficient of variation above which load looks unevenly spread
SKEW_CV_MODERATE = 0.18
MIN_NODES_FOR_SKEW = 2
HIGH_HEAP_USE_PCT = 80.0
MAX_SOURCE_AGE_SECONDS = 86400
_GC_REQUIRED = ("full_count", "avg_heap_after_pct", "pct_time_in_gc", "p99_pause_ms", "max_pause_ms")
_HOST_REQUIRED = ("cpu_busy_pct_avg", "cpu_iowait_pct_avg", "mem_used_pct_max",
                  "swap_used_pct_max", "disk_util_pct_max", "disk_await_ms_max", "net_util_pct_max")


def _source_quality(snapshot: dict | None, now: int, required: tuple[str, ...]) -> dict:
    snap = snapshot or {}
    metrics = snap.get("metrics") or {}
    supplied = snap.get("quality")
    quality = dict(supplied) if isinstance(supplied, dict) else {}
    last = quality.get("last_observed_at", (snap.get("latest") or {}).get("ts"))
    age = now - last if _finite_number(last) else quality.get("age_seconds")
    missing = [key for key in required if not _finite_number(metrics.get(key))]
    state = quality.get("state", "fresh" if _finite_number(age) else "unknown")
    if _finite_number(age) and (age > MAX_SOURCE_AGE_SECONDS or age < 0):
        state = "stale" if age > 0 else "unknown"
    elif state == "fresh":
        if not metrics:
            state = "missing"
        elif missing or (snap.get("health") or {}).get("status") == "unknown":
            state = "partial"
        elif not _finite_number(age):
            state = "unknown"
    elif not metrics and not supplied:
        state = "missing"
    return {**quality, "state": state, "last_observed_at": last, "age_seconds": age,
            "missing_required_metrics": missing}


def _cv(vals: list[float]) -> float | None:
    vals = [v for v in vals if _finite_number(v)]
    if len(vals) < 2:
        return None
    mean = statistics.fmean(vals)
    if mean <= 0:
        return None
    sd = statistics.pstdev(vals)
    return round(sd / mean, 3)


def _classify_node(gc_metrics: dict, host_metrics: dict) -> str:
    h = host_metrics or {}
    g = gc_metrics or {}

    def above(metrics, key, threshold):
        value = metrics.get(key)
        return _finite_number(value) and value > threshold

    if h.get("sample_count"):
        if above(h, "swap_used_pct_max", sar_analyzer.SWAP_CRIT_PCT):
            return "swap"
        if (above(h, "disk_util_pct_max", sar_analyzer.DISK_UTIL_CRIT_PCT)
                or above(h, "disk_await_ms_max", sar_analyzer.DISK_AWAIT_CRIT_MS)):
            return "disk"
        if above(h, "cpu_busy_pct_avg", sar_analyzer.CPU_BUSY_CRIT_PCT):
            return "cpu"
        if above(h, "cpu_iowait_pct_avg", sar_analyzer.IOWAIT_CRIT_PCT):
            return "iowait"
        if above(h, "mem_used_pct_max", sar_analyzer.MEM_CRIT_PCT):
            return "host_memory"
        if above(h, "net_util_pct_max", sar_analyzer.NET_UTIL_CRIT_PCT):
            return "network"

    if g:
        if above(g, "full_count", 0) or above(g, "avg_heap_after_pct", HIGH_HEAP_USE_PCT):
            return "heap"

    if h.get("sample_count"):
        if (above(h, "cpu_busy_pct_avg", sar_analyzer.CPU_BUSY_WARN_PCT)
                or above(h, "disk_util_pct_max", sar_analyzer.DISK_UTIL_WARN_PCT)
                or above(h, "net_util_pct_max", sar_analyzer.NET_UTIL_WARN_PCT)):
            return "watch"

    complete = (h.get("sample_count") and all(_finite_number(h.get(k)) for k in _HOST_REQUIRED)
                and all(_finite_number(g.get(k)) for k in _GC_REQUIRED))
    return "healthy" if complete else "no_data"


def analyze_cluster_scaling(c, cluster: str, role: str = "broker", now: int = None) -> dict:
    now = store.now_ts(c) if now is None else now
    instances = [i for i in store.list_instances(c) if i["cluster"] == cluster and i["role"] == role]

    if not instances:
        return {
            "cluster": cluster, "role": role, "now": now, "n_nodes": 0,
            "verdict": "insufficient_data", "confidence": "low",
            "summary": f"No '{role}' instances found in cluster '{cluster}'.",
            "evidence": [], "nodes": [], "skew": None,
        }

    nodes = []
    for inst in instances:
        gc_snap = store.current_snapshot(c, inst["id"], now)
        host_snap = store.current_host_snapshot(c, inst["id"], now)
        gc_metrics = (gc_snap or {}).get("metrics") or {}
        host_metrics = (host_snap or {}).get("metrics") or {}
        gc_health = (gc_snap or {}).get("health")
        host_health = (host_snap or {}).get("health")
        bottleneck = _classify_node(gc_metrics, host_metrics)
        quality = {"gc": _source_quality(gc_snap, now, _GC_REQUIRED),
                   "host": _source_quality(host_snap, now, _HOST_REQUIRED)}
        reliable = all(q["state"] == "fresh" for q in quality.values())
        nodes.append({
            "instance_id": inst["id"],
            "dominant_bottleneck": bottleneck if reliable else "no_data",
            "observed_pressure": bottleneck,
            "source_quality": quality,
            "reliable": reliable,
            "gc_grade": gc_health.get("grade") if gc_health else None,
            "host_grade": host_health.get("grade") if host_health else None,
            "cpu_busy_pct_avg": host_metrics.get("cpu_busy_pct_avg"),
            "mem_used_pct_avg": host_metrics.get("mem_used_pct_avg"),
            "disk_util_pct_max": host_metrics.get("disk_util_pct_max"),
            "net_tx_kbs_avg": host_metrics.get("net_tx_kbs_avg"),
            "net_util_pct_max": host_metrics.get("net_util_pct_max"),
            "heap_after_pct_avg": gc_metrics.get("avg_heap_after_pct"),
            "full_gc_24h": gc_metrics.get("full_count"),
            "has_host_data": bool(host_metrics.get("sample_count")),
            "has_gc_data": bool(gc_metrics),
        })

    n_with_data = sum(1 for n in nodes if n["has_host_data"] or n["has_gc_data"])
    if n_with_data == 0:
        return {
            "cluster": cluster, "role": role, "now": now, "n_nodes": len(nodes),
            "verdict": "insufficient_data", "confidence": "low",
            "summary": f"{len(nodes)} '{role}' node(s) found but none have GC or host metric history yet.",
            "evidence": [], "nodes": nodes, "skew": None,
        }

    # Host egress is not a Kafka throughput measurement.
    eligible = [n for n in nodes if n["reliable"]]
    tx_vals = [n["net_tx_kbs_avg"] for n in eligible]
    cpu_vals = [n["cpu_busy_pct_avg"] for n in eligible]
    tx_cv = _cv(tx_vals)
    cpu_cv = _cv(cpu_vals)
    skew = None
    if len(nodes) >= MIN_NODES_FOR_SKEW and (tx_cv is not None or cpu_cv is not None):
        hot = sorted(eligible, key=lambda n: n.get("cpu_busy_pct_avg") or 0, reverse=True)[:max(1, len(nodes) // 3)]
        skew = {
            "net_tx_cv": tx_cv, "cpu_busy_cv": cpu_cv,
            "hot_nodes": [n["instance_id"] for n in hot if (n.get("cpu_busy_pct_avg") or 0) > 0],
        }

    bottleneck_counts: dict[str, int] = {}
    for n in nodes:
        bottleneck_counts[n["dominant_bottleneck"]] = bottleneck_counts.get(n["dominant_bottleneck"], 0) + 1

    verdict, confidence, summary, evidence = _decide(nodes, bottleneck_counts, skew, n_with_data)

    return {
        "cluster": cluster, "role": role, "now": now, "n_nodes": len(nodes),
        "n_nodes_with_data": n_with_data,
        "n_nodes_with_reliable_data": len(eligible),
        "verdict": verdict, "confidence": confidence, "summary": summary, "evidence": evidence,
        "bottleneck_counts": bottleneck_counts,
        "nodes": nodes, "skew": skew,
    }


def _decide(nodes: list[dict], counts: dict[str, int], skew: dict | None, n_with_data: int):
    total = len(nodes)
    evidence = []
    unreliable = [n for n in nodes if not n.get("reliable")]
    if unreliable:
        for n in unreliable:
            states = ", ".join(f"{kind}: {q['state']}" for kind, q in n["source_quality"].items())
            evidence.append(f"{n['instance_id']}: {states}.")
        return ("insufficient_data", "low",
                "Fresh, complete GC and host evidence is required for every node before assessing cluster headroom.", evidence)

    high_skew = skew and any((skew.get(key) or 0) > SKEW_CV_HIGH for key in ("cpu_busy_cv", "net_tx_cv"))
    pressured = total - counts.get("healthy", 0)
    if not pressured and not high_skew:
        evidence.append(f"No significant pressure detected in the measured GC and host signals on {total} node(s).")
        evidence.append("This does not establish Kafka application health or spare workload capacity.")
        return "no_action", "medium", "No scaling action indicated by the observed resource metrics.", evidence

    evidence.append("Kafka-native evidence is unavailable: check leader/partition distribution, request rates, "
                    "replication traffic, latency and lag before selecting a scaling or rebalance action.")
    if high_skew:
        evidence.append(f"Observed host load is uneven: CPU-busy CV={skew.get('cpu_busy_cv')}, "
                        f"network-egress CV={skew.get('net_tx_cv')}. Host egress is not Kafka throughput.")
        if skew.get("hot_nodes"):
            evidence.append(f"Higher-CPU node(s): {', '.join(skew['hot_nodes'])}.")
        return "investigate", "medium", "Check workload distribution and process ownership on the unevenly loaded hosts.", evidence

    for kind, count in sorted(counts.items()):
        if kind not in ("healthy", "no_data"):
            evidence.append(f"{count}/{total} node(s) show elevated {kind.replace('_', ' ')} signals.")
    evidence.append("Check device latency/queueing, active paging and JVM live-set evidence; utilization and swap occupancy alone do not prove saturation.")
    return ("investigate", "medium", "Investigate the observed resource pressure before changing heap, hardware or broker count.", evidence)
