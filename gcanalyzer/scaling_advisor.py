"""
Vertical vs horizontal scaling advisor.

Answers the cluster-level question this whole SAR integration exists for: is
this Kafka cluster's workload spread evenly, and if it's under pressure, is
the fix "bigger boxes" (vertical), "more boxes" (horizontal), or "rebalance
partitions/leaders across the boxes you already have"?

Deterministic rules over the same GC (`metrics`) + host (`host_metrics`) data
correlate.py joins per-instance, but rolled up *across every node in a role
group* (brokers by default) so the signal is about the cluster's shape, not
one node's history:

  1. Classify each node's dominant bottleneck (swap / disk / cpu / iowait /
     host memory / network / JVM heap / healthy) from its 24h rollups.
  2. Measure cross-node skew (coefficient of variation) on a throughput proxy
     (network egress) and on CPU busy — low skew + uniform saturation means
     the *cluster* is out of capacity (horizontal candidate); high skew means
     load is unevenly distributed across brokers that otherwise have room
     (rebalance candidate, before spending money on more hardware).
  3. If nodes are JVM heap-bound while the host underneath has headroom, the
     fix is vertical (more heap / more RAM), not more brokers.

This module is read-only and advisory — like analyzer.py's findings, it
recommends, it never acts.
"""

from __future__ import annotations

import statistics

from . import sar_analyzer, store

SKEW_CV_HIGH = 0.30          # coefficient of variation above which load looks unevenly spread
SKEW_CV_MODERATE = 0.18
MIN_NODES_FOR_SKEW = 2
HIGH_HEAP_USE_PCT = 80.0


def _cv(vals: list[float]) -> float | None:
    vals = [v for v in vals if v is not None]
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

    if h.get("sample_count"):
        if h.get("swap_used_pct_max", 0) > sar_analyzer.SWAP_CRIT_PCT:
            return "swap"
        if (h.get("disk_util_pct_max", 0) > sar_analyzer.DISK_UTIL_CRIT_PCT
                or h.get("disk_await_ms_max", 0) > sar_analyzer.DISK_AWAIT_CRIT_MS):
            return "disk"
        if h.get("cpu_busy_pct_avg", 0) > sar_analyzer.CPU_BUSY_CRIT_PCT:
            return "cpu"
        if h.get("cpu_iowait_pct_avg", 0) > sar_analyzer.IOWAIT_CRIT_PCT:
            return "iowait"
        if h.get("mem_used_pct_max", 0) > sar_analyzer.MEM_CRIT_PCT:
            return "host_memory"
        if h.get("net_util_pct_max", 0) > sar_analyzer.NET_UTIL_CRIT_PCT:
            return "network"

    if g:
        if g.get("full_count", 0) > 0 or g.get("avg_heap_after_pct", 0) > HIGH_HEAP_USE_PCT:
            return "heap"

    if h.get("sample_count"):
        if (h.get("cpu_busy_pct_avg", 0) > sar_analyzer.CPU_BUSY_WARN_PCT
                or h.get("disk_util_pct_max", 0) > sar_analyzer.DISK_UTIL_WARN_PCT
                or h.get("net_util_pct_max", 0) > sar_analyzer.NET_UTIL_WARN_PCT):
            return "watch"

    return "healthy" if (h.get("sample_count") or g) else "no_data"


def analyze_cluster_scaling(c, cluster: str, role: str = "broker", now: int = None) -> dict:
    now = now or store.now_ts(c)
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
        nodes.append({
            "instance_id": inst["id"],
            "dominant_bottleneck": bottleneck,
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

    # Cross-node skew: network egress (throughput proxy) and CPU busy.
    tx_vals = [n["net_tx_kbs_avg"] for n in nodes if n["net_tx_kbs_avg"] is not None]
    cpu_vals = [n["cpu_busy_pct_avg"] for n in nodes if n["cpu_busy_pct_avg"] is not None]
    tx_cv = _cv(tx_vals)
    cpu_cv = _cv(cpu_vals)
    skew = None
    if len(nodes) >= MIN_NODES_FOR_SKEW and (tx_cv is not None or cpu_cv is not None):
        hot = sorted(nodes, key=lambda n: n.get("cpu_busy_pct_avg") or 0, reverse=True)[:max(1, len(nodes) // 3)]
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
        "verdict": verdict, "confidence": confidence, "summary": summary, "evidence": evidence,
        "bottleneck_counts": bottleneck_counts,
        "nodes": nodes, "skew": skew,
    }


def _decide(nodes: list[dict], counts: dict[str, int], skew: dict | None, n_with_data: int):
    total = len(nodes)
    pressured = total - counts.get("healthy", 0) - counts.get("no_data", 0) - counts.get("watch", 0)
    pressured_or_watch = pressured + counts.get("watch", 0)
    evidence = []

    high_skew = skew and ((skew.get("cpu_busy_cv") or 0) > SKEW_CV_HIGH or (skew.get("net_tx_cv") or 0) > SKEW_CV_HIGH)
    moderate_skew = skew and not high_skew and (
        (skew.get("cpu_busy_cv") or 0) > SKEW_CV_MODERATE or (skew.get("net_tx_cv") or 0) > SKEW_CV_MODERATE
    )

    if pressured_or_watch == 0:
        evidence.append(f"All {total} node(s) are healthy on both GC and host metrics — no scaling pressure detected.")
        return "no_action", "high" if n_with_data == total else "medium", \
            "Cluster has headroom; no scaling action needed right now.", evidence

    # Uniform JVM-heap pressure with calm hosts -> vertical memory, regardless of skew.
    if counts.get("heap", 0) >= max(1, total // 2) and counts.get("cpu", 0) + counts.get("disk", 0) + counts.get("network", 0) == 0:
        evidence.append(f"{counts.get('heap', 0)}/{total} node(s) are JVM heap-bound (Full GCs and/or live set "
                         f"> {HIGH_HEAP_USE_PCT:.0f}% of heap) while host CPU/disk/network show no saturation.")
        evidence.append("Host hardware has headroom underneath — the constraint is JVM heap sizing, not the box.")
        return "vertical_memory", "high" if n_with_data >= total * 0.75 else "medium", \
            "Bump JVM heap (-Xmx) / host RAM on the affected node(s) before considering more brokers.", evidence

    if high_skew:
        evidence.append(f"Cross-node load is uneven: CPU-busy CV={skew.get('cpu_busy_cv')}, "
                         f"net-egress CV={skew.get('net_tx_cv')} (>{SKEW_CV_HIGH:.2f} = high skew).")
        if skew.get("hot_nodes"):
            evidence.append(f"Hot node(s) carrying disproportionate load: {', '.join(skew['hot_nodes'])}.")
        evidence.append("Other nodes in the group have comparatively low utilization — this points at "
                         "uneven partition/leader distribution, not insufficient total cluster capacity.")
        return "rebalance", "high" if skew.get("hot_nodes") else "medium", \
            "Rebalance partitions/leaders across existing brokers (e.g. kafka-reassign-partitions, " \
            "auto leader rebalance) before adding hardware — load isn't evenly spread.", evidence

    # Uniform saturation across most/all nodes -> horizontal (or vertical on the single binding resource).
    dominant = max(
        ((k, v) for k, v in counts.items() if k not in ("healthy", "no_data", "watch")),
        key=lambda kv: kv[1], default=(None, 0),
    )
    if dominant[0] and dominant[1] >= max(2, total // 2):
        res_label = {
            "cpu": "CPU", "disk": "disk I/O", "iowait": "disk I/O (iowait)",
            "network": "network", "host_memory": "host memory", "swap": "swap/memory",
        }.get(dominant[0], dominant[0])
        evidence.append(f"{dominant[1]}/{total} node(s) are {res_label}-bound, and load is fairly evenly spread "
                         f"(no high cross-node skew) — the cluster as a whole is short on {res_label} capacity.")
        if moderate_skew:
            evidence.append("Some moderate skew exists too; a partition rebalance afterward will still help "
                             "even out the new capacity.")
        confidence = "high" if dominant[1] == total else "medium"
        if dominant[0] in ("cpu", "disk", "iowait", "network"):
            evidence.append(f"Because every node is hitting the same ceiling, vertically upgrading {res_label} "
                             f"on existing boxes is a stop-gap at best — uniform saturation across the fleet "
                             f"is the classic signature for horizontal scale-out (add brokers and let Kafka "
                             f"spread partitions across them).")
            return "horizontal", confidence, \
                f"Add broker(s) to the cluster — {res_label} pressure is uniform across nodes, not isolated " \
                f"to a hot spot a rebalance could fix.", evidence
        if dominant[0] in ("host_memory", "swap"):
            evidence.append("Memory pressure that's uniform across nodes can sometimes be solved per-box "
                             "(more RAM) more cheaply than adding brokers, if the workload itself isn't growing.")
            return "vertical_memory", confidence, \
                "Add RAM to existing hosts (and/or reduce swap pressure); consider horizontal scaling if " \
                "workload growth is expected to continue.", evidence

    evidence.append(f"{pressured_or_watch}/{total} node(s) show some resource pressure, but no single "
                     f"bottleneck or skew pattern dominates clearly enough for a confident verdict.")
    return "mixed", "low", \
        "Resource pressure is present but mixed across nodes/resources — drill into individual node " \
        "findings before committing to a scaling direction.", evidence
