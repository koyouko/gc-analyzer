"""
GC <-> host (SAR) correlation engine.

Answers the question the GC dashboard alone can't: *is this node's GC pressure
actually a JVM tuning problem, or is the underlying host starving it?* Joins
`metrics` (GC) and `host_metrics` (SAR) for one instance over a time window and
produces:

  * Pearson correlation coefficients between GC pressure signals (time-in-GC,
    pause tail, Full GC count, throughput) and host resource signals (CPU
    busy, iowait, memory, swap, disk await, NIC util).
  * A plain-language "co-occurrence" read: of the hours this node was in a GC
    storm, what fraction also showed host resource pressure? This is the
    number an operator actually wants — "GC storms coincide with CPU
    saturation 80% of the time" is more actionable than a bare r value.
  * A verdict: no_pressure | gc_bound | host_bound | mixed | insufficient_data.
    The legacy *_bound labels describe possible contributions, not causes.

Deterministic statistics only — no ML here (see ml_insights.py for the
anomaly-scoring "ML Tech Preview" layer, which is explicitly advisory).
Correlation is not causation; findings are phrased as hypotheses for an
operator to check, not as certainties.
"""

from __future__ import annotations

import math
import statistics

from . import sar_analyzer, store

MIN_POINTS_FOR_CORRELATION = 6

# (gc_metric, host_metric, label) pairs worth surfacing to an operator.
_PAIRS = [
    ("time_in_gc_pct", "cpu_busy_pct", "Time-in-GC vs host CPU busy"),
    ("time_in_gc_pct", "cpu_iowait_pct", "Time-in-GC vs host iowait"),
    ("pause_p99_ms", "cpu_iowait_pct", "GC p99 pause vs host iowait"),
    ("pause_max_ms", "disk_await_ms_max", "Worst GC pause vs disk await"),
    ("full_gc_count", "mem_used_pct", "Full GC count vs host memory used"),
    ("full_gc_count", "swap_used_pct", "Full GC count vs host swap used"),
    ("throughput_pct", "cpu_busy_pct", "JVM throughput vs host CPU busy (expect inverse)"),
]

STRONG_R = 0.7
MODERATE_R = 0.45


def _pearson(xs: list[float], ys: list[float]) -> float | None:
    n = len(xs)
    if n < 3:
        return None
    mx, my = statistics.fmean(xs), statistics.fmean(ys)
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    vx = sum((x - mx) ** 2 for x in xs)
    vy = sum((y - my) ** 2 for y in ys)
    if vx <= 0 or vy <= 0:
        return None
    r = cov / math.sqrt(vx * vy)
    return max(-1.0, min(1.0, r))


def _finite_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def rebucket_hourly(rows: list[dict], value_cols: list[str]) -> dict[int, dict]:
    """Average rows into hourly buckets so GC ticks (every scheduler interval)
    and SAR ticks (driven by the host's own sar report cadence) join cleanly
    even when their raw timestamps don't line up exactly."""
    buckets: dict[int, list[dict]] = {}
    for r in rows:
        if not _finite_number(r.get("ts")):
            continue
        hour = (int(r["ts"]) // 3600) * 3600
        buckets.setdefault(hour, []).append(r)
    out = {}
    for hour, rs in buckets.items():
        out[hour] = {}
        for col in value_cols:
            vals = [r[col] for r in rs if _finite_number(r.get(col))]
            out[hour][col] = statistics.fmean(vals) if vals else None
    return out


def _strength(r: float) -> str:
    a = abs(r)
    if a >= STRONG_R:
        return "strong"
    if a >= MODERATE_R:
        return "moderate"
    return "weak"


def correlate_instance(c, instance_id: str, days: int = 30, now: int = None) -> dict:
    now = store.now_ts(c) if now is None else now
    since = now - days * 86400

    gc_rows = store.window_rows(c, instance_id, since, now)
    host_rows = store.host_window_rows(c, instance_id, since, now)

    if len(gc_rows) < MIN_POINTS_FOR_CORRELATION or len(host_rows) < MIN_POINTS_FOR_CORRELATION:
        return {
            "instance_id": instance_id,
            "days": days,
            "n_points": 0,
            "verdict": "insufficient_data",
            "correlations": [],
            "storm_cooccurrence": None,
            "findings": [
                "Not enough overlapping GC and host-metric history yet to correlate "
                f"(need >= {MIN_POINTS_FOR_CORRELATION} points each; "
                f"have {len(gc_rows)} GC / {len(host_rows)} host rows). "
                "Once both collectors have run for a while this will fill in."
            ],
        }

    gc_cols = ["time_in_gc_pct", "pause_p99_ms", "pause_max_ms", "full_gc_count", "throughput_pct"]
    host_cols = ["cpu_busy_pct", "cpu_iowait_pct", "mem_used_pct", "swap_used_pct",
                 "disk_util_pct_max", "disk_await_ms_max", "net_util_pct_max"]

    gc_hourly = rebucket_hourly(gc_rows, gc_cols)
    host_hourly = rebucket_hourly(host_rows, host_cols)
    common_hours = sorted(set(gc_hourly) & set(host_hourly))

    if len(common_hours) < MIN_POINTS_FOR_CORRELATION:
        return {
            "instance_id": instance_id,
            "days": days,
            "n_points": len(common_hours),
            "verdict": "insufficient_data",
            "correlations": [],
            "storm_cooccurrence": None,
            "findings": [
                f"GC and host metrics only overlap on {len(common_hours)} hour(s) in this window — "
                "need more concurrent collection before correlation is meaningful."
            ],
        }

    correlations = []
    for gc_col, host_col, label in _PAIRS:
        pairs = [(gc_hourly[h][gc_col], host_hourly[h][host_col]) for h in common_hours
                 if gc_hourly[h][gc_col] is not None and host_hourly[h][host_col] is not None]
        if len(pairs) < MIN_POINTS_FOR_CORRELATION:
            continue
        xs, ys = zip(*pairs)
        r = _pearson(xs, ys)
        if r is None:
            continue
        correlations.append({
            "label": label, "gc_metric": gc_col, "host_metric": host_col,
            "r": round(r, 3), "strength": _strength(r), "n": len(pairs),
        })
    correlations.sort(key=lambda x: abs(x["r"]), reverse=True)

    # Storm co-occurrence: among hours with elevated time-in-GC, how often is
    # the host also under resource pressure? More intuitive than a bare r.
    tig_vals = [gc_hourly[h]["time_in_gc_pct"] for h in common_hours
                if gc_hourly[h]["time_in_gc_pct"] is not None]
    storm_threshold = max(5.0, _percentile(tig_vals, 90))
    storm_hours = [h for h in common_hours if gc_hourly[h]["time_in_gc_pct"] is not None
                   and gc_hourly[h]["time_in_gc_pct"] >= storm_threshold]

    cooccurrence = None
    if storm_hours:
        def pct_pressured(host_col: str, threshold: float) -> float | None:
            vals = [host_hourly[h][host_col] for h in storm_hours if host_hourly[h][host_col] is not None]
            return round(100.0 * sum(v >= threshold for v in vals) / len(vals), 1) if vals else None

        cooccurrence = {
            "storm_hours": len(storm_hours),
            "storm_threshold_time_in_gc_pct": round(storm_threshold, 2),
            "pct_with_cpu_pressure": pct_pressured("cpu_busy_pct", sar_analyzer.CPU_BUSY_WARN_PCT),
            "pct_with_iowait_pressure": pct_pressured("cpu_iowait_pct", sar_analyzer.IOWAIT_WARN_PCT),
            "pct_with_mem_pressure": pct_pressured("mem_used_pct", sar_analyzer.MEM_WARN_PCT),
            "pct_with_swap_activity": pct_pressured("swap_used_pct", sar_analyzer.SWAP_WARN_PCT),
            "pct_with_disk_pressure": pct_pressured("disk_util_pct_max", sar_analyzer.DISK_UTIL_WARN_PCT),
            "pct_with_net_pressure": pct_pressured("net_util_pct_max", sar_analyzer.NET_UTIL_WARN_PCT),
            "observed_storm_hours": {col: sum(host_hourly[h][col] is not None for h in storm_hours)
                                     for col in host_cols},
        }
        # Retain the old key for clients; occupancy is not evidence of paging.
        cooccurrence["pct_with_swap_usage"] = cooccurrence["pct_with_swap_activity"]

    gc_thresholds = {"time_in_gc_pct": 5.0, "pause_p99_ms": store.P99_PAUSE_ALERT_MS,
                     "pause_max_ms": store.PAUSE_ALERT_MS, "full_gc_count": 1.0}
    host_thresholds = dict(zip(host_cols, [sar_analyzer.CPU_BUSY_WARN_PCT,
        sar_analyzer.IOWAIT_WARN_PCT, sar_analyzer.MEM_WARN_PCT, sar_analyzer.SWAP_WARN_PCT,
        sar_analyzer.DISK_UTIL_WARN_PCT, sar_analyzer.DISK_AWAIT_WARN_MS, sar_analyzer.NET_UTIL_WARN_PCT]))
    gc_pressure = any(gc_hourly[h][col] is not None and gc_hourly[h][col] >= threshold
                      for h in common_hours for col, threshold in gc_thresholds.items())
    host_pressure = any(host_hourly[h][col] is not None and host_hourly[h][col] >= threshold
                        for h in common_hours for col, threshold in host_thresholds.items())
    complete_hours = sum(all(gc_hourly[h][col] is not None for col in gc_thresholds)
                         and all(host_hourly[h][col] is not None for col in host_cols)
                         for h in common_hours)
    verdict = _verdict(correlations, cooccurrence, gc_pressure, host_pressure,
                       complete_hours >= MIN_POINTS_FOR_CORRELATION)
    findings = _build_findings(correlations, cooccurrence, verdict)

    return {
        "instance_id": instance_id,
        "days": days,
        "n_points": len(common_hours),
        "n_complete_points": complete_hours,
        "verdict": verdict,
        "correlations": correlations,
        "storm_cooccurrence": cooccurrence,
        "findings": findings,
    }


def _percentile(vals: list[float], p: float) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    k = (len(s) - 1) * (p / 100.0)
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def _build_findings(correlations: list[dict], cooccurrence: dict | None,
                    verdict: str = "insufficient_data") -> list[str]:
    findings = []
    for corr in correlations:
        if corr["strength"] == "weak":
            continue
        direction = "rises together with" if corr["r"] > 0 else "falls as"
        findings.append(
            f"{corr['label']}: {corr['strength']} correlation (r={corr['r']:+.2f}, n={corr['n']}) — "
            f"{corr['gc_metric']} {direction} {corr['host_metric']}."
        )

    observations = [
        ("pct_with_cpu_pressure", 60, "high host CPU", "Check process CPU and runnable tasks; GC can also drive CPU."),
        ("pct_with_iowait_pressure", 50, "elevated iowait", "Check device latency, queueing and process I/O."),
        ("pct_with_swap_activity", 30, "high swap usage", "Swap occupancy does not establish active paging; check swap-in/out rates."),
        ("pct_with_mem_pressure", 50, "high host memory usage", "Check available memory, major faults and workload ownership."),
        ("pct_with_disk_pressure", 50, "high disk utilization", "Utilization alone does not establish saturation; check log-device latency and queueing."),
        ("pct_with_net_pressure", 50, "high NIC utilization", "Check Kafka traffic and replication metrics before attributing host traffic to Kafka."),
    ]
    for key, threshold, label, check in observations:
        pct = (cooccurrence or {}).get(key)
        if pct is not None and pct >= threshold:
            findings.append(f"{pct:.0f}% of observed GC-storm hours with this host metric also showed {label}. {check}")

    descriptions = {
        "no_pressure": "No significant pressure detected in the observed GC and host metrics.",
        "gc_bound": "GC pressure was observed without concurrent host pressure in the measured signals. Check JVM evidence; this does not establish a tuning problem.",
        "host_bound": "GC and host pressure coincided: possible host contribution, not a confirmed cause.",
        "mixed": "GC or host pressure was observed, but the relationship is not established.",
        "insufficient_data": "Not enough complete overlapping evidence to distinguish GC and host pressure.",
    }
    findings.insert(0, descriptions[verdict])
    if correlations or cooccurrence:
        findings.append("Association is not a confirmed cause. Hourly aggregates cannot establish event ordering or application impact.")
    return findings


def _verdict(correlations: list[dict], cooccurrence: dict | None,
             gc_pressure: bool = False, host_pressure: bool = False, complete: bool = False) -> str:
    if not complete:
        return "insufficient_data"
    if not gc_pressure:
        return "mixed" if host_pressure else "no_pressure"
    strong = [c for c in correlations if c["strength"] in ("strong", "moderate")]
    concurrent_pressure = bool(cooccurrence) and max(
        [v for k, v in cooccurrence.items() if k.startswith("pct_with_") and v is not None], default=0
    ) >= 60
    if strong and concurrent_pressure:
        return "host_bound"
    if host_pressure:
        return "mixed"
    return "gc_bound"
