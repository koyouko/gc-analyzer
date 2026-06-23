"""
GC analysis engine.

Turns a ParsedLog into the metrics a Kafka/ZooKeeper operator actually cares
about: throughput, pause percentiles, allocation & promotion pressure, heap
utilisation, GC frequency, "hot spot" time windows, an overall health score,
and concrete tuning recommendations (pros / cons).

All maths is deterministic and runs locally. The output structure is JSON-ready
so the FastAPI layer can hand it straight to the dashboard.
"""

from __future__ import annotations

import statistics
from typing import Optional

from .parser import ParsedLog, GCEvent


# Confluent/Kafka rules of thumb used for scoring & advice.
TARGET_THROUGHPUT = 99.0        # % application time; below this GC is eating CPU
MAX_HEALTHY_PAUSE_MS = 200.0    # G1 default MaxGCPauseMillis target
WARN_PAUSE_MS = 500.0           # pauses above this risk Kafka request timeouts / ISR shrink
FULL_GC_IS_BAD = True           # any Full GC on a broker is a red flag
HIGH_HEAP_USE_PCT = 80.0        # sustained post-GC occupancy above this = memory pressure
HOTSPOT_BUCKET_S = 60           # 1-minute buckets for hotspot detection

# Last-hour fleet alerts (see store.evaluate_alerts). Override at runtime via GC_ALERT_* env vars.
ALERT_RECENT_WINDOW_S = 3600
ALERT_PAUSE_CRITICAL_MS = WARN_PAUSE_MS       # single STW pause — Kafka request timeout risk
ALERT_P99_WARNING_MS = MAX_HEALTHY_PAUSE_MS # tail latency vs G1 MaxGCPauseMillis target
ALERT_HEAP_WARNING_PCT = 85.0                 # peak post-GC live set (% of heap)
ALERT_HEAP_BY_ROLE = {"zookeeper": 90.0, "connect": 88.0, "schema-registry": 88.0}
ALERT_STORM_TIME_IN_GC_PCT = 5.0
ALERT_STORM_BASELINE_MULT = 3.0
ALERT_GC_FREQ_MIN = 20.0                    # /min floor (with 2x baseline) for gc_freq alert
ALERT_GC_FREQ_BASELINE_MULT = 2.0


def _pct(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = (len(s) - 1) * (p / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def _span_seconds(events: list[GCEvent]) -> float:
    ts = [e.timestamp for e in events if e.timestamp is not None]
    if len(ts) >= 2:
        return max(ts) - min(ts)
    up = [e.uptime for e in events if e.uptime is not None]
    if len(up) >= 2:
        return max(up) - min(up)
    return 0.0


def analyze(parsed: ParsedLog) -> dict:
    events = parsed.events
    stw = [e for e in events if e.is_stw and e.pause_ms > 0]
    pauses = [e.pause_ms for e in stw]
    span = _span_seconds(events)

    young = [e for e in stw if e.phase == "young"]
    mixed = [e for e in stw if e.phase == "mixed"]
    full = [e for e in stw if e.phase == "full"]
    concurrent = [e for e in events if e.phase == "concurrent"]

    total_pause_ms = sum(pauses)
    total_pause_s = total_pause_ms / 1000.0

    # Throughput = fraction of wall-clock NOT spent in stop-the-world GC.
    throughput = 100.0 if span <= 0 else max(0.0, (1 - total_pause_s / span) * 100.0)

    # Heap utilisation: post-GC occupancy is the "real" live-set footprint.
    heap_max = parsed.heap_max_mb or (
        max((e.heap_total_mb or 0) for e in events) if events else 0
    )
    after_vals = [e.heap_after_mb for e in stw if e.heap_after_mb is not None]
    before_vals = [e.heap_before_mb for e in stw if e.heap_before_mb is not None]
    avg_after = statistics.fmean(after_vals) if after_vals else 0.0
    peak_after = max(after_vals) if after_vals else 0.0
    avg_after_pct = (avg_after / heap_max * 100.0) if heap_max else 0.0
    peak_after_pct = (peak_after / heap_max * 100.0) if heap_max else 0.0

    # Allocation rate: bytes reclaimed per second is a proxy for allocation
    # throughput in steady state (what came in must be collected).
    reclaimed_mb = sum(
        (e.heap_before_mb or 0) - (e.heap_after_mb or 0)
        for e in stw
        if e.heap_before_mb is not None and e.heap_after_mb is not None
    )
    alloc_rate_mb_s = (reclaimed_mb / span) if span > 0 else 0.0

    # Promotion pressure proxy: how often the post-GC live set keeps climbing.
    promotion_climbs = 0
    for a, b in zip(after_vals, after_vals[1:]):
        if b > a:
            promotion_climbs += 1
    promotion_trend = (promotion_climbs / max(1, len(after_vals) - 1)) * 100.0

    # GC frequency / pressure.
    gc_per_min = (len(stw) / span * 60.0) if span > 0 else 0.0
    pct_time_in_gc = (total_pause_s / span * 100.0) if span > 0 else 0.0

    metrics = {
        "event_count": len(events),
        "stw_count": len(stw),
        "young_count": len(young),
        "mixed_count": len(mixed),
        "full_count": len(full),
        "concurrent_count": len(concurrent),
        "span_seconds": round(span, 1),
        "throughput_pct": round(throughput, 3),
        "pct_time_in_gc": round(pct_time_in_gc, 3),
        "total_pause_ms": round(total_pause_ms, 1),
        "avg_pause_ms": round(statistics.fmean(pauses), 2) if pauses else 0.0,
        "max_pause_ms": round(max(pauses), 2) if pauses else 0.0,
        "p50_pause_ms": round(_pct(pauses, 50), 2),
        "p95_pause_ms": round(_pct(pauses, 95), 2),
        "p99_pause_ms": round(_pct(pauses, 99), 2),
        "gc_per_min": round(gc_per_min, 2),
        "heap_max_mb": round(heap_max, 1),
        "avg_heap_after_mb": round(avg_after, 1),
        "peak_heap_after_mb": round(peak_after, 1),
        "avg_heap_after_pct": round(avg_after_pct, 1),
        "peak_heap_after_pct": round(peak_after_pct, 1),
        "alloc_rate_mb_s": round(alloc_rate_mb_s, 1),
        "promotion_trend_pct": round(promotion_trend, 1),
        "reclaimed_mb": round(reclaimed_mb, 1),
    }

    hotspots = _find_hotspots(stw)
    health = _health_score(metrics)
    findings = _findings(metrics, parsed)
    timeline = _timeline(stw)
    histogram = _pause_histogram(pauses)

    return {
        "node_id": parsed.node_id,
        "collector": parsed.collector,
        "java_hint": parsed.java_hint,
        "warnings": parsed.warnings,
        "metrics": metrics,
        "health": health,
        "hotspots": hotspots,
        "findings": findings,
        "timeline": timeline,
        "pause_histogram": histogram,
    }


def _timeline(stw: list[GCEvent]) -> list[dict]:
    """Down-sampled series for charts: time, pause, heap before/after."""
    pts = []
    for e in stw:
        t = e.timestamp if e.timestamp is not None else e.uptime
        if t is None:
            continue
        pts.append(
            {
                "t": t,
                "pause_ms": round(e.pause_ms, 2),
                "before_mb": e.heap_before_mb,
                "after_mb": e.heap_after_mb,
                "phase": e.phase,
            }
        )
    # Cap to ~1500 points for the browser.
    if len(pts) > 1500:
        step = len(pts) // 1500 + 1
        pts = pts[::step]
    return pts


def _pause_histogram(pauses: list[float]) -> list[dict]:
    buckets = [
        (0, 10), (10, 25), (25, 50), (50, 100),
        (100, 200), (200, 500), (500, 1000), (1000, float("inf")),
    ]
    labels = ["<10", "10-25", "25-50", "50-100", "100-200", "200-500", "500-1k", ">1s"]
    counts = [0] * len(buckets)
    for p in pauses:
        for i, (lo, hi) in enumerate(buckets):
            if lo <= p < hi:
                counts[i] += 1
                break
    return [{"bucket": l, "count": c} for l, c in zip(labels, counts)]


def _find_hotspots(stw: list[GCEvent]) -> list[dict]:
    """Bucket pauses into windows and flag windows with abnormal GC load."""
    buckets: dict[int, list[GCEvent]] = {}
    for e in stw:
        t = e.timestamp if e.timestamp is not None else e.uptime
        if t is None:
            continue
        key = int(t // HOTSPOT_BUCKET_S)
        buckets.setdefault(key, []).append(e)

    if not buckets:
        return []

    rows = []
    for key, evs in buckets.items():
        pause_sum = sum(e.pause_ms for e in evs)
        rows.append(
            {
                "window_start": key * HOTSPOT_BUCKET_S,
                "gc_count": len(evs),
                "pause_sum_ms": round(pause_sum, 1),
                "max_pause_ms": round(max(e.pause_ms for e in evs), 1),
                "pct_in_gc": round(pause_sum / (HOTSPOT_BUCKET_S * 1000) * 100, 2),
                "full_gcs": sum(1 for e in evs if e.phase == "full"),
            }
        )

    # A window is a hotspot if it spends an outlier fraction of time in GC.
    pct_vals = [r["pct_in_gc"] for r in rows]
    mean = statistics.fmean(pct_vals)
    stdev = statistics.pstdev(pct_vals) if len(pct_vals) > 1 else 0.0
    threshold = max(mean + 1.5 * stdev, 2.0)  # at least 2% time-in-GC to qualify

    for r in rows:
        r["is_hotspot"] = r["pct_in_gc"] >= threshold or r["full_gcs"] > 0

    hotspots = sorted(
        [r for r in rows if r["is_hotspot"]],
        key=lambda r: r["pct_in_gc"],
        reverse=True,
    )
    return hotspots[:20]


def _health_score(m: dict) -> dict:
    """0-100 composite score with letter grade and component breakdown."""
    score = 100.0
    reasons = []

    # Throughput (weight up to 35)
    if m["throughput_pct"] < TARGET_THROUGHPUT:
        deficit = TARGET_THROUGHPUT - m["throughput_pct"]
        pen = min(35.0, deficit * 7.0)
        score -= pen
        reasons.append(f"Throughput {m['throughput_pct']:.2f}% is below {TARGET_THROUGHPUT}% target")

    # Pauses (weight up to 30)
    if m["max_pause_ms"] > WARN_PAUSE_MS:
        score -= 20
        reasons.append(f"Worst pause {m['max_pause_ms']:.0f}ms can trip Kafka request timeouts")
    elif m["p99_pause_ms"] > MAX_HEALTHY_PAUSE_MS:
        score -= 10
        reasons.append(f"p99 pause {m['p99_pause_ms']:.0f}ms exceeds {MAX_HEALTHY_PAUSE_MS:.0f}ms target")

    # Full GCs (weight up to 25)
    if m["full_count"] > 0:
        pen = min(25.0, 8.0 + m["full_count"] * 3.0)
        score -= pen
        reasons.append(f"{m['full_count']} Full GC(s) detected — major STW + fragmentation risk")

    # Heap pressure (weight up to 15)
    if m["peak_heap_after_pct"] > HIGH_HEAP_USE_PCT:
        score -= 12
        reasons.append(f"Live set peaks at {m['peak_heap_after_pct']:.0f}% of heap — little headroom")
    elif m["avg_heap_after_pct"] > HIGH_HEAP_USE_PCT:
        score -= 6
        reasons.append(f"Average live set {m['avg_heap_after_pct']:.0f}% of heap")

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


def _findings(m: dict, parsed: ParsedLog) -> dict:
    """Pros, cons, and concrete recommendations."""
    pros, cons, recs = [], [], []

    if m["throughput_pct"] >= TARGET_THROUGHPUT:
        pros.append(f"Excellent throughput ({m['throughput_pct']:.2f}%) — GC overhead is minimal.")
    else:
        cons.append(f"Throughput {m['throughput_pct']:.2f}% means GC is consuming "
                    f"{m['pct_time_in_gc']:.2f}% of wall-clock time.")

    if m["full_count"] == 0:
        pros.append("No Full GCs — G1 is keeping up with allocation incrementally.")
    else:
        cons.append(f"{m['full_count']} Full GC(s): the whole heap was compacted with all "
                    f"threads stopped. On a broker this can shrink the ISR and stall produce/fetch.")
        recs.append("Investigate Full GC causes. If 'G1 Humongous Allocation' appears, large "
                    "messages exceed half the region size — raise -XX:G1HeapRegionSize (e.g. 16m/32m) "
                    "or reduce max message/batch size.")
        recs.append("If Full GCs are 'Allocation Failure'/'Evacuation Failure', the heap is too "
                    "small for the live set — increase -Xmx or lower -XX:InitiatingHeapOccupancyPercent "
                    "(IHOP) so concurrent marking starts earlier.")

    if m["max_pause_ms"] > WARN_PAUSE_MS:
        cons.append(f"Worst pause {m['max_pause_ms']:.0f}ms exceeds typical Kafka "
                    f"request.timeout / replica.lag tolerances.")
        recs.append("Lower the pause target with -XX:MaxGCPauseMillis (try 150–200) and verify "
                    "young-gen sizing; very large young gens lengthen evacuation pauses.")
    elif m["p99_pause_ms"] <= MAX_HEALTHY_PAUSE_MS:
        pros.append(f"p99 pause {m['p99_pause_ms']:.0f}ms is within the {MAX_HEALTHY_PAUSE_MS:.0f}ms target.")

    if m["peak_heap_after_pct"] > HIGH_HEAP_USE_PCT:
        cons.append(f"Post-GC live set peaks at {m['peak_heap_after_pct']:.0f}% of heap — "
                    f"the JVM is running close to OOM.")
        recs.append(f"Add headroom: current heap ~{m['heap_max_mb']:.0f}MB. Aim for live set ≤70% "
                    f"after GC; consider raising -Xmx or trimming caches / page-cache reliance.")
    elif m["avg_heap_after_pct"] < 50 and m["heap_max_mb"] > 0:
        pros.append(f"Comfortable headroom — live set averages {m['avg_heap_after_pct']:.0f}% of heap.")
        recs.append("Heap may be over-provisioned; you could reclaim RAM for the OS page cache "
                    "(which Kafka relies on heavily) by modestly lowering -Xmx.")

    if m["promotion_trend_pct"] > 60:
        cons.append(f"Live set climbs after {m['promotion_trend_pct']:.0f}% of collections — "
                    f"possible memory leak or steadily growing cache/metadata.")
        recs.append("Confirm whether the post-GC heap returns to a baseline over time. A "
                    "monotonic climb points to a leak — capture a heap dump for HeapHero/MAT analysis.")

    if m["gc_per_min"] > 30:
        cons.append(f"High GC frequency ({m['gc_per_min']:.0f}/min) indicates a hot allocation path.")
        recs.append("Reduce allocation rate (batch/compression settings, fewer short-lived objects) "
                    "or enlarge the young generation so collections are less frequent.")
    elif m["gc_per_min"] > 0:
        pros.append(f"Moderate GC frequency ({m['gc_per_min']:.1f}/min).")

    if parsed.collector != "G1":
        recs.append(f"This node reports the {parsed.collector} collector. Modern Kafka brokers are "
                    f"tuned for and ship defaults for G1; consider standardising on G1 (or ZGC for "
                    f"very large heaps / strict latency) across the cluster.")

    if not recs:
        recs.append("No tuning changes required — this node is operating within healthy GC bounds.")

    return {"pros": pros, "cons": cons, "recommendations": recs}




def _event_epoch(e: GCEvent, uptime_offset: float) -> float | None:
    if e.timestamp is not None:
        return float(e.timestamp)
    if e.uptime is not None:
        return float(e.uptime) + uptime_offset
    return None


def auto_bucket_s(parsed: ParsedLog) -> int:
    """Pick bucket width so trend charts get useful granularity."""
    stw = [e for e in parsed.events if e.is_stw and e.pause_ms > 0]
    if len(stw) < 2:
        return 3600
    import time

    has_epoch = any(e.timestamp is not None for e in stw)
    if has_epoch:
        offset = 0.0
    else:
        uptimes = [e.uptime for e in stw if e.uptime is not None]
        if not uptimes:
            return 3600
        offset = time.time() - max(uptimes)
    times = [t for e in stw if (t := _event_epoch(e, offset)) is not None]
    if len(times) < 2:
        return 3600
    span = max(times) - min(times)
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


def bucket_metrics(parsed: ParsedLog, bucket_s: int | None = None) -> list[tuple[int, dict]]:
    """Roll parsed GC events into time buckets for trend charts."""
    stw = [e for e in parsed.events if e.is_stw and e.pause_ms > 0]
    if not stw:
        return []

    if bucket_s is None:
        bucket_s = auto_bucket_s(parsed)

    import time

    has_epoch = any(e.timestamp is not None for e in stw)
    if has_epoch:
        uptime_offset = 0.0
    else:
        uptimes = [e.uptime for e in stw if e.uptime is not None]
        if not uptimes:
            return []
        uptime_offset = time.time() - max(uptimes)

    buckets: dict[int, list[GCEvent]] = {}
    for e in stw:
        t = _event_epoch(e, uptime_offset)
        if t is None:
            continue
        b = int(t // bucket_s) * bucket_s
        buckets.setdefault(b, []).append(e)

    if not buckets:
        return []

    out: list[tuple[int, dict]] = []
    for bts in sorted(buckets):
        sub = ParsedLog(
            node_id=parsed.node_id,
            collector=parsed.collector,
            java_hint=parsed.java_hint,
            events=buckets[bts],
            heap_max_mb=parsed.heap_max_mb,
            warnings=[],
        )
        out.append((bts, analyze(sub)["metrics"]))
    return out

# --------------------------------------------------------------------------- #
# Public helpers reused by the history store (operate on a metrics dict, so the
# same scoring/advice applies whether metrics come from a parsed log or from the
# aggregated time-series store).
# --------------------------------------------------------------------------- #
class _Meta:
    def __init__(self, collector: str):
        self.collector = collector


def score_health(metrics: dict) -> dict:
    return _health_score(metrics)


def derive_findings(metrics: dict, collector: str = "G1") -> dict:
    return _findings(metrics, _Meta(collector))
