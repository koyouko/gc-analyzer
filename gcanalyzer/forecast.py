"""
Capacity forecasting: when will this node/cluster run out of headroom?

The scaling advisor (scaling_advisor.py) answers "what should we do about the
pressure we see *today*". This module answers the proactive question that
comes right before it: "at the rate each resource is trending, when does it
cross its warning/critical threshold — and should we start planning a scale
now, before it becomes an incident?"

Method — deliberately simple, robust, and explainable (matching the project's
"deterministic rules are the source of truth" stance):

  1. Aggregate each signal to one value per day over a lookback window
     (default 90 days) from the same `metrics` / `host_metrics` tables the
     rest of the dashboard reads. Averaging signals use the daily mean,
     saturation signals (disk/NIC util, swap) use the daily max.
  2. Fit a Theil-Sen trend (median of all pairwise slopes) only with fresh,
     sufficiently dense calendar history spanning at least two weekly cycles.
     This is robust to isolated outliers but does not model seasonality.
  3. Project days-until-breach for the warning and critical thresholds
     already used by sar_analyzer.py / the GC alerting rules, from the
     current level (median of the last 7 daily points).
  4. Trend consistency qualifies the fit, not predictive accuracy. Weak fits
     have no breach date. No forecast here is backtested or calibrated.

Cluster rollups recommend investigation, not a scaling direction. Kafka-native
evidence and workload ownership are needed before choosing a capacity change.

Advisory only, like everything else in this project: it recommends a planning
horizon, it never acts.
"""

from __future__ import annotations

import statistics
from datetime import datetime, timezone

from . import sar_analyzer, store
from .correlate import _finite_number

DAY_S = 86400

MIN_DAYS = 14                # minimum two weekly cycles; not a seasonal model
MIN_CALENDAR_COVERAGE = 0.80
MAX_GAP_DAYS = 2
MAX_AGE_SECONDS = DAY_S
MIN_RECENT_DAYS = 5
CURRENT_WINDOW_DAYS = 7      # "current level" = median of the last N daily points
MAX_MEANINGFUL_ETA_DAYS = 730  # beyond the 2-year retention window, an ETA is noise
IMMINENT_DAYS = 30           # critical breach inside this window -> act now
DEFAULT_HORIZON_DAYS = 90    # default planning window for "projected" status

# Slope smaller than this (units/day) is treated as flat — for %-scale
# signals that's < ~0.4% growth per year.
FLAT_SLOPE_EPS = 1e-3

# GC-side thresholds (host-side ones come straight from sar_analyzer).
HEAP_AFTER_WARN_PCT = 75.0
HEAP_AFTER_CRIT_PCT = 85.0
TIME_IN_GC_WARN_PCT = 5.0
TIME_IN_GC_CRIT_PCT = 10.0

# signal -> (table, column, daily aggregation, label, unit, warn, crit, group)
# group drives the cluster-level "which scaling direction does this imply".
SIGNALS: list[dict] = [
    {"signal": "cpu_busy_pct", "table": "host", "column": "cpu_busy_pct", "agg": "avg",
     "label": "CPU busy", "unit": "%", "warn": sar_analyzer.CPU_BUSY_WARN_PCT,
     "crit": sar_analyzer.CPU_BUSY_CRIT_PCT, "group": "compute"},
    {"signal": "cpu_iowait_pct", "table": "host", "column": "cpu_iowait_pct", "agg": "avg",
     "label": "CPU iowait", "unit": "%", "warn": sar_analyzer.IOWAIT_WARN_PCT,
     "crit": sar_analyzer.IOWAIT_CRIT_PCT, "group": "compute"},
    {"signal": "mem_used_pct", "table": "host", "column": "mem_used_pct", "agg": "avg",
     "label": "Memory used", "unit": "%", "warn": sar_analyzer.MEM_WARN_PCT,
     "crit": sar_analyzer.MEM_CRIT_PCT, "group": "memory"},
    {"signal": "swap_used_pct", "table": "host", "column": "swap_used_pct", "agg": "max",
     "label": "Swap used (peak)", "unit": "%", "warn": sar_analyzer.SWAP_WARN_PCT,
     "crit": sar_analyzer.SWAP_CRIT_PCT, "group": "memory"},
    {"signal": "disk_util_pct_max", "table": "host", "column": "disk_util_pct_max", "agg": "max",
     "label": "Disk util (busiest)", "unit": "%", "warn": sar_analyzer.DISK_UTIL_WARN_PCT,
     "crit": sar_analyzer.DISK_UTIL_CRIT_PCT, "group": "compute"},
    {"signal": "net_util_pct_max", "table": "host", "column": "net_util_pct_max", "agg": "max",
     "label": "NIC util (busiest)", "unit": "%", "warn": sar_analyzer.NET_UTIL_WARN_PCT,
     "crit": sar_analyzer.NET_UTIL_CRIT_PCT, "group": "compute"},
    {"signal": "heap_after_pct", "table": "gc", "column": "heap_after_pct", "agg": "avg",
     "label": "Heap live set", "unit": "%", "warn": HEAP_AFTER_WARN_PCT,
     "crit": HEAP_AFTER_CRIT_PCT, "group": "heap"},
    {"signal": "time_in_gc_pct", "table": "gc", "column": "time_in_gc_pct", "agg": "avg",
     "label": "Time in GC", "unit": "%", "warn": TIME_IN_GC_WARN_PCT,
     "crit": TIME_IN_GC_CRIT_PCT, "group": "gc"},
]

_GROUP_LABEL = {
    "compute": "CPU/disk/network",
    "memory": "host memory/swap",
    "heap": "JVM heap",
    "gc": "time-in-GC",
}


# --------------------------------------------------------------------------- #
# Trend fitting
# --------------------------------------------------------------------------- #
def theil_sen(points: list[tuple[float, float]]) -> tuple[float | None, float]:
    """Median pairwise slope over (x, y) points.

    Returns (slope, consistency) where consistency is the fraction of pairwise
    slopes that agree in sign with the median slope (0..1). Returns
    (None, 0.0) with < 2 points.
    """
    n = len(points)
    if n < 2:
        return None, 0.0
    slopes: list[float] = []
    for i in range(n - 1):
        xi, yi = points[i]
        for j in range(i + 1, n):
            xj, yj = points[j]
            dx = xj - xi
            if dx == 0:
                continue
            slopes.append((yj - yi) / dx)
    if not slopes:
        return None, 0.0
    slope = statistics.median(slopes)
    if abs(slope) < FLAT_SLOPE_EPS:
        return slope, 0.0
    agree = sum(1 for s in slopes if (s > 0) == (slope > 0) and abs(s) >= FLAT_SLOPE_EPS)
    return slope, round(agree / len(slopes), 3)


def _daily_series(rows: list[dict], column: str, agg: str) -> list[tuple[int, float]]:
    """[(day_epoch, value)] — one point per calendar day present in `rows`."""
    buckets: dict[int, list[float]] = {}
    for r in rows:
        v = r.get(column)
        if not _finite_number(v) or not _finite_number(r.get("ts")):
            continue
        day = (r["ts"] // DAY_S) * DAY_S
        buckets.setdefault(day, []).append(float(v))
    out = []
    for day in sorted(buckets):
        vals = buckets[day]
        out.append((day, max(vals) if agg == "max" else statistics.fmean(vals)))
    return out


def _confidence(n_days: int, consistency: float) -> str:
    if n_days >= MIN_DAYS and consistency >= 0.70:
        return "medium"
    return "low"


def _coverage(daily: list[tuple[int, float]], last: int | None, now: int) -> dict:
    today = now // DAY_S * DAY_S
    span = (today - daily[0][0]) // DAY_S + 1 if daily else 0
    coverage = len(daily) / span if span > 0 else 0.0
    recent = sum(day >= today - (CURRENT_WINDOW_DAYS - 1) * DAY_S for day, _ in daily)
    gaps = [(b[0] - a[0]) / DAY_S for a, b in zip(daily, daily[1:])]
    age = now - last if last is not None else None
    reason = None
    if not daily:
        reason = "missing_history"
    elif age is None or age < 0 or age > MAX_AGE_SECONDS:
        reason = "stale_history"
    elif len(daily) < MIN_DAYS:
        reason = "insufficient_calendar_history"
    elif coverage < MIN_CALENDAR_COVERAGE or max(gaps, default=0) > MAX_GAP_DAYS or recent < MIN_RECENT_DAYS:
        reason = "sparse_calendar_history"
    return {"calendar_span_days": span, "coverage_ratio": round(coverage, 3),
            "recent_observed_days": recent, "max_gap_days": max(gaps, default=0),
            "last_observed_at": last, "age_seconds": age, "withheld_reason": reason}


def _eta_days(current: float, threshold: float, slope: float | None) -> float | None:
    """Days until `current` reaches `threshold` at `slope` units/day."""
    if slope is None or slope <= FLAT_SLOPE_EPS:
        return None
    if current >= threshold:
        return 0.0
    eta = (threshold - current) / slope
    if eta > MAX_MEANINGFUL_ETA_DAYS:
        return None
    return round(eta, 1)


def _iso_date(now: int, days_ahead: float) -> str:
    return datetime.fromtimestamp(now + days_ahead * DAY_S, tz=timezone.utc).strftime("%Y-%m-%d")


def _signal_status(current: float, warn: float, crit: float,
                   days_to_warn: float | None, days_to_crit: float | None,
                   slope: float | None, horizon_days: int) -> str:
    if current >= crit:
        return "already_critical"
    if days_to_crit is not None and days_to_crit <= IMMINENT_DAYS:
        return "breach_imminent"
    if current >= warn:
        return "already_warning"
    if days_to_crit is not None and days_to_crit <= horizon_days:
        return "breach_projected"
    if days_to_warn is not None and days_to_warn <= horizon_days:
        return "watch"
    if slope is not None and slope < -FLAT_SLOPE_EPS:
        return "improving"
    return "stable"


_STATUS_RISK = {
    "already_critical": "critical", "breach_imminent": "critical",
    "already_warning": "warning", "breach_projected": "warning",
    "watch": "watch",
    "improving": "ok", "stable": "ok", "insufficient_data": "no_data", "trend_only": "no_data",
}
_RISK_RANK = {"critical": 3, "warning": 2, "watch": 1, "ok": 0, "no_data": -1}


# --------------------------------------------------------------------------- #
# Per-instance forecast
# --------------------------------------------------------------------------- #
def forecast_instance(c, instance_id: str, days: int = 90,
                      horizon_days: int = DEFAULT_HORIZON_DAYS, now: int = None) -> dict:
    now = store.now_ts(c) if now is None else now
    since = now - days * DAY_S
    gc_rows = store.window_rows(c, instance_id, since, now)
    host_rows = store.host_window_rows(c, instance_id, since, now)
    source_quality = {
        "gc": (store.current_snapshot(c, instance_id, now) or {}).get("quality") or {},
        "host": (store.current_host_snapshot(c, instance_id, now) or {}).get("quality") or {},
    }

    signals = []
    for spec in SIGNALS:
        rows = host_rows if spec["table"] == "host" else gc_rows
        rows = [r for r in rows if _finite_number(r.get("ts")) and since <= r["ts"] <= now
                and _finite_number(r.get(spec["column"]))]
        daily = _daily_series(rows, spec["column"], spec["agg"])
        n_days = len(daily)
        coverage = _coverage(daily, max((r["ts"] for r in rows), default=None), now)
        quality = source_quality[spec["table"]]
        if quality and quality.get("state") not in ("fresh", "partial"):
            coverage["withheld_reason"] = "source_" + str(quality.get("state", "unknown"))
        base = {
            "signal": spec["signal"], "label": spec["label"], "unit": spec["unit"],
            "kind": spec["table"], "group": spec["group"],
            "warn_threshold": spec["warn"], "crit_threshold": spec["crit"],
            "n_days": n_days,
            "method": "theil_sen", "validated": False, "source_quality": quality,
            **coverage,
        }
        if coverage["withheld_reason"]:
            signals.append({**base, "current": round(daily[-1][1], 2) if daily else None,
                            "slope_per_day": None, "consistency": 0.0, "confidence": "low",
                            "days_to_warn": None, "days_to_crit": None,
                            "warn_date": None, "crit_date": None,
                            "status": "insufficient_data", "risk": "no_data", "prediction_ready": False})
            continue

        first_day = daily[0][0]
        pts = [((day - first_day) / DAY_S, val) for day, val in daily]
        slope, consistency = theil_sen(pts)
        recent_start = (now // DAY_S - CURRENT_WINDOW_DAYS + 1) * DAY_S
        current = statistics.median(v for day, v in daily if day >= recent_start)
        confidence = _confidence(n_days, consistency)
        days_to_warn = _eta_days(current, spec["warn"], slope) if confidence == "medium" else None
        days_to_crit = _eta_days(current, spec["crit"], slope) if confidence == "medium" else None
        status = _signal_status(current, spec["warn"], spec["crit"],
                                days_to_warn, days_to_crit, slope, horizon_days)
        weak_trend = confidence == "low" and slope is not None and abs(slope) >= FLAT_SLOPE_EPS
        if weak_trend and current < spec["warn"]:
            status = "trend_only"
        signals.append({
            **base,
            "current": round(current, 2),
            "slope_per_day": round(slope, 4) if slope is not None else None,
            "consistency": consistency,
            "confidence": confidence,
            "prediction_ready": confidence == "medium",
            "withheld_reason": "weak_trend_consistency" if weak_trend else None,
            "days_to_warn": days_to_warn,
            "days_to_crit": days_to_crit,
            "warn_date": _iso_date(now, days_to_warn) if days_to_warn is not None else None,
            "crit_date": _iso_date(now, days_to_crit) if days_to_crit is not None else None,
            "status": status,
            "risk": _STATUS_RISK[status],
        })

    signals.sort(key=lambda s: (-_RISK_RANK[s["risk"]],
                                s["days_to_crit"] if s["days_to_crit"] is not None else 1e9))
    risk = signals[0]["risk"] if signals else "no_data"
    if risk == "ok" and any(s["risk"] == "no_data" for s in signals):
        risk = "no_data"
    return {
        "instance_id": instance_id, "now": now, "days": days,
        "horizon_days": horizon_days, "risk": risk,
        "headline": _headline(signals, horizon_days),
        "signals": signals,
        "quality": {"state": "missing" if all(s["risk"] == "no_data" for s in signals)
                    else "partial" if any(s["risk"] == "no_data" for s in signals) else "fresh"},
        "method": "theil_sen", "validated": False,
        "notice": "Advisory linear trend estimates, not validated capacity predictions. Dates have no calibrated uncertainty interval; seasonality and configuration changes are not modeled.",
    }


def _headline(signals: list[dict], horizon_days: int) -> str:
    worst = next((s for s in signals if s["risk"] in ("critical", "warning", "watch")), None)
    if worst is None:
        fitted = [s for s in signals if s["risk"] != "no_data"]
        if not fitted:
            return f"Not enough fresh, dense calendar history or consistent trend evidence (need >= {MIN_DAYS} observed days)."
        if len(fitted) < len(signals):
            return "No breach indicated in the usable signals; other resources lack reliable forecast evidence."
        return (f"No capacity breach projected inside the next {horizon_days} days — "
                "the usable linear trends do not cross the tracked thresholds. This is not a capacity guarantee.")
    cur = f"{worst['current']}{worst['unit']}"
    if worst["status"] == "already_critical":
        return f"{worst['label']} is already past its critical threshold ({cur} >= {worst['crit_threshold']}{worst['unit']})."
    if worst["status"] == "already_warning":
        return f"{worst['label']} is already past its warning threshold ({cur} >= {worst['warn_threshold']}{worst['unit']})."
    if worst["days_to_crit"] is not None:
        return (f"{worst['label']} is trending toward its critical threshold "
                f"({worst['crit_threshold']}{worst['unit']}) in ~{worst['days_to_crit']:.0f} days "
                f"(around {worst['crit_date']}).")
    return (f"{worst['label']} is trending toward its warning threshold "
            f"({worst['warn_threshold']}{worst['unit']}) in ~{worst['days_to_warn']:.0f} days "
            f"(around {worst['warn_date']}).")


# --------------------------------------------------------------------------- #
# Cluster rollup -> proactive scaling direction
# --------------------------------------------------------------------------- #
def forecast_cluster(c, cluster: str, role: str = "broker", days: int = 90,
                     horizon_days: int = DEFAULT_HORIZON_DAYS, now: int = None) -> dict:
    now = store.now_ts(c) if now is None else now
    instances = [i for i in store.list_instances(c) if i["cluster"] == cluster and i["role"] == role]
    if not instances:
        return {
            "cluster": cluster, "role": role, "now": now, "n_nodes": 0,
            "horizon_days": horizon_days, "verdict": "insufficient_data",
            "confidence": "low",
            "summary": f"No '{role}' instances found in cluster '{cluster}'.",
            "evidence": [], "nodes": [], "warnings": [],
        }

    nodes = []
    for inst in instances:
        fc = forecast_instance(c, inst["id"], days=days, horizon_days=horizon_days, now=now)
        at_risk = [s for s in fc["signals"] if s["risk"] in ("critical", "warning")]
        top = fc["signals"][0] if fc["signals"] else None
        nodes.append({
            "instance_id": inst["id"],
            "risk": fc["risk"],
            "quality": fc["quality"],
            "headline": fc["headline"],
            "top_signal": top["signal"] if top else None,
            "top_label": top["label"] if top else None,
            "top_status": top["status"] if top else None,
            "top_current": top["current"] if top else None,
            "top_unit": top["unit"] if top else None,
            "top_days_to_crit": top["days_to_crit"] if top else None,
            "top_crit_date": top["crit_date"] if top else None,
            "top_confidence": top["confidence"] if top else None,
            "at_risk_groups": sorted({s["group"] for s in at_risk}),
            "at_risk_signals": [
                {"signal": s["signal"], "label": s["label"], "group": s["group"],
                 "status": s["status"], "current": s["current"], "unit": s["unit"],
                 "days_to_crit": s["days_to_crit"], "crit_date": s["crit_date"],
                 "confidence": s["confidence"]}
                for s in at_risk
            ],
        })

    verdict, confidence, summary, evidence, warnings = _decide_cluster(nodes, horizon_days)
    return {
        "cluster": cluster, "role": role, "now": now, "n_nodes": len(nodes),
        "horizon_days": horizon_days, "verdict": verdict, "confidence": confidence,
        "summary": summary, "evidence": evidence, "nodes": nodes, "warnings": warnings,
    }


def _decide_cluster(nodes: list[dict], horizon_days: int):
    total = len(nodes)
    with_data = [n for n in nodes if n["risk"] != "no_data"]
    if not with_data:
        return ("insufficient_data", "low",
                f"{total} node(s) found but none have enough GC or host history to project trends.",
                [], [])

    at_risk_nodes = [n for n in with_data if n["at_risk_signals"]]
    complete = len(with_data) == total and all(n.get("quality", {}).get("state") == "fresh" for n in nodes)
    if not at_risk_nodes:
        if not complete:
            return ("insufficient_data", "low", "Some nodes or resources lack reliable forecast evidence.", [], [])
        return ("none", "medium",
                f"No capacity breach projected on any of the {len(with_data)} node(s) with history "
                f"inside the next {horizon_days} days.",
                ["No breach indicated by the usable linear trends; this is not a capacity guarantee."],
                [])

    # Which resource groups are driving the risk, and on how many nodes each?
    group_nodes: dict[str, list[dict]] = {}
    for n in at_risk_nodes:
        for g in n["at_risk_groups"]:
            group_nodes.setdefault(g, []).append(n)
    dominant_group, dominant = max(group_nodes.items(), key=lambda kv: len(kv[1]))

    # Earliest projected critical breach across the cluster (the planning clock).
    earliest = None
    for n in at_risk_nodes:
        for s in n["at_risk_signals"]:
            eta = s["days_to_crit"]
            if eta is None:
                continue
            if earliest is None or eta < earliest[2]:
                earliest = (n["instance_id"], s, eta)

    warnings = []
    for n in at_risk_nodes:
        for s in n["at_risk_signals"]:
            if s["days_to_crit"] is not None and s["status"] in ("breach_imminent", "breach_projected"):
                warnings.append(f"{n['instance_id']}: {s['label']} projected to hit critical "
                                f"({s['current']}{s['unit']} today) in ~{s['days_to_crit']:.0f} days "
                                f"(~{s['crit_date']}).")
            elif s["status"] in ("already_critical", "already_warning"):
                warnings.append(f"{n['instance_id']}: {s['label']} is already at {s['current']}{s['unit']} "
                                f"({s['status'].replace('_', ' ')}).")

    evidence = []
    label = _GROUP_LABEL[dominant_group]
    confs = [s["confidence"] for n in dominant for s in n["at_risk_signals"] if s["group"] == dominant_group]
    confidence = "medium" if complete and confs and all(cf == "medium" for cf in confs) else "low"
    if not complete:
        evidence.append("Some nodes or resources lack reliable history; cluster-wide headroom is unknown.")

    if earliest:
        evidence.append(f"Earliest projected critical breach: {earliest[1]['label']} on "
                        f"{earliest[0]} in ~{earliest[2]:.0f} days (~{earliest[1]['crit_date']}).")
    evidence.append(f"{len(dominant)}/{total} node(s) show {label} trending toward or past its "
                    f"threshold within the {horizon_days}-day planning window.")
    evidence.append("These are unvalidated linear trends, not a Kafka capacity model. Check Kafka request rates, "
                    "latency, replication and leader distribution before changing broker count, hardware or heap.")

    if len(dominant) == 1 and len(with_data) >= 2:
        n = dominant[0]
        return ("investigate", confidence,
                f"Review {label} evidence on {n['instance_id']} and compare workload distribution with peers; "
                "host trends alone do not establish leader skew or a need to rebalance.",
                evidence, warnings)

    return ("investigate", confidence,
            f"Investigate {label} trends on {len(dominant)}/{total} node(s) before choosing a capacity change.",
            evidence, warnings)
