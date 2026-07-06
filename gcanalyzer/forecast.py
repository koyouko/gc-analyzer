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
  2. Fit a Theil-Sen trend (median of all pairwise slopes). Unlike ordinary
     least squares, one incident day — a GC storm, a backfill, a noisy
     neighbor — cannot drag the slope, so the projection reflects the
     *sustained* growth rate, not the worst day.
  3. Project days-until-breach for the warning and critical thresholds
     already used by sar_analyzer.py / the GC alerting rules, from the
     current level (median of the last 7 daily points).
  4. Trend consistency (what fraction of pairwise slopes agree in sign with
     the median slope) plus history length become a low/medium/high
     confidence label — a jittery signal that happens to slope upward is
     reported, but at low confidence.

Cluster rollup maps the breaching resource onto a *proactive* scaling
direction consistent with scaling_advisor.py's reactive verdicts: uniform
CPU/disk/network growth -> plan horizontal; memory/swap growth -> plan
vertical (RAM); JVM heap growth -> plan vertical (heap); time-in-GC growth ->
tune GC first; a single hot node trending up while peers are flat -> watch /
rebalance that node before buying hardware.

Advisory only, like everything else in this project: it recommends a planning
horizon, it never acts.
"""

from __future__ import annotations

import statistics
from datetime import datetime, timezone

from . import sar_analyzer, store

DAY_S = 86400

MIN_DAYS = 7                 # fewer daily points than this -> insufficient_data
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

_GROUP_DIRECTION = {
    "compute": "plan_horizontal",
    "memory": "plan_vertical_memory",
    "heap": "plan_vertical_heap",
    "gc": "plan_tune_gc",
}

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
        if v is None:
            continue
        day = (r["ts"] // DAY_S) * DAY_S
        buckets.setdefault(day, []).append(float(v))
    out = []
    for day in sorted(buckets):
        vals = buckets[day]
        out.append((day, max(vals) if agg == "max" else statistics.fmean(vals)))
    return out


def _confidence(n_days: int, consistency: float) -> str:
    if n_days >= 21 and consistency >= 0.70:
        return "high"
    if n_days >= 14 and consistency >= 0.55:
        return "medium"
    return "low"


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
    "improving": "ok", "stable": "ok", "insufficient_data": "ok",
}
_RISK_RANK = {"critical": 3, "warning": 2, "watch": 1, "ok": 0}


# --------------------------------------------------------------------------- #
# Per-instance forecast
# --------------------------------------------------------------------------- #
def forecast_instance(c, instance_id: str, days: int = 90,
                      horizon_days: int = DEFAULT_HORIZON_DAYS, now: int = None) -> dict:
    now = now or store.now_ts(c)
    since = now - days * DAY_S
    gc_rows = store.window_rows(c, instance_id, since, now)
    host_rows = store.host_window_rows(c, instance_id, since, now)

    signals = []
    for spec in SIGNALS:
        rows = host_rows if spec["table"] == "host" else gc_rows
        daily = _daily_series(rows, spec["column"], spec["agg"])
        n_days = len(daily)
        base = {
            "signal": spec["signal"], "label": spec["label"], "unit": spec["unit"],
            "kind": spec["table"], "group": spec["group"],
            "warn_threshold": spec["warn"], "crit_threshold": spec["crit"],
            "n_days": n_days,
        }
        if n_days < MIN_DAYS:
            signals.append({**base, "current": round(daily[-1][1], 2) if daily else None,
                            "slope_per_day": None, "consistency": 0.0, "confidence": "low",
                            "days_to_warn": None, "days_to_crit": None,
                            "warn_date": None, "crit_date": None,
                            "status": "insufficient_data", "risk": "ok"})
            continue

        first_day = daily[0][0]
        pts = [((day - first_day) / DAY_S, val) for day, val in daily]
        slope, consistency = theil_sen(pts)
        current = statistics.median(v for _, v in daily[-CURRENT_WINDOW_DAYS:])
        days_to_warn = _eta_days(current, spec["warn"], slope)
        days_to_crit = _eta_days(current, spec["crit"], slope)
        status = _signal_status(current, spec["warn"], spec["crit"],
                                days_to_warn, days_to_crit, slope, horizon_days)
        signals.append({
            **base,
            "current": round(current, 2),
            "slope_per_day": round(slope, 4) if slope is not None else None,
            "consistency": consistency,
            "confidence": _confidence(n_days, consistency),
            "days_to_warn": days_to_warn,
            "days_to_crit": days_to_crit,
            "warn_date": _iso_date(now, days_to_warn) if days_to_warn is not None else None,
            "crit_date": _iso_date(now, days_to_crit) if days_to_crit is not None else None,
            "status": status,
            "risk": _STATUS_RISK[status],
        })

    signals.sort(key=lambda s: (-_RISK_RANK[s["risk"]],
                                s["days_to_crit"] if s["days_to_crit"] is not None else 1e9))
    risk = signals[0]["risk"] if signals else "ok"
    if not gc_rows and not host_rows:
        risk = "no_data"
    return {
        "instance_id": instance_id, "now": now, "days": days,
        "horizon_days": horizon_days, "risk": risk,
        "headline": _headline(signals, horizon_days),
        "signals": signals,
    }


def _headline(signals: list[dict], horizon_days: int) -> str:
    worst = next((s for s in signals if s["risk"] in ("critical", "warning", "watch")), None)
    if worst is None:
        fitted = [s for s in signals if s["status"] != "insufficient_data"]
        if not fitted:
            return "Not enough daily history yet to project capacity trends (need >= 7 days)."
        return (f"No capacity breach projected inside the next {horizon_days} days — "
                f"all tracked resources are flat, improving, or growing too slowly to matter.")
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
    now = now or store.now_ts(c)
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
    if not at_risk_nodes:
        return ("none", "high" if len(with_data) == total else "medium",
                f"No capacity breach projected on any of the {len(with_data)} node(s) with history "
                f"inside the next {horizon_days} days.",
                [f"All tracked resources on {len(with_data)}/{total} node(s) are flat, improving, "
                 f"or growing too slowly to breach within {horizon_days} days."],
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
    high_conf = sum(1 for cf in confs if cf == "high")
    confidence = "high" if high_conf >= max(1, len(confs) // 2) and len(dominant) >= 2 else \
                 ("medium" if confs else "low")

    if earliest:
        evidence.append(f"Earliest projected critical breach: {earliest[1]['label']} on "
                        f"{earliest[0]} in ~{earliest[2]:.0f} days (~{earliest[1]['crit_date']}).")
    evidence.append(f"{len(dominant)}/{total} node(s) show {label} trending toward or past its "
                    f"threshold within the {horizon_days}-day planning window.")

    if len(dominant) == 1 and len(with_data) >= 2:
        n = dominant[0]
        evidence.append(f"Only {n['instance_id']} is trending up while its peers are flat — this looks "
                        f"like uneven load (partition/leader skew), not cluster-wide growth.")
        return ("watch_hot_node", confidence,
                f"One node ({n['instance_id']}) is trending toward a {label} ceiling while peers have "
                f"headroom — rebalance partitions/leaders onto quieter brokers (and re-check the "
                f"forecast) before planning a cluster-wide scale.",
                evidence, warnings)

    verdict = _GROUP_DIRECTION[dominant_group]
    summaries = {
        "plan_horizontal":
            f"Plan a horizontal scale-out: {label} is growing across {len(dominant)}/{total} node(s). "
            f"Adding broker(s) and spreading partitions is the durable fix for cluster-wide "
            f"compute/IO/network growth.",
        "plan_vertical_memory":
            f"Plan a vertical (RAM) upgrade: host {label} is growing across {len(dominant)}/{total} "
            f"node(s). More memory per box (and swap kept off) addresses this more directly than "
            f"more brokers.",
        "plan_vertical_heap":
            f"Plan a JVM heap (-Xmx) increase: heap live set is growing across {len(dominant)}/{total} "
            f"node(s) while hosts have room — size the heap up before Full GCs start.",
        "plan_tune_gc":
            f"Time-in-GC is trending up across {len(dominant)}/{total} node(s) — tune GC (region size, "
            f"pause target, young gen) and re-check; if heap live set is also climbing, treat it as a "
            f"heap-sizing problem instead.",
    }
    return verdict, confidence, summaries[verdict], evidence, warnings
