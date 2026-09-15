"""
ML Tech Preview: anomaly scoring over combined GC + host (SAR) features.

Extends the dashboard's existing "ML Tech Preview" concept (see README) to the
joint GC/host signal correlate.py joins. Like the rest of that preview surface,
this is explicitly advisory: deterministic rules (analyzer.py, sar_analyzer.py,
scaling_advisor.py) remain the source of truth for health grades and scaling
recommendations. This module never changes a grade or a verdict — it only adds
"does this look statistically unusual for this specific node" on top.

Two methods, picked automatically:

  * robust_zscore (always available, zero extra dependencies) — per-feature
    modified z-score against a median/MAD baseline (Iglewicz & Hoaglin's
    outlier rule: |0.6745*(x-median)/MAD| > 3.5 flags a point). Robust to the
    GC storms/incidents already in the baseline window not dragging the
    "normal" estimate around the way a mean/stdev approach would.
  * isolation_forest (used when scikit-learn is installed) — a multivariate
    model over the joined hourly GC+host feature vector, so it can catch
    *combinations* that are unusual together even if no single feature crosses
    its own threshold (e.g. moderately high pause + moderately high iowait at
    the same time, on a node where those two normally move independently).

Either way the output shape is identical so the dashboard doesn't care which
ran; `method` tells you which one produced the score.
"""

from __future__ import annotations

import statistics

from . import store
from .correlate import rebucket_hourly

MIN_BASELINE_POINTS = 10
ZSCORE_FLAG = 3.5  # Iglewicz & Hoaglin's recommended modified z-score threshold

_FEATURES = [
    ("time_in_gc_pct", "gc"), ("pause_p99_ms", "gc"), ("pause_max_ms", "gc"),
    ("full_gc_count", "gc"), ("heap_after_pct", "gc"),
    ("cpu_busy_pct", "host"), ("cpu_iowait_pct", "host"), ("mem_used_pct", "host"),
    ("swap_used_pct", "host"), ("disk_util_pct_max", "host"), ("net_util_pct_max", "host"),
]

# Minimum MAD scales in each metric's native units, not severity thresholds.
_NOISE_FLOORS = {
    "time_in_gc_pct": 0.1, "pause_p99_ms": 5.0, "pause_max_ms": 5.0,
    "full_gc_count": 0.25, "heap_after_pct": 1.0,
    "cpu_busy_pct": 1.0, "cpu_iowait_pct": 0.5, "mem_used_pct": 1.0,
    "swap_used_pct": 0.5, "disk_util_pct_max": 1.0, "net_util_pct_max": 1.0,
}


def _mad(vals: list[float], median: float) -> float:
    deviations = [abs(v - median) for v in vals]
    return statistics.median(deviations) if deviations else 0.0


MAX_DISPLAY_Z = 50.0  # cap so a near-zero MAD (e.g. an always-0 baseline like
                       # full_gc_count) can't blow the score up into the millions


def _modified_zscores(baseline: list[float], recent: list[float], noise_floor: float = 0.1) -> list[float]:
    if len(baseline) < 3:
        return [0.0] * len(recent)
    median = statistics.median(baseline)
    mad = max(_mad(baseline, median), noise_floor, 0.1)
    zs = [0.6745 * (x - median) / mad for x in recent]
    return [max(-MAX_DISPLAY_Z, min(MAX_DISPLAY_Z, z)) for z in zs]


def _row_value(row: dict, gc_col_map: dict, key: str) -> float | None:
    mapped = gc_col_map.get(key, key)
    return row.get(mapped)


# Map our friendly feature names onto the actual `metrics` table columns.
_GC_COLS = {
    "time_in_gc_pct": "time_in_gc_pct", "pause_p99_ms": "pause_p99_ms",
    "pause_max_ms": "pause_max_ms", "full_gc_count": "full_gc_count",
    "heap_after_pct": "heap_after_pct",
}
_HOST_COLS = {
    "cpu_busy_pct": "cpu_busy_pct", "cpu_iowait_pct": "cpu_iowait_pct",
    "mem_used_pct": "mem_used_pct", "swap_used_pct": "swap_used_pct",
    "disk_util_pct_max": "disk_util_pct_max", "net_util_pct_max": "net_util_pct_max",
}


def _joined_hourly(c, instance_id: str, since: int, now: int) -> dict[int, dict]:
    gc_rows = store.window_rows(c, instance_id, since, now)
    host_rows = store.host_window_rows(c, instance_id, since, now)
    gc_hourly = rebucket_hourly(gc_rows, list(_GC_COLS.values())) if gc_rows else {}
    host_hourly = rebucket_hourly(host_rows, list(_HOST_COLS.values())) if host_rows else {}
    hours = sorted(set(gc_hourly) | set(host_hourly))
    out = {}
    for h in hours:
        row = {}
        row.update(gc_hourly.get(h, {}))
        row.update(host_hourly.get(h, {}))
        out[h] = row
    return out


def analyze_anomalies(c, instance_id: str, days: int = 30, recent_hours: int = 24, now: int = None) -> dict:
    now = store.now_ts(c) if now is None else now
    since = now - days * 86400
    split = now - recent_hours * 3600

    joined = _joined_hourly(c, instance_id, since, now)
    baseline_hours = sorted(h for h in joined if h < split)
    recent_hours_list = sorted(h for h in joined if h >= split)
    source_quality = {
        "gc": (store.current_snapshot(c, instance_id, now) or {}).get("quality") or {},
        "host": (store.current_host_snapshot(c, instance_id, now) or {}).get("quality") or {},
    }
    for name, src in _FEATURES:
        quality = source_quality[src]
        if quality and quality.get("state") not in ("fresh", "partial"):
            for h in recent_hours_list:
                joined[h][name] = None

    empty = {
        "instance_id": instance_id, "method": "none", "methods": [],
        "readiness": "insufficient_data", "n_baseline": len(baseline_hours),
        "n_recent": len(recent_hours_list), "overall_anomaly_score": None, "is_anomalous": False,
        "features": [], "isolation_forest": None, "notice": _NOTICE,
        "model_status": {"robust_zscore": "insufficient_data", "isolation_forest": "not_run",
                         "persistent_model": False},
        "source_quality": source_quality,
    }

    if len(baseline_hours) < MIN_BASELINE_POINTS or not recent_hours_list:
        return {
            **empty,
            "message": f"Need >= {MIN_BASELINE_POINTS} baseline hours of data "
                       f"(have {len(baseline_hours)}) before anomaly scoring is meaningful.",
        }

    feature_results = []
    for name, _src in _FEATURES:
        baseline_vals = [joined[h][name] for h in baseline_hours if joined[h].get(name) is not None]
        recent_vals = [joined[h].get(name) for h in recent_hours_list]
        recent_present = [(h, v) for h, v in zip(recent_hours_list, recent_vals) if v is not None]
        if len(baseline_vals) < MIN_BASELINE_POINTS or not recent_present:
            continue
        zs = _modified_zscores(baseline_vals, [v for _, v in recent_present], _NOISE_FLOORS[name])
        worst_idx = max(range(len(zs)), key=lambda i: abs(zs[i]))
        feature_results.append({
            "feature": name,
            "n_baseline": len(baseline_vals), "n_recent": len(recent_present),
            "noise_floor": _NOISE_FLOORS[name],
            "baseline_median": round(statistics.median(baseline_vals), 3),
            "recent_max_abs_z": round(abs(zs[worst_idx]), 2),
            "recent_worst_value": round(recent_present[worst_idx][1], 3),
            "recent_worst_hour": recent_present[worst_idx][0],
            "anomalous": abs(zs[worst_idx]) >= ZSCORE_FLAG,
            "direction": "up" if zs[worst_idx] > 0 else "down" if zs[worst_idx] < 0 else "unchanged",
        })

    if not feature_results:
        return {**empty, "message": "No feature has sufficient baseline and usable recent evidence for scoring."}

    method = "robust_zscore"
    sklearn_scores = None
    try:
        candidate = _isolation_forest_scores(joined, baseline_hours, recent_hours_list)
        model_state = candidate.get("status", "used") if candidate is not None else "insufficient_data"
        if model_state == "used":
            sklearn_scores = candidate
            method = "isolation_forest"
    except ImportError:
        model_state = "unavailable"
    except Exception:
        model_state = "error"

    feature_results.sort(key=lambda f: f["recent_max_abs_z"], reverse=True)
    top_z = feature_results[0]["recent_max_abs_z"] if feature_results else 0.0
    # Scale modified z-score onto a friendlier 0-100 "how unusual" dial; z=3.5 (flag
    # threshold) lands at 50, z>=7 saturates at 100. Purely presentational.
    overall = round(min(100.0, max(0.0, (top_z / 7.0) * 100.0)), 1)
    is_anomalous = any(f["anomalous"] for f in feature_results)

    if sklearn_scores is not None:
        is_anomalous = is_anomalous or sklearn_scores["is_anomalous"]
        overall = max(overall, sklearn_scores["overall_anomaly_score"])

    return {
        "instance_id": instance_id,
        "method": method,
        "methods": ["robust_zscore"] + (["isolation_forest"] if sklearn_scores is not None else []),
        "readiness": "ready" if len(feature_results) == len(_FEATURES) else "partial",
        "model_status": {"robust_zscore": "used", "isolation_forest": model_state,
                         "persistent_model": False,
                         "training": "request_local" if sklearn_scores is not None else "none"},
        "source_quality": source_quality,
        "n_baseline": len(baseline_hours),
        "n_recent": len(recent_hours_list),
        "overall_anomaly_score": overall,
        "is_anomalous": is_anomalous,
        "features": feature_results,
        "isolation_forest": sklearn_scores,
        "notice": _NOTICE,
    }


def _isolation_forest_scores(joined: dict[int, dict], baseline_hours: list[int], recent_hours_list: list[int]):
    """Multivariate anomaly score over the joined feature vector. Returns None
    (caller falls back to robust_zscore-only) if scikit-learn isn't installed
    or there isn't enough complete-row data to fit a model."""
    try:
        from sklearn.ensemble import IsolationForest
    except ImportError:
        return {"status": "unavailable"}

    cols = [name for name, _ in _FEATURES]

    def row_vec(h: int) -> list[float] | None:
        row = joined[h]
        vec = [row.get(c) for c in cols]
        if any(v is None for v in vec):
            return None
        return vec

    baseline_matrix = [v for h in baseline_hours if (v := row_vec(h)) is not None]
    recent_matrix = [(h, v) for h in recent_hours_list if (v := row_vec(h)) is not None]
    if len(baseline_matrix) < MIN_BASELINE_POINTS or not recent_matrix:
        return {"status": "insufficient_data"}

    model = IsolationForest(n_estimators=100, contamination="auto", random_state=7)
    model.fit(baseline_matrix)
    scores = model.decision_function([v for _, v in recent_matrix])  # higher = more normal
    preds = model.predict([v for _, v in recent_matrix])  # -1 = anomaly, 1 = normal

    worst_i = min(range(len(scores)), key=lambda i: scores[i])
    # decision_function is roughly in [-0.5, 0.5]; flip + scale to 0-100 for display.
    overall = round(min(100.0, max(0.0, float(0.5 - scores[worst_i]) / 0.7 * 100.0)), 1)
    return {
        "status": "used",
        "is_anomalous": bool((preds == -1).any()),
        "overall_anomaly_score": float(overall),
        "worst_hour": int(recent_matrix[worst_i][0]),
        "n_flagged": int((preds == -1).sum()),
        "n_scored": len(recent_matrix),
    }


_NOTICE = (
    "ML Tech Preview: statistical unusualness, advisory only, not a probability, "
    "calibrated severity or diagnosis. Scores do not change health grades. "
    "Optional Isolation Forest is fitted per request; no persistent trained model "
    "or continuous learning is running. Source quality and missing features limit readiness."
)
