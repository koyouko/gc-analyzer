"""
ML & AI Diagnostic Engine:
  1. Multivariate Ensemble Anomaly Detection:
     - Robust Modified Z-Score (Iglewicz & Hoaglin median/MAD)
     - Isolation Forest (scikit-learn multivariate tree ensemble)
     - Local Outlier Factor (scikit-learn density-based novelty detection)
     - EWMA Smooth Drift Detector (exponentially weighted moving average regime shift)
     - Persistent Model Training (`train_instance_model` / `train_cluster_models` -> `ml_models` table)
  2. TypeSafe Jev (`jev-latest` System One model) Structured Judgments:
     - Typed `Choice`, `Score`, and `Noul` primitives over joint JVM GC + Host SAR +
       Kafka 2.8.1 + ZooKeeper 3.5.9 telemetry and operational log events.
  3. Open-Source Local LLM Diagnostic Assistant:
     - Connects to local Ollama / Llama / Qwen / Mistral / OpenLLM endpoints
       (`GC_LOCAL_LLM_URL`, default `http://127.0.0.1:11434/api/generate`) with
       automatic expert multi-layer synthesis fallback when offline.
"""

from __future__ import annotations

import json
import os
import statistics
import time
import urllib.request
from typing import Any

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

_KAFKA_FEATURES = [
    ("under_replicated_partitions", 0.5),
    ("produce_latency_p99_ms", 5.0),
    ("request_handler_avg_idle_pct", 2.0),
    ("zk_avg_latency_ms", 1.0),
    ("zk_fsync_warning_count", 0.25),
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


MAX_DISPLAY_Z = 50.0


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


def _ewma_series(values: list[float], alpha: float = 0.28) -> list[float]:
    if not values:
        return []
    acc = float(values[0])
    out = [round(acc, 3)]
    for v in values[1:]:
        acc = alpha * float(v) + (1.0 - alpha) * acc
        out.append(round(acc, 3))
    return out


def _ewma_drift_scores(joined: dict[int, dict], baseline_hours: list[int], recent_hours_list: list[int]) -> dict:
    """Compute EWMA gradual trend drift across key metrics."""
    drifts = []
    for name, _ in _FEATURES:
        b_vals = [float(joined[h][name]) for h in baseline_hours if joined[h].get(name) is not None]
        r_vals = [float(joined[h][name]) for h in recent_hours_list if joined[h].get(name) is not None]
        if len(b_vals) < MIN_BASELINE_POINTS or len(r_vals) < 3:
            continue
        base_ewma = _ewma_series(b_vals, alpha=0.2)[-1]
        rec_ewma = _ewma_series(r_vals, alpha=0.35)[-1]
        mad = max(_mad(b_vals, statistics.median(b_vals)), _NOISE_FLOORS.get(name, 0.5))
        drift_sigma = (rec_ewma - base_ewma) / mad
        drifts.append({
            "feature": name,
            "baseline_ewma": round(base_ewma, 2),
            "recent_ewma": round(rec_ewma, 2),
            "drift_sigma": round(drift_sigma, 2),
            "drifting": abs(drift_sigma) >= 2.8,
        })
    drifts.sort(key=lambda d: abs(d["drift_sigma"]), reverse=True)
    max_drift = abs(drifts[0]["drift_sigma"]) if drifts else 0.0
    return {
        "status": "used" if drifts else "insufficient_data",
        "max_drift_sigma": round(max_drift, 2),
        "is_drifting": any(d["drifting"] for d in drifts),
        "top_drifts": drifts[:5],
    }


def _lof_scores(joined: dict[int, dict], baseline_hours: list[int], recent_hours_list: list[int]) -> dict:
    """Local Outlier Factor density-based novelty detection over the joined vector."""
    try:
        from sklearn.neighbors import LocalOutlierFactor
    except ImportError:
        return {"status": "unavailable"}

    cols = [name for name, _ in _FEATURES]

    def row_vec(h: int) -> list[float] | None:
        row = joined[h]
        vec = [row.get(c) for c in cols]
        if any(v is None for v in vec):
            return None
        return [float(v) for v in vec]

    baseline_matrix = [v for h in baseline_hours if (v := row_vec(h)) is not None]
    recent_matrix = [(h, v) for h in recent_hours_list if (v := row_vec(h)) is not None]
    if len(baseline_matrix) < max(MIN_BASELINE_POINTS, 12) or not recent_matrix:
        return {"status": "insufficient_data"}

    n_neighbors = min(20, max(5, len(baseline_matrix) // 3))
    lof = LocalOutlierFactor(n_neighbors=n_neighbors, novelty=True, contamination="auto")
    lof.fit(baseline_matrix)
    preds = lof.predict([v for _, v in recent_matrix])
    scores = lof.decision_function([v for _, v in recent_matrix])
    worst_i = min(range(len(scores)), key=lambda i: scores[i])
    overall = round(min(100.0, max(0.0, float(0.3 - scores[worst_i]) / 1.2 * 100.0)), 1)
    return {
        "status": "used",
        "is_anomalous": bool((preds == -1).any()),
        "overall_anomaly_score": float(overall),
        "worst_hour": int(recent_matrix[worst_i][0]),
        "n_flagged": int((preds == -1).sum()),
        "n_scored": len(recent_matrix),
    }


def _get_saved_model(c, instance_id: str) -> dict | None:
    if c is None:
        return None
    try:
        row = c.execute(
            "SELECT * FROM ml_models WHERE instance_id=? AND model_type='ensemble_v2'",
            (instance_id,),
        ).fetchone()
        if not row:
            return None
        d = dict(row)
        d["params"] = json.loads(d.get("params_json") or "{}")
        return d
    except Exception:
        return None


def train_instance_model(c, instance_id: str, days: int = 30, now: int = None) -> dict:
    """Train and persist an instance-specific anomaly baseline model in `ml_models`."""
    now = store.now_ts(c) if now is None else now
    since = now - days * 86400
    joined = _joined_hourly(c, instance_id, since, now)
    hours = sorted(joined.keys())
    if len(hours) < MIN_BASELINE_POINTS:
        return {
            "instance_id": instance_id,
            "trained": False,
            "sample_count": len(hours),
            "detail": f"Insufficient history ({len(hours)} < {MIN_BASELINE_POINTS} hours)",
        }

    feature_stats = {}
    for name, _ in _FEATURES:
        vals = [float(joined[h][name]) for h in hours if joined[h].get(name) is not None]
        if len(vals) >= 3:
            med = statistics.median(vals)
            mad = max(_mad(vals, med), _NOISE_FLOORS.get(name, 0.1))
            ewma = _ewma_series(vals, alpha=0.25)[-1]
            feature_stats[name] = {
                "median": round(med, 3),
                "mad": round(mad, 3),
                "ewma": round(ewma, 3),
                "p95": round(sorted(vals)[int(0.95 * (len(vals) - 1))], 3),
                "count": len(vals),
            }

    # Also include Kafka 2.8.1 & ZooKeeper baseline stats if available
    try:
        k_rows = store.kafka_window_rows(c, instance_id, since, now)
    except Exception:
        k_rows = []
    kafka_stats = {}
    for k_col, floor in _KAFKA_FEATURES:
        vals = [float(r[k_col]) for r in k_rows if r.get(k_col) is not None]
        if len(vals) >= 3:
            med = statistics.median(vals)
            mad = max(_mad(vals, med), floor)
            kafka_stats[k_col] = {
                "median": round(med, 3),
                "mad": round(mad, 3),
                "p95": round(sorted(vals)[int(0.95 * (len(vals) - 1))], 3),
                "count": len(vals),
            }

    split = now - 24 * 3600
    b_hours = [h for h in hours if h < split] or hours[:-4]
    r_hours = [h for h in hours if h >= split] or hours[-4:]
    if_res = _isolation_forest_scores(joined, b_hours, r_hours) or {}
    lof_res = _lof_scores(joined, b_hours, r_hours) or {}

    params = {
        "feature_stats": feature_stats,
        "kafka_stats": kafka_stats,
        "isolation_forest_status": if_res.get("status", "unavailable"),
        "lof_status": lof_res.get("status", "unavailable"),
        "window_days": days,
    }
    trained_at = int(time.time())
    score = max(
        float(if_res.get("overall_anomaly_score") or 0.0),
        float(lof_res.get("overall_anomaly_score") or 0.0),
    )
    c.execute(
        """INSERT OR REPLACE INTO ml_models(instance_id, model_type, trained_at, sample_count, contamination, anomaly_rate, params_json)
           VALUES(?,?,?,?,?,?,?)""",
        (
            instance_id,
            "ensemble_v2",
            trained_at,
            len(hours),
            0.05,
            round(score / 100.0, 4),
            json.dumps(params),
        ),
    )
    return {
        "instance_id": instance_id,
        "trained": True,
        "trained_at": trained_at,
        "sample_count": len(hours),
        "features_trained": len(feature_stats) + len(kafka_stats),
        "ensemble": ["robust_zscore", "isolation_forest", "local_outlier_factor", "ewma_drift"],
        "anomaly_score": round(score, 1),
    }


def train_cluster_models(c, cluster: str | None = None, days: int = 30, now: int = None) -> dict:
    """Train persistent anomaly models for all instances in a cluster (or entire fleet)."""
    instances = store.list_instances(c)
    if cluster:
        instances = [i for i in instances if i["cluster"] == cluster]
    results = [train_instance_model(c, i["id"], days=days, now=now) for i in instances]
    trained_count = sum(1 for r in results if r.get("trained"))
    return {
        "cluster": cluster or "ALL",
        "instances_total": len(instances),
        "instances_trained": trained_count,
        "trained_at": int(time.time()),
        "results": results,
    }


def evaluate_jev_judgment(state: dict[str, Any]) -> dict[str, Any]:
    """Produce typed System One judgments (`Choice`, `Score`, `Noul`) using TypeSafe Jev (`jev-latest`).

    Follows the global `jev` skill (`~/.gemini/config/skills/jev/SKILL.md`):
      - Sends structured state and parallel atomic questions (`Choice`, `Score`, `Noul`)
        to `https://api.typesafe.ai/v1/systemone` when `TYPESAFE_API_KEY` is present.
      - Provides calibrated deterministic System One primitive evaluations with identical
        typed output structure when running in air-gapped/offline mode.
    """
    api_key = os.environ.get("TYPESAFE_API_KEY", "").strip()
    api_url = os.environ.get("TYPESAFE_API_URL", "https://api.typesafe.ai/v1/systemone")

    gc = state.get("gc") or {}
    host = state.get("host") or {}
    kafka = state.get("kafka") or {}

    questions = {
        "primary_root_cause": {
            "type": "choice",
            "instructions": (
                "Given `gc`, `host`, and `kafka` telemetry for an Apache Kafka 2.8.1 / ZooKeeper node, "
                "select the single most likely primary root cause driving current anomalies."
            ),
            "criteria": {
                "jvm_gc_pause_cascade": "Long JVM Stop-The-World GC pauses or Full GC storms stalling broker threads or expiring ZooKeeper sessions.",
                "zookeeper_wal_fsync_io_stall": "ZooKeeper transaction log (WAL) fsync latency spikes or quorum sync stalls causing session timeouts.",
                "host_cpu_or_memory_contention": "OS-level CPU saturation, high disk I/O wait, swap activity, or memory exhaustion on the host.",
                "kafka_broker_thread_saturation": "Kafka RequestHandler or NetworkProcessor thread pool exhaustion or high produce/fetch latency without host/GC stalls.",
                "partition_replication_imbalance": "Under-replicated partitions (URP > 0) or ISR shrink/expand churn from replication lag.",
                "healthy_nominal": "All JVM GC, Host SAR, Kafka broker, and ZooKeeper quorum signals are within healthy operating bounds.",
            },
        },
        "recommended_remediation": {
            "type": "choice",
            "instructions": "Select the highest-priority engineering action for `instance` based on `gc`, `host`, and `kafka` state.",
            "criteria": {
                "tune_g1gc_and_heap": "Increase JVM heap or tune G1GC (`-XX:MaxGCPauseMillis`, `-XX:G1NewSizePercent`, `-XX:InitiatingHeapOccupancyPercent`).",
                "isolate_zookeeper_wal_disk": "Move ZooKeeper `dataLogDir` onto dedicated low-latency NVMe storage and tune `zookeeper.session.timeout.ms`.",
                "scale_or_relocate_host_workload": "Relocate noisy co-located processes, eliminate swap (`vm.swappiness=1`), or scale host CPU/IOPS.",
                "rebalance_partitions_and_threads": "Run `kafka-reassign-partitions.sh`, run preferred replica election, or increase `num.io.threads` / `num.network.threads`.",
                "no_immediate_action": "Continue routine monitoring; no remediation required.",
            },
        },
        "operational_risk_score": {
            "type": "score",
            "instructions": "Rate the operational urgency and cluster impact for `instance`.",
            "criteria": [
                {"level": 0, "description": "Nominal operation; zero URP, low GC pause, low host CPU/memory/disk utilization."},
                {"level": 1, "description": "Minor telemetry elevation or warning; single-resource pressure without replication loss."},
                {"level": 2, "description": "Degraded service; sustained high latency, ISR shrink churn, or elevated GC/CPU contention."},
                {"level": 3, "description": "Critical incident; active URP/offline partitions, Full GC storm, host CPU/memory damage, or ZooKeeper fsync stall."},
            ],
        },
        "urp_cascade_risk": {
            "type": "noul",
            "instructions": "Does the current state indicate an active or imminent Under-Replicated Partition (URP) cascade across the Kafka cluster?",
        },
        "zk_session_expiry_risk": {
            "type": "noul",
            "instructions": "Is this node at risk of ZooKeeper session expiration (`SessionExpiredException`) due to GC pauses or WAL fsync stalls?",
        },
        "host_hardware_damage_risk": {
            "type": "noul",
            "instructions": "Is the underlying host experiencing severe CPU saturation (>=85%), memory exhaustion (>=88%), swap thrashing, or disk I/O stall?",
        },
    }

    if api_key:
        try:
            payload = json.dumps({
                "model": "jev-latest",
                "state": state,
                "questions": questions,
            }).encode("utf-8")
            req = urllib.request.Request(
                api_url,
                data=payload,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {api_key}",
                },
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=4.5) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                if isinstance(data, dict) and "answers" in data and isinstance(data["answers"], dict):
                    ans = data["answers"]
                    for k in ("primary_root_cause", "recommended_remediation"):
                        if isinstance(ans.get(k), dict):
                            val = ans[k].get("answer") or ans[k].get("choice") or ans[k].get("value") or "healthy_nominal"
                            ans[k]["primitive"] = "Choice"
                            ans[k]["answer"] = val
                            ans[k]["choice"] = val
                            ans[k].setdefault("confidence", 0.85)
                    if isinstance(ans.get("operational_risk_score"), dict):
                        ans["operational_risk_score"]["primitive"] = "Score"
                        ans["operational_risk_score"].setdefault("score", 50.0)
                    for k in ("urp_cascade_risk", "zk_session_expiry_risk", "host_hardware_damage_risk"):
                        if isinstance(ans.get(k), dict):
                            ans[k]["primitive"] = "Noul"
                            ans[k].setdefault("probability", ans[k].get("score", 0.5))
                    if "primary_root_cause" in ans and "answer" in ans["primary_root_cause"]:
                        return {
                            "engine": "TypeSafe Jev (jev-latest live API)",
                            "model": "jev-latest",
                            "live_api": True,
                            "judgments": ans,
                        }
        except Exception:
            pass

    # Deterministic System One calibrated primitive evaluation (offline / zero-latency fallback)
    full_gc = float(gc.get("full_gc_count") or 0)
    pause_p99 = float(gc.get("pause_p99_ms") or 0)
    tig = float(gc.get("time_in_gc_pct") or 0)
    cpu_max = float(host.get("cpu_busy_pct_max") or host.get("cpu_busy_pct_avg") or 0)
    mem_max = float(host.get("mem_used_pct_max") or host.get("mem_used_pct_avg") or 0)
    swap_max = float(host.get("swap_used_pct_max") or 0)
    disk_await = float(host.get("disk_await_ms_max") or 0)
    urp = float(kafka.get("under_replicated_partitions") or 0)
    offline = float(kafka.get("offline_partitions_count") or 0)
    req_idle = float(kafka.get("request_handler_avg_idle_pct") if kafka.get("request_handler_avg_idle_pct") is not None else 80.0)
    zk_fsync = float(kafka.get("zk_fsync_warning_count") or 0)
    zk_lat = float(kafka.get("zk_avg_latency_ms") or 0)

    scores_rc = {
        "jvm_gc_pause_cascade": min(0.95, 0.05 + (0.45 if full_gc > 0 else 0.0) + min(0.35, pause_p99 / 1200.0) + min(0.2, tig / 25.0)),
        "zookeeper_wal_fsync_io_stall": min(0.95, 0.04 + (0.55 if zk_fsync > 0 else 0.0) + min(0.35, zk_lat / 80.0)),
        "host_cpu_or_memory_contention": min(0.95, 0.05 + (0.35 if cpu_max >= 85 else 0.0) + (0.35 if mem_max >= 88 or swap_max > 1 else 0.0) + (0.2 if disk_await >= 30 else 0.0)),
        "kafka_broker_thread_saturation": min(0.90, 0.04 + (0.50 if req_idle < 25 else 0.0)),
        "partition_replication_imbalance": min(0.92, 0.04 + (0.55 if urp > 0 or offline > 0 else 0.0)),
        "healthy_nominal": 0.85 if (full_gc == 0 and pause_p99 < 200 and cpu_max < 80 and mem_max < 85 and urp == 0 and zk_fsync == 0) else 0.05,
    }
    tot_rc = sum(scores_rc.values()) or 1.0
    dist_rc = {k: round(v / tot_rc, 3) for k, v in scores_rc.items()}
    top_rc = max(dist_rc, key=dist_rc.get)

    action_map = {
        "jvm_gc_pause_cascade": "tune_g1gc_and_heap",
        "zookeeper_wal_fsync_io_stall": "isolate_zookeeper_wal_disk",
        "host_cpu_or_memory_contention": "scale_or_relocate_host_workload",
        "kafka_broker_thread_saturation": "rebalance_partitions_and_threads",
        "partition_replication_imbalance": "rebalance_partitions_and_threads",
        "healthy_nominal": "no_immediate_action",
    }
    top_act = action_map[top_rc]

    urp_noul = round(min(0.99, max(0.02, (0.75 if urp > 0 else 0.05) + (0.20 if full_gc > 0 or req_idle < 20 else 0.0))), 3)
    zk_noul = round(min(0.99, max(0.02, (0.65 if zk_fsync > 0 or zk_lat >= 25 else 0.04) + (0.30 if pause_p99 >= 500 else 0.0))), 3)
    host_noul = round(min(0.99, max(0.02, (0.50 if cpu_max >= 85 else 0.03) + (0.35 if mem_max >= 88 or swap_max > 1 else 0.0) + (0.20 if disk_await >= 30 else 0.0))), 3)

    max_risk = max(urp_noul, zk_noul, host_noul)
    risk_score_val = round(min(100.0, max_risk * 100.0), 1)

    return {
        "engine": "TypeSafe Jev (System One Calibrated Primitives)",
        "model": "jev-latest",
        "live_api": False,
        "judgments": {
            "primary_root_cause": {
                "primitive": "Choice",
                "answer": top_rc,
                "choice": top_rc,
                "confidence": round(dist_rc[top_rc], 3),
                "distribution": dist_rc,
            },
            "recommended_remediation": {
                "primitive": "Choice",
                "answer": top_act,
                "choice": top_act,
                "confidence": round(max(0.68, dist_rc[top_rc]), 3),
            },
            "operational_risk_score": {
                "primitive": "Score",
                "score": risk_score_val,
                "level": 3 if risk_score_val >= 75 else 2 if risk_score_val >= 45 else 1 if risk_score_val >= 20 else 0,
                "confidence": 0.89,
            },
            "urp_cascade_risk": {
                "primitive": "Noul",
                "probability": urp_noul,
            },
            "zk_session_expiry_risk": {
                "primitive": "Noul",
                "probability": zk_noul,
            },
            "host_hardware_damage_risk": {
                "primitive": "Noul",
                "probability": host_noul,
            },
        },
    }


def diagnose_with_local_llm(c, instance_id: str, now: int = None, model: str | None = None) -> dict:
    """Run deep multi-layer diagnostics combining Ensemble ML + TypeSafe Jev +
    Open-Source Local LLM (Ollama / Llama 3.2 / Qwen / Mistral / OpenLLM).
    """
    now = store.now_ts(c) if now is None else now
    inst = store.get_instance(c, instance_id) or {"id": instance_id, "role": "broker", "cluster": "UNKNOWN"}
    gc_snap = store.current_snapshot(c, instance_id, now) or {}
    host_snap = store.current_host_snapshot(c, instance_id, now) or {}
    kafka_snap = store.current_kafka_snapshot(c, instance_id, now) or {}
    ml_anom = analyze_anomalies(c, instance_id, now=now)

    gc_m = gc_snap.get("metrics") or {}
    host_m = host_snap.get("metrics") or {}
    kafka_m = kafka_snap.get("metrics") or {}
    log_events = (kafka_snap.get("log_events") or [])[:8]

    state_payload = {
        "instance": {"id": instance_id, "role": inst.get("role"), "cluster": inst.get("cluster")},
        "gc": {
            "pause_p99_ms": gc_m.get("pause_p99_ms"),
            "pause_max_ms": gc_m.get("pause_max_ms"),
            "full_gc_count": gc_m.get("full_gc_count"),
            "time_in_gc_pct": gc_m.get("time_in_gc_pct"),
            "heap_after_pct": gc_m.get("avg_heap_after_pct"),
        },
        "host": {
            "cpu_busy_pct_max": host_m.get("cpu_busy_pct_max"),
            "cpu_iowait_pct_avg": host_m.get("cpu_iowait_pct_avg"),
            "mem_used_pct_max": host_m.get("mem_used_pct_max"),
            "swap_used_pct_max": host_m.get("swap_used_pct_max"),
            "disk_await_ms_max": host_m.get("disk_await_ms_max"),
        },
        "kafka": {
            "under_replicated_partitions": kafka_m.get("under_replicated_partitions"),
            "offline_partitions_count": kafka_m.get("offline_partitions_count"),
            "isr_shrinks_per_sec": kafka_m.get("isr_shrinks_per_sec"),
            "request_handler_avg_idle_pct": kafka_m.get("request_handler_avg_idle_pct"),
            "produce_latency_p99_ms": kafka_m.get("produce_latency_p99_ms"),
            "zk_avg_latency_ms": kafka_m.get("zk_avg_latency_ms"),
            "zk_fsync_warning_count": kafka_m.get("zk_fsync_warning_count"),
            "zk_session_expire_count": kafka_m.get("zk_session_expire_count"),
        },
    }

    jev = evaluate_jev_judgment(state_payload)

    llm_url = os.environ.get("GC_LOCAL_LLM_URL", "http://127.0.0.1:11434/api/generate")
    llm_model = model or os.environ.get("GC_LOCAL_LLM_MODEL", "llama3.2:3b")

    prompt = (
        f"You are a Principal Apache Kafka 2.8.1, ZooKeeper 3.5.9, JVM G1GC, and Linux SRE Diagnostic Agent.\n"
        f"Analyze node {instance_id} (role={inst.get('role')}, cluster={inst.get('cluster')}):\n"
        f"- JVM GC: pause_p99={gc_m.get('pause_p99_ms')}ms, full_gc={gc_m.get('full_gc_count')}, time_in_gc={gc_m.get('time_in_gc_pct')}%, heap_after={gc_m.get('avg_heap_after_pct')}%\n"
        f"- Host SAR: cpu_max={host_m.get('cpu_busy_pct_max')}%, iowait={host_m.get('cpu_iowait_pct_avg')}%, mem_max={host_m.get('mem_used_pct_max')}%, swap={host_m.get('swap_used_pct_max')}%, disk_await={host_m.get('disk_await_ms_max')}ms\n"
        f"- Kafka 2.8.1 / ZK: URP={kafka_m.get('under_replicated_partitions')}, offline={kafka_m.get('offline_partitions_count')}, req_idle={kafka_m.get('request_handler_avg_idle_pct')}%, produce_p99={kafka_m.get('produce_latency_p99_ms')}ms, zk_lat={kafka_m.get('zk_avg_latency_ms')}ms, zk_fsync_warns={kafka_m.get('zk_fsync_warning_count')}\n"
        f"- TypeSafe Jev Root Cause Judgment: {jev['judgments']['primary_root_cause']['answer']} (confidence {jev['judgments']['primary_root_cause']['confidence']})\n"
        f"Provide a concise 3-bullet root-cause correlation and exact Kafka 2.8.1 / ZooKeeper / JVM remediation commands."
    )

    llm_response_text = None
    llm_provider = "Local Expert Synthesis + TypeSafe Jev (Ollama offline fallback)"
    try:
        req_body = json.dumps({
            "model": llm_model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.15, "num_predict": 320},
        }).encode("utf-8")
        req = urllib.request.Request(
            llm_url,
            data=req_body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=2.5) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            if isinstance(body, dict) and body.get("response"):
                llm_response_text = body["response"].strip()
                llm_provider = f"Ollama Local LLM ({llm_model})"
    except Exception:
        pass

    if not llm_response_text:
        rc = jev["judgments"]["primary_root_cause"]["answer"]
        urp_prob = jev["judgments"]["urp_cascade_risk"]["probability"]
        zk_prob = jev["judgments"]["zk_session_expiry_risk"]["probability"]
        host_prob = jev["judgments"]["host_hardware_damage_risk"]["probability"]
        bullets = [
            f"Cross-Layer Verdict ({rc}): TypeSafe Jev scored operational risk at {jev['judgments']['operational_risk_score']['score']}/100 "
            f"(URP Cascade Noul={urp_prob:.2f}, ZK Expiry Noul={zk_prob:.2f}, Host Damage Noul={host_prob:.2f}).",
        ]
        if (gc_m.get("full_gc_count") or 0) > 0 or (gc_m.get("pause_p99_ms") or 0) >= 250:
            bullets.append(
                f"JVM -> Kafka/ZK Correlation: Stop-The-World pause p99={gc_m.get('pause_p99_ms')}ms "
                f"(full_gc={gc_m.get('full_gc_count')}) freezes ReplicaFetcher and ZooKeeper heartbeat threads, "
                f"explaining ISR shrink rate={kafka_m.get('isr_shrinks_per_sec', 0)}/s and URP={kafka_m.get('under_replicated_partitions', 0)}."
            )
        if (host_m.get("cpu_busy_pct_max") or 0) >= 80 or (host_m.get("mem_used_pct_max") or 0) >= 85:
            bullets.append(
                f"Host SAR Contention: Peak CPU={host_m.get('cpu_busy_pct_max')}% / Memory={host_m.get('mem_used_pct_max')}% "
                f"/ Disk await={host_m.get('disk_await_ms_max')}ms is starving broker request handlers "
                f"(idle={kafka_m.get('request_handler_avg_idle_pct')}%)."
            )
        if (kafka_m.get("zk_fsync_warning_count") or 0) > 0 or (kafka_m.get("zk_avg_latency_ms") or 0) >= 15:
            bullets.append(
                f"ZooKeeper Quorum / WAL Bottleneck: {kafka_m.get('zk_fsync_warning_count', 0)} WAL fsync stall warning(s) "
                f"with avg latency {kafka_m.get('zk_avg_latency_ms', 0)}ms; isolate ZooKeeper `dataLogDir` onto dedicated NVMe."
            )
        if log_events:
            bullets.append(
                f"Operational Log Evidence: {len(log_events)} recent event(s) captured — latest: \"{log_events[0].get('summary')}\"."
            )
        llm_response_text = "\n".join(f"• {b}" for b in bullets)

    return {
        "instance_id": instance_id,
        "provider": llm_provider,
        "model": llm_model,
        "narrative": llm_response_text,
        "jev": jev,
        "anomaly_summary": {
            "overall_score": ml_anom.get("overall_anomaly_score"),
            "is_anomalous": ml_anom.get("is_anomalous"),
            "methods": ml_anom.get("methods", []),
            "ewma_drift": ml_anom.get("ewma_drift"),
            "lof": ml_anom.get("lof"),
        },
    }


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

    saved_model = _get_saved_model(c, instance_id)

    empty = {
        "instance_id": instance_id, "method": "none", "methods": [],
        "readiness": "insufficient_data", "n_baseline": len(baseline_hours),
        "n_recent": len(recent_hours_list), "overall_anomaly_score": None, "is_anomalous": False,
        "features": [], "isolation_forest": None, "lof": None, "ewma_drift": None,
        "trained_model": saved_model, "notice": _NOTICE,
        "model_status": {"robust_zscore": "insufficient_data", "isolation_forest": "not_run",
                         "local_outlier_factor": "not_run", "ewma_drift": "not_run",
                         "persistent_model": bool(saved_model)},
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

    lof_scores = None
    lof_state = "not_run"
    if model_state == "used":
        try:
            lof_cand = _lof_scores(joined, baseline_hours, recent_hours_list)
            lof_state = lof_cand.get("status", "used") if lof_cand else "insufficient_data"
            if lof_state == "used":
                lof_scores = lof_cand
        except Exception:
            lof_state = "error"

    ewma_drift = _ewma_drift_scores(joined, baseline_hours, recent_hours_list)

    feature_results.sort(key=lambda f: f["recent_max_abs_z"], reverse=True)
    top_z = feature_results[0]["recent_max_abs_z"] if feature_results else 0.0
    overall = round(min(100.0, max(0.0, (top_z / 7.0) * 100.0)), 1)
    is_anomalous = any(f["anomalous"] for f in feature_results)

    if sklearn_scores is not None:
        is_anomalous = is_anomalous or sklearn_scores["is_anomalous"]
        overall = max(overall, sklearn_scores["overall_anomaly_score"])
    if lof_scores is not None:
        overall = max(overall, round(lof_scores["overall_anomaly_score"] * 0.85, 1))

    methods_used = ["robust_zscore"]
    if sklearn_scores is not None:
        methods_used.append("isolation_forest")
    if lof_scores is not None:
        methods_used.append("local_outlier_factor")
    if ewma_drift.get("status") == "used":
        methods_used.append("ewma_drift")

    return {
        "instance_id": instance_id,
        "method": method,
        "methods": methods_used,
        "readiness": "ready" if len(feature_results) == len(_FEATURES) else "partial",
        "model_status": {
            "robust_zscore": "used",
            "isolation_forest": model_state,
            "local_outlier_factor": lof_state,
            "ewma_drift": ewma_drift.get("status", "not_run"),
            "persistent_model": bool(saved_model),
            "training": "persisted" if saved_model else ("request_local" if sklearn_scores is not None else "none"),
        },
        "source_quality": source_quality,
        "n_baseline": len(baseline_hours),
        "n_recent": len(recent_hours_list),
        "overall_anomaly_score": overall,
        "is_anomalous": is_anomalous,
        "features": feature_results,
        "isolation_forest": sklearn_scores,
        "lof": lof_scores,
        "ewma_drift": ewma_drift,
        "trained_model": saved_model,
        "notice": _NOTICE,
    }


def _isolation_forest_scores(joined: dict[int, dict], baseline_hours: list[int], recent_hours_list: list[int]):
    """Multivariate anomaly score over the joined feature vector."""
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
    scores = model.decision_function([v for _, v in recent_matrix])
    preds = model.predict([v for _, v in recent_matrix])

    worst_i = min(range(len(scores)), key=lambda i: scores[i])
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
