"""Advisory safeguards tested with deterministic, collection-free evidence."""

import pytest

from gcanalyzer import correlate, forecast, ml_insights, scaling_advisor, store

DAY = 86400
HOUR = 3600
NOW = 2_000_000_000 // DAY * DAY
IID = "T--broker-1"


def gc_row(ts, **changes):
    row = dict(ts=ts, time_in_gc_pct=1.0, pause_p99_ms=60.0,
               pause_max_ms=90.0, full_gc_count=0, throughput_pct=99.0,
               heap_after_pct=30.0)
    return row | changes


def host_row(ts, **changes):
    row = dict(ts=ts, cpu_busy_pct=15.0, cpu_iowait_pct=1.0,
               mem_used_pct=40.0, swap_used_pct=0.0,
               disk_util_pct_max=10.0, disk_await_ms_max=3.0,
               net_util_pct_max=10.0)
    return row | changes


def history(monkeypatch, gc, host):
    monkeypatch.setattr(store, "window_rows", lambda *a: gc)
    monkeypatch.setattr(store, "host_window_rows", lambda *a: host)
    monkeypatch.setattr(store, "current_snapshot", lambda *a: {})
    monkeypatch.setattr(store, "current_host_snapshot", lambda *a: {})


def test_calm_constant_signals_do_not_imply_gc_bottleneck(monkeypatch):
    history(monkeypatch, [gc_row(NOW - h * HOUR) for h in range(12)],
            [host_row(NOW - h * HOUR) for h in range(12)])
    result = correlate.correlate_instance(None, IID, now=NOW)
    assert result["verdict"] == "no_pressure"
    assert "No significant pressure" in " ".join(result["findings"])


def test_low_amplitude_association_does_not_imply_bottleneck(monkeypatch):
    history(monkeypatch, [gc_row(NOW - h * HOUR, time_in_gc_pct=1 + h / 100) for h in range(12)],
            [host_row(NOW - h * HOUR, cpu_busy_pct=15 + h / 10) for h in range(12)])
    result = correlate.correlate_instance(None, IID, now=NOW)
    assert result["correlations"][0]["r"] == 1
    assert result["verdict"] == "no_pressure"


def test_hourly_aggregation_keeps_null_and_ignores_invalid_values():
    rows = [{"ts": NOW, "cpu": None}, {"ts": NOW + 10, "cpu": float("nan")},
            {"ts": None, "cpu": 9}]
    assert correlate.rebucket_hourly(rows, ["cpu"])[NOW]["cpu"] is None


def test_correlation_pairs_use_only_jointly_observed_values(monkeypatch):
    history(monkeypatch, [gc_row(NOW - h * HOUR, time_in_gc_pct=1 + h) for h in range(12)],
            [host_row(NOW - h * HOUR, cpu_busy_pct=None if h < 5 else 20 + h,
                      mem_used_pct=None) for h in range(12)])
    result = correlate.correlate_instance(None, IID, now=NOW)
    cpu = next(c for c in result["correlations"] if c["gc_metric"] == "time_in_gc_pct"
               and c["host_metric"] == "cpu_busy_pct")
    assert cpu["n"] == 7
    assert result["storm_cooccurrence"]["pct_with_mem_pressure"] is None
    assert result["verdict"] == "insufficient_data"


def test_all_missing_gc_values_are_not_healthy(monkeypatch):
    history(monkeypatch, [{"ts": NOW - h * HOUR} for h in range(12)],
            [host_row(NOW - h * HOUR) for h in range(12)])
    assert correlate.correlate_instance(None, IID, now=NOW)["verdict"] == "insufficient_data"


def test_swap_occupancy_association_does_not_claim_active_paging_or_cause(monkeypatch):
    history(monkeypatch, [gc_row(NOW - h * HOUR, time_in_gc_pct=20) for h in range(12)],
            [host_row(NOW - h * HOUR, swap_used_pct=50) for h in range(12)])
    result = correlate.correlate_instance(None, IID, now=NOW)
    text = " ".join(result["findings"]).lower()
    assert "swap usage" in text
    assert "not a confirmed cause" in text
    assert "swapped jvm pages are" not in text


def snapshots(monkeypatch, *, state="fresh", host=True, pressure=False):
    instances = [{"id": IID, "cluster": "T", "role": "broker"}]
    gc = {"metrics": {"full_count": 0, "avg_heap_after_pct": 30, "pct_time_in_gc": 1,
                      "p99_pause_ms": 60, "max_pause_ms": 90},
          "quality": {"state": state, "last_observed_at": NOW, "age_seconds": 0},
          "health": {"grade": "A", "status": "healthy"}, "latest": {"ts": NOW}}
    h = {"metrics": {"sample_count": 12, "cpu_busy_pct_avg": 95 if pressure else 15,
                     "cpu_iowait_pct_avg": 1, "mem_used_pct_max": 40,
                     "swap_used_pct_max": 0, "disk_util_pct_max": 10,
                     "disk_await_ms_max": 3, "net_util_pct_max": 10, "net_tx_kbs_avg": 20},
         "quality": {"state": state, "last_observed_at": NOW, "age_seconds": 0},
         "health": {"grade": "A", "status": "healthy"}, "latest": {"ts": NOW}}
    monkeypatch.setattr(store, "list_instances", lambda *a: instances)
    monkeypatch.setattr(store, "current_snapshot", lambda *a: gc)
    monkeypatch.setattr(store, "current_host_snapshot", lambda *a: h if host else None)
    return gc, h


@pytest.mark.parametrize("state", ["stale", "partial", "missing", "unknown"])
def test_scaling_withholds_verdict_for_unreliable_sources(monkeypatch, state):
    snapshots(monkeypatch, state=state)
    result = scaling_advisor.analyze_cluster_scaling(None, "T", now=NOW)
    assert result["verdict"] == "insufficient_data"
    assert result["confidence"] == "low"
    assert result["nodes"][0]["source_quality"]["host"]["state"] == state


def test_gc_only_scaling_cannot_claim_both_sources_healthy(monkeypatch):
    snapshots(monkeypatch, host=False)
    result = scaling_advisor.analyze_cluster_scaling(None, "T", now=NOW)
    assert result["verdict"] == "insufficient_data"
    assert "healthy on both" not in " ".join(result["evidence"])


def test_null_metrics_cannot_imply_headroom(monkeypatch):
    _, host = snapshots(monkeypatch)
    host["metrics"]["disk_util_pct_max"] = None
    result = scaling_advisor.analyze_cluster_scaling(None, "T", now=NOW)
    assert result["verdict"] == "insufficient_data"


def test_absent_quality_and_old_latest_cannot_be_current_scaling(monkeypatch):
    gc, host = snapshots(monkeypatch, pressure=True)
    for snap in (gc, host):
        snap.pop("quality")
        snap["latest"]["ts"] = NOW - 365 * DAY
    result = scaling_advisor.analyze_cluster_scaling(None, "T", now=NOW)
    assert result["verdict"] == "insufficient_data"


def test_host_pressure_without_kafka_evidence_is_investigation(monkeypatch):
    snapshots(monkeypatch, pressure=True)
    result = scaling_advisor.analyze_cluster_scaling(None, "T", now=NOW)
    assert result["verdict"] == "investigate"
    assert result["confidence"] != "high"
    assert "Kafka" in " ".join(result["evidence"])


def forecast_history(monkeypatch, offsets, values=None):
    values = values or [40 + n / 2 for n in range(len(offsets))]
    history(monkeypatch, [], [host_row(NOW - d * DAY, cpu_busy_pct=v)
                             for d, v in zip(offsets, values)])


@pytest.mark.parametrize("offsets", [list(range(6, -1, -1)), list(range(84, -1, -6)),
                                    list(range(60, 29, -1))])
def test_forecast_withholds_short_sparse_and_stale_history(monkeypatch, offsets):
    forecast_history(monkeypatch, offsets)
    result = forecast.forecast_instance(None, IID, now=NOW)
    cpu = next(s for s in result["signals"] if s["signal"] == "cpu_busy_pct")
    assert cpu["status"] == "insufficient_data"
    assert cpu["risk"] == "no_data"
    assert cpu["days_to_crit"] is None
    assert cpu["crit_date"] is None
    assert cpu["withheld_reason"]
    assert result["risk"] == "no_data"


def test_one_weekly_cycle_has_no_prediction_date(monkeypatch):
    forecast_history(monkeypatch, list(range(6, -1, -1)), [40, 45, 55, 65, 75, 65, 55])
    cpu = forecast.forecast_instance(None, IID, now=NOW)["signals"][0]
    assert cpu["days_to_crit"] is None


def test_forecast_honors_explicit_stale_source_quality(monkeypatch):
    forecast_history(monkeypatch, list(range(29, -1, -1)))
    monkeypatch.setattr(store, "current_host_snapshot", lambda *a: {"quality": {"state": "stale"}})
    result = forecast.forecast_instance(None, IID, now=NOW)
    assert all(s["days_to_crit"] is None for s in result["signals"])
    assert result["risk"] == "no_data"


def test_fresh_dense_forecast_reports_heuristic_method_and_coverage(monkeypatch):
    forecast_history(monkeypatch, list(range(29, -1, -1)))
    cpu = next(s for s in forecast.forecast_instance(None, IID, now=NOW)["signals"]
               if s["signal"] == "cpu_busy_pct")
    assert cpu["slope_per_day"] == 0.5
    assert cpu["days_to_crit"] is not None
    assert cpu["method"] == "theil_sen"
    assert cpu["validated"] is False
    assert cpu["confidence"] != "high"
    assert cpu["coverage_ratio"] == 1.0


def test_sparse_cluster_forecast_is_not_no_action(monkeypatch):
    forecast_history(monkeypatch, [2, 1, 0])
    monkeypatch.setattr(store, "list_instances", lambda *a: [{"id": IID, "cluster": "T", "role": "broker"}])
    result = forecast.forecast_cluster(None, "T", now=NOW)
    assert result["verdict"] == "insufficient_data"
    assert result["confidence"] == "low"


def test_recent_gap_is_not_hidden_by_long_dense_history(monkeypatch):
    forecast_history(monkeypatch, list(range(60, 7, -1)) + [0])
    cpu = next(s for s in forecast.forecast_instance(None, IID, now=NOW)["signals"]
               if s["signal"] == "cpu_busy_pct")
    assert cpu["status"] == "insufficient_data"
    assert cpu["days_to_crit"] is None


def test_weak_seasonal_trend_does_not_publish_precise_dates(monkeypatch):
    values = [v + i * 0.1 for i, v in enumerate([40, 45, 55, 65, 75, 65, 55] * 4)]
    forecast_history(monkeypatch, list(range(27, -1, -1)), values)
    cpu = next(s for s in forecast.forecast_instance(None, IID, now=NOW)["signals"]
               if s["signal"] == "cpu_busy_pct")
    assert cpu["confidence"] == "low"
    assert cpu["days_to_crit"] is None
    assert cpu["crit_date"] is None


def test_flat_mad_noise_does_not_become_maximal_anomaly():
    assert abs(ml_insights._modified_zscores([0] * 20, [0.001])[0]) < 1


def test_anomaly_readiness_requires_actual_feature_baseline(monkeypatch):
    history(monkeypatch, [{"ts": NOW - h * HOUR} for h in range(50)], [])
    result = ml_insights.analyze_anomalies(None, IID, now=NOW)
    assert result["method"] == "none"
    assert result["readiness"] == "insufficient_data"
    assert result["overall_anomaly_score"] is None


def test_anomaly_fallback_is_explicit_and_metric_noise_is_small(monkeypatch):
    history(monkeypatch, [], [host_row(NOW - h * HOUR, cpu_busy_pct=0 if h > 24 else 0.001)
                             for h in range(60)])

    def unavailable(*args):
        raise ImportError("optional model unavailable")

    monkeypatch.setattr(ml_insights, "_isolation_forest_scores", unavailable)
    result = ml_insights.analyze_anomalies(None, IID, now=NOW)
    assert result["method"] == "robust_zscore"
    assert result["is_anomalous"] is False
    assert result["overall_anomaly_score"] < 1
    assert result["readiness"] == "partial"
    assert result["model_status"]["isolation_forest"] == "unavailable"
    assert result["features"][0]["direction"] in ("up", "down", "unchanged")
    assert "not a probability" in result["notice"]


def test_anomaly_does_not_score_explicitly_stale_source(monkeypatch):
    history(monkeypatch, [], [host_row(NOW - h * HOUR) for h in range(60)])
    monkeypatch.setattr(store, "current_host_snapshot", lambda *a: {"quality": {"state": "stale"}})
    result = ml_insights.analyze_anomalies(None, IID, now=NOW)
    assert result["method"] == "none"
    assert result["overall_anomaly_score"] is None
