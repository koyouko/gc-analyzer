"""
Verification tests for correlate.py, scaling_advisor.py, and ml_insights.py.

Run:  python -m tests.test_correlation_scaling      (from the project root)
or:   pytest -q                                       (pytest optional)

Each test builds a small synthetic instance/cluster directly against a temp
SQLite store (not the demo seed data) so the scenarios are deterministic and
exercise one decision path each.
"""

import math
import os
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from gcanalyzer import store, correlate, scaling_advisor, ml_insights  # noqa: E402

HOUR = 3600
NOW = 1_800_000_000  # arbitrary fixed epoch so tests are deterministic


def _tmp_db(name):
    path = os.path.join(tempfile.gettempdir(), f"{name}_{os.getpid()}.db")
    if os.path.exists(path):
        os.remove(path)
    store.init_db(path)
    return path


def _add_instance(c, iid, cluster, role="broker", heap_mb=4096):
    c.execute(
        "INSERT OR REPLACE INTO instances(id,region,env,cluster,grp,role,idx,heap_max_mb,collector,node_id)"
        " VALUES(?,?,?,?,?,?,?,?,?,?)",
        (iid, "TEST", "DEV", cluster, "brokers", role, 1, heap_mb, "G1", iid),
    )


def _gc_row(**overrides):
    base = {
        "heap_used_mb": 1000.0, "heap_max_mb": 4096.0, "heap_after_pct": 30.0,
        "pause_avg_ms": 20.0, "pause_p99_ms": 60.0, "pause_max_ms": 90.0,
        "full_gc_count": 0, "young_count": 10, "gc_per_min": 5.0,
        "time_in_gc_pct": 1.0, "throughput_pct": 99.0,
    }
    base.update(overrides)
    return base


def _host_row(**overrides):
    base = {
        "cpu_user_pct_avg": 10.0, "cpu_system_pct_avg": 3.0, "cpu_iowait_pct_avg": 1.0,
        "cpu_busy_pct_avg": 15.0, "cpu_busy_pct_max": 20.0, "cpu_iowait_pct_max": 2.0,
        "load1_avg": 1.0, "load5_avg": 1.0, "runq_sz_avg": 0.0, "cswch_per_s_avg": 400.0,
        "mem_used_pct_avg": 40.0, "mem_used_pct_max": 45.0, "mem_cached_mb_avg": 1000.0,
        "swap_used_pct_avg": 0.0, "swap_used_pct_max": 0.0,
        "disk_util_pct_max": 10.0, "disk_await_ms_max": 3.0, "disk_tps_avg": 50.0,
        "net_util_pct_max": 10.0, "net_rx_kbs_avg": 2000.0, "net_tx_kbs_avg": 3000.0,
        "net_tx_kbs_max": 3500.0, "top_disks": [], "top_nics": [],
    }
    base.update(overrides)
    return base


# --------------------------------------------------------------------------- #
# correlate.py
# --------------------------------------------------------------------------- #
def test_correlate_insufficient_data_with_no_history():
    db = _tmp_db("corr_insufficient")
    with store.connect(db) as c:
        _add_instance(c, "T--broker-1", "T")
        result = correlate.correlate_instance(c, "T--broker-1", days=30, now=NOW)
    assert result["verdict"] == "insufficient_data"
    assert result["n_points"] == 0


def test_correlate_detects_host_bound_pattern():
    """GC time-in-GC and host CPU busy rise and fall together on the same
    schedule -> should be flagged as a strong correlation and host_bound."""
    db = _tmp_db("corr_host_bound")
    with store.connect(db) as c:
        _add_instance(c, "T--broker-1", "T")
        for h in range(72):
            ts = NOW - (72 - h) * HOUR
            pressured = (h % 6) >= 4  # periodic pressure window
            tig = 18.0 if pressured else 1.0
            cpu = 92.0 if pressured else 12.0
            store.record_metric(c, "T--broker-1", ts, _gc_row(time_in_gc_pct=tig, full_gc_count=1 if pressured else 0))
            store.record_host_metric(c, "T--broker-1", ts, _host_row(cpu_busy_pct_avg=cpu))
        result = correlate.correlate_instance(c, "T--broker-1", days=10, now=NOW)
    assert result["verdict"] == "host_bound", result
    assert result["n_points"] >= 6
    top = result["correlations"][0]
    assert top["r"] > 0.5, top
    assert result["storm_cooccurrence"]["pct_with_cpu_pressure"] >= 60


def test_correlate_gc_bound_when_host_stays_calm():
    db = _tmp_db("corr_gc_bound")
    with store.connect(db) as c:
        _add_instance(c, "T--broker-1", "T")
        for h in range(72):
            ts = NOW - (72 - h) * HOUR
            pressured = (h % 6) >= 4
            tig = 18.0 if pressured else 1.0
            store.record_metric(c, "T--broker-1", ts, _gc_row(time_in_gc_pct=tig, avg_heap_after_pct=88.0 if pressured else 30.0))
            store.record_host_metric(c, "T--broker-1", ts, _host_row())  # always calm
        result = correlate.correlate_instance(c, "T--broker-1", days=10, now=NOW)
    assert result["verdict"] == "gc_bound", result


# --------------------------------------------------------------------------- #
# scaling_advisor.py
# --------------------------------------------------------------------------- #
def _seed_cluster_24h(c, cluster, node_profiles):
    """node_profiles: {instance_id: (gc_overrides, host_overrides)}"""
    for iid, (gc_over, host_over) in node_profiles.items():
        _add_instance(c, iid, cluster, role="broker")
        for h in range(24):
            ts = NOW - (24 - h) * HOUR
            store.record_metric(c, iid, ts, _gc_row(**gc_over))
            store.record_host_metric(c, iid, ts, _host_row(**host_over))


def test_scaling_advisor_no_action_when_all_healthy():
    db = _tmp_db("scale_no_action")
    with store.connect(db) as c:
        _seed_cluster_24h(c, "T", {
            "T--broker-1": ({}, {}), "T--broker-2": ({}, {}), "T--broker-3": ({}, {}),
        })
        result = scaling_advisor.analyze_cluster_scaling(c, "T", role="broker", now=NOW)
    assert result["verdict"] == "no_action", result


def test_scaling_advisor_investigates_uniform_host_pressure():
    db = _tmp_db("scale_horizontal")
    with store.connect(db) as c:
        hot = {"cpu_busy_pct_avg": 94.0, "cpu_busy_pct_max": 97.0}
        _seed_cluster_24h(c, "T", {
            "T--broker-1": ({}, hot), "T--broker-2": ({}, hot), "T--broker-3": ({}, hot),
        })
        result = scaling_advisor.analyze_cluster_scaling(c, "T", role="broker", now=NOW)
    assert result["verdict"] == "investigate", result
    assert result["confidence"] in ("low", "medium")


def test_scaling_advisor_checks_distribution_when_one_node_is_hot():
    db = _tmp_db("scale_rebalance")
    with store.connect(db) as c:
        hot = {"cpu_busy_pct_avg": 95.0, "net_tx_kbs_avg": 40000.0}
        calm = {"cpu_busy_pct_avg": 12.0, "net_tx_kbs_avg": 3000.0}
        _seed_cluster_24h(c, "T", {
            "T--broker-1": ({}, calm), "T--broker-2": ({}, calm), "T--broker-3": ({}, hot),
        })
        result = scaling_advisor.analyze_cluster_scaling(c, "T", role="broker", now=NOW)
    assert result["verdict"] == "investigate", result
    assert "T--broker-3" in result["skew"]["hot_nodes"]


def test_scaling_advisor_investigates_heap_pressure_before_resizing():
    db = _tmp_db("scale_vertical_mem")
    with store.connect(db) as c:
        heap_pressure = {"full_gc_count": 2, "avg_heap_after_pct": 90.0}
        _seed_cluster_24h(c, "T", {
            "T--broker-1": (heap_pressure, {}),
            "T--broker-2": (heap_pressure, {}),
            "T--broker-3": (heap_pressure, {}),
        })
        result = scaling_advisor.analyze_cluster_scaling(c, "T", role="broker", now=NOW)
    assert result["verdict"] == "investigate", result


def test_scaling_advisor_unknown_cluster():
    db = _tmp_db("scale_unknown")
    with store.connect(db) as c:
        result = scaling_advisor.analyze_cluster_scaling(c, "NOPE", role="broker", now=NOW)
    assert result["verdict"] == "insufficient_data"
    assert result["n_nodes"] == 0


# --------------------------------------------------------------------------- #
# ml_insights.py
# --------------------------------------------------------------------------- #
def test_ml_insights_insufficient_baseline():
    db = _tmp_db("ml_insufficient")
    with store.connect(db) as c:
        _add_instance(c, "T--broker-1", "T")
        result = ml_insights.analyze_anomalies(c, "T--broker-1", days=30, recent_hours=24, now=NOW)
    assert result["method"] == "none"
    assert result["is_anomalous"] is False


def test_ml_insights_flags_obvious_spike():
    db = _tmp_db("ml_spike")
    with store.connect(db) as c:
        _add_instance(c, "T--broker-1", "T")
        # 14 days of calm baseline, then a clear spike in the most recent hours.
        for h in range(14 * 24):
            ts = NOW - (14 * 24 - h) * HOUR
            store.record_metric(c, "T--broker-1", ts, _gc_row(time_in_gc_pct=1.0 + 0.1 * math.sin(h)))
            store.record_host_metric(c, "T--broker-1", ts, _host_row(cpu_busy_pct_avg=15.0 + math.sin(h)))
        for h in range(6):
            ts = NOW - (6 - h) * HOUR
            store.record_metric(c, "T--broker-1", ts, _gc_row(time_in_gc_pct=45.0, full_gc_count=5))
            store.record_host_metric(c, "T--broker-1", ts, _host_row(cpu_busy_pct_avg=97.0))
        result = ml_insights.analyze_anomalies(c, "T--broker-1", days=30, recent_hours=24, now=NOW)
    assert result["method"] in ("robust_zscore", "isolation_forest")
    assert result["is_anomalous"] is True
    assert result["overall_anomaly_score"] > 50


def run_all():
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    passed = 0
    for fn in fns:
        try:
            fn()
            print(f"PASS  {fn.__name__}")
            passed += 1
        except AssertionError as exc:
            print(f"FAIL  {fn.__name__}: {exc}")
        except Exception as exc:  # noqa: BLE001
            print(f"ERROR {fn.__name__}: {type(exc).__name__}: {exc}")
    print(f"\n{passed}/{len(fns)} tests passed")
    return passed == len(fns)


if __name__ == "__main__":
    ok = run_all()
    sys.exit(0 if ok else 1)
