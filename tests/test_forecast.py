"""
Verification tests for forecast.py (capacity forecasting) and the SAR upload
ingest path (sar_ingest.ingest_sar_text).

Run:  python -m tests.test_forecast                  (from the project root)
or:   pytest -q tests/test_forecast.py

Same conventions as test_correlation_scaling.py: each test builds a small
synthetic instance/cluster against a temp SQLite store so every decision path
is deterministic.
"""

import json
import os
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from gcanalyzer import forecast, sar_ingest, store  # noqa: E402

HOUR = 3600
DAY = 86400
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
        "cpu_busy_pct_avg": 15.0, "load1_avg": 1.0, "load5_avg": 1.0,
        "runq_sz_avg": 0.0, "cswch_per_s_avg": 400.0,
        "mem_used_pct_avg": 40.0, "mem_cached_mb_avg": 1000.0,
        "swap_used_pct_max": 0.0,
        "disk_util_pct_max": 10.0, "disk_await_ms_max": 3.0, "disk_tps_avg": 50.0,
        "net_util_pct_max": 10.0, "net_rx_kbs_avg": 2000.0, "net_tx_kbs_avg": 3000.0,
        "top_disks": [], "top_nics": [],
    }
    base.update(overrides)
    return base


def _seed_trend(c, iid, days, host_fn=None, gc_fn=None, points_per_day=4):
    """Seed `days` of history; host_fn/gc_fn(day_index) -> row overrides."""
    for d in range(days):
        for p in range(points_per_day):
            ts = NOW - (days - d) * DAY + p * (DAY // points_per_day)
            store.record_metric(c, iid, ts, _gc_row(**(gc_fn(d) if gc_fn else {})))
            store.record_host_metric(c, iid, ts, _host_row(**(host_fn(d) if host_fn else {})))
    # These are live trend fixtures; keep the latest observation within freshness policy.
    store.record_metric(c, iid, NOW - HOUR, _gc_row(**(gc_fn(days - 1) if gc_fn else {})))
    store.record_host_metric(c, iid, NOW - HOUR, _host_row(**(host_fn(days - 1) if host_fn else {})))


# --------------------------------------------------------------------------- #
# theil_sen
# --------------------------------------------------------------------------- #
def test_theil_sen_recovers_slope_despite_outlier():
    pts = [(float(i), 40.0 + 0.5 * i) for i in range(30)]
    pts[10] = (10.0, 99.0)  # one incident day must not drag the slope
    slope, consistency = forecast.theil_sen(pts)
    assert abs(slope - 0.5) < 0.05, slope
    assert consistency > 0.8


def test_theil_sen_flat_series():
    pts = [(float(i), 50.0) for i in range(20)]
    slope, consistency = forecast.theil_sen(pts)
    assert abs(slope) < forecast.FLAT_SLOPE_EPS
    assert consistency == 0.0


# --------------------------------------------------------------------------- #
# forecast_instance
# --------------------------------------------------------------------------- #
def test_instance_insufficient_history():
    db = _tmp_db("fc_insufficient")
    with store.connect(db) as c:
        _add_instance(c, "T--broker-1", "T")
        _seed_trend(c, "T--broker-1", days=3)
        fc = forecast.forecast_instance(c, "T--broker-1", now=NOW)
    assert all(s["status"] == "insufficient_data" for s in fc["signals"])
    assert fc["risk"] == "no_data"
    assert "Not enough" in fc["headline"] or "history" in fc["headline"]


def test_instance_projects_cpu_breach():
    """CPU busy grows ~0.8%/day from 55% -> crosses 90% (crit) in ~30-45 days."""
    db = _tmp_db("fc_cpu_breach")
    with store.connect(db) as c:
        _add_instance(c, "T--broker-1", "T")
        _seed_trend(c, "T--broker-1", days=30,
                    host_fn=lambda d: {"cpu_busy_pct_avg": 55.0 + 0.8 * d})
        fc = forecast.forecast_instance(c, "T--broker-1", days=90, horizon_days=90, now=NOW)
    cpu = next(s for s in fc["signals"] if s["signal"] == "cpu_busy_pct")
    assert cpu["slope_per_day"] is not None and 0.6 < cpu["slope_per_day"] < 1.0, cpu
    assert cpu["days_to_crit"] is not None and 5 < cpu["days_to_crit"] < 30, cpu
    assert cpu["status"] in ("breach_imminent", "already_warning"), cpu
    assert cpu["confidence"] in ("medium", "high")
    assert fc["risk"] in ("critical", "warning")
    assert cpu["crit_date"] is not None


def test_instance_stable_when_flat():
    db = _tmp_db("fc_flat")
    with store.connect(db) as c:
        _add_instance(c, "T--broker-1", "T")
        _seed_trend(c, "T--broker-1", days=30)  # everything flat and calm
        fc = forecast.forecast_instance(c, "T--broker-1", now=NOW)
    assert fc["risk"] == "ok", fc["signals"][0]
    assert all(s["status"] in ("stable", "improving") for s in fc["signals"]
               if s["status"] != "insufficient_data")
    assert "No capacity breach projected" in fc["headline"]


def test_instance_improving_trend():
    db = _tmp_db("fc_improving")
    with store.connect(db) as c:
        _add_instance(c, "T--broker-1", "T")
        _seed_trend(c, "T--broker-1", days=30,
                    host_fn=lambda d: {"mem_used_pct_avg": 70.0 - 0.5 * d})
        fc = forecast.forecast_instance(c, "T--broker-1", now=NOW)
    mem = next(s for s in fc["signals"] if s["signal"] == "mem_used_pct")
    assert mem["status"] == "improving", mem
    assert mem["days_to_crit"] is None


def test_instance_already_critical():
    db = _tmp_db("fc_already_crit")
    with store.connect(db) as c:
        _add_instance(c, "T--broker-1", "T")
        _seed_trend(c, "T--broker-1", days=14,
                    host_fn=lambda d: {"cpu_busy_pct_avg": 95.0})
        fc = forecast.forecast_instance(c, "T--broker-1", now=NOW)
    cpu = next(s for s in fc["signals"] if s["signal"] == "cpu_busy_pct")
    assert cpu["status"] == "already_critical", cpu
    assert fc["risk"] == "critical"


def test_instance_heap_growth_flagged():
    db = _tmp_db("fc_heap_growth")
    with store.connect(db) as c:
        _add_instance(c, "T--broker-1", "T")
        _seed_trend(c, "T--broker-1", days=30,
                    gc_fn=lambda d: {"heap_after_pct": 55.0 + 0.6 * d})
        fc = forecast.forecast_instance(c, "T--broker-1", days=90, horizon_days=90, now=NOW)
    heap = next(s for s in fc["signals"] if s["signal"] == "heap_after_pct")
    assert heap["days_to_crit"] is not None, heap
    assert heap["group"] == "heap"
    assert heap["risk"] in ("critical", "warning"), heap


# --------------------------------------------------------------------------- #
# forecast_cluster
# --------------------------------------------------------------------------- #
def test_cluster_none_when_all_flat():
    db = _tmp_db("fc_cluster_none")
    with store.connect(db) as c:
        for i in (1, 2, 3):
            _add_instance(c, f"T--broker-{i}", "T")
            _seed_trend(c, f"T--broker-{i}", days=21)
        fc = forecast.forecast_cluster(c, "T", role="broker", now=NOW)
    assert fc["verdict"] == "none", fc
    assert not fc["warnings"]


def test_cluster_investigates_uniform_cpu_growth():
    db = _tmp_db("fc_cluster_horizontal")
    with store.connect(db) as c:
        for i in (1, 2, 3):
            _add_instance(c, f"T--broker-{i}", "T")
            _seed_trend(c, f"T--broker-{i}", days=30,
                        host_fn=lambda d: {"cpu_busy_pct_avg": 55.0 + 0.8 * d})
        fc = forecast.forecast_cluster(c, "T", role="broker", now=NOW)
    assert fc["verdict"] == "investigate", fc
    assert fc["confidence"] != "high"
    assert fc["warnings"], "expected proactive warnings for projected breaches"
    assert any("Earliest projected critical breach" in e for e in fc["evidence"])


def test_cluster_investigates_uniform_heap_growth():
    db = _tmp_db("fc_cluster_heap")
    with store.connect(db) as c:
        for i in (1, 2, 3):
            _add_instance(c, f"T--broker-{i}", "T")
            _seed_trend(c, f"T--broker-{i}", days=30,
                        gc_fn=lambda d: {"heap_after_pct": 55.0 + 0.6 * d})
        fc = forecast.forecast_cluster(c, "T", role="broker", now=NOW)
    assert fc["verdict"] == "investigate", fc


def test_cluster_watch_hot_node_when_one_node_grows():
    db = _tmp_db("fc_cluster_hot")
    with store.connect(db) as c:
        for i in (1, 2):
            _add_instance(c, f"T--broker-{i}", "T")
            _seed_trend(c, f"T--broker-{i}", days=30)  # flat peers
        _add_instance(c, "T--broker-3", "T")
        _seed_trend(c, "T--broker-3", days=30,
                    host_fn=lambda d: {"cpu_busy_pct_avg": 55.0 + 0.8 * d})
        fc = forecast.forecast_cluster(c, "T", role="broker", now=NOW)
    assert fc["verdict"] == "investigate", fc
    assert "T--broker-3" in fc["summary"]


def test_cluster_unknown():
    db = _tmp_db("fc_cluster_unknown")
    with store.connect(db) as c:
        fc = forecast.forecast_cluster(c, "NOPE", role="broker", now=NOW)
    assert fc["verdict"] == "insufficient_data"
    assert fc["n_nodes"] == 0


# --------------------------------------------------------------------------- #
# SAR upload ingest (sar_ingest.ingest_sar_text) — the no-SSH path
# --------------------------------------------------------------------------- #
def _sadf_json_text(n_samples=6, start_ts=NOW - 6 * HOUR, step_s=HOUR):
    """Minimal-but-valid `sadf -j -- -A` document the parser understands."""
    from datetime import datetime, timezone
    records = []
    for i in range(n_samples):
        ts = start_ts + i * step_s
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        records.append({
            "timestamp": {"date": dt.strftime("%m/%d/%Y"), "time": dt.strftime("%H:%M:%S"),
                          "utc": 1, "interval": 3600},
            "cpu-load-all": [{"cpu": "all", "user": 20.0, "nice": 0.0, "system": 5.0,
                              "iowait": 2.0, "steal": 0.0, "idle": 73.0}],
            "queue": {"runq-sz": 1, "plist-sz": 200, "ldavg-1": 1.2, "ldavg-5": 1.0,
                      "ldavg-15": 0.9, "blocked": 0},
            "memory": {"memfree": 2048000, "avail": 4096000, "memused": 6144000,
                       "memused-percent": 60.0, "buffers": 100000, "cached": 2000000,
                       "commit-percent": 40.0},
            "swap": {"swpfree": 2097152, "swpused": 0, "swpused-percent": 0.0},
        })
    return json.dumps({"sysstat": {"hosts": [{
        "nodename": "test-host", "sysname": "Linux", "release": "4.18.0",
        "machine": "x86_64", "number-of-cpus": 8, "file-date": "",
        "statistics": records,
    }]}})


def test_upload_ingests_sadf_json():
    db = _tmp_db("upload_json")
    with store.connect(db) as c:
        _add_instance(c, "T--broker-1", "T")
    result = sar_ingest.ingest_sar_text(_sadf_json_text(), "T--broker-1", db, now=NOW)
    assert result["recorded"] is True, result
    assert result["samples_parsed"] == 6
    assert result["samples_new"] == 6
    assert result["rows_written"] > 0
    assert result["source_format"] == "sadf-json"
    with store.connect(db) as c:
        rows = store.host_window_rows(c, "T--broker-1", NOW - 7 * HOUR, NOW)
    assert rows, "expected host_metrics rows after upload"
    assert any(r["mem_used_pct"] > 0 for r in rows)


def test_upload_dedups_reupload():
    db = _tmp_db("upload_dedup")
    with store.connect(db) as c:
        _add_instance(c, "T--broker-1", "T")
    text = _sadf_json_text()
    first = sar_ingest.ingest_sar_text(text, "T--broker-1", db, now=NOW)
    second = sar_ingest.ingest_sar_text(text, "T--broker-1", db, now=NOW)
    assert first["samples_new"] == 6
    assert second["recorded"] is True
    assert second["samples_new"] == 0, second
    assert "already ingested" in second["detail"]


def test_upload_rejects_garbage():
    db = _tmp_db("upload_garbage")
    with store.connect(db) as c:
        _add_instance(c, "T--broker-1", "T")
    result = sar_ingest.ingest_sar_text("this is not a sar report", "T--broker-1", db, now=NOW)
    assert result["recorded"] is False
    assert result["samples_parsed"] == 0

    empty = sar_ingest.ingest_sar_text("   ", "T--broker-1", db, now=NOW)
    assert empty["recorded"] is False


def test_upload_feeds_forecast():
    """End-to-end: uploaded SAR history becomes forecastable host signal."""
    db = _tmp_db("upload_forecast")
    with store.connect(db) as c:
        _add_instance(c, "T--broker-1", "T")
        # 14 days of GC history so now_ts() and GC signals exist.
        _seed_trend(c, "T--broker-1", days=14)
    with store.connect(db) as c:
        c.execute("DELETE FROM host_metrics")  # keep only the uploaded host data
    text = _sadf_json_text(n_samples=14 * 4, start_ts=NOW - 14 * DAY, step_s=6 * HOUR)
    result = sar_ingest.ingest_sar_text(text, "T--broker-1", db, now=NOW)
    assert result["recorded"], result
    with store.connect(db) as c:
        fc = forecast.forecast_instance(c, "T--broker-1", now=NOW - 6 * HOUR)
    cpu = next(s for s in fc["signals"] if s["signal"] == "cpu_busy_pct")
    assert cpu["status"] != "insufficient_data", cpu
    assert cpu["n_days"] >= forecast.MIN_DAYS


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
