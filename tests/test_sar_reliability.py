"""SAR identity, missing-data and snapshot freshness regressions (no collectors)."""

import json
import pytest

from gcanalyzer import sar_analyzer, sar_ingest, sar_parser, store


IID = "TEST--broker-1"
BASE = 1_780_272_000  # UTC midnight, minute aligned


@pytest.fixture
def db(tmp_path):
    path = str(tmp_path / "sar.db")
    store.init_db(path)
    with store.connect(path) as c:
        c.execute("INSERT INTO instances(id,heap_max_mb,collector) VALUES(?,4096,'G1')", (IID,))
    return path


def parsed(*samples):
    return sar_parser.ParsedSar(IID, "sadf-json", list(samples))


def complete(ts=BASE, **overrides):
    values = dict(cpu_idle_pct=80, cpu_user_pct=15, cpu_system_pct=5,
                  cpu_iowait_pct=0, mem_used_pct=50, swap_used_pct=0,
                  disks=[{"dev": "sda", "util_pct": 10, "await_ms": 1}],
                  nics=[{"iface": "eth0", "util_pct": 1}])
    values.update(overrides)
    return sar_parser.SarSample(ts, **values)


def json_report(stat, **host):
    return json.dumps({"sysstat": {"hosts": [{"statistics": [stat], **host}]}})


def gc_row(**overrides):
    row = dict(heap_used_mb=1024, heap_max_mb=4096, heap_after_pct=25,
               pause_avg_ms=1, pause_p99_ms=2, pause_max_ms=3, full_gc_count=0,
               young_count=1, gc_per_min=1, time_in_gc_pct=1, throughput_pct=99)
    row.update(overrides)
    return row


def test_sparse_json_preserves_missing_and_invalid_metrics():
    p = sar_parser.parse(json_report({
        "timestamp": {"date": "2026-06-01", "time": "00:00:00", "utc": 1},
        "cpu-load": [{"cpu": "all", "idle": 90, "usr": "NaN", "sys": "-"}],
        "memory": {"avail": "bad"},
    }), IID)
    assert len(p.samples) == 1
    s = p.samples[0]
    assert s.cpu_idle_pct == 90
    assert s.cpu_user_pct is None
    assert s.cpu_system_pct is None
    assert s.mem_avail_mb is None
    assert s.swap_used_pct is None


@pytest.mark.parametrize("report_date", ["2026-06-01", "06/01/2026"])
def test_text_ampm_and_report_date(report_date):
    p = sar_parser.parse_sar_text(
        "12:00:00 AM CPU %user %system %idle\n"
        "12:10:00 AM all 1 2 97\n"
        "12:10:00 PM all 3 4 93\n", IID, report_date=report_date)
    assert [s.ts for s in p.samples] == [BASE + 600, BASE + 12 * 3600 + 600]
    assert p.samples[1].cpu_user_pct == 3
    assert p.samples[1].mem_used_pct is None


def test_text_iso_banner_and_timezone_and_midnight():
    text = ("Linux host 2026-06-01\n\n"
            "11:40:00 PM CPU %user %system %idle\n"
            "11:50:00 PM all 1 2 97\n12:00:00 AM all 3 4 93\n")
    p = sar_parser.parse(text, IID, report_timezone="America/Toronto")
    assert [s.ts for s in p.samples] == [BASE + 27 * 3600 + 3000, BASE + 28 * 3600]


@pytest.mark.parametrize("timestamp,zone,expected", [
    ({"time": "01:00:00", "utc": 1}, "America/Toronto", BASE + 3600),
    ({"time": "01:00:00", "utc": 0}, "America/Toronto", BASE + 5 * 3600),
    ({"time": "01:00:00-04:00"}, None, BASE + 5 * 3600),
    ({"time": "01:00:00", "utc": 0, "timezone": "+05:30"}, None, BASE - 16200),
])
def test_json_timezone_and_explicit_date(timestamp, zone, expected):
    p = sar_parser.parse(json_report({"timestamp": timestamp, "queue": {"ldavg-1": 1}}),
                         IID, report_date="2026-06-01", report_timezone=zone)
    assert [s.ts for s in p.samples] == [expected]


def test_unresolvable_local_timezone_warns_instead_of_inventing_utc():
    p = sar_parser.parse_sadf_json(json_report({
        "timestamp": {"date": "2026-06-01", "time": "01:00:00", "utc": 0},
    }), IID)
    assert not p.samples
    assert p.warnings


def test_analyzer_unknown_and_partial_are_not_healthy():
    empty = sar_analyzer.analyze(parsed())
    assert empty["health"] == {"grade": "?", "status": "unknown", "score": None, "reasons": []}
    a = sar_analyzer.analyze(parsed(sar_parser.SarSample(BASE, cpu_idle_pct=95)))
    assert a["metrics"]["mem_used_pct_avg"] is None
    assert a["metrics"]["disk_util_pct_max"] is None
    assert a["timeline"][0]["net_util_pct"] is None
    assert a["health"]["grade"] == "?"
    assert a["health"]["status"] == "partial"
    assert a["health"]["score"] is None
    assert not any("swap" in pro.lower() or "storage" in pro.lower() for pro in a["findings"]["pros"])
    assert not any("headroom looks healthy" in r for r in a["findings"]["recommendations"])


def test_partial_disk_can_report_pressure_without_formatting_null():
    a = sar_analyzer.analyze(parsed(sar_parser.SarSample(
        BASE, disks=[{"dev": "sda", "util_pct": 99, "await_ms": None}])))
    assert a["health"]["status"] == "partial"
    assert a["findings"]["cons"]
    assert a["metrics"]["disk_await_ms_max"] is None


def test_incremental_ingest_matches_full_import_on_fixed_grid(db, tmp_path):
    samples = [complete(BASE + i * 600, cpu_idle_pct=90 - i % 10) for i in range(50)]
    whole = str(tmp_path / "whole.db")
    sar_ingest.record_parsed_sar(parsed(*samples), IID, whole, now=BASE)
    for part in [samples[:1], samples[1:4], samples[4:15], samples[15:]]:
        sar_ingest.record_parsed_sar(parsed(*part), IID, db, now=BASE)
    with store.connect(db) as c, store.connect(whole) as other:
        incremental = store.host_window_rows(c, IID, 0)
        assert incremental == store.host_window_rows(other, IID, 0)
        assert {r["bucket_seconds"] for r in incremental} == {60}


def test_late_repeated_and_corrected_samples_recompute_only_affected_bucket(db):
    first = complete(BASE + 10, cpu_idle_pct=90)
    last = complete(BASE + 50, cpu_idle_pct=70)
    other = complete(BASE + 600)
    sar_ingest.record_parsed_sar(parsed(first, other), IID, db, now=BASE)
    assert sar_ingest.record_parsed_sar(parsed(last), IID, db, now=BASE) == (1, 1)
    assert sar_ingest.record_parsed_sar(parsed(first, last, other), IID, db, now=BASE) == (0, 0)
    with store.connect(db) as c:
        r = store.host_window_rows(c, IID, BASE, BASE + 59)[0]
        assert r["cpu_busy_pct"] == 20
        assert r["sample_count"] == 2
        assert r["last_observed_at"] == BASE + 50
        assert store._host_metrics_dict_from_window([r])["span_seconds"] == 40
        assert c.execute("SELECT COUNT(*) FROM sar_samples").fetchone()[0] == 3
        assert store.get_sar_state(c, IID) == BASE + 600
    # A correction at the same sample identity changes the rollup, never the count.
    changed = sar_ingest.record_parsed_sar(parsed(complete(BASE + 50, cpu_idle_pct=50)), IID, db)
    assert changed[1] == 1
    with store.connect(db) as c:
        r = store.host_window_rows(c, IID, BASE, BASE + 59)[0]
        assert r["cpu_busy_pct"] == 30
        assert r["sample_count"] == 2


def test_complementary_reports_merge_same_sample_without_erasing_fields(db):
    sar_ingest.record_parsed_sar(parsed(sar_parser.SarSample(BASE, cpu_idle_pct=75)), IID, db)
    sar_ingest.record_parsed_sar(parsed(sar_parser.SarSample(BASE, mem_used_pct=40)), IID, db)
    with store.connect(db) as c:
        r = store.host_latest_row(c, IID)
        assert r["sample_count"] == 1
        assert r["cpu_busy_pct"] == 25
        assert r["mem_used_pct"] == 40


def test_duration_weighting_counts_and_peaks_survive_store_and_query(db):
    samples = [complete(BASE + 10, cpu_idle_pct=100, interval_seconds=10),
               complete(BASE + 50, cpu_idle_pct=0, interval_seconds=30),
               complete(BASE + 70, cpu_idle_pct=80, interval_seconds=10)]
    sar_ingest.record_parsed_sar(parsed(*samples), IID, db)
    with store.connect(db) as c:
        rows = store.host_window_rows(c, IID, BASE)
        assert rows[0]["sample_count"] == 2
        assert rows[0]["duration_seconds"] == 40
        assert rows[0]["cpu_busy_pct"] == 75
        m = store._host_metrics_dict_from_window(rows)
        assert m["sample_count"] == 3
        assert m["duration_seconds"] == 50
        assert m["cpu_busy_pct_avg"] == 64
        assert m["cpu_busy_pct_max"] == 100
        assert store.host_range_series(c, IID, BASE, BASE + 100, 3600)["series"][0]["cpu_busy_avg"] == 64
        assert store.host_trends(c, IID, now=BASE + 100)["series"][0]["cpu_busy_avg"] == 64


def test_null_host_rollups_remain_null_in_snapshots_and_series(db):
    sar_ingest.record_parsed_sar(parsed(sar_parser.SarSample(BASE, cpu_idle_pct=90)), IID, db)
    with store.connect(db) as c:
        snap = store.current_host_snapshot(c, IID, now=BASE)
        assert snap["metrics"]["mem_used_pct_avg"] is None
        assert snap["quality"]["state"] == "partial"
        assert "mem_used_pct" in snap["quality"]["missing_metrics"]
        assert "cpu_busy_pct" in snap["quality"]["available_metrics"]
        assert store.host_range_series(c, IID, BASE, BASE + 60, 60)["series"][0]["mem_avg"] is None
        assert store.host_trends(c, IID, now=BASE)["series"][0]["mem_avg"] is None


@pytest.mark.parametrize("kind", ["gc", "host"])
def test_snapshot_missing_and_stale_are_unknown_but_latest_remains_visible(db, kind):
    snapshot = store.current_snapshot if kind == "gc" else store.current_host_snapshot
    with store.connect(db) as c:
        missing = snapshot(c, IID, now=BASE)
        assert missing["health"]["grade"] == "?"
        assert missing["quality"] == {"state": "missing", "last_observed_at": None,
                                     "age_seconds": None, "available_metrics": [],
                                     "missing_metrics": missing["quality"]["missing_metrics"]}
        if kind == "gc":
            store.record_metric(c, IID, BASE, gc_row())
        else:
            store.record_host_metric(c, IID, BASE, sar_analyzer.analyze(parsed(complete()))["metrics"])
        fresh = snapshot(c, IID, now=BASE + 1)
        assert fresh["quality"]["state"] == "fresh"
        stale = snapshot(c, IID, now=BASE + 3 * 86400)
        assert stale["quality"]["state"] == "stale"
        assert stale["quality"]["age_seconds"] == 3 * 86400
        assert stale["health"]["status"] == "unknown"
        assert stale["health"]["score"] is None
        assert stale["latest"]["ts"] == BASE
        assert stale["metrics"] == {}
        assert not (stale.get("findings") or {}).get("recommendations")


def test_live_wall_clock_demo_latest_gc_or_host_and_explicit_zero(db, monkeypatch):
    monkeypatch.delenv("GC_DEMO_MODE", raising=False)
    monkeypatch.setattr(store.time, "time", lambda: BASE + 86400)
    with store.connect(db) as c:
        store.record_metric(c, IID, BASE, gc_row())
        store.record_host_metric(c, IID, BASE + 600, {"cpu_busy_pct_avg": 5})
        assert store.now_ts(c) == BASE + 86400
        assert store.current_snapshot(c, IID)["quality"]["state"] == "stale"
        monkeypatch.setenv("GC_DEMO_MODE", "1")
        assert store.now_ts(c) == BASE + 600
        assert store.current_snapshot(c, IID, now=0)["quality"]["state"] == "missing"


def test_gc_nullable_queries_do_not_average_empty_lists(db):
    with store.connect(db) as c:
        store.record_metric(c, IID, BASE, gc_row(**{k: None for k in gc_row()}))
        m = store._metrics_dict_from_window(store.window_rows(c, IID, BASE), 4096)
        assert m["throughput_pct"] is None
        assert m["full_count"] is None
        assert store.range_series(c, IID, BASE, BASE, 60)["series"][0]["heap_used_avg"] is None
        assert store.trends(c, IID, now=BASE)["series"][0]["heap_used_avg"] is None
        assert store.evaluate_alerts(c, IID, now=BASE) == []
        assert store.current_snapshot(c, IID, now=BASE)["health"]["grade"] == "?"


def test_raw_samples_follow_retention_and_instance_lifecycle(db):
    sar_ingest.record_parsed_sar(parsed(complete(BASE), complete(BASE + 600)), IID, db)
    with store.connect(db) as c:
        store.migrate_instance(c, IID, "renamed")
        assert c.execute("SELECT COUNT(*) FROM sar_samples WHERE instance_id='renamed'").fetchone()[0] == 2
        store.prune_before(c, BASE + 60)
        assert c.execute("SELECT COUNT(*) FROM sar_samples").fetchone()[0] == 1
        store.delete_instance(c, "renamed")
        assert c.execute("SELECT COUNT(*) FROM sar_samples").fetchone()[0] == 0


def test_init_is_nondestructive_and_repeatable(db):
    with store.connect(db) as c:
        store.record_host_metric(c, IID, BASE, {"cpu_busy_pct_avg": 21})
        store.set_sar_state(c, IID, BASE + 1000, BASE)
    store.init_db(db)
    store.init_db(db)
    with store.connect(db) as c:
        assert store.host_latest_row(c, IID)["cpu_busy_pct"] == 21
        assert store.host_latest_row(c, IID)["mem_used_pct"] is None
    # The old watermark is not evidence that an unseen, older sample is a duplicate.
    assert sar_ingest.record_parsed_sar(parsed(complete(BASE + 60)), IID, db)[0] == 1


@pytest.mark.parametrize("date,zone", [("not-a-date", "UTC"), ("2026-06-01", "not/a-zone"),
                                       ("2026-03-08", "America/Toronto"),
                                       ("2026-11-01", "America/Toronto")])
def test_bad_or_ambiguous_text_time_is_rejected_with_warning(date, zone):
    clock = "01:30:00" if date == "2026-11-01" else "02:30:00"
    p = sar_parser.parse_sar_text(f"00:00:00 CPU %user %idle\n{clock} all 1 99\n", IID,
                                  report_date=date, report_timezone=zone)
    assert not p.samples
    assert p.warnings


def test_concatenated_daily_text_reports_keep_each_banner_date():
    p = sar_parser.parse_sar_text(
        "Linux host 2026-06-01\n00:00:00 CPU %user %idle\n00:10:00 all 1 99\n\n"
        "Linux host 2026-06-02\n00:00:00 CPU %user %idle\n00:10:00 all 2 98\n", IID)
    assert [s.ts for s in p.samples] == [BASE + 600, BASE + 86400 + 600]


def test_malformed_json_section_does_not_erase_other_measurements():
    p = sar_parser.parse_sadf_json(json_report({
        "timestamp": {"date": "2026-06-01", "time": "00:00:00", "utc": 1},
        "cpu-load": [{"cpu": "all", "idle": 90}], "disk": [None], "network": "bad",
    }), IID)
    assert len(p.samples) == 1
    assert p.samples[0].cpu_idle_pct == 90
    assert p.samples[0].disks == []
    assert p.warnings


@pytest.mark.parametrize("document", ['[]', '{"sysstat": []}', '{"sysstat":{"hosts":[null]}}'])
def test_malformed_json_structure_warns(document):
    p = sar_parser.parse_sadf_json(document, IID)
    assert p.samples == []
    assert p.warnings


def test_partial_samples_have_per_metric_weights_and_preserve_peaks(db):
    samples = [complete(BASE + 10, cpu_idle_pct=80),
               sar_parser.SarSample(BASE + 20, cpu_idle_pct=60),
               complete(BASE + 70, cpu_idle_pct=20, mem_used_pct=80)]
    sar_ingest.record_parsed_sar(parsed(*samples), IID, db)
    with store.connect(db) as c:
        m = store._host_metrics_dict_from_window(store.host_window_rows(c, IID, BASE))
        assert m["cpu_busy_pct_avg"] == pytest.approx(46.67)
        assert m["mem_used_pct_avg"] == 65
        assert m["cpu_busy_pct_max"] == 80
        assert store.current_host_snapshot(c, IID, now=BASE + 70)["quality"]["state"] == "partial"


def test_transaction_failure_does_not_consume_sample_or_watermark(db, monkeypatch):
    def fail(*args):
        raise RuntimeError("rollup failed")
    monkeypatch.setattr(store, "recompute_host_buckets", fail)
    with pytest.raises(RuntimeError, match="rollup failed"):
        sar_ingest.record_parsed_sar(parsed(complete()), IID, db)
    with store.connect(db) as c:
        assert c.execute("SELECT COUNT(*) FROM sar_samples").fetchone()[0] == 0
        assert store.get_sar_state(c, IID) is None
        assert not store.host_window_rows(c, IID, 0)


def test_freshness_uses_actual_sample_time_and_has_a_bounded_ttl(db, monkeypatch):
    sar_ingest.record_parsed_sar(parsed(complete(BASE + 59)), IID, db)
    monkeypatch.setenv("SAR_SOURCE_MAX_AGE_S", "60")
    with store.connect(db) as c:
        fresh = store.current_host_snapshot(c, IID, now=BASE + 100)
        assert fresh["quality"]["last_observed_at"] == BASE + 59
        assert fresh["quality"]["age_seconds"] == 41
        assert fresh["quality"]["state"] == "fresh"
        assert store.current_host_snapshot(c, IID, now=BASE + 120)["quality"]["state"] == "stale"


def test_explicit_now_and_range_do_not_leak_future_samples_in_same_bucket(db):
    sar_ingest.record_parsed_sar(parsed(complete(BASE + 10, cpu_idle_pct=90),
                                        complete(BASE + 50, cpu_idle_pct=10)), IID, db)
    with store.connect(db) as c:
        assert store.current_host_snapshot(c, IID, now=BASE)["quality"]["state"] == "missing"
        snap = store.current_host_snapshot(c, IID, now=BASE + 20)
        assert snap["metrics"]["cpu_busy_pct_avg"] == 10
        assert snap["metrics"]["sample_count"] == 1
        assert snap["quality"]["last_observed_at"] == BASE + 10
        rows = store.host_window_rows(c, IID, BASE + 20, BASE + 55)
        assert len(rows) == 1
        assert rows[0]["cpu_busy_pct"] == 90
        assert rows[0]["sample_count"] == 1


def test_repeated_imports_are_serialized_without_lost_updates(db):
    from concurrent.futures import ThreadPoolExecutor

    def ingest(offset):
        return sar_ingest.record_parsed_sar(parsed(complete(BASE + offset)), IID, db)

    with ThreadPoolExecutor(max_workers=4) as workers:
        results = list(workers.map(ingest, [10, 20, 10, 30, 20, 30]))
    assert sum(count for count, _ in results) == 3
    with store.connect(db) as c:
        assert store.host_latest_row(c, IID)["sample_count"] == 3


def test_unknown_duration_falls_back_consistently_to_sample_weights(db):
    samples = [complete(BASE + 10, cpu_idle_pct=100, interval_seconds=10),
               complete(BASE + 70, cpu_idle_pct=0, interval_seconds=30),
               sar_parser.SarSample(BASE + 130, mem_used_pct=50)]
    sar_ingest.record_parsed_sar(parsed(*samples), IID, db)
    with store.connect(db) as c:
        m = store._host_metrics_dict_from_window(store.host_window_rows(c, IID, BASE))
        assert m["duration_seconds"] is None
        assert m["cpu_busy_pct_avg"] == sar_analyzer.analyze(parsed(*samples))["metrics"]["cpu_busy_pct_avg"] == 50
