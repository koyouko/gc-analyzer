"""
Verification tests for the SAR parse -> analyze -> store pipeline.

Run:  python -m tests.test_sar      (from the project root)
or:   pytest -q                      (pytest optional)
"""

import os
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from gcanalyzer import sar_parser, sar_analyzer, store  # noqa: E402

SAMPLES = os.path.join(ROOT, "samples")


def _tmp_db(name):
    return os.path.join(tempfile.gettempdir(), f"{name}_{os.getpid()}.db")


def test_sar_text_parser_basic():
    p = sar_parser.parse_file(os.path.join(SAMPLES, "broker-1-sar.txt"), node_id="broker-1")
    assert p.source_format == "sar-text", p.source_format
    assert len(p.samples) == 144, len(p.samples)
    s = p.samples[0]
    assert 0.0 <= s.cpu_idle_pct <= 100.0
    assert s.mem_used_pct > 0
    assert s.disks, "expected at least one disk row"
    assert s.nics, "expected at least one nic row"


def test_sar_json_parser_basic():
    p = sar_parser.parse_file(os.path.join(SAMPLES, "broker-1-sar.json"), node_id="broker-1")
    assert p.source_format == "sadf-json", p.source_format
    assert len(p.samples) == 144, len(p.samples)
    assert p.hostname == "broker-1"


def test_sar_json_and_text_agree():
    pt = sar_parser.parse_file(os.path.join(SAMPLES, "broker-1-sar.txt"), node_id="broker-1")
    pj = sar_parser.parse_file(os.path.join(SAMPLES, "broker-1-sar.json"), node_id="broker-1")
    assert len(pt.samples) == len(pj.samples)
    for a, b in zip(pt.samples[:10], pj.samples[:10]):
        assert a.ts == b.ts
        assert abs(a.cpu_user_pct - b.cpu_user_pct) < 0.01
        assert abs(a.mem_used_pct - b.mem_used_pct) < 0.01


def test_sar_parser_dispatcher_picks_json_when_it_looks_like_json():
    with open(os.path.join(SAMPLES, "broker-1-sar.json")) as fh:
        text = fh.read()
    p = sar_parser.parse(text, node_id="broker-1")
    assert p.source_format == "sadf-json"


def test_sar_parser_handles_garbage_gracefully():
    p = sar_parser.parse("this is not sar output at all\njust some text\n", node_id="x")
    assert p.samples == []


def test_sar_parser_handles_empty_json():
    p = sar_parser.parse_sadf_json("{}", node_id="x")
    assert p.samples == []
    assert p.warnings


def test_sar_analyzer_reports_pressure_window():
    """The generator injects a CPU/iowait/disk pressure window ~14:00-15:30 UTC."""
    p = sar_parser.parse_file(os.path.join(SAMPLES, "broker-1-sar.txt"), node_id="broker-1")
    a = sar_analyzer.analyze(p)
    m, h = a["metrics"], a["health"]
    assert m["sample_count"] == 144
    assert m["cpu_busy_pct_max"] > 40, m["cpu_busy_pct_max"]
    assert m["disk_util_pct_max"] > 50, m["disk_util_pct_max"]
    assert h["grade"] in ("A", "B", "C", "D", "F")
    assert a["findings"]["recommendations"]
    assert m["top_disks"] and m["top_disks"][0]["dev"] == "sda"


def test_sar_bucket_metrics_produces_multiple_buckets():
    p = sar_parser.parse_file(os.path.join(SAMPLES, "broker-1-sar.txt"), node_id="broker-1")
    buckets = sar_analyzer.bucket_metrics(p, bucket_s=3600)
    assert len(buckets) >= 20, len(buckets)
    for _, m in buckets:
        assert m["sample_count"] > 0


def test_store_host_metrics_roundtrip():
    db = _tmp_db("sar_roundtrip_test")
    if os.path.exists(db):
        os.remove(db)
    store.init_db(db)

    p = sar_parser.parse_file(os.path.join(SAMPLES, "broker-1-sar.txt"), node_id="broker-1")
    buckets = sar_analyzer.bucket_metrics(p, bucket_s=3600)
    assert buckets

    inst_id = "TEST--broker-1"
    with store.connect(db) as c:
        c.execute(
            "INSERT OR REPLACE INTO instances(id,region,env,cluster,grp,role,idx,heap_max_mb,collector,node_id)"
            " VALUES(?,?,?,?,?,?,?,?,?,?)",
            (inst_id, "TEST", "DEV", "TEST", "brokers", "broker", 1, 4096, "G1", "broker-1"),
        )
        for bts, m in buckets:
            store.record_host_metric(c, inst_id, bts, m)

    with store.connect(db) as c:
        rows = store.host_window_rows(c, inst_id, buckets[0][0], buckets[-1][0])
        assert len(rows) == len(buckets)
        now = buckets[-1][0]
        snap = store.current_host_snapshot(c, inst_id, now)
        assert snap["health"]["grade"] in ("A", "B", "C", "D", "F")
        assert snap["metrics"]["sample_count"] > 0

        trends = store.host_trends(c, inst_id, days=2, now=now)
        assert trends["series"]

        rng = store.host_range_series(c, inst_id, buckets[0][0], buckets[-1][0], 3600)
        assert rng["series"]


def test_sar_parses_rhel_paging_and_swap_activity():
    """RHEL 8/9 `sar -B` (paging) and `sar -W` (swapping) sections must parse
    identically from both text and JSON, and survive the store roundtrip."""
    pt = sar_parser.parse_file(os.path.join(SAMPLES, "broker-1-sar.txt"), node_id="broker-1")
    pj = sar_parser.parse_file(os.path.join(SAMPLES, "broker-1-sar.json"), node_id="broker-1")

    # The generator injects paging pressure mid-afternoon; both formats agree.
    assert max(s.pgpgout_kbs for s in pt.samples) > 1000, "expected pgpgout activity"
    assert max(s.majflt_per_s for s in pt.samples) > 5, "expected a majflt spike"
    assert max(s.pswpout_per_s for s in pt.samples) > 0, "expected some swap-out activity"
    for a, b in zip(pt.samples[:20], pj.samples[:20]):
        assert abs(a.pgpgin_kbs - b.pgpgin_kbs) < 0.01
        assert abs(a.majflt_per_s - b.majflt_per_s) < 0.01
        assert abs(a.pswpout_per_s - b.pswpout_per_s) < 0.01

    # Rollup exposes the new aggregates…
    m = sar_analyzer.analyze(pt)["metrics"]
    for key in ("pgpgin_kbs_avg", "pgpgout_kbs_avg", "fault_per_s_avg", "majflt_per_s_max",
                "pswpout_per_s_max", "cpu_steal_pct_avg", "load15_avg", "mem_commit_pct_avg"):
        assert key in m, f"missing rollup key {key}"
    assert m["majflt_per_s_max"] > 5

    # …and they survive record -> read-back through the store.
    db = _tmp_db("sar_paging_roundtrip")
    if os.path.exists(db):
        os.remove(db)
    store.init_db(db)
    buckets = sar_analyzer.bucket_metrics(pt, bucket_s=3600)
    inst_id = "TEST--broker-1"
    with store.connect(db) as c:
        c.execute(
            "INSERT OR REPLACE INTO instances(id,region,env,cluster,grp,role,idx,heap_max_mb,collector,node_id)"
            " VALUES(?,?,?,?,?,?,?,?,?,?)",
            (inst_id, "TEST", "DEV", "TEST", "brokers", "broker", 1, 4096, "G1", "broker-1"),
        )
        for bts, mm in buckets:
            store.record_host_metric(c, inst_id, bts, mm)
    with store.connect(db) as c:
        now = buckets[-1][0]
        snap = store.current_host_snapshot(c, inst_id, now)
        assert snap["metrics"]["pgpgout_kbs_avg"] > 0
        assert snap["metrics"]["majflt_per_s_max"] > 5
        rng = store.host_range_series(c, inst_id, buckets[0][0], now, 3600)
        pt0 = rng["series"][0]
        for key in ("pgpgin_avg", "pgpgout_avg", "fault_avg", "majflt_max", "pswpout_max",
                    "net_rx_avg", "cpu_user_avg", "load15_avg", "cswch_avg"):
            assert key in pt0, f"missing series key {key}"
        assert any(p["net_rx_avg"] > 0 for p in rng["series"]), "ingress series should be populated"


def test_store_migrates_legacy_host_metrics_schema():
    """A DB created before the RHEL full-metric columns must be migrated in
    place by init_db, preserving missing measurements as None."""
    db = _tmp_db("sar_migration_test")
    if os.path.exists(db):
        os.remove(db)
    import sqlite3
    conn = sqlite3.connect(db)
    conn.executescript("""
        CREATE TABLE instances (id TEXT PRIMARY KEY, region TEXT, env TEXT, cluster TEXT,
            grp TEXT, role TEXT, idx INTEGER, heap_max_mb INTEGER, collector TEXT, node_id TEXT);
        CREATE TABLE host_metrics (
            ts INTEGER, instance_id TEXT, cpu_user_pct REAL, cpu_system_pct REAL,
            cpu_iowait_pct REAL, cpu_busy_pct REAL, load1 REAL, load5 REAL, runq_sz REAL,
            cswch_per_s REAL, mem_used_pct REAL, mem_cached_mb REAL, swap_used_pct REAL,
            disk_util_pct_max REAL, disk_await_ms_max REAL, disk_tps REAL, net_util_pct_max REAL,
            net_rx_kbs REAL, net_tx_kbs REAL, top_disks_json TEXT, top_nics_json TEXT,
            PRIMARY KEY (instance_id, ts));
    """)
    conn.execute("INSERT INTO instances VALUES ('T--b1','R','E','T','brokers','broker',1,4096,'G1','b1')")
    conn.execute(
        "INSERT INTO host_metrics(ts,instance_id,cpu_user_pct,cpu_system_pct,cpu_iowait_pct,"
        "cpu_busy_pct,load1,load5,runq_sz,cswch_per_s,mem_used_pct,mem_cached_mb,swap_used_pct,"
        "disk_util_pct_max,disk_await_ms_max,disk_tps,net_util_pct_max,net_rx_kbs,net_tx_kbs,"
        "top_disks_json,top_nics_json) VALUES (1000,'T--b1',10,3,1,15,1,1,0,400,40,1000,0,10,3,50,10,2000,3000,'[]','[]')"
    )
    conn.commit()
    conn.close()

    store.init_db(db)  # migration point
    with store.connect(db) as c:
        cols = {r[1] for r in c.execute("PRAGMA table_info(host_metrics)")}
        for col in store._HOST_METRIC_EXTRA_COLS:
            assert col in cols, f"migration missed column {col}"
        snap = store.current_host_snapshot(c, "T--b1", 1000)
        assert snap["metrics"]["pgpgin_kbs_avg"] is None
        assert snap["metrics"]["cpu_busy_pct_avg"] == 15.0
        # legacy rows must not break the extended series either
        rng = store.host_range_series(c, "T--b1", 0, 2000, 3600)
        assert rng["series"][0]["pgpgout_avg"] is None
        assert rng["series"][0]["net_rx_avg"] > 0


def test_sar_collector_state_dedup_point():
    db = _tmp_db("sar_state_test")
    if os.path.exists(db):
        os.remove(db)
    store.init_db(db)
    with store.connect(db) as c:
        assert store.get_sar_state(c, "TEST--broker-1") is None
        store.set_sar_state(c, "TEST--broker-1", 12345, 12345)
    with store.connect(db) as c:
        assert store.get_sar_state(c, "TEST--broker-1") == 12345


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
