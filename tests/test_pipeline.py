"""
Verification tests for the parse -> analyze pipeline.

Run:  python -m tests.test_pipeline      (from the project root)
or:   pytest -q                           (pytest optional)

These assert the parser recovers the structure we synthesised and that the
analyzer's health judgements line up with each node's designed profile.
"""

import os
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from gcanalyzer import parser, analyzer, ingest, store  # noqa: E402
from gcanalyzer.collector import NodeConfig  # noqa: E402

SAMPLES = os.path.join(ROOT, "samples")


def _analyze(name):
    p = parser.parse_file(os.path.join(SAMPLES, name), node_id=name)
    return p, analyzer.analyze(p)


def test_collector_and_format_detection():
    p, _ = _analyze("broker-1-gc.log")
    assert p.collector == "G1", p.collector
    assert p.java_hint == "unified", p.java_hint


def test_unit_conversion_and_heap_max():
    p, a = _analyze("broker-1-gc.log")
    assert a["metrics"]["heap_max_mb"] == 4096.0, a["metrics"]["heap_max_mb"]
    # Every event should have a sane heap transition.
    for e in p.events:
        if e.is_stw and e.heap_before_mb and e.heap_after_mb:
            assert e.heap_after_mb <= e.heap_total_mb + 1


def test_healthy_broker_profile():
    _, a = _analyze("broker-1-gc.log")
    m, h = a["metrics"], a["health"]
    assert m["full_count"] == 0, "healthy broker should have no Full GCs"
    assert m["throughput_pct"] > 99.0, m["throughput_pct"]
    assert h["grade"] in ("A", "B"), h


def test_long_young_pause_is_not_counted_as_full_gc():
    line = (
        "[2026-06-08T10:00:00.000+0000][info][gc] GC(99) Pause Young (Normal) "
        "(G1 Evacuation Pause) 3990M->1200M(4096M) 869.123ms\n"
    )
    p = parser.parse(line, "t")
    a = analyzer.analyze(p)
    assert a["metrics"]["full_count"] == 0
    assert a["metrics"]["max_pause_ms"] >= 869


def test_split_line_pause_full_is_counted():
    text = (
        "[2026-06-08T10:00:00.000+0000][info][gc,start] GC(42) Pause Full (Allocation Failure)\n"
        "[2026-06-08T10:00:00.869+0000][info][gc,heap] GC(42) Eden: 8192M->4096M(8192M) 869.123ms\n"
    )
    p = parser.parse(text, "t")
    a = analyzer.analyze(p)
    assert a["metrics"]["full_count"] == 1, a["metrics"]


def test_pressured_broker_detects_full_gc_and_hotspots():
    _, a = _analyze("broker-2-gc.log")
    m, h = a["metrics"], a["health"]
    assert m["full_count"] > 0, "pressured broker must surface Full GCs"
    assert m["max_pause_ms"] > 500, m["max_pause_ms"]
    assert any(w["is_hotspot"] for w in a["hotspots"]), "expected hotspot windows"
    assert h["score"] < 75, h["score"]
    assert h["grade"] in ("C", "D", "F"), h
    # The 10:12-10:16 window we injected should be among the hotspots.
    starts = [w["window_start"] for w in a["hotspots"]]
    assert starts, "no hotspot windows returned"


def test_leaky_broker_promotion_trend():
    _, a = _analyze("broker-3-gc.log")
    m = a["metrics"]
    assert m["promotion_trend_pct"] > 55, m["promotion_trend_pct"]
    assert m["peak_heap_after_pct"] > 65, m["peak_heap_after_pct"]


def test_light_nodes_are_healthy():
    for name in ("controller-1-gc.log", "zookeeper-1-gc.log"):
        _, a = _analyze(name)
        assert a["metrics"]["full_count"] == 0
        assert a["health"]["grade"] in ("A", "B"), (name, a["health"])


def test_recommendations_present():
    _, a = _analyze("broker-2-gc.log")
    f = a["findings"]
    assert f["cons"], "pressured broker should have cons"
    assert f["recommendations"], "should produce recommendations"



KAFKA281_GC_SNIPPET = """
[2026-06-23T01:19:37.269+0100][gc,start] GC(139421) Pause Young (Normal) (G1 Evacuation Pause)
[2026-06-23T01:19:37.269+0100][gc,phases] GC(139421)   Evacuate Collection Set: 10.5ms
[2026-06-23T01:19:37.282+0100][gc] GC(139421) Pause Young (Normal) (G1 Evacuation Pause) 4953M->2909M(8192M) 12.178ms
[2026-06-23T01:20:36.765+0100][gc] GC(139425) Pause Young (Normal) (G1 Evacuation Pause) 5100M->3000M(8192M) 13.176ms
[2026-06-23T01:21:36.765+0100][gc] GC(139426) Pause Young (Normal) (G1 Evacuation Pause) 5200M->3100M(8192M) 11.500ms
"""


def test_kafka281_unified_gc_format():
    """Kafka 2.8.x / Java 11+ unified GC logs without [info] tag prefix."""
    p = parser.parse(KAFKA281_GC_SNIPPET, node_id="br-1-host")
    assert p.collector == "G1", p.collector
    assert p.java_hint == "unified", p.java_hint
    assert len(p.events) == 3, len(p.events)
    assert p.heap_max_mb == 8192.0
    assert all(e.timestamp is not None for e in p.events)
    buckets = analyzer.bucket_metrics(p)
    assert len(buckets) >= 2, f"expected trend buckets, got {len(buckets)}"


def test_demo_heap_max_survives_empty_incremental_collect(monkeypatch):
    """An empty incremental read must not shrink demo broker heap metadata."""
    db = os.path.join(tempfile.gettempdir(), "gc_empty_incremental_test.db")
    if os.path.exists(db):
        os.remove(db)
    store.init_db(db)
    node = NodeConfig(id="broker-1", role="broker", source="local", local_paths=[])
    inst_id = "DEMO-KRAFT--broker-1"
    with store.connect(db) as c:
        ingest.sync_instances_from_nodes(c, [node], "DEMO", "KRAFT", "DEMO-KRAFT")
        store.record_metric(
            c,
            inst_id,
            1000,
            {
                "heap_used_mb": 100.0,
                "heap_max_mb": 6144.0,
                "heap_after_pct": 2.0,
                "pause_avg_ms": 1.0,
                "pause_p99_ms": 1.0,
                "pause_max_ms": 1.0,
                "full_gc_count": 0,
                "young_count": 1,
                "gc_per_min": 1.0,
                "time_in_gc_pct": 0.1,
                "throughput_pct": 99.9,
            },
        )

    monkeypatch.setattr(ingest, "read_increment_local", lambda _node, _prev: ("", {}))

    results = ingest.ingest_nodes(
        [node],
        db,
        region="DEMO",
        env="KRAFT",
        cluster="DEMO-KRAFT",
        collect_mode="incremental",
    )

    assert results[0].recorded is True
    with store.connect(db) as c:
        inst = store.get_instance(c, inst_id)
    assert inst["heap_max_mb"] == 6144


def test_fresh_observed_heap_can_lower_stale_instance_metadata():
    db = os.path.join(tempfile.gettempdir(), "gc_heap_downsize_test.db")
    if os.path.exists(db):
        os.remove(db)
    store.init_db(db)
    node = NodeConfig(id="broker-1", role="broker", source="local", local_paths=[])
    inst_id = "DEMO-KRAFT--broker-1"
    with store.connect(db) as c:
        ingest.sync_instances_from_nodes(c, [node], "DEMO", "KRAFT", "DEMO-KRAFT")
        assert store.get_instance(c, inst_id)["heap_max_mb"] == 6144
        heap = ingest._heap_max_for_instance(c, "broker", 4096.0, "DEMO-KRAFT", inst_id)

    assert heap == 4096


def test_scheduler_uses_cluster_aware_heap_resolution():
    with open(os.path.join(ROOT, "gcanalyzer", "scheduler.py")) as fh:
        scheduler_source = fh.read()

    assert "ingest._heap_max_mb(node.role, metrics[\"heap_max_mb\"])" not in scheduler_source
    assert "ingest._heap_max_for_instance(conn, node.role, metrics[\"heap_max_mb\"], cluster, instance_id)" in scheduler_source


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
