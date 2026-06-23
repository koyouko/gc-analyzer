"""
Verification tests for the parse -> analyze pipeline.

Run:  python -m tests.test_pipeline      (from the project root)
or:   pytest -q                           (pytest optional)

These assert the parser recovers the structure we synthesised and that the
analyzer's health judgements line up with each node's designed profile.
"""

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from gcanalyzer import parser, analyzer  # noqa: E402

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
