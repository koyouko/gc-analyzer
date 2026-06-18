"""
Tests for the topology -> store -> fleet rollup pipeline.

Seeds a small temporary history DB, then asserts the inventory shape, the
30-day trends, the "last hour" alerting, and the status rollup.

Run:  python -m tests.test_fleet
"""

import os
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

# Point the store at a temp DB BEFORE importing seed (so seeding writes there).
_TMP = os.path.join(tempfile.gettempdir(), "gc_history_test.db")
os.environ["GC_DB"] = _TMP

from gcanalyzer import store, fleet, topology  # noqa: E402
store.DB_PATH = _TMP
from seed import seed_history  # noqa: E402


def setup():
    seed_history.main(_TMP)


def test_inventory_shape():
    with store.connect(_TMP) as c:
        insts = store.list_instances(c)
    assert len(insts) == len(topology.build_instances())
    regions = {i["region"] for i in insts}
    assert regions == {"NAM", "EMEA", "APAC"}
    emea_envs = {i["env"] for i in insts if i["region"] == "EMEA"}
    assert emea_envs == {"UAT", "PROD", "SANDBOX", "PHY", "DEV"}, emea_envs
    nam_envs = {i["env"] for i in insts if i["region"] == "NAM"}
    assert nam_envs == {"UAT", "PROD"}, nam_envs


def test_trends_cover_30_days():
    with store.connect(_TMP) as c:
        tr = store.trends(c, "NAM-PROD-broker-1", days=30)
    assert 29 <= len(tr["series"]) <= 32, len(tr["series"])
    assert tr["heap_max_mb"] == 6144
    for pt in tr["series"]:
        assert pt["heap_used_avg"] <= tr["heap_max_mb"] + 1


def test_injected_incident_raises_last_hour_alert():
    with store.connect(_TMP) as c:
        alerts = store.evaluate_alerts(c, "EMEA-PROD-broker-2")
        types = {a["type"] for a in alerts}
    assert "full_gc" in types, types
    assert any(a["severity"] == "critical" for a in alerts)


def test_heap_pressure_alert():
    with store.connect(_TMP) as c:
        alerts = store.evaluate_alerts(c, "NAM-PROD-broker-4")
    assert any(a["type"] == "heap_pressure" for a in alerts), alerts


def test_old_incident_not_in_last_hour():
    # NAM-UAT-broker-1's Full GC storm was ~26h ago: must NOT alert "now".
    with store.connect(_TMP) as c:
        alerts = store.evaluate_alerts(c, "NAM-UAT-broker-1")
    assert not any(a["type"] == "full_gc" for a in alerts), alerts


def test_healthy_instance_is_ok():
    with store.connect(_TMP) as c:
        snap = store.current_snapshot(c, "APAC-UAT-zookeeper-1")
    assert snap["health"]["grade"] in ("A", "B")
    assert snap["alerts"] == []


def test_fleet_rollup_status():
    with store.connect(_TMP) as c:
        f = fleet.build_fleet(c)
    assert f["fleet_status"] == "critical"           # we injected critical incidents
    assert f["counts"]["critical"] >= 1
    assert f["total_instances"] == len(topology.build_instances())
    # EMEA-PROD must roll up to critical because of broker-2.
    emea = [r for r in f["regions"] if r["region"] == "EMEA"][0]
    prod = [e for e in emea["envs"] if e["env"] == "PROD"][0]
    assert prod["status"] == "critical", prod["status"]


def test_cluster_overview():
    with store.connect(_TMP) as c:
        v = fleet.build_cluster(c, "EMEA-PROD")
    assert v["counts"]["total"] == 16, v["counts"]
    assert v["counts"]["healthy"] + v["counts"]["unhealthy"] == 16
    assert v["counts"]["unhealthy"] >= 1            # broker-2 is critical
    assert v["status"] == "critical"
    # Aggregate memory must be sane.
    assert v["memory"]["total_heap_mb"] > 0
    assert 0 <= v["memory"]["avg_util_pct"] <= 100
    # Config telemetry present.
    assert "G1" in v["config"]["gc_engine"]
    assert "broker" in v["config"]["heap_by_role"]
    # Attention list contains only unhealthy nodes, critical first.
    assert all(n["status"] in ("critical", "watch") for n in v["attention"])
    assert any(n["id"] == "EMEA-PROD-broker-2" for n in v["attention"])


def test_healthy_cluster_has_empty_attention():
    with store.connect(_TMP) as c:
        v = fleet.build_cluster(c, "APAC-UAT")
    assert v["status"] == "ok"
    assert v["counts"]["unhealthy"] == 0
    assert v["attention"] == []


def test_fleet_renders_non_demo_topology():
    # Regression: build_fleet must render a real cluster whose instances are NOT
    # part of the seeded demo topology. It previously raised KeyError because the
    # tree was built from topology.topology_skeleton() (the 96-node demo fleet)
    # rather than from the instances actually in the store.
    import time

    db = os.path.join(tempfile.gettempdir(), "gc_live_test.db")
    if os.path.exists(db):
        os.remove(db)
    store.init_db(db)

    inst = topology.Instance(
        id="LOCAL-DEV-broker-1", region="LOCAL", env="DEV", cluster="LOCAL-DEV",
        group="brokers", role="broker", index=1, heap_max_mb=512, busy_hour_utc=0,
    )
    ts = (int(time.time()) // 60) * 60
    row = {
        "heap_used_mb": 42.0, "heap_max_mb": 512.0, "heap_after_pct": 8.2,
        "pause_avg_ms": 5.8, "pause_p99_ms": 27.0, "pause_max_ms": 37.0,
        "full_gc_count": 0, "young_count": 41, "gc_per_min": 83.0,
        "time_in_gc_pct": 0.8, "throughput_pct": 99.2,
    }
    with store.connect(db) as c:
        store.upsert_instance(c, inst, collector="G1")
        store.record_metric(c, inst.id, ts, row)

    with store.connect(db) as c:
        f = fleet.build_fleet(c)            # must not raise KeyError

    assert f["total_instances"] == 1, f["total_instances"]
    local = [r for r in f["regions"] if r["region"] == "LOCAL"]
    assert local, [r["region"] for r in f["regions"]]
    ids = [i["id"] for c in local[0]["envs"][0]["clusters"] for g in c["groups"] for i in g["instances"]]
    assert ids == ["LOCAL-DEV-broker-1"], ids


def run_all():
    setup()
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    passed = 0
    for fn in fns:
        try:
            fn(); print(f"PASS  {fn.__name__}"); passed += 1
        except AssertionError as e:
            print(f"FAIL  {fn.__name__}: {e}")
        except Exception as e:  # noqa: BLE001
            print(f"ERROR {fn.__name__}: {type(e).__name__}: {e}")
    print(f"\n{passed}/{len(fns)} tests passed")
    return passed == len(fns)


if __name__ == "__main__":
    sys.exit(0 if run_all() else 1)
