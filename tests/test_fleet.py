"""
Tests for the topology -> store -> fleet rollup pipeline.

Seeds a small temporary history DB, then asserts the inventory shape, the
30-day trends, the "last hour" alerting, and the status rollup.

Run:  python -m tests.test_fleet
"""

import os
import sys
import tempfile
import pytest

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
    assert regions == {"DEMO"}
    demo_envs = {i["env"] for i in insts if i["region"] == "DEMO"}
    assert demo_envs == {"KRAFT", "ZOOKEEPER"}, demo_envs
    clusters = {i["cluster"] for i in insts}
    assert clusters == {"DEMO-KRAFT", "DEMO-ZK"}, clusters


def test_trends_cover_30_days():
    with store.connect(_TMP) as c:
        tr = store.trends(c, "DEMO-KRAFT--broker-1", days=30)
    assert 29 <= len(tr["series"]) <= 32, len(tr["series"])
    assert tr["heap_max_mb"] == 6144
    for pt in tr["series"]:
        assert pt["heap_used_avg"] <= tr["heap_max_mb"] + 1


def test_injected_incident_raises_last_hour_alert():
    with store.connect(_TMP) as c:
        alerts = store.evaluate_alerts(c, "DEMO-KRAFT--broker-2")
        types = {a["type"] for a in alerts}
    assert "full_gc" in types, types
    assert any(a["severity"] == "critical" for a in alerts)


def test_heap_pressure_alert():
    with store.connect(_TMP) as c:
        alerts = store.evaluate_alerts(c, "DEMO-ZK--broker-1")
    assert any(a["type"] == "heap_pressure" for a in alerts), alerts


def test_old_incident_not_in_last_hour():
    # DEMO-ZK--broker-3 Full GC storm was ~26h ago: must NOT alert "now".
    with store.connect(_TMP) as c:
        alerts = store.evaluate_alerts(c, "DEMO-ZK--broker-3")
    assert not any(a["type"] == "full_gc" for a in alerts), alerts


def test_healthy_instance_is_ok():
    with store.connect(_TMP) as c:
        snap = store.current_snapshot(c, "DEMO-ZK--zookeeper-1")
    assert snap["health"]["grade"] in ("A", "B")
    assert snap["alerts"] == []


def test_fleet_rollup_status():
    with store.connect(_TMP) as c:
        f = fleet.build_fleet(c)
    assert f["fleet_status"] == "critical"           # we injected critical incidents
    assert f["counts"]["critical"] >= 1
    assert f["total_instances"] == len(topology.build_instances())
    # DEMO-KRAFT must roll up to critical because of broker-2.
    demo = [r for r in f["regions"] if r["region"] == "DEMO"][0]
    kraft = [e for e in demo["envs"] if e["env"] == "KRAFT"][0]
    assert kraft["status"] == "critical", kraft["status"]


def test_cluster_overview():
    with store.connect(_TMP) as c:
        v = fleet.build_cluster(c, "DEMO-KRAFT")
    assert v["counts"]["total"] == 10, v["counts"]
    assert v["counts"]["healthy"] + v["counts"]["unhealthy"] == 10
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
    assert any(n["id"] == "DEMO-KRAFT--broker-2" for n in v["attention"])


def test_zookeeper_cluster_has_healthy_ensemble():
    with store.connect(_TMP) as c:
        for i in (1, 2, 3):
            snap = store.current_snapshot(c, f"DEMO-ZK--zookeeper-{i}")
            assert snap["health"]["grade"] in ("A", "B"), snap
            assert snap["alerts"] == []


def test_fleet_renders_non_demo_topology():
    # Regression: build_fleet must render a real cluster whose instances are NOT
    # part of the seeded demo topology. It previously raised KeyError because the
    # tree was built from topology.topology_skeleton() (instances actually in the store)
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


def test_startup_inventory_sync_does_not_prune_unconfigured_data(tmp_path, monkeypatch):
    """Startup config sync should not delete telemetry from an empty config dir."""
    pytest.importorskip("fastapi")
    from gcanalyzer import app as app_mod  # noqa: E402

    db = os.path.join(tmp_path, "gc_prune_guard.db")
    store.init_db(db)
    inst = topology.Instance(
        id="LOCAL-DEV--broker-1",
        region="LOCAL",
        env="DEV",
        cluster="LOCAL-DEV",
        group="brokers",
        role="broker",
        index=1,
        heap_max_mb=512,
        busy_hour_utc=0,
        node_id="broker-1",
    )
    with store.connect(db) as c:
        store.upsert_instance(c, inst, collector="G1")

    clusters_dir = os.path.join(tmp_path, "clusters")
    os.makedirs(clusters_dir)
    monkeypatch.setattr(app_mod, "CLUSTERS_DIR", clusters_dir)
    monkeypatch.setattr(app_mod.store, "DB_PATH", db)

    app_mod._sync_cluster_inventory_from_configs()

    with store.connect(db) as c:
        assert store.get_instance(c, "LOCAL-DEV--broker-1") is not None


def test_save_cluster_config_accepts_config_key_with_display_name(tmp_path, monkeypatch):
    """Editing KafkaCluster.yaml should accept cluster: Kafka Cluster."""
    pytest.importorskip("fastapi")
    pytest.importorskip("yaml")
    from gcanalyzer import app as app_mod  # noqa: E402

    db = os.path.join(tmp_path, "gc_edit_key.db")
    clusters_dir = os.path.join(tmp_path, "clusters")
    os.makedirs(clusters_dir)
    store.init_db(db)
    yaml_text = """
cluster: Kafka Cluster
region: LOCAL
env: DEV
nodes:
  - id: broker-1
    role: broker
    source: local
    local_paths: []
"""
    with open(os.path.join(clusters_dir, "KafkaCluster.yaml"), "w") as fh:
        fh.write(yaml_text)

    monkeypatch.setattr(app_mod, "CLUSTERS_DIR", clusters_dir)
    monkeypatch.setattr(app_mod.store, "DB_PATH", db)

    result = app_mod.save_cluster_config(
        "KafkaCluster",
        app_mod.ClusterConfigBody(config=yaml_text, format="yaml"),
    )

    assert result["cluster"] == "Kafka Cluster"
    with store.connect(db) as c:
        assert store.get_instance(c, "Kafka Cluster--broker-1") is not None


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
