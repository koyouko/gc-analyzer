"""Tests for Kafka 2.8.1 & ZooKeeper log/JMX analyzer, real-time active alerter,
ensemble ML model training, TypeSafe Jev primitives, and Local LLM diagnostics."""

from __future__ import annotations

from fastapi.testclient import TestClient

from gcanalyzer import app as app_mod, auth, kafka_analyzer, kafka_ingest, ml_insights, store, topology


def test_parse_kafka_281_and_zookeeper_logs():
    sample = (
        "[2026-10-22 21:00:01,112] INFO [KafkaController id=1] Elected as active controller for epoch 12 (kafka.controller.KafkaController)\n"
        "[2026-10-22 21:00:05,220] WARN [Partition orders-0 broker=1] Shrinking ISR from 1,2,3 to 1,3 (kafka.cluster.Partition)\n"
        "[2026-10-22 21:00:08,500] ERROR [ReplicaManager broker=1] Under-replicated partitions count=4 on topic orders-0 (kafka.server.ReplicaManager)\n"
        "[2026-10-22 21:00:10,900] WARN [SyncThread:1:FileTxnLog@394] - fsync-ing the write ahead log in SyncThread:1 took 1480ms which will adversely effect operation latency.\n"
        "[2026-10-22 21:00:15,100] WARN [NIOServerCnxnFactory:ZooKeeperServer@1112] - Expiring session 0x1004fa29b1c0002, timeout of 18000ms exceeded\n"
    )
    parsed = kafka_analyzer.parse_kafka_logs(sample, node_id="broker-1", role="broker")
    assert parsed.counts["controller_election"] == 1
    assert parsed.counts["isr_shrink"] == 1
    assert parsed.counts["urp_detected"] == 1
    assert parsed.counts["zk_fsync_stall"] == 1
    assert parsed.counts["zk_session_expired"] == 1
    assert parsed.max_fsync_ms == 1480.0
    assert "0x1004fa29b1c0002" in parsed.expired_sessions


def test_realtime_alerter_flags_host_damage_and_kafka_zk_emergencies(tmp_path):
    db = str(tmp_path / "rt.db")
    store.init_db(db)
    now = 1700000000
    b1 = topology.Instance(
        id="DEMO-ZK--broker-1", region="DEMO", env="ZOOKEEPER", cluster="DEMO-ZK",
        group="brokers", role="broker", index=1, heap_max_mb=8192, busy_hour_utc=14, node_id="broker-1"
    )
    zk1 = topology.Instance(
        id="DEMO-ZK--zookeeper-1", region="DEMO", env="ZOOKEEPER", cluster="DEMO-ZK",
        group="zookeeper", role="zookeeper", index=1, heap_max_mb=4096, busy_hour_utc=14, node_id="zookeeper-1"
    )
    with store.connect(db) as c:
        store.upsert_instance(c, b1, collector="G1")
        store.upsert_instance(c, zk1, collector="G1")
        store.record_host_metric(c, b1.id, now, {
            "sample_count": 6,
            "cpu_busy_pct_avg": 94.0,
            "cpu_busy_pct_max": 98.5,
            "mem_used_pct_avg": 92.0,
            "mem_used_pct_max": 96.0,
            "swap_used_pct_max": 6.5,
            "disk_util_pct_max": 95.0,
            "disk_await_ms_max": 62.0,
        })
        store.record_kafka_metric(c, b1.id, now, {
            "under_replicated_partitions": 9,
            "offline_partitions_count": 1,
            "request_handler_avg_idle_pct": 12.0,
            "produce_latency_p99_ms": 340.0,
        })
        store.record_kafka_metric(c, zk1.id, now, {
            "zk_server_state": "leader",
            "zk_avg_latency_ms": 58.0,
            "zk_fsync_warning_count": 3,
            "zk_session_expire_count": 2,
        })
        rt = store.evaluate_realtime_alerts(c, cluster="DEMO-ZK", now=now)

    assert rt["status"] == "critical"
    assert rt["critical_count"] >= 5
    assert rt["by_layer"]["host"] >= 3
    assert rt["by_layer"]["kafka"] >= 2
    assert rt["by_layer"]["zookeeper"] >= 1
    categories = {a["category"] for a in rt["alerts"]}
    assert "cpu_damage" in categories
    assert "memory_damage" in categories
    assert "urp" in categories
    assert "offline_partitions" in categories
    assert "zk_quorum_stall" in categories


def test_ml_training_jev_judgments_and_local_llm_diagnosis(tmp_path):
    db = str(tmp_path / "ml.db")
    store.init_db(db)
    now = 1700000000
    inst = topology.Instance(
        id="DEMO-ZK--broker-1", region="DEMO", env="ZOOKEEPER", cluster="DEMO-ZK",
        group="brokers", role="broker", index=1, heap_max_mb=8192, busy_hour_utc=14, node_id="broker-1"
    )
    with store.connect(db) as c:
        store.upsert_instance(c, inst, collector="G1")
        for h in range(48):
            ts = now - (48 - h) * 3600
            is_spike = h >= 44
            store.record_metric(c, inst.id, ts, {
                "heap_used_mb": 7200 if is_spike else 3200,
                "heap_max_mb": 8192,
                "heap_after_pct": 88.0 if is_spike else 39.0,
                "pause_avg_ms": 180.0 if is_spike else 22.0,
                "pause_p99_ms": 620.0 if is_spike else 55.0,
                "pause_max_ms": 950.0 if is_spike else 85.0,
                "full_gc_count": 3 if is_spike else 0,
                "young_count": 40,
                "gc_per_min": 8.0,
                "time_in_gc_pct": 18.0 if is_spike else 0.9,
                "throughput_pct": 82.0 if is_spike else 99.1,
            })
            store.record_host_metric(c, inst.id, ts, {
                "sample_count": 6,
                "cpu_busy_pct_avg": 91.0 if is_spike else 26.0,
                "cpu_busy_pct_max": 96.0 if is_spike else 34.0,
                "cpu_iowait_pct_avg": 14.0 if is_spike else 1.5,
                "mem_used_pct_avg": 89.0 if is_spike else 54.0,
                "mem_used_pct_max": 93.0 if is_spike else 58.0,
                "swap_used_pct_max": 2.0 if is_spike else 0.0,
                "disk_util_pct_max": 85.0 if is_spike else 18.0,
                "disk_await_ms_max": 42.0 if is_spike else 3.0,
                "net_util_pct_max": 72.0 if is_spike else 20.0,
            })
            store.record_kafka_metric(c, inst.id, ts, {
                "under_replicated_partitions": 6 if is_spike else 0,
                "produce_latency_p99_ms": 220.0 if is_spike else 18.0,
                "request_handler_avg_idle_pct": 16.0 if is_spike else 76.0,
                "zk_avg_latency_ms": 32.0 if is_spike else 2.0,
                "zk_fsync_warning_count": 2 if is_spike else 0,
            })

        trained = ml_insights.train_instance_model(c, inst.id, days=30, now=now)
        assert trained["trained"] is True
        assert trained["features_trained"] >= 10

        anom = ml_insights.analyze_anomalies(c, inst.id, days=30, recent_hours=24, now=now)
        assert anom["model_status"]["persistent_model"] is True
        assert "ewma_drift" in anom

        diag = ml_insights.diagnose_with_local_llm(c, inst.id, now=now)
        assert "jev" in diag
        j = diag["jev"]["judgments"]
        assert j["primary_root_cause"]["primitive"] == "Choice"
        assert j["operational_risk_score"]["primitive"] == "Score"
        assert j["urp_cascade_risk"]["primitive"] == "Noul"
        assert 0.0 <= j["urp_cascade_risk"]["probability"] <= 1.0
        assert len(diag["narrative"]) > 20


def test_kafka_and_alerter_api_endpoints(tmp_path, monkeypatch):
    import time as _time
    db = str(tmp_path / "api.db")
    monkeypatch.setattr(store, "DB_PATH", db)
    store.init_db(db)
    now = int(_time.time())
    inst = topology.Instance(
        id="DEMO-ZK--broker-1", region="DEMO", env="ZOOKEEPER", cluster="DEMO-ZK",
        group="brokers", role="broker", index=1, heap_max_mb=8192, busy_hour_utc=14, node_id="broker-1"
    )
    with store.connect(db) as c:
        store.upsert_instance(c, inst, collector="G1")
        store.record_kafka_metric(c, inst.id, now, {"under_replicated_partitions": 3, "active_controller_count": 1})

    client = TestClient(app_mod.app)
    token = auth.make_session("admin", "admin")
    client.cookies.set("gc_session", token)

    r_rt = client.get("/api/alerts/realtime")
    assert r_rt.status_code == 200
    assert r_rt.headers.get("X-Content-Type-Options") == "nosniff"
    assert r_rt.json()["total_count"] >= 1

    r_ck = client.get("/api/cluster/DEMO-ZK/kafka")
    assert r_ck.status_code == 200
    assert r_ck.json()["summary"]["total_urp"] == 3

    r_log = client.post(
        "/api/instance/DEMO-ZK--broker-1/kafka/logs",
        json={"content": "[2026-10-22 21:00:05,220] WARN [Partition orders-0 broker=1] Shrinking ISR from 1,2,3 to 1,3 (kafka.cluster.Partition)"},
    )
    assert r_log.status_code == 200
    assert r_log.json()["events_parsed"] == 1
