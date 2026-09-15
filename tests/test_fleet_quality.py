from gcanalyzer import fleet, store, topology


def test_unknown_grade_is_not_healthy():
    unknown = {"score": None, "grade": "?", "status": "unknown", "reasons": []}
    assert fleet._instance_status(unknown, []) == "unknown"
    assert fleet._host_status(unknown) == "unknown"
    assert fleet._instance_status(unknown, [{"severity":"critical"}]) == "critical"
    assert fleet._instance_status({"grade":"F"}, [{"severity":"warning"}]) == "critical"


def test_rollup_preserves_incomplete_coverage():
    assert fleet._rollup(["ok", "unknown"]) == "unknown"
    assert fleet._rollup(["ok", "ok"]) == "ok"
    assert fleet._rollup(["critical", "unknown"]) == "critical"


def test_null_counts_do_not_crash_current_fleet(tmp_path):
    db = str(tmp_path / "null.db")
    store.init_db(db)
    inst = topology.Instance(id="c--b", region="EMEA", env="UAT", cluster="c", group="brokers", role="broker", index=1, heap_max_mb=1024, busy_hour_utc=0)
    with store.connect(db) as c:
        store.upsert_instance(c, inst, collector="G1")
        c.execute("INSERT INTO metrics (ts,instance_id,pause_max_ms) VALUES(?,?,?)", (2000000000,inst.id,12))
        f = fleet.build_fleet(c, now=2000000000)
        cl = fleet.build_cluster(c, "c", now=2000000000)
    assert f["counts"]["unknown"] == 1
    assert cl["nodes"][0]["full_gc_1h"] is None
    assert cl["telemetry"]["full_gc_24h"] is None


def test_missing_sources_do_not_report_zero_pressure(tmp_path):
    db = str(tmp_path / "fleet.db")
    store.init_db(db)
    inst = topology.Instance(id="c--b", region="EMEA", env="UAT", cluster="c", group="brokers", role="broker", index=1, heap_max_mb=1024, busy_hour_utc=0)
    with store.connect(db) as c:
        store.upsert_instance(c, inst, collector="G1")
        result = fleet.build_cluster(c, "c", now=2000000000)
    assert result["counts"]["unknown"] == 1
    assert result["host_summary"]["cpu_busy_avg"] is None
    assert result["host_summary"]["disk_util_worst"] is None
    assert result["memory"]["used_mb"] is None
    assert result["telemetry"]["worst_pause_ms"] is None
    assert result["nodes"][0]["quality"]["state"] == "missing"


def test_stale_data_never_counts_as_current_health(tmp_path):
    db = str(tmp_path / "fleet.db")
    store.init_db(db)
    inst = topology.Instance(id="c--b", region="EMEA", env="UAT", cluster="c", group="brokers", role="broker", index=1, heap_max_mb=1024, busy_hour_utc=0)
    with store.connect(db) as c:
        store.upsert_instance(c, inst, collector="G1")
        store.record_metric(c, inst.id, 1000000000, {"heap_used_mb":100, "heap_max_mb":1024, "heap_after_pct":10,
            "pause_avg_ms":1, "pause_p99_ms":1, "pause_max_ms":1, "full_gc_count":0, "young_count":1,
            "gc_per_min":1, "time_in_gc_pct":0.1, "throughput_pct":99.9})
        cluster = fleet.build_cluster(c, "c", now=2000000000)
        overview = fleet.build_fleet(c, now=2000000000)
    assert cluster["counts"]["unknown"] == 1
    assert overview["counts"]["unknown"] == 1
    assert cluster["nodes"][0]["quality"]["state"] == "stale"
    assert cluster["memory"]["used_mb"] is None


def test_host_series_preserves_available_memory(tmp_path):
    db = str(tmp_path / "memory.db")
    store.init_db(db)
    with store.connect(db) as c:
        store.record_host_metric(c, "x", 2000000000, {"sample_count":1,"mem_avail_mb_avg":2345})
        series = store.host_range_series(c,"x",1999999000,2000000000,60)["series"]
    assert series[0]["mem_avail_avg"] == 2345


def test_bad_collection_excludes_previous_values_from_current_aggregates(tmp_path):
    db = str(tmp_path / "quality.db")
    store.init_db(db)
    inst = topology.Instance(id="c--b", region="EMEA", env="UAT", cluster="c", group="brokers", role="broker", index=1, heap_max_mb=1024, busy_hour_utc=0)
    with store.connect(db) as c:
        store.upsert_instance(c, inst, collector="G1")
        store.record_metric(c, inst.id, 2000000000, {"heap_used_mb":100, "heap_max_mb":1024, "heap_after_pct":10,
            "pause_avg_ms":1, "pause_p99_ms":1, "pause_max_ms":1, "full_gc_count":0, "young_count":1,
            "gc_per_min":1, "time_in_gc_pct":0.1, "throughput_pct":99.9})
        store.record_gc_collection_quality(c,inst.id,2000000060,"unknown",malformed_count=1)
        cluster = fleet.build_cluster(c,"c",now=2000000060)
        overview = fleet.build_fleet(c,now=2000000060)
    assert cluster["counts"]["unknown"] == overview["counts"]["unknown"] == 1
    assert cluster["memory"]["used_mb"] is None
    assert cluster["nodes"][0]["heap_after_pct"] is None
