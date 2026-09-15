from gcanalyzer import prometheus_settings, store, topology
import export_static


def test_export_inlines_assets_and_all_readonly_domains(tmp_path, monkeypatch):
    db = str(tmp_path / "export.db")
    store.init_db(db)
    monkeypatch.setattr(store, "DB_PATH", db)
    def no_private_settings():
        raise AssertionError("Static exports must not read private connection settings")
    monkeypatch.setattr(prometheus_settings, "describe", no_private_settings)
    inst = topology.Instance(id="c--b", region="EMEA", env="UAT", cluster="c", group="brokers", role="broker", index=1, heap_max_mb=1024, busy_hour_utc=0)
    with store.connect(db) as c:
        store.upsert_instance(c, inst, collector="G1")
        c.execute("UPDATE instances SET node_id=?", ("</script><script>alert(1)</script>",))
    out = tmp_path / "report.html"
    export_static.build(str(out))
    html = out.read_text()
    assert '<script src="/assets/' not in html
    assert '<link rel="stylesheet" href="/assets/' not in html
    assert "</script><script>alert(1)</script>" not in html
    assert '"/api/instance/c--b/sar"' in html
    assert '"/api/instance/c--b/forecast"' in html
    assert '"/api/cluster/c/scaling"' in html
    assert "Chart.js v" in html
