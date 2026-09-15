import base64
import gzip
import json

import pytest

from gcanalyzer import investigations


GC = ('[1.000s][info][gc] GC(0) Pause Young (Normal) (G1 Evacuation Pause) 80M->20M(100M) 10.000ms\n'
      '[61.000s][info][gc] GC(1) Pause Young (Normal) (G1 Evacuation Pause) 80M->21M(100M) 12.000ms\n')


def payload(content=GC, **extra):
    return {"files": [{"name": "gc.log", "content": content}], **extra}


def test_relative_log_stays_relative_and_is_not_persisted(monkeypatch):
    from gcanalyzer import store
    monkeypatch.setattr(store, "connect", lambda *a, **k: pytest.fail("investigation wrote to fleet store"))
    result = investigations.analyze_upload(payload())
    assert result["persisted"] is False
    assert result["results"][0]["time_basis"] == "relative"
    assert result["results"][0]["analysis"]["metrics"]["event_count"] == 2
    assert result["correlation"]["status"] == "unavailable"
    json.dumps(result, allow_nan=False)


def test_gzip_and_explicit_utc_anchor():
    data = payload(start_time="2026-09-12T00:00:00Z")
    data["files"][0].update(content=base64.b64encode(gzip.compress(GC.encode())).decode(), encoding="gzip-base64")
    result = investigations.analyze_upload(data)["results"][0]
    assert result["time_basis"] == "utc"
    assert result["analysis"]["timeline"][0]["t"] == 1789171201


def test_grouping_is_explicit_and_deduplicates_rotation_overlap():
    data = {"files": [{"name": "gc.log.1", "content": GC}, {"name": "gc.log", "content": GC}]}
    assert len(investigations.analyze_upload(data)["results"]) == 2
    for file in data["files"]:
        file["group"] = "broker-1"
    result = investigations.analyze_upload(data)
    assert len(result["results"]) == 1
    assert result["results"][0]["analysis"]["metrics"]["event_count"] == 2


@pytest.mark.parametrize("data", [{}, {"files": []}, payload(start_time="2026-09-12T00:00:00"),
                                    payload(start_time="yesterday"), {"files": [None]},
                                    {"files": [{"name": "gc", "content": "bad", "encoding": "zip"}]}])
def test_invalid_inputs_rejected(data):
    with pytest.raises(ValueError):
        investigations.analyze_upload(data)


def test_bad_text_is_unknown_not_healthy():
    result = investigations.analyze_upload(payload("not a gc log"))["results"][0]
    assert result["analysis"]["health"]["status"] == "unknown"
    assert result["quality"]["state"] == "missing"
    assert result["analysis"]["metrics"]["max_pause_ms"] is None


def test_gzip_expansion_limit(monkeypatch):
    monkeypatch.setattr(investigations, "MAX_FILE_BYTES", 100)
    data = payload()
    data["files"][0].update(content=base64.b64encode(gzip.compress(b"x" * 101)).decode(), encoding="gzip-base64")
    with pytest.raises(ValueError, match="limit"):
        investigations.analyze_upload(data)


def test_sar_cannot_be_assigned_to_multiple_jvms():
    data = {"files": [{"name": "a", "content": GC}, {"name": "b", "content": GC}], "sar": {"content": "{}", "name": "sar.json"}}
    with pytest.raises(ValueError, match="one JVM"):
        investigations.analyze_upload(data)


def test_sar_timezone_is_explicit_and_validated():
    data = payload(start_time="2026-09-12T00:00:00Z", report_timezone="not/a-zone",
                   sar={"name":"sar.txt", "content":""})
    with pytest.raises(ValueError, match="timezone"):
        investigations.analyze_upload(data)


def test_api_auth_bounds_and_no_history_writes(monkeypatch, tmp_path):
    from fastapi.testclient import TestClient
    from gcanalyzer import app as api, auth, store
    db = str(tmp_path / "test.db")
    store.init_db(db)
    monkeypatch.setattr(store, "DB_PATH", db)
    monkeypatch.setenv("GC_SESSION_SECRET", "isolated-test-secret")
    client = TestClient(api.app)
    assert client.post("/api/investigations/analyze", json=payload()).status_code == 401
    client.cookies.set("gc_session", auth.make_session("reviewer", "readonly"))
    assert client.post("/api/investigations/analyze", json=payload()).status_code == 200
    assert client.post("/api/investigations/analyze", json={}).status_code == 400
    with store.connect(db) as c:
        assert c.execute("SELECT COUNT(*) FROM metrics").fetchone()[0] == 0
    monkeypatch.setattr(investigations, "MAX_REQUEST_BYTES", 32)
    assert client.post("/api/investigations/analyze", content=b"x" * 33).status_code == 413


def test_local_chart_asset_available():
    from fastapi.testclient import TestClient
    from gcanalyzer.app import app
    assert TestClient(app).get("/assets/vendor/chart.umd.js").status_code == 200
