import copy
import json
import os
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import pytest

from gcanalyzer import prometheus_client as client, prometheus_settings as settings


@pytest.fixture
def config_path(tmp_path, monkeypatch):
    path = tmp_path / "prometheus.json"
    monkeypatch.setenv("GC_PROMETHEUS_CONFIG", str(path))
    return path


def configured():
    return dict(settings.defaults(), base_url="http://prometheus.test:3020")


def save_initial():
    return settings.save(configured(), settings.describe()["revision"])


def test_defaults_do_not_write_or_contact_prometheus(config_path):
    data = settings.describe()
    assert not config_path.exists()
    assert data["configured"] is False and data["saved"] is False
    assert data["config_path"] == str(config_path)
    assert data["config"]["authentication"] == "none"
    assert data["config"]["scrape_interval_seconds"] == 60
    assert data["config"]["filters"]["tier"] == ["dev", "uat", "prod", "sandbox", "stage"]
    assert data["config"]["service_roles"]["registry"] == "schema-registry"
    assert data["connection"]["status"] == "not_tested"


def test_save_is_private_persistent_and_revision_checked(config_path):
    before = settings.describe()
    after = settings.save(configured(), before["revision"])
    assert after["configured"] and after["saved"]
    assert json.loads(config_path.read_text()) == after["config"]
    assert os.stat(config_path).st_mode & 0o777 == 0o600
    assert settings.describe()["revision"] == after["revision"]
    with pytest.raises(settings.ConflictError):
        settings.save(configured(), before["revision"])
    assert json.loads(config_path.read_text()) == after["config"]


@pytest.mark.parametrize("url", ["file:///etc/passwd", "ftp://example.test", "http://u:p@example.test",
                                    "http://example.test?token=x", "http://example.test/#x",
                                    "http://example.test:99999", "http://", "http://example.test/\nfoo",
                                    "http://example.test\\@other.test", "http://example.test/api/v1/query"])
def test_invalid_urls_rejected(url):
    with pytest.raises(ValueError):
        settings.normalize(dict(configured(), base_url=url))


@pytest.mark.parametrize("field,value", [("timeout_seconds", 0), ("timeout_seconds", True),
                                        ("timeout_seconds", 31), ("scrape_interval_seconds", 0),
                                        ("scrape_interval_seconds", 3601), ("authentication", "basic"),
                                        ("version", 2), ("version", True), ("unknown", "value")])
def test_invalid_settings_rejected(field, value):
    with pytest.raises(ValueError):
        settings.normalize(dict(configured(), **{field: value}))


def test_labels_preserve_cross_region_topology_and_exact_values():
    raw = configured()
    raw["filters"] = dict(raw["filters"], job=["bvp-amer-uat"], region=["emea"],
                          instance=["broker.test:39092", "broker.test:32181"])
    normalized = settings.normalize(raw)
    assert normalized["filters"] == raw["filters"]
    assert settings.normalize(dict(raw, base_url="https://prometheus.test/monitor/"))["base_url"] == "https://prometheus.test/monitor"


@pytest.mark.parametrize("change", [{"filters": {"typo": []}}, {"filters": {"job": "bvp"}},
                                  {"filters": {"job": ["x"] * 65}}, {"filters": {"az": ["\x00"]}},
                                  {"service_roles": {"kafka": "bogus"}}])
def test_bad_label_and_role_configuration_rejected(change):
    with pytest.raises(ValueError):
        settings.normalize(dict(configured(), **change))


def test_atomic_write_failure_preserves_existing_file(config_path, monkeypatch):
    saved = save_initial()
    original = config_path.read_bytes()
    def fail(*args):
        raise OSError("replace failed")
    monkeypatch.setattr(settings.os, "replace", fail)
    with pytest.raises(settings.StorageError):
        settings.save(dict(configured(), timeout_seconds=8), saved["revision"])
    assert config_path.read_bytes() == original
    assert not list(config_path.parent.glob(".prometheus-*.tmp"))


def test_invalid_existing_file_is_not_silently_replaced(config_path):
    config_path.write_text("{broken")
    with pytest.raises(settings.StorageError):
        settings.describe()
    with pytest.raises(settings.StorageError):
        settings.save(configured(), "anything")
    assert config_path.read_text() == "{broken"


def test_symlink_and_oversized_config_rejected(config_path):
    target = config_path.parent / "real.json"
    target.write_text("{}")
    config_path.symlink_to(target)
    with pytest.raises(settings.StorageError):
        settings.describe()
    config_path.unlink()
    config_path.write_bytes(b" " * (settings.MAX_CONFIG_BYTES + 1))
    with pytest.raises(settings.StorageError):
        settings.describe()


def test_test_only_saved_revision_and_invalidate_on_change(config_path, monkeypatch):
    saved = save_initial()
    calls = []
    def probe(config):
        calls.append(copy.deepcopy(config))
        return {"status": "connected", "message": "Query API responded."}
    monkeypatch.setattr(client, "probe", probe)
    result = settings.test_connection(saved["revision"])
    assert result["connection"]["status"] == "connected"
    assert len(calls) == 1 and calls[0] == saved["config"]
    assert settings.describe()["connection"]["checked_at"]
    updated = settings.save(dict(configured(), timeout_seconds=6), saved["revision"])
    assert updated["connection"]["status"] == "not_tested"
    with pytest.raises(settings.ConflictError):
        settings.test_connection(saved["revision"])
    assert len(calls) == 1


def test_config_change_during_test_cannot_mark_new_config_connected(config_path, monkeypatch):
    saved = save_initial()
    def probe(config):
        settings.save(dict(config, timeout_seconds=7), saved["revision"])
        return {"status": "connected", "message": "Query API responded."}
    monkeypatch.setattr(client, "probe", probe)
    with pytest.raises(settings.ConflictError):
        settings.test_connection(saved["revision"])
    assert settings.describe()["connection"]["status"] == "not_tested"


@pytest.fixture
def prometheus_server():
    state = {"code": 200, "body": {"status": "success", "data": {"resultType": "vector", "result": [{"metric": {}, "value": [1700000000, "1"]}]}}, "paths": []}
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            state["paths"].append(self.path)
            self.send_response(state["code"])
            self.send_header("Content-Type", "application/json")
            if state["code"] == 302:
                self.send_header("Location", "/redirect-destination")
            self.end_headers()
            body = state["body"]
            self.wfile.write(body if isinstance(body, bytes) else json.dumps(body).encode())
        def log_message(self, *args):
            pass
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", state
    finally:
        server.shutdown()
        server.server_close()
        worker.join()


def test_real_probe_is_read_only_and_validates_query_api(prometheus_server):
    url, state = prometheus_server
    result = client.probe(dict(configured(), base_url=url + "/prom"))
    assert result["status"] == "connected"
    query = urlsplit(state["paths"][0])
    assert query.path == "/prom/api/v1/query"
    assert parse_qs(query.query)["query"] == ["vector(1)"]
    assert len(state["paths"]) == 1


@pytest.mark.parametrize("code,body", [(302, {}), (401, {}), (503, {}), (200, b"<html>login</html>"),
                                    (200, {"status": "error", "error": "failure"}),
                                    (200, {"status": "success", "data": {}}),
                                    (200, {"status": "success", "data": {"resultType": "vector", "result": []}})])
def test_probe_failure_is_not_connected_and_redirects_are_not_followed(prometheus_server, code, body):
    url, state = prometheus_server
    state.update(code=code, body=body)
    assert client.probe(dict(configured(), base_url=url))["status"] == "failed"
    assert len(state["paths"]) == 1


def test_probe_bounded_body(prometheus_server):
    url, state = prometheus_server
    state["body"] = b" " * (client.MAX_RESPONSE_BYTES + 1)
    assert client.probe(dict(configured(), base_url=url))["status"] == "failed"


def test_process_deadline_bounds_dns_and_network(monkeypatch):
    def timeout(command, **kwargs):
        assert kwargs["timeout"] <= 6
        assert "shell" not in kwargs
        assert "prometheus.test" not in " ".join(command)
        raise subprocess.TimeoutExpired(command, kwargs["timeout"])
    monkeypatch.setattr(client.subprocess, "run", timeout)
    result = client.probe(configured())
    assert result["status"] == "failed" and "timed out" in result["message"]


def test_api_admin_only_and_save_does_not_query(config_path, monkeypatch):
    from fastapi.testclient import TestClient
    from gcanalyzer import app as api, auth
    monkeypatch.setenv("GC_SESSION_SECRET", "prometheus-settings-test")
    http = TestClient(api.app)
    routes = [("GET", "/api/settings/prometheus"), ("PUT", "/api/settings/prometheus"), ("POST", "/api/settings/prometheus/test")]
    for method, path in routes:
        assert http.request(method, path, json={}).status_code == 401
    http.cookies.set("gc_session", auth.make_session("reader", "readonly"))
    for method, path in routes:
        assert http.request(method, path, json={}).status_code == 403
    http.cookies.set("gc_session", auth.make_session("admin", "admin"))
    def unexpected(config):
        pytest.fail("save contacted Prometheus")
    monkeypatch.setattr(client, "probe", unexpected)
    data = http.get("/api/settings/prometheus").json()
    response = http.put("/api/settings/prometheus", json={"config": configured(), "revision": data["revision"]})
    assert response.status_code == 200 and response.json()["configured"]
    assert http.put("/api/settings/prometheus", json={"config": configured(), "revision": data["revision"]}).status_code == 409
    assert http.put("/api/settings/prometheus", json={"config": dict(configured(), timeout_seconds=True), "revision": response.json()["revision"]}).status_code == 400
    monkeypatch.setattr(client, "probe", lambda c: {"status": "failed", "message": "Cannot resolve the Prometheus host from this server."})
    tested = http.post("/api/settings/prometheus/test", json={"revision": response.json()["revision"]})
    assert tested.status_code == 200 and tested.json()["connection"]["status"] == "failed"
    assert http.put("/api/settings/prometheus", content=b"x" * 65537).status_code == 413
