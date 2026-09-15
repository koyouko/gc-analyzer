import asyncio
import pytest


def test_lifespan_initializes_empty_database_and_stops_scheduler(monkeypatch, tmp_path):
    from fastapi.testclient import TestClient
    from gcanalyzer import app as api, store
    monkeypatch.setattr(store, "DB_PATH", str(tmp_path / "new.db"))
    monkeypatch.setattr(api, "CLUSTERS_DIR", str(tmp_path / "no-configs"))
    monkeypatch.setenv("GC_SCHED_ENABLED", "1")
    finished = []
    async def loop(*args, **kwargs):
        try:
            await asyncio.sleep(3600)
        finally:
            finished.append(True)
    monkeypatch.setattr(api.scheduler, "scheduler_loop", loop)
    with TestClient(api.app) as client:
        assert client.get("/api/health").json()["ok"] is True
    assert finished == [True]


def test_failed_health_has_failing_http_status(monkeypatch):
    from fastapi.testclient import TestClient
    from gcanalyzer import app as api, store
    def fail(*a, **k):
        raise OSError("private path")
    monkeypatch.setattr(store, "connect", fail)
    response = TestClient(api.app).get("/api/health")
    assert response.status_code == 503
    assert "private path" not in response.text
