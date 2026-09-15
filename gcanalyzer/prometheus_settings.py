"""Validated, private app-side Prometheus settings; no scrape-server writes."""

from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timezone
import fcntl
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import stat
import tempfile
import threading
import time
from urllib.parse import urlsplit, urlunsplit

from . import prometheus_client

ROOT = Path(__file__).resolve().parent.parent
MAX_CONFIG_BYTES = 64 * 1024
ROLES = {"broker", "connect", "schema-registry", "zookeeper", "controller", "other"}
_CACHE_LOCK = threading.Lock()
_LAST_TEST = {}
_TEST_SLOTS = threading.BoundedSemaphore(2)


class StorageError(ValueError):
    pass


class ConflictError(ValueError):
    pass


class BusyError(ValueError):
    pass


def defaults():
    return {
        "version": 1, "base_url": "", "authentication": "none",
        "timeout_seconds": 5, "scrape_interval_seconds": 60,
        "filters": {"job": [], "region": ["emea", "amer", "apac"],
                    "tier": ["dev", "uat", "prod", "sandbox", "stage"],
                    "infra": ["icp", "phy"], "az": [],
                    "service": ["kafka", "connect", "registry", "zookeeper"], "instance": []},
        "service_roles": {"kafka": "broker", "connect": "connect",
                          "registry": "schema-registry", "zookeeper": "zookeeper"},
    }


def config_path():
    path = Path(os.environ.get("GC_PROMETHEUS_CONFIG", str(ROOT / "prometheus.json"))).expanduser()
    return Path(os.path.abspath(path))


def _text(value, name, limit=255):
    if (not isinstance(value, str) or len(value) > limit
            or any(ord(c) < 32 or ord(c) == 127 for c in value)):
        raise ValueError(f"{name} must be text without control characters (maximum {limit} characters).")
    return value.strip()


def _url(value):
    value = _text(value, "Server URL", 2048)
    if not value:
        return ""
    try:
        parts = urlsplit(value)
        host, port = parts.hostname, parts.port
        if (parts.scheme not in ("http", "https") or not host or parts.username is not None
                or parts.password is not None or parts.query or parts.fragment
                or "?" in value or "#" in value or "\\" in value
                or any(c.isspace() for c in value) or (port is not None and not 1 <= port <= 65535)):
            raise ValueError()
        if ":" in host:
            ipaddress.IPv6Address(host)
        elif len(host) > 253 or not re.fullmatch(r"[A-Za-z0-9_.-]+", host):
            raise ValueError()
        path = parts.path.rstrip("/")
        if re.search(r"/api/v1(?:/|$)", path):
            raise ValueError()
        return urlunsplit((parts.scheme, parts.netloc, path, "", ""))
    except ValueError:
        raise ValueError("Enter an HTTP or HTTPS server base URL without credentials, query, fragment, or /api/v1 endpoint.") from None


def normalize(raw):
    result = defaults()
    if not isinstance(raw, dict) or set(raw) - set(result):
        raise ValueError("Configuration must contain only the supported Prometheus settings.")
    result.update(raw)
    if type(result["version"]) is not int or result["version"] != 1:
        raise ValueError("Configuration version must be 1.")
    result["base_url"] = _url(result["base_url"])
    if result["authentication"] != "none":
        raise ValueError("Only no-authentication Prometheus connections are supported.")
    for key, maximum in (("timeout_seconds", 30), ("scrape_interval_seconds", 3600)):
        if type(result[key]) is not int or not 1 <= result[key] <= maximum:
            raise ValueError(f"{key} must be a whole number between 1 and {maximum}.")
    filters = defaults()["filters"]
    submitted = result["filters"]
    if not isinstance(submitted, dict) or set(submitted) - set(filters):
        raise ValueError("Unknown Prometheus filter label.")
    filters.update(submitted)
    for key, values in filters.items():
        maximum = 256 if key == "instance" else 64
        if not isinstance(values, list) or len(values) > maximum:
            raise ValueError(f"Filter {key} must be a list with at most {maximum} values.")
        clean = [_text(value, f"Filter {key}") for value in values]
        if any(not value for value in clean):
            raise ValueError(f"Filter {key} cannot contain an empty value; use an empty list for no selection.")
        filters[key] = list(dict.fromkeys(clean))
    result["filters"] = filters
    roles = result["service_roles"]
    if not isinstance(roles, dict) or len(roles) > 32:
        raise ValueError("Service roles must be a mapping with at most 32 entries.")
    for service, role in roles.items():
        if not _text(service, "Service name") or service != service.strip() or not isinstance(role, str) or role not in ROLES:
            raise ValueError("Each service must map to broker, connect, schema-registry, zookeeper, controller, or other.")
    return deepcopy(result)


def _encoded(config):
    return (json.dumps(config, indent=2, ensure_ascii=True, allow_nan=False) + "\n").encode()


def _revision(config):
    return hashlib.sha256(json.dumps(config, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _load(path):
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
        with os.fdopen(fd, "rb") as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise StorageError("Prometheus configuration must be a regular file.")
            data = stream.read(MAX_CONFIG_BYTES + 1)
        if len(data) > MAX_CONFIG_BYTES:
            raise StorageError("Prometheus configuration exceeds the 64 KiB size limit.")
        return normalize(json.loads(data)), True
    except FileNotFoundError:
        return defaults(), False
    except (OSError, ValueError, UnicodeError, RecursionError) as exc:
        raise StorageError("Cannot read Prometheus configuration. Check the JSON, file permissions, and configuration path.") from exc


def _untested(revision):
    return {"status": "not_tested", "checked_at": None, "message": "Connection not tested.",
            "revision": revision, "latency_ms": None}


def describe():
    path = config_path()
    config, saved = _load(path)
    revision = _revision(config)
    with _CACHE_LOCK:
        cached = _LAST_TEST.get((str(path), revision))
        connection = deepcopy(cached) if cached else _untested(revision)
    return {"config": config, "config_path": str(path), "saved": saved,
            "configured": bool(saved and config["base_url"]), "revision": revision, "connection": connection}


@contextmanager
def _write_lock(path):
    fd = os.open(str(path) + ".lock", os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise StorageError("Prometheus configuration lock must be a regular file.")
        os.fchmod(fd, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        os.close(fd)


def save(raw, expected_revision):
    config = normalize(raw)
    data = _encoded(config)
    if len(data) > MAX_CONFIG_BYTES:
        raise ValueError("Prometheus configuration exceeds the 64 KiB size limit.")
    path = config_path()
    temporary = None
    try:
        with _write_lock(path):
            previous, _ = _load(path)
            if expected_revision != _revision(previous):
                raise ConflictError("Settings changed since this page loaded. Reload before saving.")
            fd, temporary = tempfile.mkstemp(prefix=".prometheus-", suffix=".tmp", dir=path.parent)
            with os.fdopen(fd, "wb") as stream:
                os.fchmod(stream.fileno(), 0o600)
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
            temporary = None
            with _CACHE_LOCK:
                _LAST_TEST.clear()
    except OSError as exc:
        raise StorageError("Cannot save Prometheus configuration. Check the directory and file permissions.") from exc
    finally:
        if temporary is not None:
            os.unlink(temporary)
    return describe()


def test_connection(expected_revision):
    before = describe()
    if expected_revision != before["revision"]:
        raise ConflictError("Settings changed. Reload and test the saved configuration.")
    if not before["configured"]:
        raise ValueError("Save a Prometheus server URL before testing the connection.")
    if not _TEST_SLOTS.acquire(blocking=False):
        raise BusyError("Two connection tests are already running. Try again shortly.")
    try:
        started = time.monotonic()
        answer = prometheus_client.probe(before["config"])
        result = {**answer, "checked_at": datetime.now(timezone.utc).isoformat(),
                  "revision": before["revision"], "latency_ms": round((time.monotonic() - started) * 1000)}
        current = describe()
        if current["revision"] != before["revision"] or current["config_path"] != before["config_path"]:
            raise ConflictError("Settings changed while the connection test was running. Test again.")
        with _CACHE_LOCK:
            _LAST_TEST.clear()
            _LAST_TEST[(before["config_path"], before["revision"])] = deepcopy(result)
        return {"connection": result}
    finally:
        _TEST_SLOTS.release()
