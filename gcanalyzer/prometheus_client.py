"""Read-only Prometheus API probe, isolated to enforce a total deadline."""

from __future__ import annotations

import json
import math
from pathlib import Path
import socket
import ssl
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request

MAX_RESPONSE_BYTES = 64 * 1024
ROOT = Path(__file__).resolve().parent.parent


def _failed(message):
    return {"status": "failed", "message": message}


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _probe_http(config):
    query = urllib.parse.urlencode({"query": "vector(1)", "timeout": f"{config['timeout_seconds']}s"})
    url = config["base_url"] + "/api/v1/query?" + query
    # Keep internal requests off inherited proxies and never forward to a redirect.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect(),
                                        urllib.request.HTTPSHandler(context=ssl.create_default_context()))
    request = urllib.request.Request(url, headers={"Accept": "application/json", "Accept-Encoding": "identity"})
    try:
        with opener.open(request, timeout=config["timeout_seconds"]) as response:
            if response.status != 200:
                return _failed(f"Prometheus returned HTTP {response.status}.")
            if response.headers.get_content_type() != "application/json":
                return _failed("The endpoint did not return a Prometheus JSON response.")
            body = response.read(MAX_RESPONSE_BYTES + 1)
        if len(body) > MAX_RESPONSE_BYTES:
            return _failed("The connection-test response exceeded the size limit.")
        result = json.loads(body)
        if not isinstance(result, dict) or result.get("status") != "success":
            return _failed("Prometheus returned an API error.")
        data = result.get("data")
        if not isinstance(data, dict) or data.get("resultType") != "vector":
            return _failed("The endpoint returned an unexpected query result.")
        rows = data.get("result")
        if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict):
            return _failed("The endpoint returned an unexpected query result.")
        value = rows[0].get("value")
        if (not isinstance(rows[0].get("metric"), dict) or not isinstance(value, list)
                or len(value) != 2 or isinstance(value[0], bool)
                or not isinstance(value[0], (int, float)) or not math.isfinite(value[0])
                or value[1] != "1"):
            return _failed("The endpoint returned an unexpected query result.")
        message = "Query API responded. Kafka metric availability has not been checked."
        if result.get("warnings") or result.get("infos"):
            message += " Prometheus also returned query annotations."
        return {"status": "connected", "message": message}
    except urllib.error.HTTPError as exc:
        if 300 <= exc.code < 400:
            return _failed("The endpoint redirected the request. Enter its direct Prometheus URL.")
        return _failed(f"Prometheus returned HTTP {exc.code}.")
    except urllib.error.URLError as exc:
        if isinstance(exc.reason, socket.gaierror):
            return _failed("Cannot resolve the Prometheus host from this server.")
        if isinstance(exc.reason, ssl.SSLError):
            return _failed("TLS certificate verification failed.")
        if isinstance(exc.reason, (TimeoutError, socket.timeout)):
            return _failed("Connection test timed out.")
        return _failed("Cannot connect to Prometheus from this server. Check the address and network access.")
    except (TimeoutError, socket.timeout):
        return _failed("Connection test timed out.")
    except (ValueError, UnicodeError, OSError):
        return _failed("The endpoint did not return a valid Prometheus response.")


def probe(config):
    """A child process makes DNS and body reads subject to one killable deadline."""
    try:
        result = subprocess.run(
            [sys.executable, "-m", "gcanalyzer.prometheus_client"],
            input=json.dumps(config), text=True, capture_output=True,
            cwd=ROOT, timeout=config["timeout_seconds"] + 0.5, check=False,
        )
        if result.returncode != 0 or len(result.stdout) > MAX_RESPONSE_BYTES:
            return _failed("The connection test could not complete.")
        answer = json.loads(result.stdout)
        if (not isinstance(answer, dict) or answer.get("status") not in ("connected", "failed")
                or not isinstance(answer.get("message"), str)):
            return _failed("The connection test returned an invalid result.")
        return answer
    except subprocess.TimeoutExpired:
        return _failed("Connection test timed out.")
    except (OSError, ValueError):
        return _failed("The connection test could not complete.")


if __name__ == "__main__":
    from .prometheus_settings import normalize
    try:
        raw = sys.stdin.buffer.read(MAX_RESPONSE_BYTES + 1)
        if len(raw) > MAX_RESPONSE_BYTES:
            raise ValueError("Configuration too large")
        config = normalize(json.loads(raw))
        if not config["base_url"]:
            raise ValueError("Endpoint required")
        print(json.dumps(_probe_http(config)))
    except (ValueError, OSError):
        print(json.dumps(_failed("The connection test configuration is invalid.")))
