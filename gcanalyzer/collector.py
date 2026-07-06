"""
Log + host-metrics collection layer.

Two sources are supported:

  * local : read GC log files already on disk (used for the bundled samples and
            for logs you have already pulled down).
  * ssh   : fetch GC logs directly from Kafka brokers, the KRaft controller /
            history nodes, and ZooKeeper hosts over SSH.

SSH uses paramiko if it is installed. The default log location follows the
Confluent / Apache Kafka convention ($KAFKA_HOME/logs or
/var/log/kafka), but every node may override `log_paths` explicitly — globs are
supported so `kafkaServer-gc.log*` rotations are all collected.

If paramiko is not installed, the SSH collector raises a clear error telling you
to `pip install paramiko`; everything else (parsing, analysis, dashboard) works
without it so you can demo against the bundled samples offline.

SAR (sysstat) collection reuses the same `source`/SSH credentials as GC log
collection (it's the same host), but it doesn't read a file — it runs a report
command and captures its stdout. `TZ=UTC` forces sar/sadf to render its
internally-UTC-stored samples in UTC regardless of the host's local timezone,
so host metrics line up with GC metrics (also recorded as UTC epoch seconds)
without needing to know each host's TZ. See sar_parser.py for the two output
formats this feeds (sadf JSON, classic sar text).
"""

from __future__ import annotations

import glob
import os
from dataclasses import dataclass, field
from typing import Optional


# Default GC-log glob patterns by node role, relative to the resolved Kafka home.
DEFAULT_LOG_GLOBS = {
    "broker": ["logs/kafkaServer-gc.log*", "logs/gc.log*"],
    "controller": ["logs/kafkaServer-gc.log*", "logs/controller-gc.log*"],
    "history": ["logs/kafka-history-gc.log*", "logs/gc.log*"],
    "zookeeper": ["logs/zookeeper-gc.log*", "logs/zookeeperServer-gc.log*", "logs/gc.log*"],
}
DEFAULT_KAFKA_HOME = "/opt/kafka"
FALLBACK_DIRS = ["/var/log/kafka", "/var/log/zookeeper"]


@dataclass
class NodeConfig:
    id: str
    role: str                       # broker | controller | history | zookeeper
    source: str = "ssh"             # ssh | local
    host: Optional[str] = None
    port: int = 22
    user: Optional[str] = None
    key_path: Optional[str] = None
    password: Optional[str] = None
    kafka_home: str = DEFAULT_KAFKA_HOME
    log_paths: list = field(default_factory=list)   # explicit paths/globs override defaults
    local_paths: list = field(default_factory=list) # for source == local

    # --- SAR / host-metrics collection (same host, optional separate creds) ---
    sar_enabled: bool = True
    sar_source: Optional[str] = None       # defaults to `source` when unset
    sar_bin: str = "sar"
    sadf_bin: str = "sadf"
    sar_local_path: Optional[str] = None   # local/demo: a captured sar -A or sadf -j dump

    def effective_sar_source(self) -> str:
        return self.sar_source or self.source

    def effective_globs(self) -> list[str]:
        if self.log_paths:
            return self.log_paths
        rel = DEFAULT_LOG_GLOBS.get(self.role, DEFAULT_LOG_GLOBS["broker"])
        return [os.path.join(self.kafka_home, r) for r in rel]


@dataclass
class CollectedLog:
    node_id: str
    role: str
    source_detail: str   # path or host:path it came from
    text: str


# --------------------------------------------------------------------------- #
# Local collection
# --------------------------------------------------------------------------- #
def collect_local(node: NodeConfig, log_callback=None) -> list[CollectedLog]:
    paths: list[str] = []
    patterns = node.local_paths or node.effective_globs()
    for pat in patterns:
        paths.extend(sorted(glob.glob(pat)))
    if log_callback:
        log_callback(f"Found {len(paths)} local file(s) matching patterns: {patterns}")
    out = []
    for p in paths:
        try:
            if log_callback:
                log_callback(f"Reading local file: {p}")
            with open(p, "r", errors="replace") as fh:
                out.append(CollectedLog(node.id, node.role, p, fh.read()))
        except OSError as exc:
            if log_callback:
                log_callback(f"Error reading file {p}: {exc}")
            out.append(CollectedLog(node.id, node.role, p, f"# READ ERROR: {exc}"))
    return out


# --------------------------------------------------------------------------- #
# SSH collection
# --------------------------------------------------------------------------- #
def collect_ssh(node: NodeConfig, log_callback=None) -> list[CollectedLog]:
    try:
        import paramiko  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "paramiko is required for SSH collection. Install it with "
            "`pip install paramiko`, or set the node's source to 'local'."
        ) from exc

    import paramiko

    if log_callback:
        log_callback(f"SSH: Connecting to {node.user or 'default'}@{node.host}:{node.port}...")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    connect_kwargs = {
        "hostname": node.host,
        "port": node.port,
        "username": node.user,
        "timeout": 15,
    }
    if node.key_path:
        connect_kwargs["key_filename"] = os.path.expanduser(node.key_path)
    if node.password:
        connect_kwargs["password"] = node.password

    out: list[CollectedLog] = []
    try:
        client.connect(**connect_kwargs)
        if log_callback:
            log_callback(f"SSH: Connected successfully. Checking log paths...")
        # Expand globs remotely (sh -c so wildcards resolve on the broker).
        for pattern in node.effective_globs():
            cmd = f"ls -1 {pattern}"
            if log_callback:
                log_callback(f"SSH: Listing files matching '{pattern}'...")
            _in, _stdout, _err = client.exec_command(cmd)
            files = [ln.strip() for ln in _stdout.read().decode().splitlines() if ln.strip()]
            stderr_text = _err.read().decode().strip()
            if stderr_text and log_callback:
                log_callback(f"SSH (stderr): {stderr_text}")
            for remote_path in files:
                if log_callback:
                    log_callback(f"SSH: Fetching file '{remote_path}'...")
                _in, _stdout, _err = client.exec_command(f"cat {remote_path}")
                text = _stdout.read().decode(errors="replace")
                cat_stderr = _err.read().decode().strip()
                if cat_stderr and log_callback:
                    log_callback(f"SSH (stderr from cat): {cat_stderr}")
                out.append(
                    CollectedLog(node.id, node.role, f"{node.host}:{remote_path}", text)
                )
        # Fallback common directories if nothing matched yet.
        if not out:
            if log_callback:
                log_callback(f"SSH: No files matched globs. Checking fallback directories: {FALLBACK_DIRS}")
            for d in FALLBACK_DIRS:
                cmd = f"ls -1 {d}/*gc*.log*"
                _in, _stdout, _err = client.exec_command(cmd)
                files = [ln.strip() for ln in _stdout.read().decode().splitlines() if ln.strip()]
                stderr_text = _err.read().decode().strip()
                if stderr_text and log_callback:
                    log_callback(f"SSH (stderr): {stderr_text}")
                for remote_path in files:
                    if log_callback:
                        log_callback(f"SSH: Fetching fallback file '{remote_path}'...")
                    _in, _stdout, _err = client.exec_command(f"cat {remote_path}")
                    text = _stdout.read().decode(errors="replace")
                    cat_stderr = _err.read().decode().strip()
                    if cat_stderr and log_callback:
                        log_callback(f"SSH (stderr from cat): {cat_stderr}")
                    out.append(
                        CollectedLog(node.id, node.role, f"{node.host}:{remote_path}", text)
                    )
        if log_callback:
            log_callback(f"SSH: Completed collection for node. Collected {len(out)} files.")
    except Exception as e:
        if log_callback:
            log_callback(f"SSH: Error during collection: {e}")
        raise
    finally:
        client.close()
    return out



def read_increment_local(node: NodeConfig, prev: dict) -> tuple[str, dict]:
    """Read only bytes appended since last offset (local files)."""
    patterns = node.local_paths or node.effective_globs()
    parts: list[str] = []
    new_offsets: dict[str, dict] = {}
    for pat in patterns:
        for path in sorted(glob.glob(pat)):
            try:
                st = os.stat(path)
            except OSError:
                continue
            inode, size = st.st_ino, st.st_size
            seen = prev.get(path)
            start = seen["offset"] if (seen and seen["inode"] == inode and seen["offset"] <= size) else 0
            try:
                with open(path, "r", errors="replace") as fh:
                    fh.seek(start)
                    parts.append(fh.read())
            except OSError:
                continue
            new_offsets[path] = {"inode": inode, "offset": size}
    return "\n".join(parts), new_offsets

def collect(node: NodeConfig, log_callback=None) -> list[CollectedLog]:
    if node.source == "local":
        return collect_local(node, log_callback=log_callback)
    return collect_ssh(node, log_callback=log_callback)


def collect_ssh_incremental(
    node: NodeConfig, prev: dict, log_callback=None
) -> tuple[str, dict]:
    try:
        import paramiko  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "paramiko is required for SSH collection. Install it with "
            "`pip install paramiko`, or set the node's source to 'local'."
        ) from exc

    import paramiko

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    connect_kwargs = {
        "hostname": node.host,
        "port": node.port,
        "username": node.user,
        "timeout": 15,
    }
    if node.key_path:
        connect_kwargs["key_filename"] = os.path.expanduser(node.key_path)
    if node.password:
        connect_kwargs["password"] = node.password

    parts: list[str] = []
    new_offsets: dict[str, dict] = {}

    try:
        if log_callback:
            log_callback(f"SSH (inc): Connecting to {node.user or 'default'}@{node.host}:{node.port}...")
        client.connect(**connect_kwargs)
        if log_callback:
            log_callback(f"SSH (inc): Connected successfully. Expanding paths...")

        files = []
        for pattern in node.effective_globs():
            cmd = f"ls -1 {pattern}"
            _in, _stdout, _err = client.exec_command(cmd)
            lines = [ln.strip() for ln in _stdout.read().decode().splitlines() if ln.strip()]
            files.extend(lines)
            stderr_text = _err.read().decode().strip()
            if stderr_text and not lines and log_callback:
                log_callback(f"SSH (inc stderr) for '{pattern}': {stderr_text}")

        if not files:
            if log_callback:
                log_callback(f"SSH (inc): No files matched globs. Checking fallback directories...")
            for d in FALLBACK_DIRS:
                cmd = f"ls -1 {d}/*gc*.log*"
                _in, _stdout, _err = client.exec_command(cmd)
                lines = [ln.strip() for ln in _stdout.read().decode().splitlines() if ln.strip()]
                files.extend(lines)
                stderr_text = _err.read().decode().strip()
                if stderr_text and not lines and log_callback:
                    log_callback(f"SSH (inc stderr) for '{d}': {stderr_text}")

        for remote_path in files:
            stat_cmd = f"stat -c '%i %s' '{remote_path}' 2>/dev/null || stat -f '%i %z' '{remote_path}'"
            _in, _stdout, _err = client.exec_command(stat_cmd)
            stat_out = _stdout.read().decode().strip()
            if not stat_out:
                continue
            try:
                inode, size = map(int, stat_out.split())
            except ValueError:
                inode, size = 0, 0

            seen = prev.get(remote_path)
            start = seen["offset"] if (seen and seen["inode"] == inode and seen["offset"] <= size) else 0

            if log_callback:
                log_callback(f"SSH (inc): Reading '{remote_path}' from offset {start} (inode {inode}, size {size})...")

            if start > 0:
                read_cmd = f"tail -c +{start + 1} '{remote_path}'"
            else:
                read_cmd = f"cat '{remote_path}'"

            _in, _stdout, _err = client.exec_command(read_cmd)
            text = _stdout.read().decode(errors="replace")
            parts.append(text)

            new_offsets[remote_path] = {"inode": inode, "offset": size}

        if log_callback:
            log_callback(f"SSH (inc): Completed. Read {len(parts)} file delta(s).")
    except Exception as e:
        if log_callback:
            log_callback(f"SSH (inc): Error during collection: {e}")
        raise
    finally:
        client.close()

    return "\n".join(parts), new_offsets


# --------------------------------------------------------------------------- #
# SAR (sysstat) collection — runs a report command and captures stdout. There
# is no byte-offset incrementality here (sar/sadf reports are re-queryable,
# not append-only files); sar_ingest.py dedups by sample timestamp instead.
# --------------------------------------------------------------------------- #
@dataclass
class CollectedSar:
    node_id: str
    source_detail: str
    text: str
    fmt_hint: Optional[str]   # "json" | "text" | None (unknown/empty)
    report_date: Optional[str]  # MM/DD/YYYY, for the text-parser fallback
    error: Optional[str] = None


def collect_sar_local(node: NodeConfig, log_callback=None) -> CollectedSar:
    if not node.sar_local_path:
        return CollectedSar(node.id, "-", "", None, None, error="no sar_local_path configured")
    path = node.sar_local_path
    try:
        with open(path, "r", errors="replace") as fh:
            text = fh.read()
        fmt_hint = "json" if path.endswith(".json") else "text"
        if log_callback:
            log_callback(f"SAR: read local file '{path}' ({len(text)} bytes)")
        return CollectedSar(node.id, path, text, fmt_hint, None)
    except OSError as exc:
        if log_callback:
            log_callback(f"SAR: error reading '{path}': {exc}")
        return CollectedSar(node.id, path, "", None, None, error=str(exc))


def collect_sar_ssh(node: NodeConfig, log_callback=None) -> CollectedSar:
    try:
        import paramiko  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "paramiko is required for SSH collection. Install it with "
            "`pip install paramiko`, or set the node's sar_source to 'local'."
        ) from exc

    import paramiko

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    connect_kwargs = {
        "hostname": node.host,
        "port": node.port,
        "username": node.user,
        "timeout": 15,
    }
    if node.key_path:
        connect_kwargs["key_filename"] = os.path.expanduser(node.key_path)
    if node.password:
        connect_kwargs["password"] = node.password

    detail = f"{node.host}:sar"
    try:
        if log_callback:
            log_callback(f"SAR: Connecting to {node.user or 'default'}@{node.host}:{node.port}...")
        client.connect(**connect_kwargs)

        # Remote UTC date, independent of sar's own date formatting, used as a
        # reliable fallback for the text parser if the report banner is missing.
        _in, _out, _err = client.exec_command("date -u +%m/%d/%Y")
        report_date = _out.read().decode().strip() or None

        # Prefer JSON (sysstat >= 11.x): structured, no column-alignment guessing.
        json_cmd = f"TZ=UTC LC_ALL=C {node.sadf_bin} -j -- -A 2>/dev/null"
        if log_callback:
            log_callback(f"SAR: trying '{json_cmd}'...")
        _in, _out, _err = client.exec_command(json_cmd)
        text = _out.read().decode(errors="replace")
        if text.strip().startswith("{"):
            if log_callback:
                log_callback(f"SAR: got sadf JSON ({len(text)} bytes)")
            return CollectedSar(node.id, detail, text, "json", report_date)

        # Fallback: classic `sar -A` text report (works on essentially every
        # sysstat version, including ones too old to support `-j`).
        text_cmd = f"TZ=UTC LC_ALL=C {node.sar_bin} -A 2>/dev/null"
        if log_callback:
            log_callback(f"SAR: sadf JSON unavailable, trying '{text_cmd}'...")
        _in, _out, _err = client.exec_command(text_cmd)
        text = _out.read().decode(errors="replace")
        stderr_text = _err.read().decode().strip()
        if not text.strip():
            msg = stderr_text or "sar produced no output (is sysstat installed and collecting? see /etc/cron.d/sysstat)"
            if log_callback:
                log_callback(f"SAR: {msg}")
            return CollectedSar(node.id, detail, "", None, report_date, error=msg)
        if log_callback:
            log_callback(f"SAR: got sar text report ({len(text)} bytes)")
        return CollectedSar(node.id, detail, text, "text", report_date)
    except Exception as e:
        if log_callback:
            log_callback(f"SAR: Error during collection: {e}")
        return CollectedSar(node.id, detail, "", None, None, error=str(e))
    finally:
        client.close()


def collect_sar(node: NodeConfig, log_callback=None) -> CollectedSar:
    if not node.sar_enabled:
        return CollectedSar(node.id, "-", "", None, None, error="sar disabled for this node")
    if node.effective_sar_source() == "local":
        return collect_sar_local(node, log_callback=log_callback)
    return collect_sar_ssh(node, log_callback=log_callback)
