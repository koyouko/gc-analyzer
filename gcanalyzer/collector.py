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
import re
import shlex
import stat
import time
from dataclasses import dataclass, field
from typing import Optional

from .parser import MAX_LOG_BYTES, complete_prefix, read_log_text

SSH_COMMAND_TIMEOUT_S = 30.0
MAX_SAR_BYTES = 16 * 1024 * 1024
MAX_LIST_BYTES = 1024 * 1024
MAX_LOG_FILES = 128


class SSHCommandError(RuntimeError):
    """Command failure with bounded stdout/stderr evidence, never partial success."""

    def __init__(self, message, stdout=b"", stderr=b""):
        super().__init__(message)
        self.stdout, self.stderr = stdout, stderr


def _run_ssh_command(client, command: str, *, timeout: float = SSH_COMMAND_TIMEOUT_S,
                     max_bytes: int = MAX_LOG_BYTES, check: bool = True) -> tuple[bytes, bytes, int]:
    """Drain both channel streams under one wall-clock deadline and byte budget."""
    if timeout <= 0 or max_bytes <= 0:
        raise ValueError("SSH timeout and output limit must be positive")
    deadline = time.monotonic() + timeout
    streams = ()
    channel = None
    out, err = bytearray(), bytearray()
    try:
        streams = client.exec_command(command, timeout=timeout)
        channel = streams[1].channel
        channel.settimeout(timeout)
        streams[0].close()
        while True:
            if time.monotonic() >= deadline:
                raise SSHCommandError("SSH command deadline exceeded", bytes(out), bytes(err))
            received = False
            for ready, recv, target in ((channel.recv_ready, channel.recv, out),
                                        (channel.recv_stderr_ready, channel.recv_stderr, err)):
                if ready():
                    remaining = max_bytes - len(out) - len(err)
                    chunk = recv(min(65536, remaining + 1))
                    target.extend(chunk[:remaining])
                    if len(chunk) > remaining:
                        raise SSHCommandError("SSH captured output exceeds byte limit", bytes(out), bytes(err))
                    received = received or bool(chunk)
            if channel.exit_status_ready() and not channel.recv_ready() and not channel.recv_stderr_ready():
                status = channel.recv_exit_status()
                if check and status != 0:
                    detail = bytes(err[:4096]).decode(errors="replace")
                    raise SSHCommandError(f"SSH command exited {status}: {detail}", bytes(out), bytes(err))
                return bytes(out), bytes(err), status
            if not received:
                time.sleep(min(0.01, max(0, deadline - time.monotonic())))
    except SSHCommandError:
        raise
    except Exception as exc:
        raise SSHCommandError(f"SSH command failed: {exc}", bytes(out), bytes(err)) from exc
    finally:
        if channel is not None:
            channel.close()
        for stream in streams:
            stream.close()


def _quote_glob(pattern: str) -> str:
    # Expand only the requested wildcard characters, never shell syntax.
    return "".join(part if part in ("*", "?") else shlex.quote(part)
                   for part in re.split(r"([*?])", pattern) if part)


def _remote_files(client, node, log_callback=None) -> list[str]:
    def listing(pattern):
        out, err, _ = _run_ssh_command(client, f"ls -1d -- {_quote_glob(pattern)}",
                                      max_bytes=MAX_LIST_BYTES, check=False)
        if err and log_callback:
            log_callback(f"SSH listing: {err[:4096].decode(errors='replace').strip()}")
        return [line for line in out.decode(errors="strict").splitlines() if line]

    files = []
    for pattern in node.effective_globs():
        files.extend(listing(pattern))
        if len(files) > MAX_LOG_FILES:
            raise ValueError("GC file count exceeds limit")
    if not files:
        for directory in FALLBACK_DIRS:
            files.extend(listing(f"{directory}/*gc*.log*"))
    files = list(dict.fromkeys(files))
    if len(files) > MAX_LOG_FILES:
        raise ValueError("GC file count exceeds limit")
    return files


def _previous_offset(prev: dict, path: str, inode: int, size: int) -> int:
    same_inode = [entry for entry in prev.values() if entry["inode"] == inode]
    seen = prev.get(path)
    if seen and seen["inode"] == inode:
        same_inode.append(seen)
    return max((entry["offset"] for entry in same_inode if 0 <= entry["offset"] <= size), default=0)


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
    paths = list(dict.fromkeys(paths))
    if len(paths) > MAX_LOG_FILES:
        raise ValueError("GC file count exceeds limit")
    if log_callback:
        log_callback(f"Found {len(paths)} local file(s) matching patterns: {patterns}")
    out = []
    total_bytes = 0
    for p in paths:
        try:
            if log_callback:
                log_callback(f"Reading local file: {p}")
            text = read_log_text(p)
            total_bytes += len(text.encode("utf-8"))
            if total_bytes > MAX_LOG_BYTES:
                raise ValueError("GC collection exceeds byte limit")
            out.append(CollectedLog(node.id, node.role, p, text))
        except OSError as exc:
            if log_callback:
                log_callback(f"Error reading file {p}: {exc}")
            raise
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
        "banner_timeout": 15,
        "auth_timeout": 15,
        "channel_timeout": SSH_COMMAND_TIMEOUT_S,
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
        total_bytes = 0
        for remote_path in _remote_files(client, node, log_callback):
            if log_callback:
                log_callback(f"SSH: Fetching file '{remote_path}'...")
            if remote_path.endswith(".gz"):
                raise ValueError("SSH gzip collection is unsupported; use local bounded gzip parsing")
            data, err, _ = _run_ssh_command(client, f"cat -- {shlex.quote(remote_path)}")
            total_bytes += len(data)
            if total_bytes > MAX_LOG_BYTES:
                raise ValueError("GC collection exceeds byte limit")
            if err and log_callback:
                log_callback(f"SSH stderr: {err[:4096].decode(errors='replace')}")
            out.append(CollectedLog(node.id, node.role, f"{node.host}:{remote_path}", data.decode(errors="replace")))
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
    paths = list(dict.fromkeys(path for pat in patterns for path in sorted(glob.glob(pat))))
    if len(paths) > MAX_LOG_FILES:
        raise ValueError("GC file count exceeds limit")
    total_bytes = 0
    seen_inodes = set()
    for path in paths:
        if path.endswith(".gz"):
            raise ValueError("Incremental gzip collection is unsupported; use bounded full-file parsing")
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as fh:
            st = os.fstat(fh.fileno())
            if not stat.S_ISREG(st.st_mode):
                raise ValueError("GC input must be a regular file")
            if (st.st_dev, st.st_ino) in seen_inodes:
                continue
            seen_inodes.add((st.st_dev, st.st_ino))
            inode, size = st.st_ino, st.st_size
            start = _previous_offset(prev, path, inode, size)
            fh.seek(start)
            data = fh.read(min(size - start, MAX_LOG_BYTES - total_bytes + 1))
            total_bytes += len(data)
            if total_bytes > MAX_LOG_BYTES:
                raise ValueError("GC incremental collection exceeds byte limit")
            complete = complete_prefix(data)
            if complete:
                parts.append(complete.decode("utf-8", errors="replace"))
            new_offsets[path] = {"inode": inode, "offset": start + len(complete)}
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
        "banner_timeout": 15,
        "auth_timeout": 15,
        "channel_timeout": SSH_COMMAND_TIMEOUT_S,
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

        total_bytes = 0
        seen_inodes = set()
        for remote_path in _remote_files(client, node, log_callback):
            if remote_path.endswith(".gz"):
                raise ValueError("Incremental gzip collection is unsupported; use bounded full-file parsing")
            quoted = shlex.quote(remote_path)
            stat_cmd = f"stat -c '%i %s' -- {quoted} 2>/dev/null || stat -f '%i %z' -- {quoted}"
            stat_out, _, _ = _run_ssh_command(client, stat_cmd, max_bytes=MAX_LIST_BYTES)
            try:
                inode, size = map(int, stat_out.split())
            except ValueError as exc:
                raise ValueError("Invalid remote GC file stat output") from exc
            if inode <= 0 or size < 0:
                raise ValueError("Invalid remote GC file inode/size")
            if inode in seen_inodes:
                continue
            seen_inodes.add(inode)
            start = _previous_offset(prev, remote_path, inode, size)

            if log_callback:
                log_callback(f"SSH (inc): Reading '{remote_path}' from offset {start} (inode {inode}, size {size})...")

            if size - start > MAX_LOG_BYTES - total_bytes:
                raise ValueError("GC incremental collection exceeds byte limit")
            # Bounded capture also handles a growing file. Check stat again so a
            # rotation between stat and read cannot acknowledge the wrong inode.
            read_cmd = f"tail -c +{start + 1} -- {quoted}"
            data, _, _ = _run_ssh_command(client, read_cmd)
            total_bytes += len(data)
            if total_bytes > MAX_LOG_BYTES:
                raise ValueError("GC incremental collection exceeds byte limit")
            after, _, _ = _run_ssh_command(client, stat_cmd, max_bytes=MAX_LIST_BYTES)
            after_inode, after_size = map(int, after.split())
            if after_inode != inode or after_size < size or len(data) < size - start:
                raise RuntimeError("GC file changed during incremental read; offsets retained")
            complete = complete_prefix(data[:size - start])
            if complete:
                parts.append(complete.decode(errors="replace"))
            new_offsets[remote_path] = {"inode": inode, "offset": start + len(complete)}

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
        text = read_log_text(path, max_bytes=MAX_SAR_BYTES)
        fmt_hint = "json" if path.endswith(".json") else "text"
        if log_callback:
            log_callback(f"SAR: read local file '{path}' ({len(text)} bytes)")
        return CollectedSar(node.id, path, text, fmt_hint, None)
    except (OSError, EOFError, ValueError) as exc:
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
        "banner_timeout": 15,
        "auth_timeout": 15,
        "channel_timeout": SSH_COMMAND_TIMEOUT_S,
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
        date_out, _, _ = _run_ssh_command(client, "date -u +%m/%d/%Y", max_bytes=MAX_LIST_BYTES)
        report_date = date_out.decode().strip() or None

        # Prefer JSON (sysstat >= 11.x): structured, no column-alignment guessing.
        json_cmd = f"TZ=UTC LC_ALL=C {shlex.quote(node.sadf_bin)} -j -- -A"
        if log_callback:
            log_callback(f"SAR: trying '{json_cmd}'...")
        data, _, status = _run_ssh_command(client, json_cmd, max_bytes=MAX_SAR_BYTES, check=False)
        text = data.decode(errors="replace")
        if status == 0 and text.strip().startswith("{"):
            if log_callback:
                log_callback(f"SAR: got sadf JSON ({len(text)} bytes)")
            return CollectedSar(node.id, detail, text, "json", report_date)

        # Fallback: classic `sar -A` text report (works on essentially every
        # sysstat version, including ones too old to support `-j`).
        text_cmd = f"TZ=UTC LC_ALL=C {shlex.quote(node.sar_bin)} -A"
        if log_callback:
            log_callback(f"SAR: sadf JSON unavailable, trying '{text_cmd}'...")
        data, err, _ = _run_ssh_command(client, text_cmd, max_bytes=MAX_SAR_BYTES)
        text = data.decode(errors="replace")
        stderr_text = err[:4096].decode(errors="replace").strip()
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
