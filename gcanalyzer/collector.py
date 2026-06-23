"""
Log collection layer.

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
