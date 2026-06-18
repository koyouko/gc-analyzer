"""Cluster configuration loading (YAML or JSON) into NodeConfig objects."""

from __future__ import annotations

import json
import os

from .collector import NodeConfig


def _load_raw(path: str) -> dict:
    with open(path, "r") as fh:
        text = fh.read()
    if path.endswith((".yaml", ".yml")):
        try:
            import yaml
        except ImportError as exc:
            raise RuntimeError(
                "PyYAML is required to read YAML config. `pip install pyyaml` "
                "or use a .json config instead."
            ) from exc
        return yaml.safe_load(text)
    return json.loads(text)


def parse_cluster(raw: dict) -> tuple[str, list[NodeConfig], str | None, str | None]:
    """Build (cluster_name, nodes, region, env) from an already-parsed config mapping."""
    if not isinstance(raw, dict):
        raise ValueError("Cluster config must be a mapping with 'cluster' and 'nodes'.")
    cluster_name = raw.get("cluster", "Kafka Cluster")
    region = raw.get("region")
    env = raw.get("env")
    defaults = raw.get("defaults", {})
    nodes = []
    for entry in raw.get("nodes", []):
        merged = {**defaults, **entry}
        if "id" not in merged:
            raise ValueError("Every node needs an 'id'.")
        nodes.append(
            NodeConfig(
                id=merged["id"],
                role=merged.get("role", "broker"),
                source=merged.get("source", "ssh"),
                host=merged.get("host"),
                port=int(merged.get("port", 22)),
                user=merged.get("user"),
                key_path=merged.get("key_path"),
                password=merged.get("password"),
                kafka_home=merged.get("kafka_home", "/opt/kafka"),
                log_paths=merged.get("log_paths", []),
                local_paths=merged.get("local_paths", []),
            )
        )
    return cluster_name, nodes, region, env


def load_cluster(path: str) -> tuple[str, list[NodeConfig], str | None, str | None]:
    return parse_cluster(_load_raw(path))


def load_cluster_text(text: str, fmt: str = "yaml") -> tuple[str, list[NodeConfig], str | None, str | None]:
    """Parse a cluster config supplied as text (dashboard paste/upload)."""
    if fmt == "json":
        raw = json.loads(text)
    else:
        try:
            import yaml
        except ImportError as exc:
            raise RuntimeError(
                "PyYAML is required to read YAML config. `pip install pyyaml` "
                "or submit JSON instead."
            ) from exc
        raw = yaml.safe_load(text)
    return parse_cluster(raw)


def sample_cluster(samples_dir: str) -> tuple[str, list[NodeConfig], str, str]:
    """Build a local-source cluster pointed at the bundled sample logs."""
    nodes = [
        NodeConfig(id="broker-1", role="broker", source="local",
                   local_paths=[os.path.join(samples_dir, "broker-1-gc.log")]),
        NodeConfig(id="broker-2", role="broker", source="local",
                   local_paths=[os.path.join(samples_dir, "broker-2-gc.log")]),
        NodeConfig(id="broker-3", role="broker", source="local",
                   local_paths=[os.path.join(samples_dir, "broker-3-gc.log")]),
        NodeConfig(id="controller-1", role="controller", source="local",
                   local_paths=[os.path.join(samples_dir, "controller-1-gc.log")]),
        NodeConfig(id="zookeeper-1", role="zookeeper", source="local",
                   local_paths=[os.path.join(samples_dir, "zookeeper-1-gc.log")]),
    ]
    return "Demo Kafka Cluster (sample logs)", nodes, "LOCAL", "DEV"
