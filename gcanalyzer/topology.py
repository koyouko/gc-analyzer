"""
Cluster topology model.

Demo fleet: two Kafka clusters that showcase every component type and GC scenario:

    Region (DEMO)
      -> Environment (KRAFT | ZOOKEEPER)
        -> Cluster  (DEMO-KRAFT or DEMO-ZK)
          -> Component group (brokers, schema-registry, connect,
                              controllers *or* zookeeper)
            -> Instance (a single JVM, e.g. DEMO-KRAFT-broker-2)

DEMO-KRAFT is a KRaft cluster (controllers, no ZooKeeper).
DEMO-ZK is a classic ZooKeeper-backed cluster (no KRaft controllers).

In production this inventory would come from your config or service discovery;
here it is generated deterministically for the dashboard demo.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict


COMPONENT_GROUPS = [
    {"key": "brokers", "label": "Brokers", "role": "broker"},
    {"key": "schema-registry", "label": "Schema Registry", "role": "schema-registry"},
    {"key": "connect", "label": "Kafka Connect", "role": "connect"},
    {"key": "controllers", "label": "Controllers / KRaft", "role": "controller"},
    {"key": "zookeeper", "label": "ZooKeeper", "role": "zookeeper"},
]

_HEAP = {
    "broker": 6144,
    "schema-registry": 1024,
    "connect": 2048,
    "controller": 1024,
    "zookeeper": 1024,
}

DEMO_CLUSTERS = [
    {
        "id": "DEMO-KRAFT",
        "region": "DEMO",
        "env": "KRAFT",
        "mode": "kraft",
        "busy_hour_utc": 10,
        "groups": [
            ("brokers", "broker", 3),
            ("schema-registry", "schema-registry", 2),
            ("connect", "connect", 2),
            ("controllers", "controller", 3),
        ],
    },
    {
        "id": "DEMO-ZK",
        "region": "DEMO",
        "env": "ZOOKEEPER",
        "mode": "zookeeper",
        "busy_hour_utc": 10,
        "groups": [
            ("brokers", "broker", 3),
            ("schema-registry", "schema-registry", 2),
            ("connect", "connect", 2),
            ("zookeeper", "zookeeper", 3),
        ],
    },
]


@dataclass
class Instance:
    id: str
    region: str
    env: str
    cluster: str
    group: str
    role: str
    index: int
    heap_max_mb: int
    busy_hour_utc: int
    mode: str = ""
    node_id: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


def default_heap_mb(role: str) -> int:
    return _HEAP.get(role, 1024)


def build_instances() -> list[Instance]:
    """Demo fleet inventory — ids match ingest._instance_id_for (cluster--node_id)."""
    out: list[Instance] = []
    for cl in DEMO_CLUSTERS:
        for group_key, role, count in cl["groups"]:
            for i in range(1, count + 1):
                node_id = f"{role}-{i}"
                out.append(
                    Instance(
                        id=f"{cl['id']}--{node_id}",
                        region=cl["region"],
                        env=cl["env"],
                        cluster=cl["id"],
                        group=group_key,
                        role=role,
                        index=i,
                        heap_max_mb=_HEAP[role],
                        busy_hour_utc=cl["busy_hour_utc"],
                        mode=cl["mode"],
                        node_id=node_id,
                    )
                )
    return out


def topology_skeleton() -> dict:
    tree = {}
    for inst in build_instances():
        r = tree.setdefault(inst.region, {"region": inst.region, "envs": {}})
        e = r["envs"].setdefault(inst.env, {"env": inst.env, "cluster": inst.cluster, "groups": {}})
        g = e["groups"].setdefault(inst.group, {"group": inst.group, "instances": []})
        g["instances"].append(inst.id)
    return tree


GROUP_LABEL = {g["key"]: g["label"] for g in COMPONENT_GROUPS}


if __name__ == "__main__":
    insts = build_instances()
    print(f"{len(insts)} instances across {len(DEMO_CLUSTERS)} demo clusters")
    from collections import Counter
    print("by role:", dict(Counter(i.role for i in insts)))
    print("by cluster:", dict(Counter(i.cluster for i in insts)))
