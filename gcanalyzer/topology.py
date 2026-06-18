"""
Cluster topology model.

Describes the fleet the dashboard navigates:

    Region (NAM / EMEA / APAC)
      -> Environment (UAT, PROD; EMEA also SANDBOX, PHY, DEV)
        -> Cluster  (one per region+env, e.g. EMEA-PROD)
          -> Component group (brokers, schema-registry, connect,
                              controllers, zookeeper)
            -> Instance (a single JVM, e.g. EMEA-PROD-broker-2)

Every instance is a JVM, so the per-instance view shows JVM heap / GC metrics.

In production this inventory would come from your config or service discovery;
here it is generated deterministically so the demo has a realistic shape. The
same `Instance` records drive both the live SSH collector and the history store.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict


# Region -> ordered list of environments present in that region.
REGION_ENVS = {
    "NAM": ["UAT", "PROD"],
    "EMEA": ["UAT", "PROD", "SANDBOX", "PHY", "DEV"],
    "APAC": ["UAT", "PROD"],
}

# Component groups every cluster contains. (label, role, default heap by tier)
COMPONENT_GROUPS = [
    {"key": "brokers", "label": "Brokers", "role": "broker"},
    {"key": "schema-registry", "label": "Schema Registry", "role": "schema-registry"},
    {"key": "connect", "label": "Kafka Connect", "role": "connect"},
    {"key": "controllers", "label": "Controllers / KRaft", "role": "controller"},
    {"key": "zookeeper", "label": "ZooKeeper", "role": "zookeeper"},
]

# How many instances of each role exist, by environment "tier".
# PROD is full-scale; UAT mid; SANDBOX/PHY/DEV are small.
_COUNTS = {
    "PROD":    {"broker": 5, "schema-registry": 2, "connect": 3, "controller": 3, "zookeeper": 3},
    "UAT":     {"broker": 3, "schema-registry": 1, "connect": 2, "controller": 1, "zookeeper": 1},
    "SANDBOX": {"broker": 2, "schema-registry": 1, "connect": 1, "controller": 1, "zookeeper": 1},
    "PHY":     {"broker": 3, "schema-registry": 1, "connect": 2, "controller": 3, "zookeeper": 3},
    "DEV":     {"broker": 2, "schema-registry": 1, "connect": 1, "controller": 1, "zookeeper": 1},
}

# Heap size (MB) by role and whether the env is production-grade.
_HEAP = {
    "broker":          {"prod": 6144, "non": 4096},
    "schema-registry": {"prod": 1024, "non": 1024},
    "connect":         {"prod": 2048, "non": 2048},
    "controller":      {"prod": 1024, "non": 1024},
    "zookeeper":       {"prod": 1024, "non": 1024},
}

_REGION_BUSY_HOUR_UTC = {"NAM": 17, "EMEA": 10, "APAC": 3}  # local-business peak in UTC


@dataclass
class Instance:
    id: str
    region: str
    env: str
    cluster: str          # region-env
    group: str            # component group key
    role: str
    index: int            # 1-based within its group
    heap_max_mb: int
    busy_hour_utc: int

    def to_dict(self) -> dict:
        return asdict(self)


def _heap_for(role: str, env: str) -> int:
    tier = "prod" if env == "PROD" else "non"
    return _HEAP[role][tier]


def build_instances() -> list[Instance]:
    out: list[Instance] = []
    for region, envs in REGION_ENVS.items():
        for env in envs:
            cluster = f"{region}-{env}"
            counts = _COUNTS[env]
            for grp in COMPONENT_GROUPS:
                role = grp["role"]
                n = counts.get(role, 0)
                for i in range(1, n + 1):
                    out.append(
                        Instance(
                            id=f"{cluster}-{role}-{i}",
                            region=region,
                            env=env,
                            cluster=cluster,
                            group=grp["key"],
                            role=role,
                            index=i,
                            heap_max_mb=_heap_for(role, env),
                            busy_hour_utc=_REGION_BUSY_HOUR_UTC[region],
                        )
                    )
    return out


def topology_skeleton() -> dict:
    """Nested region -> env -> cluster -> groups structure (no metrics)."""
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
    print(f"{len(insts)} instances across "
          f"{sum(len(v) for v in REGION_ENVS.values())} clusters")
    from collections import Counter
    print("by role:", dict(Counter(i.role for i in insts)))
    print("by cluster:", dict(Counter(i.cluster for i in insts)))
