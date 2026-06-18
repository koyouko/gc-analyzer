"""
Live ingest bridge: real GC logs -> history store.

This is the "periodic collection job" that store.py and seed/seed_history.py
refer to but that the repo otherwise lacks. It closes the loop between the
collect -> parse -> analyze pipeline (service.py) and the SQLite time-series the
dashboard reads (store.py):

    collect (local/ssh) -> parser.parse -> analyzer.analyze
        -> map metrics -> store.upsert_instance + store.record_metric

Run once (or on a schedule / loop) against a cluster config:

    python -m gcanalyzer.ingest --config cluster.live.yaml --db gc_live.db

Each invocation writes one metric row per node, timestamped at the top of the
current minute, so repeated runs build a real time-series the trend charts and
last-hour alerting can read back. It never touches the demo gc_history.db unless
you point --db at it.
"""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass

from . import analyzer, parser, store, topology
from .collector import NodeConfig, collect
from .config import load_cluster

# role -> component-group key, reusing the dashboard's own taxonomy so live
# nodes slot into the same Region -> Env -> Cluster -> Group tree.
_ROLE_TO_GROUP = {g["role"]: g["key"] for g in topology.COMPONENT_GROUPS}

# Fallback heap (MB) when a log is too short for the parser to observe the
# committed heap size. Keyed by role; only used if analyzer reports 0.
_FALLBACK_HEAP_MB = {"broker": 512, "controller": 256}
_DEFAULT_HEAP_MB = 1024


@dataclass(frozen=True)
class NodeResult:
    """Outcome of ingesting one node — for the run summary."""

    node_id: str
    instance_id: str
    role: str
    recorded: bool
    detail: str


def metrics_to_row(m: dict) -> dict:
    """Map an analyzer metrics dict onto store.record_metric's column set."""
    return {
        "heap_used_mb": m["avg_heap_after_mb"],
        "heap_max_mb": m["heap_max_mb"],
        "heap_after_pct": m["avg_heap_after_pct"],
        "pause_avg_ms": m["avg_pause_ms"],
        "pause_p99_ms": m["p99_pause_ms"],
        "pause_max_ms": m["max_pause_ms"],
        "full_gc_count": m["full_count"],
        "young_count": m["young_count"],
        "gc_per_min": m["gc_per_min"],
        "time_in_gc_pct": m["pct_time_in_gc"],
        "throughput_pct": m["throughput_pct"],
    }


def _index_of(node_id: str, fallback: int) -> int:
    """Best-effort 1-based index from a trailing number in the node id."""
    tail = node_id.rsplit("-", 1)[-1]
    return int(tail) if tail.isdigit() else fallback


def _heap_max_mb(role: str, observed: float) -> int:
    if observed and observed > 0:
        return int(round(observed))
    return _FALLBACK_HEAP_MB.get(role, _DEFAULT_HEAP_MB)


def build_instance(
    node: NodeConfig, region: str, env: str, cluster: str, index: int, heap_max_mb: int
) -> topology.Instance:
    """Construct a topology.Instance so the live node joins the dashboard tree."""
    return topology.Instance(
        id=f"{cluster}-{node.role}-{index}",
        region=region,
        env=env,
        cluster=cluster,
        group=_ROLE_TO_GROUP.get(node.role, node.role),
        role=node.role,
        index=index,
        heap_max_mb=heap_max_mb,
        busy_hour_utc=0,
    )


def _analyze_node(node: NodeConfig, log_callback=None) -> dict | None:
    """collect -> parse -> analyze for one node. None if nothing was collected."""
    if log_callback:
        log_callback(f"Starting GC log collection for node '{node.id}'...")
    logs = collect(node, log_callback=log_callback)
    if not logs:
        if log_callback:
            log_callback(f"No GC log files collected for node '{node.id}'.")
        return None
    text = "\n".join(log.text for log in logs)
    if log_callback:
        log_callback(f"Parsing {len(text)} bytes of GC logs for node '{node.id}'...")
    parsed = parser.parse(text, node_id=node.id)
    if log_callback:
        log_callback(f"Analyzing parsed GC data for node '{node.id}'...")
    result = analyzer.analyze(parsed)
    result["_files"] = [log.source_detail for log in logs]
    return result


def _record_nodes(conn, nodes, region: str, env: str, cluster: str, ts: int, log_callback=None) -> list[NodeResult]:
    """Collect -> analyze -> upsert+record each node on an open connection."""
    results: list[NodeResult] = []
    for ordinal, node in enumerate(nodes, start=1):
        try:
            analysis = _analyze_node(node, log_callback=log_callback)
        except Exception as exc:  # surface per-node collection/parse failures
            if log_callback:
                log_callback(f"Failed to process node '{node.id}': {exc}")
            results.append(NodeResult(node.id, "-", node.role, False, f"error: {exc}"))
            continue

        if analysis is None:
            results.append(NodeResult(node.id, "-", node.role, False, "no GC log files found"))
            continue

        metrics = analysis["metrics"]
        index = _index_of(node.id, ordinal)
        heap_max = _heap_max_mb(node.role, metrics["heap_max_mb"])
        inst = build_instance(node, region, env, cluster, index, heap_max)

        if metrics["stw_count"] <= 0:
            store.upsert_instance(conn, inst, collector=analysis["collector"])
            if log_callback:
                log_callback(f"Successfully upserted node '{node.id}' to topology, but no GC events were parsed.")
            results.append(NodeResult(node.id, inst.id, node.role, False, "no GC events parsed yet"))
            continue

        store.upsert_instance(conn, inst, collector=analysis["collector"])
        store.record_metric(conn, inst.id, ts, metrics_to_row(metrics))
        detail = (
            f"{analysis['collector']} | {metrics['stw_count']} GCs "
            f"| p99 {metrics['p99_pause_ms']:.0f}ms | max {metrics['max_pause_ms']:.0f}ms "
            f"| full {metrics['full_count']} | thr {metrics['throughput_pct']:.2f}%"
        )
        if log_callback:
            log_callback(f"Successfully recorded metrics for node '{node.id}': {detail}")
        results.append(NodeResult(node.id, inst.id, node.role, True, detail))
    return results


def ingest_nodes(
    nodes: list[NodeConfig],
    db_path: str,
    region: str = "LOCAL",
    env: str = "DEV",
    cluster: str | None = None,
    now: int | None = None,
    log_callback=None,
) -> list[NodeResult]:
    """Collect, analyze, and record an already-parsed node list (used by the
    dashboard onboarding endpoint, which has nodes parsed from pasted YAML)."""
    cluster = cluster or f"{region}-{env}"
    ts = now if now is not None else (int(time.time()) // 60) * 60
    store.init_db(db_path)
    if log_callback:
        log_callback(f"Connecting to database at {db_path}...")
    with store.connect(db_path) as conn:
        return _record_nodes(conn, nodes, region, env, cluster, ts, log_callback=log_callback)


def ingest(
    config_path: str,
    db_path: str,
    region: str | None = None,
    env: str | None = None,
    now: int | None = None,
) -> list[NodeResult]:
    """Collect, analyze, and record every node in a cluster config file."""
    _, nodes, cfg_region, cfg_env = load_cluster(config_path)
    r = region or cfg_region or "LOCAL"
    e = env or cfg_env or "DEV"
    return ingest_nodes(nodes, db_path, region=r, env=e, now=now)


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest real GC logs into the history store")
    ap.add_argument("--config", required=True, help="cluster config (YAML/JSON)")
    ap.add_argument("--db", default="gc_live.db", help="SQLite history DB to write")
    ap.add_argument("--region", default="LOCAL")
    ap.add_argument("--env", default="DEV")
    args = ap.parse_args()

    results = ingest(args.config, args.db, region=args.region, env=args.env)

    recorded = sum(1 for r in results if r.recorded)
    print(f"ingest -> {args.db}  ({recorded}/{len(results)} nodes recorded)")
    for r in results:
        mark = "OK " if r.recorded else "-- "
        print(f"  {mark}{r.node_id:<16} {r.instance_id:<22} {r.detail}")


if __name__ == "__main__":
    main()
