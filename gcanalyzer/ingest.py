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
import concurrent.futures
import re
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


class JobCancelled(Exception):
    """Raised when an ingest job is cancelled mid-flight."""


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


_INDEX_PATTERNS = (
    re.compile(r"^(?:br|broker)-(\d+)(?:-|_|$)", re.I),
    re.compile(r"^(?:zk|zookeeper)-(\d+)(?:-|_|$)", re.I),
    re.compile(r"^(?:sr|schema-registry)-(\d+)(?:-|_|$)", re.I),
    re.compile(r"^(?:connect)-(\d+)(?:-|_|$)", re.I),
    re.compile(r"^(?:ctrl|controller)-(\d+)(?:-|_|$)", re.I),
)


def _index_from_node_id(node_id: str) -> int | None:
    """Parse br-1-host, zk-2-host, broker-3, etc."""
    for pat in _INDEX_PATTERNS:
        m = pat.match(node_id)
        if m:
            return int(m.group(1))
    tail = node_id.rsplit("-", 1)[-1]
    return int(tail) if tail.isdigit() else None


def _index_of(node_id: str, fallback: int) -> int:
    return _index_from_node_id(node_id) or fallback


def _instance_id_for(cluster: str, node: NodeConfig, ordinal: int, used: set[str]) -> tuple[str, int]:
    index = _index_from_node_id(node.id) or ordinal
    base = f"{cluster}-{node.role}-{index}"
    if base not in used:
        used.add(base)
        return base, index
    safe = re.sub(r"[^a-zA-Z0-9_-]", "-", node.id).strip("-") or f"node-{ordinal}"
    iid = f"{cluster}--{safe}"
    used.add(iid)
    return iid, index


def _heap_max_mb(role: str, observed: float) -> int:
    if observed and observed > 0:
        return int(round(observed))
    return _FALLBACK_HEAP_MB.get(role, _DEFAULT_HEAP_MB)


def build_instance(
    node: NodeConfig, region: str, env: str, cluster: str, index: int, heap_max_mb: int,
    instance_id: str | None = None,
) -> topology.Instance:
    """Construct a topology.Instance so the live node joins the dashboard tree."""
    return topology.Instance(
        id=instance_id or f"{cluster}-{node.role}-{index}",
        region=region,
        env=env,
        cluster=cluster,
        group=_ROLE_TO_GROUP.get(node.role, node.role),
        role=node.role,
        index=index,
        heap_max_mb=heap_max_mb,
        busy_hour_utc=0,
        node_id=node.id,
    )



def _register_instance(conn, node, region, env, cluster, ordinal, used_ids, collector="G1"):
    iid, index = _instance_id_for(cluster, node, ordinal, used_ids)
    heap_max = _heap_max_mb(node.role, 0)
    inst = build_instance(node, region, env, cluster, index, heap_max, instance_id=iid)
    store.upsert_instance(conn, inst, collector=collector)
    return inst


def sync_instances_from_nodes(
    conn,
    nodes: list[NodeConfig],
    region: str,
    env: str,
    cluster: str,
    collector: str = "G1",
) -> int:
    """Upsert instance rows from a cluster config without collecting GC logs."""
    count = 0
    used_ids: set[str] = set()
    for ordinal, node in enumerate(nodes, start=1):
        iid, index = _instance_id_for(cluster, node, ordinal, used_ids)
        heap_max = _heap_max_mb(node.role, 0)
        inst = build_instance(node, region, env, cluster, index, heap_max, instance_id=iid)
        store.upsert_instance(conn, inst, collector=collector)
        count += 1
    return count




def record_analysis_metrics(conn, inst_id: str, parsed, analysis: dict, ts: int | None = None) -> int:
    """Persist hourly buckets from the log plus a latest snapshot row."""
    ts = ts if ts is not None else (int(time.time()) // 60) * 60
    metrics = analysis["metrics"]
    buckets = analyzer.bucket_metrics(parsed)
    written = 0
    if buckets:
        for bts, m in buckets:
            store.record_metric(conn, inst_id, bts, metrics_to_row(m))
            written += 1
    else:
        store.record_metric(conn, inst_id, ts, metrics_to_row(metrics))
        written = 1
    return written


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
    result["_parsed"] = parsed
    return result


def ingest_nodes(
    nodes: list[NodeConfig],
    db_path: str,
    region: str = "LOCAL",
    env: str = "DEV",
    cluster: str | None = None,
    now: int | None = None,
    log_callback=None,
    cancel_check=None,
) -> list[NodeResult]:
    """Collect, analyze, and record an already-parsed node list (used by the
    dashboard onboarding endpoint, which has nodes parsed from pasted YAML)."""
    cluster = cluster or f"{region}-{env}"
    ts = now if now is not None else (int(time.time()) // 60) * 60

    def _cancelled() -> bool:
        return bool(cancel_check and cancel_check())

    if _cancelled():
        raise JobCancelled("Job cancelled by user")

    # 1. Run node analysis concurrently
    if log_callback:
        log_callback(f"Analyzing {len(nodes)} nodes concurrently (max workers: 5)...")

    analysis_results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_node = {}
        for node in nodes:
            # Bind the node_id to log messages for this node
            def node_log_cb(msg: str, nid=node.id):
                if log_callback:
                    try:
                        log_callback(msg, node_id=nid)
                    except TypeError:
                        log_callback(msg)

            future = executor.submit(_analyze_node, node, log_callback=node_log_cb)
            future_to_node[future] = node

        for future in concurrent.futures.as_completed(future_to_node):
            if _cancelled():
                raise JobCancelled("Job cancelled by user")
            node = future_to_node[future]
            try:
                analysis = future.result()
                analysis_results[node.id] = (analysis, None)
            except Exception as exc:
                analysis_results[node.id] = (None, exc)

    if _cancelled():
        raise JobCancelled("Job cancelled by user")

    # 2. Record findings sequentially in a database connection context
    store.init_db(db_path)
    if log_callback:
        log_callback(f"Connecting to database at {db_path} to record findings...")

    results: list[NodeResult] = []
    used_ids: set[str] = set()
    with store.connect(db_path) as conn:
        for ordinal, node in enumerate(nodes, start=1):
            if _cancelled():
                raise JobCancelled("Job cancelled by user")
            res_tuple = analysis_results.get(node.id)
            if not res_tuple:
                results.append(NodeResult(node.id, "-", node.role, False, "skipped"))
                continue

            analysis, exc = res_tuple
            if exc:
                if log_callback:
                    try:
                        log_callback(f"Failed to process node '{node.id}': {exc}", node_id=node.id)
                    except TypeError:
                        log_callback(f"Failed to process node '{node.id}': {exc}")
                inst = _register_instance(conn, node, region, env, cluster, ordinal, used_ids)
                results.append(NodeResult(node.id, inst.id, node.role, False, f"error: {exc}"))
                continue

            if analysis is None:
                inst = _register_instance(conn, node, region, env, cluster, ordinal, used_ids)
                results.append(NodeResult(node.id, inst.id, node.role, False, "no GC log files found"))
                continue

            metrics = analysis["metrics"]
            parsed = analysis.get("_parsed")
            iid, index = _instance_id_for(cluster, node, ordinal, used_ids)
            heap_max = _heap_max_mb(node.role, metrics["heap_max_mb"])
            inst = build_instance(node, region, env, cluster, index, heap_max, instance_id=iid)

            if metrics["stw_count"] <= 0:
                store.upsert_instance(conn, inst, collector=analysis["collector"])
                if log_callback:
                    try:
                        log_callback(f"Successfully upserted node '{node.id}' to topology, but no GC events were parsed.", node_id=node.id)
                    except TypeError:
                        log_callback(f"Successfully upserted node '{node.id}' to topology, but no GC events were parsed.")
                results.append(NodeResult(node.id, inst.id, node.role, False, "no GC events parsed yet"))
                continue

            store.upsert_instance(conn, inst, collector=analysis["collector"])
            if parsed:
                record_analysis_metrics(conn, inst.id, parsed, analysis, ts=ts)
            else:
                store.record_metric(conn, inst.id, ts, metrics_to_row(metrics))
            detail = (
                f"{analysis['collector']} | {metrics['stw_count']} GCs "
                f"| p99 {metrics['p99_pause_ms']:.0f}ms | max {metrics['max_pause_ms']:.0f}ms "
                f"| full {metrics['full_count']} | thr {metrics['throughput_pct']:.2f}%"
            )
            if log_callback:
                try:
                    log_callback(f"Successfully recorded metrics for node '{node.id}': {detail}", node_id=node.id)
                except TypeError:
                    log_callback(f"Successfully recorded metrics for node '{node.id}': {detail}")
            results.append(NodeResult(node.id, inst.id, node.role, True, detail))

    return results


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
