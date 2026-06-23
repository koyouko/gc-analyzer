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
from .collector import NodeConfig, collect, collect_ssh_incremental, read_increment_local
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
    """Stable id from YAML node id — survives config edits without duplicating hosts."""
    index = _index_from_node_id(node.id) or ordinal
    safe = re.sub(r"[^a-zA-Z0-9_-]", "-", node.id).strip("-") or f"node-{ordinal}"
    iid = f"{cluster}--{safe}"
    if iid in used:
        iid = f"{cluster}--{safe}-{ordinal}"
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
    """Upsert YAML nodes and remove stale instance rows for this cluster."""
    used_ids: set[str] = set()
    expected: list[tuple[str, topology.Instance]] = []
    yaml_node_ids = {n.id for n in nodes}
    for ordinal, node in enumerate(nodes, start=1):
        iid, index = _instance_id_for(cluster, node, ordinal, used_ids)
        heap = _heap_max_mb(node.role, 0)
        existing = store.get_instance(conn, iid)
        if existing and (existing["heap_max_mb"] or 0) > heap:
            heap = existing["heap_max_mb"]
        inst = build_instance(node, region, env, cluster, index, heap, instance_id=iid)
        expected.append((iid, inst))

    expected_ids = {iid for iid, _ in expected}
    for iid, inst in expected:
        for row in conn.execute(
            "SELECT id FROM instances WHERE cluster=? AND node_id=? AND id!=?",
            (cluster, inst.node_id, iid),
        ):
            store.migrate_instance(conn, row["id"], iid)

    for row in conn.execute("SELECT id, node_id FROM instances WHERE cluster=?", (cluster,)):
        rid, nid = row["id"], row["node_id"] or ""
        if rid in expected_ids:
            continue
        if nid and nid in yaml_node_ids:
            continue
        store.delete_instance(conn, rid)

    for iid, inst in expected:
        store.upsert_instance(conn, inst, collector=collector)
    return len(expected)




def record_analysis_metrics(
    conn,
    inst_id: str,
    parsed,
    analysis: dict,
    ts: int | None = None,
    *,
    incremental: bool = False,
    new_offsets: dict | None = None,
) -> int:
    """Persist metrics: full log backfill on first collect, one row per incremental tick."""
    ts = ts if ts is not None else (int(time.time()) // 60) * 60
    metrics = analysis["metrics"]
    if incremental:
        store.record_metric(conn, inst_id, ts, metrics_to_row(metrics))
        if new_offsets:
            for fp, stt in new_offsets.items():
                store.set_offset(conn, inst_id, fp, stt["inode"], stt["offset"], ts)
        return 1

    buckets = analyzer.bucket_metrics(parsed)
    written = 0
    if buckets:
        for bts, m in buckets:
            store.record_metric(conn, inst_id, bts, metrics_to_row(m))
            written += 1
    else:
        store.record_metric(conn, inst_id, ts, metrics_to_row(metrics))
        written = 1
    if new_offsets:
        for fp, stt in new_offsets.items():
            store.set_offset(conn, inst_id, fp, stt["inode"], stt["offset"], ts)
    return written


@dataclass(frozen=True)
class _CollectCtx:
    instance_id: str
    prev_offsets: dict
    has_history: bool
    collect_mode: str  # auto | full | incremental


def _should_incremental(ctx: _CollectCtx) -> bool:
    if ctx.collect_mode == "full":
        return False
    if ctx.collect_mode == "incremental":
        return ctx.has_history or bool(ctx.prev_offsets)
    return ctx.has_history or bool(ctx.prev_offsets)


def _collect_log_text(node: NodeConfig, ctx: _CollectCtx, log_callback=None) -> tuple[str, dict | None, bool]:
    """Return (text, new_offsets, used_incremental)."""
    incremental = _should_incremental(ctx)
    if incremental:
        if log_callback:
            log_callback(f"Incremental GC log fetch for '{node.id}' (offsets on file)...")
        if node.source == "local":
            text, new_offsets = read_increment_local(node, ctx.prev_offsets)
        else:
            text, new_offsets = collect_ssh_incremental(node, ctx.prev_offsets, log_callback=log_callback)
        if not text.strip():
            return "", new_offsets, True
        return text, new_offsets, True

    if log_callback:
        log_callback(f"Full GC log fetch for '{node.id}'...")
    logs = collect(node, log_callback=log_callback)
    if not logs:
        return "", None, False
    return "\n".join(log.text for log in logs), None, False


def _analyze_node(node: NodeConfig, ctx: _CollectCtx, log_callback=None) -> dict | None:
    """collect -> parse -> analyze for one node."""
    text, new_offsets, used_inc = _collect_log_text(node, ctx, log_callback=log_callback)
    if not text.strip():
        if used_inc:
            if log_callback:
                log_callback(f"No new GC log bytes for '{node.id}'.")
            return {"_empty_incremental": True, "_parsed": None, "metrics": {}, "collector": "G1"}
        if log_callback:
            log_callback(f"No GC log files collected for node '{node.id}'.")
        return None
    if log_callback:
        log_callback(f"Parsing {len(text)} bytes of GC logs for node '{node.id}'...")
    parsed = parser.parse(text, node_id=node.id)
    if log_callback:
        log_callback(f"Analyzing parsed GC data for node '{node.id}'...")
    result = analyzer.analyze(parsed)
    result["_parsed"] = parsed
    result["_incremental"] = used_inc
    result["_new_offsets"] = new_offsets
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
    collect_mode: str = "auto",
    progress_callback=None,
) -> list[NodeResult]:
    """Collect, analyze, and record an already-parsed node list (used by the
    dashboard onboarding endpoint, which has nodes parsed from pasted YAML)."""
    cluster = cluster or f"{region}-{env}"
    ts = now if now is not None else (int(time.time()) // 60) * 60

    def _cancelled() -> bool:
        return bool(cancel_check and cancel_check())

    if _cancelled():
        raise JobCancelled("Job cancelled by user")

    total_nodes = len(nodes) or 1

    def _progress(pct: int, message: str, node_entry: dict | None = None) -> None:
        if not progress_callback:
            return
        progress_callback(pct, message, node_entry)

    store.init_db(db_path)
    contexts: dict[str, _CollectCtx] = {}
    with store.connect(db_path) as conn:
        used_pre: set[str] = set()
        for ordinal, node in enumerate(nodes, start=1):
            iid, _ = _instance_id_for(cluster, node, ordinal, used_pre)
            prev = store.get_offsets(conn, iid)
            has_hist = store.latest_row(conn, iid) is not None
            contexts[node.id] = _CollectCtx(iid, prev, has_hist, collect_mode)

    _progress(10, f"Starting collection of {total_nodes} nodes")
    if log_callback:
        mode_label = collect_mode if collect_mode != "auto" else "auto (incremental when history exists)"
        log_callback(f"Collecting and analyzing {len(nodes)} nodes ({mode_label}, max workers: 5)...")

    analysis_results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_node = {}
        for node in nodes:
            ctx = contexts[node.id]

            def node_log_cb(msg: str, nid=node.id):
                if log_callback:
                    try:
                        log_callback(msg, node_id=nid)
                    except TypeError:
                        log_callback(msg)

            future = executor.submit(_analyze_node, node, ctx, log_callback=node_log_cb)
            future_to_node[future] = node

        collected = 0
        for future in concurrent.futures.as_completed(future_to_node):
            if _cancelled():
                raise JobCancelled("Job cancelled by user")
            node = future_to_node[future]
            try:
                analysis = future.result()
                analysis_results[node.id] = (analysis, None)
            except Exception as exc:
                analysis_results[node.id] = (None, exc)
            collected += 1
            _progress(10 + int(70 * collected / total_nodes), f"Collected {collected}/{total_nodes} nodes")

    if _cancelled():
        raise JobCancelled("Job cancelled by user")

    # 2. Record findings sequentially in a database connection context
    if log_callback:
        log_callback(f"Connecting to database at {db_path} to record findings...")

    results: list[NodeResult] = []
    used_ids: set[str] = set()
    recorded = 0

    def _append_result(nr: NodeResult) -> None:
        nonlocal recorded
        results.append(nr)
        recorded += 1
        entry = {
            "node_id": nr.node_id,
            "instance_id": nr.instance_id,
            "role": nr.role,
            "recorded": nr.recorded,
            "detail": nr.detail,
        }
        _progress(80 + int(18 * recorded / total_nodes), f"Recorded {recorded}/{total_nodes} nodes", entry)

    with store.connect(db_path) as conn:
        sync_instances_from_nodes(conn, nodes, region, env, cluster)
        _progress(78, "Writing results to database")
        for ordinal, node in enumerate(nodes, start=1):
            if _cancelled():
                raise JobCancelled("Job cancelled by user")
            res_tuple = analysis_results.get(node.id)
            if not res_tuple:
                _append_result(NodeResult(node.id, "-", node.role, False, "skipped"))
                continue

            analysis, exc = res_tuple
            if exc:
                if log_callback:
                    try:
                        log_callback(f"Failed to process node '{node.id}': {exc}", node_id=node.id)
                    except TypeError:
                        log_callback(f"Failed to process node '{node.id}': {exc}")
                inst = _register_instance(conn, node, region, env, cluster, ordinal, used_ids)
                _append_result(NodeResult(node.id, inst.id, node.role, False, f"error: {exc}"))
                continue

            if analysis is None:
                inst = _register_instance(conn, node, region, env, cluster, ordinal, used_ids)
                _append_result(NodeResult(node.id, inst.id, node.role, False, "no GC log files found"))
                continue

            if analysis.get("_empty_incremental"):
                iid, index = _instance_id_for(cluster, node, ordinal, used_ids)
                inst = build_instance(node, region, env, cluster, index, _heap_max_mb(node.role, 0), instance_id=iid)
                store.upsert_instance(conn, inst, collector="G1")
                new_off = analysis.get("_new_offsets")
                if new_off:
                    for fp, stt in new_off.items():
                        store.set_offset(conn, inst.id, fp, stt["inode"], stt["offset"], ts)
                _append_result(NodeResult(node.id, inst.id, node.role, True, "no new GC bytes (incremental)"))
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
                _append_result(NodeResult(node.id, inst.id, node.role, False, "no GC events parsed yet"))
                continue

            store.upsert_instance(conn, inst, collector=analysis["collector"])
            if parsed:
                record_analysis_metrics(
                    conn, inst.id, parsed, analysis, ts=ts,
                    incremental=analysis.get("_incremental", False),
                    new_offsets=analysis.get("_new_offsets"),
                )
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
            _append_result(NodeResult(node.id, inst.id, node.role, True, detail))

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
