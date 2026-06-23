"""
Periodic re-collection scheduler.

Runs as a background asyncio task inside the FastAPI app (started on startup).
Every interval it walks each onboarded cluster config in clusters/*.yaml and, for
every node, reads its GC log(s) *from where it left off* (byte offset + inode
tracked in the collector_state table), parses only the newly appended events,
and records one metric point. Reading the delta — not the whole file — means each
point reflects just that interval, so the trend charts get a true time series.

After collecting, it prunes metrics older than the retention window (default 2
years) so the store stays bounded.

    python -m gcanalyzer.scheduler --db gc_live.db --interval 300
"""

from __future__ import annotations

import argparse
import asyncio
import glob
import os
import time
import concurrent.futures
import json

from . import analyzer, config, ingest, parser, store
from .collector import NodeConfig, collect_ssh, collect_ssh_incremental, read_increment_local

CLUSTERS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "clusters")
DEFAULT_INTERVAL_S = int(os.environ.get("GC_SCHED_INTERVAL", "300"))
RETENTION_DAYS = int(os.environ.get("GC_RETENTION_DAYS", "730"))  # 2 years
CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.json")


def _cluster_configs() -> list[str]:
    if not os.path.isdir(CLUSTERS_DIR):
        return []
    return sorted(os.path.join(CLUSTERS_DIR, f) for f in os.listdir(CLUSTERS_DIR) if f.endswith(".yaml"))


def get_max_concurrency() -> int:
    concurrency = 20
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r") as fh:
                data = json.load(fh)
                if isinstance(data, dict) and "concurrency" in data:
                    concurrency = int(data["concurrency"])
        except Exception:
            pass
    return int(os.environ.get("GC_SCHED_CONCURRENCY", str(concurrency)))


def _derive_identity(cluster_name: str) -> tuple[str, str, str]:
    parts = cluster_name.split("-", 1)
    region = parts[0]
    env = parts[1] if len(parts) > 1 else "PROD"
    return region, env, cluster_name


def _scrape_node_task(node: NodeConfig, ts: int, prev_offsets: dict) -> dict:
    try:
        if node.source == "local":
            text, new_offsets = read_increment_local(node, prev_offsets)
        else:
            text, new_offsets = collect_ssh_incremental(node, prev_offsets)
        return {
            "success": True,
            "text": text,
            "new_offsets": new_offsets,
            "error": None
        }
    except Exception as e:
        return {
            "success": False,
            "text": "",
            "new_offsets": {},
            "error": str(e)
        }


def tick(db_path: str, now: int | None = None, on_tick_start=None, on_node_result=None, on_tick_complete=None, is_running_cb=None) -> dict:
    """One scheduler pass: incremental collect every cluster, then prune."""
    store.init_db(db_path)
    ts = now if now is not None else (int(time.time()) // 60) * 60
    
    concurrency = get_max_concurrency()
    
    # 1. Load configs and prepare scrape tasks on the main thread
    tasks = []
    configs = []
    with store.connect(db_path) as conn:
        for cfg_path in _cluster_configs():
            try:
                cluster_name, nodes, cfg_region, cfg_env = config.load_cluster(cfg_path)
            except Exception as e:
                if on_node_result:
                    on_node_result(None, f"Malformed config {cfg_path}: {e}", False)
                continue
            
            derived_region, derived_env, cluster = _derive_identity(cluster_name)
            if is_running_cb and is_running_cb(cluster):
                continue
            region = cfg_region or derived_region
            env = cfg_env or derived_env
            configs.append((cluster, region, env, nodes))
            
            used_ids: set[str] = set()
            for ordinal, node in enumerate(nodes, start=1):
                instance_id, index = ingest._instance_id_for(cluster, node, ordinal, used_ids)
                prev_offsets = store.get_offsets(conn, instance_id)
                
                tasks.append({
                    "node": node,
                    "ordinal": ordinal,
                    "region": region,
                    "env": env,
                    "cluster": cluster,
                    "instance_id": instance_id,
                    "prev_offsets": prev_offsets
                })
    # Call on_tick_start for each cluster in configs before starting concurrent scrapers
    for cluster, region, env, nodes in configs:
        if on_tick_start:
            on_tick_start(ts, cluster, region, env, len(nodes))

    # 2. Run scrapers concurrently
    scrape_results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        future_to_task = {
            executor.submit(
                _scrape_node_task,
                t["node"],
                ts,
                t["prev_offsets"]
            ): t for t in tasks
        }
        for future in concurrent.futures.as_completed(future_to_task):
            task = future_to_task[future]
            try:
                res = future.result()
                scrape_results[task["instance_id"]] = res
            except Exception as e:
                scrape_results[task["instance_id"]] = {
                    "success": False,
                    "text": "",
                    "new_offsets": {},
                    "error": str(e)
                }

    # 3. Process outcomes sequentially (saves to SQLite in a thread-safe way)
    collected = 0
    clusters_count = len(configs)
    
    with store.connect(db_path) as conn:
        for cluster, region, env, nodes in configs:
            cluster_collected = 0
            used_ids: set[str] = set()
            for ordinal, node in enumerate(nodes, start=1):
                instance_id, index = ingest._instance_id_for(cluster, node, ordinal, used_ids)
                res = scrape_results.get(instance_id)
                if not res:
                    continue
                
                if not res["success"]:
                    if on_node_result:
                        on_node_result(cluster, f"Failed to scrape node '{node.id}': {res['error']}", False, node.id)
                    continue
                
                for fp, stt in res["new_offsets"].items():
                    store.set_offset(conn, instance_id, fp, stt["inode"], stt["offset"], ts)
                
                text = res["text"]
                if not text.strip():
                    if on_node_result:
                        on_node_result(cluster, f"Node '{node.id}' scraped but no new GC events found.", True, node.id)
                    continue
                
                try:
                    parsed = parser.parse(text, node_id=node.id)
                    analysis = analyzer.analyze(parsed)
                    metrics = analysis["metrics"]
                    
                    if metrics["stw_count"] <= 0:
                        if on_node_result:
                            on_node_result(cluster, f"Node '{node.id}' scraped but no new GC events parsed.", True, node.id)
                        continue
                    
                    heap_max = ingest._heap_max_for_instance(conn, node.role, metrics["heap_max_mb"], cluster, instance_id)
                    inst = ingest.build_instance(node, region, env, cluster, index, heap_max, instance_id=instance_id)
                    store.upsert_instance(conn, inst, collector=analysis["collector"])
                    ingest.record_analysis_metrics(conn, inst.id, parsed, analysis, ts=ts, incremental=True, new_offsets=res.get("new_offsets"))
                    
                    cluster_collected += 1
                    collected += 1
                    if on_node_result:
                        on_node_result(cluster, f"Successfully scraped node '{node.id}' ({metrics['stw_count']} GC events)", True, node.id)
                except Exception as exc:
                    if on_node_result:
                        on_node_result(cluster, f"Failed to parse/record node '{node.id}': {exc}", False, node.id)
            
            if on_tick_complete:
                on_tick_complete(ts, cluster, {"points": cluster_collected, "total": len(nodes)})

        pruned = store.prune_before(conn, ts - RETENTION_DAYS * 86400)
    
    summary = {"ts": ts, "clusters": clusters_count, "points": collected, "pruned": pruned}
    return summary


async def scheduler_loop(
    db_path: str,
    interval: int = DEFAULT_INTERVAL_S,
    on_tick_start=None,
    on_node_result=None,
    on_tick_complete=None,
    is_running_cb=None
) -> None:
    while True:
        try:
            summary = await asyncio.to_thread(
                tick,
                db_path,
                None,
                on_tick_start,
                on_node_result,
                on_tick_complete,
                is_running_cb
            )
            print(f"[scheduler] {summary['clusters']} clusters, "
                  f"{summary['points']} points, pruned {summary['pruned']}")
        except Exception as exc:  # never let the loop die
            print(f"[scheduler] tick error: {exc}")
        await asyncio.sleep(interval)


def main() -> None:
    ap = argparse.ArgumentParser(description="GC Analyzer re-collection scheduler")
    ap.add_argument("--db", default="gc_live.db")
    ap.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_S)
    ap.add_argument("--once", action="store_true", help="run a single tick and exit")
    args = ap.parse_args()
    if args.once:
        print(tick(args.db))
    else:
        asyncio.run(scheduler_loop(args.db, args.interval))


if __name__ == "__main__":
    main()
