"""
Live ingest bridge: SAR (sysstat) reports -> host_metrics history store.

Mirrors ingest.py's contract but for OS-level metrics instead of GC logs:

    collect (local/ssh) -> sar_parser.parse -> sar_analyzer.bucket_metrics
        -> store.record_host_metric

It rides on the *same* instance inventory ingest.py already maintains (each
instance_id is `{cluster}--{node_id}`, one row per JVM/host) — sar_ingest never
creates instances itself, it just records host_metrics rows keyed by the same
instance_id so correlate.py and scaling_advisor.py can join the two tables
without any extra mapping step.

Unlike GC log collection, there's no byte offset to track: `sar -A` / `sadf -j`
re-report the whole day's accumulated samples on every call. "Incremental"
here just means dedup-by-timestamp — store.get_sar_state()/set_sar_state()
remember the newest sample timestamp already recorded per instance, and only
samples newer than that get written.

Run once (or on a schedule / loop) against a cluster config:

    python -m gcanalyzer.sar_ingest --config cluster.yaml --db gc_live.db
"""

from __future__ import annotations

import argparse
import concurrent.futures
import time
from dataclasses import dataclass

from . import sar_analyzer, sar_parser, store
from .collector import NodeConfig, collect_sar
from .config import load_cluster
from .ingest import _instance_id_for


@dataclass(frozen=True)
class SarNodeResult:
    node_id: str
    instance_id: str
    recorded: bool
    samples_written: int
    detail: str


def _bucketed_rows(parsed: sar_parser.ParsedSar) -> list[tuple[int, dict]]:
    return sar_analyzer.bucket_metrics(parsed)


def ingest_sar_node(
    node: NodeConfig, instance_id: str, db_path: str, now: int | None = None, log_callback=None,
) -> SarNodeResult:
    ts = now if now is not None else (int(time.time()) // 60) * 60

    if not node.sar_enabled:
        return SarNodeResult(node.id, instance_id, False, 0, "sar disabled for this node")

    collected = collect_sar(node, log_callback=log_callback)
    if collected.error and not collected.text.strip():
        return SarNodeResult(node.id, instance_id, False, 0, f"collection error: {collected.error}")
    if not collected.text.strip():
        return SarNodeResult(node.id, instance_id, False, 0, "no SAR output collected")

    parsed = sar_parser.parse(
        collected.text, node_id=node.id, fmt_hint=collected.fmt_hint, report_date=collected.report_date,
    )
    if not parsed.samples:
        warn = f" ({'; '.join(parsed.warnings)})" if parsed.warnings else ""
        return SarNodeResult(node.id, instance_id, False, 0, f"no SAR samples parsed{warn}")

    store.init_db(db_path)
    with store.connect(db_path) as conn:
        last_ts = store.get_sar_state(conn, instance_id)
        new_samples = [s for s in parsed.samples if last_ts is None or s.ts > last_ts]
        if not new_samples:
            return SarNodeResult(node.id, instance_id, True, 0, "no new SAR samples since last collection")

        subset = sar_parser.ParsedSar(
            node_id=parsed.node_id, source_format=parsed.source_format, samples=new_samples,
            hostname=parsed.hostname,
        )
        written = 0
        for bts, m in _bucketed_rows(subset):
            store.record_host_metric(conn, instance_id, bts, m)
            written += 1
        store.set_sar_state(conn, instance_id, max(s.ts for s in new_samples), ts)

    detail = f"{parsed.source_format} | {len(new_samples)} new sample(s) -> {written} bucket row(s)"
    return SarNodeResult(node.id, instance_id, True, written, detail)


def ingest_sar_nodes(
    nodes: list[NodeConfig],
    db_path: str,
    cluster: str,
    now: int | None = None,
    log_callback=None,
    cancel_check=None,
    max_workers: int = 5,
) -> list[SarNodeResult]:
    """SAR companion to ingest.ingest_nodes(): same instance-id derivation, run
    concurrently per node, results recorded sequentially."""
    ts = now if now is not None else (int(time.time()) // 60) * 60
    used_ids: set[str] = set()
    targets: list[tuple[NodeConfig, str]] = []
    for ordinal, node in enumerate(nodes, start=1):
        iid, _ = _instance_id_for(cluster, node, ordinal, used_ids)
        targets.append((node, iid))

    def _cancelled() -> bool:
        return bool(cancel_check and cancel_check())

    results: list[SarNodeResult] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_target = {
            executor.submit(ingest_sar_node, node, iid, db_path, now=ts, log_callback=log_callback): (node, iid)
            for node, iid in targets
        }
        for future in concurrent.futures.as_completed(future_to_target):
            if _cancelled():
                break
            node, iid = future_to_target[future]
            try:
                results.append(future.result())
            except Exception as exc:  # noqa: BLE001
                results.append(SarNodeResult(node.id, iid, False, 0, f"error: {exc}"))
    return results


def ingest_sar(config_path: str, db_path: str, now: int | None = None) -> list[SarNodeResult]:
    cluster_name, nodes, _region, _env = load_cluster(config_path)
    return ingest_sar_nodes(nodes, db_path, cluster=cluster_name, now=now)


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest SAR/sysstat host metrics into the history store")
    ap.add_argument("--config", required=True, help="cluster config (YAML/JSON)")
    ap.add_argument("--db", default="gc_live.db", help="SQLite history DB to write")
    args = ap.parse_args()

    results = ingest_sar(args.config, args.db)
    recorded = sum(1 for r in results if r.recorded)
    print(f"sar_ingest -> {args.db}  ({recorded}/{len(results)} nodes recorded)")
    for r in results:
        mark = "OK " if r.recorded else "-- "
        print(f"  {mark}{r.node_id:<16} {r.instance_id:<22} {r.detail}")


if __name__ == "__main__":
    main()
