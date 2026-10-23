"""
Live ingest bridge for Apache Kafka 2.8.1 & ZooKeeper JMX/Prometheus metrics
and operational logs (server.log, controller.log, state-change.log, zookeeper.log).

Mirrors sar_ingest.py:
    collect (Prometheus/JMX + SSH/local logs) -> kafka_analyzer.parse_kafka_logs
        -> store.record_kafka_metric + store.record_kafka_log_events
"""

from __future__ import annotations

import concurrent.futures
import os
import re
import time
import urllib.request
from dataclasses import dataclass

from . import kafka_analyzer, store
from .collector import NodeConfig, collect_kafka_logs
from .config import load_cluster
from .ingest import _instance_id_for


@dataclass(frozen=True)
class KafkaNodeResult:
    node_id: str
    instance_id: str
    recorded: bool
    events_written: int
    detail: str


def _scrape_jmx_exporter_text(url: str, timeout: float = 4.0) -> dict:
    """Parse Prometheus text exposition format from a JMX Exporter endpoint."""
    try:
        req = urllib.request.Request(url, headers={"Accept": "text/plain"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read(2 * 1024 * 1024).decode("utf-8", errors="replace")
    except Exception:
        return {}

    metrics: dict = {}
    patterns = {
        "under_replicated_partitions": re.compile(r"^kafka_server_replicamanager_underreplicatedpartitions\b[^\n]*?\s+([0-9.eE+-]+)$", re.M),
        "offline_partitions_count": re.compile(r"^kafka_controller_kafkacontroller_offlinepartitionscount\b[^\n]*?\s+([0-9.eE+-]+)$", re.M),
        "active_controller_count": re.compile(r"^kafka_controller_kafkacontroller_activecontrollercount\b[^\n]*?\s+([0-9.eE+-]+)$", re.M),
        "partition_count": re.compile(r"^kafka_server_replicamanager_partitioncount\b[^\n]*?\s+([0-9.eE+-]+)$", re.M),
        "leader_count": re.compile(r"^kafka_server_replicamanager_leadercount\b[^\n]*?\s+([0-9.eE+-]+)$", re.M),
        "produce_p99_ms": re.compile(r'^kafka_network_requestmetrics_totaltimems\{[^}]*request="Produce"[^}]*quantile="0\.99"[^}]*\}\s+([0-9.eE+-]+)$', re.M),
        "fetch_consumer_p99_ms": re.compile(r'^kafka_network_requestmetrics_totaltimems\{[^}]*request="FetchConsumer"[^}]*quantile="0\.99"[^}]*\}\s+([0-9.eE+-]+)$', re.M),
        "fetch_follower_p99_ms": re.compile(r'^kafka_network_requestmetrics_totaltimems\{[^}]*request="FetchFollower"[^}]*quantile="0\.99"[^}]*\}\s+([0-9.eE+-]+)$', re.M),
        "zk_avg_latency_ms": re.compile(r"^zookeeper_avg_request_latency\b[^\n]*?\s+([0-9.eE+-]+)$", re.M),
        "zk_max_latency_ms": re.compile(r"^zookeeper_max_request_latency\b[^\n]*?\s+([0-9.eE+-]+)$", re.M),
        "zk_outstanding_requests": re.compile(r"^zookeeper_outstanding_requests\b[^\n]*?\s+([0-9.eE+-]+)$", re.M),
        "zk_alive_connections": re.compile(r"^zookeeper_num_alive_connections\b[^\n]*?\s+([0-9.eE+-]+)$", re.M),
        "zk_pending_syncs": re.compile(r"^zookeeper_pending_syncs\b[^\n]*?\s+([0-9.eE+-]+)$", re.M),
    }
    for key, pat in patterns.items():
        m = pat.search(body)
        if m:
            try:
                metrics[key] = float(m.group(1))
            except ValueError:
                pass
    return metrics


def ingest_kafka_node(
    node: NodeConfig,
    instance_id: str,
    db_path: str,
    now: int | None = None,
    log_callback=None,
) -> KafkaNodeResult:
    ts = now if now is not None else (int(time.time()) // 60) * 60
    prom_url = node.prometheus_url or os.environ.get("GC_PROMETHEUS_URL")

    # 1. Scrape Prometheus or JMX exporter if configured
    scraped: dict = {}
    if prom_url:
        scraped.update(
            kafka_analyzer.scrape_prometheus_kafka_metrics(
                prom_url, instance_filter=node.host or node.id
            )
        )
    if node.jmx_exporter_url:
        scraped.update(_scrape_jmx_exporter_text(node.jmx_exporter_url))

    # 2. Collect operational Kafka / ZooKeeper logs (server.log, controller.log, zookeeper.log)
    logs = collect_kafka_logs(node, log_callback=log_callback)
    combined_text = "\n".join(item.text for item in logs if item.text.strip())
    parsed_logs = (
        kafka_analyzer.parse_kafka_logs(combined_text, node_id=node.id, role=node.role)
        if combined_text
        else None
    )

    if not scraped and (not parsed_logs or not parsed_logs.events):
        return KafkaNodeResult(
            node.id, instance_id, False, 0, "no Prometheus/JMX metrics or new Kafka/ZK log events"
        )

    store.init_db(db_path)
    with store.connect(db_path) as conn:
        existing = store.kafka_latest_row(conn, instance_id) or {}
        merged = dict(existing)
        merged.update(scraped)
        merged["kafka_version"] = node.kafka_version or "2.8.1"

        events_written = 0
        if parsed_logs and parsed_logs.events:
            c = parsed_logs.counts
            if c.get("urp_detected", 0) > 0:
                merged["under_replicated_partitions"] = max(
                    int(merged.get("under_replicated_partitions") or 0), c["urp_detected"]
                )
            if c.get("offline_partition", 0) > 0:
                merged["offline_partitions_count"] = max(
                    int(merged.get("offline_partitions_count") or 0), c["offline_partition"]
                )
            if c.get("isr_shrink", 0) > 0:
                merged["isr_shrinks_per_sec"] = round(
                    float(merged.get("isr_shrinks_per_sec") or 0.0) + c["isr_shrink"] / 60.0, 3
                )
            if c.get("isr_expand", 0) > 0:
                merged["isr_expands_per_sec"] = round(
                    float(merged.get("isr_expands_per_sec") or 0.0) + c["isr_expand"] / 60.0, 3
                )
            if c.get("zk_fsync_stall", 0) > 0:
                merged["zk_fsync_warning_count"] = int(c["zk_fsync_stall"])
                merged["zk_max_latency_ms"] = max(
                    float(merged.get("zk_max_latency_ms") or 0.0), float(parsed_logs.max_fsync_ms)
                )
            if c.get("zk_session_expired", 0) > 0:
                merged["zk_session_expire_count"] = int(c["zk_session_expired"])
            merged["log_error_count"] = int(parsed_logs.error_count)
            merged["log_warn_count"] = int(parsed_logs.warn_count)

            ev_dicts = [
                {
                    "ts": e.ts or ts,
                    "level": e.level,
                    "component": e.component,
                    "category": e.category,
                    "severity": e.severity,
                    "summary": e.summary,
                    "raw_line": e.raw_line,
                }
                for e in parsed_logs.events
            ]
            events_written = store.record_kafka_log_events(conn, instance_id, ev_dicts, default_ts=ts)

        store.record_kafka_metric(conn, instance_id, ts, merged)

    detail = f"recorded Kafka 2.8.1 / ZK metrics ({events_written} log events)"
    return KafkaNodeResult(node.id, instance_id, True, events_written, detail)


def ingest_kafka_log_text(
    text: str,
    instance_id: str,
    db_path: str,
    role: str = "broker",
    now: int | None = None,
) -> dict:
    """Ingest pasted/uploaded Kafka 2.8.1 (server.log / controller.log / state-change.log)
    or ZooKeeper (zookeeper.log) log text for an onboarded instance."""
    ts = now if now is not None else (int(time.time()) // 60) * 60
    if not (text or "").strip():
        return {
            "instance_id": instance_id,
            "recorded": False,
            "events_parsed": 0,
            "error_count": 0,
            "warn_count": 0,
            "counts": {},
            "detail": "Empty log content",
        }

    parsed = kafka_analyzer.parse_kafka_logs(text, node_id=instance_id, role=role)
    store.init_db(db_path)
    with store.connect(db_path) as conn:
        ev_dicts = [
            {
                "ts": e.ts or ts,
                "level": e.level,
                "component": e.component,
                "category": e.category,
                "severity": e.severity,
                "summary": e.summary,
                "raw_line": e.raw_line,
            }
            for e in parsed.events
        ]
        written = store.record_kafka_log_events(conn, instance_id, ev_dicts, default_ts=ts)

        existing = store.kafka_latest_row(conn, instance_id) or {}
        merged = dict(existing)
        c = parsed.counts
        if c.get("urp_detected", 0) > 0:
            merged["under_replicated_partitions"] = max(
                int(merged.get("under_replicated_partitions") or 0), c["urp_detected"]
            )
        if c.get("offline_partition", 0) > 0:
            merged["offline_partitions_count"] = max(
                int(merged.get("offline_partitions_count") or 0), c["offline_partition"]
            )
        if c.get("isr_shrink", 0) > 0:
            merged["isr_shrinks_per_sec"] = round(
                float(merged.get("isr_shrinks_per_sec") or 0.0) + c["isr_shrink"] / 60.0, 3
            )
        if c.get("zk_fsync_stall", 0) > 0:
            merged["zk_fsync_warning_count"] = int(c["zk_fsync_stall"])
            merged["zk_max_latency_ms"] = max(
                float(merged.get("zk_max_latency_ms") or 0.0), float(parsed.max_fsync_ms)
            )
        if c.get("zk_session_expired", 0) > 0:
            merged["zk_session_expire_count"] = int(c["zk_session_expired"])
        merged["log_error_count"] = int(merged.get("log_error_count") or 0) + int(parsed.error_count)
        merged["log_warn_count"] = int(merged.get("log_warn_count") or 0) + int(parsed.warn_count)
        store.record_kafka_metric(conn, instance_id, ts, merged)

    return {
        "instance_id": instance_id,
        "recorded": True,
        "events_parsed": written,
        "error_count": parsed.error_count,
        "warn_count": parsed.warn_count,
        "counts": parsed.counts,
        "max_fsync_ms": parsed.max_fsync_ms,
        "expired_sessions": parsed.expired_sessions,
        "affected_partitions": parsed.affected_partitions,
        "detail": (
            f"Parsed {written} operational event(s) "
            f"({parsed.error_count} ERROR, {parsed.warn_count} WARN) across {parsed.lines_scanned} lines"
        ),
    }


def ingest_kafka_nodes(
    nodes: list[NodeConfig],
    db_path: str,
    cluster: str,
    now: int | None = None,
    log_callback=None,
    cancel_check=None,
    max_workers: int = 5,
) -> list[KafkaNodeResult]:
    ts = now if now is not None else (int(time.time()) // 60) * 60
    used_ids: set[str] = set()
    targets: list[tuple[NodeConfig, str]] = []
    for ordinal, node in enumerate(nodes, start=1):
        iid, _ = _instance_id_for(cluster, node, ordinal, used_ids)
        targets.append((node, iid))

    results: list[KafkaNodeResult] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_target = {
            executor.submit(ingest_kafka_node, node, iid, db_path, now=ts, log_callback=log_callback): (node, iid)
            for node, iid in targets
        }
        for future in concurrent.futures.as_completed(future_to_target):
            if cancel_check and cancel_check():
                break
            node, iid = future_to_target[future]
            try:
                results.append(future.result())
            except Exception as exc:
                results.append(KafkaNodeResult(node.id, iid, False, 0, f"error: {exc}"))
    return results


def ingest_kafka(config_path: str, db_path: str, now: int | None = None) -> list[KafkaNodeResult]:
    cluster_name, nodes, _region, _env = load_cluster(config_path)
    return ingest_kafka_nodes(nodes, db_path, cluster=cluster_name, now=now)
