"""
Seed the SQLite store with 30 days of hourly Kafka 2.8.1 & ZooKeeper 3.5.9
JMX/Prometheus metrics and parsed operational log events (server.log,
controller.log, state-change.log, zookeeper.log), correlated with the GC and
Host (SAR) incidents.

Run:  python -m seed.seed_kafka_history
"""

from __future__ import annotations

import math
import os
import random
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from gcanalyzer import kafka_analyzer, ml_insights, store, topology  # noqa: E402
from seed.seed_history import build_incidents, diurnal  # noqa: E402

random.seed(23)

DAYS = 30
HOUR = 3600


def base_kafka_row(inst, ts: int) -> dict:
    hour = (ts // HOUR) % 24
    load = diurnal(hour, inst.busy_hour_utc)
    role = inst.role
    is_active_ctrl = (
        (inst.cluster == "DEMO-ZK" and inst.node_id == "broker-1")
        or (inst.cluster == "DEMO-KRAFT" and inst.node_id == "controller-1")
    )
    zk_state = ""
    if role == "zookeeper":
        zk_state = "leader" if inst.node_id == "zookeeper-1" else "follower"

    if role == "broker":
        msg_in = (18500 + 28000 * load) * random.uniform(0.92, 1.08)
        bytes_in = msg_in * random.uniform(780, 1150)
        bytes_out = bytes_in * random.uniform(1.85, 2.45)
        req_idle = max(22.0, min(92.0, 82.0 - 34.0 * load + random.uniform(-3.0, 3.0)))
        net_idle = max(28.0, min(95.0, 88.0 - 26.0 * load + random.uniform(-2.5, 2.5)))
        prod_p99 = (14.0 + 28.0 * load) * random.uniform(0.88, 1.15)
        fetch_c_p99 = (11.0 + 22.0 * load) * random.uniform(0.9, 1.12)
        fetch_f_p99 = (8.0 + 16.0 * load) * random.uniform(0.9, 1.12)
        req_q = int(max(1, round(4 + 18 * load + random.uniform(-2, 3))))
        resp_q = int(max(1, round(2 + 9 * load + random.uniform(-1, 2))))
        flush_p99 = round(3.5 + 9.0 * load + random.uniform(-1.0, 2.0), 2)
        return {
            "kafka_version": "2.8.1",
            "under_replicated_partitions": 0,
            "offline_partitions_count": 0,
            "active_controller_count": 1 if is_active_ctrl else 0,
            "partition_count": 420,
            "leader_count": 140,
            "isr_shrinks_per_sec": 0.0,
            "isr_expands_per_sec": 0.0,
            "unclean_leader_elections_per_sec": 0.0,
            "leader_election_rate_per_sec": round(random.uniform(0.0, 0.02), 3),
            "messages_in_per_sec": round(msg_in, 1),
            "bytes_in_per_sec": round(bytes_in, 0),
            "bytes_out_per_sec": round(bytes_out, 0),
            "bytes_rejected_per_sec": 0.0,
            "failed_produce_per_sec": 0.0,
            "failed_fetch_per_sec": 0.0,
            "request_handler_avg_idle_pct": round(req_idle, 1),
            "network_processor_avg_idle_pct": round(net_idle, 1),
            "produce_latency_p99_ms": round(prod_p99, 1),
            "fetch_consumer_latency_p99_ms": round(fetch_c_p99, 1),
            "fetch_follower_latency_p99_ms": round(fetch_f_p99, 1),
            "request_queue_size": req_q,
            "response_queue_size": resp_q,
            "log_flush_rate_ms_p99": flush_p99,
            "zk_avg_latency_ms": round(1.8 + 2.2 * load + random.uniform(-0.4, 0.6), 2),
            "zk_max_latency_ms": round(9.0 + 14.0 * load + random.uniform(-2.0, 4.0), 1),
            "zk_outstanding_requests": int(round(1 + 3 * load)),
            "zk_alive_connections": 1,
            "zk_znode_count": 0,
            "zk_watch_count": 0,
            "zk_server_state": "",
            "zk_synced_followers": 0,
            "zk_pending_syncs": 0,
            "zk_packets_received_per_sec": 0.0,
            "zk_packets_sent_per_sec": 0.0,
            "zk_fsync_warning_count": 0,
            "zk_session_expire_count": 0,
            "log_error_count": 0,
            "log_warn_count": 0,
        }

    if role == "zookeeper":
        zk_lat = (1.4 + 2.4 * load) * random.uniform(0.85, 1.18)
        zk_max = (8.0 + 18.0 * load) * random.uniform(0.9, 1.25)
        pkts = (420 + 680 * load) * random.uniform(0.9, 1.1)
        return {
            "kafka_version": "2.8.1-zk3.5.9",
            "under_replicated_partitions": 0,
            "offline_partitions_count": 0,
            "active_controller_count": 0,
            "partition_count": 0,
            "leader_count": 0,
            "isr_shrinks_per_sec": 0.0,
            "isr_expands_per_sec": 0.0,
            "unclean_leader_elections_per_sec": 0.0,
            "leader_election_rate_per_sec": 0.0,
            "messages_in_per_sec": 0.0,
            "bytes_in_per_sec": 0.0,
            "bytes_out_per_sec": 0.0,
            "bytes_rejected_per_sec": 0.0,
            "failed_produce_per_sec": 0.0,
            "failed_fetch_per_sec": 0.0,
            "request_handler_avg_idle_pct": 86.0,
            "network_processor_avg_idle_pct": 89.0,
            "produce_latency_p99_ms": 0.0,
            "fetch_consumer_latency_p99_ms": 0.0,
            "fetch_follower_latency_p99_ms": 0.0,
            "request_queue_size": 0,
            "response_queue_size": 0,
            "log_flush_rate_ms_p99": round(2.1 + 4.5 * load, 2),
            "zk_avg_latency_ms": round(zk_lat, 2),
            "zk_max_latency_ms": round(zk_max, 1),
            "zk_outstanding_requests": int(round(1 + 4 * load)),
            "zk_alive_connections": 34,
            "zk_znode_count": 1842,
            "zk_watch_count": 528,
            "zk_server_state": zk_state,
            "zk_synced_followers": 2 if zk_state == "leader" else 0,
            "zk_pending_syncs": 0,
            "zk_packets_received_per_sec": round(pkts, 1),
            "zk_packets_sent_per_sec": round(pkts * 1.08, 1),
            "zk_fsync_warning_count": 0,
            "zk_session_expire_count": 0,
            "log_error_count": 0,
            "log_warn_count": 0,
        }

    # controller / connect / schema-registry
    return {
        "kafka_version": "2.8.1",
        "under_replicated_partitions": 0,
        "offline_partitions_count": 0,
        "active_controller_count": 1 if is_active_ctrl else 0,
        "partition_count": 0,
        "leader_count": 0,
        "isr_shrinks_per_sec": 0.0,
        "isr_expands_per_sec": 0.0,
        "unclean_leader_elections_per_sec": 0.0,
        "leader_election_rate_per_sec": 0.0,
        "messages_in_per_sec": round((1200 + 2400 * load) if role == "connect" else 240 * load, 1),
        "bytes_in_per_sec": round((480000 + 920000 * load) if role == "connect" else 42000 * load, 0),
        "bytes_out_per_sec": round((510000 + 980000 * load) if role == "connect" else 45000 * load, 0),
        "bytes_rejected_per_sec": 0.0,
        "failed_produce_per_sec": 0.0,
        "failed_fetch_per_sec": 0.0,
        "request_handler_avg_idle_pct": round(78.0 - 18.0 * load, 1),
        "network_processor_avg_idle_pct": round(84.0 - 14.0 * load, 1),
        "produce_latency_p99_ms": round(9.0 + 14.0 * load, 1),
        "fetch_consumer_latency_p99_ms": round(8.0 + 12.0 * load, 1),
        "fetch_follower_latency_p99_ms": 0.0,
        "request_queue_size": int(round(1 + 4 * load)),
        "response_queue_size": 1,
        "log_flush_rate_ms_p99": round(2.5 + 4.0 * load, 2),
        "zk_avg_latency_ms": round(1.5 + 1.8 * load, 2),
        "zk_max_latency_ms": round(8.0 + 10.0 * load, 1),
        "zk_outstanding_requests": 1,
        "zk_alive_connections": 1,
        "zk_znode_count": 0,
        "zk_watch_count": 0,
        "zk_server_state": "",
        "zk_synced_followers": 0,
        "zk_pending_syncs": 0,
        "zk_packets_received_per_sec": 0.0,
        "zk_packets_sent_per_sec": 0.0,
        "zk_fsync_warning_count": 0,
        "zk_session_expire_count": 0,
        "log_error_count": 0,
        "log_warn_count": 0,
    }


def apply_kafka_incident(row: dict, inst, kind: str, intensity: float) -> dict:
    r = dict(row)
    if inst.role == "broker":
        if kind in ("full_gc_storm", "throughput_drop"):
            r["under_replicated_partitions"] = max(2, int(round(14 * intensity)))
            r["isr_shrinks_per_sec"] = round(0.45 + 1.6 * intensity, 2)
            r["isr_expands_per_sec"] = round(0.25 + 0.9 * intensity, 2)
            r["leader_election_rate_per_sec"] = round(0.15 + 0.65 * intensity, 2)
            r["request_handler_avg_idle_pct"] = round(max(8.0, 26.0 - 16.0 * intensity), 1)
            r["network_processor_avg_idle_pct"] = round(max(14.0, 34.0 - 16.0 * intensity), 1)
            r["produce_latency_p99_ms"] = round(140.0 + 290.0 * intensity, 1)
            r["fetch_consumer_latency_p99_ms"] = round(95.0 + 180.0 * intensity, 1)
            r["fetch_follower_latency_p99_ms"] = round(110.0 + 220.0 * intensity, 1)
            r["request_queue_size"] = int(round(120 + 260 * intensity))
            r["failed_produce_per_sec"] = round(1.2 + 4.8 * intensity, 2)
            r["zk_avg_latency_ms"] = round(22.0 + 38.0 * intensity, 1)
            r["zk_max_latency_ms"] = round(420.0 + 950.0 * intensity, 1)
            r["zk_session_expire_count"] = 1 if intensity > 0.65 else 0
            r["log_error_count"] = int(round(2 + 6 * intensity))
            r["log_warn_count"] = int(round(6 + 15 * intensity))
        elif kind == "heap_pressure":
            r["under_replicated_partitions"] = max(1, int(round(6 * intensity)))
            r["isr_shrinks_per_sec"] = round(0.18 + 0.55 * intensity, 2)
            r["request_handler_avg_idle_pct"] = round(max(16.0, 36.0 - 16.0 * intensity), 1)
            r["produce_latency_p99_ms"] = round(85.0 + 135.0 * intensity, 1)
            r["request_queue_size"] = int(round(55 + 95 * intensity))
            r["log_warn_count"] = int(round(4 + 9 * intensity))
    return r


def _build_sample_logs(now: int) -> dict[str, tuple[str, str]]:
    """Build realistic Kafka 2.8.1 & ZooKeeper 3.5.9 operational log snippets
    keyed by instance_id -> (role, raw_log_text)."""
    ts_str_1 = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(now - 900)) + ",412"
    ts_str_2 = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(now - 620)) + ",185"
    ts_str_3 = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(now - 310)) + ",804"
    ts_str_4 = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(now - 120)) + ",092"

    return {
        "DEMO-ZK--broker-1": (
            "broker",
            f"[{ts_str_1}] INFO [KafkaController id=1] Elected as active controller for epoch 42 (kafka.controller.KafkaController)\n"
            f"[{ts_str_2}] WARN [Partition orders-events-3 broker=1] Shrinking ISR from 1,2,3 to 1,3 due to follower 2 out of sync (kafka.cluster.Partition)\n"
            f"[{ts_str_3}] WARN [ReplicaManager broker=1] Under-replicated partitions detected on topic payments-stream-0 (kafka.server.ReplicaManager)\n"
            f"[{ts_str_4}] WARN [KafkaRequestHandlerPool] RequestHandlerAvgIdlePercent dropped to 17.8% during peak produce batching (kafka.server.KafkaRequestHandlerPool)\n",
        ),
        "DEMO-ZK--broker-2": (
            "broker",
            f"[{ts_str_1}] WARN [ClientCnxn] Client session timed out, have not heard from server in 12044ms for sessionid 0x1004fa29b1c0002 (org.apache.zookeeper.ClientCnxn)\n"
            f"[{ts_str_2}] ERROR [ZkClient] ZooKeeper session expired for session 0x1004fa29b1c0002, re-registering broker 2 ephemeral znode (kafka.zk.KafkaZkClient)\n"
            f"[{ts_str_3}] WARN [Partition payments-stream-0 broker=2] Shrinking ISR from 1,2,3 to 1,3 (kafka.cluster.Partition)\n"
            f"[{ts_str_4}] WARN [ReplicaFetcherThread-0-1] NotLeaderOrFollowerException while replicating topic orders-events-3 (kafka.server.ReplicaFetcherThread)\n",
        ),
        "DEMO-ZK--zookeeper-1": (
            "zookeeper",
            f"[{ts_str_1}] WARN [SyncThread:1:FileTxnLog@394] - fsync-ing the write ahead log in SyncThread:1 took 1420ms which will adversely effect operation latency.\n"
            f"[{ts_str_2}] WARN [SyncThread:1:FileTxnLog@394] - fsync-ing the write ahead log in SyncThread:1 took 1185ms which will adversely effect operation latency.\n"
            f"[{ts_str_3}] WARN [NIOServerCnxnFactory:ZooKeeperServer@1112] - Expiring session 0x1004fa29b1c0002, timeout of 18000ms exceeded\n"
            f"[{ts_str_4}] INFO [QuorumPeer[myid=1]/0.0.0.0:2181:Leader@642] - LEADING - LEADER ELECTION TOOK - 48 MS\n",
        ),
        "DEMO-ZK--zookeeper-2": (
            "zookeeper",
            f"[{ts_str_2}] WARN [SyncThread:2:FileTxnLog@394] - fsync-ing the write ahead log in SyncThread:2 took 1090ms which will adversely effect operation latency.\n"
            f"[{ts_str_3}] WARN [NIOServerCnxn@378] - caught end of stream exception EndOfStreamException: Unable to read additional data from client sessionid 0x1004fa29b1c0002\n",
        ),
        "DEMO-KRAFT--broker-2": (
            "broker",
            f"[{ts_str_1}] WARN [Partition telemetry-raw-7 broker=2] Shrinking ISR from 1,2,3 to 1,3 after GC pause (kafka.cluster.Partition)\n"
            f"[{ts_str_2}] ERROR [ReplicaManager broker=2] Under-replicated partitions count=9 during Full GC storm (kafka.server.ReplicaManager)\n"
            f"[{ts_str_3}] WARN [KafkaRequestHandlerPool] Request handler pool starved, queue size=285 (kafka.server.KafkaRequestHandlerPool)\n"
            f"[{ts_str_4}] INFO [Partition telemetry-raw-7 broker=2] Expanding ISR from 1,3 to 1,2,3 (kafka.cluster.Partition)\n",
        ),
    }


def main(db_path: str = None) -> None:
    db_path = db_path or store.DB_PATH
    if not os.path.exists(db_path):
        raise SystemExit(f"{db_path} not found — run `python -m seed.seed_history` first.")
    store.init_db(db_path)

    instances = topology.build_instances()
    incidents = build_incidents()

    now = (int(time.time()) // HOUR) * HOUR
    start = now - DAYS * 24 * HOUR

    incident_map: dict[str, list] = {}
    for iid, kind, hours_before, dur in incidents:
        center = now - hours_before * HOUR
        for h in range(dur):
            ts = center + h * HOUR
            frac = (h + 0.5) / dur
            intensity = math.sin(frac * math.pi)
            incident_map.setdefault(iid, []).append((ts, kind, max(0.35, intensity)))

    rows_written = 0
    events_written = 0
    with store.connect(db_path) as c:
        for inst in instances:
            inc = {ts: (kind, inten) for ts, kind, inten in incident_map.get(inst.id, [])}
            ts = start
            while ts <= now:
                row = base_kafka_row(inst, ts)
                if ts in inc:
                    kind, inten = inc[ts]
                    row = apply_kafka_incident(row, inst, kind, inten)
                # Inject ZooKeeper WAL fsync pressure on DEMO-ZK--zookeeper-1 in the current hour
                if inst.id == "DEMO-ZK--zookeeper-1" and ts >= now - HOUR:
                    row["zk_avg_latency_ms"] = 34.5
                    row["zk_max_latency_ms"] = 1420.0
                    row["zk_outstanding_requests"] = 16
                    row["zk_fsync_warning_count"] = 2
                    row["zk_session_expire_count"] = 1
                    row["log_warn_count"] = 3
                store.record_kafka_metric(c, inst.id, ts, row)
                rows_written += 1
                ts += HOUR

        # Parse and store realistic Kafka 2.8.1 & ZooKeeper 3.5.9 operational log events
        c.execute("DELETE FROM kafka_log_events")
        for iid, (role, raw_text) in _build_sample_logs(now).items():
            parsed = kafka_analyzer.parse_kafka_logs(raw_text, node_id=iid, role=role)
            ev_dicts = [
                {
                    "ts": e.ts or now,
                    "level": e.level,
                    "component": e.component,
                    "category": e.category,
                    "severity": e.severity,
                    "summary": e.summary,
                    "raw_line": e.raw_line,
                }
                for e in parsed.events
            ]
            events_written += store.record_kafka_log_events(c, iid, ev_dicts, default_ts=now)

        # Pre-train persistent Ensemble ML models across all instances
        train_summary = ml_insights.train_cluster_models(c, days=DAYS, now=now)

    print(
        f"seeded {len(instances)} instances x ~{DAYS*24} hours = {rows_written} kafka_metrics rows "
        f"+ {events_written} kafka_log_events + {train_summary['instances_trained']} trained ML models -> {db_path}"
    )


if __name__ == "__main__":
    main()
