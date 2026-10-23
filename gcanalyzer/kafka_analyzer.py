"""
Kafka 2.8.1 & ZooKeeper Diagnostic Engine + Log & JMX/Prometheus Analytics.

Provides:
  1. Log parsing and anomaly classification for Apache/Confluent Kafka 2.8.1
     broker logs (`server.log`, `controller.log`, `state-change.log`) and
     ZooKeeper 3.5.x logs (`zookeeper.log`, `zookeeper.out`).
  2. Core Kafka 2.8.1 & ZooKeeper JMX / Prometheus metric evaluation:
     - UnderReplicatedPartitions (URP), OfflinePartitionsCount, ActiveControllerCount
     - ISR Shrinks / Expands, Unclean Leader Elections, Leader Election Rate
     - Broker Throughput (MessagesInPerSec, BytesInPerSec, BytesOutPerSec)
     - Request Handler & Network Processor Avg Idle %
     - Produce / FetchConsumer / FetchFollower p99 Latencies & Request Queue Size
     - Partition & Leader Balance / Skew across Brokers
     - ZooKeeper Quorum Health (Avg/Max Latency, Outstanding Requests, Pending Syncs,
       WAL fsync stalls, Session Expirations, Alive Connections, ZNodes/Watches)
  3. Live Prometheus PromQL scraping for Kafka & ZooKeeper JMX exporters.
  4. Unified Cross-Layer Root Cause Diagnostics correlating:
     JVM GC Pauses <-> Host SAR (CPU/Memory/Swap/Disk) <-> Kafka 2.8.1 Broker <-> ZooKeeper.
"""

from __future__ import annotations

import json
import math
import re
import ssl
import statistics
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional

# --------------------------------------------------------------------------- #
# Thresholds tailored for Apache Kafka 2.8.1 (ZooKeeper-backed & KRaft)
# --------------------------------------------------------------------------- #
URP_WARN = 1
URP_CRIT = 1
OFFLINE_PARTITIONS_CRIT = 1
ISR_SHRINK_WARN_PER_MIN = 1.0
ISR_SHRINK_CRIT_PER_MIN = 5.0
REQ_HANDLER_IDLE_WARN_PCT = 35.0
REQ_HANDLER_IDLE_CRIT_PCT = 18.0
NET_PROC_IDLE_WARN_PCT = 35.0
NET_PROC_IDLE_CRIT_PCT = 18.0
PRODUCE_P99_WARN_MS = 120.0
PRODUCE_P99_CRIT_MS = 300.0
FETCH_P99_WARN_MS = 150.0
FETCH_P99_CRIT_MS = 400.0
REQ_QUEUE_WARN = 25
REQ_QUEUE_CRIT = 80
LOG_FLUSH_WARN_MS = 40.0
LOG_FLUSH_CRIT_MS = 120.0

# ZooKeeper 3.5.9 (Kafka 2.8.1 companion) thresholds
ZK_LATENCY_AVG_WARN_MS = 15.0
ZK_LATENCY_AVG_CRIT_MS = 50.0
ZK_LATENCY_MAX_WARN_MS = 150.0
ZK_LATENCY_MAX_CRIT_MS = 600.0
ZK_OUTSTANDING_WARN = 10
ZK_OUTSTANDING_CRIT = 40
ZK_FSYNC_WARN_COUNT = 1
ZK_SESSION_EXPIRE_WARN = 1
ZK_SESSION_EXPIRE_CRIT = 3


@dataclass
class KafkaLogEvent:
    ts: int
    level: str            # INFO | WARN | ERROR | FATAL
    category: str         # isr_shrink | zk_session_expired | zk_fsync_stall | ...
    component: str        # broker | controller | zookeeper
    summary: str
    raw_line: str
    partition: Optional[str] = None
    duration_ms: Optional[float] = None
    severity: str = "warning"


@dataclass
class ParsedKafkaLogs:
    node_id: str
    role: str
    events: list[KafkaLogEvent] = field(default_factory=list)
    counts: dict[str, int] = field(default_factory=dict)
    total_lines: int = 0
    error_lines: int = 0
    warn_lines: int = 0
    fsync_max_ms: float = 0.0
    affected_partitions: list[str] = field(default_factory=list)
    expired_sessions: list[str] = field(default_factory=list)

    @property
    def lines_scanned(self) -> int:
        return self.total_lines

    @property
    def error_count(self) -> int:
        return self.error_lines

    @property
    def warn_count(self) -> int:
        return self.warn_lines

    @property
    def max_fsync_ms(self) -> float:
        return self.fsync_max_ms


# Timestamp regexes for Kafka (`[2026-10-22 14:05:12,345]`) and ZooKeeper (`2026-10-22 14:05:12,345`)
_TS_RE = re.compile(
    r"\[?(?P<dt>\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})(?:[.,](?P<ms>\d{1,3}))?\]?\s+"
    r"(?:\[[^\]]+\]\s*[-:]?\s*)?(?P<lvl>TRACE|DEBUG|INFO|WARN|WARNING|ERROR|FATAL)\b",
    re.IGNORECASE,
)

# Log pattern rules: (category, component, regex, summary_template)
_LOG_PATTERNS = [
    (
        "zk_fsync_stall",
        "zookeeper",
        re.compile(r"fsync-ing the write ahead log.*took\s+(?P<ms>\d+(?:\.\d+)?)\s*ms", re.I),
        "ZooKeeper WAL fsync stall ({ms} ms) — disk latency threatens quorum heartbeats",
    ),
    (
        "zk_session_expired",
        "zookeeper",
        re.compile(
            r"(?:Session\s+(?P<sess>0x[0-9a-fA-F]+)\s+for\s+server.*expired|"
            r"Expiring\s+session\s+(?P<sess2>0x[0-9a-fA-F]+)|"
            r"KeeperErrorCode\s*=\s*SessionExpired|"
            r"session\s+expired|"
            r"Client\s+session\s+timed\s+out|"
            r"Unable\s+to\s+read\s+additional\s+data\s+from\s+client\s+sessionid\s+(?P<sess3>0x[0-9a-fA-F]+)?|"
            r"Processed\s+session\s+termination\s+for\s+sessionid[:\s]*(?P<sess4>0x[0-9a-fA-F]+)?)",
            re.I,
        ),
        "ZooKeeper session timeout/expiration detected — often triggered by broker STW GC pause or network stall",
    ),
    (
        "zk_quorum_election",
        "zookeeper",
        re.compile(
            r"(?:New\s+election\.|Notification\s+time\s+out|LOOKING|FOLLOWING\s+-\s+LEADER\s+ELECTION\s+TOOK|"
            r"LEADING\s+-\s+LEADER\s+ELECTION\s+TOOK|Have\s+quorum\s+of\s+supporters|"
            r"Exception\s+when\s+following\s+the\s+leader)",
            re.I,
        ),
        "ZooKeeper quorum state transition / leader election activity",
    ),
    (
        "zk_connection_drop",
        "zookeeper",
        re.compile(r"(?:Too\s+many\s+connections\s+from|Connection\s+broken\s+for\s+id|Unexpected\s+Exception:\s+java\.io\.IOException:\s+Connection\s+reset)", re.I),
        "ZooKeeper client or peer connection drop / maxClientCnxns limit hit",
    ),
    (
        "isr_shrink",
        "broker",
        re.compile(
            r"(?:Shrinking\s+ISR\s+for\s+partition\s+(?P<part>[A-Za-z0-9_.-]+)|"
            r"\[Partition\s+(?P<part2>[A-Za-z0-9_.-]+)[^\]]*\]\s*Shrinking\s+ISR|"
            r"Shrinking\s+ISR\s+from)",
            re.I,
        ),
        "Broker shrunk ISR for partition {part} (follower exceeded replica.lag.time.max.ms)",
    ),
    (
        "isr_expand",
        "broker",
        re.compile(
            r"(?:Expanding\s+ISR\s+for\s+partition\s+(?P<part>[A-Za-z0-9_.-]+)|"
            r"\[Partition\s+(?P<part2>[A-Za-z0-9_.-]+)[^\]]*\]\s*Expanding\s+ISR|"
            r"Expanding\s+ISR\s+from)",
            re.I,
        ),
        "Follower caught up; expanded ISR for partition {part}",
    ),
    (
        "urp_detected",
        "broker",
        re.compile(
            r"(?:UnderReplicatedPartitions|Under-replicated\s+partitions|"
            r"Cached\s+zkVersion\s+\d+\s+not\s+equal\s+to\s+that\s+in\s+zookeeper|failed\s+to\s+update\s+ISR)",
            re.I,
        ),
        "Partition ISR metadata mismatch or under-replication detected",
    ),
    (
        "offline_partition",
        "controller",
        re.compile(
            r"(?:OfflinePartitionsCount|state\s+change.*to\s+OfflinePartition|"
            r"No\s+broker\s+in\s+ISR\s+is\s+alive\s+for\s+(?:partition\s+)?(?P<part>[A-Za-z0-9_.-]+))",
            re.I,
        ),
        "Partition went OFFLINE (no live in-sync leader available)",
    ),
    (
        "controller_election",
        "controller",
        re.compile(
            r"(?:Controller\s+\d+\s+epoch\s+\d+\s+initiated\s+state\s+change|"
            r"elected\s+as\s+(?:active\s+)?controller|"
            r"broker\s+\d+\s+has\s+been\s+elected\s+as\s+the\s+controller|"
            r"Controller\s+moved\s+to\s+another\s+broker)",
            re.I,
        ),
        "Kafka Active Controller election / failover occurred",
    ),
    (
        "leader_election",
        "controller",
        re.compile(r"(?:Starting\s+replica\s+leader\s+election|Unclean\s+leader\s+election|PreferredReplicaLeaderElection)", re.I),
        "Partition replica leader election triggered",
    ),
    (
        "not_leader_error",
        "broker",
        re.compile(r"(?:NotLeaderForPartitionException|NotLeaderOrFollowerException)", re.I),
        "Clients hitting NotLeaderOrFollowerException due to leadership churn or stale metadata",
    ),
    (
        "request_handler_starvation",
        "broker",
        re.compile(r"(?:RequestChannel.*queue|SocketServer.*Processor|TooManyConnectionsException|Disconnecting\s+node.*due\s+to\s+request\s+timeout)", re.I),
        "Broker network processor or request channel saturation / timeout",
    ),
    (
        "storage_io_error",
        "broker",
        re.compile(r"(?:OfflineLogDirectoryCount|LogDirFailureChannel|Error\s+while\s+appending\s+records\s+to\s+(?P<part>[A-Za-z0-9_.-]+)|CorruptRecordException|halting\s+because\s+log\s+truncation\s+is\s+not\s+allowed)", re.I),
        "Kafka log directory / storage I/O error or log corruption",
    ),
    (
        "jvm_oom",
        "broker",
        re.compile(r"java\.lang\.OutOfMemoryError", re.I),
        "Critical JVM OutOfMemoryError in broker/ZooKeeper process",
    ),
]


def _parse_ts(dt_str: str, fallback_ts: int) -> int:
    try:
        clean = dt_str.replace("T", " ").strip()
        dt = datetime.strptime(clean, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return fallback_ts


def parse_kafka_logs(text: str, node_id: str = "broker-1", role: str = "broker", now: int | None = None) -> ParsedKafkaLogs:
    """Parse Kafka 2.8.1 server/controller/state-change logs and ZooKeeper logs."""
    now_ts = now or int(time.time())
    res = ParsedKafkaLogs(node_id=node_id, role=role)
    if not text:
        return res

    partitions_seen: set[str] = set()
    sessions_seen: set[str] = set()
    lines = text.splitlines()
    res.total_lines = len(lines)
    last_ts = now_ts

    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        m_ts = _TS_RE.search(line)
        lvl = "INFO"
        if m_ts:
            last_ts = _parse_ts(m_ts.group("dt"), last_ts)
            lvl = m_ts.group("lvl").upper()
            if lvl == "WARNING":
                lvl = "WARN"
        elif "ERROR" in line or "FATAL" in line:
            lvl = "ERROR"
        elif "WARN" in line:
            lvl = "WARN"

        if lvl in ("ERROR", "FATAL"):
            res.error_lines += 1
        elif lvl == "WARN":
            res.warn_lines += 1

        for cat, comp, regex, tmpl in _LOG_PATTERNS:
            m = regex.search(line)
            if m:
                gd = m.groupdict()
                part = gd.get("part") or gd.get("part2")
                sess = gd.get("sess") or gd.get("sess2") or gd.get("sess3") or gd.get("sess4")
                ms_val = float(gd["ms"]) if gd.get("ms") else None
                if part:
                    partitions_seen.add(part)
                if sess:
                    sessions_seen.add(sess)
                if ms_val is not None and cat == "zk_fsync_stall":
                    res.fsync_max_ms = max(res.fsync_max_ms, ms_val)
                summary = tmpl.format(
                    part=part or "unknown",
                    ms=f"{ms_val:.0f}" if ms_val is not None else "?",
                )
                res.counts[cat] = res.counts.get(cat, 0) + 1
                sev = (
                    "critical"
                    if lvl in ("ERROR", "FATAL")
                    or cat in ("offline_partition", "jvm_oom", "storage_io_error", "zk_session_expired")
                    else "warning"
                )
                if len(res.events) < 250:
                    res.events.append(
                        KafkaLogEvent(
                            ts=last_ts,
                            level=lvl if lvl in ("WARN", "ERROR", "FATAL") else "WARN",
                            category=cat,
                            component=comp if role != "zookeeper" else "zookeeper",
                            summary=summary,
                            raw_line=line[:400],
                            partition=part,
                            duration_ms=ms_val,
                            severity=sev,
                        )
                    )
                break

    res.affected_partitions = sorted(partitions_seen)
    res.expired_sessions = sorted(sessions_seen)
    return res


def score_kafka_health(m: dict, role: str = "broker", log_counts: dict | None = None) -> dict:
    """Compute a 0-100 health score and A-F grade for a Kafka 2.8.1 broker or ZooKeeper node."""
    if not m:
        return {"score": None, "grade": "?", "status": "unknown", "reasons": []}

    log_counts = log_counts or {}
    score = 100.0
    reasons: list[str] = []

    if role == "zookeeper":
        avg_lat = m.get("zk_avg_latency_ms")
        max_lat = m.get("zk_max_latency_ms")
        out_req = m.get("zk_outstanding_requests")
        fsync_w = m.get("zk_fsync_warning_count") or log_counts.get("zk_fsync_stall", 0)
        sess_exp = m.get("zk_session_expire_count") or log_counts.get("zk_session_expired", 0)

        if isinstance(fsync_w, (int, float)) and fsync_w > 0:
            score -= min(35.0, 15.0 + fsync_w * 5.0)
            reasons.append(f"ZooKeeper WAL fsync stalls detected ({int(fsync_w)} warning(s)) — disk I/O threatening quorum stability")
        if isinstance(avg_lat, (int, float)):
            if avg_lat >= ZK_LATENCY_AVG_CRIT_MS:
                score -= 30.0
                reasons.append(f"ZooKeeper avg latency critical at {avg_lat:.1f} ms (>= {ZK_LATENCY_AVG_CRIT_MS:.0f} ms)")
            elif avg_lat >= ZK_LATENCY_AVG_WARN_MS:
                score -= 12.0
                reasons.append(f"ZooKeeper avg latency elevated at {avg_lat:.1f} ms")
        if isinstance(max_lat, (int, float)):
            if max_lat >= ZK_LATENCY_MAX_CRIT_MS:
                score -= 18.0
                reasons.append(f"ZooKeeper max latency spiked to {max_lat:.0f} ms")
            elif max_lat >= ZK_LATENCY_MAX_WARN_MS:
                score -= 8.0
                reasons.append(f"ZooKeeper max latency reached {max_lat:.0f} ms")
        if isinstance(out_req, (int, float)):
            if out_req >= ZK_OUTSTANDING_CRIT:
                score -= 22.0
                reasons.append(f"ZooKeeper queued outstanding requests at {out_req:.0f} — quorum bottleneck")
            elif out_req >= ZK_OUTSTANDING_WARN:
                score -= 10.0
                reasons.append(f"ZooKeeper outstanding requests at {out_req:.0f}")
        if isinstance(sess_exp, (int, float)) and sess_exp >= ZK_SESSION_EXPIRE_CRIT:
            score -= 20.0
            reasons.append(f"{int(sess_exp)} ZooKeeper session expirations recorded — brokers losing ephemeral znodes")
        elif isinstance(sess_exp, (int, float)) and sess_exp >= ZK_SESSION_EXPIRE_WARN:
            score -= 8.0
            reasons.append(f"{int(sess_exp)} ZooKeeper session expiration(s) observed")
    else:
        urp = m.get("under_replicated_partitions")
        offline = m.get("offline_partitions_count")
        isr_shrink = m.get("isr_shrinks_per_sec")
        req_idle = m.get("request_handler_avg_idle_pct")
        net_idle = m.get("network_processor_avg_idle_pct")
        prod_p99 = m.get("produce_latency_p99_ms")
        fetch_p99 = m.get("fetch_consumer_latency_p99_ms")
        req_q = m.get("request_queue_size")
        unclean = m.get("unclean_leader_elections_per_sec")

        if isinstance(offline, (int, float)) and offline > 0:
            score -= 45.0
            reasons.append(f"{int(offline)} Offline Partition(s) — immediate data availability impact!")
        if isinstance(urp, (int, float)) and urp > 0:
            score -= min(38.0, 22.0 + urp * 1.5)
            reasons.append(f"{int(urp)} Under-Replicated Partition(s) (URP > 0) — replication lagging or broker out of ISR")
        if isinstance(unclean, (int, float)) and unclean > 0:
            score -= 30.0
            reasons.append(f"Unclean leader election detected ({unclean}/s) — potential data loss")
        if isinstance(isr_shrink, (int, float)) and isr_shrink >= 0.1:
            score -= 15.0
            reasons.append(f"ISR shrinking actively ({isr_shrink:.2f}/s) — followers dropping out of sync")
        elif log_counts.get("isr_shrink", 0) > 0:
            score -= 12.0
            reasons.append(f"Log analysis found {log_counts['isr_shrink']} ISR shrink event(s)")
        if isinstance(req_idle, (int, float)):
            if req_idle < REQ_HANDLER_IDLE_CRIT_PCT:
                score -= 22.0
                reasons.append(f"Kafka Request Handler Pool critically starved ({req_idle:.1f}% idle < {REQ_HANDLER_IDLE_CRIT_PCT:.0f}%)")
            elif req_idle < REQ_HANDLER_IDLE_WARN_PCT:
                score -= 10.0
                reasons.append(f"Kafka Request Handler Pool busy ({req_idle:.1f}% idle)")
        if isinstance(net_idle, (int, float)):
            if net_idle < NET_PROC_IDLE_CRIT_PCT:
                score -= 18.0
                reasons.append(f"Kafka Network Processor threads critically busy ({net_idle:.1f}% idle)")
            elif net_idle < NET_PROC_IDLE_WARN_PCT:
                score -= 8.0
                reasons.append(f"Kafka Network Processor idle at {net_idle:.1f}%")
        if isinstance(prod_p99, (int, float)):
            if prod_p99 >= PRODUCE_P99_CRIT_MS:
                score -= 18.0
                reasons.append(f"Produce p99 latency critical at {prod_p99:.0f} ms (> {PRODUCE_P99_CRIT_MS:.0f} ms)")
            elif prod_p99 >= PRODUCE_P99_WARN_MS:
                score -= 8.0
                reasons.append(f"Produce p99 latency elevated at {prod_p99:.0f} ms")
        if isinstance(fetch_p99, (int, float)) and fetch_p99 >= FETCH_P99_CRIT_MS:
            score -= 10.0
            reasons.append(f"Consumer Fetch p99 latency elevated at {fetch_p99:.0f} ms")
        if isinstance(req_q, (int, float)) and req_q >= REQ_QUEUE_CRIT:
            score -= 12.0
            reasons.append(f"Request channel queue backlog at {int(req_q)} requests")

    score = max(0.0, round(score, 1))
    if score >= 90:
        grade, status = "A", "healthy"
    elif score >= 75:
        grade, status = "B", "good"
    elif score >= 60:
        grade, status = "C", "watch"
    elif score >= 40:
        grade, status = "D", "at risk"
    else:
        grade, status = "F", "critical"

    return {"score": score, "grade": grade, "status": status, "reasons": reasons}


def derive_kafka_findings(
    m: dict,
    role: str = "broker",
    gc_metrics: dict | None = None,
    host_metrics: dict | None = None,
    log_counts: dict | None = None,
) -> dict:
    """Cross-layer diagnostic engine combining Kafka 2.8.1 JMX, Broker/ZK logs, JVM GC, and Host SAR."""
    pros: list[str] = []
    cons: list[str] = []
    recs: list[str] = []
    root_causes: list[dict] = []

    m = m or {}
    gc_metrics = gc_metrics or {}
    host_metrics = host_metrics or {}
    log_counts = log_counts or {}

    max_pause = gc_metrics.get("max_pause_ms") or gc_metrics.get("pause_max_ms") or 0.0
    full_gc = gc_metrics.get("full_count") or gc_metrics.get("full_gc_count") or 0
    cpu_busy = host_metrics.get("cpu_busy_pct_avg") or host_metrics.get("cpu_busy_pct") or 0.0
    iowait = host_metrics.get("cpu_iowait_pct_avg") or host_metrics.get("cpu_iowait_pct") or 0.0
    disk_await = host_metrics.get("disk_await_ms_max") or 0.0
    swap_used = host_metrics.get("swap_used_pct_max") or host_metrics.get("swap_used_pct") or 0.0

    if role == "zookeeper":
        avg_lat = m.get("zk_avg_latency_ms") or 0.0
        fsync_w = (m.get("zk_fsync_warning_count") or 0) + log_counts.get("zk_fsync_stall", 0)
        sess_exp = (m.get("zk_session_expire_count") or 0) + log_counts.get("zk_session_expired", 0)

        if fsync_w > 0 or disk_await > 25.0:
            cons.append(
                f"ZooKeeper transaction log (WAL) fsync stalls observed ({int(fsync_w)} events; host disk await {disk_await:.1f} ms)."
            )
            recs.append(
                "Kafka 2.8.1 ZooKeeper Best Practice: Place ZooKeeper `dataLogDir` (transaction WAL) on a dedicated, low-latency NVMe/SSD volume separate from `dataDir` snapshots and OS logs."
            )
            root_causes.append({
                "id": "zk_wal_io_stall",
                "severity": "critical",
                "title": "Host Storage Latency -> ZooKeeper WAL fsync Stall -> Session Timeouts",
                "detail": f"Disk await ({disk_await:.1f} ms) and iowait ({iowait:.1f}%) are stalling ZooKeeper SyncThread fsyncs, delaying quorum acks and causing broker session drops.",
            })
        else:
            pros.append("ZooKeeper transaction log fsync latency is healthy with zero WAL stall warnings.")

        if sess_exp > 0:
            cons.append(f"{int(sess_exp)} broker/client session expiration(s) recorded by ZooKeeper.")
            recs.append(
                "Check broker JVM GC pauses against `zookeeper.session.timeout.ms` (default 18000ms in Kafka 2.8.1). If GC pauses or host swap stalls freeze the broker SendThread, ZooKeeper expires the ephemeral broker registration and triggers ISR churn."
            )
        if avg_lat < ZK_LATENCY_AVG_WARN_MS and fsync_w == 0:
            pros.append(f"ZooKeeper quorum request latency is low ({avg_lat:.1f} ms avg).")
    else:
        urp = m.get("under_replicated_partitions") or 0
        offline = m.get("offline_partitions_count") or 0
        isr_shrink = m.get("isr_shrinks_per_sec") or 0.0
        req_idle = m.get("request_handler_avg_idle_pct")
        net_idle = m.get("network_processor_avg_idle_pct")
        prod_p99 = m.get("produce_latency_p99_ms") or 0.0

        if urp == 0 and offline == 0:
            pros.append("Zero Under-Replicated Partitions (URP = 0) and zero Offline Partitions.")
        if offline > 0:
            cons.append(f"{int(offline)} Offline Partition(s) detected — partitions have no active leader in ISR.")
            recs.append(
                "Inspect `controller.log` and `state-change.log` immediately for offline brokers or unclean leader election requirements (`unclean.leader.election.enable=false`)."
            )
        if urp > 0:
            cons.append(f"{int(urp)} Under-Replicated Partition(s) (URP) active on this broker.")
            # Cross-layer root cause determination
            if max_pause >= 400 or full_gc > 0:
                root_causes.append({
                    "id": "gc_induced_urp",
                    "severity": "critical",
                    "title": "JVM Stop-The-World GC Pause -> Replica Fetch Stall / ZK Timeout -> URP",
                    "detail": (
                        f"Broker experienced {int(full_gc)} Full GC(s) and {max_pause:.0f} ms max pause while holding {int(urp)} URPs. "
                        "During STW pauses, replica fetcher threads freeze and leaders drop this broker from ISR (`replica.lag.time.max.ms`)."
                    ),
                })
                recs.append(
                    "Eliminate Full GCs and cap G1 pause times (`-XX:MaxGCPauseMillis=100`, `-XX:G1ReservePercent=15`) or increase `replica.lag.time.max.ms` (default 30000ms in Kafka 2.8.1) while tuning heap."
                )
            if iowait >= 10.0 or disk_await >= 25.0:
                root_causes.append({
                    "id": "io_induced_urp",
                    "severity": "critical",
                    "title": "Host Disk I/O Bottleneck -> Slow Log Append -> Follower Lag & URP",
                    "detail": (
                        f"Host disk await peaked at {disk_await:.1f} ms with {iowait:.1f}% CPU iowait alongside {int(urp)} URPs "
                        f"and {prod_p99:.0f} ms Produce p99 latency."
                    ),
                })
                recs.append(
                    "Spread Kafka `log.dirs` across multiple independent NVMe/JBOD spindles, increase `num.io.threads` (default 8 -> 16), and verify `num.replica.fetchers` (default 1 -> 2-4 in Kafka 2.8.1)."
                )
            if swap_used > 0.5:
                root_causes.append({
                    "id": "swap_induced_urp",
                    "severity": "critical",
                    "title": "OS Swap Activity -> JVM Page Thrashing -> Broker ISR Drop",
                    "detail": f"Host swap usage ({swap_used:.1f}%) is stalling JVM memory pages, causing severe pause spikes and {int(urp)} URPs.",
                })
                recs.append("Set `vm.swappiness=1` immediately on all Kafka 2.8.1 & ZooKeeper hosts and ensure OS page cache has >= 30% RAM headroom.")

        if isinstance(req_idle, (int, float)):
            if req_idle < REQ_HANDLER_IDLE_WARN_PCT:
                cons.append(f"Request Handler Pool idle percent is low ({req_idle:.1f}%).")
                recs.append(
                    f"Increase `num.io.threads` in `server.properties` (currently {req_idle:.1f}% idle) and inspect disk await ({disk_await:.1f} ms) which blocks I/O handler threads."
                )
                if cpu_busy >= 80.0:
                    root_causes.append({
                        "id": "cpu_thread_starvation",
                        "severity": "warning",
                        "title": "Host CPU Saturation -> Request Handler Pool Exhaustion",
                        "detail": f"Host CPU busy is {cpu_busy:.1f}% while RequestHandlerAvgIdlePercent dropped to {req_idle:.1f}%.",
                    })
            else:
                pros.append(f"Request Handler Pool has healthy headroom ({req_idle:.1f}% idle).")

        if isinstance(net_idle, (int, float)) and net_idle < NET_PROC_IDLE_WARN_PCT:
            cons.append(f"Network Processor Pool idle percent is low ({net_idle:.1f}%).")
            recs.append(
                "Increase `num.network.threads` (default 3 -> 6-8 in Kafka 2.8.1) or check for TLS handshake storms from short-lived client connections."
            )

        if log_counts.get("zk_session_expired", 0) > 0:
            cons.append(f"Broker logs show {log_counts['zk_session_expired']} ZooKeeper session expiration event(s).")
            recs.append(
                "In Kafka 2.8.1, verify `zookeeper.session.timeout.ms=18000` and ensure G1GC pauses never exceed 1/3 of the session timeout."
            )

    if not recs:
        recs.append("Kafka 2.8.1 and ZooKeeper telemetry are within healthy operational SLOs.")

    return {
        "pros": pros,
        "cons": cons,
        "recommendations": recs,
        "root_causes": root_causes,
    }


# --------------------------------------------------------------------------- #
# Prometheus PromQL Scraper for Kafka 2.8.1 & ZooKeeper JMX Metrics
# --------------------------------------------------------------------------- #
PROMETHEUS_KAFKA_QUERIES = {
    "under_replicated_partitions": 'kafka_server_replicamanager_underreplicatedpartitions',
    "offline_partitions_count": 'kafka_controller_kafkacontroller_offlinepartitionscount',
    "active_controller_count": 'kafka_controller_kafkacontroller_activecontrollercount',
    "isr_shrinks_per_sec": 'rate(kafka_server_replicamanager_isrshrinks_total[5m])',
    "isr_expands_per_sec": 'rate(kafka_server_replicamanager_isrexpands_total[5m])',
    "messages_in_per_sec": 'rate(kafka_server_brokertopicmetrics_messagesin_total{topic=""}[5m])',
    "bytes_in_per_sec": 'rate(kafka_server_brokertopicmetrics_bytesin_total{topic=""}[5m])',
    "bytes_out_per_sec": 'rate(kafka_server_brokertopicmetrics_bytesout_total{topic=""}[5m])',
    "request_handler_avg_idle_pct": 'kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent * 100',
    "network_processor_avg_idle_pct": 'kafka_network_socketserver_networkprocessoravgidlepercent * 100',
    "produce_latency_p99_ms": 'kafka_network_requestmetrics_totaltimems{request="Produce",quantile="0.99"}',
    "fetch_consumer_latency_p99_ms": 'kafka_network_requestmetrics_totaltimems{request="FetchConsumer",quantile="0.99"}',
    "fetch_follower_latency_p99_ms": 'kafka_network_requestmetrics_totaltimems{request="FetchFollower",quantile="0.99"}',
    "request_queue_size": 'kafka_network_requestchannel_requestqueuesize',
    "partition_count": 'kafka_server_replicamanager_partitioncount',
    "leader_count": 'kafka_server_replicamanager_leadercount',
    "zk_avg_latency_ms": 'zookeeper_avg_latency',
    "zk_max_latency_ms": 'zookeeper_max_latency',
    "zk_outstanding_requests": 'zookeeper_outstanding_requests',
    "zk_alive_connections": 'zookeeper_num_alive_connections',
    "zk_pending_syncs": 'zookeeper_pending_syncs',
}


def query_prometheus_instant(base_url: str, promql: str, timeout_s: int = 5) -> list[dict]:
    """Execute a read-only instant query against Prometheus `/api/v1/query`."""
    if not base_url:
        return []
    params = urllib.parse.urlencode({"query": promql, "timeout": f"{timeout_s}s"})
    url = base_url.rstrip("/") + "/api/v1/query?" + params
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({}),
        urllib.request.HTTPSHandler(context=ssl.create_default_context()),
    )
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with opener.open(req, timeout=timeout_s) as resp:
            if resp.status != 200:
                return []
            raw = resp.read(256 * 1024)
            payload = json.loads(raw)
            if payload.get("status") != "success":
                return []
            return payload.get("data", {}).get("result", [])
    except Exception:
        return []
