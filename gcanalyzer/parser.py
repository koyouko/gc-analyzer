"""
JVM Garbage Collection log parser.

Primary target: Java 11+ "unified logging" format (-Xlog:gc*) with the G1
collector, which is the Confluent / Apache Kafka default on modern JVMs. The
parser also recognises ZGC, Shenandoah, Parallel, CMS and Serial markers so the
analyzer can report which engine a node is actually running, and it degrades
gracefully on the legacy Java 8 (-XX:+PrintGCDetails) format.

The parser is deliberately self-contained: no data ever leaves the host. It
reads raw text and produces a list of structured GCEvent records plus file-level
metadata (detected collector, Java hints, time span).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional


# --------------------------------------------------------------------------- #
# Data model
# --------------------------------------------------------------------------- #
@dataclass
class GCEvent:
    """A single garbage-collection record extracted from the log."""

    seq: Optional[int]          # GC(N) sequence number when present
    timestamp: Optional[float]  # epoch seconds (None if only uptime is known)
    uptime: Optional[float]     # seconds since JVM start when present
    phase: str                  # "young", "mixed", "full", "concurrent", "remark", "cleanup", "other"
    cause: str                  # e.g. "G1 Evacuation Pause", "Metadata GC Threshold"
    pause_ms: float             # stop-the-world pause for this event (0 for concurrent-only lines)
    heap_before_mb: Optional[float]
    heap_after_mb: Optional[float]
    heap_total_mb: Optional[float]
    is_stw: bool                # True if this event stopped application threads

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ParsedLog:
    node_id: str
    collector: str              # "G1", "ZGC", "Shenandoah", "Parallel", "CMS", "Serial", "Unknown"
    java_hint: str              # "unified" (Java 9+) or "legacy" (Java 8) or "unknown"
    events: list = field(default_factory=list)
    heap_max_mb: Optional[float] = None
    warnings: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "node_id": self.node_id,
            "collector": self.collector,
            "java_hint": self.java_hint,
            "heap_max_mb": self.heap_max_mb,
            "events": [e.to_dict() for e in self.events],
            "warnings": self.warnings,
        }


# --------------------------------------------------------------------------- #
# Regular expressions
# --------------------------------------------------------------------------- #
# Unified-log timestamp decorator, e.g. [2026-06-08T10:15:30.123+0000]
_TS_RE = re.compile(r"\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}[+\-]\d{4})\]")
# Uptime decorator, e.g. [12.345s]
_UPTIME_RE = re.compile(r"\[(\d+\.\d+)s\]")
# Sequence number, e.g. GC(42)
_SEQ_RE = re.compile(r"GC\((\d+)\)")

# Heap transition + pause on a unified "Pause" summary line, e.g.
#   512M->128M(2048M) 12.345ms
_HEAP_PAUSE_RE = re.compile(
    r"(\d+(?:\.\d+)?)([KMGB])->(\d+(?:\.\d+)?)([KMGB])\((\d+(?:\.\d+)?)([KMGB])\)\s+(\d+(?:\.\d+)?)ms"
)

# Legacy Java 8 heap transition, e.g. 512M->128M(2048M), 0.0123456 secs
_LEGACY_RE = re.compile(
    r"(\d+(?:\.\d+)?)([KMGB])->(\d+(?:\.\d+)?)([KMGB])\((\d+(?:\.\d+)?)([KMGB])\),?\s+(\d+(?:\.\d+)?)\s*secs"
)

_UNIT = {"B": 1 / (1024 * 1024), "K": 1 / 1024, "M": 1.0, "G": 1024.0}


def _to_mb(value: str, unit: str) -> float:
    return float(value) * _UNIT[unit]


def _parse_ts(line: str) -> Optional[float]:
    m = _TS_RE.search(line)
    if not m:
        return None
    raw = m.group(1)
    # Normalise +0000 -> +00:00 for fromisoformat
    iso = raw[:-5] + raw[-5:-2] + ":" + raw[-2:]
    try:
        return datetime.fromisoformat(iso).timestamp()
    except ValueError:
        try:
            return datetime.strptime(raw, "%Y-%m-%dT%H:%M:%S.%f%z").timestamp()
        except ValueError:
            return None


def _parse_uptime(line: str) -> Optional[float]:
    m = _UPTIME_RE.search(line)
    return float(m.group(1)) if m else None


# --------------------------------------------------------------------------- #
# Collector / format detection
# --------------------------------------------------------------------------- #
def detect_collector(text: str) -> str:
    head = text[:20000]
    if "Using G1" in head or "G1 Evacuation Pause" in head or "Pause Young (Normal)" in head:
        return "G1"
    if "Using The Z Garbage Collector" in head or re.search(r"\bZGC\b", head):
        return "ZGC"
    if "Using Shenandoah" in head or "Shenandoah" in head:
        return "Shenandoah"
    if "Using Parallel" in head or "PSYoungGen" in head or "ParOldGen" in head:
        return "Parallel"
    if "Using Concurrent Mark Sweep" in head or "CMS-" in head or "ParNew" in head:
        return "CMS"
    if "Using Serial" in head or "DefNew" in head:
        return "Serial"
    if "G1" in head:
        return "G1"
    return "Unknown"


def detect_java_hint(text: str) -> str:
    head = text[:20000]
    if "secs]" in head or "PrintGCDetails" in head or "[Full GC" in head:
        return "legacy"
    if re.search(r"\]\[info\s*\]\[gc", head) or _UPTIME_RE.search(head) or (_TS_RE.search(head) and "ms" in head):
        return "unified"
    return "unknown"


# --------------------------------------------------------------------------- #
# Phase classification
# --------------------------------------------------------------------------- #
def _classify_unified(line: str) -> tuple[str, str, bool]:
    """Return (phase, cause, is_stw) for a unified-log line."""
    causes = re.findall(r"\(([^)]+)\)", line)
    cause = causes[-1] if causes else ""

    low = line.lower()
    if "pause full" in low:
        return "full", cause, True
    if "pause young" in low and "mixed" in low:
        return "mixed", cause, True
    if "pause young" in low:
        return "young", cause, True
    if "pause mixed" in low:
        return "mixed", cause, True
    if "pause remark" in low:
        return "remark", cause or "Remark", True
    if "pause cleanup" in low:
        return "cleanup", cause or "Cleanup", True
    if "pause initial mark" in low or "concurrent start" in low:
        return "young", cause or "Concurrent Start", True
    if "concurrent" in low:
        return "concurrent", cause or "Concurrent Cycle", False
    return "other", cause, True


# --------------------------------------------------------------------------- #
# Main entry point
# --------------------------------------------------------------------------- #
def parse(text: str, node_id: str = "node") -> ParsedLog:
    collector = detect_collector(text)
    java_hint = detect_java_hint(text)
    parsed = ParsedLog(node_id=node_id, collector=collector, java_hint=java_hint)

    heap_max = 0.0
    matched = 0

    for line in text.splitlines():
        if "->" not in line:
            continue

        m = _HEAP_PAUSE_RE.search(line)
        is_legacy = False
        if not m:
            m = _LEGACY_RE.search(line)
            is_legacy = bool(m)
        if not m:
            continue

        low = line.lower()
        if not is_legacy and "pause" not in low and "full" not in low:
            if "gc(" not in low:
                continue

        before = _to_mb(m.group(1), m.group(2))
        after = _to_mb(m.group(3), m.group(4))
        total = _to_mb(m.group(5), m.group(6))
        if is_legacy:
            pause_ms = float(m.group(7)) * 1000.0
        else:
            pause_ms = float(m.group(7))

        heap_max = max(heap_max, total)

        seq_m = _SEQ_RE.search(line)
        seq = int(seq_m.group(1)) if seq_m else None

        if is_legacy:
            if "full gc" in low:
                phase, cause, is_stw = "full", "Full GC", True
            else:
                phase, cause, is_stw = "young", "Young GC", True
        else:
            phase, cause, is_stw = _classify_unified(line)

        parsed.events.append(
            GCEvent(
                seq=seq,
                timestamp=_parse_ts(line),
                uptime=_parse_uptime(line),
                phase=phase,
                cause=cause,
                pause_ms=pause_ms,
                heap_before_mb=round(before, 2),
                heap_after_mb=round(after, 2),
                heap_total_mb=round(total, 2),
                is_stw=is_stw,
            )
        )
        matched += 1

    parsed.heap_max_mb = round(heap_max, 2) if heap_max else None

    if matched == 0:
        parsed.warnings.append(
            "No GC pause events were parsed. Confirm the file is a JVM GC log "
            "(unified -Xlog:gc* or legacy -XX:+PrintGCDetails)."
        )
    if collector == "Unknown":
        parsed.warnings.append("Could not positively identify the GC collector.")

    # If timestamps are missing but uptimes exist, synthesise a pseudo epoch so
    # the timeline still renders (anchored to now minus span).
    if parsed.events and parsed.events[0].timestamp is None:
        uptimes = [e.uptime for e in parsed.events if e.uptime is not None]
        if uptimes:
            base = datetime.now(timezone.utc).timestamp() - max(uptimes)
            for e in parsed.events:
                if e.timestamp is None and e.uptime is not None:
                    e.timestamp = base + e.uptime
            parsed.warnings.append(
                "Log had no wall-clock timestamps; timeline is anchored to JVM uptime."
            )

    return parsed


def parse_file(path: str, node_id: Optional[str] = None) -> ParsedLog:
    with open(path, "r", errors="replace") as fh:
        text = fh.read()
    return parse(text, node_id or path)
