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

import gzip
import hashlib
import io
import math
import os
import re
import stat
from dataclasses import dataclass, field, asdict
from datetime import datetime
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
    jvm_id: Optional[str] = None
    phase_detail: str = ""
    source_lines: list[int] = field(default_factory=list)

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
    time_basis: str = "unknown"
    anchor_source: Optional[str] = None
    record_counts: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "node_id": self.node_id,
            "collector": self.collector,
            "java_hint": self.java_hint,
            "heap_max_mb": self.heap_max_mb,
            "events": [e.to_dict() for e in self.events],
            "warnings": self.warnings,
            "time_basis": self.time_basis,
            "anchor_source": self.anchor_source,
            "record_counts": self.record_counts,
        }


# --------------------------------------------------------------------------- #
# Regular expressions
# --------------------------------------------------------------------------- #
# Unified-log timestamp decorator, e.g. [2026-06-08T10:15:30.123+0000]
_TS_RE = re.compile(r"(?<!\d)(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+\-]\d{2}:?\d{2}))")
# Uptime decorator, e.g. [12.345s]
_UPTIME_RE = re.compile(r"\[(\d+(?:\.\d+)?)s\]")
_LEGACY_UPTIME_RE = re.compile(r"(?:^|:\s+)(\d+(?:\.\d+)?):\s+\[(?:Full )?GC")
# Sequence number, e.g. GC(42)
_SEQ_RE = re.compile(r"GC\((\d+)\)")

# Heap transition + pause on a unified "Pause" summary line, e.g.
#   512M->128M(2048M) 12.345ms
_HEAP_PAUSE_RE = re.compile(
    r"(\d+(?:\.\d+)?)([KMGB])->(\d+(?:\.\d+)?)([KMGB])\((\d+(?:\.\d+)?)([KMGB])\)\s+(\d+(?:\.\d+)?)ms"
)
_HEAP_RE = re.compile(r"(\d+(?:\.\d+)?)([KMGB])->(\d+(?:\.\d+)?)([KMGB])\((\d+(?:\.\d+)?)([KMGB])\)")
_DURATION_RE = re.compile(r"(?:^|\s)(\d+(?:\.\d+)?)ms\s*$")
_GC_SUMMARY_RE = re.compile(r"\[gc\s*\]")
_LEGACY_START_RE = re.compile(r"\[(?:Full )?GC(?:\s|\()")
MAX_LOG_BYTES = 64 * 1024 * 1024
MAX_COMPRESSED_BYTES = 16 * 1024 * 1024

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
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
    except ValueError:
        try:
            return datetime.strptime(raw, "%Y-%m-%dT%H:%M:%S.%f%z").timestamp()
        except ValueError:
            return None


def _parse_uptime(line: str) -> Optional[float]:
    m = _UPTIME_RE.search(line) or _LEGACY_UPTIME_RE.search(line)
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
    if re.search(r"\]\[info\s*\]\[gc", head) or re.search(r"\]\[gc", head):
        return "unified"
    if _UPTIME_RE.search(head) or (_TS_RE.search(head) and "ms" in head):
        return "unified"
    return "unknown"


# --------------------------------------------------------------------------- #
# Phase classification
# --------------------------------------------------------------------------- #
def _classify_unified(line: str) -> tuple[str, str, bool]:
    """Return (phase, cause, is_stw) for a unified-log line."""
    description = _SEQ_RE.sub("", _HEAP_RE.split(line)[0])
    causes = re.findall(r"\(([^)]+)\)", description)
    cause = causes[-1] if causes else ""

    low = line.lower()
    if "pause full" in low or re.search(r"\bfull\s+gc\b", low):
        return "full", cause or "Full GC", True
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


def _phase_detail(line: str) -> str:
    m = re.search(r"\b(Pause|Concurrent)\s+", line)
    if not m:
        return ""
    description = _HEAP_RE.split(line[m.start():])[0]
    description = _DURATION_RE.sub("", description)
    return re.sub(r"\s+", " ", description).strip()


def complete_prefix(data: bytes) -> bytes:
    """Acknowledge only newline-terminated, completed GC records in live reads.

    An unfinished pause holds back the entire suffix, including interleaved
    completed events, so replay never loses its start/phase information.
    """
    end = data.rfind(b"\n") + 1
    pending: dict[tuple, int] = {}
    legacy_start = None
    legacy_depth = 0
    offset = 0
    for raw in data[:end].splitlines(keepends=True):
        line = raw.decode("utf-8", errors="replace")
        seq_m = _SEQ_RE.search(line)
        if seq_m:
            seq = int(seq_m.group(1))
            detail = _phase_detail(line)
            if re.search(r"\[gc,start\s*\]", line) and "Pause " in line:
                pending.setdefault((seq, detail), offset)
            elif _GC_SUMMARY_RE.search(line) and detail.startswith("Pause ") and not _DURATION_RE.search(line):
                # A completed summary with an invalid duration cannot be fixed
                # by another append. Retire its own start, not other phases.
                for key in list(pending):
                    if key[0] == seq and (not key[1] or detail.startswith(key[1])):
                        pending.pop(key)
            elif _HEAP_RE.search(line) and not _DURATION_RE.search(line):
                pending.setdefault((seq, ""), offset)
            elif _DURATION_RE.search(line) and ("Pause " in line or _HEAP_PAUSE_RE.search(line)):
                if not detail:
                    candidates = [key for key in pending if key[0] == seq]
                    if candidates:
                        pending.pop(max(candidates, key=pending.get))
                else:
                    pending.pop((seq, detail), None)
                pending.pop((seq, ""), None)
        if legacy_start is not None or _LEGACY_START_RE.search(line):
            if legacy_start is None:
                legacy_start = offset
            legacy_depth += line.count("[") - line.count("]")
            if legacy_depth <= 0:
                legacy_start = None
                legacy_depth = 0
        offset += len(raw)
    boundaries = [end, *pending.values()]
    if legacy_start is not None:
        boundaries.append(legacy_start)
    return data[:min(boundaries)]


def _logical_lines(text: str):
    """Join legacy nested-bracket records without losing their timestamp prefix."""
    pending = []
    depth = 0
    start = 0
    for number, line in enumerate(text.splitlines(), 1):
        if pending or _LEGACY_START_RE.search(line):
            if not pending:
                start = number
            pending.append(line)
            depth += line.count("[") - line.count("]")
            if depth > 0:
                continue
            yield start, " ".join(pending), True
            pending = []
            depth = 0
        else:
            yield number, line, True
    if pending:
        yield start, " ".join(pending), False


# --------------------------------------------------------------------------- #
# Main entry point
# --------------------------------------------------------------------------- #
def parse(text: str, node_id: str = "node", start_time: Optional[float] = None) -> ParsedLog:
    """Parse GC evidence; start_time is verified JVM-start Unix epoch seconds.

    No wall-clock is inferred from collection time. An explicit start applies
    only to the first JVM context; later relative restarts need their own anchor.
    Existing wall-clock decorators always win over the supplied anchor.
    """
    if start_time is not None:
        if not isinstance(start_time, (int, float)) or isinstance(start_time, bool) or not math.isfinite(start_time):
            raise ValueError("start_time must be finite JVM-start Unix epoch seconds")
    collector = detect_collector(text)
    java_hint = detect_java_hint(text)
    parsed = ParsedLog(node_id=node_id, collector=collector, java_hint=java_hint)

    # incomplete is a subset of malformed: it must be retried, whereas a
    # newline-terminated malformed record can be skipped with partial quality.
    counts = dict(recognized=0, ignored=0, malformed=0, incomplete=0, unsupported=0, duplicate=0)
    pending: dict = {}
    heaps: dict = {}
    identities: dict = {}
    context = "relative:0"
    restart = 0
    last_uptime = None
    last_seq = None
    used_anchor = False
    logical_lines = list(_logical_lines(text))
    starts = [i for i, (_, line, _) in enumerate(logical_lines) if "Using " in line and not _SEQ_RE.search(line)]
    fingerprints = {}
    for lo, hi in zip(starts, starts[1:] + [len(logical_lines)]):
        digest = hashlib.sha256()
        for _, line, _ in logical_lines[lo:hi]:
            digest.update(line.encode("utf-8"))
            digest.update(b"\n")
        fingerprints[logical_lines[lo][0]] = digest.digest()
    startup_contexts = {}
    for number, line, complete in logical_lines:
        if not complete:
            counts["malformed"] += 1
            counts["incomplete"] += 1
            continue
        timestamp, uptime = _parse_ts(line), _parse_uptime(line)
        seq_m = _SEQ_RE.search(line)
        seq = int(seq_m.group(1)) if seq_m else None
        if timestamp is not None and uptime is not None:
            # Decorator rounding can differ by a few milliseconds across lines.
            context = f"epoch:{round(timestamp - uptime)}"
        elif number in fingerprints:
            fingerprint = fingerprints[number]
            if fingerprint in startup_contexts:
                context = startup_contexts[fingerprint]
                parsed.warnings.append("Identical JVM sections treated as overlapping rotations; indistinguishable relative-only restarts require external boot identity.")
            else:
                if parsed.events:
                    restart += 1
                context = f"epoch:{timestamp}" if timestamp is not None else f"relative:{restart}"
                startup_contexts[fingerprint] = context
            pending.clear()
            heaps.clear()
            last_seq = last_uptime = None
        elif (timestamp is None and uptime is not None and last_uptime is not None
              and uptime < last_uptime and seq == 0 and last_seq not in (None, 0)):
            # Repeated rotation events are checked against their full identity
            # below; a reset lacking boot metadata remains intrinsically ambiguous.
            known = any(k[:4] == (context, None, uptime, seq) for k in identities)
            if not known:
                restart += 1
                context = f"relative:{restart}"
                pending.clear()
                heaps.clear()
        key = (context, seq)
        legacy = _LEGACY_RE.search(line) if _LEGACY_START_RE.search(line) else None
        duration = _DURATION_RE.search(line)
        heap = legacy or _HEAP_RE.search(line)
        detail = _phase_detail(line)
        phase_key = (context, seq, detail)
        phase, cause, is_stw = _classify_unified(line)
        if legacy:
            phase = "full" if "full gc" in line.lower() else "young"
            cause = "Full GC" if phase == "full" else "Young GC"
            detail = cause
        elif re.search(r"\[gc,start\s*\]", line) and detail:
            pending[phase_key] = (phase, cause, is_stw, detail, number)
            counts["ignored"] += 1
            continue
        elif re.search(r"\[gc,(?:phases|cpu)[^\]]*\]", line):
            counts["ignored"] += 1
            continue
        if heap and seq is not None:
            heaps[key] = (heap, number)
        if not detail and heap:
            candidates = [k for k in pending if k[:2] == key]
            if candidates:
                phase_key = max(candidates, key=lambda k: pending[k][4])
        if not legacy and not (duration and (detail or (heap and phase_key in pending))):
            if ("Pause " in line and not re.search(r"\[gc,start\s*\]", line)):
                counts["malformed"] += 1
                if number == logical_lines[-1][0] and not text.endswith(("\n", "\r")):
                    counts["incomplete"] += 1
                elif _GC_SUMMARY_RE.search(line):
                    for pending_key in list(pending):
                        if pending_key[:2] == key and detail.startswith(pending_key[2]):
                            pending.pop(pending_key)
                    heaps.pop(key, None)
            elif detail.startswith("Concurrent"):
                counts["ignored"] += 1
            elif seq is not None and not heap and not re.search(r"\[gc,[^\]]+\]", line):
                if number == logical_lines[-1][0] and not text.endswith(("\n", "\r")):
                    counts["malformed"] += 1
                    counts["incomplete"] += 1
                else:
                    counts["unsupported"] += 1
            else:
                counts["ignored"] += 1
            continue
        sources = [number]
        if phase_key in pending:
            hint = pending.pop(phase_key)
            if not detail:
                phase, cause, is_stw, detail = hint[:4]
            sources.append(hint[4])
        if heap is None and key in heaps:
            heap, heap_line = heaps[key]
            sources.append(heap_line)
        heaps.pop(key, None)
        pause_ms = float(legacy.group(7)) * 1000 if legacy else float(duration.group(1))
        values = [round(_to_mb(heap.group(i), heap.group(i + 1)), 2) for i in (1, 3, 5)] if heap else [None] * 3
        original_timestamp = timestamp
        if timestamp is None and uptime is not None and start_time is not None and restart == 0 and context == "relative:0":
            timestamp = start_time + uptime
            used_anchor = True
        identity = (context, original_timestamp, uptime, seq, phase, detail)
        # Without either clock, a repeated line cannot be proved to be a duplicate.
        if timestamp is None and uptime is None:
            identity += (number,)
        event = GCEvent(seq, timestamp, uptime, phase, cause,
                        pause_ms if is_stw else 0.0, *values, is_stw,
                        jvm_id=context, phase_detail=detail, source_lines=sorted(set(sources)))
        if identity in identities:
            existing = identities[identity]
            for attr in ("heap_before_mb", "heap_after_mb", "heap_total_mb"):
                if getattr(existing, attr) is None:
                    setattr(existing, attr, getattr(event, attr))
            existing.source_lines = sorted(set(existing.source_lines + event.source_lines))
            if existing.pause_ms != event.pause_ms:
                parsed.warnings.append(f"Conflicting durations for GC({seq}) at line {number}; retained the larger pause.")
                existing.pause_ms = max(existing.pause_ms, event.pause_ms)
            counts["duplicate"] += 1
        else:
            identities[identity] = event
            parsed.events.append(event)
            counts["recognized"] += 1
        last_uptime, last_seq = uptime, seq

    if pending:
        counts["malformed"] += len(pending)
        counts["incomplete"] += len(pending)
        parsed.warnings.append("Incomplete GC start records have no matching completion.")
    parsed.heap_max_mb = max((e.heap_total_mb or 0 for e in parsed.events), default=0) or None
    if not parsed.events:
        parsed.warnings.append(
            "No GC pause events were parsed. Confirm the file is a JVM GC log "
            "(unified -Xlog:gc* or legacy -XX:+PrintGCDetails)."
        )
    if collector == "Unknown":
        parsed.warnings.append("Could not positively identify the GC collector.")

    absolute = sum(e.timestamp is not None for e in parsed.events)
    if parsed.events:
        parsed.time_basis = "absolute" if absolute == len(parsed.events) else "mixed" if absolute else "relative"
        parsed.anchor_source = "provided_start_time" if used_anchor else "log_timestamp" if absolute else None
        if absolute == len(parsed.events):
            parsed.events.sort(key=lambda e: e.timestamp)
        elif not absolute and len({e.jvm_id for e in parsed.events}) == 1:
            parsed.events.sort(key=lambda e: e.uptime if e.uptime is not None else float("inf"))
        if absolute < len(parsed.events):
            parsed.warnings.append("Unanchored events retain a relative JVM-uptime timeline; absolute-time correlation is unavailable.")
        if restart:
            parsed.warnings.append("JVM restart context detected. Relative-only restart/rotation identity can be ambiguous without boot timestamps.")
    if counts["malformed"] or counts["unsupported"]:
        skipped = counts["malformed"] - counts["incomplete"]
        parsed.warnings.append(f"Partial GC coverage: skipped {skipped} complete malformed records; {counts['incomplete']} incomplete and {counts['unsupported']} unsupported records.")
    parsed.record_counts = counts
    return parsed


def read_log_text(path, *, max_bytes: int = MAX_LOG_BYTES,
                  max_compressed_bytes: int = MAX_COMPRESSED_BYTES) -> str:
    """Read a bounded regular text/gzip file, including decompressed-size checks."""
    if max_bytes <= 0 or max_compressed_bytes <= 0:
        raise ValueError("File size limits must be positive")
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as fh:
        if not stat.S_ISREG(os.fstat(fh.fileno()).st_mode):
            raise ValueError("GC input must be a regular file")
        magic = fh.read(2)
        compressed = magic == b"\x1f\x8b" or os.fspath(path).endswith(".gz")
        limit = max_compressed_bytes if compressed else max_bytes
        data = magic + fh.read(max(0, limit + 1 - len(magic)))
    if len(data) > limit:
        raise ValueError("GC input exceeds file size limit")
    if compressed:
        with gzip.GzipFile(fileobj=io.BytesIO(data)) as fh:
            data = fh.read(max_bytes + 1)
        if len(data) > max_bytes:
            raise ValueError("GC input exceeds decompressed size limit")
    return data.decode("utf-8", errors="replace")


def parse_file(path: str, node_id: Optional[str] = None,
               start_time: Optional[float] = None, *, max_bytes: int = MAX_LOG_BYTES,
               max_compressed_bytes: int = MAX_COMPRESSED_BYTES) -> ParsedLog:
    text = read_log_text(path, max_bytes=max_bytes, max_compressed_bytes=max_compressed_bytes)
    return parse(text, node_id or os.fspath(path), start_time=start_time)
