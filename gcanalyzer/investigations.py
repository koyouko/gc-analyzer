"""Bounded, stateless analysis of operator-supplied logs. No fleet-store access."""

from __future__ import annotations

import base64
import binascii
import gzip
import io
import statistics
from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from . import analyzer, parser, sar_analyzer, sar_parser

MAX_REQUEST_BYTES = 12 * 1024 * 1024
MAX_FILE_BYTES = 4 * 1024 * 1024
MAX_TOTAL_BYTES = 8 * 1024 * 1024
MAX_FILES = 12
BUCKET_SECONDS = 60


def _decode_file(file: dict) -> tuple[str, str, int]:
    if not isinstance(file, dict):
        raise ValueError("Each file must have a name and content.")
    name, content = file.get("name", "upload"), file.get("content")
    if not isinstance(name, str) or not name or len(name) > 255 or not isinstance(content, str):
        raise ValueError("Provide a short file name and text content.")
    encoding = file.get("encoding", "text")
    if encoding == "text":
        raw = content.encode("utf-8")
    elif encoding == "gzip-base64":
        try:
            compressed = base64.b64decode(content, validate=True)
            with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as archive:
                raw = archive.read(MAX_FILE_BYTES + 1)
        except (binascii.Error, OSError, EOFError) as exc:
            raise ValueError(f"Invalid gzip file: {name}") from exc
    else:
        raise ValueError("Supported encodings are text and gzip-base64.")
    if len(raw) > MAX_FILE_BYTES:
        raise ValueError(f"File exceeds the {MAX_FILE_BYTES // 1024 // 1024} MiB expanded size limit.")
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError(f"{name} is not UTF-8 text. Export binary SAR data as sadf JSON first.") from exc
    if "\x00" in text:
        raise ValueError(f"{name} contains binary data. Supply a text or JSON export.")
    return name, text, len(raw)


def _start_epoch(value: str | None) -> float | None:
    if value is None or value == "":
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None or dt.utcoffset() is None:
            raise ValueError
        return dt.timestamp()
    except (ValueError, TypeError, AttributeError, OverflowError) as exc:
        raise ValueError("JVM start time must be ISO-8601 with a timezone, for example 2026-09-12T00:00:00Z.") from exc


def _correlation(parsed, host) -> dict:
    unavailable = {"status": "unavailable", "matched_minutes": 0, "series": []}
    if host is None or not host.samples:
        return {**unavailable, "summary": "Host data not supplied or not recognized."}
    if not parsed.events or any(e.timestamp is None for e in parsed.events):
        return {**unavailable, "summary": "A confirmed UTC timeline is needed to compare GC and host data."}
    gc, sar = {}, {}
    for event in parsed.events:
        if event.is_stw:
            gc.setdefault(int(event.timestamp // BUCKET_SECONDS), []).append(event.pause_ms)
    for sample in host.samples:
        sar.setdefault(int(sample.ts // BUCKET_SECONDS), []).append(sample)
    series = []
    for minute in sorted(gc.keys() & sar.keys()):
        samples = sar[minute]
        cpu = [100 - s.cpu_idle_pct for s in samples if s.cpu_idle_pct is not None]
        io_wait = [s.cpu_iowait_pct for s in samples if s.cpu_iowait_pct is not None]
        memory = [s.mem_avail_mb for s in samples if s.mem_avail_mb is not None]
        series.append({"t": minute * BUCKET_SECONDS, "pause_max_ms": max(gc[minute]),
                       "cpu_busy_pct": statistics.fmean(cpu) if cpu else None,
                       "iowait_pct": statistics.fmean(io_wait) if io_wait else None,
                       "mem_available_mb": statistics.fmean(memory) if memory else None})
    usable = [r for r in series if r["cpu_busy_pct"] is not None]
    coefficient = None
    if len(usable) >= 6:
        try:
            coefficient = round(statistics.correlation([r["pause_max_ms"] for r in usable],
                                                       [r["cpu_busy_pct"] for r in usable]), 3)
        except statistics.StatisticsError:
            pass
    return {"status": "available" if series else "unavailable", "matched_minutes": len(series),
            "series": series[:1500], "cpu_pause_correlation": coefficient,
            "summary": ("GC and host samples overlap. Association does not confirm a cause or Kafka impact."
                        if series else "No overlapping GC and host samples in the same UTC minute."),
            "sampling_note": "Only minutes containing both observed GC events and host samples are compared; gaps are not zero-filled."}


def analyze_upload(data: dict) -> dict:
    if not isinstance(data, dict):
        raise ValueError("Provide a JSON object containing files.")
    files = data.get("files")
    if not isinstance(files, list) or not 1 <= len(files) <= MAX_FILES:
        raise ValueError(f"Provide between 1 and {MAX_FILES} GC files.")
    anchor = _start_epoch(data.get("start_time"))
    groups: dict[str, list] = {}
    total = 0
    for index, file in enumerate(files):
        name, text, size = _decode_file(file)
        total += size
        if total > MAX_TOTAL_BYTES:
            raise ValueError("Combined expanded upload exceeds the 8 MiB limit.")
        group = file.get("group")
        if group is not None and (not isinstance(group, str) or not group.strip() or len(group) > 255):
            raise ValueError("A JVM group must be a short non-empty name.")
        # Files remain independent unless the operator explicitly identifies a shared JVM.
        key = f"group:{group}" if group else f"file:{index}"
        groups.setdefault(key, []).append((name, text))
    host = None
    if data.get("sar") is not None:
        if len(groups) != 1:
            raise ValueError("Host data can only be compared with one JVM group at a time.")
        name, text, size = _decode_file(data["sar"])
        total += size
        if total > MAX_TOTAL_BYTES:
            raise ValueError("Combined expanded upload exceeds the 8 MiB limit.")
        report_date = data.get("report_date")
        if report_date is not None:
            try:
                datetime.strptime(report_date, "%Y-%m-%d")
            except (ValueError, TypeError) as exc:
                raise ValueError("SAR report date must be YYYY-MM-DD.") from exc
        report_timezone = data.get("report_timezone", "UTC")
        try:
            if not isinstance(report_timezone, str) or len(report_timezone) > 100:
                raise ValueError
            if report_timezone != "UTC":
                ZoneInfo(report_timezone)
        except (ValueError, ZoneInfoNotFoundError) as exc:
            raise ValueError("SAR timezone must be UTC or a valid IANA timezone name.") from exc
        host = sar_parser.parse(text, node_id=name, report_date=report_date, report_timezone=report_timezone)
    if total > MAX_TOTAL_BYTES:
        raise ValueError("Combined expanded upload exceeds the 8 MiB limit.")
    results, logs = [], []
    for group_files in groups.values():
        name = group_files[0][0]
        text = "\n".join(content for _, content in group_files)
        parsed = parser.parse(text, node_id=name, **({"start_time": anchor} if anchor is not None else {}))
        result = analyzer.analyze(parsed)
        if not result["metrics"]["stw_count"]:
            for key in ("avg_pause_ms", "max_pause_ms", "p50_pause_ms", "p95_pause_ms", "p99_pause_ms"):
                result["metrics"][key] = None
        absolute = bool(parsed.events) and all(e.timestamp is not None for e in parsed.events)
        times = [e.timestamp if absolute else e.uptime for e in parsed.events]
        times = [t for t in times if t is not None]
        quality = {"state": "missing" if not parsed.events else ("available" if absolute and result["health"]["score"] is not None else "partial"),
                   "event_count": len(parsed.events), "start": min(times) if times else None,
                   "end": max(times) if times else None, "warnings": list(parsed.warnings)}
        results.append({"name": name, "files": [name for name, _ in group_files],
                        "time_basis": "utc" if absolute else "relative", "quality": quality, "analysis": result})
        logs.append(parsed)
    correlation = _correlation(logs[0], host) if len(logs) == 1 else {
        "status": "unavailable", "matched_minutes": 0, "series": [], "summary": "Select one JVM group for host comparison."}
    return {"status": "Analysis complete", "persisted": False, "generated_at": datetime.now(timezone.utc).isoformat(),
            "results": results, "host": sar_analyzer.analyze(host) if host else None, "correlation": correlation,
            "limitations": ["Uploaded data is not added to fleet history or learning baselines.",
                            "Kafka service impact cannot be confirmed without Kafka metrics."]}
