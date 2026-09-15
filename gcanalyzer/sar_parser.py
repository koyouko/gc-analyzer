"""
sar (sysstat) output parser.

Mirrors parser.py's contract (raw text -> structured samples) but for OS-level
activity reports instead of GC logs. Two source formats are supported, tried in
this order:

  1. sadf JSON  (`TZ=UTC LC_ALL=C sadf -j -- -A`) — machine-readable, preferred
     when sysstat >= 11.x is available. The exact nested keys have drifted a
     little across sysstat releases, so this parser is deliberately defensive:
     every section is read with `.get()` chains over several known key aliases
     and wrapped so one missing/renamed section never breaks the rest.
  2. sar text   (`TZ=UTC LC_ALL=C sar -A`) — the classic, extremely stable
     columnar report every sysstat version since the 1990s can produce. This is
     the same technique kSar (https://github.com/vlsi/ksar) uses for its
     "classic" parser: identify each section by its header row, then zip the
     header tokens against each data row positionally. It is the most portable
     path (works against ancient sysstat, busybox `sar`, etc.) so it is also
     used as the automatic fallback when JSON parsing comes back empty.

Both paths produce the same normalized `SarSample` shape, so sar_analyzer.py
never needs to know which format the data came from.

`TZ=UTC` matters: sar/sadc store samples as UTC internally and render them in
whatever TZ the *reporting* command runs under, so forcing TZ=UTC on read makes
every node's samples directly comparable (and joinable against GC metrics,
which are recorded as UTC epoch seconds) regardless of the host's local
timezone — without needing to know or guess that timezone.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo


@dataclass
class SarSample:
    """One sar report timestamp's worth of OS metrics for a single host."""

    ts: int  # epoch seconds, UTC

    cpu_user_pct: float | None = None
    cpu_nice_pct: float | None = None
    cpu_system_pct: float | None = None
    cpu_iowait_pct: float | None = None
    cpu_steal_pct: float | None = None
    cpu_idle_pct: float | None = None

    load1: float | None = None
    load5: float | None = None
    load15: float | None = None
    runq_sz: float | None = None
    plist_sz: float | None = None
    blocked: float | None = None

    proc_per_s: float | None = None
    cswch_per_s: float | None = None

    mem_free_mb: float | None = None
    mem_avail_mb: float | None = None
    mem_used_mb: float | None = None
    mem_used_pct: float | None = None
    mem_buffers_mb: float | None = None
    mem_cached_mb: float | None = None
    mem_commit_pct: float | None = None

    swap_used_mb: float | None = None
    swap_used_pct: float | None = None

    # Paging activity (`sar -B`) — kB paged in/out from disk per second and
    # fault rates. On a Kafka broker, sustained majflt/pgpgin is the page
    # cache being cold or memory being reclaimed — an early-warning signal
    # the plain memory-used% number hides. Present on RHEL 8/9 sysstat
    # (11.7.x / 12.5.x) in both `sar -A` text and `sadf -j` JSON.
    pgpgin_kbs: float | None = None
    pgpgout_kbs: float | None = None
    fault_per_s: float | None = None
    majflt_per_s: float | None = None

    # Swapping activity (`sar -W`) — pages swapped in/out per second. Distinct
    # from swap *occupancy* (%swpused above): occupancy says swap was ever
    # touched, activity says the box is actively thrashing right now.
    pswpin_per_s: float | None = None
    pswpout_per_s: float | None = None

    # Per-device breakdown for the busiest devices in this sample (kept small
    # — the analyzer rolls these into "max busiest device" aggregates, and the
    # dashboard only needs the top few for drill-down, not every block device).
    disks: list = field(default_factory=list)   # [{dev, tps, rd_kbs, wr_kbs, util_pct, await_ms}]
    nics: list = field(default_factory=list)    # [{iface, rx_kbs, tx_kbs, rx_pck_s, tx_pck_s, util_pct}]
    interval_seconds: float | None = None


@dataclass
class ParsedSar:
    node_id: str
    source_format: str  # "sadf-json" | "sar-text"
    samples: list  # list[SarSample]
    warnings: list = field(default_factory=list)
    hostname: str = ""


# --------------------------------------------------------------------------- #
# sadf JSON path
# --------------------------------------------------------------------------- #
def _f(d: dict, *keys, default=None) -> float | None:
    """First present numeric value across a list of key aliases."""
    if not isinstance(d, dict):
        return default
    for k in keys:
        if k in d and d[k] is not None:
            try:
                value = float(d[k])
                if math.isfinite(value):
                    return value
            except (TypeError, ValueError):
                continue
    return default


def _mb(d: dict, *keys) -> float | None:
    value = _f(d, *keys)
    return value / 1024.0 if value is not None else None


def _timezone(name: str | None):
    if not name or name in ("UTC", "Z"):
        return timezone.utc
    offset = re.fullmatch(r"([+-])(\d{2}):?(\d{2})", name)
    if offset:
        sign, hours, minutes = offset.groups()
        if int(hours) > 23 or int(minutes) > 59:
            raise ValueError("invalid timezone offset")
        delta = timedelta(hours=int(hours), minutes=int(minutes))
        return timezone(delta if sign == "+" else -delta)
    return ZoneInfo(name)


def _date(date_s: str):
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(date_s, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"invalid SAR report date: {date_s}")


def _localize(dt: datetime, zone):
    early, late = dt.replace(tzinfo=zone, fold=0), dt.replace(tzinfo=zone, fold=1)
    # A wall time in a DST gap/fold cannot identify an observation uniquely.
    if early.utcoffset() != late.utcoffset():
        raise ValueError("ambiguous/nonexistent local SAR time; provide a numeric UTC offset")
    if datetime.fromtimestamp(early.timestamp(), zone).replace(tzinfo=None) != dt:
        raise ValueError("nonexistent local SAR time")
    return early


def _parse_json_timestamp(ts_obj: dict, file_date: str | None, report_timezone=None) -> int | None:
    date_s = ts_obj.get("date") or file_date
    time_s = ts_obj.get("time")
    if not date_s or not time_s:
        raise ValueError("missing SAR timestamp date/time")
    dt = datetime.fromisoformat(f"{_date(date_s).isoformat()}T{time_s}")
    if dt.tzinfo is None:
        utc = ts_obj.get("utc")
        zone = ts_obj.get("timezone") or report_timezone
        if utc in (True, 1, "1"):
            zone = "UTC"
        elif utc in (False, 0, "0") and not zone:
            raise ValueError("local SAR timestamp requires report_timezone or a UTC offset")
        dt = _localize(dt, _timezone(zone))
    return int(dt.timestamp())


def _sample_from_json_stat(stat: dict, file_date: str | None, report_timezone=None) -> SarSample | None:
    ts_obj = stat.get("timestamp") or {}
    ts = _parse_json_timestamp(ts_obj, file_date, report_timezone)
    if ts is None:
        return None

    s = SarSample(ts=ts)
    interval = _f(ts_obj, "interval")
    s.interval_seconds = interval if interval is not None and interval > 0 else None

    for cpu in stat.get("cpu-load") or stat.get("cpu-load-all") or []:
        if str(cpu.get("cpu", "all")).lower() != "all":
            continue
        s.cpu_user_pct = _f(cpu, "usr", "user")
        s.cpu_nice_pct = _f(cpu, "nice")
        s.cpu_system_pct = _f(cpu, "sys", "system")
        s.cpu_iowait_pct = _f(cpu, "iowait")
        s.cpu_steal_pct = _f(cpu, "steal")
        s.cpu_idle_pct = _f(cpu, "idle")
        break

    q = stat.get("queue") or {}
    s.runq_sz = _f(q, "runq-sz", "runq_sz")
    s.plist_sz = _f(q, "plist-sz", "plist_sz")
    s.load1 = _f(q, "ldavg-1", "ldavg_1")
    s.load5 = _f(q, "ldavg-5", "ldavg_5")
    s.load15 = _f(q, "ldavg-15", "ldavg_15")
    s.blocked = _f(q, "blocked")

    pcsw = stat.get("process-and-context-switch") or stat.get("process_and_context_switch") or {}
    s.proc_per_s = _f(pcsw, "proc")
    s.cswch_per_s = _f(pcsw, "cswch")

    mem = stat.get("memory") or {}
    s.mem_free_mb = _mb(mem, "memfree")
    s.mem_avail_mb = _mb(mem, "avail", "memavail")
    s.mem_used_mb = _mb(mem, "memused")
    s.mem_used_pct = _f(mem, "memused-percent", "memused_percent")
    s.mem_buffers_mb = _mb(mem, "buffers")
    s.mem_cached_mb = _mb(mem, "cached")
    s.mem_commit_pct = _f(mem, "commit-percent", "commit_percent")

    swap = stat.get("swap") or {}
    s.swap_used_mb = _mb(swap, "swpused")
    s.swap_used_pct = _f(swap, "swpused-percent", "swpused_percent")

    paging = stat.get("paging") or {}
    s.pgpgin_kbs = _f(paging, "pgpgin", "pgpgin/s")
    s.pgpgout_kbs = _f(paging, "pgpgout", "pgpgout/s")
    s.fault_per_s = _f(paging, "fault", "fault/s")
    s.majflt_per_s = _f(paging, "majflt", "majflt/s")

    # sysstat has used both "swap-pages" and "swap-activity" for `sar -W`.
    swp_act = stat.get("swap-pages") or stat.get("swap_pages") or stat.get("swap-activity") or {}
    s.pswpin_per_s = _f(swp_act, "pswpin", "pswpin/s")
    s.pswpout_per_s = _f(swp_act, "pswpout", "pswpout/s")

    for disk in stat.get("disk") or []:
        dev = disk.get("disk_device") or disk.get("dev") or "?"
        s.disks.append({
            "dev": dev,
            "tps": _f(disk, "tps"),
            "rd_kbs": _f(disk, "rkB/s", "rd_sec_per_sec"),
            "wr_kbs": _f(disk, "wkB/s", "wr_sec_per_sec"),
            "await_ms": _f(disk, "await"),
            "util_pct": _f(disk, "util-percent", "util_percent"),
        })

    net = stat.get("network") or {}
    for nic in net.get("net-dev") or net.get("net_dev") or []:
        iface = nic.get("iface") or "?"
        if iface == "lo":
            continue
        s.nics.append({
            "iface": iface,
            "rx_kbs": _f(nic, "rxkB/s", "rxkb_per_sec"),
            "tx_kbs": _f(nic, "txkB/s", "txkb_per_sec"),
            "rx_pck_s": _f(nic, "rxpck/s"),
            "tx_pck_s": _f(nic, "txpck/s"),
            "util_pct": _f(nic, "ifutil-percent", "ifutil_percent"),
        })

    return s


def parse_sadf_json(text: str, node_id: str, report_date=None, report_timezone=None) -> ParsedSar:
    warnings: list[str] = []
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        return ParsedSar(node_id, "sadf-json", [], warnings=[f"invalid JSON: {exc}"])

    sysstat = data.get("sysstat") if isinstance(data, dict) else None
    hosts = sysstat.get("hosts") if isinstance(sysstat, dict) else None
    if not isinstance(hosts, list) or not hosts or not isinstance(hosts[0], dict):
        return ParsedSar(node_id, "sadf-json", [], warnings=["no 'sysstat.hosts' in sadf JSON"])

    host = hosts[0]
    hostname = host.get("nodename", "")
    file_date = host.get("file-date") or report_date
    samples: list[SarSample] = []
    if len(hosts) > 1:
        warnings.append("multiple SAR hosts: only the first host is used for this instance")
    entries = host.get("statistics") or []
    if not isinstance(entries, list):
        return ParsedSar(node_id, "sadf-json", [], warnings=["invalid SAR statistics list"])
    for stat in entries:
        try:
            stat = _validated_sections(stat, warnings)
            s = _sample_from_json_stat(stat, file_date, report_timezone or host.get("timezone"))
            if s is not None:
                samples.append(s)
        except Exception as exc:  # one bad section must not drop the whole sample
            warnings.append(f"skipped malformed statistics entry: {exc}")

    samples.sort(key=lambda s: s.ts)
    return ParsedSar(node_id, "sadf-json", samples, warnings=warnings, hostname=hostname)


def _validated_sections(stat: dict, warnings: list) -> dict:
    if not isinstance(stat, dict):
        raise ValueError("statistics entry must be an object")
    stat = dict(stat)
    list_sections = {"cpu-load", "cpu-load-all", "disk"}
    dict_sections = {"timestamp", "queue", "memory", "swap", "paging", "swap-pages", "swap_pages",
                     "swap-activity", "process-and-context-switch", "process_and_context_switch", "network"}
    for key in list_sections | dict_sections:
        if key not in stat:
            continue
        expected = list if key in list_sections else dict
        if not isinstance(stat[key], expected):
            warnings.append(f"malformed SAR section: {key}")
            stat[key] = expected()
        elif expected is list:
            valid = [item for item in stat[key] if isinstance(item, dict)]
            if len(valid) != len(stat[key]):
                warnings.append(f"malformed SAR device/CPU entry: {key}")
            stat[key] = valid
    network = dict(stat.get("network") or {})
    for key in ("net-dev", "net_dev"):
        if key in network:
            value = network[key]
            valid = [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []
            if valid != value:
                warnings.append(f"malformed SAR network entries: {key}")
            network[key] = valid
    stat["network"] = network
    return stat


# --------------------------------------------------------------------------- #
# Classic `sar -A` text path (kSar-style: header-driven generic column zip)
# --------------------------------------------------------------------------- #
_TIME_RE = re.compile(r"^\d{2}:\d{2}:\d{2}$")
_DATE_RE = re.compile(r"(\d{2}/\d{2}/\d{4}|\d{4}-\d{2}-\d{2})")

# Header keyword -> section kind. Matched against the *set* of header tokens
# (after the leading fake-timestamp token), so column order/extra columns
# across sysstat versions don't matter.
_SECTION_MARKERS = {
    "cpu": {"%user", "%usr", "%nice", "%system", "%sys", "%iowait", "%idle", "CPU"},
    "queue": {"runq-sz", "plist-sz", "ldavg-1", "ldavg-5", "ldavg-15"},
    "pcsw": {"proc/s", "cswch/s"},
    "mem": {"kbmemfree", "kbmemused", "%memused"},
    "swap": {"kbswpfree", "kbswpused", "%swpused"},
    "paging": {"pgpgin/s", "pgpgout/s", "fault/s", "majflt/s", "pgfree/s"},
    "pswap": {"pswpin/s", "pswpout/s"},
    "disk": {"DEV", "tps"},
    "net": {"IFACE", "rxpck/s", "%ifutil"},
}


def _classify_header(tokens: list[str]) -> str | None:
    tokset = set(tokens)
    best, best_hits = None, 0
    for kind, markers in _SECTION_MARKERS.items():
        hits = len(tokset & markers)
        if hits > best_hits:
            best, best_hits = kind, hits
    return best if best_hits >= 2 else None


def _to_num(tok: str):
    try:
        return float(tok)
    except ValueError:
        return tok


def _parse_block(lines: list[str]) -> tuple[str | None, list[dict]]:
    if not lines:
        return None, []
    header_tokens = lines[0].split()
    if not header_tokens or not (_TIME_RE.match(header_tokens[0]) or header_tokens[0].lower().startswith(("12:", "01:"))):
        return None, []
    _, cols = _time_columns(header_tokens)
    kind = _classify_header(cols)
    if kind is None:
        return None, []

    rows = []
    for line in lines[1:]:
        if line.startswith("Average") or not line.strip():
            continue
        toks = line.split()
        if not toks or not _TIME_RE.match(toks[0]):
            continue
        time_s, vals = _time_columns(toks)
        row = {"_time": time_s}
        for c, v in zip(cols, vals):
            row[c] = _to_num(v)
        rows.append(row)
    return kind, rows


def _time_columns(tokens: list[str]) -> tuple[str, list[str]]:
    if len(tokens) > 1 and tokens[1].upper() in ("AM", "PM"):
        dt = datetime.strptime(" ".join(tokens[:2]).upper(), "%I:%M:%S %p")
        return dt.strftime("%H:%M:%S"), tokens[2:]
    return tokens[0], tokens[1:]


def _epoch(date_s: str, time_s: str, report_timezone=None) -> int | None:
    try:
        dt = datetime.fromisoformat(f"{_date(date_s).isoformat()}T{time_s}")
        dt = _localize(dt, _timezone(report_timezone))
        return int(dt.timestamp())
    except (ValueError, KeyError, TypeError):
        return None


def parse_sar_text(text: str, node_id: str, report_date: str | None = None,
                   report_timezone: str | None = None) -> ParsedSar:
    """Parse `sar -A` (or a concatenation of individual `sar -u/-r/-d/...`
    reports) into samples, keyed by wall-clock time within `report_date`
    (falls back to the date embedded in the report's banner line, then to
    None -> sample is dropped with a warning)."""
    warnings: list[str] = []
    date_s = report_date
    if not date_s:
        m = _DATE_RE.search(text)
        date_s = m.group(1) if m else None
    if not date_s:
        warnings.append("could not determine report date; SAR samples discarded")
        return ParsedSar(node_id, "sar-text", [], warnings=warnings)
    try:
        _date(date_s)
        _timezone(report_timezone)
    except (ValueError, KeyError, TypeError) as exc:
        return ParsedSar(node_id, "sar-text", [], warnings=[f"invalid SAR date/timezone: {exc}"])

    blocks: list[tuple[str, list[str]]] = []
    current: list[str] = []
    block_date = date_s
    for line in text.splitlines():
        tokens = line.split()
        banner_date = _DATE_RE.search(line) if line.lstrip().startswith("Linux") else None
        try:
            is_header = bool(tokens and _TIME_RE.match(tokens[0]) and
                             _classify_header(_time_columns(tokens)[1]))
        except ValueError:
            warnings.append("invalid SAR time token")
            continue
        if not tokens or is_header or banner_date:
            if current:
                blocks.append((block_date, current))
            current = []
        if banner_date:
            block_date = report_date or banner_date.group(1)
            continue
        if tokens:
            current.append(line)
    if current:
        blocks.append((block_date, current))

    by_time: dict[int, dict] = {}

    def bucket(ts: int) -> dict:
        return by_time.setdefault(ts, {
            "cpu": None, "queue": None, "pcsw": None, "mem": None, "swap": None,
            "paging": None, "pswap": None,
            "disks": [], "nics": [],
        })

    for date_s, block in blocks:
        try:
            _date(date_s)
            kind, rows = _parse_block(block)
        except ValueError as exc:
            warnings.append(f"invalid SAR time: {exc}")
            continue
        if kind is None:
            continue
        previous_time, _ = _time_columns(block[0].split())
        day_offset = 0
        previous_ts = _epoch(date_s, previous_time, report_timezone)
        for row in rows:
            t = row["_time"]
            if t < previous_time:
                day_offset += 1
            row_date = (_date(date_s) + timedelta(days=day_offset)).isoformat()
            ts = _epoch(row_date, t, report_timezone)
            previous_time = t
            if ts is None:
                warnings.append("invalid SAR sample timestamp")
                continue
            b = bucket(ts)
            if previous_ts is not None and ts > previous_ts:
                b.setdefault("interval", ts - previous_ts)
            previous_ts = ts
            if kind == "cpu":
                cpu_id = row.get("CPU", "all")
                if str(cpu_id).lower() != "all":
                    continue
                b["cpu"] = row
            elif kind == "queue":
                b["queue"] = row
            elif kind == "pcsw":
                b["pcsw"] = row
            elif kind == "mem":
                b["mem"] = row
            elif kind == "swap":
                b["swap"] = row
            elif kind == "paging":
                b["paging"] = row
            elif kind == "pswap":
                b["pswap"] = row
            elif kind == "disk":
                b["disks"].append(row)
            elif kind == "net":
                iface = row.get("IFACE", "?")
                if iface == "lo":
                    continue
                b["nics"].append(row)

    samples: list[SarSample] = []
    for ts, b in sorted(by_time.items()):
        s = SarSample(ts=ts, interval_seconds=b.get("interval"))
        cpu = b["cpu"] or {}
        s.cpu_user_pct = _f(cpu, "%user", "%usr")
        s.cpu_nice_pct = _f(cpu, "%nice")
        s.cpu_system_pct = _f(cpu, "%system", "%sys")
        s.cpu_iowait_pct = _f(cpu, "%iowait")
        s.cpu_steal_pct = _f(cpu, "%steal")
        s.cpu_idle_pct = _f(cpu, "%idle")

        q = b["queue"] or {}
        s.runq_sz = _f(q, "runq-sz")
        s.plist_sz = _f(q, "plist-sz")
        s.load1 = _f(q, "ldavg-1")
        s.load5 = _f(q, "ldavg-5")
        s.load15 = _f(q, "ldavg-15")
        s.blocked = _f(q, "blocked")

        pcsw = b["pcsw"] or {}
        s.proc_per_s = _f(pcsw, "proc/s")
        s.cswch_per_s = _f(pcsw, "cswch/s")

        mem = b["mem"] or {}
        s.mem_free_mb = _mb(mem, "kbmemfree")
        s.mem_avail_mb = _mb(mem, "kbavail")
        s.mem_used_mb = _mb(mem, "kbmemused")
        s.mem_used_pct = _f(mem, "%memused")
        s.mem_buffers_mb = _mb(mem, "kbbuffers")
        s.mem_cached_mb = _mb(mem, "kbcached")
        s.mem_commit_pct = _f(mem, "%commit")

        swap = b["swap"] or {}
        s.swap_used_mb = _mb(swap, "kbswpused")
        s.swap_used_pct = _f(swap, "%swpused")

        paging = b["paging"] or {}
        s.pgpgin_kbs = _f(paging, "pgpgin/s")
        s.pgpgout_kbs = _f(paging, "pgpgout/s")
        s.fault_per_s = _f(paging, "fault/s")
        s.majflt_per_s = _f(paging, "majflt/s")

        pswap = b["pswap"] or {}
        s.pswpin_per_s = _f(pswap, "pswpin/s")
        s.pswpout_per_s = _f(pswap, "pswpout/s")

        for d in b["disks"]:
            s.disks.append({
                "dev": d.get("DEV", "?"),
                "tps": _f(d, "tps"),
                "rd_kbs": _f(d, "rkB/s", "rd_sec/s"),
                "wr_kbs": _f(d, "wkB/s", "wr_sec/s"),
                "await_ms": _f(d, "await"),
                "util_pct": _f(d, "%util"),
            })
        for n in b["nics"]:
            s.nics.append({
                "iface": n.get("IFACE", "?"),
                "rx_kbs": _f(n, "rxkB/s"),
                "tx_kbs": _f(n, "txkB/s"),
                "rx_pck_s": _f(n, "rxpck/s"),
                "tx_pck_s": _f(n, "txpck/s"),
                "util_pct": _f(n, "%ifutil"),
            })
        samples.append(s)

    return ParsedSar(node_id, "sar-text", samples, warnings=warnings)


# --------------------------------------------------------------------------- #
# Dispatcher
# --------------------------------------------------------------------------- #
def parse(text: str, node_id: str, fmt_hint: str | None = None, report_date: str | None = None,
          report_timezone: str | None = None) -> ParsedSar:
    """Best-effort parse: try JSON first (unless text obviously isn't JSON or
    fmt_hint says otherwise), fall back to the classic text format."""
    stripped = text.lstrip()
    looks_json = stripped.startswith("{")
    if fmt_hint == "json" or (fmt_hint is None and looks_json):
        result = parse_sadf_json(text, node_id, report_date, report_timezone)
        if result.samples or looks_json:
            return result
        # fall through to text parsing in case this was actually text that
        # happened to start with '{' (extremely unlikely) or JSON we couldn't
        # read — better to try the universal fallback than return nothing.
    return parse_sar_text(text, node_id, report_date=report_date, report_timezone=report_timezone)


def parse_file(path: str, node_id: str, **kw) -> ParsedSar:
    with open(path, "r", errors="replace") as fh:
        return parse(fh.read(), node_id, **kw)
