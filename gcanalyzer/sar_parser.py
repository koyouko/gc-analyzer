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
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta


@dataclass
class SarSample:
    """One sar report timestamp's worth of OS metrics for a single host."""

    ts: int  # epoch seconds, UTC

    cpu_user_pct: float = 0.0
    cpu_nice_pct: float = 0.0
    cpu_system_pct: float = 0.0
    cpu_iowait_pct: float = 0.0
    cpu_steal_pct: float = 0.0
    cpu_idle_pct: float = 100.0

    load1: float = 0.0
    load5: float = 0.0
    load15: float = 0.0
    runq_sz: float = 0.0
    plist_sz: float = 0.0
    blocked: float = 0.0

    proc_per_s: float = 0.0
    cswch_per_s: float = 0.0

    mem_free_mb: float = 0.0
    mem_avail_mb: float = 0.0
    mem_used_mb: float = 0.0
    mem_used_pct: float = 0.0
    mem_buffers_mb: float = 0.0
    mem_cached_mb: float = 0.0
    mem_commit_pct: float = 0.0

    swap_used_mb: float = 0.0
    swap_used_pct: float = 0.0

    # Paging activity (`sar -B`) — kB paged in/out from disk per second and
    # fault rates. On a Kafka broker, sustained majflt/pgpgin is the page
    # cache being cold or memory being reclaimed — an early-warning signal
    # the plain memory-used% number hides. Present on RHEL 8/9 sysstat
    # (11.7.x / 12.5.x) in both `sar -A` text and `sadf -j` JSON.
    pgpgin_kbs: float = 0.0
    pgpgout_kbs: float = 0.0
    fault_per_s: float = 0.0
    majflt_per_s: float = 0.0

    # Swapping activity (`sar -W`) — pages swapped in/out per second. Distinct
    # from swap *occupancy* (%swpused above): occupancy says swap was ever
    # touched, activity says the box is actively thrashing right now.
    pswpin_per_s: float = 0.0
    pswpout_per_s: float = 0.0

    # Per-device breakdown for the busiest devices in this sample (kept small
    # — the analyzer rolls these into "max busiest device" aggregates, and the
    # dashboard only needs the top few for drill-down, not every block device).
    disks: list = field(default_factory=list)   # [{dev, tps, rd_kbs, wr_kbs, util_pct, await_ms}]
    nics: list = field(default_factory=list)    # [{iface, rx_kbs, tx_kbs, rx_pck_s, tx_pck_s, util_pct}]


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
def _f(d: dict, *keys, default=0.0) -> float:
    """First present numeric value across a list of key aliases."""
    for k in keys:
        if k in d and d[k] is not None:
            try:
                return float(d[k])
            except (TypeError, ValueError):
                continue
    return default


def _parse_json_timestamp(ts_obj: dict, file_date: str | None) -> int | None:
    try:
        date_s = ts_obj.get("date") or file_date
        time_s = ts_obj.get("time")
        if not date_s or not time_s:
            return None
        # sadf JSON dates are typically MM/DD/YYYY; tolerate YYYY-MM-DD too.
        for fmt in ("%m/%d/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(f"{date_s} {time_s}", fmt).replace(tzinfo=timezone.utc)
                return int(dt.timestamp())
            except ValueError:
                continue
    except Exception:
        pass
    return None


def _sample_from_json_stat(stat: dict, file_date: str | None) -> SarSample | None:
    ts_obj = stat.get("timestamp") or {}
    ts = _parse_json_timestamp(ts_obj, file_date)
    if ts is None:
        return None

    s = SarSample(ts=ts)

    for cpu in stat.get("cpu-load") or stat.get("cpu-load-all") or []:
        if str(cpu.get("cpu", "all")).lower() != "all":
            continue
        s.cpu_user_pct = _f(cpu, "usr", "user")
        s.cpu_nice_pct = _f(cpu, "nice")
        s.cpu_system_pct = _f(cpu, "sys", "system")
        s.cpu_iowait_pct = _f(cpu, "iowait")
        s.cpu_steal_pct = _f(cpu, "steal")
        s.cpu_idle_pct = _f(cpu, "idle", default=100.0)
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
    s.mem_free_mb = _f(mem, "memfree") / 1024.0
    s.mem_avail_mb = _f(mem, "avail", "memavail") / 1024.0
    s.mem_used_mb = _f(mem, "memused") / 1024.0
    s.mem_used_pct = _f(mem, "memused-percent", "memused_percent")
    s.mem_buffers_mb = _f(mem, "buffers") / 1024.0
    s.mem_cached_mb = _f(mem, "cached") / 1024.0
    s.mem_commit_pct = _f(mem, "commit-percent", "commit_percent")

    swap = stat.get("swap") or {}
    s.swap_used_mb = _f(swap, "swpused") / 1024.0
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


def parse_sadf_json(text: str, node_id: str) -> ParsedSar:
    warnings: list[str] = []
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        return ParsedSar(node_id, "sadf-json", [], warnings=[f"invalid JSON: {exc}"])

    hosts = (((data or {}).get("sysstat") or {}).get("hosts")) or []
    if not hosts:
        return ParsedSar(node_id, "sadf-json", [], warnings=["no 'sysstat.hosts' in sadf JSON"])

    host = hosts[0]
    hostname = host.get("nodename", "")
    file_date = host.get("file-date")
    samples: list[SarSample] = []
    for stat in host.get("statistics") or []:
        try:
            s = _sample_from_json_stat(stat, file_date)
            if s is not None:
                samples.append(s)
        except Exception as exc:  # one bad section must not drop the whole sample
            warnings.append(f"skipped malformed statistics entry: {exc}")

    samples.sort(key=lambda s: s.ts)
    return ParsedSar(node_id, "sadf-json", samples, warnings=warnings, hostname=hostname)


# --------------------------------------------------------------------------- #
# Classic `sar -A` text path (kSar-style: header-driven generic column zip)
# --------------------------------------------------------------------------- #
_TIME_RE = re.compile(r"^\d{2}:\d{2}:\d{2}$")
_DATE_RE = re.compile(r"(\d{2}/\d{2}/\d{4})")

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
    cols = header_tokens[1:]
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
        time_s = toks[0]
        vals = toks[1:]
        row = {"_time": time_s}
        for c, v in zip(cols, vals):
            row[c] = _to_num(v)
        rows.append(row)
    return kind, rows


def _epoch(date_s: str, time_s: str) -> int | None:
    try:
        dt = datetime.strptime(f"{date_s} {time_s}", "%m/%d/%Y %H:%M:%S").replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except ValueError:
        return None


def parse_sar_text(text: str, node_id: str, report_date: str | None = None) -> ParsedSar:
    """Parse `sar -A` (or a concatenation of individual `sar -u/-r/-d/...`
    reports) into samples, keyed by wall-clock time within `report_date`
    (falls back to the date embedded in the report's banner line, then to
    None -> sample is dropped with a warning)."""
    warnings: list[str] = []
    date_s = report_date
    if not date_s:
        m = _DATE_RE.search(text.splitlines()[0]) if text.splitlines() else None
        date_s = m.group(1) if m else None
    if not date_s:
        warnings.append("could not determine report date; SAR samples discarded")
        return ParsedSar(node_id, "sar-text", [], warnings=warnings)

    blocks: list[list[str]] = []
    current: list[str] = []
    for line in text.splitlines():
        if line.strip() == "":
            if current:
                blocks.append(current)
            current = []
        else:
            current.append(line)
    if current:
        blocks.append(current)

    by_time: dict[str, dict] = {}

    def bucket(time_s: str) -> dict:
        return by_time.setdefault(time_s, {
            "cpu": None, "queue": None, "pcsw": None, "mem": None, "swap": None,
            "paging": None, "pswap": None,
            "disks": [], "nics": [],
        })

    for block in blocks:
        kind, rows = _parse_block(block)
        if kind is None:
            continue
        for row in rows:
            t = row["_time"]
            b = bucket(t)
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
    for time_s in sorted(by_time):
        ts = _epoch(date_s, time_s)
        if ts is None:
            continue
        b = by_time[time_s]
        s = SarSample(ts=ts)
        cpu = b["cpu"] or {}
        s.cpu_user_pct = _f(cpu, "%user", "%usr")
        s.cpu_nice_pct = _f(cpu, "%nice")
        s.cpu_system_pct = _f(cpu, "%system", "%sys")
        s.cpu_iowait_pct = _f(cpu, "%iowait")
        s.cpu_steal_pct = _f(cpu, "%steal")
        s.cpu_idle_pct = _f(cpu, "%idle", default=100.0)

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
        s.mem_free_mb = _f(mem, "kbmemfree") / 1024.0
        s.mem_avail_mb = _f(mem, "kbavail") / 1024.0
        s.mem_used_mb = _f(mem, "kbmemused") / 1024.0
        s.mem_used_pct = _f(mem, "%memused")
        s.mem_buffers_mb = _f(mem, "kbbuffers") / 1024.0
        s.mem_cached_mb = _f(mem, "kbcached") / 1024.0
        s.mem_commit_pct = _f(mem, "%commit")

        swap = b["swap"] or {}
        s.swap_used_mb = _f(swap, "kbswpused") / 1024.0
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
def parse(text: str, node_id: str, fmt_hint: str | None = None, report_date: str | None = None) -> ParsedSar:
    """Best-effort parse: try JSON first (unless text obviously isn't JSON or
    fmt_hint says otherwise), fall back to the classic text format."""
    stripped = text.lstrip()
    looks_json = stripped.startswith("{")
    if fmt_hint == "json" or (fmt_hint is None and looks_json):
        result = parse_sadf_json(text, node_id)
        if result.samples:
            return result
        # fall through to text parsing in case this was actually text that
        # happened to start with '{' (extremely unlikely) or JSON we couldn't
        # read — better to try the universal fallback than return nothing.
    return parse_sar_text(text, node_id, report_date=report_date)


def parse_file(path: str, node_id: str, **kw) -> ParsedSar:
    with open(path, "r", errors="replace") as fh:
        return parse(fh.read(), node_id, **kw)
