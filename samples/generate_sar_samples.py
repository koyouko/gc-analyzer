"""
Generate a realistic `sar -A` text dump and an equivalent `sadf -j -- -A` JSON
dump for one synthetic day (10-minute intervals, the sysstat default), so
sar_parser.py / sar_analyzer.py have offline fixtures to test and demo against
— mirrors samples/generate_samples.py's role for GC logs.

Profile: broker-1, a moderately busy host with a short CPU+iowait pressure
window mid-afternoon (correlates with the GC pressure already injected into
broker-2 in samples/generate_samples.py, for a believable joint demo).

Run:  python -m samples.generate_sar_samples
"""

from __future__ import annotations

import json
import os
import random
from datetime import datetime, timedelta, timezone

random.seed(11)
HERE = os.path.dirname(os.path.abspath(__file__))
DAY = datetime(2026, 6, 8, tzinfo=timezone.utc)
INTERVAL_MIN = 10
N = (24 * 60) // INTERVAL_MIN


def times():
    for i in range(N):
        yield DAY + timedelta(minutes=i * INTERVAL_MIN)


def pressure_factor(t: datetime) -> float:
    """0..1, peaking 14:00-15:30 UTC to create a believable CPU/iowait window."""
    minutes = t.hour * 60 + t.minute
    peak = 14 * 60 + 45
    width = 90
    d = abs(minutes - peak)
    return max(0.0, 1.0 - d / width) if d < width else 0.0


def sample(t: datetime) -> dict:
    p = pressure_factor(t)
    cpu_user = round(8 + 35 * p + random.uniform(-1.5, 1.5), 2)
    cpu_iowait = round(1 + 18 * p + random.uniform(-0.5, 0.5), 2)
    cpu_system = round(2 + 6 * p + random.uniform(-0.5, 0.5), 2)
    cpu_idle = round(max(0.0, 100 - cpu_user - cpu_iowait - cpu_system), 2)
    mem_used_pct = round(55 + 15 * p + random.uniform(-2, 2), 2)
    swap_used_pct = round(max(0.0, 2.0 * max(0.0, p - 0.7)), 2)
    disk_util = round(10 + 70 * p + random.uniform(-3, 3), 2)
    disk_await = round(2 + 35 * p + random.uniform(-1, 1), 2)
    net_tx = round(8000 + 25000 * (0.4 + 0.6 * p) + random.uniform(-500, 500), 1)
    net_rx = round(net_tx * random.uniform(0.85, 1.05), 1)
    net_util = round(5 + 40 * p + random.uniform(-2, 2), 2)
    load1 = round(1.0 + 5.0 * p + random.uniform(-0.3, 0.3), 2)
    return {
        "t": t, "cpu_user": cpu_user, "cpu_nice": 0.0, "cpu_system": cpu_system,
        "cpu_iowait": cpu_iowait, "cpu_steal": 0.0, "cpu_idle": cpu_idle,
        "proc_s": round(0.3 + 0.4 * p, 2), "cswch_s": round(400 + 900 * p, 1),
        "runq_sz": int(p * 3), "plist_sz": 215 + int(p * 10),
        "ldavg1": load1, "ldavg5": round(load1 * 0.9, 2), "ldavg15": round(load1 * 0.8, 2), "blocked": int(p * 2),
        "kbmemfree": int(1_200_000 - 600_000 * (mem_used_pct / 100)),
        "kbavail": int(6_000_000 - 1_000_000 * (mem_used_pct / 100)),
        "kbmemused": int(8_000_000 * (mem_used_pct / 100)), "memused_pct": mem_used_pct,
        "kbbuffers": 120_000, "kbcached": int(2_500_000 * (1 - 0.3 * p)),
        "kbcommit": 5_500_000, "commit_pct": round(60 + 5 * p, 2),
        "kbswpused": int(80_000 * swap_used_pct / 5.0) if swap_used_pct else 0, "swpused_pct": swap_used_pct,
        "disk_tps": round(40 + 250 * p, 2), "disk_rkbs": round(800 + 4000 * p, 2),
        "disk_wkbs": round(2000 + 9000 * p, 2), "disk_await": disk_await, "disk_util": disk_util,
        "net_rxpck": round(net_rx / 1.2, 1), "net_txpck": round(net_tx / 1.3, 1),
        "net_rxkb": net_rx, "net_txkb": net_tx, "net_util": net_util,
    }


def build_samples() -> list[dict]:
    return [sample(t) for t in times()]


def write_sar_text(samples: list[dict], path: str) -> None:
    d0 = samples[0]["t"]
    lines = [f"Linux 5.15.0-kafka-broker (broker-1) \t{d0:%m/%d/%Y} \t_x86_64_\t(8 CPU)", ""]

    def hdr_time(t: datetime) -> str:
        return t.strftime("%H:%M:%S")

    # Real `sar` output repeats a leading time token on the header row itself
    # (sar_parser.py relies on this to recognize section boundaries), e.g.:
    #   00:00:00        CPU     %user     %nice   %system   %iowait    %steal     %idle
    h0 = hdr_time(samples[0]["t"])

    lines.append(f"{h0:>15} {'CPU':>4} {'%user':>9} {'%nice':>9} {'%system':>9} {'%iowait':>9} {'%steal':>9} {'%idle':>9}")
    for s in samples:
        lines.append(f"{hdr_time(s['t']):>15} all {s['cpu_user']:9.2f} {s['cpu_nice']:9.2f} "
                      f"{s['cpu_system']:9.2f} {s['cpu_iowait']:9.2f} {s['cpu_steal']:9.2f} {s['cpu_idle']:9.2f}")
    lines.append("")

    lines.append(f"{h0:>15} {'proc/s':>9} {'cswch/s':>9}")
    for s in samples:
        lines.append(f"{hdr_time(s['t']):>15} {s['proc_s']:9.2f} {s['cswch_s']:9.1f}")
    lines.append("")

    lines.append(f"{h0:>15} {'runq-sz':>9} {'plist-sz':>9} {'ldavg-1':>9} {'ldavg-5':>9} {'ldavg-15':>9} {'blocked':>9}")
    for s in samples:
        lines.append(f"{hdr_time(s['t']):>15} {s['runq_sz']:9d} {s['plist_sz']:9d} "
                      f"{s['ldavg1']:9.2f} {s['ldavg5']:9.2f} {s['ldavg15']:9.2f} {s['blocked']:9d}")
    lines.append("")

    lines.append(f"{h0:>15} {'kbmemfree':>9} {'kbavail':>9} {'kbmemused':>9} {'%memused':>9} "
                  f"{'kbbuffers':>9} {'kbcached':>9} {'kbcommit':>9} {'%commit':>9}")
    for s in samples:
        lines.append(f"{hdr_time(s['t']):>15} {s['kbmemfree']:9d} {s['kbavail']:9d} {s['kbmemused']:9d} "
                      f"{s['memused_pct']:9.2f} {s['kbbuffers']:9d} {s['kbcached']:9d} "
                      f"{s['kbcommit']:9d} {s['commit_pct']:9.2f}")
    lines.append("")

    lines.append(f"{h0:>15} {'kbswpfree':>9} {'kbswpused':>9} {'%swpused':>9} {'kbswpcad':>9} {'%swpcad':>9}")
    for s in samples:
        lines.append(f"{hdr_time(s['t']):>15} {0:9d} {s['kbswpused']:9d} {s['swpused_pct']:9.2f} {0:9d} {0.0:9.2f}")
    lines.append("")

    lines.append(f"{h0:>15} {'DEV':>9} {'tps':>9} {'rkB/s':>9} {'wkB/s':>9} {'await':>9} {'%util':>9}")
    for s in samples:
        for dev, frac in (("sda", 1.0), ("sdb", 0.4)):
            lines.append(f"{hdr_time(s['t']):>15} {dev:>9} {s['disk_tps'] * frac:9.2f} "
                          f"{s['disk_rkbs'] * frac:9.2f} {s['disk_wkbs'] * frac:9.2f} "
                          f"{s['disk_await']:9.2f} {min(99.9, s['disk_util'] * frac):9.2f}")
    lines.append("")

    lines.append(f"{h0:>15} {'IFACE':>9} {'rxpck/s':>9} {'txpck/s':>9} {'rxkB/s':>9} {'txkB/s':>9} {'%ifutil':>9}")
    for s in samples:
        lines.append(f"{hdr_time(s['t']):>15} {'eth0':>9} {s['net_rxpck']:9.1f} {s['net_txpck']:9.1f} "
                      f"{s['net_rxkb']:9.1f} {s['net_txkb']:9.1f} {s['net_util']:9.2f}")
    lines.append("")

    with open(path, "w") as fh:
        fh.write("\n".join(lines) + "\n")


def write_sadf_json(samples: list[dict], path: str) -> None:
    stats = []
    for s in samples:
        t = s["t"]
        stats.append({
            "timestamp": {"date": t.strftime("%m/%d/%Y"), "time": t.strftime("%H:%M:%S"), "utc": 1, "interval": INTERVAL_MIN * 60},
            "cpu-load": [{"cpu": "all", "usr": s["cpu_user"], "nice": s["cpu_nice"], "sys": s["cpu_system"],
                          "iowait": s["cpu_iowait"], "steal": s["cpu_steal"], "idle": s["cpu_idle"]}],
            "process-and-context-switch": {"proc": s["proc_s"], "cswch": s["cswch_s"]},
            "queue": {"runq-sz": s["runq_sz"], "plist-sz": s["plist_sz"], "ldavg-1": s["ldavg1"],
                      "ldavg-5": s["ldavg5"], "ldavg-15": s["ldavg15"], "blocked": s["blocked"]},
            "memory": {"memfree": s["kbmemfree"], "avail": s["kbavail"], "memused": s["kbmemused"],
                       "memused-percent": s["memused_pct"], "buffers": s["kbbuffers"], "cached": s["kbcached"],
                       "commit": s["kbcommit"], "commit-percent": s["commit_pct"]},
            "swap": {"swpused": s["kbswpused"], "swpused-percent": s["swpused_pct"]},
            "disk": [
                {"disk_device": "sda", "tps": s["disk_tps"], "rkB/s": s["disk_rkbs"], "wkB/s": s["disk_wkbs"],
                 "await": s["disk_await"], "util-percent": min(99.9, s["disk_util"])},
                {"disk_device": "sdb", "tps": round(s["disk_tps"] * 0.4, 2), "rkB/s": round(s["disk_rkbs"] * 0.4, 2),
                 "wkB/s": round(s["disk_wkbs"] * 0.4, 2), "await": s["disk_await"],
                 "util-percent": min(99.9, round(s["disk_util"] * 0.4, 2))},
            ],
            "network": {"net-dev": [
                {"iface": "eth0", "rxpck/s": s["net_rxpck"], "txpck/s": s["net_txpck"],
                 "rxkB/s": s["net_rxkb"], "txkB/s": s["net_txkb"], "ifutil-percent": s["net_util"]},
            ]},
        })

    payload = {
        "sysstat": {
            "hosts": [{
                "nodename": "broker-1",
                "sysname": "Linux",
                "release": "5.15.0-kafka-broker",
                "machine": "x86_64",
                "number-of-cpus": 8,
                "file-date": samples[0]["t"].strftime("%m/%d/%Y"),
                "statistics": stats,
            }]
        }
    }
    with open(path, "w") as fh:
        json.dump(payload, fh, indent=1)


def main() -> None:
    samples = build_samples()
    write_sar_text(samples, os.path.join(HERE, "broker-1-sar.txt"))
    write_sadf_json(samples, os.path.join(HERE, "broker-1-sar.json"))
    print(f"wrote {len(samples)} samples -> broker-1-sar.txt, broker-1-sar.json")


if __name__ == "__main__":
    main()
