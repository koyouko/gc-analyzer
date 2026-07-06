"""
Seed the SQLite store with 30 days of hourly host (SAR) metrics for the whole
demo fleet, correlated with the GC incidents seed_history.py already injects —
so correlate.py and scaling_advisor.py have something real to say out of the
box, the same way seed_history.py gives the GC dashboard something to show.

Run:  python -m seed.seed_sar_history     (writes ./gc_history.db, same DB
                                            seed_history.py uses — run that
                                            first so instances exist)

Design, mirroring seed_history.py's incident list exactly (imported, not
duplicated, so the time windows can't drift apart):

  * Each GC incident is tagged "host_driven" (host resources spike alongside
    the GC pressure — a noisy-neighbor / genuine-contention story) or
    "gc_only" (host stays calm — a pure JVM heap-sizing story). This gives
    correlate.py's per-instance verdict real variety (host_bound vs gc_bound)
    instead of a single contrived case.
  * A handful of instances get a permanent baseline multiplier independent of
    any GC incident, to create realistic *cross-node* shape for
    scaling_advisor.py: DEMO-KRAFT--broker-3 runs hot relative to its peers
    (skew -> "rebalance" verdict), while all three DEMO-ZK brokers run
    uniformly close to their CPU ceiling (low skew, uniform pressure ->
    "horizontal" verdict). Everything else stays comfortably healthy (->
    "no_action" elsewhere in the fleet).
"""

from __future__ import annotations

import math
import os
import random
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from gcanalyzer import store, topology  # noqa: E402
from seed.seed_history import build_incidents, diurnal  # noqa: E402

random.seed(13)

DAYS = 30
HOUR = 3600

ROLE_PROFILE = {
    # (cpu_busy_lo, cpu_busy_hi), iowait_hi, mem_lo_hi, net_tx_kbs_base, disk_util_hi
    "broker":          {"cpu": (18, 38), "iowait_hi": 4.0, "mem": (45, 68), "net_tx": (6000, 22000), "disk_hi": 22},
    "schema-registry": {"cpu": (6, 16), "iowait_hi": 1.0, "mem": (30, 48), "net_tx": (300, 1500), "disk_hi": 5},
    "connect":         {"cpu": (10, 24), "iowait_hi": 2.0, "mem": (35, 55), "net_tx": (1500, 6000), "disk_hi": 10},
    "controller":      {"cpu": (5, 14), "iowait_hi": 1.0, "mem": (25, 42), "net_tx": (500, 2200), "disk_hi": 4},
    "zookeeper":        {"cpu": (6, 16), "iowait_hi": 1.5, "mem": (28, 46), "net_tx": (400, 1800), "disk_hi": 6},
}

# Permanent baseline multipliers (independent of any GC incident) that shape
# the cluster-level scaling_advisor demo: one hot/skewed node in DEMO-KRAFT,
# uniformly-pressured brokers in DEMO-ZK.
INSTANCE_CPU_MULT = {
    "DEMO-KRAFT--broker-3": 2.15,
    "DEMO-ZK--broker-1": 1.85,
    "DEMO-ZK--broker-2": 1.85,
    "DEMO-ZK--broker-3": 1.85,
}

# Whether each GC incident's window also spikes host resources. Anything not
# listed defaults to "host_driven" (the more common real-world case).
HOST_BEHAVIOR = {
    ("DEMO-ZK--broker-1", "heap_pressure"): "gc_only",
    ("DEMO-ZK--broker-3", "full_gc_storm"): "gc_only",
    ("DEMO-KRAFT--broker-1", "heap_pressure"): "gc_only",
}


def base_row(inst, ts: int) -> dict:
    p = ROLE_PROFILE[inst.role]
    hour = (ts // HOUR) % 24
    load = diurnal(hour, inst.busy_hour_utc)
    mult = INSTANCE_CPU_MULT.get(inst.id, 1.0)

    cpu_lo, cpu_hi = p["cpu"]
    cpu_busy = (cpu_lo + (cpu_hi - cpu_lo) * load) * mult + random.uniform(-2, 2)
    cpu_busy = max(2.0, min(98.0, cpu_busy))
    iowait = max(0.2, p["iowait_hi"] * load * (0.6 + 0.5 * (mult - 1)) + random.uniform(-0.3, 0.3))
    cpu_system = cpu_busy * random.uniform(0.18, 0.28)
    cpu_user = max(0.0, cpu_busy - cpu_system - iowait)

    mem_lo, mem_hi = p["mem"]
    mem_used = (mem_lo + (mem_hi - mem_lo) * load) + random.uniform(-2, 2)

    net_lo, net_hi = p["net_tx"]
    net_tx = (net_lo + (net_hi - net_lo) * load) * mult * random.uniform(0.9, 1.1)
    net_rx = net_tx * random.uniform(0.85, 1.05)
    net_util = min(95.0, (net_tx / (net_hi * 2.2)) * 100 * mult)

    disk_util = min(95.0, p["disk_hi"] * (0.5 + load) * mult * random.uniform(0.8, 1.2))
    disk_await = max(0.5, disk_util * 0.45 + random.uniform(-1, 1))

    load1 = max(0.05, cpu_busy / 100 * 8 * random.uniform(0.8, 1.2))

    return {
        "sample_count": 6,
        "cpu_user_pct_avg": round(cpu_user, 2),
        "cpu_system_pct_avg": round(cpu_system, 2),
        "cpu_iowait_pct_avg": round(iowait, 2),
        "cpu_busy_pct_avg": round(cpu_busy, 2),
        "cpu_busy_pct_max": round(min(99.5, cpu_busy * random.uniform(1.05, 1.2)), 2),
        "cpu_iowait_pct_max": round(min(99.0, iowait * random.uniform(1.2, 1.8)), 2),
        "load1_avg": round(load1, 2),
        "load5_avg": round(load1 * 0.92, 2),
        "runq_sz_avg": round(max(0.0, (cpu_busy - 60) / 20), 2),
        "cswch_per_s_avg": round(300 + cpu_busy * 12, 1),
        "mem_used_pct_avg": round(max(5.0, min(97.0, mem_used)), 2),
        "mem_used_pct_max": round(max(5.0, min(98.0, mem_used + random.uniform(1, 4))), 2),
        "mem_cached_mb_avg": round(inst.heap_max_mb * random.uniform(2.5, 4.0), 1),
        "swap_used_pct_avg": 0.0,
        "swap_used_pct_max": 0.0,
        "disk_util_pct_max": round(disk_util, 2),
        "disk_await_ms_max": round(disk_await, 2),
        "disk_tps_avg": round(20 + disk_util * 2.2, 1),
        "net_util_pct_max": round(net_util, 2),
        "net_rx_kbs_avg": round(net_rx, 1),
        "net_tx_kbs_avg": round(net_tx, 1),
        "net_tx_kbs_max": round(net_tx * random.uniform(1.1, 1.3), 1),
        "top_disks": [{"dev": "sda", "util_pct_avg": round(disk_util, 1), "util_pct_max": round(min(99, disk_util * 1.15), 1)}],
        "top_nics": [{"iface": "eth0", "util_pct_avg": round(net_util, 1), "util_pct_max": round(min(99, net_util * 1.15), 1)}],
    }


def apply_host_incident(row: dict, kind: str, intensity: float) -> dict:
    r = dict(row)
    if kind == "full_gc_storm":
        r["cpu_busy_pct_avg"] = round(min(98, row["cpu_busy_pct_avg"] + 30 * intensity), 2)
        r["cpu_busy_pct_max"] = round(min(99.5, row["cpu_busy_pct_max"] + 35 * intensity), 2)
        r["cpu_iowait_pct_avg"] = round(min(60, row["cpu_iowait_pct_avg"] + 12 * intensity), 2)
        r["mem_used_pct_avg"] = round(min(97, row["mem_used_pct_avg"] + 15 * intensity), 2)
        r["mem_used_pct_max"] = round(min(98, row["mem_used_pct_max"] + 18 * intensity), 2)
        r["swap_used_pct_max"] = round(max(row["swap_used_pct_max"], 2.5 * intensity), 2)
    elif kind == "heap_pressure":
        r["mem_used_pct_avg"] = round(min(95, row["mem_used_pct_avg"] + 10 * intensity), 2)
        r["mem_used_pct_max"] = round(min(97, row["mem_used_pct_max"] + 14 * intensity), 2)
    elif kind == "long_pause":
        r["disk_util_pct_max"] = round(min(97, row["disk_util_pct_max"] + 40 * intensity), 2)
        r["disk_await_ms_max"] = round(row["disk_await_ms_max"] + 35 * intensity, 2)
        r["cpu_iowait_pct_avg"] = round(min(50, row["cpu_iowait_pct_avg"] + 9 * intensity), 2)
    elif kind == "throughput_drop":
        r["cpu_busy_pct_avg"] = round(min(96, row["cpu_busy_pct_avg"] + 22 * intensity), 2)
        r["cpu_busy_pct_max"] = round(min(98, row["cpu_busy_pct_max"] + 26 * intensity), 2)
        r["net_util_pct_max"] = round(min(96, row["net_util_pct_max"] + 20 * intensity), 2)
    return r


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
        behavior = HOST_BEHAVIOR.get((iid, kind), "host_driven")
        if behavior == "gc_only":
            continue
        center = now - hours_before * HOUR
        for h in range(dur):
            ts = center + h * HOUR
            frac = (h + 0.5) / dur
            intensity = math.sin(frac * math.pi)
            incident_map.setdefault(iid, []).append((ts, kind, max(0.3, intensity)))

    rows_written = 0
    with store.connect(db_path) as c:
        for inst in instances:
            inc = {ts: (kind, inten) for ts, kind, inten in incident_map.get(inst.id, [])}
            ts = start
            while ts <= now:
                row = base_row(inst, ts)
                if ts in inc:
                    kind, inten = inc[ts]
                    row = apply_host_incident(row, kind, inten)
                store.record_host_metric(c, inst.id, ts, row)
                rows_written += 1
                ts += HOUR

    host_driven = sum(1 for iid, kind, *_ in incidents if HOST_BEHAVIOR.get((iid, kind), "host_driven") == "host_driven")
    print(f"seeded {len(instances)} instances x ~{DAYS*24} hours = {rows_written} host_metrics rows -> {db_path}")
    print(f"correlated {host_driven}/{len(incidents)} GC incidents with host pressure "
          f"(rest are pure-heap 'gc_only' stories)")
    print(f"baseline skew/pressure overrides: {INSTANCE_CPU_MULT}")


if __name__ == "__main__":
    main()
