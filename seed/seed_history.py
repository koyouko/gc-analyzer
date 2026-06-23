"""
Seed the SQLite store with 30 days of hourly GC metrics for the whole fleet,
including a handful of injected incidents so the dashboard's "right now" and
"last hour" alerting (and the 30-day trends) have something real to show.

Run:  python -m seed.seed_history          (writes ./gc_history.db)

In production you would not run this — a periodic collection job would call
store.record_metric() with real parsed numbers instead.
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

random.seed(7)

DAYS = 30
HOUR = 3600

# Per-role baseline behaviour (fractions of heap, pause ranges, etc.).
ROLE_PROFILE = {
    "broker":          {"live": (0.28, 0.46), "p99": (55, 130), "freq": (6, 16),  "tig": (0.4, 1.6)},
    "schema-registry": {"live": (0.10, 0.22), "p99": (8, 25),   "freq": (1, 4),   "tig": (0.05, 0.3)},
    "connect":         {"live": (0.22, 0.40), "p99": (25, 80),  "freq": (3, 9),   "tig": (0.2, 1.0)},
    "controller":      {"live": (0.08, 0.20), "p99": (6, 22),   "freq": (1, 3),   "tig": (0.05, 0.25)},
    "zookeeper":       {"live": (0.10, 0.24), "p99": (5, 20),   "freq": (1, 4),   "tig": (0.05, 0.3)},
}


# Incidents: (instance_id, kind, hours_before_now_start, duration_hours)
# kinds: full_gc_storm, heap_pressure, long_pause, throughput_drop, historical
def build_incidents():
    return [
        # --- active in the last hour (red / amber "right now") ---
        ("DEMO-KRAFT--broker-2", "full_gc_storm", 2, 2),
        ("DEMO-KRAFT--connect-1", "long_pause", 1, 1),
        ("DEMO-ZK--broker-1", "heap_pressure", 6, 6),
        ("DEMO-ZK--broker-2", "throughput_drop", 1, 2),
        # --- recent history (trends only, not in last-hour alerts) ---
        ("DEMO-ZK--broker-3", "full_gc_storm", 26, 3),
        ("DEMO-KRAFT--schema-registry-1", "long_pause", 50, 2),
        # --- mid-history (30-day trend context) ---
        ("DEMO-KRAFT--broker-1", "heap_pressure", 12 * 24, 18),
        ("DEMO-ZK--connect-1", "full_gc_storm", 20 * 24, 4),
    ]


def diurnal(hour_utc: int, busy: int) -> float:
    """0..1 load factor peaking at the region's busy hour."""
    delta = min((hour_utc - busy) % 24, (busy - hour_utc) % 24)
    return 0.55 + 0.45 * math.cos(delta / 12 * math.pi)


def base_row(inst, ts: int) -> dict:
    p = ROLE_PROFILE[inst.role]
    heap = inst.heap_max_mb
    hour = (ts // HOUR) % 24
    load = diurnal(hour, inst.busy_hour_utc)
    prod_boost = 1.12

    live_lo, live_hi = p["live"]
    live_pct = (live_lo + (live_hi - live_lo) * load) * prod_boost
    live_pct = min(0.7, live_pct) * 100 + random.uniform(-3, 3)
    live_pct = max(5.0, live_pct)

    p99 = random.uniform(*p["p99"]) * (0.7 + 0.6 * load) * prod_boost
    pmax = p99 * random.uniform(1.4, 2.2)
    pavg = p99 * random.uniform(0.35, 0.55)
    freq = random.uniform(*p["freq"]) * (0.6 + 0.8 * load) * prod_boost
    tig = random.uniform(*p["tig"]) * (0.6 + 0.9 * load) * prod_boost
    throughput = max(95.0, 100 - tig)

    return {
        "heap_used_mb": round(heap * live_pct / 100, 1),
        "heap_max_mb": heap,
        "heap_after_pct": round(live_pct, 1),
        "pause_avg_ms": round(pavg, 1),
        "pause_p99_ms": round(p99, 1),
        "pause_max_ms": round(pmax, 1),
        "full_gc_count": 0,
        "young_count": int(freq * 60),
        "gc_per_min": round(freq, 1),
        "time_in_gc_pct": round(tig, 3),
        "throughput_pct": round(throughput, 3),
    }


def apply_incident(row: dict, inst, kind: str, intensity: float) -> dict:
    heap = inst.heap_max_mb
    r = dict(row)
    if kind == "full_gc_storm":
        r["full_gc_count"] = max(1, int(3 + 5 * intensity))
        r["pause_max_ms"] = round(800 + 700 * intensity * random.uniform(0.8, 1.2), 1)
        r["pause_p99_ms"] = round(400 + 300 * intensity, 1)
        r["time_in_gc_pct"] = round(15 + 30 * intensity, 2)
        r["throughput_pct"] = round(100 - r["time_in_gc_pct"], 2)
        r["heap_after_pct"] = round(min(95, row["heap_after_pct"] + 25 * intensity), 1)
        r["gc_per_min"] = round(row["gc_per_min"] * (1.5 + intensity), 1)
    elif kind == "heap_pressure":
        r["heap_after_pct"] = round(min(94, 84 + 8 * intensity + random.uniform(-1, 2)), 1)
        r["pause_p99_ms"] = round(row["pause_p99_ms"] * (1.3 + intensity), 1)
        r["pause_max_ms"] = round(row["pause_max_ms"] * (1.4 + intensity), 1)
        r["time_in_gc_pct"] = round(row["time_in_gc_pct"] * (2 + intensity), 2)
        if intensity > 0.8:
            r["full_gc_count"] = 1
    elif kind == "long_pause":
        r["pause_max_ms"] = round(550 + 400 * intensity * random.uniform(0.8, 1.3), 1)
        r["pause_p99_ms"] = round(max(row["pause_p99_ms"], 350 + 150 * intensity), 1)
        r["time_in_gc_pct"] = round(row["time_in_gc_pct"] + 3 * intensity, 2)
    elif kind == "throughput_drop":
        r["time_in_gc_pct"] = round(6 + 8 * intensity, 2)
        r["throughput_pct"] = round(100 - r["time_in_gc_pct"], 2)
        r["gc_per_min"] = round(row["gc_per_min"] * (2 + intensity), 1)
        r["pause_p99_ms"] = round(row["pause_p99_ms"] * (1.5 + intensity), 1)
    r["heap_used_mb"] = round(heap * r["heap_after_pct"] / 100, 1)
    return r


def main(db_path: str = None):
    db_path = db_path or store.DB_PATH
    if os.path.exists(db_path):
        os.remove(db_path)
    store.init_db(db_path)

    instances = topology.build_instances()
    by_id = {i.id: i for i in instances}
    incidents = build_incidents()

    # Anchor "now" to the top of the current hour.
    now = (int(time.time()) // HOUR) * HOUR
    start = now - DAYS * 24 * HOUR

    # Precompute incident hour-windows -> {instance_id: [(ts, kind, intensity)]}
    incident_map: dict[str, list] = {}
    for iid, kind, hours_before, dur in incidents:
        center = now - hours_before * HOUR
        for h in range(dur):
            ts = center + h * HOUR
            # intensity ramps then fades across the window
            frac = (h + 0.5) / dur
            intensity = math.sin(frac * math.pi)  # 0..1..0
            incident_map.setdefault(iid, []).append((ts, kind, max(0.3, intensity)))

    rows_written = 0
    with store.connect(db_path) as c:
        for inst in instances:
            store.upsert_instance(c, inst, collector="G1")

        for inst in instances:
            inc = {ts: (kind, inten) for ts, kind, inten in incident_map.get(inst.id, [])}
            ts = start
            while ts <= now:
                row = base_row(inst, ts)
                if ts in inc:
                    kind, inten = inc[ts]
                    row = apply_incident(row, inst, kind, inten)
                store.record_metric(c, inst.id, ts, row)
                rows_written += 1
                ts += HOUR

    print(f"seeded {len(instances)} instances x ~{DAYS*24} hours = {rows_written} rows -> {db_path}")
    print(f"now anchored at epoch {now} ({time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime(now))})")
    print(f"injected {len(incidents)} incidents; "
          f"{sum(1 for i in incidents if i[2] <= 2)} active within the last hour")


if __name__ == "__main__":
    main()
