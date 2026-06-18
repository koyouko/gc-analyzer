"""
Generate realistic Java 11+ unified (-Xlog:gc*) G1GC sample logs for several
Kafka node profiles, so the analyzer and dashboard have meaningful data offline.

Profiles:
  broker-1     : healthy broker  (good throughput, no Full GC)
  broker-2     : pressured broker (frequent young GCs, a hotspot burst, Full GCs)
  broker-3     : leaky broker     (live set climbs steadily -> promotion pressure)
  controller-1 : KRaft controller (light, very healthy)
  zookeeper-1  : ZooKeeper        (small heap, light load)

The line format matches OpenJDK unified logging, e.g.:
  [2026-06-08T10:15:30.123+0000][info][gc] GC(42) Pause Young (Normal) (G1 Evacuation Pause) 1536M->402M(4096M) 18.421ms
"""

from __future__ import annotations

import os
import random
from datetime import datetime, timedelta, timezone

random.seed(42)
HERE = os.path.dirname(os.path.abspath(__file__))


def fmt_ts(dt: datetime) -> str:
    s = dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}"
    return s + "+0000"


def header(heap_mb: int, region_mb: int) -> list[str]:
    t = datetime(2026, 6, 8, 10, 0, 0, tzinfo=timezone.utc)
    return [
        f"[{fmt_ts(t)}][info][gc] Using G1",
        f"[{fmt_ts(t)}][info][gc,init] Version: 17.0.10+7 (release)",
        f"[{fmt_ts(t)}][info][gc,init] CPUs: 8 total, 8 available",
        f"[{fmt_ts(t)}][info][gc,init] Heap Region Size: {region_mb}M",
        f"[{fmt_ts(t)}][info][gc,init] Heap Max Capacity: {heap_mb}M",
        f"[{fmt_ts(t)}][info][gc,init] Concurrent Workers: 2",
    ]


def young_line(t, seq, before, after, total, pause, cause="G1 Evacuation Pause", kind="Normal"):
    return (f"[{fmt_ts(t)}][info][gc] GC({seq}) Pause Young ({kind}) ({cause}) "
            f"{before}M->{after}M({total}M) {pause:.3f}ms")


def mixed_line(t, seq, before, after, total, pause):
    return (f"[{fmt_ts(t)}][info][gc] GC({seq}) Pause Young (Mixed) (G1 Evacuation Pause) "
            f"{before}M->{after}M({total}M) {pause:.3f}ms")


def full_line(t, seq, before, after, total, pause, cause="G1 Compaction Pause"):
    return (f"[{fmt_ts(t)}][info][gc] GC({seq}) Pause Full ({cause}) "
            f"{before}M->{after}M({total}M) {pause:.3f}ms")


def conc_lines(t, seq):
    return [
        f"[{fmt_ts(t)}][info][gc] GC({seq}) Concurrent Cycle",
        f"[{fmt_ts(t + timedelta(milliseconds=80))}][info][gc] GC({seq}) Pause Remark "
        f"512M->512M(4096M) 2.140ms",
        f"[{fmt_ts(t + timedelta(milliseconds=140))}][info][gc] GC({seq}) Pause Cleanup "
        f"512M->500M(4096M) 0.890ms",
        f"[{fmt_ts(t + timedelta(milliseconds=160))}][info][gc] GC({seq}) Concurrent Cycle 81.2ms",
    ]


def gen_healthy(heap=4096, region=8, minutes=30):
    lines = header(heap, region)
    t = datetime(2026, 6, 8, 10, 0, 5, tzinfo=timezone.utc)
    seq = 0
    baseline = int(heap * 0.10)          # ~10% live set, lots of headroom
    while t < datetime(2026, 6, 8, 10, minutes, tzinfo=timezone.utc):
        seq += 1
        before = baseline + random.randint(int(heap * 0.30), int(heap * 0.45))
        after = baseline + random.randint(-30, 60)
        after = max(baseline - 50, after)
        pause = random.uniform(8, 35)
        lines.append(young_line(t, seq, before, after, heap, pause))
        if seq % 25 == 0:
            seq += 1
            lines.extend(conc_lines(t + timedelta(milliseconds=200), seq))
        t += timedelta(seconds=random.uniform(4, 9))
    return "\n".join(lines) + "\n"


def gen_pressured(heap=4096, region=8, minutes=30):
    lines = header(heap, region)
    t = datetime(2026, 6, 8, 10, 0, 5, tzinfo=timezone.utc)
    seq = 0
    baseline = int(heap * 0.35)
    hotspot_start = datetime(2026, 6, 8, 10, 12, tzinfo=timezone.utc)
    hotspot_end = datetime(2026, 6, 8, 10, 16, tzinfo=timezone.utc)
    while t < datetime(2026, 6, 8, 10, minutes, tzinfo=timezone.utc):
        in_hotspot = hotspot_start <= t < hotspot_end
        seq += 1
        if in_hotspot:
            before = baseline + random.randint(int(heap * 0.45), int(heap * 0.58))
            after = baseline + random.randint(60, 220)
            pause = random.uniform(120, 480)
        else:
            before = baseline + random.randint(int(heap * 0.30), int(heap * 0.45))
            after = baseline + random.randint(-20, 120)
            pause = random.uniform(20, 90)
        after = min(after, heap - 50)
        lines.append(young_line(t, seq, before, after, heap, pause))
        # Humongous-allocation driven Full GCs during the hotspot.
        if in_hotspot and random.random() < 0.22:
            seq += 1
            fbefore = heap - random.randint(40, 120)
            fafter = baseline + random.randint(40, 120)
            lines.append(full_line(t + timedelta(milliseconds=300), seq, fbefore, fafter, heap,
                                   random.uniform(700, 1400),
                                   cause="G1 Humongous Allocation"))
        if seq % 18 == 0:
            seq += 1
            lines.extend(conc_lines(t + timedelta(milliseconds=200), seq))
        t += timedelta(seconds=random.uniform(2.0, 5.0) if not in_hotspot else random.uniform(0.8, 2.0))
    return "\n".join(lines) + "\n"


def gen_leaky(heap=4096, region=8, minutes=30):
    lines = header(heap, region)
    t = datetime(2026, 6, 8, 10, 0, 5, tzinfo=timezone.utc)
    seq = 0
    live = int(heap * 0.20)
    end = datetime(2026, 6, 8, 10, minutes, tzinfo=timezone.utc)
    total_secs = (end - t).total_seconds()
    while t < end:
        seq += 1
        frac = (t - datetime(2026, 6, 8, 10, 0, 5, tzinfo=timezone.utc)).total_seconds() / total_secs
        live = int(heap * (0.20 + 0.55 * frac))   # steady climb 20% -> 75%
        before = min(heap - 30, live + random.randint(int(heap * 0.20), int(heap * 0.30)))
        after = min(heap - 40, live + random.randint(-15, 25))
        pause = random.uniform(15, 70) + frac * 120   # pauses grow as heap fills
        lines.append(young_line(t, seq, before, after, heap, pause))
        if frac > 0.85 and random.random() < 0.15:
            seq += 1
            lines.append(full_line(t + timedelta(milliseconds=250), seq,
                                   heap - 30, int(heap * 0.72), heap,
                                   random.uniform(900, 1500),
                                   cause="Allocation Failure"))
        if seq % 22 == 0:
            seq += 1
            lines.extend(conc_lines(t + timedelta(milliseconds=200), seq))
        t += timedelta(seconds=random.uniform(3, 7))
    return "\n".join(lines) + "\n"


def gen_light(heap=1024, region=4, minutes=30, period=(12, 25)):
    lines = header(heap, region)
    t = datetime(2026, 6, 8, 10, 0, 5, tzinfo=timezone.utc)
    seq = 0
    baseline = int(heap * 0.12)
    while t < datetime(2026, 6, 8, 10, minutes, tzinfo=timezone.utc):
        seq += 1
        before = baseline + random.randint(int(heap * 0.25), int(heap * 0.40))
        after = baseline + random.randint(-10, 30)
        after = max(baseline - 20, after)
        pause = random.uniform(3, 18)
        lines.append(young_line(t, seq, before, after, heap, pause))
        t += timedelta(seconds=random.uniform(*period))
    return "\n".join(lines) + "\n"


def main():
    files = {
        "broker-1-gc.log": gen_healthy(heap=4096, region=8),
        "broker-2-gc.log": gen_pressured(heap=4096, region=8),
        "broker-3-gc.log": gen_leaky(heap=4096, region=8),
        "controller-1-gc.log": gen_light(heap=1024, region=4, period=(15, 30)),
        "zookeeper-1-gc.log": gen_light(heap=1024, region=4, period=(10, 22)),
    }
    for name, content in files.items():
        path = os.path.join(HERE, name)
        with open(path, "w") as fh:
            fh.write(content)
        print(f"wrote {name}: {content.count(chr(10))} lines")


if __name__ == "__main__":
    main()
