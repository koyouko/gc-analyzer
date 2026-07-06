"""
Overlay growth *trends* onto the demo fleet's host (SAR) history so the
capacity-forecast feature has something real to project out of the box.

seed_sar_history.py gives every node a believable diurnal shape, but no
sustained day-over-day growth — so forecast.py correctly reports "stable"
everywhere. This script rebuilds the host history (via seed_sar_history) and
then applies deterministic linear growth to a few chosen nodes, creating one
story per demo cluster:

  * DEMO-ZK brokers 1-3   — uniform CPU growth (~+0.5%/day) on top of their
                            already-high baseline. All three trend toward the
                            90% critical ceiling together
                            -> cluster forecast verdict: **plan_horizontal**.
  * DEMO-KRAFT broker-3   — the existing hot/skewed node also *grows*
                            (~+0.7%/day CPU, ~+0.5%/day memory) while its
                            peers stay flat
                            -> cluster forecast verdict: **watch_hot_node**
                            (rebalance before buying hardware — consistent
                            with the reactive advisor's "rebalance" story).

Everything else keeps its flat/diurnal baseline (-> "stable"/"none"), so the
dashboard demonstrates the full verdict range instead of one alarm everywhere.

Run:  python -m seed.seed_forecast_demo    (after seed_history; re-runnable —
                                            it rebuilds host history first, so
                                            trends never double-apply)
"""

from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from gcanalyzer import store  # noqa: E402
from seed import seed_sar_history  # noqa: E402

DAY_S = 86400

# instance_id -> [(column, slope_per_day, cap)]
# Slopes are tuned against seed_sar_history's baselines (30-day window) so
# "today" sits between the warning and critical thresholds with a projected
# critical breach comfortably inside the 90-day planning horizon.
TRENDS: dict[str, list[tuple[str, float, float]]] = {
    "DEMO-ZK--broker-1": [("cpu_busy_pct", 0.50, 97.0)],
    "DEMO-ZK--broker-2": [("cpu_busy_pct", 0.48, 97.0)],
    "DEMO-ZK--broker-3": [("cpu_busy_pct", 0.52, 97.0)],
    "DEMO-KRAFT--broker-3": [
        ("cpu_busy_pct", 0.70, 98.0),
        ("mem_used_pct", 0.50, 96.0),
    ],
}


def apply_trends(db_path: str) -> int:
    """Add slope*(days since window start) to each trending column, capped."""
    updated = 0
    with store.connect(db_path) as c:
        for iid, specs in TRENDS.items():
            row = c.execute(
                "SELECT MIN(ts) AS lo, MAX(ts) AS hi FROM host_metrics WHERE instance_id=?", (iid,)
            ).fetchone()
            if row is None or row["lo"] is None:
                print(f"  !! {iid}: no host_metrics rows — run `python -m seed.seed_history` first")
                continue
            lo = int(row["lo"])
            for column, slope, cap in specs:
                # value += slope * days_elapsed, capped; deterministic and
                # single-shot because the base history was just rebuilt.
                cur = c.execute(
                    f"UPDATE host_metrics SET {column} = MIN(?, {column} + ? * ((ts - ?) / 86400.0)) "
                    f"WHERE instance_id = ?",
                    (cap, slope, lo, iid),
                )
                updated += cur.rowcount
            print(f"  ++ {iid}: " + ", ".join(f"{col} +{slope}/day (cap {cap}%)" for col, slope, cap in specs))
    return updated


def main(db_path: str | None = None) -> None:
    db_path = db_path or store.DB_PATH
    if not os.path.exists(db_path):
        raise SystemExit(f"{db_path} not found — run `python -m seed.seed_history` first.")

    # Rebuild the flat/diurnal host baseline first so re-running this script
    # never stacks trend on top of trend.
    print("rebuilding host (SAR) baseline via seed_sar_history…")
    with store.connect(db_path) as c:
        c.execute("DELETE FROM host_metrics")
        c.execute("DELETE FROM sar_collector_state")
    seed_sar_history.main(db_path)

    print("applying capacity-growth trends:")
    updated = apply_trends(db_path)
    print(f"updated {updated} column-rows -> {db_path}")
    print()
    print("expected dashboard stories:")
    print("  DEMO-ZK    cluster forecast : plan_horizontal (uniform CPU growth on brokers 1-3)")
    print("  DEMO-KRAFT cluster forecast : watch_hot_node  (broker-3 CPU+memory trending up, peers flat)")
    print("  everything else             : stable / no scaling needed in horizon")


if __name__ == "__main__":
    main()
