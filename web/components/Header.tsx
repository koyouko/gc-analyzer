"use client";

import Link from "next/link";
import { useFleet } from "@/lib/fleetContext";
import { PILL_COLOR } from "@/lib/api";

export default function Header() {
  const { fleet, refresh, loading } = useFleet();
  const status = fleet?.fleet_status ?? "unknown";
  return (
    <header className="topbar">
      <div>
        {/* Clicking the title returns to the main fleet overview. */}
        <Link href="/" aria-label="Go to fleet overview">
          <h1>BSP Kafka GC Analyzer</h1>
        </Link>
        <div className="sub">
          {fleet ? `${fleet.total_instances} components · ${fleet.regions.length} regions` : "loading…"}
        </div>
      </div>
      <div className="right">
        <span className="statuspill" style={{ background: PILL_COLOR[status] }}>
          {fleet ? status : "—"}
        </span>
        <button className="btn" onClick={refresh} disabled={loading}>
          {loading ? "Loading…" : "Refresh"}
        </button>
      </div>
    </header>
  );
}
