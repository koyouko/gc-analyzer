"use client";

import { useRouter } from "next/navigation";
import { useFleet } from "@/lib/fleetContext";
import { fmtTime } from "@/lib/api";

export default function FleetOverview() {
  const { fleet, error } = useFleet();
  const router = useRouter();

  if (error) return <div className="empty">Failed to load: {error}</div>;
  if (!fleet) return <div className="empty">Loading fleet…</div>;

  const c = fleet.counts;
  return (
    <>
      <div className="breadcrumb">
        Fleet overview · {fleet.total_instances} JVM components · snapshot {fmtTime(fleet.now)}
      </div>
      <div className="cards">
        <div className="card ok"><div className="l">Healthy</div><div className="v">{c.ok || 0}</div></div>
        <div className="card watch"><div className="l">Watch</div><div className="v">{c.watch || 0}</div></div>
        <div className="card crit"><div className="l">Critical</div><div className="v">{c.critical || 0}</div></div>
        <div className="card"><div className="l">Total components</div><div className="v">{fleet.total_instances}</div></div>
        <div className="card"><div className="l">Active alerts (1h)</div><div className="v">{fleet.active_alerts.length}</div></div>
      </div>

      <h2 className="sec">Issues in the last hour</h2>
      <div className="legend">
        <span><span className="dot ok" /> healthy</span>
        <span><span className="dot watch" /> watch</span>
        <span><span className="dot crit" /> critical</span>
        <span>🔴 Full GC in last hour</span>
      </div>
      <div className="alerts">
        {fleet.active_alerts.length ? (
          fleet.active_alerts.map((a, i) => (
            <div
              key={i}
              className={"alert " + a.severity}
              onClick={() => a.instance_id && router.push(`/instance/${a.instance_id}`)}
            >
              <span className={"sev " + a.severity}>{a.severity}</span>
              <span className="where">{a.instance_id}</span>
              <span className="msg">{a.msg}</span>
            </div>
          ))
        ) : (
          <div className="muted">No GC issues detected across the fleet in the last hour.</div>
        )}
      </div>

      <h2 className="sec" style={{ marginTop: 26 }}>Tip</h2>
      <div className="muted">
        Pick any component in the left tree (or click an alert) to see its current health and
        30-day JVM heap / GC trends. Click a cluster name to open its overview.
      </div>
    </>
  );
}
