"use client";

import dynamic from "next/dynamic";
import { SarSnapshot, SarSeries } from "@/lib/types";

const SarTrendCharts = dynamic(() => import("./SarTrendCharts"), {
  ssr: false,
  loading: () => <div className="muted" style={{ padding: 12 }}>Loading host charts…</div>,
});

/** "Server health (SAR)" section on the instance page — the OS-level
 * counterpart to the GC metrics above it: CPU/iowait, memory/swap, disk and
 * NIC pressure for the host this JVM runs on. */
export default function HostMetricsPanel({ sar, sarSeries }: { sar: SarSnapshot | null; sarSeries: SarSeries | null }) {
  if (!sar || !sar.health) {
    return (
      <>
        <h2 className="sec">Server health (SAR)</h2>
        <div className="panel full">
          <div className="muted">
            No host metrics collected for this instance yet. Enable <code>sar:</code> in its cluster.yaml node
            entry (requires <code>sysstat</code> installed + sa1/sa2 cron on the host), or wait for the next
            scheduler tick.
          </div>
        </div>
      </>
    );
  }

  const m = sar.metrics as Required<SarSnapshot>["metrics"] & {
    cpu_busy_pct_avg: number; cpu_busy_pct_max: number; cpu_iowait_pct_avg: number; cpu_iowait_pct_max: number;
    load1_avg: number; mem_used_pct_avg: number; mem_used_pct_max: number; swap_used_pct_max: number;
    disk_util_pct_max: number; disk_await_ms_max: number; net_util_pct_max: number; net_tx_kbs_avg: number;
    top_disks: any[]; top_nics: any[];
  };
  const h = sar.health;
  const findings = sar.findings || { pros: [], cons: [], recommendations: [] };

  const mc: [string, React.ReactNode][] = [
    ["CPU busy (avg/max)", `${m.cpu_busy_pct_avg}% / ${m.cpu_busy_pct_max}%`],
    ["iowait (avg/max)", `${m.cpu_iowait_pct_avg}% / ${m.cpu_iowait_pct_max}%`],
    ["Load avg (1m)", m.load1_avg],
    ["Memory used (avg/max)", `${m.mem_used_pct_avg}% / ${m.mem_used_pct_max}%`],
    ["Swap used (max)", `${m.swap_used_pct_max}%`],
    ["Disk util (max)", `${m.disk_util_pct_max}%`],
    ["Disk await (max)", `${m.disk_await_ms_max} ms`],
    ["NIC util (max)", `${m.net_util_pct_max}%`],
    ["Net egress (avg)", `${(m.net_tx_kbs_avg / 1024).toFixed(1)} MB/s`],
  ];

  return (
    <>
      <h2 className="sec">Server health (SAR) — 24h window</h2>
      <div className="ihdr">
        <div className={"grade " + (h.grade || "F")}>{h.grade}</div>
        <div>
          <div style={{ fontSize: 15, fontWeight: 700 }}>Host resource pressure</div>
          <div className="muted">{h.score} / 100 · {h.status}</div>
        </div>
      </div>
      <div className="muted" style={{ marginBottom: 8 }}>{h.reasons.join(" · ") || "No host resource pressure detected."}</div>

      <div className="metric-grid">
        {mc.map(([l, v], i) => (
          <div key={i} className="metric"><div className="l">{l}</div><div className="v">{v}</div></div>
        ))}
      </div>

      {sarSeries && sarSeries.series.length ? <SarTrendCharts sar={sarSeries} /> : null}

      {(m.top_disks?.length || m.top_nics?.length) ? (
        <div className="panels" style={{ marginTop: 12 }}>
          {m.top_disks?.length ? (
            <div className="panel">
              <h3>Busiest disks</h3>
              <table className="sar-table">
                <tbody>
                  <tr><th>Device</th><th>avg util</th><th>max util</th></tr>
                  {m.top_disks.map((d: any, i: number) => (
                    <tr key={i}><td>{d.dev}</td><td>{d.util_pct_avg}%</td><td>{d.util_pct_max}%</td></tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : null}
          {m.top_nics?.length ? (
            <div className="panel">
              <h3>Busiest NICs</h3>
              <table className="sar-table">
                <tbody>
                  <tr><th>Interface</th><th>avg util</th><th>max util</th></tr>
                  {m.top_nics.map((n: any, i: number) => (
                    <tr key={i}><td>{n.iface}</td><td>{n.util_pct_avg}%</td><td>{n.util_pct_max}%</td></tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : null}
        </div>
      ) : null}

      <div className="panels" style={{ marginTop: 12 }}>
        <div className="panel"><h3 style={{ color: "#7ee787" }}>Pros</h3>
          <ul className="list pros">{findings.pros.length ? findings.pros.map((x, i) => <li key={i}>{x}</li>) : <li className="muted">—</li>}</ul>
        </div>
        <div className="panel"><h3 style={{ color: "#ffa198" }}>Cons</h3>
          <ul className="list cons">{findings.cons.length ? findings.cons.map((x, i) => <li key={i}>{x}</li>) : <li className="muted">None.</li>}</ul>
        </div>
        <div className="panel full"><h3 style={{ color: "var(--accent)" }}>How to improve</h3>
          <ul className="list recs">{findings.recommendations.map((x, i) => <li key={i}>{x}</li>)}</ul>
        </div>
      </div>
    </>
  );
}
