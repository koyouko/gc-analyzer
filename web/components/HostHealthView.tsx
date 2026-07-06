"use client";

import { useState } from "react";
import Link from "next/link";
import dynamic from "next/dynamic";
import { useApi } from "@/lib/api";
import { useFleet } from "@/lib/fleetContext";
import { SarSnapshot, SarSeries, SarMetrics, InstanceForecast } from "@/lib/types";

const HostFullCharts = dynamic(() => import("./HostFullCharts"), {
  ssr: false,
  loading: () => <div className="muted" style={{ padding: 12 }}>Loading host charts…</div>,
});

const RANGES = ["1h", "3h", "6h", "12h", "24h", "2d", "7d", "30d", "90d", "1y", "2y"];

const METRIC_HELP: Record<string, string> = {
  "CPU busy": "Total non-idle CPU (user+system+iowait+steal). Sustained >75% leaves little headroom for GC threads and replica fetchers.",
  "CPU user / system": "Time in application code vs kernel. A high system share often means heavy network/disk syscall load.",
  "iowait": "CPU idle while waiting on disk I/O. The classic sign the storage layer, not the CPU, is the bottleneck.",
  "CPU steal": "Time the hypervisor gave this vCPU to someone else. Sustained steal on a VM means a noisy neighbor or oversold host.",
  "Load avg (1/5/15m)": "Runnable + uninterruptible tasks. Compare against CPU count; a rising 15m load is sustained saturation.",
  "Run queue / blocked": "Tasks waiting for CPU vs blocked on I/O — the raw ingredients of load average.",
  "Context switches": "cswch/s. Very high rates can indicate thread thrash — relevant for Kafka's network/request thread pools.",
  "New processes": "proc/s — process creation rate. Usually near zero on a dedicated broker; spikes mean cron/automation activity.",
  "Memory used": "Physical memory in use (including page cache on older sysstat). RHEL sysstat reports %memused against total RAM.",
  "Memory available": "Kernel's estimate of memory available for new workloads without swapping — the truest headroom number.",
  "Page cache": "kbcached — Kafka serves consumers from here. A shrinking cache under memory pressure directly hurts fetch latency.",
  "Commit %": "Committed virtual memory vs RAM+swap. >100% means overcommit; watch alongside swap activity.",
  "Swap used": "Swap space occupancy. Any nonzero value on a Kafka broker deserves a look at vm.swappiness.",
  "Paging in / out": "pgpgin/s / pgpgout/s — kB moved between disk and memory. Egress writes flow through here via the page cache.",
  "Faults / major": "Page faults per second. Major faults require a disk read — sustained majflt/s means the working set doesn't fit.",
  "Swap in / out": "pswpin/s / pswpout/s — active swapping right now. Swap-out under load is an emergency-grade memory signal.",
  "Disk util (max)": "Busiest device's %util. >70% sustained means the device is approaching saturation.",
  "Disk await (max)": "Average I/O service time incl. queueing on the busiest device. Latency spikes here become produce latency.",
  "Disk tps": "I/O requests per second across devices.",
  "NIC util (max)": "Busiest interface's %ifutil vs link speed. Kafka replication + consumer fan-out saturates NICs before CPU.",
  "Net ingress / egress": "rxkB/s / txkB/s across NICs. Egress (produce fan-out + consumer fetch) is usually the binding side.",
};

function Metric({ label, value }: { label: string; value: React.ReactNode }) {
  const help = METRIC_HELP[label] || "";
  return (
    <div className="metric" title={help}>
      <div className="l">{label}{help ? <span className="info">ⓘ</span> : null}</div>
      <div className="v">{value}</div>
      {help ? <span className="tip">{help}</span> : null}
    </div>
  );
}

const mbps = (kbs?: number) => `${((kbs ?? 0) / 1024).toFixed(1)} MB/s`;
const gb = (mb?: number) => `${((mb ?? 0) / 1024).toFixed(1)} GB`;

/** Standalone "Host health analysis" page: every SAR metric the analyzer
 * collects from the RHEL host, judged on its own — no GC data on this page.
 * The instance page keeps its compact Server-health summary; this is the
 * full drill-down for "is the *box* healthy?". */
export default function HostHealthView({ id }: { id: string }) {
  const { tick } = useFleet();
  const [range, setRange] = useState("24h");
  const { data: sar, error } = useApi<SarSnapshot>(`/api/instance/${id}/sar`, tick);
  const { data: series } = useApi<SarSeries>(`/api/instance/${id}/sar/series?range=${range}`, tick);
  const { data: fc } = useApi<InstanceForecast>(`/api/instance/${id}/forecast`, tick);

  if (error) return <div className="empty">Failed to load host view for {id}: {error}</div>;
  if (!sar) return <div className="empty">Loading host health for {id}…</div>;

  const h = sar.health;
  if (!h || !sar.metrics || !("sample_count" in sar.metrics)) {
    return (
      <>
        <div className="breadcrumb">
          <Link href={`/instance/${encodeURIComponent(id)}`}>{id}</Link> › host
        </div>
        <div className="empty">
          No host (SAR) metrics collected for this instance yet — enable <code>sar:</code> in its
          cluster.yaml, wait for a scheduler tick, or upload a <code>sadf -j -- -A</code> export from
          the instance page.
        </div>
      </>
    );
  }

  const m = sar.metrics as SarMetrics;
  const inst = sar.instance;
  const grade = h.grade || "F";
  const hostSignals = fc ? fc.signals.filter((s) => s.kind === "host") : [];
  const trending = hostSignals.filter((s) => s.risk !== "ok");

  return (
    <>
      <div className="breadcrumb">
        {inst.region} › {inst.env} › {inst.cluster} ›{" "}
        <Link href={`/instance/${encodeURIComponent(id)}`}>{inst.role} {id}</Link> › host
      </div>
      <div className="ihdr">
        <div className={"grade " + grade}>{grade}</div>
        <div>
          <div style={{ fontSize: 18, fontWeight: 700 }}>Host health analysis — {id}</div>
          <div className="muted">
            {h.score} / 100 · {h.status} · OS-level view only (no GC data on this page) ·{" "}
            {m.sample_count} sample rows in 24h window
          </div>
        </div>
        <span className="engine">
          <Link href={`/instance/${encodeURIComponent(id)}`}>← back to JVM view</Link>
        </span>
      </div>
      <div className="muted" style={{ marginBottom: 8 }}>
        {h.reasons.join(" · ") || "No host resource pressure detected."}
      </div>

      {trending.length ? (
        <div className="alerts" style={{ margin: "6px 0 4px" }}>
          {trending.map((s, i) => (
            <div key={i} className={"alert " + (s.risk === "critical" ? "critical" : "warning")}>
              <span className={"sev " + (s.risk === "critical" ? "critical" : "warning")}>trend</span>
              <span className="msg">
                {s.label} at {s.current}{s.unit}
                {s.days_to_crit != null
                  ? ` — projected to hit ${s.crit_threshold}${s.unit} in ~${Math.round(s.days_to_crit)}d (${s.crit_date})`
                  : ` — past ${s.status.includes("critical") ? "critical" : "warning"} threshold`}
              </span>
            </div>
          ))}
        </div>
      ) : null}

      <h2 className="sec">CPU &amp; scheduler (24h)</h2>
      <div className="metric-grid">
        <Metric label="CPU busy" value={`${m.cpu_busy_pct_avg}% / ${m.cpu_busy_pct_max}%`} />
        <Metric label="CPU user / system" value={`${m.cpu_user_pct_avg ?? 0}% / ${m.cpu_system_pct_avg ?? 0}%`} />
        <Metric label="iowait" value={`${m.cpu_iowait_pct_avg}% / ${m.cpu_iowait_pct_max}%`} />
        <Metric label="CPU steal" value={`${m.cpu_steal_pct_avg ?? 0}% / ${m.cpu_steal_pct_max ?? 0}%`} />
        <Metric label="Load avg (1/5/15m)" value={`${m.load1_avg} / ${m.load5_avg ?? 0} / ${m.load15_avg ?? 0}`} />
        <Metric label="Run queue / blocked" value={`${m.runq_sz_avg ?? 0} / ${m.blocked_avg ?? 0}`} />
        <Metric label="Context switches" value={`${(m.cswch_per_s_avg ?? 0).toLocaleString()}/s`} />
        <Metric label="New processes" value={`${m.proc_per_s_avg ?? 0}/s`} />
      </div>

      <h2 className="sec">Memory, paging &amp; swap (24h)</h2>
      <div className="metric-grid">
        <Metric label="Memory used" value={`${m.mem_used_pct_avg}% / ${m.mem_used_pct_max}%`} />
        <Metric label="Memory available" value={gb(m.mem_avail_mb_avg)} />
        <Metric label="Page cache" value={gb(m.mem_cached_mb_avg)} />
        <Metric label="Commit %" value={`${m.mem_commit_pct_avg ?? 0}%`} />
        <Metric label="Swap used" value={`${m.swap_used_pct_max}% peak`} />
        <Metric label="Paging in / out" value={`${(m.pgpgin_kbs_avg ?? 0).toFixed(0)} / ${(m.pgpgout_kbs_avg ?? 0).toFixed(0)} kB/s`} />
        <Metric label="Faults / major" value={`${(m.fault_per_s_avg ?? 0).toFixed(0)} / ${(m.majflt_per_s_avg ?? 0).toFixed(1)}/s`} />
        <Metric label="Swap in / out" value={`${m.pswpin_per_s_avg ?? 0} / ${m.pswpout_per_s_avg ?? 0} pg/s`} />
      </div>

      <h2 className="sec">Disk &amp; network (24h)</h2>
      <div className="metric-grid">
        <Metric label="Disk util (max)" value={`${m.disk_util_pct_max}%`} />
        <Metric label="Disk await (max)" value={`${m.disk_await_ms_max} ms`} />
        <Metric label="Disk tps" value={`${m.disk_tps_avg ?? 0}/s`} />
        <Metric label="NIC util (max)" value={`${m.net_util_pct_max}%`} />
        <Metric label="Net ingress / egress" value={`${mbps(m.net_rx_kbs_avg)} / ${mbps(m.net_tx_kbs_avg)}`} />
      </div>

      <h2 className="sec" style={{ display: "flex", alignItems: "center", gap: 12 }}>
        All SAR trends
        <select className="inp" style={{ width: "auto", margin: 0, textTransform: "none", letterSpacing: 0 }}
                value={range} onChange={(e) => setRange(e.target.value)}>
          {RANGES.map((r) => <option key={r} value={r}>{r}</option>)}
        </select>
      </h2>
      {series && series.series.length
        ? <HostFullCharts sar={series} />
        : <div className="muted" style={{ padding: 12 }}>No samples in this range.</div>}

      {(m.top_disks?.length || m.top_nics?.length) ? (
        <div className="panels" style={{ marginTop: 12 }}>
          {m.top_disks?.length ? (
            <div className="panel">
              <h3>Busiest disks</h3>
              <table className="sar-table"><tbody>
                <tr><th>Device</th><th>avg util</th><th>max util</th></tr>
                {m.top_disks.map((d, i) => (
                  <tr key={i}><td>{d.dev}</td><td>{d.util_pct_avg}%</td><td>{d.util_pct_max}%</td></tr>
                ))}
              </tbody></table>
            </div>
          ) : null}
          {m.top_nics?.length ? (
            <div className="panel">
              <h3>Busiest NICs</h3>
              <table className="sar-table"><tbody>
                <tr><th>Interface</th><th>avg util</th><th>max util</th></tr>
                {m.top_nics.map((n, i) => (
                  <tr key={i}><td>{n.iface}</td><td>{n.util_pct_avg}%</td><td>{n.util_pct_max}%</td></tr>
                ))}
              </tbody></table>
            </div>
          ) : null}
        </div>
      ) : null}

      {sar.findings ? (
        <div className="panels" style={{ marginTop: 12 }}>
          <div className="panel"><h3 style={{ color: "#7ee787" }}>Pros</h3>
            <ul className="list pros">{sar.findings.pros.length ? sar.findings.pros.map((x, i) => <li key={i}>{x}</li>) : <li className="muted">—</li>}</ul>
          </div>
          <div className="panel"><h3 style={{ color: "#ffa198" }}>Cons</h3>
            <ul className="list cons">{sar.findings.cons.length ? sar.findings.cons.map((x, i) => <li key={i}>{x}</li>) : <li className="muted">None.</li>}</ul>
          </div>
          <div className="panel full"><h3 style={{ color: "var(--accent)" }}>How to improve</h3>
            <ul className="list recs">{sar.findings.recommendations.map((x, i) => <li key={i}>{x}</li>)}</ul>
          </div>
        </div>
      ) : null}
    </>
  );
}
