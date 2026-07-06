"use client";

import { useRouter } from "next/navigation";
import { useApi, PILL_COLOR, fmtTime, shortId } from "@/lib/api";
import { useFleet } from "@/lib/fleetContext";
import { ClusterView as CV, ClusterNode, ClusterHostNode, ScalingResult, ClusterForecast } from "@/lib/types";
import ScalingAdvisorPanel from "./ScalingAdvisorPanel";
import ForecastPanel from "./ForecastPanel";

// Plain-language explanations shown on hover.
const MEM_HELP: Record<string, string> = {
  "Total heap": "Sum of -Xmx (max heap) across every JVM in this cluster.",
  "In use (live set)":
    "Combined post-GC live memory currently held across all nodes — the real footprint after garbage is collected.",
  "Used of heap":
    "Cluster live set as a percentage of total allocated heap. Lower leaves more headroom for spikes.",
  "Peak node util":
    "Highest single-node post-GC heap occupancy in the cluster. Near 100% means that node is close to OutOfMemoryError.",
};
const TEL_HELP: Record<string, string> = {
  "GC engine": "The garbage collector each JVM is running (e.g. G1). Modern Kafka defaults to and is tuned for G1.",
  "GC log format": "Format of the GC logs being parsed. Unified (-Xlog:gc*) is the Java 11+ standard.",
  "Pause target": "G1's MaxGCPauseMillis goal — the pause time G1 tries to stay under per collection.",
  "Avg throughput":
    "Average percentage of wall-clock time across the cluster spent running the app rather than paused in GC. Aim ≥ 99%.",
  "Full GCs (1h / 24h)":
    "Count of Full GCs (whole-heap stop-the-world compactions) across the cluster in the last hour and last 24 hours. Should be 0.",
  "Worst pause (now)":
    "Longest stop-the-world GC pause observed on any node in the most recent sample. Long pauses risk request timeouts and ISR shrink.",
  "Heap sizing": "Configured max heap (-Xmx) per component role in this cluster.",
};

function Mem({ label, children }: { label: string; children: React.ReactNode }) {
  const help = MEM_HELP[label];
  return (
    <div className="metric" title={help || ""}>
      <div className="l">{label}{help ? <span className="info">ⓘ</span> : null}</div>
      <div className="v">{children}</div>
      {help ? <span className="tip">{help}</span> : null}
    </div>
  );
}

function CfgKey({ label }: { label: string }) {
  const help = TEL_HELP[label];
  if (!help) return <span className="k">{label}</span>;
  return (
    <span className="k help" title={help}>
      {label}<span className="info">ⓘ</span>
      <span className="tip">{help}</span>
    </span>
  );
}

function NodeCard({ n, onClick }: { n: ClusterNode; onClick: () => void }) {
  return (
    <div className={"ncard " + n.status} onClick={onClick}>
      <div className="nid">
        <span>{shortId(n.id)}</span>
        {n.grade ? <span className={"minigrade " + n.grade}>{n.grade}</span> : null}
      </div>
      <div className="nmeta">
        <span>{n.role}</span>
        <span>heap {n.heap_after_pct != null ? n.heap_after_pct + "%" : "—"}</span>
      </div>
      {n.full_gc_1h ? (
        <div className="nmeta" style={{ color: "var(--crit)" }}>🔴 Full GC in last hour</div>
      ) : null}
    </div>
  );
}

function HostCard({ n, onClick }: { n: ClusterHostNode; onClick: () => void }) {
  return (
    <div className={"ncard " + n.status} onClick={onClick}>
      <div className="nid">
        <span>{n.node_id || shortId(n.id)}</span>
        {n.grade ? <span className={"minigrade " + n.grade}>{n.grade}</span> : null}
      </div>
      <div className="nmeta">
        <span>cpu {n.cpu_busy_pct_avg != null ? n.cpu_busy_pct_avg + "%" : "—"}</span>
        <span>mem {n.mem_used_pct_avg != null ? n.mem_used_pct_avg + "%" : "—"}</span>
      </div>
      <div className="nmeta">
        <span>disk {n.disk_util_pct_max != null ? n.disk_util_pct_max + "%" : "—"}</span>
        <span>nic {n.net_util_pct_max != null ? n.net_util_pct_max + "%" : "—"}</span>
      </div>
      {(n.swap_used_pct_max ?? 0) > 0 ? (
        <div className="nmeta" style={{ color: "var(--crit)" }}>swap touched</div>
      ) : null}
    </div>
  );
}

export default function ClusterView({ cluster }: { cluster: string }) {
  const router = useRouter();
  const { tick } = useFleet();
  const { data: v, error } = useApi<CV>(`/api/cluster/${cluster}`, tick);
  const { data: scaling } = useApi<ScalingResult>(`/api/cluster/${cluster}/scaling?role=broker`, tick);
  const { data: fc } = useApi<ClusterForecast>(`/api/cluster/${cluster}/forecast?role=broker`, tick);

  if (error) return <div className="empty">Failed to load {cluster}: {error}</div>;
  if (!v) return <div className="empty">Loading {cluster}…</div>;

  const m = v.memory, t = v.telemetry, cfg = v.config, c = v.counts;
  const heapRoles = Object.entries(cfg.heap_by_role)
    .map(([r, s]) => `${r} ${s.join("/")} MB`)
    .join(" · ");

  const cards: { l: string; v: React.ReactNode; cls?: string }[] = [
    { l: "Total nodes", v: c.total },
    { l: "Healthy", v: c.healthy, cls: "ok" },
    { l: "Unhealthy", v: c.unhealthy, cls: c.unhealthy ? "crit" : "ok" },
    {
      l: "Cluster memory",
      v: (
        <>
          {(m.used_mb / 1024).toFixed(1)}
          <span className="muted" style={{ fontSize: 13 }}> / {(m.total_heap_mb / 1024).toFixed(0)} GB</span>
        </>
      ),
    },
    { l: "Avg heap util", v: m.avg_util_pct + "%" },
    { l: "GC engine", v: cfg.gc_engine.join(", ") },
  ];

  return (
    <>
      <div className="breadcrumb">{v.region} › {v.env}</div>
      <div className="ihdr">
        <span className="statuspill" style={{ background: PILL_COLOR[v.status] }}>{v.status}</span>
        <div style={{ fontSize: 18, fontWeight: 700 }}>{v.cluster} cluster</div>
        <span className="muted">snapshot {fmtTime(v.now)}</span>
      </div>

      <div className="cards">
        {cards.map((x, i) => (
          <div key={i} className={"card " + (x.cls || "")}>
            <div className="l">{x.l}</div>
            <div className="v">{x.v}</div>
          </div>
        ))}
      </div>

      <div className="panels">
        <div className="panel">
          <h3>Cluster memory</h3>
          <div className="metric-grid">
            <Mem label="Total heap">{(m.total_heap_mb / 1024).toFixed(0)} GB</Mem>
            <Mem label="In use (live set)">{(m.used_mb / 1024).toFixed(1)} GB</Mem>
            <Mem label="Used of heap">{m.used_pct}%</Mem>
            <Mem label="Peak node util">{m.peak_util_pct}%</Mem>
          </div>
        </div>
        <div className="panel">
          <h3>Java / GC configuration &amp; telemetry</h3>
          <div className="cfg">
            <CfgKey label="GC engine" /><span>{cfg.gc_engine.join(", ")}</span>
            <CfgKey label="GC log format" /><span>{cfg.log_format}</span>
            <CfgKey label="Pause target" /><span>{cfg.pause_target_ms} ms (MaxGCPauseMillis)</span>
            <CfgKey label="Avg throughput" /><span>{t.avg_throughput_pct}%</span>
            <CfgKey label="Full GCs (1h / 24h)" />
            <span style={{ color: t.full_gc_1h ? "var(--crit)" : "inherit" }}>{t.full_gc_1h} / {t.full_gc_24h}</span>
            <CfgKey label="Worst pause (now)" />
            <span style={{ color: t.worst_pause_ms > 500 ? "var(--crit)" : "inherit" }}>{t.worst_pause_ms} ms</span>
            <CfgKey label="Heap sizing" /><span>{heapRoles}</span>
          </div>
        </div>
      </div>

      <h2 className="sec">Nodes — current GC health</h2>
      <div className="legend">
        <span><span className="dot ok" /> healthy</span>
        <span><span className="dot watch" /> watch</span>
        <span><span className="dot crit" /> critical</span>
        <span>🔴 Full GC in last hour</span>
      </div>
      <div className="nodegrid">
        {v.nodes.map((n) => (
          <NodeCard key={n.id} n={n} onClick={() => router.push(`/instance/${n.id}`)} />
        ))}
      </div>

      <h2 className="sec" style={{ marginTop: 24 }}>
        Needs attention{v.attention.length ? ` (${v.attention.length})` : ""} — click to investigate
      </h2>
      <div className="alerts">
        {v.attention.length ? (
          v.attention.map((n) => (
            <div
              key={n.id}
              className={"alert " + (n.status === "critical" ? "critical" : "warning")}
              onClick={() => router.push(`/instance/${n.id}`)}
            >
              <span className={"sev " + (n.status === "critical" ? "critical" : "warning")}>{n.status}</span>
              <span className="where">{n.id}</span>
              <span className="msg">{(n.alerts[0] && n.alerts[0].msg) || n.reason || "grade " + n.grade}</span>
              {n.grade ? <span className={"minigrade " + n.grade} style={{ marginLeft: "auto" }}>{n.grade}</span> : null}
            </div>
          ))
        ) : (
          <div className="muted">All nodes healthy — nothing needs attention. 🟢</div>
        )}
      </div>

      {v.host_nodes && v.host_summary ? (
        <>
          <h2 className="sec" style={{ marginTop: 24, display: "flex", alignItems: "center", gap: 10 }}>
            Hosts — server health (SAR)
            {v.host_status ? (
              <span className="statuspill" style={{ background: PILL_COLOR[v.host_status], fontSize: 11 }}>
                {v.host_status}
              </span>
            ) : null}
            <span className="muted" style={{ fontSize: 12, fontWeight: 400 }}>
              {v.host_counts?.healthy ?? 0} healthy · {v.host_counts?.unhealthy ?? 0} under pressure ·{" "}
              {v.host_summary.n_with_data}/{v.host_counts?.total ?? 0} reporting
            </span>
          </h2>

          <div className="panels">
            <div className="panel full">
              <h3>Cluster host metrics (24h)</h3>
              <div className="metric-grid">
                <div className="metric"><div className="l">CPU busy (avg / peak node)</div><div className="v">{v.host_summary.cpu_busy_avg}% / {v.host_summary.cpu_busy_peak}%</div></div>
                <div className="metric"><div className="l">iowait (avg)</div><div className="v">{v.host_summary.iowait_avg}%</div></div>
                <div className="metric"><div className="l">Memory used (avg / peak node)</div><div className="v">{v.host_summary.mem_used_avg}% / {v.host_summary.mem_used_peak}%</div></div>
                <div className="metric"><div className="l">Worst disk util / await</div><div className="v">{v.host_summary.disk_util_worst}% / {v.host_summary.disk_await_worst} ms</div></div>
                <div className="metric"><div className="l">Worst NIC util</div><div className="v">{v.host_summary.net_util_worst}%</div></div>
                <div className="metric"><div className="l">Cluster ingress / egress</div><div className="v">{(v.host_summary.net_rx_total_kbs / 1024).toFixed(1)} / {(v.host_summary.net_tx_total_kbs / 1024).toFixed(1)} MB/s</div></div>
                <div className="metric"><div className="l">Nodes with swap touched</div><div className="v" style={{ color: v.host_summary.swap_touched_nodes ? "var(--crit)" : "inherit" }}>{v.host_summary.swap_touched_nodes}</div></div>
                <div className="metric"><div className="l">Nodes with majflt storms</div><div className="v" style={{ color: v.host_summary.majflt_hot_nodes ? "var(--crit)" : "inherit" }}>{v.host_summary.majflt_hot_nodes}</div></div>
              </div>
            </div>
          </div>

          <div className="legend" style={{ marginTop: 10 }}>
            <span><span className="dot ok" /> healthy</span>
            <span><span className="dot watch" /> watch</span>
            <span><span className="dot crit" /> critical</span>
            <span className="muted">click a host card for the full SAR analysis; GC cards above open the JVM view</span>
          </div>
          <div className="nodegrid">
            {v.host_nodes.map((n) => (
              <HostCard key={n.id} n={n} onClick={() => router.push(`/host/${n.id}`)} />
            ))}
          </div>

          <h2 className="sec" style={{ marginTop: 24 }}>
            Host needs attention{v.host_attention?.length ? ` (${v.host_attention.length})` : ""} — click to investigate
          </h2>
          <div className="alerts">
            {v.host_attention?.length ? (
              v.host_attention.map((n) => (
                <div
                  key={n.id}
                  className={"alert " + (n.status === "critical" ? "critical" : "warning")}
                  onClick={() => router.push(`/host/${n.id}`)}
                >
                  <span className={"sev " + (n.status === "critical" ? "critical" : "warning")}>{n.status}</span>
                  <span className="where">{n.id}</span>
                  <span className="msg">{n.reason || "grade " + n.grade}</span>
                  {n.grade ? <span className={"minigrade " + n.grade} style={{ marginLeft: "auto" }}>{n.grade}</span> : null}
                </div>
              ))
            ) : (
              <div className="muted">All hosts healthy — no OS-level resource pressure. 🟢</div>
            )}
          </div>
        </>
      ) : null}

      <h2 className="sec" style={{ marginTop: 24 }}>Capacity &amp; scaling</h2>
      <div className="panels">
        <ScalingAdvisorPanel scaling={scaling ?? null} />
        <ForecastPanel forecast={fc ?? null} />
      </div>
    </>
  );
}
