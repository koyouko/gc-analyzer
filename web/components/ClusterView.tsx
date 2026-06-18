"use client";

import { useRouter } from "next/navigation";
import { useApi, PILL_COLOR, fmtTime, shortId } from "@/lib/api";
import { useFleet } from "@/lib/fleetContext";
import { ClusterView as CV, ClusterNode } from "@/lib/types";

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

export default function ClusterView({ cluster }: { cluster: string }) {
  const router = useRouter();
  const { tick } = useFleet();
  const { data: v, error } = useApi<CV>(`/api/cluster/${cluster}`, tick);

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
    </>
  );
}
