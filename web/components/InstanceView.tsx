"use client";

import dynamic from "next/dynamic";
import { useApi, STATUS_DOT } from "@/lib/api";
import { useFleet } from "@/lib/fleetContext";
import { InstanceSnapshot, Trends, Status } from "@/lib/types";

// Charts use the canvas/DOM, so load them client-only.
const TrendCharts = dynamic(() => import("./TrendCharts"), {
  ssr: false,
  loading: () => <div className="muted" style={{ padding: 12 }}>Loading charts…</div>,
});

// Plain-language explanation shown on hover for each metric card.
const METRIC_HELP: Record<string, string> = {
  "Throughput":
    "Percentage of wall-clock time the application ran instead of being paused by GC. Higher is better — aim for ≥ 99%.",
  "Time in GC":
    "Share of wall-clock time spent inside stop-the-world GC pauses. The inverse of throughput; lower is better.",
  "Avg pause":
    "Average duration of a stop-the-world GC pause over the window. Application threads are frozen during each pause.",
  "p99 pause":
    "99th-percentile GC pause — 99% of pauses were shorter than this. Tail latency that directly affects Kafka request times.",
  "Max pause":
    "Longest single stop-the-world pause in the window. Long pauses can trip request.timeout, shrink the ISR, and stall produce/fetch.",
  "GC freq":
    "Garbage collections per minute. Very high frequency points to a hot allocation path or an undersized young generation.",
  "Full GCs (24h)":
    "Count of Full GCs (whole-heap compaction with all threads stopped) in the last 24h. Should be 0 on a healthy broker.",
  "Heap max":
    "Configured maximum heap size (-Xmx) for this JVM process.",
  "Avg live set":
    "Average post-GC heap occupancy as a percentage of max heap — the steady-state live memory footprint after collections.",
  "Peak live set":
    "Highest post-GC heap occupancy as a percentage of max heap. Near 100% means little headroom before OutOfMemoryError.",
  "Promotion trend":
    "How often the post-GC live set grew versus the previous collection. A steady climb hints at a memory leak or growing cache.",
};

export default function InstanceView({ id }: { id: string }) {
  const { tick } = useFleet();
  const { data: s, error } = useApi<InstanceSnapshot>(`/api/instance/${id}`, tick);
  const { data: tr } = useApi<Trends>(`/api/instance/${id}/trends?days=30`, tick);

  if (error) return <div className="empty">Failed to load {id}: {error}</div>;
  if (!s) return <div className="empty">Loading {id}…</div>;

  const m = s.metrics, h = s.health, inst = s.instance;
  const status: Status = s.alerts.some((a) => a.severity === "critical")
    ? "critical"
    : s.alerts.length || ["C", "D", "F"].includes(h.grade)
    ? "watch"
    : "ok";

  const mc: [string, React.ReactNode][] = [
    ["Throughput", m.throughput_pct + "%"],
    ["Time in GC", m.pct_time_in_gc + "%"],
    ["Avg pause", m.avg_pause_ms + " ms"],
    ["p99 pause", m.p99_pause_ms + " ms"],
    ["Max pause", m.max_pause_ms + " ms"],
    ["GC freq", m.gc_per_min + "/min"],
    ["Full GCs (24h)", m.full_count],
    ["Heap max", m.heap_max_mb + " MB"],
    ["Avg live set", m.avg_heap_after_pct + "%"],
    ["Peak live set", m.peak_heap_after_pct + "%"],
    ["Promotion trend", m.promotion_trend_pct + "%"],
  ];

  return (
    <>
      <div className="breadcrumb">
        {inst.region} › {inst.env} › {inst.cluster} › {inst.role}
      </div>
      <div className="ihdr">
        <div className={"grade " + h.grade}>{h.grade}</div>
        <div>
          <div style={{ fontSize: 18, fontWeight: 700 }}>{inst.id}</div>
          <div className="muted">{h.score} / 100 · {h.status} · heap {inst.heap_max_mb} MB</div>
        </div>
        <span className="engine">
          <span className={"dot " + (STATUS_DOT[status] || "unknown")} /> {inst.collector} · {inst.role}
        </span>
      </div>
      <div className="muted" style={{ marginBottom: 8 }}>{h.reasons.join(" · ") || "No issues detected."}</div>

      {s.alerts.length ? (
        <div className="alerts" style={{ margin: "6px 0 4px" }}>
          {s.alerts.map((a, i) => (
            <div key={i} className={"alert " + a.severity}>
              <span className={"sev " + a.severity}>{a.severity}</span>
              <span className="msg">{a.msg}</span>
            </div>
          ))}
        </div>
      ) : null}

      <h2 className="sec">Current metrics (24h window)</h2>
      <div className="metric-grid">
        {mc.map(([l, v], i) => (
          <div key={i} className="metric" title={METRIC_HELP[l as string] || ""}>
            <div className="l">{l}<span className="info">ⓘ</span></div>
            <div className="v">{v}</div>
            {METRIC_HELP[l as string] ? <span className="tip">{METRIC_HELP[l as string]}</span> : null}
          </div>
        ))}
      </div>

      <h2 className="sec">30-day trends</h2>
      {tr ? <TrendCharts tr={tr} /> : <div className="muted" style={{ padding: 12 }}>Loading trends…</div>}

      <div className="panels">
        <div className="panel"><h3 style={{ color: "#7ee787" }}>Pros</h3>
          <ul className="list pros">{s.findings.pros.length ? s.findings.pros.map((x, i) => <li key={i}>{x}</li>) : <li className="muted">—</li>}</ul>
        </div>
        <div className="panel"><h3 style={{ color: "#ffa198" }}>Cons</h3>
          <ul className="list cons">{s.findings.cons.length ? s.findings.cons.map((x, i) => <li key={i}>{x}</li>) : <li className="muted">None.</li>}</ul>
        </div>
        <div className="panel full"><h3 style={{ color: "var(--accent)" }}>How to improve</h3>
          <ul className="list recs">{s.findings.recommendations.map((x, i) => <li key={i}>{x}</li>)}</ul>
        </div>
      </div>
    </>
  );
}
