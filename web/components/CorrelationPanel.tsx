"use client";

import { CorrelationResult, AnomalyResult } from "@/lib/types";

const VERDICT_LABEL: Record<string, string> = {
  gc_bound: "GC-bound (JVM tuning)",
  host_bound: "Host-bound (OS contention)",
  mixed: "Mixed signal",
  insufficient_data: "Insufficient data",
};

/** "GC <-> host correlation" panel — is this node's GC pressure actually a
 * JVM tuning problem, or is the host starving it? Plus the ML Tech Preview
 * anomaly badge, since both answer "is something here worth a closer look". */
export default function CorrelationPanel({ corr, anomaly }: { corr: CorrelationResult | null; anomaly: AnomalyResult | null }) {
  if (!corr || corr.verdict === "insufficient_data") {
    return (
      <div className="panel full">
        <h3>GC ↔ host correlation</h3>
        <div className="muted">{(corr && corr.findings[0]) || "Not enough overlapping GC + host history yet."}</div>
      </div>
    );
  }

  const shown = corr.correlations.filter((c) => c.strength !== "weak");

  return (
    <div className="panel full">
      <h3 style={{ display: "flex", alignItems: "center", gap: 10 }}>
        GC ↔ host correlation
        <span className={"verdictpill " + corr.verdict}>{VERDICT_LABEL[corr.verdict] || corr.verdict}</span>
        <span className="muted" style={{ fontSize: 11, fontWeight: 400 }}>
          {corr.n_points} overlapping hour(s) over {corr.days}d
        </span>
      </h3>

      <table className="sar-table" style={{ marginBottom: 10 }}>
        <tbody>
          <tr><th>Signal pair</th><th>r</th><th>strength</th></tr>
          {shown.length ? shown.map((c, i) => (
            <tr key={i}>
              <td>{c.label}</td>
              <td>{c.r > 0 ? "+" : ""}{c.r}</td>
              <td><span className={"bnpill " + (c.strength === "strong" ? "host_memory" : "watch")}>{c.strength}</span></td>
            </tr>
          )) : (
            <tr><td colSpan={3} className="muted">No moderate/strong correlations in this window.</td></tr>
          )}
        </tbody>
      </table>

      <ul className="list recs">{corr.findings.map((x, i) => <li key={i}>{x}</li>)}</ul>

      {anomaly && anomaly.method !== "none" ? (
        <div style={{ marginTop: 10 }}>
          <span className={"anomaly-badge " + (anomaly.is_anomalous ? "" : "calm")}>
            ML Tech Preview: {anomaly.is_anomalous ? "unusual pattern detected" : "looks normal"}
            {" "}(score {anomaly.overall_anomaly_score}/100, {anomaly.method})
          </span>
          <div className="muted" style={{ marginTop: 6, fontSize: 11 }}>{anomaly.notice}</div>
        </div>
      ) : null}
    </div>
  );
}
