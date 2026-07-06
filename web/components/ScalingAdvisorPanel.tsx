"use client";

import { ScalingResult } from "@/lib/types";
import { shortId } from "@/lib/api";

const VERDICT_LABEL: Record<string, string> = {
  horizontal: "Scale horizontally (add brokers)",
  vertical_memory: "Scale vertically (heap / RAM)",
  rebalance: "Rebalance partitions/leaders",
  no_action: "No action needed",
  mixed: "Mixed — investigate per-node",
};

/** "Capacity & scaling" section on the cluster page: vertical vs horizontal
 * vs rebalance recommendation, with the per-node evidence behind it. */
export default function ScalingAdvisorPanel({ scaling }: { scaling: ScalingResult | null }) {
  if (!scaling || scaling.verdict === "insufficient_data") {
    return (
      <div className="panel full">
        <h3>Scaling advisor</h3>
        <div className="muted">{(scaling && scaling.summary) || "Not enough broker GC + host history yet to recommend a scaling direction."}</div>
      </div>
    );
  }

  const skew = scaling.skew;

  return (
    <div className="panel full">
      <h3 style={{ display: "flex", alignItems: "center", gap: 10 }}>
        Scaling advisor ({scaling.role}s)
        <span className={"verdictpill " + scaling.verdict}>{VERDICT_LABEL[scaling.verdict] || scaling.verdict}</span>
        <span className="muted" style={{ fontSize: 11, fontWeight: 400 }}>confidence: {scaling.confidence}</span>
      </h3>
      <div style={{ marginBottom: 8 }}>{scaling.summary}</div>
      <ul className="list recs">{scaling.evidence.map((x, i) => <li key={i}>{x}</li>)}</ul>

      {skew ? (
        <div className="muted" style={{ marginTop: 8, fontSize: 11 }}>
          Cross-node skew: CPU CV={skew.cpu_busy_cv ?? "—"}, net-egress CV={skew.net_tx_cv ?? "—"}
          {skew.hot_nodes.length ? ` · hot: ${skew.hot_nodes.map((id) => shortId(id)).join(", ")}` : ""}
        </div>
      ) : null}

      <table className="sar-table" style={{ marginTop: 10 }}>
        <tbody>
          <tr><th>Node</th><th>Bottleneck</th><th>GC</th><th>Host</th><th>CPU avg</th><th>NIC max</th><th>Full GC 24h</th></tr>
          {scaling.nodes.map((n) => (
            <tr key={n.instance_id}>
              <td>{shortId(n.instance_id)}</td>
              <td><span className={"bnpill " + n.dominant_bottleneck}>{n.dominant_bottleneck.replace("_", " ")}</span></td>
              <td>{n.gc_grade ? <span className={"minigrade " + n.gc_grade}>{n.gc_grade}</span> : "—"}</td>
              <td>{n.host_grade ? <span className={"minigrade " + n.host_grade}>{n.host_grade}</span> : "—"}</td>
              <td>{n.cpu_busy_pct_avg != null ? n.cpu_busy_pct_avg + "%" : "—"}</td>
              <td>{n.net_util_pct_max != null ? n.net_util_pct_max + "%" : "—"}</td>
              <td>{n.full_gc_24h ?? "—"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
