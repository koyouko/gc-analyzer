"use client";

import { ClusterForecast } from "@/lib/types";
import { shortId } from "@/lib/api";

const VERDICT_LABEL: Record<string, string> = {
  plan_horizontal: "Plan horizontal scale (add brokers)",
  plan_vertical_memory: "Plan vertical scale (host RAM)",
  plan_vertical_heap: "Plan vertical scale (JVM heap)",
  plan_tune_gc: "Tune GC before scaling",
  watch_hot_node: "Watch hot node / rebalance first",
  none: "No scaling needed in horizon",
  insufficient_data: "Insufficient history",
};

const STATUS_LABEL: Record<string, string> = {
  already_critical: "already critical",
  breach_imminent: "critical soon",
  already_warning: "already warning",
  breach_projected: "breach projected",
  watch: "watch",
  improving: "improving",
  stable: "stable",
  insufficient_data: "no history",
};

/** "Capacity forecast" section on the cluster page: proactive, trend-based
 * companion to the (reactive) scaling advisor — when will each broker run
 * out of headroom, and which scaling direction should be planned now. */
export default function ForecastPanel({ forecast }: { forecast: ClusterForecast | null }) {
  if (!forecast || forecast.verdict === "insufficient_data") {
    return (
      <div className="panel full">
        <h3>Capacity forecast</h3>
        <div className="muted">
          {(forecast && forecast.summary) ||
            "Not enough daily GC + host history yet to project capacity trends (need >= 7 days per node)."}
        </div>
      </div>
    );
  }

  return (
    <div className="panel full">
      <h3 style={{ display: "flex", alignItems: "center", gap: 10 }}>
        Capacity forecast ({forecast.role}s · next {forecast.horizon_days} days)
        <span className={"verdictpill " + forecast.verdict}>
          {VERDICT_LABEL[forecast.verdict] || forecast.verdict}
        </span>
        <span className="muted" style={{ fontSize: 11, fontWeight: 400 }}>
          confidence: {forecast.confidence}
        </span>
      </h3>
      <div style={{ marginBottom: 8 }}>{forecast.summary}</div>
      {forecast.evidence.length ? (
        <ul className="list recs">{forecast.evidence.map((x, i) => <li key={i}>{x}</li>)}</ul>
      ) : null}

      {forecast.warnings.length ? (
        <div className="alerts" style={{ margin: "8px 0" }}>
          {forecast.warnings.map((w, i) => (
            <div key={i} className="alert warning">
              <span className="sev warning">forecast</span>
              <span className="msg">{w}</span>
            </div>
          ))}
        </div>
      ) : null}

      <table className="sar-table" style={{ marginTop: 10 }}>
        <tbody>
          <tr><th>Node</th><th>Risk</th><th>Leading signal</th><th>Today</th><th>Critical in</th><th>Confidence</th></tr>
          {forecast.nodes.map((n) => (
            <tr key={n.instance_id}>
              <td>{shortId(n.instance_id)}</td>
              <td><span className={"fcrisk " + n.risk}>{n.risk.replace("_", " ")}</span></td>
              <td>
                {n.top_label ?? "—"}
                {n.top_status ? (
                  <span className={"fcstatus " + n.top_status} style={{ marginLeft: 6 }}>
                    {STATUS_LABEL[n.top_status] || n.top_status}
                  </span>
                ) : null}
              </td>
              <td>{n.top_current != null ? `${n.top_current}${n.top_unit ?? ""}` : "—"}</td>
              <td>
                {n.top_days_to_crit != null
                  ? `~${Math.round(n.top_days_to_crit)}d (${n.top_crit_date})`
                  : "—"}
              </td>
              <td>{n.top_confidence ?? "—"}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <div className="muted" style={{ marginTop: 8, fontSize: 11 }}>
        Theil-Sen trend over daily aggregates — robust to single-day incidents. Advisory only:
        validate against expected workload changes before acting.
      </div>
    </div>
  );
}
