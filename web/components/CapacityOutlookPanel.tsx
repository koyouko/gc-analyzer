"use client";

import { InstanceForecast } from "@/lib/types";

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

/** Per-instance "Capacity outlook": Theil-Sen trend per GC/host signal with
 * projected days-until-threshold-breach. The proactive counterpart to the
 * point-in-time Server health panel above it. */
export default function CapacityOutlookPanel({ forecast }: { forecast: InstanceForecast | null }) {
  if (!forecast) {
    return (
      <div className="panel full">
        <h3>Capacity outlook</h3>
        <div className="muted">Loading capacity forecast…</div>
      </div>
    );
  }

  const fitted = forecast.signals.filter((s) => s.status !== "insufficient_data");

  return (
    <div className="panel full">
      <h3 style={{ display: "flex", alignItems: "center", gap: 10 }}>
        Capacity outlook (next {forecast.horizon_days} days)
        <span className={"fcrisk " + forecast.risk}>{forecast.risk.replace("_", " ")}</span>
      </h3>
      <div style={{ marginBottom: 8 }}>{forecast.headline}</div>

      {fitted.length ? (
        <table className="sar-table">
          <tbody>
            <tr>
              <th>Signal</th><th>Today</th><th>Trend / day</th><th>Warn at</th>
              <th>Crit at</th><th>Warn in</th><th>Critical in</th><th>Status</th><th>Confidence</th>
            </tr>
            {forecast.signals.map((s) => (
              <tr key={s.signal}>
                <td>{s.label}</td>
                <td>{s.current != null ? `${s.current}${s.unit}` : "—"}</td>
                <td>
                  {s.slope_per_day != null
                    ? `${s.slope_per_day > 0 ? "+" : ""}${s.slope_per_day}${s.unit}`
                    : "—"}
                </td>
                <td>{s.warn_threshold}{s.unit}</td>
                <td>{s.crit_threshold}{s.unit}</td>
                <td>{s.days_to_warn != null ? `~${Math.round(s.days_to_warn)}d` : "—"}</td>
                <td>
                  {s.days_to_crit != null
                    ? `~${Math.round(s.days_to_crit)}d (${s.crit_date})`
                    : "—"}
                </td>
                <td><span className={"fcstatus " + s.status}>{STATUS_LABEL[s.status] || s.status}</span></td>
                <td>{s.confidence}</td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <div className="muted">
          Not enough daily history yet — capacity trends need at least 7 days of GC/host metrics.
        </div>
      )}
      <div className="muted" style={{ marginTop: 8, fontSize: 11 }}>
        Trend = Theil-Sen (median pairwise slope) over daily aggregates from the last {forecast.days} days;
        &quot;today&quot; = median of the last 7 daily points. Advisory only.
      </div>
    </div>
  );
}
