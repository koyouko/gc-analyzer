"use client";

import { useState } from "react";
import { useApi } from "@/lib/api";
import { SarUploadResult } from "@/lib/types";
import { useFleet } from "@/lib/fleetContext";

interface Me {
  user: string;
  role: string;
}

/** "Upload SAR report" — the no-SSH ingestion path. Run on the RedHat host:
 *    TZ=UTC LC_ALL=C sadf -j -- -A     (preferred, JSON)
 *    TZ=UTC LC_ALL=C sar -A           (classic text; set the report date)
 * then paste or drop the output here. Same parse/dedup/record pipeline as
 * SSH collection. Admin-only (enforced server-side too). */
export default function SarUploadPanel({ instanceId }: { instanceId: string }) {
  const { refresh } = useFleet();
  const { data: me } = useApi<Me>("/api/me");
  const [content, setContent] = useState("");
  const [format, setFormat] = useState("auto");
  const [reportDate, setReportDate] = useState("");
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<SarUploadResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [open, setOpen] = useState(false);

  const isAdmin = me?.role === "admin";

  async function readFile(f: File) {
    setContent(await f.text());
  }

  async function submit() {
    setBusy(true);
    setError(null);
    setResult(null);
    try {
      const r = await fetch(`/api/instance/${instanceId}/sar/upload`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          content,
          format,
          report_date: reportDate || null,
        }),
      });
      const body = await r.json().catch(() => null);
      if (!r.ok) throw new Error((body && body.detail) || `HTTP ${r.status}`);
      setResult(body as SarUploadResult);
      setContent("");
      refresh(); // host panels re-read the new samples
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="panel full">
      <h3 style={{ display: "flex", alignItems: "center", gap: 10 }}>
        Upload SAR report
        <button className="linklike" onClick={() => setOpen(!open)}>
          {open ? "hide" : "show"}
        </button>
      </h3>
      {!open ? (
        <div className="muted" style={{ fontSize: 12 }}>
          No SSH configured for this host? Run <code>TZ=UTC LC_ALL=C sadf -j -- -A</code> (or{" "}
          <code>sar -A</code>) on it and drop the output here.
        </div>
      ) : !isAdmin ? (
        <div className="muted">
          Uploading SAR data requires the <b>admin</b> role — sign in as an admin to use this.
        </div>
      ) : (
        <>
          <div className="muted" style={{ fontSize: 12, marginBottom: 8 }}>
            On the RedHat host: <code>TZ=UTC LC_ALL=C sadf -j -- -A</code> (preferred, carries its own
            dates) or <code>TZ=UTC LC_ALL=C sar -A</code> (set the report date below). Samples already
            ingested are deduped by timestamp, so re-uploading is safe.
          </div>
          <textarea
            className="sar-upload-text"
            placeholder="Paste sadf -j JSON or sar -A text here… (or use the file picker)"
            value={content}
            onChange={(e) => setContent(e.target.value)}
            rows={6}
          />
          <div style={{ display: "flex", gap: 10, alignItems: "center", marginTop: 8, flexWrap: "wrap" }}>
            <input
              type="file"
              accept=".json,.txt,.log,.out"
              onChange={(e) => e.target.files?.[0] && readFile(e.target.files[0])}
            />
            <select value={format} onChange={(e) => setFormat(e.target.value)}>
              <option value="auto">auto-detect</option>
              <option value="sadf-json">sadf -j (JSON)</option>
              <option value="sar-text">sar -A (text)</option>
            </select>
            <input
              type="date"
              value={reportDate}
              onChange={(e) => setReportDate(e.target.value)}
              title="Report date anchor for classic `sar -A` text dumps (sadf JSON doesn't need it)"
            />
            <button className="btn" disabled={busy || !content.trim()} onClick={submit}>
              {busy ? "Ingesting…" : "Ingest report"}
            </button>
          </div>
          {error ? <div className="alert critical" style={{ marginTop: 8 }}><span className="sev critical">error</span><span className="msg">{error}</span></div> : null}
          {result ? (
            <div className="alert" style={{ marginTop: 8 }}>
              <span className="sev">ok</span>
              <span className="msg">
                {result.detail}
                {result.warnings.length ? ` · parser warnings: ${result.warnings.join("; ")}` : ""}
              </span>
            </div>
          ) : null}
        </>
      )}
    </div>
  );
}
