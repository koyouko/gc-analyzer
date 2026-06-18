"use client";

import { useEffect, useState } from "react";

export async function api<T>(path: string): Promise<T> {
  const r = await fetch(path, { cache: "no-store" });
  if (!r.ok) throw new Error((await r.text()) || `HTTP ${r.status}`);
  return (await r.json()) as T;
}

/** Fetch `path` on mount and whenever `path` or `dep` changes. */
export function useApi<T>(path: string | null, dep: unknown = 0) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState<boolean>(!!path);

  useEffect(() => {
    if (!path) return;
    let alive = true;
    setLoading(true);
    setError(null);
    api<T>(path)
      .then((d) => alive && setData(d))
      .catch((e) => alive && setError(e.message))
      .finally(() => alive && setLoading(false));
    return () => {
      alive = false;
    };
  }, [path, dep]);

  return { data, error, loading };
}

export const STATUS_DOT: Record<string, string> = {
  ok: "ok",
  watch: "watch",
  critical: "crit",
  unknown: "unknown",
};
export const PILL_COLOR: Record<string, string> = {
  ok: "var(--ok)",
  watch: "var(--watch)",
  critical: "var(--crit)",
  unknown: "var(--unknown)",
};

export const fmtTime = (ts?: number) => (ts ? new Date(ts * 1000).toLocaleString() : "—");
export const fmtDay = (ts: number) =>
  new Date(ts * 1000).toLocaleDateString([], { month: "short", day: "numeric" });
export const shortId = (id: string) => id.split("-").slice(-2).join("-");
