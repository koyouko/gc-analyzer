"use client";

import { useEffect, useState } from "react";
import { usePathname, useRouter } from "next/navigation";
import { useFleet } from "@/lib/fleetContext";
import { STATUS_DOT, shortId } from "@/lib/api";
import { Status } from "@/lib/types";

const Dot = ({ s }: { s: Status }) => <span className={"dot " + (STATUS_DOT[s] || "unknown")} />;

export default function Sidebar() {
  const { fleet } = useFleet();
  const router = useRouter();
  const pathname = usePathname();
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});
  const [autoDone, setAutoDone] = useState(false);

  // Auto-expand any path that contains a degraded component (once).
  useEffect(() => {
    if (!fleet || autoDone) return;
    const e: Record<string, boolean> = {};
    for (const r of fleet.regions) {
      e["r:" + r.region] = true;
      for (const env of r.envs) {
        if (env.status === "critical" || env.status === "watch") {
          e["e:" + r.region + env.env] = true;
          for (const g of env.groups) if (g.status !== "ok") e["g:" + env.cluster + g.group] = true;
        }
      }
    }
    setExpanded(e);
    setAutoDone(true);
  }, [fleet, autoDone]);

  if (!fleet) return <div className="sidebar"><div className="muted" style={{ padding: 12 }}>Loading…</div></div>;

  const toggle = (k: string) => setExpanded((p) => ({ ...p, [k]: !p[k] }));
  const selCluster = (c: string) => pathname === `/cluster/${c}`;
  const selInst = (id: string) => pathname === `/instance/${id}`;

  return (
    <div className="sidebar">
      {fleet.regions.map((r) => {
        const rk = "r:" + r.region;
        const ro = expanded[rk] !== false;
        return (
          <div key={r.region}>
            <div className="tree-row" onClick={() => toggle(rk)}>
              <span className="tw">{ro ? "▾" : "▸"}</span>
              <Dot s={r.status} />
              <span className="lbl"><b>{r.region}</b></span>
            </div>
            {ro &&
              r.envs.map((env) => {
                const ek = "e:" + r.region + env.env;
                const eo = !!expanded[ek];
                return (
                  <div key={env.env}>
                    <div
                      className={"tree-row indent1 " + (selCluster(env.cluster) ? "sel" : "")}
                      onClick={() => router.push(`/cluster/${env.cluster}`)}
                      title={`Open ${env.cluster} cluster dashboard`}
                    >
                      <span
                        className="tw"
                        onClick={(e) => { e.stopPropagation(); toggle(ek); }}
                      >
                        {eo ? "▾" : "▸"}
                      </span>
                      <Dot s={env.status} />
                      <span className="lbl">{env.env}</span>
                      {env.alert_count ? <span className="cnt"> ⚠{env.alert_count}</span> : null}
                    </div>
                    {eo &&
                      env.groups.map((g) => {
                        const gk = "g:" + env.cluster + g.group;
                        const go = !!expanded[gk];
                        return (
                          <div key={g.group}>
                            <div className="tree-row indent2" onClick={() => toggle(gk)}>
                              <span className="tw">{go ? "▾" : "▸"}</span>
                              <Dot s={g.status} />
                              <span className="lbl">{g.label}</span>
                              <span className="cnt">{g.count}</span>
                            </div>
                            {go &&
                              g.instances.map((i) => (
                                <div
                                  key={i.id}
                                  className={"tree-row indent3 " + (selInst(i.id) ? "sel" : "")}
                                  onClick={() => router.push(`/instance/${i.id}`)}
                                >
                                  <Dot s={i.status} />
                                  <span className="lbl">{shortId(i.id)}</span>
                                  {i.full_gc_1h ? <span className="cnt">🔴</span> : null}
                                </div>
                              ))}
                          </div>
                        );
                      })}
                  </div>
                );
              })}
          </div>
        );
      })}
    </div>
  );
}
