"""
Fleet rollup: turn the inventory + history store into the nested structure the
dashboard renders, with "right now" health and "last hour" alert flags rolled up
at every level (instance -> group -> cluster/env -> region -> fleet).

Status vocabulary (drives color coding in the UI):
  ok       green   healthy now, no recent alerts
  watch    amber   degraded health or a warning-level last-hour alert
  critical red     critical last-hour alert (Full GC / long pause) or failing health
  unknown  grey    no data
"""

from __future__ import annotations

import statistics

from . import store, topology

STATUS_RANK = {"unknown": 0, "ok": 1, "watch": 2, "critical": 3}


def _instance_status(health: dict | None, alerts: list[dict]) -> str:
    if health is None:
        return "unknown"
    if any(a["severity"] == "critical" for a in alerts):
        return "critical"
    grade = health["grade"]
    if grade in ("D", "F"):
        return "critical"
    if any(a["severity"] == "warning" for a in alerts) or grade == "C":
        return "watch"
    return "ok"


def _worse(a: str, b: str) -> str:
    return a if STATUS_RANK[a] >= STATUS_RANK[b] else b


# Deterministic display ordering. Known regions/envs/groups keep the demo's
# original order; anything else (e.g. a live "LOCAL" region) sorts after them.
_REGION_ORDER = {"DEMO": 0, "NAM": 1, "EMEA": 2, "APAC": 3}
_ENV_ORDER = {"KRAFT": 0, "ZOOKEEPER": 1, "UAT": 2, "PROD": 3, "SANDBOX": 4, "PHY": 5, "DEV": 6}
_GROUP_ORDER = {g["key"]: n for n, g in enumerate(topology.COMPONENT_GROUPS)}


def _skeleton_from_store(insts: dict) -> dict:
    """Region -> env -> cluster -> group -> [instance ids], derived from the
    instances actually in the store. This lets the fleet view render any real
    cluster, not just the seeded demo topology, while keeping the demo's render
    order unchanged (known region/env/group order; instances by id)."""
    ordered = sorted(
        insts.values(),
        key=lambda i: (
            _REGION_ORDER.get(i["region"], len(_REGION_ORDER)), i["region"],
            _ENV_ORDER.get(i["env"], len(_ENV_ORDER)), i["env"],
            i["cluster"],
            _GROUP_ORDER.get(i["grp"], len(_GROUP_ORDER)), i["id"],
        ),
    )
    tree: dict = {}
    for inst in ordered:
        r = tree.setdefault(inst["region"], {"region": inst["region"], "envs": {}})
        e = r["envs"].setdefault(
            inst["env"], {"env": inst["env"], "clusters": {}}
        )
        c = e["clusters"].setdefault(
            inst["cluster"], {"cluster": inst["cluster"], "groups": {}}
        )
        g = c["groups"].setdefault(inst["grp"], {"group": inst["grp"], "instances": []})
        g["instances"].append(inst["id"])
    return tree


def build_fleet(c, now: int = None) -> dict:
    now = now or store.now_ts(c)
    insts = {i["id"]: i for i in store.list_instances(c)}

    # Compute per-instance status once.
    inst_status: dict[str, dict] = {}
    for iid, inst in insts.items():
        last = store.latest_row(c, iid, now)
        if not last:
            inst_status[iid] = {"status": "unknown", "alerts": [], "grade": None, "score": None,
                                "heap_after_pct": None, "max_pause_ms": None, "full_gc": 0}
            continue
        day = store.window_rows(c, iid, now - 86400, now)
        if not day and last:
            day = [last]
        from . import analyzer
        m = store._metrics_dict_from_window(day, inst["heap_max_mb"])
        health = analyzer.score_health(m)
        alerts = store.evaluate_alerts(c, iid, now)
        inst_status[iid] = {
            "status": _instance_status(health, alerts),
            "alerts": alerts,
            "grade": health["grade"],
            "score": health["score"],
            "heap_after_pct": last["heap_after_pct"],
            "max_pause_ms": last["pause_max_ms"],
            "full_gc_1h": sum(r["full_gc_count"] for r in store.window_rows(c, iid, now - 3600, now)),
        }

    # Assemble the tree from the instances actually in the store, layering
    # status on top — so live clusters render, not just the demo topology.
    skeleton = _skeleton_from_store(insts)
    regions = []
    fleet_status = "unknown"
    fleet_counts = {"ok": 0, "watch": 0, "critical": 0, "unknown": 0}

    region_keys = sorted(skeleton, key=lambda r: (_REGION_ORDER.get(r, len(_REGION_ORDER)), r))
    for region_key in region_keys:
        rnode = skeleton.get(region_key)
        if not rnode:
            continue
        envs = []
        region_status = "unknown"
        for env_key, enode in rnode["envs"].items():
            clusters = []
            env_status = "unknown"
            env_alerts = 0
            
            for ckey, cnode in enode["clusters"].items():
                groups = []
                cluster_status = "unknown"
                cluster_alerts = 0
                for gkey, gnode in cnode["groups"].items():
                    ginsts = []
                    group_status = "unknown"
                    for iid in gnode["instances"]:
                        st = inst_status[iid]
                        group_status = _worse(group_status, st["status"])
                        cluster_alerts += len(st["alerts"])
                        ginsts.append({
                            "id": iid,
                            "node_id": insts[iid].get("node_id") or "",
                            "role": insts[iid]["role"],
                            "status": st["status"],
                            "grade": st["grade"],
                            "score": st["score"],
                            "heap_after_pct": st["heap_after_pct"],
                            "max_pause_ms": st["max_pause_ms"],
                            "full_gc_1h": st.get("full_gc_1h", 0),
                            "alerts": st["alerts"],
                        })
                        fleet_counts[st["status"]] = fleet_counts.get(st["status"], 0) + 1
                    cluster_status = _worse(cluster_status, group_status)
                    groups.append({
                        "group": gkey,
                        "label": topology.GROUP_LABEL.get(gkey, gkey),
                        "status": group_status,
                        "count": len(ginsts),
                        "instances": ginsts,
                    })
                env_status = _worse(env_status, cluster_status)
                env_alerts += cluster_alerts
                clusters.append({
                    "cluster": ckey,
                    "status": cluster_status,
                    "alert_count": cluster_alerts,
                    "groups": groups,
                })
            
            region_status = _worse(region_status, env_status)
            envs.append({
                "env": env_key,
                "status": env_status,
                "alert_count": env_alerts,
                "clusters": clusters,
            })
        fleet_status = _worse(fleet_status, region_status)
        regions.append({"region": region_key, "status": region_status, "envs": envs})

    # Flat list of current critical/warning alerts across the fleet.
    active = []
    for iid, st in inst_status.items():
        for a in st["alerts"]:
            active.append({"instance_id": iid, "cluster": insts[iid]["cluster"],
                           "role": insts[iid]["role"], **a})
    active.sort(key=lambda a: 0 if a["severity"] == "critical" else 1)

    return {
        "now": now,
        "fleet_status": fleet_status,
        "counts": fleet_counts,
        "total_instances": len(insts),
        "active_alerts": active,
        "regions": regions,
    }


def build_cluster(c, cluster: str, now: int = None) -> dict | None:
    """One-picture overview for a single cluster (region-env).

    Counts healthy vs unhealthy, aggregate memory, GC engine + config telemetry,
    every node's current GC health, and a focused "needs attention" list.
    """
    from . import analyzer

    now = now or store.now_ts(c)
    insts = [i for i in store.list_instances(c) if i["cluster"] == cluster]
    if not insts:
        return None

    nodes = []
    healthy = unhealthy = 0
    total_heap = total_used = 0.0
    util_vals, thru_vals = [], []
    full_1h_total = full_24h_total = 0
    worst_pause = 0.0
    collectors = set()
    heap_by_role: dict[str, set] = {}
    status_counts = {"ok": 0, "watch": 0, "critical": 0, "unknown": 0}

    for inst in insts:
        last = store.latest_row(c, inst["id"], now)
        day = store.window_rows(c, inst["id"], now - 86400, now)
        if not day and last:
            day = [last]
        m = store._metrics_dict_from_window(day, inst["heap_max_mb"])
        health = analyzer.score_health(m) if m else None
        alerts = store.evaluate_alerts(c, inst["id"], now)
        status = _instance_status(health, alerts)
        status_counts[status] += 1
        if status in ("critical", "watch"):
            unhealthy += 1
        elif status == "ok":
            healthy += 1

        collectors.add(inst["collector"])
        total_heap += inst["heap_max_mb"]
        heap_by_role.setdefault(inst["role"], set()).add(inst["heap_max_mb"])
        full_1h = sum(r["full_gc_count"] for r in store.window_rows(c, inst["id"], now - 3600, now))
        full_24h = int(sum(r["full_gc_count"] for r in day))
        full_1h_total += full_1h
        full_24h_total += full_24h
        if last:
            total_used += last["heap_used_mb"]
            util_vals.append(last["heap_after_pct"])
            worst_pause = max(worst_pause, last["pause_max_ms"])
        if m:
            thru_vals.append(m["throughput_pct"])

        nodes.append({
            "id": inst["id"],
            "role": inst["role"],
            "group": inst["grp"],
            "status": status,
            "grade": health["grade"] if health else None,
            "score": health["score"] if health else None,
            "heap_after_pct": last["heap_after_pct"] if last else None,
            "heap_max_mb": inst["heap_max_mb"],
            "max_pause_ms": last["pause_max_ms"] if last else None,
            "full_gc_1h": full_1h,
            "alerts": alerts,
            "reason": (health["reasons"][0] if health and health["reasons"] else
                       (alerts[0]["msg"] if alerts else "")),
        })

    nodes.sort(key=lambda n: (-STATUS_RANK[n["status"]], n["score"] if n["score"] is not None else 999))
    attention = [n for n in nodes if n["status"] in ("critical", "watch")]

    # Region/env come from the stored instance rows (onboarding writes them).
    # Do NOT parse the cluster name — names like "demo" have no "-" and would crash.
    region = insts[0]["region"]
    env = insts[0]["env"]
    cluster_status = "ok"
    for n in nodes:
        cluster_status = _worse(cluster_status, n["status"])

    memory = {
        "total_heap_mb": round(total_heap, 0),
        "used_mb": round(total_used, 0),
        "used_pct": round(total_used / total_heap * 100, 1) if total_heap else 0.0,
        "avg_util_pct": round(statistics.fmean(util_vals), 1) if util_vals else 0.0,
        "peak_util_pct": round(max(util_vals), 1) if util_vals else 0.0,
    }
    telemetry = {
        "avg_throughput_pct": round(statistics.fmean(thru_vals), 2) if thru_vals else 0.0,
        "full_gc_1h": full_1h_total,
        "full_gc_24h": full_24h_total,
        "worst_pause_ms": round(worst_pause, 0),
    }
    config = {
        "gc_engine": sorted(collectors),
        "log_format": "Unified (-Xlog:gc*), Java 11+",
        "pause_target_ms": analyzer.MAX_HEALTHY_PAUSE_MS,
        "heap_by_role": {r: sorted(v) for r, v in sorted(heap_by_role.items())},
    }

    return {
        "cluster": cluster,
        "region": region,
        "env": env,
        "status": cluster_status,
        "now": now,
        "counts": {"healthy": healthy, "unhealthy": unhealthy, "total": len(insts), **status_counts},
        "memory": memory,
        "telemetry": telemetry,
        "config": config,
        "nodes": nodes,
        "attention": attention,
    }
