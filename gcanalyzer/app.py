"""
FastAPI application: fleet GC dashboard + REST API.

Run:
    python -m seed.seed_history          # one-time: build 30 days of demo history
    python -m gcanalyzer.app             # serve dashboard at http://127.0.0.1:8000

The dashboard navigates Region -> Environment -> Cluster -> Component, shows
"right now" health with last-hour alert color coding, and 30-day per-instance
trends. Data is read from the SQLite history store (gcanalyzer/store.py).

Endpoints:
    GET  /                               -> dashboard (single page)
    GET  /api/fleet                      -> topology tree + rollup health + last-hour alerts
    GET  /api/cluster/{cluster}          -> one-cluster overview: counts, memory, config, nodes
    GET  /api/instance/{id}              -> current snapshot, health, alerts, findings
    GET  /api/instance/{id}/trends?days=30 -> daily-aggregated trend series
    GET  /api/instance/{id}/recent?hours=48 -> fine-grained recent series
    GET  /api/health                     -> liveness probe
"""

from __future__ import annotations

import argparse
import os
import time
import uuid

import asyncio

from .env import load_dotenv

load_dotenv()

from fastapi import FastAPI, HTTPException, Request, Response, Cookie
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from . import store, fleet, ingest as ingest_mod, config as config_mod, auth, scheduler

HERE = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(os.path.dirname(HERE), "frontend")
CLUSTERS_DIR = os.path.join(os.path.dirname(HERE), "clusters")

app = FastAPI(title="BSP Kafka GC Analyzer", version="2.1.0")

# Endpoints reachable without a session. Everything else under /api requires one;
# /api/clusters mutations (POST/DELETE) additionally require the 'admin' role.
_PUBLIC_API = {"/api/login", "/api/logout", "/api/health", "/api/me"}

RUNNING_CLUSTERS: set[str] = set()


@app.middleware("http")
async def _auth_guard(request: Request, call_next):
    path = request.url.path
    if path.startswith("/api/") and path not in _PUBLIC_API:
        sess = auth.read_session(request.cookies.get("gc_session"))
        if not sess:
            return JSONResponse({"detail": "login required"}, status_code=401)
        admin_write = (
            path.startswith("/api/clusters") and request.method in ("POST", "DELETE", "PUT")
        ) or (
            path.startswith("/api/jobs/") and request.method == "POST" and path.endswith("/cancel")
        )
        if admin_write and sess["role"] != "admin":
            return JSONResponse({"detail": "admin role required"}, status_code=403)
        request.state.user = sess
    return await call_next(request)


@app.on_event("startup")
async def _start_scheduler():
    _sync_cluster_inventory_from_configs()
    if os.environ.get("GC_SCHED_ENABLED", "1") != "0":
        interval = int(os.environ.get("GC_SCHED_INTERVAL", "300"))
        
        def on_tick_start(ts, cluster, region, env, node_count=0):
            job_id = f"sched-{cluster}-{ts}"
            _prune_old_jobs()
            RUNNING_CLUSTERS.add(cluster)
            JOBS[job_id] = {
                "id": job_id,
                "cluster": f"Scheduler: {cluster}",
                "region": region,
                "env": env,
                "status": "running",
                "progress": 10,
                "message": f"Running periodic GC scraping for cluster {cluster}...",
                "created_at": ts,
                "completed_at": None,
                "error": None,
                "nodes": [],
                "nodes_total": 0,
                "cancel_requested": False,
                "logs": [f"[{time.strftime('%H:%M:%S')}] Started scheduler re-collection tick for cluster {cluster}."],
                "node_logs": {"_general": [f"[{time.strftime('%H:%M:%S')}] Started scheduler re-collection tick for cluster {cluster}."]}
            }
            
        def on_node_result(cluster, msg, success, node_id=None):
            if cluster:
                prefix = f"sched-{cluster}-"
                sched_jobs = [jid for jid in JOBS if jid.startswith(prefix)]
            else:
                sched_jobs = [jid for jid in JOBS if jid.startswith("sched-")]
            
            if not sched_jobs:
                return
            job_id = max(sched_jobs)
            timestamp = time.strftime("%H:%M:%S")
            JOBS[job_id]["logs"].append(f"[{timestamp}] {msg}")
            
            if "node_logs" not in JOBS[job_id]:
                JOBS[job_id]["node_logs"] = {"_general": []}
            nid = node_id if node_id else "_general"
            if nid not in JOBS[job_id]["node_logs"]:
                JOBS[job_id]["node_logs"][nid] = []
            JOBS[job_id]["node_logs"][nid].append(f"[{timestamp}] {msg}")
            
            if node_id:
                if JOBS[job_id].get("nodes") is None:
                    JOBS[job_id]["nodes"] = []
                JOBS[job_id]["nodes"].append({
                    "node_id": f"{cluster}/{node_id}" if cluster else node_id,
                    "instance_id": "-",
                    "role": "node",
                    "recorded": success,
                    "detail": msg
                })
                total = JOBS[job_id].get("nodes_total") or len(JOBS[job_id]["nodes"])
                done = len(JOBS[job_id]["nodes"])
                JOBS[job_id]["progress"] = min(99, 10 + int(90 * done / max(total, 1)))
                
        def on_tick_complete(ts, cluster, summary):
            job_id = f"sched-{cluster}-{ts}"
            RUNNING_CLUSTERS.discard(cluster)
            if job_id not in JOBS:
                return
            
            has_failures = any(not n["recorded"] for n in (JOBS[job_id]["nodes"] or []))
            
            if has_failures:
                JOBS[job_id]["status"] = "warning"
                JOBS[job_id]["message"] = f"Scheduler tick completed with errors. Scraped {summary['points']} points."
            else:
                JOBS[job_id]["status"] = "completed"
                JOBS[job_id]["message"] = f"Scheduler tick completed. Scraped {summary['points']} points."
                
            JOBS[job_id]["progress"] = 100
            JOBS[job_id]["completed_at"] = time.time()
            JOBS[job_id]["logs"].append(f"[{time.strftime('%H:%M:%S')}] Completed. Summary: {summary}")
            if "node_logs" not in JOBS[job_id]:
                JOBS[job_id]["node_logs"] = {"_general": []}
            JOBS[job_id]["node_logs"]["_general"].append(f"[{time.strftime('%H:%M:%S')}] Completed. Summary: {summary}")

        asyncio.create_task(
            scheduler.scheduler_loop(
                store.DB_PATH,
                interval,
                on_tick_start=on_tick_start,
                on_node_result=on_node_result,
                on_tick_complete=on_tick_complete,
                is_running_cb=lambda c: c in RUNNING_CLUSTERS
            )
        )


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    with open(os.path.join(STATIC_DIR, "index.html")) as fh:
        return fh.read()


@app.get("/api/health")
def health() -> dict:
    try:
        with store.connect() as c:
            n = len(store.list_instances(c))
        return {"ok": True, "instances": n}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": str(exc)}


@app.get("/api/fleet")
def get_fleet() -> dict:
    with store.connect() as c:
        if not store.list_instances(c):
            raise HTTPException(503, "History store is empty. Run: python -m seed.seed_history")
        return fleet.build_fleet(c)


@app.get("/api/cluster/{cluster}")
def get_cluster(cluster: str) -> dict:
    with store.connect() as c:
        view = fleet.build_cluster(c, cluster)
    if not view:
        raise HTTPException(404, f"Unknown cluster: {cluster}")
    return view


@app.get("/api/instance/{instance_id}")
def get_instance(instance_id: str) -> dict:
    with store.connect() as c:
        snap = store.current_snapshot(c, instance_id)
    if not snap:
        raise HTTPException(404, f"Unknown instance: {instance_id}")
    return snap


@app.get("/api/instance/{instance_id}/trends")
def get_trends(instance_id: str, days: int = 30) -> dict:
    with store.connect() as c:
        if not store.get_instance(c, instance_id):
            raise HTTPException(404, f"Unknown instance: {instance_id}")
        return store.trends(c, instance_id, days=days)


@app.get("/api/instance/{instance_id}/recent")
def get_recent(instance_id: str, hours: int = 48) -> dict:
    with store.connect() as c:
        if not store.get_instance(c, instance_id):
            raise HTTPException(404, f"Unknown instance: {instance_id}")
        return {"instance_id": instance_id, "series": store.hourly_series(c, instance_id, hours=hours)}


# --------------------------------------------------------------------------- #
# Cluster onboarding: paste/upload a cluster.yaml from the dashboard, register
# its nodes (brokers, ZooKeeper, schema-registry, connect), and run an initial
# collection into the same history store the dashboard reads.
# --------------------------------------------------------------------------- #
class OnboardRequest(BaseModel):
    config: str                       # cluster.yaml (or JSON) text
    format: str = "yaml"              # "yaml" | "json"
    region: str | None = None         # optional override; else derived from cluster name
    env: str | None = None
    is_edit: bool = False
    collect_mode: str = "auto"  # auto | full | incremental


def _derive_identity(cluster_name: str, region: str | None, env: str | None) -> tuple[str, str, str]:
    """Region/env from explicit overrides, else split a REGION-ENV cluster name."""
    parts = cluster_name.split("-", 1)
    r = region or parts[0]
    e = env or (parts[1] if len(parts) > 1 else "PROD")
    return r, e, cluster_name


def _persist_cluster_config(cluster: str, text: str) -> str:
    """Save the submitted config so a scheduled job can re-collect later."""
    os.makedirs(CLUSTERS_DIR, exist_ok=True)
    safe = "".join(ch for ch in cluster if ch.isalnum() or ch in "-_") or "cluster"
    path = os.path.join(CLUSTERS_DIR, safe + ".yaml")
    with open(path, "w") as fh:
        fh.write(text)
    return path


JOBS: dict[str, dict] = {}




def _set_job_progress(job_id: str, pct: int, message: str | None = None, node_entry: dict | None = None) -> None:
    job = JOBS.get(job_id)
    if not job:
        return
    job["progress"] = min(99, max(int(job.get("progress", 0)), int(pct)))
    if message:
        if job.get("cancel_requested") and job.get("status") == "running":
            job["message"] = f"Cancellation requested… ({message})"
        else:
            job["message"] = message
    if node_entry:
        if job.get("nodes") is None:
            job["nodes"] = []
        job["nodes"].append(node_entry)

def _job_cancelled(job_id: str) -> bool:
    return bool(JOBS.get(job_id, {}).get("cancel_requested"))


def _finish_cancelled_job(job_id: str, cluster: str, log_cb=None) -> None:
    JOBS[job_id]["status"] = "cancelled"
    JOBS[job_id]["progress"] = 100
    JOBS[job_id]["completed_at"] = time.time()
    JOBS[job_id]["message"] = "Job cancelled by user."
    if log_cb:
        log_cb("Job cancelled by user.")


def _parse_cluster_config(config: str, fmt: str = "yaml", expected_cluster: str | None = None):
    try:
        cluster_name, nodes, cfg_region, cfg_env = config_mod.load_cluster_text(config, fmt)
    except Exception as exc:
        raise HTTPException(400, f"Invalid cluster config: {exc}") from exc
    if not nodes:
        raise HTTPException(400, "Config defines no nodes.")
    derived_region, derived_env, cluster = _derive_identity(cluster_name, None, None)
    region = cfg_region or derived_region
    env = cfg_env or derived_env
    if expected_cluster and cluster != expected_cluster:
        raise HTTPException(
            400,
            f"Config cluster name '{cluster}' must match '{expected_cluster}'.",
        )
    return cluster, nodes, region, env




def _safe_cluster_slug(cluster: str) -> str:
    return "".join(ch for ch in cluster if ch.isalnum() or ch in "-_") or "cluster"


def _onboarded_cluster_names() -> set[str]:
    """Cluster names declared in clusters/*.yaml (the fleet inventory source of truth)."""
    names: set[str] = set()
    if not os.path.isdir(CLUSTERS_DIR):
        return names
    for fname in os.listdir(CLUSTERS_DIR):
        if not fname.endswith(".yaml"):
            continue
        path = os.path.join(CLUSTERS_DIR, fname)
        try:
            cluster_name, _, _, _ = config_mod.load_cluster(path)
            names.add(cluster_name)
        except Exception:
            pass
    return names


def _cluster_delete_names(cluster: str) -> set[str]:
    """All cluster name variants that may exist in the instances table."""
    names = {cluster, _safe_cluster_slug(cluster)}
    if os.path.isdir(CLUSTERS_DIR):
        for fname in os.listdir(CLUSTERS_DIR):
            if not fname.endswith(".yaml"):
                continue
            stem = fname[:-5]
            if stem not in (cluster, _safe_cluster_slug(cluster)):
                continue
            path = os.path.join(CLUSTERS_DIR, fname)
            try:
                cluster_name, _, _, _ = config_mod.load_cluster(path)
                names.add(cluster_name)
                names.add(stem)
            except Exception:
                names.add(stem)
    return names


def _sync_cluster_inventory_from_configs() -> None:
    """Ensure every onboarded clusters/*.yaml appears in the fleet tree."""
    if not os.path.isdir(CLUSTERS_DIR):
        return
    with store.connect() as c:
        for fname in sorted(os.listdir(CLUSTERS_DIR)):
            if not fname.endswith(".yaml"):
                continue
            path = os.path.join(CLUSTERS_DIR, fname)
            try:
                cluster_name, nodes, cfg_region, cfg_env = config_mod.load_cluster(path)
            except Exception:
                continue
            derived_region, derived_env, cluster = _derive_identity(
                cluster_name, cfg_region, cfg_env
            )
            region = cfg_region or derived_region
            env = cfg_env or derived_env
            ingest_mod.sync_instances_from_nodes(c, nodes, region, env, cluster)
        store.prune_orphan_clusters(c, _onboarded_cluster_names())


def _prune_old_jobs() -> None:
    now = time.time()
    one_week_ago = now - 7 * 86400  # 7 days in seconds
    to_remove = [jid for jid, job in JOBS.items() if job.get("created_at", 0) < one_week_ago]
    for jid in to_remove:
        JOBS.pop(jid, None)


async def run_onboard_job(job_id: str, nodes: list, db_path: str, region: str, env: str, cluster: str, collect_mode: str = "auto"):
    if _job_cancelled(job_id):
        _finish_cancelled_job(job_id, cluster)
        return

    def log_cb(msg: str, node_id: str = "_general"):
        timestamp = time.strftime("%H:%M:%S")
        if "node_logs" not in JOBS[job_id]:
            JOBS[job_id]["node_logs"] = {"_general": []}
        if node_id not in JOBS[job_id]["node_logs"]:
            JOBS[job_id]["node_logs"][node_id] = []
        JOBS[job_id]["node_logs"][node_id].append(f"[{timestamp}] {msg}")

        if "logs" not in JOBS[job_id]:
            JOBS[job_id]["logs"] = []
        JOBS[job_id]["logs"].append(f"[{timestamp}] {msg}")

    RUNNING_CLUSTERS.add(cluster)
    JOBS[job_id]["status"] = "running"
    JOBS[job_id]["progress"] = 8
    JOBS[job_id]["message"] = f"Connecting to {len(nodes)} nodes to collect and parse GC logs..."
    log_cb(f"Starting onboarding job for cluster '{cluster}'...")
    log_cb(f"Environment: {env}, Region: {region}")
    log_cb(f"Targeting {len(nodes)} node(s).")
    
    try:
        if _job_cancelled(job_id):
            _finish_cancelled_job(job_id, cluster, log_cb)
            return

        results = await asyncio.to_thread(
            ingest_mod.ingest_nodes,
            nodes,
            db_path,
            region=region,
            env=env,
            cluster=cluster,
            log_callback=log_cb,
            cancel_check=lambda: _job_cancelled(job_id),
            collect_mode=collect_mode,
            progress_callback=lambda pct, msg, node=None: _set_job_progress(job_id, pct, msg, node),
        )

        if _job_cancelled(job_id):
            _finish_cancelled_job(job_id, cluster, log_cb)
            return
        
        nodes_recorded = sum(1 for r in results if r.recorded)
        if nodes_recorded == 0:
            JOBS[job_id]["status"] = "failed"
            JOBS[job_id]["message"] = f"Failed to onboard: 0/{len(results)} nodes succeeded."
            log_cb(f"Job failed. No nodes succeeded (0/{len(results)} recorded).")
        elif nodes_recorded < len(results):
            JOBS[job_id]["status"] = "warning"
            JOBS[job_id]["message"] = f"Onboarded with warnings: {nodes_recorded}/{len(results)} nodes succeeded."
            log_cb(f"Job completed with warnings. Only {nodes_recorded}/{len(results)} nodes succeeded.")
        else:
            JOBS[job_id]["status"] = "completed"
            JOBS[job_id]["message"] = f"Successfully onboarded {nodes_recorded}/{len(results)} nodes."
            log_cb(f"Job completed successfully. All {nodes_recorded}/{len(results)} nodes succeeded.")

        JOBS[job_id]["progress"] = 100
        JOBS[job_id]["completed_at"] = time.time()
        if not JOBS[job_id].get("nodes"):
            JOBS[job_id]["nodes"] = [
                {
                    "node_id": r.node_id,
                    "instance_id": r.instance_id,
                    "role": r.role,
                    "recorded": r.recorded,
                    "detail": r.detail,
                }
                for r in results
            ]
    except ingest_mod.JobCancelled:
        _finish_cancelled_job(job_id, cluster, log_cb)
    except Exception as e:
        JOBS[job_id]["status"] = "failed"
        JOBS[job_id]["progress"] = 100
        JOBS[job_id]["completed_at"] = time.time()
        JOBS[job_id]["error"] = str(e)
        JOBS[job_id]["message"] = f"Failed: {e}"
        log_cb(f"Job failed with error: {e}")
    finally:
        RUNNING_CLUSTERS.discard(cluster)


@app.post("/api/clusters")
async def onboard_cluster(req: OnboardRequest) -> dict:
    cluster, nodes, region, env = _parse_cluster_config(req.config, req.format)
    if (req.region or "").strip():
        region = req.region.strip()
    if (req.env or "").strip():
        env = req.env.strip()

    if cluster in RUNNING_CLUSTERS:
        raise HTTPException(409, f"A job is already running for cluster '{cluster}'. Please try again later.")

    if not req.is_edit:
        safe = "".join(ch for ch in cluster if ch.isalnum() or ch in "-_") or "cluster"
        config_path = os.path.join(CLUSTERS_DIR, safe + ".yaml")
        if os.path.exists(config_path):
            raise HTTPException(400, f"Cluster '{cluster}' is already onboarded.")

    config_path = _persist_cluster_config(cluster, req.config)

    with store.connect() as c:
        synced = ingest_mod.sync_instances_from_nodes(c, nodes, region, env, cluster)

    job_id = str(uuid.uuid4())
    JOBS[job_id] = {
        "id": job_id,
        "cluster": cluster,
        "region": region,
        "env": env,
        "status": "pending",
        "progress": 5,
        "message": "Queued collection job...",
        "created_at": time.time(),
        "completed_at": None,
        "error": None,
        "nodes": [],
        "nodes_total": len(nodes),
        "cancel_requested": False,
        "logs": [],
        "node_logs": {"_general": []}
    }

    # Start the job in the background using asyncio.create_task
    asyncio.create_task(run_onboard_job(job_id, nodes, store.DB_PATH, region, env, cluster, req.collect_mode))

    return {
        "job_id": job_id,
        "cluster": cluster,
        "region": region,
        "env": env,
        "status": "pending",
        "nodes_synced": synced,
    }


@app.get("/api/jobs")
def list_jobs() -> dict:
    _prune_old_jobs()
    # Return jobs sorted by created_at descending
    sorted_jobs = sorted(JOBS.values(), key=lambda x: x["created_at"], reverse=True)
    return {"jobs": sorted_jobs}


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str) -> dict:
    if job_id not in JOBS:
        raise HTTPException(404, f"Job {job_id} not found.")
    return JOBS[job_id]


class ClusterConfigBody(BaseModel):
    config: str
    format: str = "yaml"


@app.put("/api/clusters/{cluster}/config")
def save_cluster_config(cluster: str, req: ClusterConfigBody) -> dict:
    """Persist cluster.yaml and sync instance metadata without starting collection."""
    safe = "".join(ch for ch in cluster if ch.isalnum() or ch in "-_") or "cluster"
    path = os.path.join(CLUSTERS_DIR, safe + ".yaml")
    if not os.path.exists(path):
        raise HTTPException(404, f"Config for cluster {cluster} not found.")

    _, nodes, region, env = _parse_cluster_config(req.config, req.format, expected_cluster=cluster)
    _persist_cluster_config(cluster, req.config)
    with store.connect() as c:
        synced = ingest_mod.sync_instances_from_nodes(c, nodes, region, env, cluster)
    return {"cluster": cluster, "region": region, "env": env, "nodes_synced": synced}





@app.post("/api/jobs/{job_id}/cancel")
def cancel_job(job_id: str) -> dict:
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(404, f"Job {job_id} not found.")
    if job["status"] not in ("pending", "running"):
        raise HTTPException(409, f"Job is already {job['status']} and cannot be cancelled.")
    job["cancel_requested"] = True
    if job["status"] == "pending":
        _finish_cancelled_job(job_id, job.get("cluster", ""))
    else:
        job["message"] = "Cancellation requested…"
    return {"job_id": job_id, "status": job["status"]}


@app.get("/api/clusters")
def list_clusters() -> dict:
    if not os.path.isdir(CLUSTERS_DIR):
        return {"clusters": []}
    clusters = []
    for fname in sorted(os.listdir(CLUSTERS_DIR)):
        if not fname.endswith(".yaml"):
            continue
        stem = fname[:-5]
        path = os.path.join(CLUSTERS_DIR, fname)
        try:
            cluster_name, _, _, _ = config_mod.load_cluster(path)
            clusters.append({"key": stem, "name": cluster_name})
        except Exception:
            clusters.append({"key": stem, "name": stem})
    return {"clusters": clusters}


@app.get("/api/clusters/{cluster}/config")
def get_cluster_config(cluster: str) -> dict:
    safe = "".join(ch for ch in cluster if ch.isalnum() or ch in "-_")
    path = os.path.join(CLUSTERS_DIR, safe + ".yaml")
    if not os.path.exists(path):
        raise HTTPException(404, f"Config for cluster {cluster} not found.")
    try:
        with open(path, "r") as fh:
            text = fh.read()
    except Exception as e:
        raise HTTPException(500, f"Failed to read config: {e}")
    return {"cluster": cluster, "config": text}


@app.delete("/api/clusters/{cluster}")
def remove_cluster(cluster: str) -> dict:
    """Admin-only (enforced by middleware): drop a cluster's instances, metrics,
    collector offsets, and its persisted config."""
    names = _cluster_delete_names(cluster)
    with store.connect() as c:
        removed = store.delete_clusters(c, names)
        store.prune_orphan_clusters(c, _onboarded_cluster_names())
    safe = _safe_cluster_slug(cluster)
    cfg = os.path.join(CLUSTERS_DIR, safe + ".yaml")
    config_existed = os.path.exists(cfg)
    if config_existed:
        os.remove(cfg)
    if removed == 0 and not config_existed:
        raise HTTPException(404, f"Unknown cluster: {cluster}")
    return {"cluster": cluster, "removed_instances": removed}


# --------------------------------------------------------------------------- #
# Auth: login / logout / me  (session is a signed cookie; see gcanalyzer.auth)
# --------------------------------------------------------------------------- #
class LoginRequest(BaseModel):
    user: str
    password: str


@app.post("/api/login")
def login(req: LoginRequest, response: Response) -> dict:
    u = auth.authenticate(req.user, req.password)
    if not u:
        raise HTTPException(401, "Invalid username or password")
    token = auth.make_session(u["user"], u["role"])
    response.set_cookie("gc_session", token, httponly=True, samesite="lax",
                        max_age=auth.SESSION_TTL_S)
    return {"user": u["user"], "role": u["role"]}


@app.post("/api/logout")
def logout(response: Response) -> dict:
    response.delete_cookie("gc_session")
    return {"ok": True}


@app.get("/api/me")
def me(gc_session: str | None = Cookie(default=None)) -> dict:
    sess = auth.read_session(gc_session)
    if not sess:
        raise HTTPException(401, "Not logged in")
    return sess


# --------------------------------------------------------------------------- #
# Selectable time-range trend series (1h .. 2y), bucketed by range.
# --------------------------------------------------------------------------- #
_RANGES = {"1h": 3600, "3h": 3 * 3600, "6h": 6 * 3600, "12h": 12 * 3600,
           "24h": 86400, "2d": 2 * 86400, "7d": 7 * 86400, "30d": 30 * 86400,
           "90d": 90 * 86400, "1y": 365 * 86400, "2y": 730 * 86400}


def _bucket_for(window_s: int) -> int:
    if window_s <= 6 * 3600:
        return 300            # 5-minute points
    if window_s <= 86400:
        return 900            # 15-minute
    if window_s <= 7 * 86400:
        return 3600           # hourly
    if window_s <= 90 * 86400:
        return 6 * 3600
    return 86400              # daily


@app.get("/api/instance/{instance_id}/series")
def get_range_series(instance_id: str, range: str = "24h") -> dict:
    window = _RANGES.get(range)
    if window is None:
        raise HTTPException(400, f"Unknown range '{range}'. Options: {', '.join(_RANGES)}")
    with store.connect() as c:
        if not store.get_instance(c, instance_id):
            raise HTTPException(404, f"Unknown instance: {instance_id}")
        now = store.now_ts(c)
        data = store.range_series(c, instance_id, now - window, now, _bucket_for(window))
    data["range"] = range
    return data


def main() -> None:
    ap = argparse.ArgumentParser(description="Kafka Fleet GC Analyzer")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--db", help="Path to SQLite history DB (default gc_history.db)")
    args = ap.parse_args()
    if args.db:
        os.environ["GC_DB"] = args.db
        store.DB_PATH = args.db
    import uvicorn
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
