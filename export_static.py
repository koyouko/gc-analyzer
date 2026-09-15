"""
Render a fully self-contained static dashboard (no server, no DB, no login).

Reads the history store, precomputes the fleet rollup, every cluster overview,
every instance snapshot, and a trend series per instance, then inlines it all
into one HTML file. It shims both `api()` and `fetch()` so the dashboard runs
entirely offline, and auto-authenticates so the login gate is bypassed.

Use for sharing a frozen snapshot / design preview. The live server
(`python -m gcanalyzer.app`) is what you run against a real history store.
"""

from __future__ import annotations

import argparse
import json
import os
import re

from gcanalyzer import store, fleet, correlate, scaling_advisor, forecast, ml_insights

HERE = os.path.dirname(os.path.abspath(__file__))

# Keep in sync with gcanalyzer.app._RANGES / _bucket_for
_RANGES = {
    "1h": 3600, "3h": 3 * 3600, "6h": 6 * 3600, "12h": 12 * 3600,
    "24h": 86400, "2d": 2 * 86400, "7d": 7 * 86400, "30d": 30 * 86400,
    "90d": 90 * 86400, "1y": 365 * 86400, "2y": 730 * 86400,
}


def _bucket_for(window_s: int) -> int:
    if window_s <= 6 * 3600:
        return 300
    if window_s <= 86400:
        return 900
    if window_s <= 7 * 86400:
        return 3600
    if window_s <= 90 * 86400:
        return 6 * 3600
    return 86400


def build(out: str, chartjs: str | None = None) -> None:
    with store.connect() as c:
        if not store.list_instances(c):
            raise SystemExit("History store empty. Run: python -m seed.seed_history")
        now = store.now_ts(c)
        f = fleet.build_fleet(c)
        instances, series, clusters = {}, {}, {}
        routes = {}
        for inst in store.list_instances(c):
            iid = inst["id"]
            instances[iid] = store.current_snapshot(c, iid)
            routes[f"/api/instance/{iid}/sar"] = store.current_host_snapshot(c, iid, now)
            routes[f"/api/instance/{iid}/correlation"] = correlate.correlate_instance(c, iid, now=now)
            routes[f"/api/instance/{iid}/anomalies"] = ml_insights.analyze_anomalies(c, iid, now=now)
            routes[f"/api/instance/{iid}/forecast"] = forecast.forecast_instance(c, iid, now=now)
            by_range = {}
            for range_key, window in _RANGES.items():
                s = store.range_series(c, iid, now - window, now, _bucket_for(window))
                s["range"] = range_key
                by_range[range_key] = s
                routes[f"/api/instance/{iid}/sar/series?range={range_key}"] = store.host_range_series(
                    c, iid, now - window, now, _bucket_for(window))
            series[iid] = by_range
            clusters.setdefault(inst["cluster"], None)
        for cl in list(clusters):
            clusters[cl] = fleet.build_cluster(c, cl)
            routes[f"/api/cluster/{cl}/scaling"] = scaling_advisor.analyze_cluster_scaling(c, cl, now=now)
            routes[f"/api/cluster/{cl}/forecast"] = forecast.forecast_cluster(c, cl, now=now)
        cluster_names = sorted(clusters)

    data = {
        "fleet": f, "instances": instances, "series": series,
        "clusters": clusters, "cluster_names": cluster_names, "routes": routes,
    }
    html = open(os.path.join(HERE, "frontend", "index.html")).read()

    # Inline data + shim api() and fetch() for every /api/* path, and auto-login.
    inject = (
        "<script>window.__STATIC__=" + json.dumps(data, allow_nan=False).replace("<", "\\u003c") + ";</script>\n<script>\n"
        "function __route(p){\n"
        "  const path=p.split('?')[0];\n"
        "  const decoded=decodeURIComponent(p);\n"
        "  if(window.__STATIC__.routes[decoded]) return window.__STATIC__.routes[decoded];\n"
        "  if(window.__STATIC__.routes[decodeURIComponent(path)]) return window.__STATIC__.routes[decodeURIComponent(path)];\n"
        "  if(path==='/api/me') return {user:'readonly',role:'readonly'};\n"
        "  if(path==='/api/health') return {ok:true};\n"
        "  if(path==='/api/fleet') return window.__STATIC__.fleet;\n"
        "  if(path==='/api/clusters') return {clusters:window.__STATIC__.cluster_names};\n"
        "  if(path==='/api/jobs') return {jobs:[]};\n"
        "  if(path==='/api/login'||path==='/api/logout') return {ok:true};\n"
        "  let m=path.match(/\\/api\\/cluster\\/([^/]+)$/);\n"
        "  if(m){const v=window.__STATIC__.clusters[decodeURIComponent(m[1])]; if(!v) throw new Error('not found'); return v;}\n"
        "  m=path.match(/\\/api\\/clusters\\/([^/]+)\\/config$/);\n"
        "  if(m) return {cluster:decodeURIComponent(m[1]),config:'# config not stored in static preview'};\n"
        "  m=path.match(/\\/api\\/instance\\/([^/]+)\\/series$/);\n"
        "  if(m){\n"
        "    const iid=decodeURIComponent(m[1]);\n"
        "    const range=new URLSearchParams(p.includes('?')?p.split('?')[1]:'').get('range')||'24h';\n"
        "    const byRange=window.__STATIC__.series[iid];\n"
        "    if(!byRange) throw new Error('not found');\n"
        "    const s=byRange[range];\n"
        "    if(!s) throw new Error('unknown range '+range);\n"
        "    return s;\n"
        "  }\n"
        "  m=path.match(/\\/api\\/instance\\/([^/]+)$/);\n"
        "  if(m){const s=window.__STATIC__.instances[decodeURIComponent(m[1])]; if(!s) throw new Error('not found'); return s;}\n"
        "  throw new Error('unknown path '+p);\n"
        "}\n"
        "async function api(p){return __route(p);}\n"
        "const __realFetch=window.fetch;\n"
        "window.fetch=async function(p,opts){\n"
        "  if(typeof p==='string' && p.startsWith('/api/')){\n"
        "    const method=(opts&&opts.method)||'GET';\n"
        "    const path=p.split('?')[0];\n"
        "    if(method!=='GET'&&method!=='HEAD'&&path!=='/api/login'&&path!=='/api/logout'){\n"
        "      return {ok:false,status:405,json:async()=>({detail:'Read-only static preview'}),text:async()=>'Read-only static preview'};\n"
        "    }\n"
        "    try{const d=__route(p);return {ok:true,status:200,json:async()=>d,text:async()=>JSON.stringify(d)};}\n"
        "    catch(e){return {ok:false,status:404,json:async()=>({detail:e.message}),text:async()=>e.message};}\n"
        "  }\n"
        "  return __realFetch(p,opts);\n"
        "};\n"
        "document.addEventListener('DOMContentLoaded',()=>document.body.classList.add('authenticated'));\n"
    )

    # Replace the real api() one-liner, then inject our shim before the app script.
    html = re.sub(r"async function api\(p\)\{[^\n]*\}", "/* api() replaced by static shim */", html, count=1)
    html = html.replace("<script>\nconst SC=", inject + "const SC=")

    # Bundle only known frontend assets; reports must not depend on the live server.
    def inline_script(match):
        path = match.group(1)
        if path == "investigations.js":
            return '<script>function openInvestigation(){alert("Log analysis requires the running app.");}</script>'
        if path == "vendor/chart.umd.js" and chartjs:
            return chartjs
        if path not in ("vendor/chart.umd.js", "dashboard.js", "prometheus-settings.js"):
            raise ValueError(f"Unrecognized export asset: {path}")
        with open(os.path.join(HERE, "frontend", path)) as asset:
            return "<script>" + asset.read().replace("</script", "<\\/script") + "</script>"

    html = re.sub(r'<script src="/assets/([^"]+)"[^>]*></script>', inline_script, html)
    def inline_style(match):
        path = match.group(1)
        if path not in ("dashboard.css", "investigations.css", "prometheus-settings.css"):
            raise ValueError(f"Unrecognized export stylesheet: {path}")
        with open(os.path.join(HERE, "frontend", path)) as asset:
            return "<style>" + asset.read() + "</style>"
    html = re.sub(r'<link rel="stylesheet" href="/assets/([^"]+)"[^>]*>', inline_style, html)

    with open(out, "w") as fh:
        fh.write(html)
    print(f"wrote {out} ({len(html)//1024} KB), {len(instances)} instances, {len(cluster_names)} clusters")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(HERE, "dashboard-preview.html"))
    ap.add_argument("--cowork", action="store_true",
                    help="emit a Cowork-artifact variant using the jsdelivr Chart.js CDN")
    args = ap.parse_args()
    cdn = ('<script src="https://cdn.jsdelivr.net/npm/chart.js@4.5.0/dist/chart.umd.js" '
           'integrity="sha384-iU8HYtnGQ8Cy4zl7gbNMOhsDTTKX02BTXptVP/vqAWIaTfM7isw76iyZCsjL2eVi" '
           'crossorigin="anonymous"></script>') if args.cowork else None
    build(args.out, chartjs=cdn)
