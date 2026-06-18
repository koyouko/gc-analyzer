"""
Render a fully self-contained static dashboard (no server, no DB required).

Reads the history store, precomputes the fleet rollup plus every instance's
snapshot and 30-day trends, inlines it all into one HTML file, and overrides the
dashboard's api() to read from that inlined data. Double-click the output to
preview the whole fleet dashboard offline.

The live server (`python -m gcanalyzer.app`) is what you run against a real,
continuously-updated history store; this is a frozen snapshot for sharing.
"""

from __future__ import annotations

import argparse
import json
import os
import re

from gcanalyzer import store, fleet

HERE = os.path.dirname(os.path.abspath(__file__))


def build(out: str) -> None:
    with store.connect() as c:
        if not store.list_instances(c):
            raise SystemExit("History store empty. Run: python -m seed.seed_history")
        f = fleet.build_fleet(c)
        instances, trends = {}, {}
        clusters = {}
        for inst in store.list_instances(c):
            iid = inst["id"]
            instances[iid] = store.current_snapshot(c, iid)
            trends[iid] = store.trends(c, iid, days=30)
            clusters.setdefault(inst["cluster"], None)
        for cl in list(clusters):
            clusters[cl] = fleet.build_cluster(c, cl)

    data = {"fleet": f, "instances": instances, "trends": trends, "clusters": clusters}
    html = open(os.path.join(HERE, "frontend", "index.html")).read()

    inject = (
        "<script>window.__STATIC__=" + json.dumps(data) + ";</script>\n<script>\n"
        "async function api(p){\n"
        "  if(p==='/api/fleet') return window.__STATIC__.fleet;\n"
        "  if(p==='/api/health') return {ok:true};\n"
        "  let m=p.match(/\\/api\\/cluster\\/([^/]+)$/);\n"
        "  if(m){const v=window.__STATIC__.clusters[decodeURIComponent(m[1])]; if(!v) throw new Error('not found'); return v;}\n"
        "  m=p.match(/\\/api\\/instance\\/([^/]+)\\/trends/);\n"
        "  if(m) return window.__STATIC__.trends[decodeURIComponent(m[1])];\n"
        "  m=p.match(/\\/api\\/instance\\/([^/]+)$/);\n"
        "  if(m){const s=window.__STATIC__.instances[decodeURIComponent(m[1])]; if(!s) throw new Error('not found'); return s;}\n"
        "  throw new Error('unknown path '+p);\n"
        "}\n"
    )
    html = re.sub(r"async function api\(p\)\{[^\n]*\}", "/* api() overridden */", html, count=1)
    html = html.replace("<script>\nconst SC=", inject + "const SC=")
    html = html.replace('onclick="load()"', 'style="display:none"')

    with open(out, "w") as fh:
        fh.write(html)
    print(f"wrote {out} ({len(html)//1024} KB), {len(instances)} instances inlined")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(HERE, "dashboard-preview.html"))
    args = ap.parse_args()
    build(args.out)
