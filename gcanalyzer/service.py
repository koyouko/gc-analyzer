"""
Orchestration: collect -> parse -> analyze for a whole cluster.

Keeps an in-memory cache of the latest analysis per node so the dashboard is
fast and a single collection run can be reused across API calls.
"""

from __future__ import annotations

import time
from typing import Optional

from . import parser, analyzer
from .collector import NodeConfig, collect


class ClusterState:
    def __init__(self, cluster_name: str, nodes: list[NodeConfig]):
        self.cluster_name = cluster_name
        self.nodes = {n.id: n for n in nodes}
        self.analyses: dict[str, dict] = {}
        self.last_run: Optional[float] = None
        self.errors: dict[str, str] = {}

    def refresh(self) -> None:
        self.analyses.clear()
        self.errors.clear()
        for node in self.nodes.values():
            try:
                logs = collect(node)
                if not logs:
                    self.errors[node.id] = "No GC log files found."
                    continue
                # Concatenate all rotation files for the node, newest content last.
                text = "\n".join(l.text for l in logs)
                parsed = parser.parse(text, node_id=node.id)
                result = analyzer.analyze(parsed)
                result["role"] = node.role
                result["source"] = node.source
                result["files"] = [l.source_detail for l in logs]
                self.analyses[node.id] = result
            except Exception as exc:  # surface collection/parse errors per-node
                self.errors[node.id] = str(exc)
        self.last_run = time.time()

    def cluster_summary(self) -> dict:
        nodes = []
        scores = []
        total_full = 0
        worst_pause = 0.0
        collectors = set()
        for nid, node in self.nodes.items():
            a = self.analyses.get(nid)
            if not a:
                nodes.append({
                    "node_id": nid, "role": node.role,
                    "error": self.errors.get(nid, "not analysed"),
                })
                continue
            m, h = a["metrics"], a["health"]
            scores.append(h["score"])
            total_full += m["full_count"]
            worst_pause = max(worst_pause, m["max_pause_ms"])
            collectors.add(a["collector"])
            nodes.append({
                "node_id": nid,
                "role": node.role,
                "collector": a["collector"],
                "score": h["score"],
                "grade": h["grade"],
                "status": h["status"],
                "throughput_pct": m["throughput_pct"],
                "max_pause_ms": m["max_pause_ms"],
                "p99_pause_ms": m["p99_pause_ms"],
                "full_count": m["full_count"],
                "avg_heap_after_pct": m["avg_heap_after_pct"],
                "peak_heap_after_pct": m["peak_heap_after_pct"],
                "gc_per_min": m["gc_per_min"],
                "hotspot_count": sum(1 for w in a["hotspots"] if w.get("is_hotspot")),
            })
        avg_score = round(sum(scores) / len(scores), 1) if scores else 0.0
        return {
            "cluster_name": self.cluster_name,
            "last_run": self.last_run,
            "node_count": len(self.nodes),
            "analysed_count": len(self.analyses),
            "avg_health_score": avg_score,
            "total_full_gcs": total_full,
            "worst_pause_ms": round(worst_pause, 1),
            "collectors": sorted(collectors),
            "nodes": nodes,
            "errors": self.errors,
        }
