from gcanalyzer import analyzer, parser, service
from gcanalyzer.collector import NodeConfig


def test_legacy_service_summary_handles_unknown_analysis():
    node = NodeConfig(id="x", role="broker", source="local")
    state = service.ClusterState("test", [node])
    state.analyses[node.id] = analyzer.analyze(parser.parse("not a log", node.id))
    assert state.cluster_summary()["avg_health_score"] is None
