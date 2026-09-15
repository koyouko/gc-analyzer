"""GC correctness checks using captured-format logs and local/fake I/O only."""

import gzip
import importlib
import io
import sys
from types import SimpleNamespace

import pytest

from gcanalyzer import analyzer, collector, forecast, ingest, parser, scaling_advisor, scheduler, store


def pause(seq=7, uptime=10, ms=18.2, phase="Remark", heap="", stamp=""):
    date = f"[{stamp}]" if stamp else ""
    return f"{date}[{uptime:.3f}s][info][gc] GC({seq}) Pause {phase} {heap}{ms:.3f}ms\n"


def young(seq=1, uptime=10, ms=100, stamp=""):
    return pause(seq, uptime, ms, "Young (Normal) (G1 Evacuation Pause)",
                 "512M->128M(1024M) ", stamp)


def test_uptime_only_is_relative_and_repeatable():
    text = young() + young(2, 90)
    first = parser.parse(text)
    assert [e.timestamp for e in first.events] == [None, None]
    assert first.to_dict() == parser.parse(text).to_dict()
    assert first.time_basis == "relative"
    assert first.anchor_source is None
    assert any("relative" in w.lower() for w in first.warnings)
    assert analyzer.bucket_metrics(first) == []


def test_explicit_jvm_start_anchor_preserves_wall_clock():
    p = parser.parse(young() + young(2, 90), "broker", start_time=1700000000.0)
    assert [e.timestamp for e in p.events] == [1700000010, 1700000090]
    assert p.anchor_source == "provided_start_time"
    p = parser.parse(young(stamp="2026-06-08T10:00:00.000Z"), start_time=1)
    assert p.events[0].timestamp == 1780912800


@pytest.mark.parametrize("anchor", [float("nan"), float("inf"), "yesterday"])
def test_invalid_anchor_rejected(anchor):
    with pytest.raises(ValueError, match="start_time"):
        parser.parse(young(), start_time=anchor)


@pytest.mark.parametrize("text", ["", "INFO application started\n"])
def test_no_events_never_healthy(text):
    result = analyzer.analyze(parser.parse(text))
    assert result["health"]["score"] is None
    assert result["health"]["grade"] == "?"
    assert result["health"]["status"] == "unknown"
    assert result["findings"]["pros"] == []
    assert result["metrics"]["throughput_pct"] is None


@pytest.mark.parametrize("text", [young(), young() + young(2, 10.1)])
def test_inadequate_span_has_no_rates_or_grade(text):
    result = analyzer.analyze(parser.parse(text))
    for key in ("throughput_pct", "pct_time_in_gc", "gc_per_min", "alloc_rate_mb_s"):
        assert result["metrics"][key] is None
    assert result["metrics"]["max_pause_ms"] == 100
    assert result["health"]["score"] is None
    assert result["data_quality"]["status"] == "limited"


def test_pause_only_remark_cleanup_and_concurrent_subphases():
    text = ("[10.000s][info][gc,start] GC(7) Pause Remark\n"
            "[10.001s][info][gc,phases] GC(7) Finalize Marking 3.000ms\n"
            + pause() + pause(7, 10.2, 1.1, "Cleanup")
            + "[10.300s][info][gc] GC(7) Concurrent Cleanup for Next Mark 2.000ms\n")
    p = parser.parse(text)
    assert [e.phase for e in p.events] == ["remark", "cleanup", "concurrent"]
    assert p.events[0].heap_after_mb is None
    assert p.events[0].cause == "Remark"
    assert analyzer.analyze(p)["metrics"]["total_pause_ms"] == 19.3


def test_phase_details_do_not_collapse_distinct_zgc_pauses():
    p = parser.parse(pause(7, 10, 0.1, "Mark Start") + pause(7, 10, 0.1, "Mark End"))
    assert len(p.events) == 2


def test_split_heap_phase_summary_is_one_event_and_real_cause():
    text = ("[10.000s][gc,start] GC(42) Pause Full (Allocation Failure)\n"
            "[10.900s][gc,heap] GC(42) 900M->200M(1024M)\n"
            "[10.900s][gc] GC(42) Pause Full (Allocation Failure) 900.000ms\n")
    p = parser.parse(text + text)
    assert len(p.events) == 1
    e = p.events[0]
    assert (e.phase, e.pause_ms, e.heap_after_mb) == ("full", 900, 200)
    assert e.cause == "Allocation Failure"
    assert p.record_counts["duplicate"] == 1
    assert e.source_lines


def test_heap_duration_and_final_pause_summary_not_double_counted():
    text = ("[10.000s][gc,start] GC(42) Pause Full (Allocation Failure)\n"
            "[10.900s][gc,heap] GC(42) Eden: 900M->200M(1024M) 900.000ms\n"
            "[10.900s][gc] GC(42) Pause Full (Allocation Failure) 900.000ms\n")
    p = parser.parse(text)
    assert len(p.events) == 1
    assert p.events[0].heap_after_mb == 200


def test_rotations_dedup_and_timestamped_restarts_keep_recycled_ids():
    old = young(0, 10, stamp="2026-06-08T10:00:10.000+0000")
    new = young(0, 10, stamp="2026-06-08T11:00:10.000+0000")
    p = parser.parse(new + old + old + new)
    assert len(p.events) == 2
    assert p.events[0].timestamp < p.events[1].timestamp
    assert p.events[0].jvm_id != p.events[1].jvm_id


def test_relative_restart_does_not_borrow_epoch_anchor():
    text = (young(9, 100) + "[0.001s][gc] Using G1\n" + young(0, 1))
    p = parser.parse(text, start_time=1700000000)
    assert p.events[0].timestamp == 1700000100
    assert p.events[1].timestamp is None
    assert p.events[0].jvm_id != p.events[1].jvm_id


def test_legacy_multiline_uses_outer_heap_and_absolute_timestamp():
    text = ("2026-06-08T10:00:00.000+0000: 10.000: [Full GC (Allocation Failure)\n"
            " [PSYoungGen: 1024K->0K(2048K)] [ParOldGen: 8192K->4096K(16384K)]\n"
            " 9216K->4096K(18432K), 0.1234567 secs]\n")
    p = parser.parse(text)
    assert len(p.events) == 1
    assert p.events[0].timestamp == 1780912800
    assert p.events[0].uptime == 10
    assert p.events[0].heap_after_mb == 4


def test_gzip_file_and_decompressed_limit(tmp_path):
    path = tmp_path / "gc.log.gz"
    path.write_bytes(gzip.compress(young().encode()))
    p = parser.parse_file(path, "broker", start_time=1700000000)
    assert len(p.events) == 1
    assert p.events[0].timestamp == 1700000010
    with pytest.raises(ValueError, match="limit"):
        parser.parse_file(path, max_bytes=10)
    with pytest.raises(ValueError, match="limit"):
        parser.parse_file(path, max_compressed_bytes=10)


def test_corrupt_gzip_is_not_silently_accepted(tmp_path):
    path = tmp_path / "gc.log.gz"
    path.write_bytes(gzip.compress(young().encode())[:-5])
    with pytest.raises((OSError, EOFError, ValueError)):
        parser.parse_file(path)


def test_downsampling_preserves_pause_and_heap_peaks_and_endpoints():
    p = parser.parse("".join(young(i, i, 7000 if i == 777 else 1) for i in range(4000)))
    p.events[1231].heap_after_mb = 1023
    p.events[1881].heap_before_mb = 2048
    points = analyzer.analyze(p)["timeline"]
    assert len(points) <= 1500
    assert points[0]["t"] == 0 and points[-1]["t"] == 3999
    assert max(r["pause_ms"] for r in points) == 7000
    assert max(r["after_mb"] for r in points) == 1023
    assert max(r["before_mb"] for r in points) == 2048


def local_node(path):
    return collector.NodeConfig("broker-1", "broker", source="local",
                                local_paths=[str(path)], sar_enabled=False)


def test_increment_retries_partial_newline_and_uses_byte_offsets(tmp_path):
    path = tmp_path / "gc.log"
    first = "# caf\u00e9\n" + young()
    second = young(2, 70)
    path.write_bytes((first + second[:30]).encode())
    node = local_node(path)
    text, offsets = collector.read_increment_local(node, {})
    assert text == first
    assert offsets[str(path)]["offset"] == len(first.encode())
    path.write_bytes((first + second).encode())
    text, _ = collector.read_increment_local(node, offsets)
    assert text == second


def test_increment_retries_multiline_start_until_summary(tmp_path):
    path = tmp_path / "gc.log"
    first = young()
    start = "[20.000s][gc,start] GC(2) Pause Full (Allocation Failure)\n"
    path.write_text(first + start)
    text, offsets = collector.read_increment_local(local_node(path), {})
    assert text == first
    assert offsets[str(path)]["offset"] == len(first.encode())
    path.write_text(first + start + "[20.900s][gc,heap] GC(2) 900M->200M(1024M) 900.000ms\n")
    text, _ = collector.read_increment_local(local_node(path), offsets)
    assert parser.parse(text).events[0].phase == "full"


def test_increment_dedups_globs_and_tracks_inode_across_rename(tmp_path):
    path = tmp_path / "gc.log"
    path.write_text(young())
    node = local_node(path)
    node.local_paths.append(str(tmp_path / "gc.*"))
    text, offsets = collector.read_increment_local(node, {})
    assert len(parser.parse(text).events) == 1
    path.rename(tmp_path / "gc.log.1")
    text, _ = collector.read_increment_local(node, offsets)
    assert text == ""


def setup_db(tmp_path):
    db = str(tmp_path / "history.db")
    store.init_db(db)
    return db


def test_live_relative_logs_do_not_get_collection_time(tmp_path):
    db = setup_db(tmp_path)
    p = parser.parse(young())
    with store.connect(db) as conn:
        with pytest.raises(ValueError, match="anchor|wall.clock"):
            ingest.record_analysis_metrics(conn, "broker", p, analyzer.analyze(p),
                                           ts=1800000000, incremental=True)
        assert conn.execute("SELECT COUNT(*) FROM metrics").fetchone()[0] == 0


def test_metric_and_offsets_rollback_together_on_second_offset_failure(tmp_path, monkeypatch):
    db = setup_db(tmp_path)
    p = parser.parse(young(stamp="2026-06-08T10:00:00.000Z"))
    real = store.set_offset

    def failing(conn, iid, path, inode, offset, ts):
        if path == "second":
            raise RuntimeError("disk full")
        real(conn, iid, path, inode, offset, ts)

    monkeypatch.setattr(store, "set_offset", failing)
    with store.connect(db) as conn:
        with pytest.raises(RuntimeError, match="disk full"):
            ingest.record_analysis_metrics(conn, "broker", p, analyzer.analyze(p),
                incremental=True, new_offsets={"first": {"inode": 1, "offset": 100},
                                               "second": {"inode": 2, "offset": 100}})
        assert store.get_offsets(conn, "broker") == {}
        assert conn.execute("SELECT COUNT(*) FROM metrics").fetchone()[0] == 0


@pytest.mark.parametrize("failure", ["parse", "write", "unsupported"])
def test_scheduler_never_advances_failed_offsets(tmp_path, monkeypatch, failure):
    db = setup_db(tmp_path)
    node = local_node(tmp_path / "gc.log")
    monkeypatch.setattr(scheduler, "_cluster_configs", lambda: ["fake.yaml"])
    monkeypatch.setattr(scheduler.config, "load_cluster", lambda _: ("LOCAL-DEV", [node], "LOCAL", "DEV"))
    monkeypatch.setattr(scheduler.sar_ingest, "ingest_sar_nodes", lambda *a, **kw: [])
    text = young(stamp="2026-06-08T10:00:00.000Z") if failure != "unsupported" else "application message\n"
    monkeypatch.setattr(scheduler, "read_increment_local", lambda *a: (text, {"gc.log": {"inode": 1, "offset": 999}}))

    def fail(*a, **kw):
        raise RuntimeError("injected failure")

    if failure == "parse":
        monkeypatch.setattr(scheduler.parser, "parse", fail)
    elif failure == "write":
        monkeypatch.setattr(store, "record_metric", fail)
    scheduler.tick(db, now=1780912800)
    with store.connect(db) as conn:
        assert store.get_offsets(conn, "LOCAL-DEV--broker-1") == {}


class Channel:
    def __init__(self, out=b"", err=b"", done=True, status=0):
        self.out, self.err = bytearray(out), bytearray(err)
        self.done, self.status, self.closed = done, status, False

    def settimeout(self, timeout):
        self.timeout = timeout

    def recv_ready(self):
        return bool(self.out)

    def recv_stderr_ready(self):
        return bool(self.err)

    def recv(self, size):
        result = bytes(self.out[:size])
        del self.out[:size]
        return result

    def recv_stderr(self, size):
        result = bytes(self.err[:size])
        del self.err[:size]
        return result

    def exit_status_ready(self):
        return self.done

    def recv_exit_status(self):
        return self.status

    def close(self):
        self.closed = True


class Client:
    def __init__(self, channel):
        self.channel = channel
        self.calls = []

    def exec_command(self, command, **kwargs):
        self.calls.append((command, kwargs))
        return io.BytesIO(), SimpleNamespace(channel=self.channel, close=lambda: None), io.BytesIO()


def test_ssh_channel_drains_both_streams_and_closes():
    ch = Channel(b"hello", b"warning")
    client = Client(ch)
    out, err, status = collector._run_ssh_command(client, "captured command", max_bytes=100)
    assert (out, err, status) == (b"hello", b"warning", 0)
    assert ch.closed
    assert client.calls[0][1]["timeout"] > 0


@pytest.mark.parametrize("out,err", [(b"x" * 200, b""), (b"", b"x" * 200)])
def test_ssh_output_limit_closes_channel_with_bounded_capture(out, err):
    ch = Channel(out, err)
    with pytest.raises(RuntimeError, match="limit") as exc:
        collector._run_ssh_command(Client(ch), "captured command", max_bytes=100)
    assert len(exc.value.stdout) + len(exc.value.stderr) <= 100
    assert ch.closed


def test_ssh_timeout_closes_stalled_channel():
    ch = Channel(done=False)
    with pytest.raises(RuntimeError, match="timeout|deadline"):
        collector._run_ssh_command(Client(ch), "captured command", timeout=0.01)
    assert ch.closed


def test_ssh_nonzero_exit_does_not_return_partial_success():
    ch = Channel(b"partial", b"read error", status=1)
    with pytest.raises(RuntimeError, match="read error"):
        collector._run_ssh_command(Client(ch), "captured command")
    assert ch.closed


def test_heap_only_suffix_without_start_is_retried(tmp_path):
    path = tmp_path / "gc.log"
    first = young()
    heap = "[20.100s][gc,heap] GC(2) 900M->200M(1024M)\n"
    path.write_text(first + heap)
    text, offsets = collector.read_increment_local(local_node(path), {})
    assert text == first
    path.write_text(first + heap + pause(2, 20.1, 100, "Full (Allocation Failure)"))
    text, _ = collector.read_increment_local(local_node(path), offsets)
    assert parser.parse(text).events[0].heap_after_mb == 200


def test_different_phase_does_not_complete_pending_pause(tmp_path):
    path = tmp_path / "gc.log"
    start = "[10.000s][gc,start] GC(7) Pause Remark\n"
    path.write_text(start + pause(7, 20, 1, "Cleanup"))
    text, offsets = collector.read_increment_local(local_node(path), {})
    assert text == ""
    assert offsets[str(path)]["offset"] == 0
    p = parser.parse(path.read_text())
    assert p.events[0].phase == "cleanup"
    assert p.record_counts["malformed"] > 0


def test_full_ingest_seeds_offsets_and_followup_does_not_replay(tmp_path):
    path = tmp_path / "gc.log"
    path.write_text(young(stamp="2026-06-08T10:00:00.000Z") + young(2, 90, stamp="2026-06-08T10:01:20.000Z"))
    db = setup_db(tmp_path)
    node = local_node(path)
    results = ingest.ingest_nodes([node], db, cluster="test", collect_mode="full", now=1780912920)
    assert results[0].recorded, results[0].detail
    with store.connect(db) as conn:
        offsets = store.get_offsets(conn, "test--broker-1")
        assert offsets[str(path)]["offset"] == path.stat().st_size
        before = conn.execute("SELECT COUNT(*) FROM metrics").fetchone()[0]
    results = ingest.ingest_nodes([node], db, cluster="test", now=1780912980)
    assert "no new GC" in results[0].detail
    with store.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM metrics").fetchone()[0] == before


def test_partial_parse_with_good_events_is_not_graded_healthy():
    p = parser.parse(young() + "[30.000s][gc] GC(2) Pause Remark BROKENms\n" + young(3, 90))
    result = analyzer.analyze(p)
    assert result["health"]["score"] is None
    assert result["data_quality"]["status"] == "limited"


def test_unobserved_heap_never_becomes_healthy_capacity():
    result = analyzer.analyze(parser.parse(pause() + pause(8, 90)))
    assert result["health"]["score"] is None
    assert result["data_quality"]["heap_observed"] is False
    assert not any("headroom" in s.lower() for s in result["findings"]["pros"])


def test_relative_duplicate_startup_rotations_dedup():
    block = "[0.001s][gc] Using G1\n" + young(0, 10) + young(1, 90)
    p = parser.parse(block + block)
    assert len(p.events) == 2


class ScriptedClient:
    """Fake SSH transport backed only by captured command output."""

    def __init__(self, replies):
        self.replies = iter(replies)
        self.channels, self.calls = [], []
        self.closed = False

    def set_missing_host_key_policy(self, policy):
        pass

    def connect(self, **kwargs):
        self.connect_kwargs = kwargs

    def exec_command(self, command, **kwargs):
        out, err, status = next(self.replies)
        ch = Channel(out, err, status=status)
        self.calls.append((command, kwargs))
        self.channels.append(ch)
        return io.BytesIO(), SimpleNamespace(channel=ch, close=lambda: None), io.BytesIO()

    def close(self):
        self.closed = True


def fake_ssh(monkeypatch, replies):
    client = ScriptedClient(replies)
    monkeypatch.setitem(sys.modules, "paramiko", SimpleNamespace(SSHClient=lambda: client, AutoAddPolicy=lambda: None))
    return client


def test_sar_ssh_date_json_and_text_all_use_bounded_channels(monkeypatch):
    client = fake_ssh(monkeypatch, [(b"06/08/2026\n", b"", 0), (b"", b"sadf absent", 127), (b"Linux host\n", b"", 0)])
    result = collector.collect_sar_ssh(collector.NodeConfig("broker", "broker", host="fake"))
    assert result.error is None
    assert result.fmt_hint == "text"
    assert result.text == "Linux host\n"
    assert len(client.calls) == 3
    assert all(kwargs["timeout"] > 0 for _, kwargs in client.calls)
    assert all(ch.closed for ch in client.channels) and client.closed


def test_ssh_increment_offset_matches_complete_snapshot(monkeypatch):
    raw = young().encode()
    data = raw + b"[20.000s][gc] GC(2) Pau"
    stat_out = f"42 {len(data)}".encode()
    client = fake_ssh(monkeypatch, [(b"/tmp/gc.log\n", b"", 0), (stat_out, b"", 0), (data, b"", 0), (stat_out, b"", 0)])
    text, offsets = collector.collect_ssh_incremental(collector.NodeConfig("broker", "broker", host="fake", log_paths=["/tmp/gc.log"]), {})
    assert text == raw.decode()
    assert offsets["/tmp/gc.log"] == {"inode": 42, "offset": len(raw)}
    assert all(ch.closed for ch in client.channels) and client.closed


def test_sar_output_limit_is_explicit_failure(monkeypatch):
    client = fake_ssh(monkeypatch, [(b"06/08/2026\n", b"", 0), (b"{" + b"x" * 100, b"", 0)])
    monkeypatch.setattr(collector, "MAX_SAR_BYTES", 32)
    result = collector.collect_sar_ssh(collector.NodeConfig("broker", "broker", host="fake"))
    assert "limit" in result.error
    assert result.text == ""
    assert client.closed


def test_increment_legacy_multiline_retried(tmp_path):
    path = tmp_path / "gc.log"
    start = "2026-06-08T10:00:00.000+0000: 10.000: [Full GC (Allocation Failure)\n"
    path.write_text(start + " [PSYoungGen: 1024K->0K(2048K)]\n")
    text, offsets = collector.read_increment_local(local_node(path), {})
    assert text == ""
    assert offsets[str(path)]["offset"] == 0


def test_local_collection_limits_reject_oversized_captures(tmp_path, monkeypatch):
    path = tmp_path / "gc.log"
    path.write_text(young())
    monkeypatch.setattr(collector, "MAX_LOG_BYTES", 32)
    with pytest.raises(ValueError, match="limit"):
        collector.read_increment_local(local_node(path), {})
    with pytest.raises(ValueError, match="limit"):
        collector.collect_local(local_node(path))


def test_public_findings_accept_nullable_store_metrics():
    metrics = analyzer.analyze(parser.parse(""))["metrics"]
    metrics["max_pause_ms"] = None
    assert analyzer.derive_findings(metrics)["pros"] == []


def test_scheduler_uses_config_directory_override(tmp_path, monkeypatch):
    isolated = tmp_path / "isolated-clusters"
    isolated.mkdir()
    (isolated / "test.yaml").write_text("# isolated scheduler configuration\n")
    try:
        with monkeypatch.context() as scoped:
            scoped.setenv("GC_CONFIG_DIR", str(isolated))
            importlib.reload(scheduler)
            assert scheduler.CLUSTERS_DIR == str(isolated)
            assert scheduler._cluster_configs() == [str(isolated / "test.yaml")]
    finally:
        importlib.reload(scheduler)


MALFORMED_PAUSE = "[2026-06-08T10:00:30.000Z][30.000s][gc] GC(2) Pause Remark BROKENms\n"
MALFORMED_HEAP_PAUSE = young(2, 30, stamp="2026-06-08T10:00:30.000Z").replace("100.000ms", "BROKENms")
UNSUPPORTED_COLLECTION = "[2026-06-08T10:00:30.000Z][30.000s][gc] GC(2) Garbage Collection (Warmup) 512M(50%)->128M(12%)\n"


@pytest.mark.parametrize("entrypoint", ["scheduler", "ingest"])
@pytest.mark.parametrize("bad_only_poll", [True, False])
@pytest.mark.parametrize("malformed_line", [MALFORMED_PAUSE, MALFORMED_HEAP_PAUSE])
def test_complete_malformed_record_does_not_poison_later_polls(tmp_path, monkeypatch, entrypoint, bad_only_poll, malformed_line):
    path = tmp_path / "gc.log"
    db = setup_db(tmp_path)
    node = local_node(path)
    messages = []
    monkeypatch.setattr(scheduler, "_cluster_configs", lambda: ["isolated.yaml"])
    monkeypatch.setattr(scheduler.config, "load_cluster", lambda _: ("test", [node], "LOCAL", "DEV"))
    monkeypatch.setattr(scheduler.sar_ingest, "ingest_sar_nodes", lambda *a, **kw: [])

    def poll(now):
        if entrypoint == "scheduler":
            scheduler.tick(db, now=now, on_node_result=lambda *args: messages.append(args[1]))
        else:
            ingest.ingest_nodes([node], db, cluster="test", now=now,
                               log_callback=lambda message, **kwargs: messages.append(message))

    first = young(1, 10, stamp="2026-06-08T10:00:10.000Z")
    path.write_text(first)
    poll(1780912860)
    bad_batch = first + malformed_line
    if not bad_only_poll:
        bad_batch += young(3, 40, stamp="2026-06-08T10:00:40.000Z")
        bad_batch += young(4, 120, stamp="2026-06-08T10:02:00.000Z")
    path.write_text(bad_batch)
    poll(1780912920)
    with store.connect(db) as conn:
        assert store.get_offsets(conn, "test--broker-1")[str(path)]["offset"] == len(bad_batch.encode())
        rows = store.window_rows(conn, "test--broker-1", 0)
        assert len(rows) == (1 if bad_only_poll else 2)
        if not bad_only_poll:
            assert rows[-1]["throughput_pct"] is None
            assert rows[-1]["gc_per_min"] is None
    assert any("malformed" in message.lower() for message in messages)

    third = young(5, 180, stamp="2026-06-08T10:03:00.000Z")
    path.write_text(bad_batch + third[:40])
    poll(1780912980)
    with store.connect(db) as conn:
        assert store.get_offsets(conn, "test--broker-1")[str(path)]["offset"] == len(bad_batch.encode())
    path.write_text(bad_batch + third)
    poll(1780913040)
    poll(1780913100)
    with store.connect(db) as conn:
        rows = store.window_rows(conn, "test--broker-1", 0)
        assert len(rows) == (2 if bad_only_poll else 3)
        assert sum(row["young_count"] for row in rows) == (2 if bad_only_poll else 4)
        assert store.get_offsets(conn, "test--broker-1")[str(path)]["offset"] == path.stat().st_size


def test_backfill_buckets_preserve_partial_parse_quality():
    text = (young(1, 10, stamp="2026-06-08T10:00:10.000Z") + MALFORMED_PAUSE
            + young(3, 90, stamp="2026-06-08T10:01:30.000Z"))
    p = parser.parse(text)
    assert analyzer.analyze(p)["health"]["score"] is None
    [(ts, metrics)] = analyzer.bucket_metrics(p, bucket_s=300)
    assert metrics["throughput_pct"] is None
    assert metrics["rate_basis"] == "incomplete_coverage"
    assert analyzer.score_health(metrics)["score"] is None


def test_malformed_prefix_commits_but_multiline_suffix_remains_retryable(tmp_path):
    path = tmp_path / "gc.log"
    db = setup_db(tmp_path)
    first = young(stamp="2026-06-08T10:00:10.000Z") + MALFORMED_PAUSE
    start = "[2026-06-08T10:02:00.000Z][120.000s][gc,start] GC(3) Pause Full (Allocation Failure)\n"
    path.write_text(first + start)
    text, offsets = collector.read_increment_local(local_node(path), {})
    p = parser.parse(text)
    with store.connect(db) as conn:
        ingest.record_analysis_metrics(conn, "broker", p, analyzer.analyze(p), incremental=True, new_offsets=offsets)
        committed = store.get_offsets(conn, "broker")
        assert committed[str(path)]["offset"] == len(first.encode())
    path.write_text(first + start + "[2026-06-08T10:02:00.900Z][120.900s][gc] GC(3) Pause Full (Allocation Failure) 900M->200M(1024M) 900.000ms\n")
    text, _ = collector.read_increment_local(local_node(path), committed)
    assert parser.parse(text).events[0].phase == "full"


def test_parser_distinguishes_complete_malformed_from_incomplete_records(tmp_path):
    complete = parser.parse(MALFORMED_PAUSE)
    assert complete.record_counts["malformed"] == 1
    assert complete.record_counts["incomplete"] == 0
    text = young(stamp="2026-06-08T10:00:10.000Z") + "[20.000s][gc,start] GC(3) Pause Remark\n"
    incomplete = parser.parse(text)
    assert incomplete.record_counts["incomplete"] == 1
    with store.connect(setup_db(tmp_path)) as conn:
        with pytest.raises(ValueError, match="Incomplete"):
            ingest.record_analysis_metrics(conn, "broker", incomplete, analyzer.analyze(incomplete),
                incremental=True, new_offsets={"gc.log": {"inode": 1, "offset": len(text)}})
        assert store.get_offsets(conn, "broker") == {}


def test_malformed_only_acknowledgement_rolls_back_on_offset_failure(tmp_path, monkeypatch):
    db = setup_db(tmp_path)
    real = store.set_offset

    def failing(conn, iid, path, inode, offset, ts):
        if path == "second":
            raise RuntimeError("disk full")
        real(conn, iid, path, inode, offset, ts)

    monkeypatch.setattr(store, "set_offset", failing)
    p = parser.parse(MALFORMED_PAUSE)
    with store.connect(db) as conn:
        with pytest.raises(RuntimeError, match="disk full"):
            ingest.record_analysis_metrics(conn, "broker", p, analyzer.analyze(p), incremental=True,
                new_offsets={"first": {"inode": 1, "offset": 100}, "second": {"inode": 2, "offset": 100}})
        assert store.get_offsets(conn, "broker") == {}
        assert conn.execute("SELECT COUNT(*) FROM metrics").fetchone()[0] == 0


def test_complete_bad_summary_retires_only_its_own_start(tmp_path):
    path = tmp_path / "gc.log"
    remark_start = "[2026-06-08T10:00:29.000Z][29.000s][gc,start] GC(2) Pause Remark\n"
    cleanup_start = "[2026-06-08T10:00:31.000Z][31.000s][gc,start] GC(2) Pause Cleanup\n"
    path.write_text(remark_start + MALFORMED_PAUSE + cleanup_start)
    text, offsets = collector.read_increment_local(local_node(path), {})
    assert text == remark_start + MALFORMED_PAUSE
    assert offsets[str(path)]["offset"] == len(text.encode())
    p = parser.parse(text)
    assert p.record_counts["malformed"] == 1
    assert p.record_counts["incomplete"] == 0
    assert p.events == []


def test_unterminated_malformed_line_is_still_incomplete():
    p = parser.parse(MALFORMED_PAUSE.rstrip("\n"))
    assert p.record_counts["incomplete"] == 1


QUALITY_BASE = 1780912920
QUALITY_IID = "test--broker-1"
HEALTHY_COLLECTION = (young(1, 10, stamp="2026-06-08T10:00:10.000Z")
                      + young(3, 90, stamp="2026-06-08T10:01:30.000Z"))


def quality_db(tmp_path, healthy=True):
    db = setup_db(tmp_path)
    with store.connect(db) as conn:
        node = local_node(tmp_path / "gc.log")
        inst = ingest.build_instance(node, "LOCAL", "DEV", "test", 1, 1024, instance_id=QUALITY_IID)
        store.upsert_instance(conn, inst)
        if healthy:
            write_gc_collection(conn, HEALTHY_COLLECTION, QUALITY_BASE, 100)
            assert store.current_snapshot(conn, QUALITY_IID, now=QUALITY_BASE)["health"]["grade"] == "A"
    return db


def write_gc_collection(conn, text, ts, offset, iid=QUALITY_IID):
    p = parser.parse(text)
    return ingest.record_analysis_metrics(conn, iid, p, analyzer.analyze(p), ts=ts, incremental=True,
        new_offsets={"gc.log": {"inode": 1, "offset": offset}})


@pytest.mark.parametrize("mixed", [False, True])
def test_collection_quality_survives_reopen_and_recovers_without_rewriting_history(tmp_path, mixed):
    db = quality_db(tmp_path)
    bad = MALFORMED_PAUSE + HEALTHY_COLLECTION if mixed else MALFORMED_PAUSE
    with store.connect(db) as conn:
        original = store.latest_row(conn, QUALITY_IID)
        assert write_gc_collection(conn, bad, QUALITY_BASE + 60, 200) == int(mixed)
    store.init_db(db)
    with store.connect(db) as conn:
        snap = store.current_snapshot(conn, QUALITY_IID, now=QUALITY_BASE + 61)
        assert snap["quality"]["state"] in ("unknown", "partial")
        assert snap["health"]["grade"] == "?"
        assert snap["health"]["score"] is None
        assert snap["findings"]["pros"] == []
        assert snap["findings"]["recommendations"] == []
        assert snap["quality"]["collection"]["state"] == ("partial" if mixed else "unknown")
        assert snap["quality"]["collection"]["malformed_count"] == 1
        assert snap["quality"]["collection"]["ts"] == QUALITY_BASE + 60
        if not mixed:
            assert snap["latest"] == original
            assert snap["quality"]["last_observed_at"] == original["ts"]
            assert snap["quality"]["age_seconds"] == 61
            assert len(store.window_rows(conn, QUALITY_IID, 0)) == 1
        assert store.current_snapshot(conn, QUALITY_IID, now=QUALITY_BASE + 59)["health"]["grade"] == "A"
        write_gc_collection(conn, HEALTHY_COLLECTION, QUALITY_BASE + 120, 300)
        recovered = store.current_snapshot(conn, QUALITY_IID, now=QUALITY_BASE + 120)
        assert recovered["quality"]["state"] == "fresh"
        assert recovered["quality"]["collection"]["state"] == "complete"
        assert recovered["health"]["grade"] == "A"
        assert store.current_snapshot(conn, QUALITY_IID, now=QUALITY_BASE + 60)["health"]["score"] is None
        assert store.current_snapshot(conn, QUALITY_IID, now=0)["latest"] is None


def test_bad_only_without_observations_does_not_invent_latest_or_observation_time(tmp_path):
    db = quality_db(tmp_path, healthy=False)
    with store.connect(db) as conn:
        write_gc_collection(conn, MALFORMED_PAUSE, QUALITY_BASE, 100)
        snap = store.current_snapshot(conn, QUALITY_IID, now=QUALITY_BASE)
        assert snap["quality"]["collection"]["state"] == "unknown"
        assert snap["latest"] is None
        assert snap["quality"]["last_observed_at"] is None
        assert snap["quality"]["age_seconds"] is None
        assert snap["metrics"] == {}
        assert snap["health"]["score"] is None


@pytest.mark.parametrize("stage", ["record_metric", "set_offset", "record_gc_collection_quality"])
def test_failed_recovery_rolls_back_quality_metrics_and_offsets(tmp_path, monkeypatch, stage):
    db = quality_db(tmp_path)
    with store.connect(db) as conn:
        write_gc_collection(conn, MALFORMED_PAUSE, QUALITY_BASE + 60, 200)
    original = getattr(store, stage)

    def fail_after_write(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("simulated write failure")

    monkeypatch.setattr(store, stage, fail_after_write)
    with store.connect(db) as conn:
        with pytest.raises(RuntimeError, match="simulated write failure"):
            write_gc_collection(conn, HEALTHY_COLLECTION, QUALITY_BASE + 120, 300)
    with store.connect(db) as conn:
        snap = store.current_snapshot(conn, QUALITY_IID, now=QUALITY_BASE + 120)
        assert snap["quality"]["collection"]["ts"] == QUALITY_BASE + 60
        assert snap["health"]["score"] is None
        assert store.get_offsets(conn, QUALITY_IID)["gc.log"]["offset"] == 200
        assert store.latest_row(conn, QUALITY_IID)["ts"] == QUALITY_BASE


@pytest.mark.parametrize("bad_text", [MALFORMED_PAUSE, UNSUPPORTED_COLLECTION])
def test_bad_marker_write_failure_cannot_commit_bad_only_offset(tmp_path, monkeypatch, bad_text):
    db = quality_db(tmp_path)
    original = store.record_gc_collection_quality

    def fail_after_write(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("quality write failed")

    monkeypatch.setattr(store, "record_gc_collection_quality", fail_after_write)
    with store.connect(db) as conn:
        with pytest.raises(RuntimeError, match="quality write failed"):
            write_gc_collection(conn, bad_text, QUALITY_BASE + 60, 200)
        assert store.get_offsets(conn, QUALITY_IID)["gc.log"]["offset"] == 100
        snap = store.current_snapshot(conn, QUALITY_IID, now=QUALITY_BASE + 60)
        assert snap["quality"]["collection"]["ts"] == QUALITY_BASE
        assert snap["health"]["grade"] == "A"


def test_no_event_parse_cannot_clear_bad_collection_marker(tmp_path):
    db = quality_db(tmp_path)
    with store.connect(db) as conn:
        write_gc_collection(conn, MALFORMED_PAUSE, QUALITY_BASE + 60, 200)
        with pytest.raises(ValueError, match="No usable GC"):
            write_gc_collection(conn, "", QUALITY_BASE + 120, 300)
        assert store.current_snapshot(conn, QUALITY_IID, now=QUALITY_BASE + 120)["health"]["score"] is None
        assert store.get_offsets(conn, QUALITY_IID)["gc.log"]["offset"] == 200


def test_collection_marker_cannot_make_old_observations_fresh(tmp_path):
    db = quality_db(tmp_path)
    with store.connect(db) as conn:
        write_gc_collection(conn, MALFORMED_PAUSE, QUALITY_BASE + 86400, 200)
        snap = store.current_snapshot(conn, QUALITY_IID, now=QUALITY_BASE + 86400)
        assert snap["quality"]["state"] == "stale"
        assert snap["quality"]["last_observed_at"] == QUALITY_BASE
        assert snap["quality"]["age_seconds"] == 86400
        assert snap["quality"]["collection"]["ts"] == QUALITY_BASE + 86400
        assert snap["health"]["score"] is None


def test_demo_clock_includes_collection_marker_without_changing_observation_time(tmp_path, monkeypatch):
    db = quality_db(tmp_path)
    monkeypatch.setenv("GC_DEMO_MODE", "1")
    with store.connect(db) as conn:
        write_gc_collection(conn, MALFORMED_PAUSE, QUALITY_BASE + 60, 200)
        snap = store.current_snapshot(conn, QUALITY_IID)
        assert store.now_ts(conn) == QUALITY_BASE + 60
        assert snap["health"]["score"] is None
        assert snap["quality"]["last_observed_at"] == QUALITY_BASE


def test_out_of_order_quality_markers_do_not_override_newer_recovery(tmp_path):
    db = quality_db(tmp_path)
    with store.connect(db) as conn:
        write_gc_collection(conn, HEALTHY_COLLECTION, QUALITY_BASE + 120, 300)
        write_gc_collection(conn, MALFORMED_PAUSE, QUALITY_BASE + 60, 200)
        assert store.current_snapshot(conn, QUALITY_IID, now=QUALITY_BASE + 60)["health"]["score"] is None
        assert store.current_snapshot(conn, QUALITY_IID, now=QUALITY_BASE + 120)["health"]["grade"] == "A"


def test_collection_quality_follows_retention_migration_and_deletion(tmp_path):
    db = quality_db(tmp_path)
    with store.connect(db) as conn:
        write_gc_collection(conn, MALFORMED_PAUSE, QUALITY_BASE + 60, 200)
        write_gc_collection(conn, HEALTHY_COLLECTION, QUALITY_BASE + 120, 300)
        store.migrate_instance(conn, QUALITY_IID, "renamed")
        assert conn.execute("SELECT COUNT(*) FROM gc_collection_quality WHERE instance_id=?", (QUALITY_IID,)).fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM gc_collection_quality WHERE instance_id='renamed'").fetchone()[0] == 3
        store.prune_before(conn, QUALITY_BASE + 60)
        assert [r[0] for r in conn.execute("SELECT ts FROM gc_collection_quality ORDER BY ts")] == [QUALITY_BASE + 60, QUALITY_BASE + 120]
        store.delete_instance(conn, "renamed")
        assert conn.execute("SELECT COUNT(*) FROM gc_collection_quality").fetchone()[0] == 0


def test_migration_does_not_discard_bad_marker_on_timestamp_conflict(tmp_path):
    db = quality_db(tmp_path)
    with store.connect(db) as conn:
        write_gc_collection(conn, MALFORMED_PAUSE, QUALITY_BASE + 60, 200)
        write_gc_collection(conn, HEALTHY_COLLECTION, QUALITY_BASE + 60, 200, iid="renamed")
        store.migrate_instance(conn, QUALITY_IID, "renamed")
        marker = store.latest_gc_collection_quality(conn, "renamed", now=QUALITY_BASE + 60)
        assert marker["state"] == "unknown"
        assert marker["malformed_count"] == 1


def test_bad_collection_quality_reaches_existing_advisory_gates(tmp_path):
    db = quality_db(tmp_path)
    with store.connect(db) as conn:
        row = store.latest_row(conn, QUALITY_IID)
        for day in range(1, 30):
            store.record_metric(conn, QUALITY_IID, QUALITY_BASE - day * 86400,
                                row | {"heap_after_pct": row["heap_after_pct"] - day * 0.1})
        before = forecast.forecast_instance(conn, QUALITY_IID, now=QUALITY_BASE)
        assert any(s["prediction_ready"] for s in before["signals"] if s["kind"] == "gc")
        write_gc_collection(conn, MALFORMED_PAUSE, QUALITY_BASE + 60, 200)
        after = forecast.forecast_instance(conn, QUALITY_IID, now=QUALITY_BASE + 60)
        assert all(not s["prediction_ready"] for s in after["signals"] if s["kind"] == "gc")
        scaling = scaling_advisor.analyze_cluster_scaling(conn, "test", now=QUALITY_BASE + 60)
        assert scaling["nodes"][0]["source_quality"]["gc"]["state"] == "unknown"
        assert scaling["nodes"][0]["reliable"] is False


@pytest.mark.parametrize("bad_text", [MALFORMED_PAUSE, UNSUPPORTED_COLLECTION])
def test_bad_only_quality_preserves_recent_confirmed_gc_alerts(tmp_path, monkeypatch, bad_text):
    db = quality_db(tmp_path, healthy=False)
    monkeypatch.setenv("GC_SOURCE_MAX_AGE_S", "7200")
    text = HEALTHY_COLLECTION + pause(4, 110, 900, "Full (Allocation Failure)",
                                     "900M->200M(1024M) ", "2026-06-08T10:01:50.000Z")
    with store.connect(db) as conn:
        write_gc_collection(conn, text, QUALITY_BASE, 100)
        confirmed = store.current_snapshot(conn, QUALITY_IID, now=QUALITY_BASE)["alerts"]
        assert {a["type"] for a in confirmed} >= {"full_gc", "long_pause"}
        write_gc_collection(conn, bad_text, QUALITY_BASE + 60, 200)
        snap = store.current_snapshot(conn, QUALITY_IID, now=QUALITY_BASE + 60)
        assert snap["quality"]["state"] == "unknown"
        assert snap["alerts"] == confirmed
        assert snap["metrics"] == {}
        assert snap["health"]["grade"] == "?"
        assert snap["health"]["score"] is None
        assert snap["findings"]["recommendations"] == []
        expired = store.current_snapshot(conn, QUALITY_IID, now=QUALITY_BASE + store.RECENT_WINDOW_S + 1)
        assert expired["quality"]["state"] == "unknown"
        assert expired["alerts"] == []
        monkeypatch.setenv("GC_SOURCE_MAX_AGE_S", "30")
        stale = store.current_snapshot(conn, QUALITY_IID, now=QUALITY_BASE + 60)
        assert stale["quality"]["state"] == "stale"
        assert stale["alerts"] == []


@pytest.mark.parametrize("entrypoint", ["scheduler", "ingest"])
def test_unsupported_only_poll_commits_unknown_then_recovers_with_partial_suffix_retry(tmp_path, monkeypatch, entrypoint):
    db = setup_db(tmp_path)
    path = tmp_path / "gc.log"
    node = local_node(path)
    path.write_text(HEALTHY_COLLECTION)
    with store.connect(db) as conn:
        inst = ingest.build_instance(node, "LOCAL", "DEV", "test", 1, 1024, instance_id=QUALITY_IID)
        store.upsert_instance(conn, inst)
        text, offsets = collector.read_increment_local(node, {})
        parsed = parser.parse(text)
        ingest.record_analysis_metrics(conn, QUALITY_IID, parsed, analyzer.analyze(parsed),
                                       ts=QUALITY_BASE, incremental=True, new_offsets=offsets)
        original = store.latest_row(conn, QUALITY_IID)
        assert store.current_snapshot(conn, QUALITY_IID, now=QUALITY_BASE)["health"]["grade"] == "A"
    monkeypatch.setattr(scheduler, "_cluster_configs", lambda: ["isolated.yaml"])
    monkeypatch.setattr(scheduler.config, "load_cluster", lambda _: ("test", [node], "LOCAL", "DEV"))
    monkeypatch.setattr(scheduler.sar_ingest, "ingest_sar_nodes", lambda *a, **kw: [])

    def poll(now):
        if entrypoint == "scheduler":
            scheduler.tick(db, now=now)
        else:
            ingest.ingest_nodes([node], db, cluster="test", now=now)

    path.write_text(HEALTHY_COLLECTION + UNSUPPORTED_COLLECTION.rstrip("\n"))
    poll(QUALITY_BASE + 30)
    with store.connect(db) as conn:
        assert store.get_offsets(conn, QUALITY_IID)[str(path)]["offset"] == len(HEALTHY_COLLECTION.encode())
        assert store.latest_gc_collection_quality(conn, QUALITY_IID, now=QUALITY_BASE + 30)["ts"] == QUALITY_BASE
    path.write_text(HEALTHY_COLLECTION + UNSUPPORTED_COLLECTION)
    poll(QUALITY_BASE + 60)
    with store.connect(db) as conn:
        assert store.get_offsets(conn, QUALITY_IID)[str(path)]["offset"] == path.stat().st_size
        snap = store.current_snapshot(conn, QUALITY_IID, now=QUALITY_BASE + 60)
        assert snap["quality"]["state"] == "unknown"
        assert snap["quality"]["collection"] == {"ts": QUALITY_BASE + 60, "state": "unknown",
                                              "malformed_count": 0, "unsupported_count": 1}
        assert snap["latest"] == original
        assert snap["metrics"] == {}
        assert snap["health"]["score"] is None
    recovery = (young(10, 200, stamp="2026-06-08T10:03:20.000Z")
                + young(11, 280, stamp="2026-06-08T10:04:40.000Z"))
    path.write_text(HEALTHY_COLLECTION + UNSUPPORTED_COLLECTION + recovery)
    poll(QUALITY_BASE + 180)
    with store.connect(db) as conn:
        recovered = store.current_snapshot(conn, QUALITY_IID, now=QUALITY_BASE + 180)
        assert recovered["quality"]["state"] == "fresh"
        assert recovered["quality"]["collection"]["state"] == "complete"
        assert recovered["health"]["grade"] == "A"
        assert len(store.window_rows(conn, QUALITY_IID, 0)) == 2


def test_direct_partial_unsupported_batch_cannot_advance_or_clear_quality(tmp_path):
    db = quality_db(tmp_path)
    partial = UNSUPPORTED_COLLECTION.rstrip("\n")
    assert parser.parse(partial).record_counts["incomplete"] == 1
    with store.connect(db) as conn:
        with pytest.raises(ValueError, match="Incomplete"):
            write_gc_collection(conn, partial, QUALITY_BASE + 60, 200)
        assert store.get_offsets(conn, QUALITY_IID)["gc.log"]["offset"] == 100
        assert store.latest_gc_collection_quality(conn, QUALITY_IID, now=QUALITY_BASE + 60)["ts"] == QUALITY_BASE
