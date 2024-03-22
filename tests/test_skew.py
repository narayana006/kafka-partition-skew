import io
import json
from pathlib import Path

import pytest

from kafka_partition_skew.analyze import Thresholds, analyze, cv, gini, human
from kafka_partition_skew.cli import main
from kafka_partition_skew.parse import (
    InputError,
    parse_log_dirs,
    parse_offsets,
    split_partition,
)

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"
REAL = EXAMPLES / "kafka-log-dirs-single-broker.txt"
CLUSTER = EXAMPLES / "kafka-log-dirs-3-brokers.txt"


def log_dirs(brokers):
    """brokers: {broker: {dir: [(partition, size, lag, future), ...]}}"""
    return json.dumps({"version": 1, "brokers": [
        {"broker": b, "logDirs": [
            {"logDir": d, "error": None, "partitions": [
                {"partition": p, "size": s, "offsetLag": lag, "isFuture": fut}
                for p, s, lag, fut in parts]}
            for d, parts in dirs.items()]}
        for b, dirs in brokers.items()]})


def kinds(rep):
    return sorted(f.kind for f in rep.findings)


# --- parsing --------------------------------------------------------------------

def test_split_partition_with_dashes():
    assert split_partition("my-topic-12") == ("my-topic", 12)
    assert split_partition("__consumer_offsets-0") == ("__consumer_offsets", 0)
    with pytest.raises(InputError):
        split_partition("nopartition")


def test_parse_real_kafka_output_with_banner_lines():
    data = parse_log_dirs(REAL.read_text())
    assert data.brokers == [1]
    assert len(data.log_dirs) == 2
    assert len(data.replicas) == 10
    orders3 = next(r for r in data.replicas if (r.topic, r.partition) == ("orders", 3))
    assert orders3.size == 184854 and orders3.log_dir == "/tmp/timeline-kafka/data-1"


def test_parse_errors():
    with pytest.raises(InputError):
        parse_log_dirs("Querying brokers for log directories information\n")
    with pytest.raises(InputError):
        parse_log_dirs('{"version": 1}')
    with pytest.raises(InputError):
        parse_log_dirs("{not json")


def test_parse_offsets():
    offs = parse_offsets("orders:0:122\n# c\n\nmy:topic?:1:5\n")
    assert offs == {("orders", 0): 122, ("my:topic?", 1): 5}
    with pytest.raises(InputError):
        parse_offsets("orders-0 122\n")
    with pytest.raises(InputError):
        parse_offsets("orders:0:abc\n")


# --- statistics -----------------------------------------------------------------

def test_gini_and_cv():
    assert gini([5, 5, 5, 5]) == 0
    assert gini([0, 0, 0, 10]) == pytest.approx(0.75)
    assert gini([]) == 0 and gini([0, 0]) == 0
    assert cv([1, 1, 1]) == 0
    assert cv([0, 2]) == pytest.approx(1.0)


def test_human():
    assert human(512) == "512 B"
    assert human(1536) == "1.5 KiB"
    assert human(3 * 1024**3) == "3.0 GiB"


# --- analysis -------------------------------------------------------------------

def test_real_capture_hot_partition():
    rep = analyze(parse_log_dirs(REAL.read_text()), th=Thresholds(min_topic_bytes=0))
    orders = next(t for t in rep.topics if t.topic == "orders")
    assert orders.hot_partitions(2.0) == [3]
    assert rep.topics[0].topic == "orders"  # sorted by skew
    assert kinds(rep) == ["log-dir-share", "partition-skew"]


def test_min_topic_bytes_suppresses_small_topics():
    rep = analyze(parse_log_dirs(REAL.read_text()))
    assert rep.findings == []


def test_replicas_use_largest_copy_and_count_disk_per_broker():
    text = log_dirs({
        1: {"/d": [("t-0", 100, 0, False), ("t-1", 100, 0, False)]},
        2: {"/d": [("t-0", 90, 0, False), ("t-1", 100, 0, False)]},
    })
    rep = analyze(parse_log_dirs(text), th=Thresholds(min_topic_bytes=0))
    assert rep.topics[0].sizes == {0: 100, 1: 100}
    assert rep.broker_bytes == {1: 200, 2: 190}
    assert rep.findings == []


def test_broker_share_and_lag_and_future():
    text = log_dirs({
        1: {"/a": [("t-0", 1000, 0, False)]},
        2: {"/a": [("t-1", 1000, 50000, False)]},
        3: {"/a": [("t-2", 1000, 0, False), ("u-0", 3000, 0, False)],
            "/b": [("t-2", 400, 900, True)]},
    })
    rep = analyze(parse_log_dirs(text), th=Thresholds(min_topic_bytes=0))
    assert kinds(rep) == ["broker-share", "future-replica", "log-dir-share", "replica-lag"]
    assert rep.broker_bytes[3] == 4400
    assert rep.future_replicas == 1
    assert rep.topics[0].sizes  # future replica not counted as partition size
    assert next(t for t in rep.topics if t.topic == "t").sizes == {0: 1000, 1: 1000, 2: 1000}


def test_log_dir_error_reported():
    data = json.loads(log_dirs({1: {"/a": [("t-0", 1, 0, False)]}}))
    data["brokers"][0]["logDirs"][0]["error"] = "KafkaStorageException"
    rep = analyze(parse_log_dirs(json.dumps(data)))
    assert kinds(rep) == ["log-dir-error"]


def test_offsets_distinguish_hot_key_from_big_records():
    text = log_dirs({1: {"/a": [("t-0", 100, 0, False), ("t-1", 100, 0, False),
                                ("t-2", 100, 0, False), ("t-3", 900, 0, False)]}})
    data = parse_log_dirs(text)
    th = Thresholds(min_topic_bytes=0, share_tolerance=10)
    even = {("t", p): 1000 for p in range(4)}
    rep = analyze(data, even, th=th)
    assert "larger records" in rep.findings[0].suggestion
    skewed = {("t", 0): 100, ("t", 1): 100, ("t", 2): 100, ("t", 3): 900}
    rep = analyze(data, skewed, th=th)
    assert "partitioning key is concentrated" in rep.findings[0].suggestion
    # start offsets turn end offsets into message counts
    starts = {("t", 3): 800}
    rep = analyze(data, skewed, starts, th=th)
    assert rep.topics[0].messages[3] == 100


def test_topic_filter():
    rep = analyze(parse_log_dirs(CLUSTER.read_text()), topic_filter={"payments"})
    assert [t.topic for t in rep.topics] == ["payments"]


# --- CLI ------------------------------------------------------------------------

def test_cli_cluster_example(capsys):
    rc = main([str(CLUSTER), "--end-offsets", str(EXAMPLES / "end-offsets-3-brokers.txt")])
    out = capsys.readouterr().out
    assert rc == 1
    assert "[partition-skew] clickstream" in out and "hot: 4" in out
    assert "[broker-share] broker 2" in out
    assert "bytes are not" in out  # media-uploads


def test_cli_json_and_exit_zero(capsys):
    rc = main([str(REAL), "--min-topic-bytes", "0", "--format", "json", "--exit-zero"])
    data = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert data["brokers"] == {"1": 366004}
    assert data["topics"][0]["topic"] == "orders"
    assert {f["kind"] for f in data["findings"]} == {"partition-skew", "log-dir-share"}


def test_cli_stdin_and_errors(monkeypatch, capsys, tmp_path):
    monkeypatch.setattr("sys.stdin", io.StringIO(REAL.read_text()))
    assert main([]) == 0
    assert main([str(tmp_path / "missing")]) == 2
    bad = tmp_path / "offsets.txt"
    bad.write_text("garbage\n")
    assert main([str(REAL), "--end-offsets", str(bad)]) == 2
    assert "expected topic:partition:offset" in capsys.readouterr().err


def test_parse_kafka_37_output():
    # Captured from apache/kafka:3.7.0 (empty partitions of a new topic).
    text = (
        "Querying brokers for log directories information\n"
        "Received log directory information from brokers 1\n"
        '{"brokers":[{"broker":1,"logDirs":[{"partitions":[{"partition":"t-1","size":0,'
        '"offsetLag":0,"isFuture":false},{"partition":"t-0","size":0,"offsetLag":0,'
        '"isFuture":false}],"error":null,"logDir":"/tmp/kafka-logs"}]}],"version":1}\n'
    )
    rep = analyze(parse_log_dirs(text), th=Thresholds(min_topic_bytes=0))
    assert rep.topics[0].sizes == {0: 0, 1: 0}
    assert rep.findings == []
