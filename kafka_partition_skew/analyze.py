"""Skew statistics and findings for partitions, brokers and log dirs."""

from __future__ import annotations

import math
import statistics
from collections import defaultdict
from dataclasses import dataclass, field

from .parse import LogDirs


def gini(values: list[float]) -> float:
    """Gini coefficient: 0 = perfectly even, approaching 1 = one holds everything."""
    xs = sorted(v for v in values)
    n, total = len(xs), sum(xs)
    if n == 0 or total == 0:
        return 0.0
    return sum((2 * i - n - 1) * x for i, x in enumerate(xs, start=1)) / (n * total)


def cv(values: list[float]) -> float:
    """Coefficient of variation (population stdev / mean)."""
    if not values:
        return 0.0
    mean = statistics.fmean(values)
    return statistics.pstdev(values) / mean if mean else 0.0


@dataclass
class Thresholds:
    max_mean_ratio: float = 2.0
    cv: float = 0.5
    hot_factor: float = 2.0
    min_topic_bytes: int = 1024 * 1024
    share_tolerance: float = 0.25
    lag: int = 10_000


@dataclass
class TopicStats:
    topic: str
    sizes: dict[int, int]  # partition -> bytes (largest current replica)
    messages: dict[int, int] = field(default_factory=dict)

    @property
    def total(self) -> int:
        return sum(self.sizes.values())

    @property
    def mean(self) -> float:
        return self.total / len(self.sizes) if self.sizes else 0.0

    @property
    def max_mean(self) -> float:
        return max(self.sizes.values()) / self.mean if self.mean else 0.0

    @property
    def cv(self) -> float:
        return cv(list(self.sizes.values()))

    @property
    def gini(self) -> float:
        return gini(list(self.sizes.values()))

    def top_share(self) -> tuple[int, float]:
        """Share of bytes held by the largest 10% of partitions (at least one)."""
        k = max(1, math.ceil(len(self.sizes) * 0.1))
        top = sorted(self.sizes.values(), reverse=True)[:k]
        return k, (sum(top) / self.total if self.total else 0.0)

    def hot_partitions(self, factor: float) -> list[int]:
        median = statistics.median(self.sizes.values())
        if median == 0:
            return [p for p, s in self.sizes.items() if s > 0] if self.total else []
        return sorted(p for p, s in self.sizes.items() if s >= factor * median)

    def to_dict(self) -> dict:
        k, share = self.top_share()
        return {
            "topic": self.topic, "partitions": len(self.sizes), "bytes": self.total,
            "max_mean": round(self.max_mean, 3), "cv": round(self.cv, 3),
            "gini": round(self.gini, 3), "top_partitions": k,
            "top_share": round(share, 3),
            "sizes": {str(p): s for p, s in sorted(self.sizes.items())},
            "messages": {str(p): m for p, m in sorted(self.messages.items())},
        }


@dataclass
class Finding:
    kind: str
    subject: str
    message: str
    suggestion: str

    def to_dict(self) -> dict:
        return {"kind": self.kind, "subject": self.subject,
                "message": self.message, "suggestion": self.suggestion}


@dataclass
class Report:
    topics: list[TopicStats]
    broker_bytes: dict[int, int]
    dir_bytes: dict[tuple[int, str], int]
    findings: list[Finding]
    future_replicas: int


def human(n: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024 or unit == "TiB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return str(n)


def analyze(
    data: LogDirs,
    end_offsets: dict[tuple[str, int], int] | None = None,
    start_offsets: dict[tuple[str, int], int] | None = None,
    th: Thresholds | None = None,
    topic_filter: set[str] | None = None,
) -> Report:
    th = th or Thresholds()
    sizes: dict[str, dict[int, int]] = defaultdict(dict)
    broker_bytes: dict[int, int] = {b: 0 for b in data.brokers}
    dir_bytes: dict[tuple[int, str], int] = {d: 0 for d in data.log_dirs}
    findings: list[Finding] = []
    future = 0

    for r in data.replicas:
        if topic_filter and r.topic not in topic_filter:
            continue
        # Future replicas (an in-progress move between log dirs) still use disk.
        broker_bytes[r.broker] = broker_bytes.get(r.broker, 0) + r.size
        dir_bytes[(r.broker, r.log_dir)] = dir_bytes.get((r.broker, r.log_dir), 0) + r.size
        if r.is_future:
            future += 1
            findings.append(Finding(
                "future-replica", f"{r.topic}-{r.partition} on broker {r.broker}",
                f"being moved to {r.log_dir}: {human(r.size)} copied, offsetLag "
                f"{r.offset_lag:,}",
                "A replica move between log dirs is in progress; disk numbers "
                "include both copies until it completes.",
            ))
            continue
        # With replication, every replica of a partition is listed; use the largest.
        cur = sizes[r.topic].get(r.partition, 0)
        sizes[r.topic][r.partition] = max(cur, r.size)
        if r.offset_lag > th.lag:
            findings.append(Finding(
                "replica-lag", f"{r.topic}-{r.partition} on broker {r.broker}",
                f"offsetLag {r.offset_lag:,} on {r.log_dir}",
                "The replica's log end is behind the high watermark. Check "
                "under-replicated partitions and the broker's disk and network.",
            ))

    for broker, path, err in data.errors:
        findings.append(Finding(
            "log-dir-error", f"broker {broker} {path}", f"log dir reported error: {err}",
            "An offline or failing log dir hides its partitions from this report. "
            "Check the broker logs and disk health first.",
        ))

    topics = []
    for topic in sorted(sizes):
        stats = TopicStats(topic, sizes[topic])
        if end_offsets:
            for p in stats.sizes:
                if (topic, p) in end_offsets:
                    start = (start_offsets or {}).get((topic, p), 0)
                    stats.messages[p] = max(0, end_offsets[(topic, p)] - start)
        topics.append(stats)
        findings.extend(_topic_findings(stats, th))

    findings.extend(_share_findings(
        {f"broker {b}": v for b, v in broker_bytes.items()}, "broker-share", th,
        "Move partitions off this broker with kafka-reassign-partitions (or Cruise "
        "Control), and check whether its hot topics have skewed partitions."))
    by_broker: dict[int, dict[str, int]] = defaultdict(dict)
    for (b, path), v in dir_bytes.items():
        by_broker[b][f"broker {b} {path}"] = v
    for dirs in by_broker.values():
        findings.extend(_share_findings(
            dirs, "log-dir-share", th,
            "New partitions are placed on the log dir with the fewest partitions, "
            "not the fewest bytes. Use kafka-reassign-partitions with log_dirs to "
            "move large replicas to the emptier disk."))

    topics.sort(key=lambda t: (-t.max_mean, t.topic))
    return Report(topics, broker_bytes, dir_bytes, findings, future)


HOT_KEY_CAUSE = ("Records are probably concentrated on a few keys (hot key) or "
                 "routed by a custom partitioner.")
HOT_KEY_STEPS = ("Sample keys from the hot partitions (kafka-console-consumer "
                 "--partition N with print.key=true) and consider a "
                 "better-distributed key or key salting. Adding partitions does not "
                 "move existing data and changes the key-to-partition mapping.")
BIG_RECORDS = ("Message counts are fairly even but bytes are not: a few partitions "
               "carry much larger records. Check record sizes per key or producer, "
               "and compression settings.")


def _topic_findings(t: TopicStats, th: Thresholds) -> list[Finding]:
    if len(t.sizes) < 2 or t.total < th.min_topic_bytes:
        return []
    if t.max_mean < th.max_mean_ratio and t.cv < th.cv:
        return []
    hot = t.hot_partitions(th.hot_factor)
    k, share = t.top_share()
    msg = (f"max/mean {t.max_mean:.2f}, cv {t.cv:.2f}, gini {t.gini:.2f}; "
           f"top {k} of {len(t.sizes)} partitions hold {share:.0%} of {human(t.total)}")
    if hot:
        msg += f"; hot: {', '.join(str(p) for p in hot)}"
    suggestion = f"{HOT_KEY_CAUSE} {HOT_KEY_STEPS}"
    if t.messages and len(t.messages) == len(t.sizes):
        counts = list(t.messages.values())
        mean_count = statistics.fmean(counts)
        count_ratio = max(counts) / mean_count if mean_count else 0.0
        msg += f"; message-count max/mean {count_ratio:.2f}"
        if count_ratio >= th.max_mean_ratio:
            suggestion = ("Message counts are skewed too, so the "
                          f"partitioning key is concentrated. {HOT_KEY_STEPS}")
        elif count_ratio < th.max_mean_ratio / 1.5 and t.max_mean >= th.max_mean_ratio:
            suggestion = BIG_RECORDS
    return [Finding("partition-skew", t.topic, msg, suggestion)]


def _share_findings(values: dict[str, int], kind: str, th: Thresholds,
                    suggestion: str) -> list[Finding]:
    if len(values) < 2:
        return []
    total = sum(values.values())
    if total < th.min_topic_bytes:
        return []
    fair = total / len(values)
    out = []
    for name, v in sorted(values.items()):
        if v > fair * (1 + th.share_tolerance):
            out.append(Finding(
                kind, name,
                f"{human(v)} = {v / total:.0%} of {human(total)} "
                f"(even share {1 / len(values):.0%})",
                suggestion,
            ))
    return out
