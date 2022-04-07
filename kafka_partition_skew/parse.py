"""Parse `kafka-log-dirs --describe` JSON and `kafka-get-offsets` output."""

from __future__ import annotations

import json
from dataclasses import dataclass


class InputError(ValueError):
    pass


@dataclass(frozen=True)
class Replica:
    broker: int
    log_dir: str
    topic: str
    partition: int
    size: int
    offset_lag: int
    is_future: bool


@dataclass
class LogDirs:
    replicas: list[Replica]
    brokers: list[int]
    log_dirs: list[tuple[int, str]]
    errors: list[tuple[int, str, str]]  # (broker, logDir, error)


def split_partition(name: str) -> tuple[str, int]:
    """'my-topic-12' -> ('my-topic', 12). Topic names may contain '-'."""
    topic, sep, num = name.rpartition("-")
    if not sep or not topic or not num.isdigit():
        raise InputError(f"unexpected partition name {name!r}")
    return topic, int(num)


def _find_json(text: str) -> dict:
    """kafka-log-dirs prints status lines before the JSON document."""
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("{"):
            try:
                return json.loads(stripped)
            except json.JSONDecodeError:
                break
    start = text.find("{")
    if start < 0:
        raise InputError("no JSON object found; expected `kafka-log-dirs --describe` output")
    try:
        return json.loads(text[start:])
    except json.JSONDecodeError as exc:
        raise InputError(f"invalid kafka-log-dirs JSON: {exc}") from exc


def parse_log_dirs(text: str) -> LogDirs:
    data = _find_json(text)
    brokers_data = data.get("brokers")
    if not isinstance(brokers_data, list):
        raise InputError("JSON has no 'brokers' list; is this kafka-log-dirs output?")
    replicas, brokers, dirs, errors = [], [], [], []
    for b in brokers_data:
        broker = int(b["broker"])
        brokers.append(broker)
        for d in b.get("logDirs", []):
            path = d.get("logDir", "?")
            dirs.append((broker, path))
            if d.get("error"):
                errors.append((broker, path, str(d["error"])))
            for p in d.get("partitions", []):
                topic, num = split_partition(p["partition"])
                replicas.append(Replica(
                    broker=broker,
                    log_dir=path,
                    topic=topic,
                    partition=num,
                    size=int(p.get("size", 0)),
                    offset_lag=int(p.get("offsetLag", 0)),
                    is_future=bool(p.get("isFuture", False)),
                ))
    return LogDirs(replicas, brokers, dirs, errors)


def parse_offsets(text: str) -> dict[tuple[str, int], int]:
    """Parse `kafka-get-offsets` output lines of the form topic:partition:offset."""
    offsets = {}
    for lineno, line in enumerate(text.splitlines(), start=1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        topic, sep1, rest = line.rpartition(":")
        topic2, sep2, part = topic.rpartition(":")
        if not (sep1 and sep2 and part.isdigit()):
            raise InputError(f"line {lineno}: expected topic:partition:offset, got {line!r}")
        try:
            offset = int(rest)
        except ValueError as exc:
            raise InputError(f"line {lineno}: offset {rest!r} is not a number") from exc
        offsets[(topic2, int(part))] = offset
    return offsets
