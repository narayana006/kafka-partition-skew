"""Command-line interface for kafka-partition-skew."""

from __future__ import annotations

import argparse
import json
import sys

from . import __version__
from .analyze import Report, Thresholds, analyze, human
from .parse import InputError, parse_log_dirs, parse_offsets


def _read(path: str) -> str:
    if path == "-":
        return sys.stdin.read()
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="kafka-partition-skew",
        description="Find partition, broker and log-dir skew in "
                    "`kafka-log-dirs --describe` output.",
        epilog="exit codes: 0 no findings, 1 findings, 2 input error",
    )
    p.add_argument("file", nargs="?", default="-",
                   help="kafka-log-dirs --describe output ('-' or omitted: stdin)")
    p.add_argument("--end-offsets", metavar="FILE",
                   help="kafka-get-offsets output (topic:partition:offset), latest offsets")
    p.add_argument("--start-offsets", metavar="FILE",
                   help="kafka-get-offsets --time earliest output; message count = end - start")
    p.add_argument("--topic", action="append", dest="topics", metavar="NAME",
                   help="only analyse this topic (repeatable)")
    p.add_argument("--max-mean-ratio", type=float, default=2.0,
                   help="flag a topic when largest/mean partition size reaches this (default 2.0)")
    p.add_argument("--cv", type=float, default=0.5,
                   help="or when the coefficient of variation reaches this (default 0.5)")
    p.add_argument("--hot-factor", type=float, default=2.0,
                   help="a partition is hot at this multiple of the median size (default 2.0)")
    p.add_argument("--min-topic-bytes", type=int, default=1024 * 1024,
                   help="ignore topics, and skip broker/disk checks, below this size (default 1 MiB)")
    p.add_argument("--share-tolerance", type=float, default=0.25,
                   help="flag a broker or log dir holding more than (1+T) x its even share (default 0.25)")
    p.add_argument("--lag", type=int, default=10_000,
                   help="flag replicas with offsetLag above this (default 10000)")
    p.add_argument("--top", type=int, default=10, help="topics to show in the table (default 10)")
    p.add_argument("--format", choices=("text", "json"), default="text")
    p.add_argument("--exit-zero", action="store_true", help="exit 0 even with findings")
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    return p


def format_text(rep: Report, top: int) -> str:
    out = []
    total = sum(rep.broker_bytes.values())
    out.append(f"Brokers ({human(total)} on disk"
               + (f", incl. {rep.future_replicas} future replica(s)" if rep.future_replicas else "")
               + ")")
    for b, v in sorted(rep.broker_bytes.items()):
        share = v / total if total else 0
        out.append(f"  broker {b:<6} {human(v):>11}  {share:6.1%}")
        for (bb, path), dv in sorted(rep.dir_bytes.items()):
            if bb == b:
                out.append(f"    {path:<30} {human(dv):>11}")
    out.append("")
    out.append(f"Topics by skew (top {min(top, len(rep.topics))} of {len(rep.topics)})")
    out.append(f"  {'topic':<24} {'parts':>5} {'size':>11} {'max/mean':>8} {'cv':>5} {'gini':>5}  top-10%")
    for t in rep.topics[:top]:
        k, share = t.top_share()
        out.append(f"  {t.topic:<24} {len(t.sizes):>5} {human(t.total):>11} "
                   f"{t.max_mean:>8.2f} {t.cv:>5.2f} {t.gini:>5.2f}  {share:.0%} in {k}")
    out.append("")
    if not rep.findings:
        out.append("No findings.")
    else:
        out.append(f"Findings ({len(rep.findings)})")
        for f in rep.findings:
            out.append(f"  [{f.kind}] {f.subject}: {f.message}")
            out.append(f"      -> {f.suggestion}")
    return "\n".join(out)


def format_json(rep: Report) -> str:
    return json.dumps({
        "brokers": {str(b): v for b, v in sorted(rep.broker_bytes.items())},
        "log_dirs": [{"broker": b, "logDir": p, "bytes": v}
                     for (b, p), v in sorted(rep.dir_bytes.items())],
        "future_replicas": rep.future_replicas,
        "topics": [t.to_dict() for t in rep.topics],
        "findings": [f.to_dict() for f in rep.findings],
    }, indent=2)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        data = parse_log_dirs(_read(args.file))
        end = parse_offsets(_read(args.end_offsets)) if args.end_offsets else None
        start = parse_offsets(_read(args.start_offsets)) if args.start_offsets else None
    except (OSError, InputError) as exc:
        print(f"kafka-partition-skew: error: {exc}", file=sys.stderr)
        return 2
    th = Thresholds(args.max_mean_ratio, args.cv, args.hot_factor,
                    args.min_topic_bytes, args.share_tolerance, args.lag)
    rep = analyze(data, end, start, th, set(args.topics) if args.topics else None)
    print(format_json(rep) if args.format == "json" else format_text(rep, args.top))
    return 1 if rep.findings and not args.exit_zero else 0


if __name__ == "__main__":
    sys.exit(main())
