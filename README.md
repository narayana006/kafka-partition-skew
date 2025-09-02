# kafka-partition-skew

Find partition skew in Apache Kafka from `kafka-log-dirs --describe` output: uneven partition sizes, hot brokers and disks, and whether the cause looks like a hot key or a few oversized records.

**Status:** v0.1. Parses the `kafka-log-dirs` JSON (checked against real Kafka 3.7.0 and 3.9.0 output), computes per-topic skew (max/mean, coefficient of variation, Gini, top-10% share), flags hot partitions, brokers and log dirs, lagging and in-flight (future) replicas, and uses optional `kafka-get-offsets` output to tell key skew from record-size skew. Offline only: it reads files, it never connects to a cluster.

## Install

```bash
pip install git+https://github.com/narayana006/kafka-partition-skew
```

Python 3.10+, no runtime dependencies.

## Quick start

`examples/kafka-log-dirs-3-brokers.txt` is an **illustrative** three-broker cluster (generated, not captured) with a hot-key topic, a topic with a few huge records, a broker holding extra data, and a replica move in progress:

```bash
kafka-partition-skew examples/kafka-log-dirs-3-brokers.txt --end-offsets examples/end-offsets-3-brokers.txt
```

```text
Brokers (241.3 GiB on disk, incl. 1 future replica(s))
  broker 1         68.3 GiB   28.3%
    /var/lib/kafka/data-0             38.9 GiB
    /var/lib/kafka/data-1             29.4 GiB
  broker 2        104.1 GiB   43.1%
    /var/lib/kafka/data-0             39.5 GiB
    /var/lib/kafka/data-1             64.6 GiB
  broker 3         68.9 GiB   28.6%
    /var/lib/kafka/data-0             29.6 GiB
    /var/lib/kafka/data-1             39.3 GiB

Topics by skew (top 4 of 4)
  topic                    parts        size max/mean    cv  gini  top-10%
  clickstream                  6    35.6 GiB     3.30  1.03  0.39  55% in 1
  media-uploads                4     9.2 GiB     2.69  0.98  0.43  67% in 1
  payments                    12    23.5 GiB     1.04  0.02  0.01  17% in 2
  audit-log                    3    35.8 GiB     1.04  0.03  0.01  35% in 1

Findings (4)
  [future-replica] payments-0 on broker 3: being moved to /var/lib/kafka/data-1: 614.4 MiB copied, offsetLag 1,830,442
      -> A replica move between log dirs is in progress; disk numbers include both copies until it completes.
  [partition-skew] clickstream: max/mean 3.30, cv 1.03, gini 0.39; top 1 of 6 partitions hold 55% of 35.6 GiB; hot: 4; message-count max/mean 3.30
      -> Message counts are skewed too, so the partitioning key is concentrated. Sample keys from the hot partitions (kafka-console-consumer --partition N with print.key=true) and consider a better-distributed key or key salting. Adding partitions does not move existing data and changes the key-to-partition mapping.
  [partition-skew] media-uploads: max/mean 2.69, cv 0.98, gini 0.43; top 1 of 4 partitions hold 67% of 9.2 GiB; hot: 1; message-count max/mean 1.01
      -> Message counts are fairly even but bytes are not: a few partitions carry much larger records. Check record sizes per key or producer, and compression settings.
  [broker-share] broker 2: 104.1 GiB = 43% of 241.3 GiB (even share 33%)
      -> Move partitions off this broker with kafka-reassign-partitions (or Cruise Control), and check whether its hot topics have skewed partitions.
```

Exit code `1` because there are findings.

`examples/kafka-log-dirs-single-broker.txt` and `examples/end-offsets-single-broker.txt` are **real** output from a throwaway single-broker Kafka 3.9.0 container with two log dirs, where 75% of the `orders` records used one key. It is tiny, so lower the size floor to see the findings:

```bash
kafka-partition-skew examples/kafka-log-dirs-single-broker.txt \
  --end-offsets examples/end-offsets-single-broker.txt --min-topic-bytes 0
```

```text
Brokers (521.2 KiB on disk)
  broker 1        521.2 KiB  100.0%
    /tmp/kraft-logs-a                140.2 KiB
    /tmp/kraft-logs-b                380.9 KiB

Topics by skew (top 2 of 2)
  topic                    parts        size max/mean    cv  gini  top-10%
  orders                       6   344.5 KiB     4.51  1.57  0.62  75% in 1
  clicks                       4   176.7 KiB     1.03  0.02  0.01  26% in 1

Findings (2)
  [partition-skew] orders: max/mean 4.51, cv 1.57, gini 0.62; top 1 of 6 partitions hold 75% of 344.5 KiB; hot: 3; message-count max/mean 4.52
      -> Message counts are skewed too, so the partitioning key is concentrated. Sample keys from the hot partitions (kafka-console-consumer --partition N with print.key=true) and consider a better-distributed key or key salting. Adding partitions does not move existing data and changes the key-to-partition mapping.
  [log-dir-share] broker 1 /tmp/kraft-logs-b: 380.9 KiB = 73% of 521.2 KiB (even share 50%)
      -> New partitions are placed on the log dir with the fewest partitions, not the fewest bytes. Use kafka-reassign-partitions with log_dirs to move large replicas to the emptier disk.
```

## Getting the input

```bash
kafka-log-dirs.sh --bootstrap-server broker:9092 --command-config admin.properties \
  --describe > log-dirs.txt
# optional: restrict to some topics
kafka-log-dirs.sh --bootstrap-server broker:9092 --describe --topic-list orders,clicks > log-dirs.txt

# optional: message counts per partition
kafka-get-offsets.sh --bootstrap-server broker:9092 --topic-partitions orders,clicks > end.txt
kafka-get-offsets.sh --bootstrap-server broker:9092 --topic-partitions orders,clicks --time earliest > start.txt

kafka-partition-skew log-dirs.txt --end-offsets end.txt --start-offsets start.txt
```

The tool skips the `Querying brokers ...` / `Received log directory information ...` lines that `kafka-log-dirs` prints before the JSON. Offsets files use the `topic:partition:offset` lines that `kafka-get-offsets` prints. Without `--start-offsets` the message count is the end offset, which overstates counts for partitions where retention already deleted data.

**Permissions** (if your cluster uses ACLs): `kafka-log-dirs` calls DescribeLogDirs, which needs `DESCRIBE` on the `CLUSTER` resource; `kafka-get-offsets` needs `DESCRIBE` on the topics. The tool itself needs no access to anything.

## How it computes things

- **Partition size** is the largest current (non-future) replica of that partition across brokers.
- **Broker and log-dir totals** add up every replica on that broker or disk, including future replicas, because they use disk.
- **Topic skew** uses partition sizes: `max/mean`, coefficient of variation (population stdev / mean), Gini coefficient, and the share held by the largest 10% of partitions (at least one). A topic is flagged when `max/mean >= --max-mean-ratio` (2.0) or `cv >= --cv` (0.5) and it holds at least `--min-topic-bytes` (1 MiB). Hot partitions are those at `--hot-factor` (2.0) times the median size or more.
- **Hot key vs big records**: with `--end-offsets`, if message counts are skewed too (count max/mean at or above the ratio) the key is concentrated; if counts are fairly even (below ratio / 1.5) while bytes are skewed, a few partitions carry larger records.
- **Brokers and log dirs**: flagged when they hold more than `(1 + --share-tolerance)` times an even share (brokers against the cluster, log dirs against the other dirs of the same broker). Skipped when the total is below `--min-topic-bytes`.
- **Replica lag**: current replicas with `offsetLag` above `--lag` (10000). **Future replicas** (`isFuture: true`, a move between log dirs in progress) and log dirs with an `error` are always reported.

## Usage

```text
kafka-partition-skew [FILE] [--end-offsets FILE] [--start-offsets FILE]
                     [--topic NAME]... [--max-mean-ratio R] [--cv C]
                     [--hot-factor F] [--min-topic-bytes N]
                     [--share-tolerance T] [--lag N] [--top N]
                     [--format text|json] [--exit-zero]
```

`FILE` defaults to stdin, so `kafka-log-dirs.sh ... --describe | kafka-partition-skew` works. `--format json` prints broker and log-dir totals, per-topic stats with every partition size and message count, and the findings.

Exit codes: `0` no findings (or `--exit-zero`), `1` findings, `2` unreadable or unparseable input.

## Features

- [x] Parse `kafka-log-dirs --describe` JSON (with the status lines it prints)
- [x] Per-topic skew: max/mean, CV, Gini, top-10% share, hot partitions
- [x] Broker and log-dir share checks
- [x] Replica lag, future replicas and log-dir errors
- [x] Optional `kafka-get-offsets` input: message counts vs bytes
- [x] Suggested next step for every finding
- [x] Text and JSON output, CI exit codes
- [ ] Leader-count skew (needs `kafka-topics --describe` input)
- [ ] Proposed reassignment JSON
- [ ] Trend over several snapshots

## Limitations

- Bytes on disk only: no throughput rates, no leader placement, and no rack awareness.
- Compacted topics and topics with per-partition retention differences can look skewed without a key problem.
- Brokers that did not answer are absent from the `kafka-log-dirs` output and therefore from the report.
- The three-broker example is generated for illustration; the single-broker example is a real capture.

## Contributing

Issues and pull requests are welcome. If parsing fails on your cluster's output, please include an anonymised snippet.

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
pytest -q
```

Built and maintained by [Narayana Vemburaj](https://github.com/narayana006).

Licensed under the [MIT License](LICENSE).
