# kafka-partition-skew

Find partition skew in Apache Kafka topics from kafka-log-dirs output: uneven partition sizes, hot brokers and disks, and likely key problems.

## Planned scope

Parse the JSON from `kafka-log-dirs --describe` (brokers → logDirs → partitions with size, offsetLag, isFuture), aggregate per topic, partition, broker and log dir, compute skew (max/mean, coefficient of variation, gini), flag topics where a few partitions hold most data (likely hot keys), brokers/disks over a share threshold, and suggest next steps. Optional per-partition end-offset input to compare message counts vs bytes.
