# SimdKafka

[简体中文](README.md) · English

SimdKafka is an independent message broker written in **C# for .NET 10**. It serves the Kafka binary protocol directly over TCP, so applications can produce and consume with Kafka clients and administer topics, partitions, and groups with Kafka CLI tools. SimdKafka manages its own logs, metadata, and offsets. It runs as a single broker or as a Raft cluster, with no ZooKeeper, Apache Kafka broker, or Kafka KRaft controller required.

It is intended for teams that want a .NET messaging deployment while retaining Kafka clients and tooling. Java clients, Confluent.Kafka, and kafka-go have been tested across different interoperability scenarios. Check the API and behavior required by each application before migrating it.

[Full compatibility matrix](KAFKA_COMPATIBILITY.en.md) · [Configuration and security (中文)](CONFIGURATION.md) · [Architecture and cluster setup (中文)](IMPLEMENTATION.md) · [Benchmark results](bench/PERFORMANCE.md#english)

## Quick start

Install the .NET 10 SDK. The following commands start a single broker on `127.0.0.1:9092` and persist data under `./data`.

```sh
dotnet build SimdKafka.slnx -c Release
dotnet run --project src/SimdKafka -c Release --no-build -- --data ./data --no-auto-create
```

Create a topic with the Kafka CLI, then start the producer and consumer in separate terminals.

```sh
kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --topic orders --partitions 1 --replication-factor 1
kafka-console-producer.sh --bootstrap-server 127.0.0.1:9092 --topic orders
kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic orders --from-beginning
```

Set `bootstrap.servers=127.0.0.1:9092`, or your client's equivalent. For remote clients, set `--advertise-host` to a reachable address. TLS, SASL, and ACLs are available for external access. [Configuration and security](CONFIGURATION.md)

## Feature comparison

| Capability | Apache Kafka 4.4.0 | SimdKafka |
| --- | --- | --- |
| Native Kafka TCP | Client and administrative protocol | Common produce, consume, and administrative APIs with version negotiation |
| Storage and clustering | Segmented logs and KRaft | Durable single-node logs. Clusters use their own Raft protocol for controller metadata and partition logs |
| Produce and fetch | Idempotent produce, long polling, incremental fetch, compression | Idempotent produce, long polling, gzip, Snappy, LZ4, and Zstandard. Incremental fetch in cluster mode |
| Timestamps and offsets | Create time, append time, and timestamp lookups | `CreateTime`, `LogAppendTime`, timestamp range validation, and `ListOffsets` v11 |
| Acknowledgments and availability | ISR, `min.insync.replicas`, and leader election | Single-node fsync by default. Cluster `acks=1` and `acks=all` wait for durable replication to a Raft majority. Full ISR semantics are pending |
| Replica reads and moves | Rack awareness, reassignment, and log directory moves | Eligible rack-local reads and cross-broker reassignment. Log directory moves are pending |
| Consumer groups | Classic, modern, Share, and Streams | Classic groups. Cluster mode adds modern groups and Share consumption and acknowledgments. Streams Groups and offset expiration are pending |
| Transactions | Transactional writes, offset commits, and `read_committed` | Cluster commit, abort, producer fencing, transactional offset commits, and `read_committed` |
| Security and quotas | TLS, SASL, ACLs, tokens, and quotas | TLS/mTLS, PLAIN, SCRAM, local-JWKS OAuth, ACLs, delegation tokens, and selected quotas |
| Log lifecycle | Truncation, retention, compaction, and tiered storage | Durable `DeleteRecords` and a single-node filesystem tier. General retention, compaction, cluster tiering, and remote cleanup are pending |

Single-node mode advertises 36 API keys by default. Cluster mode advertises 55, or up to 61 with SASL and delegation tokens enabled. See the [full API matrix](KAFKA_COMPATIBILITY.en.md) for exact versions and behavior. SimdKafka and Kafka brokers use different internal protocols and form separate clusters.

## Design and verification

- [Server](src/SimdKafka/Server) and [Protocol](src/SimdKafka/Protocol) handle async sockets, frame validation, authentication, version negotiation, and request routing.
- [Storage](src/SimdKafka/Storage) manages record batches, timestamp indexes, CRC32C, fsync, recovery, and the single-node remote tier. [Cluster](src/SimdKafka/Cluster) manages Raft metadata, partition replication, transactions, and reassignment.
- The write path reuses network buffers and groups durable writes. Batch validation uses hardware CRC32C, and the timestamp index uses SIMD when available. [Implementation and wire examples (中文)](IMPLEMENTATION.md)

```sh
dotnet run --project tests/SimdKafka.Smoke -c Release --no-build
```

In separate 24-hour single-replica durable mixed workloads on the same broker host, SimdKafka and Kafka 4.4.0 processed **19,947** and **10,362 records/s**. Peak RSS was **107.3** and **1,218.3 MiB**, and average CPU use was **0.251** and **0.119 cores**. The two runs reconciled about 2.60 billion records. See the [benchmark report](bench/PERFORMANCE.md#english) for test conditions and the quick and 10-minute results.
