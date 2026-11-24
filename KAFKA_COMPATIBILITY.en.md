# SimdKafka and Apache Kafka 4.4.0 compatibility

[简体中文](KAFKA_COMPATIBILITY.md) · English

SimdKafka advertises **36 API keys in single-node mode** and **55 in cluster mode**. SASL adds two keys and delegation tokens add four. A configured cluster can advertise **61 of the 79 stable API keys** in the Kafka 4.4 protocol table. The key count measures negotiated endpoints. Use the tables below to check versions and behavior. [Apache Kafka protocol](https://kafka.apache.org/44/design/protocol/#api-keys) · [Single-node router](src/SimdKafka/Protocol/Router.cs) · [Cluster router](src/SimdKafka/Protocol/ClusterRouter.cs)

## Deployment and storage

| Capability | Apache Kafka 4.4.0 | SimdKafka |
| --- | --- | --- |
| Kafka TCP and discovery | Kafka wire protocol, metadata, and separate listener roles | Kafka wire protocol with configurable advertised endpoints. One client listener per broker |
| Metadata and controller quorum | KRaft controller election, broker registration, and quorum management | Independent Raft controller and durable metadata. Broker and voter membership are configured at startup. `DescribeQuorum` exposes status and replication progress |
| Partition durability | ISR replication and `min.insync.replicas` | Single-node fsync by default. Cluster `acks=1` and `acks=all` both wait for durable replication to a Raft majority. Full ISR semantics and preferred leader elections are pending |
| Replica reads and migration | Rack-aware follower fetch, reassignment, and log directory moves | Eligible rack-local reads and cross-broker reassignment with progress and cancellation. Log directory moves are pending |
| Recovery | Segment and replica recovery | Single-node restart rebuilds indexes and truncates incomplete tails. Cluster recovery replays the WAL. More disk replacement scenarios remain to be validated |
| Retention and compaction | Size and time retention, key compaction, and offset truncation | Durable logical `DeleteRecords`. General retention, key compaction, and reclamation of active cluster WAL data are pending |
| Tiered storage | Pluggable remote storage with tier-aware replication | Single-node filesystem archive with SHA-256 validation, sparse block reclamation, cross-tier fetch, and offset lookups. Cluster tiering, object storage plugins, and remote expiration are pending |

## Client and administration behavior

| Capability | Apache Kafka 4.4.0 | SimdKafka |
| --- | --- | --- |
| Produce and fetch | Magic 2 batches, compression, long polling, and incremental fetch | Magic 2 with gzip, Snappy, LZ4, and Zstandard. One batch per Produce partition entry. Encoded payloads are preserved. Incremental fetch in cluster mode |
| Timestamps | `CreateTime`, `LogAppendTime`, and timestamp bounds | Both timestamp modes, configurable bounds, and `ListOffsets` through v11 |
| Producer idempotence | Producer IDs, sequence deduplication, epoch fencing, and expiration | Recent sequence ranges are deduplicated, and producer fencing survives leader changes and restart. Automatic expiration of non-transactional producer state is pending |
| Transactions | Transactional writes and offset commits, isolation, and coordinator recovery | Cluster commit and abort, transactional offset commits, `read_committed`, v2 transactions, and the implemented subset of KIP-939 recovery |
| Consumer groups | Classic, modern, Share, and Streams groups | Classic groups in both modes. Cluster adds KIP-848 groups and Share Groups. Streams Groups and automatic offset expiration are pending |
| Share Groups | Shared partition delivery, acknowledgment, and group operations | Cluster mode supports acquisition, accept, release, reject, lock renewal, redelivery, and offset administration. Internal Kafka Share state RPCs are handled by SimdKafka's own Raft state |
| Topic and broker administration | Topic and partition management and dynamic configurations | Topic and partition management, batch sizes, timestamp settings, and single-node tier settings. Other policies and config resources remain limited |
| Security | TLS, SASL, ACLs, delegation tokens, and SCRAM credentials | TLS/mTLS and SASL_SSL with PLAIN, SCRAM, and RS256 OAuth using a local JWKS. Persistent ACLs, delegation tokens, and online SCRAM. Kerberos, SASL_PLAINTEXT, and automatic HTTPS JWKS refresh are pending |
| Quotas and telemetry | Client quota types and telemetry collection | Producer and consumer byte rates and IP connection creation limits. Request utilization quotas, controller mutation quotas, and client telemetry are pending |

Interoperability tests cover Java clients and tooling, Confluent.Kafka, and kafka-go across different scenarios. The [implementation guide](IMPLEMENTATION.md) describes platform limits and the [benchmark report](bench/PERFORMANCE.md#english) describes measured workloads. [Apache Kafka protocol](https://kafka.apache.org/44/design/protocol/) · [Streams rebalance protocol](https://kafka.apache.org/44/streams/developer-guide/streams-rebalance-protocol/)

## API versions by key

The Kafka column follows the Apache Kafka 4.4 protocol table. A version range without a mode annotation applies to both SimdKafka modes. SASL and delegation token keys are advertised when configured. `InitProducerId` v6 implements the KIP-939 wire protocol and is marked unstable in the Apache Kafka 4.4.0 request schema. [Protocol messages](https://kafka.apache.org/44/design/protocol/#the-messages) · [InitProducerId schema](https://github.com/apache/kafka/blob/4.4.0/clients/src/main/resources/common/message/InitProducerIdRequest.json)

| Key | API | Kafka 4.4 | SimdKafka | Scope |
| ---: | --- | --- | --- | --- |
| 0 | `Produce` | v3–13 | v3–13 | Magic 2, idempotence, topic IDs in v13, and cluster transactions |
| 1 | `Fetch` | v4–18 | v4–13 | Long polling and v13 topic IDs. Cluster incremental sessions, replica reads, and transaction isolation |
| 2 | `ListOffsets` | v1–11 | Single v0–11<br>Cluster v1–11 | Earliest, latest, timestamp, maximum timestamp, and tier positions |
| 3 | `Metadata` | v0–13 | v0–13 | Broker, topic, and partition metadata and v12 topic ID lookups |
| 8 | `OffsetCommit` | v2–10 | Single v0–7<br>Cluster v0–10 | Consumer offset commits, static membership, and cluster v10 topic IDs |
| 9 | `OffsetFetch` | v1–10 | Single v0–5<br>Cluster v0–10 | Cluster group batching, stable reads, member epochs, and topic IDs |
| 10 | `FindCoordinator` | v0–6 | v0–6 | Classic groups and cluster modern groups, Share Groups, and transactions |
| 11 | `JoinGroup` | v0–9 | v0–5 | Classic dynamic and static joins |
| 12 | `Heartbeat` | v0–4 | v0–3 | Classic sessions and static identity checks |
| 13 | `LeaveGroup` | v0–5 | v0–3 | Classic group leave and v3 batch operations |
| 14 | `SyncGroup` | v0–5 | v0–3 | Classic client assignments |
| 15 | `DescribeGroups` | v0–6 | v0–6 | Classic group state, members, and assignments |
| 16 | `ListGroups` | v0–5 | v0–5 | Group listing with cluster modern and Share types |
| 17 | `SaslHandshake` | v0–1 | v0–1 | Negotiates enabled SASL mechanisms |
| 18 | `ApiVersions` | v0–5 | v0–5 | API negotiation with cluster and broker ID checks in v5 |
| 19 | `CreateTopics` | v2–7 | v0–7 | Topic creation, validate-only, and replica placement |
| 20 | `DeleteTopics` | v1–6 | v0–6 | Deletes by name or ID |
| 21 | `DeleteRecords` | v0–2 | v0–2 | Durable log-start advancement |
| 22 | `InitProducerId` | v0–6 | Single v0–5<br>Cluster v0–6 | Producer IDs and cluster transaction fencing and recovery |
| 23 | `OffsetForLeaderEpoch` | v2–4 | v2–4 | Log end for a partition leader epoch |
| 24 | `AddPartitionsToTxn` | v0–5 | Cluster v0–3 | Client-side partition enlistment |
| 25 | `AddOffsetsToTxn` | v0–4 | Cluster v0–4 | Enlists a consumer group in a transaction |
| 26 | `EndTxn` | v0–5 | Cluster v0–5 | Commits or aborts a transaction |
| 27 | `WriteTxnMarkers` | v1–2 | Not advertised | Transaction markers use SimdKafka internal RPC |
| 28 | `TxnOffsetCommit` | v0–6 | Cluster v0–6 | Transactional offset commits and v6 topic IDs |
| 29 | `DescribeAcls` | v1–3 | v0–3 | Filters ACLs |
| 30 | `CreateAcls` | v1–3 | v0–3 | Creates literal or prefixed ACLs |
| 31 | `DeleteAcls` | v1–3 | v0–3 | Removes matching ACLs and returns details |
| 32 | `DescribeConfigs` | v1–4 | v0–4 | Broker and topic settings and inheritance |
| 33 | `AlterConfigs` | v0–2 | v0–2 | Replaces supported settings, including validate-only |
| 34 | `AlterReplicaLogDirs` | v1–2 | Not implemented | Replica log directory moves |
| 35 | `DescribeLogDirs` | v1–5 | v1–4 | Local replica size and volume capacity |
| 36 | `SaslAuthenticate` | v0–2 | v0–2 | PLAIN, SCRAM, and OAUTHBEARER over TLS |
| 37 | `CreatePartitions` | v0–3 | v0–3 | Partition expansion and cluster placement |
| 38 | `CreateDelegationToken` | v1–3 | v1–3 | Creates a delegation token when enabled |
| 39 | `RenewDelegationToken` | v1–2 | v1–2 | Renews a delegation token |
| 40 | `ExpireDelegationToken` | v1–2 | v1–2 | Revokes a token or changes expiration |
| 41 | `DescribeDelegationToken` | v1–3 | v1–3 | Lists tokens visible to the requester |
| 42 | `DeleteGroups` | v0–3 | v0–3 | Deletes idle groups, including cluster Share Groups |
| 43 | `ElectLeaders` | v0–2 | Not implemented | Manual partition leader election |
| 44 | `IncrementalAlterConfigs` | v0–1 | v0–1 | Sets or deletes supported settings, including validate-only |
| 45 | `AlterPartitionReassignments` | v0–1 | Cluster v0–1 | Starts or cancels reassignment |
| 46 | `ListPartitionReassignments` | v0 | Cluster v0 | Shows reassignment progress |
| 47 | `OffsetDelete` | v0 | v0 | Deletes group offsets for selected partitions |
| 48 | `DescribeClientQuotas` | v0–1 | v0–1 | Lists user and client bandwidth and IP connection limits |
| 49 | `AlterClientQuotas` | v0–1 | v0–1 | Sets or removes supported quotas |
| 50 | `DescribeUserScramCredentials` | v0 | v0 | Lists dynamic SCRAM mechanisms and iterations |
| 51 | `AlterUserScramCredentials` | v0 | v0 | Creates, rotates, or deletes dynamic SCRAM credentials |
| 55 | `DescribeQuorum` | v0–2 | Cluster v0–2 | SimdKafka controller leadership and log progress |
| 57 | `UpdateFeatures` | v0–2 | Not implemented | Cluster feature levels |
| 60 | `DescribeCluster` | v0–2 | v0–2 | Cluster ID and broker endpoints |
| 61 | `DescribeProducers` | v0 | Cluster v0 | Producer sequences, epochs, and open transaction offsets |
| 64 | `UnregisterBroker` | v0 | Not implemented | Broker deregistration |
| 65 | `DescribeTransactions` | v0 | Cluster v0 | Transaction state, identity, timeout, and partitions |
| 66 | `ListTransactions` | v0–2 | Cluster v0–2 | Filtered transaction listing |
| 68 | `ConsumerGroupHeartbeat` | v0–1 | Cluster v0–1 | KIP-848 sessions, subscription, and cooperative assignment |
| 69 | `ConsumerGroupDescribe` | v0–1 | Cluster v0–1 | Modern group state, epochs, and assignments |
| 71 | `GetTelemetrySubscriptions` | v0 | Not implemented | Client telemetry subscriptions |
| 72 | `PushTelemetry` | v0 | Not implemented | Client telemetry uploads |
| 74 | `ListConfigResources` | v0–1 | v1 | Lists topics and brokers with dynamic overrides |
| 75 | `DescribeTopicPartitions` | v0 | v0 | Paginates topic and partition metadata |
| 76 | `ShareGroupHeartbeat` | v1 | Cluster v1 | Share Group membership and assignment |
| 77 | `ShareGroupDescribe` | v1 | Cluster v1 | Share Group members, subscriptions, and assignments |
| 78 | `ShareFetch` | v1–2 | Cluster v1–2 | Acquires records from shared partitions |
| 79 | `ShareAcknowledge` | v1–2 | Cluster v1–2 | Accepts, rejects, releases, or renews a delivery lock |
| 80 | `AddRaftVoter` | v0–1 | Not implemented | Dynamic KRaft quorum membership |
| 81 | `RemoveRaftVoter` | v0 | Not implemented | Dynamic KRaft quorum membership |
| 83 | `InitializeShareGroupState` | v0 | Not advertised | Share cursor initialization uses Raft |
| 84 | `ReadShareGroupState` | v0 | Not advertised | Delivery state is read from Raft |
| 85 | `WriteShareGroupState` | v0–1 | Not advertised | Delivery updates are stored through Raft |
| 86 | `DeleteShareGroupState` | v0 | Not advertised | State deletion is replicated through Raft |
| 87 | `ReadShareGroupStateSummary` | v0–1 | Not advertised | Public queries use `DescribeShareGroupOffsets` |
| 88 | `StreamsGroupHeartbeat` | v0–1 | Not implemented | Streams Group member and task coordination |
| 89 | `StreamsGroupDescribe` | v0–1 | Not implemented | Streams Group status and assignment |
| 90 | `DescribeShareGroupOffsets` | v0–1 | Cluster v0–1 | Share offsets and eligible lag queries |
| 91 | `AlterShareGroupOffsets` | v0 | Cluster v0 | Resets offsets for idle groups |
| 92 | `DeleteShareGroupOffsets` | v0 | Cluster v0 | Clears offsets and delivery state for idle groups |
| 93 | `StreamsGroupTopologyDescriptionUpdate` | v0 | Not implemented | Streams topology descriptions |
| 94 | `UnregisterController` | v0 | Not implemented | Controller deregistration |
