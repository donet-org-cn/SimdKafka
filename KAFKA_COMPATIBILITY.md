# SimdKafka 与 Apache Kafka 4.4.0 功能对照

简体中文 · [English](KAFKA_COMPATIBILITY.en.md)

核对日期为 2026-11-21。SimdKafka 单节点公布 **36 个 API key**，集群公布 **55 个**。两种模式启用 SASL_SSL 后各增加 **2 个**，启用委派令牌再增加 **4 个**，集群最多公布 **61 个**。Kafka 4.4 官方协议表列出 **79 个稳定 API key**，其中 **18 个**仍未公布，包括 Kafka Broker 和控制器内部接口。版本及语义范围见各行。 [单节点路由](src/SimdKafka/Protocol/Router.cs) · [集群路由](src/SimdKafka/Protocol/ClusterRouter.cs) · [Kafka 4.4 官方 API Keys 表](https://kafka.apache.org/44/design/protocol/#api-keys)

## 部署、集群与高可用

| 能力 | Apache Kafka 4.4.0 | SimdKafka 当前实现 |
| --- | --- | --- |
| 原生 Kafka TCP | 支持客户端版本协商、Broker 和控制器监听器 | **支持**原生 Kafka TCP。默认监听 `127.0.0.1:9092`，可设置对外监听和公布地址 |
| 无 ZooKeeper | KRaft 管理集群元数据 | **支持**独立运行。C# 与 .NET 10，在本地管理元数据和日志 |
| 集群元数据 | Broker、主题、分区、Topic ID、leader、ISR、机架与控制器 | **部分支持**。集群视图公布多 Broker、Topic ID、分区、副本和 leader epoch。ISR 公布经过确认的 leader。Broker 注册来自启动配置 |
| 多 Broker、分区副本与高可用 | 支持副本同步、ISR、选主、故障切换、机架感知与优先副本选举 | **部分支持**。每个分区使用独立 Raft 日志，多数派落盘后确认。三副本可在一台故障后继续写入。尚无 Kafka ISR 与首选副本选举语义 |
| 控制器与元数据仲裁 | KRaft 选举、仲裁成员管理、注册和功能级别 | **部分支持**。独立 Raft 控制器仲裁及故障切换，复制 Topic ID、分区、副本分配、producer ID 和消费 offset。`DescribeQuorum` 查询 leader、任期、提交进度和成员。控制器集合在启动时配置 |
| 读副本与副本迁移 | 支持机架感知优选副本、分区副本重分配、日志目录迁移 | **部分支持**。Fetch v11 可选择追上请求 offset 的同机架副本。原生管理 API 发起、查询和取消副本重分配。尚无日志目录迁移 |

[Kafka KRaft](https://kafka.apache.org/44/operations/kraft/) · [Kafka 设计](https://kafka.apache.org/44/design/design/) · [SimdKafka 启动参数](src/SimdKafka/Program.cs)

## 生产、拉取与消费组

| 能力 | Apache Kafka 4.4.0 | SimdKafka 当前实现 |
| --- | --- | --- |
| 消息格式和顺序 | Magic 2 批次，支持 key、value、headers、时间戳和分区内 offset | **支持** Magic 2，保留这些字段和分区内顺序。Produce v3–13 每个分区项接收一个 Magic 2 批次，v13 通过 Topic ID 路由 |
| 消息确认 | `acks=0/1/all`。`acks=all` 与 ISR 和 `min.insync.replicas` 一起决定可确认性 | **部分支持** `0/1/-1`。单节点默认在本地刷盘后确认。集群 `1` 和 `-1` 均等待 Raft 多数副本持久化。`0` 不返回确认 |
| 压缩与转码 | 无压缩、gzip、Snappy、LZ4、Zstandard，支持 Topic 压缩策略 | **部分支持**。支持上述四种压缩和无压缩，Zstandard 要求 Produce v7+ 和 Fetch v10+。限量解压校验后按原编码落盘及拉取，无 Broker 转码 |
| 时间戳 | `CreateTime`、`LogAppendTime` 及 CreateTime 时钟偏差限制 | **支持**。Topic 可在线切换，缺省继承集群级 Broker 默认值。Produce、Fetch、ListOffsets 和幂等重试使用落盘批次的时间。支持 CreateTime 的前后范围，超限批次不落盘 |
| 非事务幂等写入 | Producer ID、epoch、序列与去重，并按配置过期生产者状态 | **支持 epoch、序列和去重**。同一分区识别最近五批的重复序列范围，返回原 offset 并保留原消息。已观察到的更高 epoch 会隔离旧批次，切主和重启后仍生效。生产者状态自动过期待实现 |
| 事务与 exactly-once | 事务协调、提交和回滚、跨分区事务、事务内提交消费 offset | **集群支持客户端事务**。包括提交、回滚、旧 producer 隔离、超时回滚、事务内消费 offset 和协调器接管。事务 v2 支持自动登记和逐事务提升 epoch。集群实现 KIP-939 的 v6 开启与保留事务，以及恢复身份提交或回滚。空闲 transactional ID 会过期 |
| 消费的隔离级别 | `read_uncommitted` 和事务感知的 `read_committed` | **集群支持**。使用 LSO 限制可读范围，以标记和 aborted index 让客户端跳过回滚数据。单节点仅有非事务记录 |
| 手动分区消费与长轮询 | 支持指定分区、起始 offset、等待时间和字节预算 | **支持**。可自行分配分区、设置读取位置，支持 `min_bytes`、字节预算及长轮询 |
| Fetch 会话与 Topic ID | 支持全量及增量 Fetch，支持 Topic ID 和副本读取 | **部分支持**。v13 按 Topic ID 读取，集群支持从本地副本读取和 TCP 连接内的增量会话，每个会话最多 256 个分区。重新连接时客户端重建会话。单节点使用全量 Fetch |
| offset 定位 | 最早、最新、按时间、leader epoch、最新归档位置和最早待上传位置 | **支持这些定位类型**。单节点分层 Topic 返回最新归档和最早待归档 offset，未启用分层时返回未找到。远端时间查找的超时以文件系统读取返回为边界 |
| 经典动态消费组 | 支持 Join、Sync、心跳、离组、客户端分配与再平衡 | **支持**动态经典组及代际校验。消费组成员和分配存于内存，重启后重新入组 |
| 静态成员与服务端分配 | 经典静态成员和 KIP-848 新消费组协议 | **集群支持 KIP-848 按名称及服务端正则订阅**。成员、epoch 和分配由 Raft 持久化，支持动态和静态成员、协作式撤销、`range` 和按负载分配的 `uniform`。支持空组切换协议。混合组在线迁移、机架感知分配和完全一致的 Kafka `uniform` 分配顺序待实现 |
| 消费 offset | 支持提交、查询、重置所需 API、事务提交和自动过期 | **部分支持**。可提交和查询指定分区或组内全部 offset。集群支持事务提交、稳定性查询、多组查询、现代成员 epoch 校验和 v10 Topic ID。自动过期待实现 |
| 消费组运维 | 列出、描述、删除组和指定分区的消费 offset | **部分支持**。两种模式均支持经典组运维。集群还可按类型筛选和描述现代组，支持删除空组及删除未被订阅分区的 offset |
| Share Groups | 支持同分区并行领取、单条确认和重新投递 | **集群支持原生消费和 offset 运维**。包括心跳、描述、同分区多成员领取、确认、释放、拒绝、续租和超时重投。控制器 Raft 保留状态。动态 Share 配置、Apache Kafka 内部 Share 状态 RPC 与精确的分配策略待实现 |
| Streams Group 协议 | KIP-1071 由 Broker 协调 Kafka Streams 的应用实例与任务，KIP-1331 支持拓扑描述 | **未实现**。缺少持久化的拓扑、任务协调、组管理和拓扑描述。 [Streams Rebalance Protocol](https://kafka.apache.org/44/streams/developer-guide/streams-rebalance-protocol/) |

[Kafka 4.4 协议](https://kafka.apache.org/44/design/protocol/) · [Kafka 4.4 消费组协议](https://kafka.apache.org/44/operations/consumer-rebalance-protocol/) · [经典组实现](src/SimdKafka/Groups/GroupCoordinator.cs) · [现代组实现](src/SimdKafka/Cluster/ReplicatedMetadata.ConsumerGroups.cs)

## 持久化、Topic 与运维

| 能力 | Apache Kafka 4.4.0 | SimdKafka 当前实现 |
| --- | --- | --- |
| 落盘与恢复 | 分区分段日志、刷盘策略、恢复和副本机制 | **部分支持**。单节点追加日志启动时重建索引，截断不完整尾批。集群使用带完整性校验的分区 WAL，按分区合并批次，多数派复制后推进已提交位置 |
| 元数据与 offset 持久化 | 通过 KRaft 管理集群元数据，使用内部 Topic 保存组 offset 等状态 | **支持独立持久化**。单节点刷盘并原子替换带 SHA-256 的目录快照。集群元数据和 offset 由控制器仲裁复制，使用带 SHA-256 的快照与 WAL 恢复 |
| 磁盘故障 | 配置足够副本时可从其他副本恢复或接替读写 | **部分支持**。读取核对 CRC32C 和批次边界。集群在有多数副本时可重新选主和追赶，损坏磁盘更换的完整流程待验证 |
| 日志生命周期 | 分段滚动、按大小或时间保留、按 key 压实、按 offset 删历史记录 | **部分支持**。两种模式支持持久化 `DeleteRecords`。单节点分层主题按本地字节数或记录时间归档，可回收已归档本地块。通用保留、压实、远端过期清理和集群 WAL 回收待实现 |
| 分层存储 | 支持可插拔远端存储 | **单节点支持挂载文件系统后端**。按完整批次归档、验证 SHA-256 后提交目录并释放本地空间。原生 Fetch 可跨层读取，基于 Topic 配置启用。集群副本远端恢复、存储插件与远端生命周期管理待实现 |
| 创建 Topic | 支持默认分区、显式副本分配、Topic 自定义配置、validate-only | **部分支持**。两种模式支持自动或显式创建、validate-only、显式副本分配、`max.message.bytes` 和 `message.timestamp.type` |
| 扩分区和删除 Topic | 支持增加分区、按名称或 Topic ID 删除 | **支持**。两种模式均支持扩分区、按名称或 ID 删除、回收已删除 Topic 的目录及同名重建。启用双向 TLS 和端口回收的集群还会释放对应的副本端口槽位。迁移移出的副本目录保留至 Topic 删除 |
| 查询集群与分区 | 支持集群、Topic 和分区详情及分页 | **部分支持**。集群提供多 Broker 和分区副本视图，`DescribeTopicPartitions` 支持游标，`DescribeCluster` 仅支持 Broker 端点 |
| 配置管理 | 查询及动态修改 Broker、Topic、Group 等配置 | **部分支持**。在线修改消息上限、时间戳类型及单节点分层主题的 `remote.storage.enable`、`segment.bytes`、`local.retention.bytes` 和 `local.retention.ms`。可列出有显式覆盖的 Topic 和 Broker。其他日志策略、Group 和客户端遥测配置待实现。 [配置说明](CONFIGURATION.md) |
| 日志目录运维 | 查询目录和副本占用，移动副本与停用目录 | **部分支持**。单节点和集群可查询所在卷容量和已提交日志的编码字节数。同节点目录迁移与停用待实现 |
| 网络、内存与计算 | Java 实现具备自身 I/O、缓存和配额机制 | **支持 .NET 实现**。异步 Socket、池化缓冲区、按字节切片解析、SIMD 时间索引查找和硬件 CRC32C。操作系统 I/O 与扩容仍可能复制数据 |

[Kafka Topic 配置](https://kafka.apache.org/44/configuration/topic-configs/) · [分层存储](https://kafka.apache.org/44/operations/tiered-storage/) · [SimdKafka 存储目录](src/SimdKafka/Storage/Catalog.cs) · [分区日志](src/SimdKafka/Storage/PartitionLog.cs)

## 安全、配额与生态

| 能力 | Apache Kafka 4.4.0 | SimdKafka 当前实现 |
| --- | --- | --- |
| 明文 TCP 与 TLS | 支持 PLAINTEXT、SSL、多监听器和各自安全设置 | **部分支持**。一个 TCP 监听器，选择 PLAINTEXT 或 TLS。TLS 1.2/1.3，支持 PFX 和可选客户端证书校验 |
| SASL 机制与传输 | 支持 PLAIN、SCRAM、GSSAPI、OAUTHBEARER，支持 SASL_SSL 和 SASL_PLAINTEXT | **部分支持**。SASL_SSL 支持 PLAIN、SCRAM-SHA-256、SCRAM-SHA-512 和使用本地 JWKS 校验 RS256 的 OAUTHBEARER。HTTPS JWKS 自动刷新、Kerberos 和 SASL_PLAINTEXT 待实现 |
| 用户、权限与令牌 | ACL、SCRAM 凭据管理及委派令牌 | **支持持久化 ACL、在线 SCRAM 和委派令牌**。可按 Topic、Group、Cluster、TransactionalId、User 和 DelegationToken 授权。令牌支持原生签发、查询、续期、撤销及 SCRAM 登录 |
| 配额与节流 | 支持按用户、客户端等设置配额和限流 | **部分支持**。持久化用户、client ID 和联合范围的生产与消费字节速率，以及具体或默认 IP 的新连接速率。按 Broker 独立节流。线程占用和控制器变更配额待实现 |
| 可观测性 | JMX 监控指标、客户端遥测订阅及推送 | **未实现 Kafka 监控和遥测接口**。提供启动和故障日志 |
| Kafka Connect、MirrorMaker 2、Kafka Streams | Apache Kafka 项目包含这些独立组件或库 | **未提供组件，端到端兼容未验证**。SimdKafka 当前交付为 Broker |

[Kafka 安全](https://kafka.apache.org/44/security/security-overview/) · [Kafka 监控](https://kafka.apache.org/44/operations/monitoring/) · [Kafka Connect](https://kafka.apache.org/44/connect/) · [Kafka Streams](https://kafka.apache.org/44/streams/) · [SimdKafka 连接和鉴权](src/SimdKafka/Server/Broker.cs)

## SimdKafka 固定上限与实测范围

| 项目 | 当前值或范围 |
| --- | --- |
| TCP 和请求 | 最多 256 个连接。Kafka 帧体上限 32 MiB，SASL 认证前 16 KiB |
| Produce | 编码批次最多 8 MiB，单批解压最多 32 MiB，同时解压 1～4 批，取决于 CPU 数 |
| Fetch | 数据预算最多 16 MiB，最长等待 30 秒，首个完整批次可超过客户端给出的较小预算 |
| 分区和刷盘 | 每 Topic 最多 128 分区，全 Broker 最多 1,024 分区。单次合并刷盘最多 64 批或 16 MiB |
| 经典消费组 | 内存协调器最多 1,024 个组，每组最多 64 个成员，含待确认成员。成员会话超时 1～300 秒。持久化的历史 offset 不受该内存组数限制 |
| 现代消费组 | 集群每组最多 64 个成员，心跳间隔 5 秒，会话超时 45 秒。接任协调器确认当前元数据后重建会话计时。成员 epoch 和分区控制状态保留 |
| 早期客户端实测 | Java 3.9.1、Confluent.Kafka 2.15.0、kafka-go v0.4.49 已验证基础建 Topic、生产、订阅消费和 offset 提交 |
| 三 Broker 客户端实测 | Kafka 4.4.0 Java CLI、Confluent.Kafka 2.15.0 和 kafka-go v0.4.49 各核对 257 条 Zstandard 消息、分区顺序和所提交的消费 offset |
| 安全与配置功能实测 | 用 Kafka 4.4.0 原生 Java 工具验证 SASL_SSL、ACL 增查删、拒绝优先、批次超限拒写、重启后 ACL 和配置恢复、已确认记录读取。本机三 Broker 双向 TLS 回归验证从控制器 follower 修改 ACL 和配置、全停重启及未授权心跳拒绝 |
| 在线凭据、配额和截断实测 | 使用 Kafka 4.4.0 原生工具验证两个 SCRAM 机制、在线删除与重启登录、生产及消费配额管理、IP 配额管理、单节点截断后读取、时间戳定位和日志目录查询。本机三 Broker 验证 SCRAM 与配额经控制器复制、全停重启和新登录撤权 |
| OAuth 和委派令牌实测 | 用 Kafka 4.4.0 原生 Java 客户端完成 OAuth 登录、令牌签发、查询和续期、SCRAM 写入、重启读回和撤销。本机验证签名、发行方、受众、到期校验和拒绝已撤销连接，三个 Broker 验证控制器转发和重启恢复 |
| 控制器仲裁实测 | 启动三个隔离 Broker，使用 Kafka 4.4.0 的 `kafka-metadata-quorum` 查询状态和复制进度。停止 leader 后完成选举，离线节点报未知进度，剩余节点继续提交。三个协议版本与 ACL 已在本机测试 |
| Share Groups 实测 | 在单 Broker Raft 模式，两个 Kafka 4.4 原生客户端从同一分区领取不同记录，验证确认、释放、重投、拒绝、组和游标运维及重启恢复。本机三 Broker 验证所有权隔离、续租与重启后游标，并注入时间验证锁定超时 |
| 分层存储实测 | 用 Kafka 4.4.0 原生客户端在线调整 `local.retention.ms`，验证低流量分区归档和重启后跨层按顺序读取。按大小归档测试使用隔离目录，分区逻辑文件 198,624 字节，本地实际分配 69,632 字节，重启后逐字节核对 24 条记录。本机验证远端段损坏时拒绝启动 |
| LogAppendTime 实测 | 本机验证了压缩载荷不变、时间戳配置继承、幂等重试、切主及重启恢复。以 Kafka 4.4.0 原生 Java 客户端验证单机 GZIP 写入和三副本 GZIP 事务写入，并分别在重启后核对时间定位、消息内容、顺序和消费记录的时间戳类型 |

Kafka Streams 的 `group.protocol=streams` 使用 KIP-1071。Broker 接收拓扑和成员持有的任务，校验输入 Topic、输入分区对齐、重分区 Topic 和 changelog，分配 active 和 standby 任务，并在协调器切换后通过持久化组状态恢复。现有消费组分配的是 Topic 分区，Share Groups 管理的是记录交付与确认。Streams Group 需要独立的任务和拓扑状态，相关接口暂未公布。API 93 对应 Kafka 4.4 的可选拓扑描述功能。 [Streams Rebalance Protocol](https://kafka.apache.org/44/streams/developer-guide/streams-rebalance-protocol/) · [Topology Description Plugin](https://kafka.apache.org/44/streams/developer-guide/topology-description-plugin/)

KIP-848 正则在服务端对完整 Topic 名称匹配。使用 .NET 非回溯引擎，支持常用 RE2/J 写法，包括分组、交替、内联标志、POSIX 字符类和十六进制转义。模式长度上限为 2,048 个 UTF-16 单元。Unicode 脚本名等部分 RE2/J 写法尚未支持。匹配集合随 Topic 元数据变更更新，消费组描述返回原始模式。

KIP-939 的 InitProducerId v6 在 Apache Kafka 4.4.0 源码中标记为不稳定版。SimdKafka 实现该线协议。启用 2PC 的事务不执行普通超时回滚，恢复请求待参与分区持久化写入隔离后，返回新身份和原事务身份。恢复者以 EndTxn v5 作出决定。旧客户端继续协商其支持的版本。原生 Java 4.4 的高层事务恢复未作为 v6 验证依据。 [KIP-939](https://cwiki.apache.org/confluence/display/KAFKA/KIP-939%3A+Support+Participation+in+2PC) · [Apache Kafka 4.4 请求定义](https://raw.githubusercontent.com/apache/kafka/4.4.0/clients/src/main/resources/common/message/InitProducerIdRequest.json)

集群按 Kafka 默认值每小时检查空闲 transactional ID，保留期为七天。正在写入或等待事务决定的身份不会被删除。没有最后更新时间的旧快照条目从下次状态更新起计。检查间隔和保留期当前固定。 [Kafka Broker 配置](https://kafka.apache.org/44/configuration/broker-configs/#transactional.id.expiration.ms)

本轮在三台测试 Broker 上执行了功能验证。Kafka 4.4.0 原生 Java 客户端以 `SubscriptionPattern` 消费两个匹配 Topic，其中一个在订阅后创建。客户端提交两个 offset 为 1，并收到非法模式错误。原生 TCP 的两阶段事务验证覆盖两个三副本分区、连续两次恢复、旧身份隔离、提交、回滚及结果重试。全停重启后，三个副本分别对 12 条记录及其 CRC32C、事务标记和回滚索引完成核对。Confluent.Kafka 2.15.0 的普通事务提交、回滚和 offset 回归也通过。空闲身份过期由本机三节点测试注入阈值时间，验证过期不会删除正在等待决定的事务或已确认记录。 [Java 正则订阅用例](tests/interop/RegexConsumer.java) · [三节点功能回归](tests/SimdKafka.Smoke/Program.cs)

[参数上限](src/SimdKafka/Program.cs) · [基础验证](README.md#实现与验证) · [性能报告](bench/PERFORMANCE.md)

## Kafka 4.4 稳定 API 逐项对照

Kafka 列使用官方文档的稳定请求版本。SimdKafka 列默认同时覆盖单节点和集群，范围有差异的条目单独注明。SASL 和委派令牌 API 按启动配置公布。关闭委派令牌时，相应的已知请求版本返回 `DELEGATION_TOKEN_AUTH_DISABLED`。其他未知 key 或版本会关闭当前连接，较新的 `ApiVersions` 请求会收到 v0 格式的 `UNSUPPORTED_VERSION`。 [Kafka 4.4 请求定义](https://kafka.apache.org/44/design/protocol/#the-messages) · [集群 API 注册及分发](src/SimdKafka/Protocol/ClusterRouter.cs)

| Key | API | Kafka 4.4 稳定版本 | SimdKafka 版本 | SimdKafka 范围或缺失能力 |
| ---: | --- | --- | --- | --- |
| 0 | `Produce` | v3–13 | v3–13 | v3–12 按名称，v13 按 Topic ID。支持 Magic 2 分区级确认和非事务幂等，集群支持事务写入 |
| 1 | `Fetch` | v4–18 | v4–13 | 全量 Fetch、长轮询、v13 Topic ID。集群支持从本地副本读取、连接内增量会话和事务隔离 |
| 2 | `ListOffsets` | v1–11 | 单节点 v0–11<br>集群 v1–11 | 查最早、最新、按时间、最大时间戳和本地最早位置。分层 Topic 支持最新归档及最早待归档位置 |
| 3 | `Metadata` | v0–13 | v0–13 | Broker 与分区元数据、自动建 Topic、v12 起按 Topic ID 查询，v13 顶层错误字段 |
| 8 | `OffsetCommit` | v2–10 | 单节点 v0–7<br>集群 v0–10 | 提交组或手动分配消费者的 offset。支持静态成员校验、集群成员 epoch 和 v10 Topic ID |
| 9 | `OffsetFetch` | v1–10 | 单节点 v0–5<br>集群 v0–10 | 集群支持多组查询、稳定性检查、v9 成员 epoch 校验和 v10 Topic ID |
| 10 | `FindCoordinator` | v0–6 | v0–6 | 支持经典消费组，集群另支持现代消费组、Share Groups 和事务协调器。v4 起可批量查询 |
| 11 | `JoinGroup` | v0–9 | v0–5 | 经典动态成员及静态成员入组、组 leader 与协议选择 |
| 12 | `Heartbeat` | v0–4 | v0–3 | 经典成员心跳和静态身份校验 |
| 13 | `LeaveGroup` | v0–5 | v0–3 | 经典成员离组，v3 支持批量操作和按静态实例 ID 移除 |
| 14 | `SyncGroup` | v0–5 | v0–3 | 同步经典客户端分配，v3 验证静态成员身份 |
| 15 | `DescribeGroups` | v0–6 | v0–6 | 经典组状态、成员与客户端分配 |
| 16 | `ListGroups` | v0–5 | v0–5 | 集群列表包括经典组、现代消费组和 Share Groups，可按状态和类型过滤 |
| 17 | `SaslHandshake` | v0–1 | v0–1 | 启用 SASL_SSL 时公布，协商 PLAIN、SCRAM 或 OAUTHBEARER，支持旧版 SASL 原始帧 |
| 18 | `ApiVersions` | v0–5 | v0–5 | 协商 API 范围，v5 校验 Cluster ID 和 Broker ID，误路由时返回 `REBOOTSTRAP_REQUIRED` |
| 19 | `CreateTopics` | v2–7 | v0–7 | 创建 Topic，支持副本分配、validate-only 和默认配置 |
| 20 | `DeleteTopics` | v1–6 | v0–6 | 按名称或 ID 删除 Topic |
| 21 | `DeleteRecords` | v0–2 | v0–2 | 逻辑截断至指定 offset，`-1` 使用当前可见日志末尾。集群等所有分配副本应用后返回低水位。物理空间回收待实现 |
| 22 | `InitProducerId` | v0–6 | 单节点 v0–5；集群 v0–6 | 分配非事务 producer ID。集群支持事务身份、epoch 隔离及 v6 外部两阶段提交和事务恢复 |
| 23 | `OffsetForLeaderEpoch` | v2–4 | v2–4 | 查询分区 leader epoch 对应的日志结束 offset。单节点使用 epoch 0 |
| 24 | `AddPartitionsToTxn` | v0–5 | 集群 v0–3 | 客户端登记分区。v4–5 是 Kafka Broker 间请求 |
| 25 | `AddOffsetsToTxn` | v0–4 | 集群 v0–4 | 客户端在事务中登记消费组 |
| 26 | `EndTxn` | v0–5 | 集群 v0–5 | 提交或回滚，v5 返回用于下一笔事务的 producer ID 与 epoch |
| 27 | `WriteTxnMarkers` | v1–2 | **未公布** | 集群通过内部双向 TLS RPC 写入标准 Kafka 日志标记 |
| 28 | `TxnOffsetCommit` | v0–6 | 集群 v0–6 | 事务内暂存消费 offset。v5 自动登记组，v6 使用 Topic ID |
| 29 | `DescribeAcls` | v1–3 | v0–3 | 按资源、名称匹配、用户、主机、操作和许可筛选 |
| 30 | `CreateAcls` | v1–3 | v0–3 | 创建精确名称或前缀 ACL，重复提交同一条目幂等 |
| 31 | `DeleteAcls` | v1–3 | v0–3 | 删除匹配条目并返回删除明细 |
| 32 | `DescribeConfigs` | v1–4 | v0–4 | 查询 Broker 和 Topic 设置、动态来源与继承 |
| 33 | `AlterConfigs` | v0–2 | v0–2 | 替换支持的动态配置，支持 validate-only |
| 34 | `AlterReplicaLogDirs` | v1–2 | **未实现** | 迁移副本的日志目录 |
| 35 | `DescribeLogDirs` | v1–5 | v1–4 | 查询本机副本的已提交编码字节数和卷容量。集群 follower lag 以未知值返回 |
| 36 | `SaslAuthenticate` | v0–2 | v0–2 | 启用 SASL_SSL 时公布，支持 PLAIN、SCRAM-SHA-256、SCRAM-SHA-512 和 OAUTHBEARER |
| 37 | `CreatePartitions` | v0–3 | v0–3 | 增加分区数，支持 validate-only 和集群副本分配 |
| 38 | `CreateDelegationToken` | v1–3 | v1–3 | 启用令牌时公布。v3 可指定所有者，按 User ACL 检查代签权限 |
| 39 | `RenewDelegationToken` | v1–2 | v1–2 | 所有者或续期者更新令牌的有效期 |
| 40 | `ExpireDelegationToken` | v1–2 | v1–2 | 所有者或续期者立即撤销令牌或调整过期时间 |
| 41 | `DescribeDelegationToken` | v1–3 | v1–3 | 按所有者筛选并检查令牌可见权限，v3 返回签发请求者 |
| 42 | `DeleteGroups` | v0–3 | v0–3 | 删除空闲组，集群包含 Share Groups |
| 43 | `ElectLeaders` | v0–2 | **未实现** | 触发分区 leader 选举 |
| 44 | `IncrementalAlterConfigs` | v0–1 | v0–1 | 针对已支持的配置执行 SET、DELETE 和 validate-only |
| 45 | `AlterPartitionReassignments` | v0–1 | 集群 v0–1 | 发起或取消副本重分配 |
| 46 | `ListPartitionReassignments` | v0 | 集群 v0 | 查询正在进行的副本重分配 |
| 47 | `OffsetDelete` | v0 | v0 | 删除空闲组指定分区的 offset |
| 48 | `DescribeClientQuotas` | v0–1 | v0–1 | 查询用户和客户端带宽、来源 IP 连接速率 |
| 49 | `AlterClientQuotas` | v0–1 | v0–1 | 动态设置、删除和验证带宽及连接速率 |
| 50 | `DescribeUserScramCredentials` | v0 | v0 | 启用动态 SCRAM 后查询机制和迭代次数 |
| 51 | `AlterUserScramCredentials` | v0 | v0 | 启用动态 SCRAM 后新增、轮换和删除指定用户与机制的在线凭据 |
| 55 | `DescribeQuorum` | v0–2 | 集群 v0–2 | 从 Broker 查询 SimdKafka 控制器 Raft 的 leader、任期、提交进度和成员日志末端 |
| 57 | `UpdateFeatures` | v0–2 | **未实现** | 更新集群功能级别 |
| 60 | `DescribeCluster` | v0–2 | v0–2 | 查询集群 ID 和已配置 Broker。仅 Broker 端点 |
| 61 | `DescribeProducers` | v0 | 集群 v0 | 查询本机 producer 序号、epoch、时间戳和打开事务的首 offset |
| 64 | `UnregisterBroker` | v0 | **未实现** | 注销 Broker |
| 65 | `DescribeTransactions` | v0 | 集群 v0 | 查询事务状态、producer 身份、超时与分区 |
| 66 | `ListTransactions` | v0–2 | 集群 v0–2 | 按状态、producer ID、时长和事务 ID 表达式列出事务。表达式使用 .NET 正则引擎 |
| 68 | `ConsumerGroupHeartbeat` | v0–1 | 集群 v0–1 | KIP-848 按名称或服务端正则订阅、加入、退出、会话与协作分配 |
| 69 | `ConsumerGroupDescribe` | v0–1 | 集群 v0–1 | 查询现代组的状态、epoch、成员与当前和目标分配 |
| 71 | `GetTelemetrySubscriptions` | v0 | **未实现** | 获取遥测订阅 |
| 72 | `PushTelemetry` | v0 | **未实现** | 推送客户端遥测 |
| 74 | `ListConfigResources` | v0–1 | v1 | 列出有显式动态覆盖的 Topic 与 Broker。v0 用于旧客户端遥测配置 |
| 75 | `DescribeTopicPartitions` | v0 | v0 | 带游标查询 Topic 和分区 |
| 76 | `ShareGroupHeartbeat` | v1 | 集群 v1 | 加入、续会话与离组，分配成员所订阅的已授权分区 |
| 77 | `ShareGroupDescribe` | v1 | 集群 v1 | 查询活跃成员、订阅和分配 |
| 78 | `ShareFetch` | v1–2 | 集群 v1–2 | 按记录领取，同分区允许多个成员，支持随请求发送确认 |
| 79 | `ShareAcknowledge` | v1–2 | 集群 v1–2 | 确认、拒绝、释放和 v2 续租。核对领取者身份 |
| 80 | `AddRaftVoter` | v0–1 | **未实现** | 增加 KRaft 投票成员 |
| 81 | `RemoveRaftVoter` | v0 | **未实现** | 移除 KRaft 投票成员 |
| 83 | `InitializeShareGroupState` | v0 | **未公布** | Kafka 内部状态接口。SimdKafka 通过 Raft 初始化游标 |
| 84 | `ReadShareGroupState` | v0 | **未公布** | Kafka 内部状态接口。SimdKafka 从 Raft 读取交付状态 |
| 85 | `WriteShareGroupState` | v0–1 | **未公布** | Kafka 内部状态接口。SimdKafka 将领取和确认写入 Raft |
| 86 | `DeleteShareGroupState` | v0 | **未公布** | Kafka 内部状态接口。SimdKafka 在 Raft 清理持久状态 |
| 87 | `ReadShareGroupStateSummary` | v0–1 | **未公布** | Kafka 内部状态接口。客户端使用 `DescribeShareGroupOffsets` |
| 88 | `StreamsGroupHeartbeat` | v0–1 | **未实现** | Streams Group 成员心跳与协调 |
| 89 | `StreamsGroupDescribe` | v0–1 | **未实现** | 查询 Streams Group |
| 90 | `DescribeShareGroupOffsets` | v0–1 | 集群 v0–1 | 批量查询起始 offset，v1 可访问分区 leader 时返回 lag |
| 91 | `AlterShareGroupOffsets` | v0 | 集群 v0 | 重置空组的分区 offset，校验 leader 和日志范围 |
| 92 | `DeleteShareGroupOffsets` | v0 | 集群 v0 | 按 Topic 删除空组持久化游标和交付状态 |
| 93 | `StreamsGroupTopologyDescriptionUpdate` | v0 | **未实现** | 更新 Streams Group 拓扑描述 |
| 94 | `UnregisterController` | v0 | **未实现** | 注销控制器 |

Kafka 稳定表之外的 Broker 和 KRaft 内部协议，SimdKafka 也没有实现，例如仲裁投票和快照传输、Broker 和控制器注册、Broker 心跳、ISR 更新、跨节点 producer ID 分配、请求转发和副本目录分配。这里的 79 项沿用官方稳定协议表的口径。 [Kafka KRaft](https://kafka.apache.org/44/operations/kraft/) · [Kafka 4.4.0 协议消息源码](https://github.com/apache/kafka/tree/4.4.0/clients/src/main/resources/common/message)
