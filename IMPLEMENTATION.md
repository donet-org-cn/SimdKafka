# SimdKafka 实现与部署

[项目首页](README.md) · [English](README.en.md)

SimdKafka 是用 C# 和 .NET 10 实现的 Kafka broker。它在 TCP 9092 上处理原生 Kafka 帧，自己管理 topic、分区、broker 标识、消费组和 offset。默认使用单节点存储，也可启动基于 Raft 的集群。运行时无需 ZooKeeper、Kafka KRaft 或其他 broker。Snappy、LZ4 和 Zstandard 由托管 .NET 库解码。

## 启动

```powershell
dotnet build SimdKafka.slnx -c Release
dotnet run -c Release --no-build --project src/SimdKafka -- --data ./data
```

客户端的 `bootstrap.servers`（Go 里相应的 broker 地址）使用 `127.0.0.1:9092`。生产者可以使用非事务幂等写入，支持 `none`、`gzip`、`snappy`、`lz4` 和 `zstd`。消费者可以按分区读取或用 `Subscribe` 加入经典消费组。集群模式支持 `group.protocol=consumer` 的按名称订阅，也支持通过 `transactional.id` 写事务，消费者用 `isolation.level=read_committed` 读取已提交结果。客户端按 `ApiVersions` 协商协议版本。

默认自动创建 topic，每个 topic 一个分区。可以通过 `--partitions 4` 调整默认值，通过 `--no-auto-create` 禁用自动创建并使用标准客户端的 `CreateTopics`。

跨机器连接时，`Metadata` 和 `FindCoordinator` 返回的地址必须能被客户端访问。以下命令中的域名应替换为实际地址。

```powershell
dotnet run -c Release --no-build --project src/SimdKafka -- --listen 0.0.0.0:9092 --advertise-host kafka.example.com --data ./data
```

`--advertise-port` 可指定 NAT 后客户端使用的端口。默认 `PLAINTEXT` 允许任何连接者访问所有已实现的 API。使用 `--tls-pfx broker.pfx` 启用 `SSL`，证书应覆盖 `--advertise-host`。通过 `--tls-password-file` 读取 PFX 密码，通过 `--tls-client-ca ca.pem` 要求客户端提供由指定 CA 签发的证书。

使用 `--sasl-plain-users users.json` 启用 `SASL_SSL` 和 `PLAIN`，需同时配置 `--tls-pfx`。启用 `--acl-super-user User:admin` 后，普通账号按 Topic、Group、Cluster 和 TransactionalId 的 ACL 授权。启用集群 ACL 时，所有节点使用包含 `"clientAclRequired": true` 的配置文件和内部双向 TLS。完整参数、客户端属性和原生管理命令见[安全与配置说明](CONFIGURATION.md)。

`--sasl-scram-users FILE` 配置随 Broker 启动加载的 SCRAM 验证材料。`--sasl-dynamic-scram` 开启原生在线查询、创建、轮换和删除接口，支持 `SCRAM-SHA-256` 和 `SCRAM-SHA-512`。两种配置均可与 PLAIN 同时使用。启动方式和客户端配置见 [安全与配置说明](CONFIGURATION.md)。

`--sasl-oauth-config FILE` 启用 OAUTHBEARER，使用本地 JWKS 校验 RS256 JWT。`--delegation-token-secret-file FILE` 启用 Kafka 委派令牌，支持授权签发、续期、撤销及使用令牌进行 SCRAM 登录。集群需统一设置 `"delegationTokens": true` 和内部双向 TLS。 [配置说明](CONFIGURATION.md#oauth-bearer-登录)

## 集群模式

各 Broker 使用同一集群配置文件、各自的 `--node-id` 与独立数据目录。`host` 和 `port` 是客户端可达的 Kafka 地址。`control`、`internal` 和副本端口范围用于节点之间通信，应在专用网络内可达。

```json
{
  "clusterId": "aa02d2cb-c7b7-4408-9791-bda5317c4310",
  "recycleReplicaPorts": true,
  "brokers": [
    {"id": 1, "host": "192.0.2.11", "port": 9092, "rack": "a", "control": "192.0.2.11:19093", "internal": "192.0.2.11:19094", "firstReplicaPort": 20000, "replicaPortCount": 256},
    {"id": 2, "host": "192.0.2.12", "port": 9092, "rack": "b", "control": "192.0.2.12:19093", "internal": "192.0.2.12:19094", "firstReplicaPort": 20000, "replicaPortCount": 256},
    {"id": 3, "host": "192.0.2.13", "port": 9092, "rack": "c", "control": "192.0.2.13:19093", "internal": "192.0.2.13:19094", "firstReplicaPort": 20000, "replicaPortCount": 256}
  ]
}
```

将示例地址改为各节点实际地址，并为新集群生成独立的 `clusterId`。每个节点的内部 PFX 由同一集群 CA 签发，需要具备 TLS 服务端和客户端用途，SAN 覆盖 `simdkafka-raft` 及本节点的内部 IP。下面以节点 1 为例。

新集群可用 `recycleReplicaPorts: true` 循环使用副本端口范围。该模式要求节点间双向 TLS，并以 Raft 组 UUID 校验每次握手。它在首次启动时写入日志身份，已有数据目录继续使用创建时的配置值。

```powershell
dotnet run -c Release --no-build --project src/SimdKafka -- --cluster-config cluster.json --node-id 1 --listen 192.0.2.11:9092 --data ./data-node1 --cluster-pfx node1.pfx --cluster-ca cluster-ca.pem
```

| 能力 | 集群实现和限制 |
| --- | --- |
| Broker、控制器与分区元数据 | 控制器独立选主并复制 Topic ID、分区、leader epoch 和消费 offset。Broker 注册来自启动配置，暂不支持动态注册或调整控制器投票集。 |
| 分区持久化与可用性 | 每分区有自己的 Raft 日志。`acks=1` 和 `acks=all` 均等待多数副本持久化。三副本容许一个副本故障。多数副本不可用时停止确认写入。 |
| 机架读副本 | Fetch v11 协商 `client.rack`，只有当前 epoch 下已经追上请求 offset 的同机架副本才会获得读流量。 |
| 增量 Fetch | 集群 Fetch v7–13 支持连接内会话及最多 256 个分区。Broker 缓存分区请求，元数据未改变且无数据的分区可从增量响应省略。 |
| 副本迁移 | 支持发起、查询及取消进行中的重新分配。将目标副本加入并追赶后移出源副本。源节点数据暂时保留。 |
| 经典消费组 | 控制器 leader 托管动态成员及 `group.instance.id` 静态成员。相同订阅的静态成员重启可保留分配并隔离旧成员。offset 已复制，控制器选主后活跃客户端重新入组。 |
| KIP-848 消费组 | 集群支持按 Topic 名称订阅、服务端分配、静态成员和协作式交接。控制器故障后从 Raft 恢复成员、分配和 epoch。 |
| 存储和隔离 | 每个活动分区占用一个全局副本端口槽位。启用回收时，删除 Topic 可释放槽位。新分区使用 32 MiB WAL 文件块。控制器确认删除 Topic 后，各节点回收对应日志目录。活动 Topic 尚无集群保留策略、压实或 WAL 快照。客户端 ACL 和配置由控制器仲裁持久化。 |

集群与单节点分别公布 `ApiVersions`。集群另实现了 `AlterPartitionReassignments` v0–1、`ListPartitionReassignments` v0 和 `DescribeProducers` v0。两种模式均支持 `OffsetForLeaderEpoch` v2–4、`DescribeLogDirs` v1–4、原生 ACL 和消息上限管理。集群 `ListOffsets` 支持 v1–11。`CreateTopics` v0–7 和 `CreatePartitions` v0–3 支持显式副本分配。

## 模块

| 模块 | 职责 |
| --- | --- |
| [Server/Broker.cs](src/SimdKafka/Server/Broker.cs) | 异步 Socket、TCP 分帧、输入大小检查、部分读写、按连接顺序处理请求 |
| [Protocol/Wire.cs](src/SimdKafka/Protocol/Wire.cs) 和 [Router.cs](src/SimdKafka/Protocol/Router.cs) | 大端字段、变长整数、compact string 和 tagged fields 解码、版本协商、按 API key 分发 |
| [Protocol/Router.Log.cs](src/SimdKafka/Protocol/Router.Log.cs) | Produce、Fetch 长轮询和 ListOffsets |
| [Protocol/Router.Groups.cs](src/SimdKafka/Protocol/Router.Groups.cs) 和 [Groups/GroupCoordinator.cs](src/SimdKafka/Groups/GroupCoordinator.cs) | 经典组成员、心跳、代际、再平衡、客户端 leader 分配及 offset 提交 |
| [Protocol/Router.Admin.cs](src/SimdKafka/Protocol/Router.Admin.cs) 和 [Router.GroupAdmin.cs](src/SimdKafka/Protocol/Router.GroupAdmin.cs) | Topic、集群、配置和消费组管理 |
| [Storage/Catalog.cs](src/SimdKafka/Storage/Catalog.cs) | 集群 ID、topic ID、producer ID、组 offset 和待回收目录的持久化 |
| [Storage/PartitionLog.cs](src/SimdKafka/Storage/PartitionLog.cs) 和 [RecordBatch.cs](src/SimdKafka/Storage/RecordBatch.cs) | 每分区追加日志、批次和时间索引、CRC32C、幂等序列及启动恢复 |
| [Storage/RecordCompression.cs](src/SimdKafka/Storage/RecordCompression.cs) | 压缩批次解码、解压大小及并发限制 |
| [Cluster/BrokerCluster.cs](src/SimdKafka/Cluster/BrokerCluster.cs) 和 [Cluster/RaftJournal.cs](src/SimdKafka/Cluster/RaftJournal.cs) | 多 Broker 启动、任期与投票管理、节点间 TLS、持久化复制 |
| [Cluster/ReplicatedMetadata.cs](src/SimdKafka/Cluster/ReplicatedMetadata.cs) 和 [Cluster/ReplicatedPartition.cs](src/SimdKafka/Cluster/ReplicatedPartition.cs) | 控制器状态机、分区批次合并、读取已提交数据、迁移状态 |
| [Protocol/ClusterRouter.cs](src/SimdKafka/Protocol/ClusterRouter.cs) | 集群模式的 Kafka API 路由和版本协商 |

## 单节点 Kafka API keys

下表是单节点模式的版本范围。集群额外公布的 API 见[完整功能清单](KAFKA_COMPATIBILITY.md)。支持的 flexible 版本按对应 Kafka schema 解析 tagged fields。`ApiVersions` 的响应头始终为 v0，高于公布范围的版本返回 v0 格式的 `UNSUPPORTED_VERSION`。其他未经公布的 key 或版本会关闭当前连接。

| Key | API | 版本 | 作用 |
| --- | --- | --- | --- |
| 0 | Produce | 3–13 | 追加 Magic 2 批次，各分区返回自己的结果。支持 acks 0、1 和 -1，v13 使用 Topic ID |
| 1 | Fetch | 4–13 | 按分区和 offset 读取，支持字节限制和长轮询，v13 使用 topic ID |
| 2 | ListOffsets | 0–11 | 查最早、最新、指定时间、最大时间戳、本地最早、最新归档及最早待归档 offset。v0 每分区最多返回一个位置 |
| 3 | Metadata | 0–13 | 返回 broker、controller、topic、leader、ISR 和分区，v10 起返回 topic ID |
| 8 | OffsetCommit | 0–7 | 写入消费组或手动分配消费者的已提交 offset，v7 校验静态成员 |
| 9 | OffsetFetch | 0–5 | 查询指定分区或组内全部已提交 offset |
| 10 | FindCoordinator | 0–6 | 查询经典消费组 coordinator，v4 起支持批量查询 |
| 11 | JoinGroup | 0–5 | 动态和静态成员加入、选举组 leader、协商分配策略 |
| 12 | Heartbeat | 0–3 | 维护会话、校验静态成员身份并通知成员重新加入 |
| 13 | LeaveGroup | 0–3 | 退出组，v3 支持批量退出及按静态实例 ID 移除 |
| 14 | SyncGroup | 0–3 | Broker 保存 leader 的分配结果并交给组成员 |
| 15 | DescribeGroups | 0–6 | 查询组状态、成员、订阅元数据和分配结果 |
| 16 | ListGroups | 0–5 | 列出经典消费组，可按状态和类型过滤 |
| 17 | SaslHandshake | 0–1 | 启用 SASL_SSL 后公布，协商 SASL 机制及认证帧格式 |
| 18 | ApiVersions | 0–5 | 引导协议协商，v5 检测误路由的 Cluster ID 和 Broker ID |
| 19 | CreateTopics | 0–7 | 使用默认配置创建单副本 topic，支持 validate-only |
| 20 | DeleteTopics | 0–6 | 按名称或 topic ID 删除，回收日志目录 |
| 21 | DeleteRecords | 0–2 | 持久化提升分区的日志起始 offset |
| 22 | InitProducerId | 0–5 | 为非事务幂等生产者分配持久化 ID，epoch 用尽后可申请新 ID |
| 23 | OffsetForLeaderEpoch | 2–4 | 查询单节点 epoch 0 的日志结束位置 |
| 29–31 | DescribeAcls、CreateAcls、DeleteAcls | 0–3 | 查询、添加和删除 ACL |
| 32 | DescribeConfigs | 0–4 | 查询已实现的 broker 和 topic 配置 |
| 33 | AlterConfigs | 0–2 | 替换资源上已实现的动态配置 |
| 35 | DescribeLogDirs | 1–4 | 查询数据目录容量和每个分区的已提交编码字节数 |
| 36 | SaslAuthenticate | 0–2 | 启用 SASL_SSL 后公布，验证 PLAIN、SCRAM 或 OAuth 凭据 |
| 38–41 | Delegation Token | 见兼容表 | 启用令牌后公布签发、续期、撤销和查询 |
| 37 | CreatePartitions | 0–3 | 扩容单副本 topic 的分区数，支持 validate-only |
| 42 | DeleteGroups | 0–3 | 删除无活跃成员的消费组 |
| 44 | IncrementalAlterConfigs | 0–1 | 设置、删除和验证 Broker 或 Topic 的批次字节上限 |
| 47 | OffsetDelete | 0 | 删除空闲消费组指定分区的 offset |
| 48–49 | DescribeClientQuotas、AlterClientQuotas | 0–1 | 查询和调整生产带宽、消费带宽及来源 IP 连接速率 |
| 50–51 | DescribeUserScramCredentials、AlterUserScramCredentials | 0 | 在线查询、轮换和删除 SCRAM 凭据 |
| 60 | DescribeCluster | 0–2 | 查询 broker、controller 和集群 ID |
| 74 | ListConfigResources | 1 | 列出有显式动态覆盖的 Topic 与 Broker |
| 75 | DescribeTopicPartitions | 0 | 查询 topic ID、分区和 leader，支持游标 |

字段顺序见 [Apache Kafka 协议规范](https://kafka.apache.org/44/design/protocol/) 和 [Apache Kafka 消息定义](https://github.com/apache/kafka/tree/4.4.0/clients/src/main/resources/common/message)。

双方功能、已实现版本边界及 Kafka 4.4 的全部稳定 API 对照见 [完整功能清单](KAFKA_COMPATIBILITY.md)。

## 编解码示例

Kafka 使用四字节有符号大端长度，后接请求或响应。读取长度后由 Socket 循环收齐指定字节数，再把同一个缓冲区的切片传给路由。以下省略了长度保护和 flexible 请求头中的 tags。

```csharp
var reader = new WireReader(frame.Span);
var key = (Api)reader.I16();
short version = reader.I16();
int correlationId = reader.I32();
reader.SkipString(); // ClientId

output.I32(0);
output.I32(correlationId);
// 根据 key 和 version 写入响应体。
output.PatchI32(0, output.Length - sizeof(int));
```

Produce 验证 [Magic 2 批次格式](https://kafka.apache.org/28/implementation/message-format/) 中 CRC32C 覆盖的字节。分配 offset 后只改 CRC 范围外的两个字段，然后直接从接收缓冲区追加到分区日志。

```csharp
BinaryPrimitives.WriteInt64BigEndian(batchBytes, baseOffset);
BinaryPrimitives.WriteInt32BigEndian(batchBytes[12..], leaderEpoch);
RandomAccess.Write(fileHandle, batchBytes, filePosition);
```

## 单节点性能和持久化

- `Socket` 读入可复用的 `ArrayPool<byte>` 缓冲区。Produce 直接校验并追加接收帧中的批次。Fetch 用 `RandomAccess.Read` 把选中的磁盘范围直接读入响应缓冲区，再用 `SendAsync` 发送。整个过程保留批次格式。
- 收发热路径按 topic 名称的 span 或 topic ID 查找目录，省去托管字符串分配。响应缓冲区扩容时可能移动已写入的字节。磁盘和网络 I/O 仍经过操作系统缓冲区。
- 压缩批次入队时使用池化缓冲区解压校验，落盘和出队保留原始压缩字节。单批解压上限 32 MiB，同时最多四个批次解压。超出上限的批次会被拒绝。
- `ListOffsets` 按时间查询时用 `Vector256<long>` 一次筛选四个批次的最大时间戳。取到索引后释放锁，再读取和解码候选批次。无 Vector256 硬件加速时走标量路径。CRC32C 由运行时在支持的 x64 或 ARM64 上使用硬件 CRC 指令。
- `catalog.json` 保存集群 ID、producer ID、topic 和消费组提交。写入带 SHA-256 的快照，刷盘后原子替换，Unix 同步目录，Windows 使用 write-through 替换。旧目录格式可以读入，下一次更新自动写入新快照。目录丢失但分区日志仍在时拒绝初始化。活跃组成员重启后重新入组。
- 默认将同一分区积压的批次合并刷盘。专用工作线程按顺序追加，Linux 用 `fdatasync`，刷盘成功后统一推进可读 offset 并返回成功。单次最多 64 批或 16 MiB。写入或刷盘失败时，分区停止接收写入，尝试撤销未确认尾部。
- Fetch 发送前验证偏移、批次边界和 CRC32C。启动时扫描并重建索引，截断不完整的尾批，对损坏的完整批次拒绝启动。幂等重试还会比对原始批次字节。同一序号携带不同数据时返回错误。
- Topic 删除先持久化删除状态，已接收的操作结束后回收旧日志。同名重建使用新的 topic ID 和独立目录，已解析到旧 topic ID 的 offset 提交会被拒绝。启动时会重试清理未完成的目录。
- `--unsafe-no-fsync` 仅适用于接受断电丢数据的场合。默认持久化依赖存储兑现刷盘语义。单副本无法从磁盘永久故障中恢复，且客户端没收到确认时，完整批次仍可能在重启后可见，可用幂等生产者安全重试支持的序号窗口。

## 集群性能和持久化

- 每个分区按照请求顺序合并积压批次，一次最多 64 批或 16 MiB。事务写入共用该合批路径。原生请求的池化缓冲区保持到复制结束，合并的批次直接写入 WAL。返回成功前等待多数副本落盘。读取只返回已被 Raft 确认的记录，在响应缓冲区里设置 offset 和 leader epoch，并核对 CRC32C。
- 控制器元数据单独持久化。分区和控制器 WAL 使用 XxHash3 检查完整性。重启时重建批次及时间索引，并保留幂等生产者的序号状态。跨节点安全和端点配置见[集群模式](#集群模式)。
- 事务协调器在元数据 Raft 提交决定，分区 Leader 将 Kafka commit 或 abort 标记写入各自的 Raft 日志。全部标记完成后发布事务内的消费 offset。接任的协调器接续尚未结束的决定。Fetch 通过 LSO 和 aborted index 实现 `read_committed`。事务协议 v2 按分区自动登记并在结束时返回新的 producer epoch。

## 边界

- 默认单节点模式只有一个 Broker 和一个副本，分区数据追加到单文件。集群模式使用 Raft 多副本和文件块。两种模式的批次索引常驻内存。活动日志的时间或大小保留和按 key 压实尚未实现。
- 只支持 Magic 2 记录，不向下转换旧记录格式。Zstandard 需要至少 Produce v7 和 Fetch v10。
- 非事务幂等写入支持客户端提升 producer epoch，并按分区拒绝旧 epoch。最近五个批次的序列范围用于去重，重试返回原 offset。分区生产者去重状态的空闲过期待实现。集群模式支持客户端事务、KIP-939 外部两阶段提交的 v6 恢复及空闲 transactional ID 过期。
- 分区和控制器 Raft 状态机分别使用版本 5 和 12，继续回放历史 WAL 和控制器快照。三副本集群需要两个同等或更高状态机版本的节点才能确认新格式写入。集群新增功能应在所有节点完成升级后统一启用。 [DotNext 滚动升级语义](https://dotnet.github.io/dotNext/features/cluster/raft.html#rolling-updates)
- 首次升级保留原 Broker ID、机架、分区复制起始端口和全局可用槽位数。控制器把这些影响重放的布局参数持久化，后续会在重放前拒绝不一致配置。
- 经典组和集群 KIP-848 组均支持动态和静态成员。KIP-848 v1 支持服务端正则订阅。集群支持 Kafka Share Groups 的多成员领取、确认、释放、拒绝、续租和游标运维。混合协议在线迁移和普通消费 offset 自动过期尚待实现。集群支持 Fetch v7–13 增量会话，单节点使用全量请求。 [功能对照](KAFKA_COMPATIBILITY.md)
- 动态配置覆盖批次大小和时间戳类型，以及单节点分层的启用、归档块大小、本地保留字节数和时间。单节点可挂载文件系统后端归档、释放本地稀疏块并从远端读取。集群 WAL 分层、远端生命周期、HTTPS JWKS 自动刷新和 Kerberos 待实现。使用新功能写入数据后，继续用支持该状态格式的 Broker 打开该数据目录。 [配置说明](CONFIGURATION.md)
- 单帧上限 32 MiB、单批 8 MiB、Fetch 数据预算 16 MiB、单次 Fetch 等待最多 30 秒。Kafka 首批可超过客户端提供的小预算，以保证消费者继续前进。默认最多 256 个 TCP 连接、1,024 个分区、每 topic 128 个分区和每组 64 个动态成员。

## 验证

```powershell
dotnet run -c Release --project tests/SimdKafka.Smoke
```

测试使用临时目录和本机回环端口，检查分片 TCP 帧、版本协商、按 topic ID 拉取、CRC 拒绝、压缩数据损坏和解压上限、并发删除和重建、幂等重试、消费组再平衡、offset 提交及重启恢复。三节点测试覆盖双向 TLS、多数派确认、重新选主、读副本、副本迁移、事务标记与未稳定 offset、事务 epoch 隔离、控制器快照以及完整重启后的事务重放。添加 `-- --security-only` 可单独运行安全和配置功能回归。

使用 Apache Kafka Java 客户端 3.9.1、Confluent.Kafka 2.15.0 和 kafka-go v0.4.49 实测了建 topic、生产、订阅消费和提交 offset。Java 和 C# 客户端还验证了幂等生产者初始化。

单副本的快速、10 分钟和 24 小时落盘对比见[性能报告](bench/PERFORMANCE.md)。跨 Broker 故障恢复和消息一致性检查见 [C# 回归用例](tests/SimdKafka.Smoke)。
