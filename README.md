# SimdKafka

简体中文 · [English](README.en.md)

SimdKafka 是用 **C# 和 .NET 10** 实现的独立消息 Broker，直接在 TCP 上处理 Kafka 二进制协议。应用可以用 Kafka 客户端生产、消费消息，也可以用 Kafka CLI 管理 Topic、分区和消费组。SimdKafka 自己存储日志、元数据和 offset，可单节点运行，也可组成 Raft 集群。部署无需 ZooKeeper、Apache Kafka Broker 或 Kafka KRaft 控制器。

适合希望在 .NET 环境部署消息服务，同时保留 Kafka 客户端和工具链的团队。Java、Confluent.Kafka 和 kafka-go 已覆盖不同范围的互操作测试。迁移现有应用前，可按实际使用的协议版本和语义核对功能。

[完整功能对照](KAFKA_COMPATIBILITY.md) · [配置与安全](CONFIGURATION.md) · [架构与集群部署](IMPLEMENTATION.md) · [性能报告](bench/PERFORMANCE.md)

## 快速开始

### Docker 快速部署与测试

Docker 会为 `linux/amd64` 或 `linux/arm64` 自动选择镜像。快速体验用 `latest`，需要固定版本时可换成 `dotnetorgcn/simdkafka:1.0.0`。 [Docker Hub 镜像](https://hub.docker.com/r/dotnetorgcn/simdkafka/tags)

```bash
docker run -d --pull always --name simdkafka \
  -p 127.0.0.1:9092:9092 \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092 \
  -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false \
  -v simdkafka-data:/var/lib/kafka/data \
  dotnetorgcn/simdkafka:latest

docker logs --tail 20 simdkafka
```

看到 `SimdKafka listening` 后，用 Apache Kafka 4.4.0 镜像中的 CLI 创建 Topic 并收发消息。CLI 容器共享 Broker 的网络，可以直接连接 `127.0.0.1:9092`。

```bash
docker run --rm --network container:simdkafka \
  --entrypoint /opt/kafka/bin/kafka-topics.sh apache/kafka:4.4.0 \
  --bootstrap-server 127.0.0.1:9092 --create --if-not-exists \
  --topic simdkafka-smoke --partitions 1 --replication-factor 1

printf '%s\n' 'hello-simdkafka' | docker run --rm -i \
  --network container:simdkafka \
  --entrypoint /opt/kafka/bin/kafka-console-producer.sh apache/kafka:4.4.0 \
  --bootstrap-server 127.0.0.1:9092 --topic simdkafka-smoke \
  --command-property acks=all

docker run --rm --network container:simdkafka \
  --entrypoint /opt/kafka/bin/kafka-console-consumer.sh apache/kafka:4.4.0 \
  --bootstrap-server 127.0.0.1:9092 --topic simdkafka-smoke \
  --partition 0 --offset earliest --max-messages 1 --timeout-ms 15000
```

应读到 `hello-simdkafka`。执行 `docker restart simdkafka`，等 Broker 恢复监听后再运行消费命令，验证命名卷中的消息可以读回。 [Compose 与容器参数](DOCKER.md)

### 从源码启动

需要 .NET 10 SDK。以下命令启动单节点 Broker，默认监听 `127.0.0.1:9092`，数据写入 `./data`。

```sh
dotnet build SimdKafka.slnx -c Release
dotnet run --project src/SimdKafka -c Release --no-build -- --data ./data --no-auto-create
```

使用 Kafka CLI 创建一个 Topic，在两个终端分别写入和读取。

```sh
kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --topic orders --partitions 1 --replication-factor 1
kafka-console-producer.sh --bootstrap-server 127.0.0.1:9092 --topic orders
kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic orders --from-beginning
```

其他 Kafka 客户端使用 `bootstrap.servers=127.0.0.1:9092` 或各自对应的配置。跨主机部署时，`--advertise-host` 要设置为客户端可访问的地址。面向外部客户端部署时可配置 TLS、SASL 和 ACL。 [配置与安全](CONFIGURATION.md)

## 功能对照

| 能力 | Apache Kafka 4.4.0 | SimdKafka |
| --- | --- | --- |
| 原生 Kafka TCP | 客户端和管理协议 | 支持常用生产、消费、管理 API 和版本协商 |
| 存储与集群 | 分段日志及 KRaft | 单节点持久化日志。集群使用自有 Raft 协议复制控制器元数据与分区日志 |
| 生产与消费 | 幂等写入、长轮询、增量 Fetch、压缩 | 支持幂等写入、长轮询、gzip、Snappy、LZ4、Zstandard。增量 Fetch 在集群可用 |
| 时间戳与 offset | 创建时间、追加时间、按时间查找 | 支持 `CreateTime`、`LogAppendTime`、时间范围校验及 `ListOffsets` v11 |
| 写入确认与高可用 | ISR、`min.insync.replicas`、选主 | 单节点默认刷盘确认，集群的 `acks=1` 和 `acks=all` 均等待 Raft 多数副本落盘。完整 ISR 语义待实现 |
| 副本读取和迁移 | 机架感知、副本重分配、日志目录迁移 | 集群支持满足读取条件的同机架副本及跨 Broker 重分配。日志目录迁移待实现 |
| 消费组 | 经典、现代、Share、Streams | 支持经典组，集群支持现代组及 Share 消费与确认。Streams Groups 和 offset 自动过期待实现 |
| 事务 | 事务写入、消费 offset 提交、`read_committed` | 集群支持提交、回滚、producer 隔离、事务内 offset 提交和 `read_committed` |
| 安全与配额 | TLS、SASL、ACL、令牌和配额 | 支持 TLS/mTLS、PLAIN、SCRAM、基于本地 JWKS 的 OAuth、ACL、委派令牌及部分配额 |
| 日志生命周期 | 截断、自动保留、压实、分层存储 | 支持持久化 `DeleteRecords` 和单节点文件系统分层。通用保留、压实、集群分层和远端清理待实现 |

单节点默认公布 36 个 API key，集群默认公布 55 个。启用 SASL 和委派令牌后，集群最多公布 61 个。各 API 的版本和行为见[逐项对照表](KAFKA_COMPATIBILITY.md)。Kafka Broker 与控制器内部通信采用各自的协议，两种实现应分别部署。

## 实现与验证

- [Server](src/SimdKafka/Server) 和 [Protocol](src/SimdKafka/Protocol) 处理异步 Socket、帧校验、身份认证、版本协商及请求分发。
- [Storage](src/SimdKafka/Storage) 负责批次、时间索引、CRC32C、刷盘、重启恢复和单节点分层。[Cluster](src/SimdKafka/Cluster) 负责 Raft 元数据、分区复制、事务和副本迁移。
- 写入路径复用网络缓冲区并合并落盘。批次校验使用硬件 CRC32C，时间索引在支持的平台使用 SIMD。 [实现与编解码示例](IMPLEMENTATION.md)

```sh
dotnet run --project tests/SimdKafka.Smoke -c Release --no-build
```

在同一台单副本 Broker 主机上分别跑满 24 小时的落盘读写测试中，SimdKafka 与 Kafka 4.4.0 分别达到 **19,947** 和 **10,362 条/秒**，峰值 RSS 为 **107.3** 和 **1,218.3 MiB**，平均 CPU 为 **0.251** 和 **0.119 核**。两段测试合计核对约 25.97 亿条消息。完整条件、快速及 10 分钟结果见[性能报告](bench/PERFORMANCE.md)。
