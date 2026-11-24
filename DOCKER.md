# SimdKafka Docker 部署

简体中文 · [English](#english)

镜像封装 SimdKafka v1.0.0 的已发布程序和 Microsoft .NET 10 Runtime。默认使用非 root 用户，监听容器内 TCP 9092，数据写入 `/var/lib/kafka/data`。对外地址默认是 `localhost:9092`。 [v1.0.0 发行页](https://github.com/donet-org-cn/SimdKafka/releases/tag/v1.0.0)

## 启动

Docker Hub 的 `latest` 和 `1.0.0` 都支持 `linux/amd64` 和 `linux/arm64`。快速体验可用 `latest`，需要固定部署版本时使用 `1.0.0`。下方命令在宿主机回环地址公开 9092，并将数据写入命名卷。 [Docker Hub 镜像](https://hub.docker.com/r/dotnetorgcn/simdkafka)

```sh
docker run -d --pull always --name simdkafka -p 127.0.0.1:9092:9092 \
  -v simdkafka-data:/var/lib/kafka/data \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  dotnetorgcn/simdkafka:latest --no-auto-create
```

仓库中的 Compose 文件也可使用已发布的镜像。

```sh
SIMDKAFKA_IMAGE=dotnetorgcn/simdkafka:latest docker compose up -d --no-build --pull always
```

在本地从 Dockerfile 构建时，在仓库根目录使用 `docker compose up -d --build`。启动 Broker 后，通过原生 Kafka CLI 建 Topic 和收发消息。

```sh
kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --topic orders --partitions 1 --replication-factor 1
kafka-console-producer.sh --bootstrap-server 127.0.0.1:9092 --topic orders
kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic orders --from-beginning
```

单独运行时也可使用 `docker run`。下方例子把宿主机 29092 映射到容器 9092，并通过 Metadata 把宿主机端口返回给客户端。

```sh
docker build -t simdkafka:1.0.0 .
docker run -d --name simdkafka -p 127.0.0.1:29092:9092 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:29092 \
  -v simdkafka-data:/var/lib/kafka/data \
  simdkafka:1.0.0 --no-auto-create
```

Docker 网络里的客户端应使用可解析的容器网络名称和容器端口作为公布地址。宿主机外的客户端需要使用可访问的宿主机 IP 或 DNS 名称和映射端口。使用 bind mount 时，让容器用户 `1654` 有权写入数据目录。公开部署的 TLS、SASL 和 ACL 配置见 [配置与安全](CONFIGURATION.md)。

## 多架构构建

在仓库根目录用 Buildx 构建并推送两个架构。将命令中的镜像名称替换为你有权发布的仓库。

```sh
docker buildx build --platform linux/amd64,linux/arm64 \
  -t your-user/simdkafka:1.0.0 --push .
```

可用 `docker buildx imagetools inspect dotnetorgcn/simdkafka:1.0.0` 查看公开镜像的架构清单。 [Docker 多架构构建文档](https://docs.docker.com/build/building/multi-platform/)

## 与 Apache Kafka 镜像参数对照

| 参数 | SimdKafka 行为 |
| --- | --- |
| `KAFKA_LISTENERS` | 单个客户端监听器，例如 `PLAINTEXT://:9092` 或 `SSL://0.0.0.0:9092`。绑定地址用 IP、`localhost` 或空地址 |
| `KAFKA_ADVERTISED_LISTENERS` | 单个客户端可访问地址，例如 `PLAINTEXT://broker.example.com:29092`。名称与监听器一致 |
| `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` | 可映射自定义监听器名，例如 `CLIENT:SSL`。协议支持 `PLAINTEXT`、`SSL` 和 `SASL_SSL`，必须与启用的认证选项匹配 |
| `KAFKA_LOG_DIRS` / `KAFKA_LOG_DIR` | 一个数据目录，默认 `/var/lib/kafka/data` |
| `KAFKA_NUM_PARTITIONS` | 默认分区数，默认为 1 |
| `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | `true` 或 `false`，裸镜像默认为 `true` |
| `KAFKA_NODE_ID` | 单节点 ID 固定为 1。集群中对应 `--node-id` |
| `KAFKA_DEFAULT_REPLICATION_FACTOR` | 单节点固定为 1。集群由建 Topic 时的 `--replication-factor` 和已有集群配置决定 |
| `KAFKA_SSL_KEYSTORE_FILENAME` / `KAFKA_SSL_KEYSTORE_CREDENTIALS` | PKCS#12 证书和密码文件。相对文件名位于 `/etc/kafka/secrets`，可用 `SIMDKAFKA_TLS_PFX` 和 `SIMDKAFKA_TLS_PASSWORD_FILE` 覆盖 |
| `KAFKA_SSL_CLIENT_AUTH` | 支持 `none` 和 `required`。双向 TLS 需要 `SIMDKAFKA_TLS_CLIENT_CA` 或 `--tls-client-ca` |

镜像名后可继续传入原有 CLI 参数，例如 `--tls-pfx`、`--acl-super-user`、`--cluster-config`。命令行值在对应的环境变量之后应用。SASL 账号也可通过 `SIMDKAFKA_SASL_PLAIN_USERS`、`SIMDKAFKA_SASL_SCRAM_USERS` 或 `SIMDKAFKA_SASL_OAUTH_CONFIG` 指向挂载的配置文件。

集群使用 `SIMDKAFKA_CLUSTER_CONFIG=/etc/simdkafka/cluster.json`、`KAFKA_NODE_ID` 以及各节点独立的数据卷。集群 JSON 的节点 `host` 和 `port` 要与对外公布地址一致。控制器、内部 RPC 和副本端口按集群 JSON 开放。Kafka KRaft 的 `KAFKA_PROCESS_ROLES`、`CLUSTER_ID` 和控制器仲裁参数需要移除。Apache Kafka 的 `server.properties`、多客户端监听器、JKS 和任意 Kafka 环境变量透传均不在此镜像的支持范围。 [集群配置](IMPLEMENTATION.md#集群模式) · [Apache Kafka 镜像参数](https://github.com/apache/kafka/blob/4.4.0/docker/examples/README.md)

## English

The published `latest` and `1.0.0` tags contain Linux amd64 and arm64 variants. Use `latest` for a quick try or pin `1.0.0` to deploy that version. Run Compose with `SIMDKAFKA_IMAGE=dotnetorgcn/simdkafka:latest docker compose up -d --no-build --pull always`. The single-node example publishes Kafka TCP on `127.0.0.1:9092`, disables topic auto-creation and persists data in a named volume. To build locally from this repository, run `docker compose up -d --build`. [Docker Hub image](https://hub.docker.com/r/dotnetorgcn/simdkafka)

The image runs the verified SimdKafka 1.0.0 release on .NET 10 as a non-root user. It accepts one Kafka listener, honors the Kafka-style listener, advertised address, log directory, partition count and auto-creation settings in the table above, and keeps the regular SimdKafka CLI options. Set the advertised address to a host and port that clients can reach, and mount `/var/lib/kafka/data` for persistence.

Clusters use a mounted SimdKafka JSON file and `KAFKA_NODE_ID` for each member. Use the native SimdKafka TLS and SASL settings or PKCS#12 file aliases in the table. See [configuration and security](CONFIGURATION.md) and the [Kafka compatibility matrix](KAFKA_COMPATIBILITY.en.md) for supported features.
