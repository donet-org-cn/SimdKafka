# SimdKafka 安全与配置

SimdKafka 通过原生 Kafka TCP 接收请求。未设置 TLS 的实例默认为明文传输，适合受控测试网络。对外提供服务时可使用 `SASL_SSL` 或双向 `SSL`，并启用 ACL。

## 启动设置

运行 `dotnet run --project src/SimdKafka -- --help` 查看参数。启动参数变更需要重启对应 Broker。

| 参数 | 默认值或用途 |
| --- | --- |
| `--listen IP:PORT` | `127.0.0.1:9092`。监听所有网卡时必须指定客户端可达的公布地址 |
| `--advertise-host HOST`、`--advertise-port PORT` | 默认使用绑定的 IP 和端口。原生客户端通过 Metadata 和 FindCoordinator 获得这些地址 |
| `--data DIR` | `data`。每个实例使用自己的目录，存储锁阻止两个进程共享目录 |
| `--tiered-storage-dir DIR` | 单节点分层后端。指定已挂载的文件系统目录，与数据目录分开 |
| `--tier-segment-bytes BYTES`、`--local-retention-bytes BYTES` | 单节点分层主题的默认归档块下限和本地尾部字节数。默认 16 MiB 和 64 MiB |
| `--local-retention-ms MS` | 单节点分层主题的默认本地时间阈值。默认 `-1`，关闭按时间归档 |
| `--partitions N`、`--no-auto-create` | 默认自动创建一个分区。分区数有效范围为 1～128。可用 `--no-auto-create` 关闭自动创建 |
| `--unsafe-no-fsync` | 单节点默认刷盘后确认。该选项关闭确认前刷盘，仅适用于允许掉电丢失数据的测试。集群拒绝此选项 |
| `--tls-pfx FILE`、`--tls-password-file FILE` | 启用 TLS 1.2/1.3。PFX 需要私钥，证书 SAN 覆盖公布地址。密码文件末尾换行会被忽略 |
| `--tls-client-ca FILE` | 要求对外客户端持有指定 CA 签发的证书。也可与 SASL 同时使用 |
| `--sasl-plain-users FILE` | 启用 `SASL_SSL` 的 `PLAIN`，文件格式为用户名到密码的 JSON 对象 |
| `--sasl-scram-users FILE` | 启用已配置的 `SCRAM-SHA-256` 或 `SCRAM-SHA-512`，可与 PLAIN 同时启用 |
| `--sasl-dynamic-scram` | 启用持久化在线 SCRAM 管理。需要 TLS 和 ACL 管理员；集群 JSON 设置 `"dynamicScram": true` |
| `--sasl-oauth-config FILE` | 启用 `SASL_SSL` 的 `OAUTHBEARER`，从 JSON 指定发行方、受众和 JWKS |
| `--delegation-token-secret-file FILE` | 委派令牌的 Base64 签名密钥。需要 TLS、持久化和一个直接身份认证机制 |
| `--acl-super-user User:NAME` | 启用默认拒绝的 ACL。可重复指定，需要 SASL 或双向 TLS |
| `--cluster-config FILE`、`--node-id ID` | 启动集群 Broker，所有节点使用同一集群 JSON 和各自的 ID 与数据目录 |
| `--cluster-pfx FILE`、`--cluster-ca FILE`、`--cluster-password-file FILE` | 独立的集群内部双向 TLS。内网和公网证书可使用不同的 CA |

PLAIN 文件包含明文密码。SCRAM 文件以用户名为一级键、机制为二级键，每个条目包含 `iterations`、`salt`、`storedKey`、`serverKey`。后三个字段采用 Base64，存放 SCRAM 验证材料。应仅向服务运行账户授予这些文件、证书私钥和密码文件的读取权限。PLAIN 与 SCRAM 文件凭据在启动时加载。 [SCRAM 规范](https://www.rfc-editor.org/rfc/rfc5802)

## 启用认证和 ACL

以下示例从指定路径加载凭据。`users.json` 中应包含管理账号 `admin` 和需要单独授权的应用账号。

```bash
dotnet SimdKafka.dll --listen 0.0.0.0:9092 --advertise-host kafka.example.com \
  --data /var/lib/simdkafka --no-auto-create \
  --tls-pfx broker.pfx --tls-password-file broker.pass \
  --sasl-plain-users users.json --acl-super-user User:admin
```

Java Kafka 工具和客户端读取标准 `admin.properties`。将示例密码字段替换成对应账号的值，并限制文件访问权限。SCRAM 客户端使用对应的 `sasl.mechanism` 和 `org.apache.kafka.common.security.scram.ScramLoginModule`。Confluent.Kafka 和 Go 客户端可用各自的 TLS 与 SASL 配置传入相同参数。

```properties
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
ssl.truststore.type=PEM
ssl.truststore.location=/etc/simdkafka/ca.pem
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="REPLACE_WITH_ADMIN_PASSWORD";
```

- SASL 身份为 `User:<用户名>`，区分大小写。仅使用双向 TLS 时，身份为 `User:` 加 .NET 读取到的完整证书 Subject。双向 TLS 与 SASL 同时配置时，账户授权使用 SASL 用户名。
- 支持 Topic、Group、Cluster、TransactionalId、User 和 DelegationToken 资源，可使用精确名称、名称前缀、通配主体 `User:*`、通配主机 `*` 或实际 TCP 对端 IP。相匹配的 DENY 优先于同操作的 ALLOW。`--acl-super-user` 指定的认证身份拥有全部权限。
- 普通消费者至少需要 Topic `Read`，使用消费组时还需 Group `Read`。普通生产者需 Topic `Write`，事务生产者还需 TransactionalId `Write`。外部两阶段提交需要 TransactionalId `TwoPhaseCommit`。
- ACL 修改由 `CreateAcls` 和 `DeleteAcls` 管理，需 Cluster `Alter`。查询需要 Cluster `Describe`。限制每集群或单节点最多 4,096 条及序列化大小上限。管理员可通过 Kafka 工具为已连接客户端授予或收回权限。
- 启用集群 ACL 时，在各节点使用的集群 JSON 顶层设置 `"clientAclRequired": true`，各 Broker 配置客户端认证、相同管理身份及 `--cluster-pfx`、`--cluster-ca`。对外监听器、控制器、RPC 和副本端口各自独立。启用 ACL 的已有集群应先替换全部 Broker，统一开启设置后再开放客户端访问。普通用户请求无法确认最新控制器权限时按拒绝处理。

```bash
kafka-acls.sh --bootstrap-server kafka.example.com:9092 --command-config admin.properties \
  --add --allow-principal User:orders-reader --operation Read \
  --topic orders- --resource-pattern-type prefixed
kafka-acls.sh --bootstrap-server kafka.example.com:9092 --command-config admin.properties \
  --add --allow-principal User:orders-reader --operation Read --group orders-consumers
kafka-acls.sh --bootstrap-server kafka.example.com:9092 --command-config admin.properties \
  --list --topic orders-2026 --resource-pattern-type match
```

## 在线调整批次限制

| 层级 | 参数 | 生效范围 |
| --- | --- | --- |
| Topic | `max.message.bytes` | 此 Topic 的所有新 Produce 批次。可在 `CreateTopics` 原子设定或事后修改 |
| 指定 Broker | `message.max.bytes` | 此 Broker 接收的写入，且 Topic 没有专属值 |
| Broker 默认 | `message.max.bytes` | Topic 与指定 Broker 均未配置时继承 |

默认值及可配置上限为 8,388,608 字节，允许设为 0 以拒绝新记录批次。单位为编码后的整个批次字节数。降低限制后，之前确认的批次仍可读取。超限返回 Kafka `MESSAGE_TOO_LARGE`。`DescribeConfigs` 可查询值与继承来源，`IncrementalAlterConfigs` 支持 SET、DELETE 和 validate-only，旧版 `AlterConfigs` 按资源替换可写配置。配置变更纳入目录快照或控制器仲裁日志，集群各 Broker 在应用元数据后使用新值。

```bash
kafka-configs.sh --bootstrap-server kafka.example.com:9092 --command-config admin.properties \
  --entity-type topics --entity-name orders-2026 --alter --add-config max.message.bytes=1048576
kafka-configs.sh --bootstrap-server kafka.example.com:9092 --command-config admin.properties \
  --entity-type brokers --entity-default --alter --add-config message.max.bytes=2097152
kafka-configs.sh --bootstrap-server kafka.example.com:9092 --command-config admin.properties \
  --entity-type topics --entity-name orders-2026 --describe --all
kafka-configs.sh --bootstrap-server kafka.example.com:9092 --command-config admin.properties \
  --entity-type topics --entity-name orders-2026 --alter --delete-config max.message.bytes
```

查询和修改 Topic 设置分别要求 Topic `DescribeConfigs` 和 `AlterConfigs`，Broker 设置需要对应 Cluster 权限。`ListConfigResources` v1 需要 Cluster `DescribeConfigs`，可按类型查询带显式覆盖的 Topic 与 Broker。其他能查询的设置显示为只读，修改未实现的键会返回错误。全量 API 版本及未实现项见[兼容对照表](KAFKA_COMPATIBILITY.md)。 [Kafka 配置参考](https://kafka.apache.org/44/configuration/topic-configs/) · [Kafka ACL 参考](https://kafka.apache.org/44/security/authorization-and-acls/)

## 消息时间戳

`message.timestamp.type` 可在创建 Topic 时指定，也可在线修改。默认 `CreateTime` 保留生产者时间。设置 `LogAppendTime` 后，Broker 为新写入的整个批次赋同一个追加时间，并在 Produce 回执中返回该时间。消费记录和按时间查询的 offset 都使用写入时的类型。修改设置后，历史批次保留原值。幂等重试返回原 offset 和原追加时间。

```bash
kafka-configs.sh --bootstrap-server 127.0.0.1:9092 \
  --entity-type topics --entity-name orders --alter --add-config message.timestamp.type=LogAppendTime
kafka-configs.sh --bootstrap-server 127.0.0.1:9092 \
  --entity-type brokers --entity-default --alter --add-config log.message.timestamp.type=LogAppendTime
```

Topic 级设置优先于 Broker 集群级默认值。删除 Topic 设置后恢复继承。单机和集群均持久化配置。集群由分区 leader 在复制前赋值，副本按已复制的批次恢复时间戳。Magic 2 批次只更新头、时间戳类型位和 CRC32C，保留压缩后的记录字节。 [Kafka 消息格式](https://kafka.apache.org/44/implementation/message-format/)

`message.timestamp.before.max.ms` 和 `message.timestamp.after.max.ms` 控制 `CreateTime` 相对 Broker 时钟允许提前和超前的最大毫秒数。默认过去方向无限制，未来方向为一小时。集群级默认键使用 `log.` 前缀。设置可在创建 Topic 时提供，也可在线修改。超限批次返回 `INVALID_TIMESTAMP`，不会移动 offset。使用 `LogAppendTime` 的 Topic 使用 Broker 赋值。 [Kafka Topic 配置](https://kafka.apache.org/44/configuration/topic-configs/#message.timestamp.type)

## 在线管理 SCRAM 凭据

在已有 TLS 和 ACL 启动参数中添加 `--sasl-dynamic-scram`。首次启动时可用 PLAIN 或文件 SCRAM 管理员建立在线账号。集群所有节点使用相同的 TLS、ACL 和 SCRAM 设置。在共享集群 JSON 中同时设置 `"clientAclRequired": true` 和 `"dynamicScram": true`。文件定义的账号和机制仍从对应文件管理，各 Broker 使用一致的文件凭据。

```bash
kafka-configs.sh --bootstrap-server kafka.example.com:9092 --command-config admin.properties \
  --alter --entity-type users --entity-name orders-app \
  --add-config-file orders-scram.properties
kafka-configs.sh --bootstrap-server kafka.example.com:9092 --command-config admin.properties \
  --describe --entity-type users --entity-name orders-app
kafka-configs.sh --bootstrap-server kafka.example.com:9092 --command-config admin.properties \
  --alter --entity-type users --entity-name orders-app --delete-config SCRAM-SHA-256
```

`orders-scram.properties` 示例内容为 `SCRAM-SHA-256=[iterations=8192,password=REPLACE_WITH_PASSWORD]`。文件只允许部署账号读取。可以分开管理 SHA-256 与 SHA-512，在线凭据支持 4,096～16,384 次迭代。查询只返回机制和迭代次数，存储只保留盐值、迭代次数与验证密钥。更新或删除影响新连接和进行中的认证，已认证连接仍用当前身份直至断开。查询和修改分别要求 Cluster `Describe` 和 `Alter`。使用原有数据目录开启在线凭据管理前，应把数据目录及集群 `control` 目录权限限制为服务账户可访问。 [Kafka SCRAM 管理](https://kafka.apache.org/44/security/authentication-using-sasl/#authentication-using-sasl-scram)

## OAuth Bearer 登录

在 TLS 启动参数中加 `--sasl-oauth-config /etc/simdkafka/oauth.json`。同一个监听器可以同时配置 PLAIN、SCRAM 和 OAUTHBEARER。Broker 启动时加载配置和 JWKS，修改密钥后重启对应 Broker。

```json
{
  "issuer": "https://id.example.com",
  "audience": "simdkafka",
  "jwksFile": "issuer.jwks.json",
  "clockSkewSeconds": 15
}
```

`jwksFile` 可相对 OAuth 配置文件解析。JWT 必须使用 `RS256`，带有在 JWKS 中唯一匹配的 `kid`，以及有效的 `iss`、`aud`、`sub` 和 `exp`。RSA 公钥最少 2,048 位。Broker 校验可选 `nbf`，`clockSkewSeconds` 仅用于允许略早于该时间的客户端，默认为 0，范围 0～120。ACL 身份为 `User:<sub>`。支持在原连接上使用同一身份的新 JWT 重新认证。连接过期后再次发请求会被断开，客户端可用新凭据重连。HTTPS JWKS 自动轮换、其他签名算法和 Kerberos 尚未实现。 [Kafka SASL 重新认证协议](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/89068981/KIP-368%2BAllow%2BSASL%2BConnections%2Bto%2BPeriodically%2BRe-Authenticate)

Kafka Java 客户端补充以下选项，并通过原生 Kafka OAuth 回调提供凭据。回调可向发行方申请令牌。

```properties
security.protocol=SASL_SSL
sasl.mechanism=OAUTHBEARER
sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;
sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler
sasl.oauthbearer.token.endpoint.url=https://id.example.com/oauth2/token
```

另行配置 TLS 信任链和发行方要求的 OAuth 客户端凭据。 [Kafka OAUTHBEARER 配置](https://kafka.apache.org/44/security/authentication-using-sasl/#authentication-using-sasl-oauthbearer)

## 委派令牌

将至少 32 字节随机密钥编码为 Base64 文件，仅授予服务账户读取权限。通过 `--delegation-token-secret-file FILE` 加载，同时配置 TLS 和 PLAIN、SCRAM 或 OAuth 供首次登录。集群在共享 JSON 中设置 `"delegationTokens": true`，所有 Broker 和控制器使用相同密钥并通过内部双向 TLS 通信。单节点使用目录快照，集群使用控制器 Raft 持久化。令牌 HMAC 和签名密钥不会写入元数据快照。

```bash
kafka-delegation-tokens.sh --bootstrap-server kafka.example.com:9092 \
  --command-config admin.properties --create --renewer-principal User:orders-renewer
kafka-delegation-tokens.sh --bootstrap-server kafka.example.com:9092 \
  --command-config admin.properties --describe
```

新令牌默认有效 24 小时，最长可续至签发后 7 天。单节点或集群最多保存 1,024 个活跃令牌，每个令牌最多 16 个续期者。所有者和列出的续期者可更新或立即撤销令牌。代其他身份签发需要在对应 `User:NAME` 资源上获得 `CreateTokens`，也可用 `DescribeTokens` 或 `DelegationToken` 的 `Describe` 授予查看权限。查看和创建的响应含 HMAC，应按密码处理。

```properties
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="TOKEN_ID" password="TOKEN_HMAC_BASE64" tokenauth="true";
```

令牌连接使用所有者的 ACL，不能管理委派令牌。撤销后新认证会读取已提交的状态，已有连接在所在 Broker 应用撤销后失效。令牌密钥采用手动轮换，先撤销旧令牌并更新全部节点密钥，再签发新令牌。 [Kafka 委派令牌](https://kafka.apache.org/44/security/authentication-using-sasl/#authentication-using-delegation-tokens)

## 带宽与连接速率配额

原生配额 API 接受 `producer_byte_rate` 和 `consumer_byte_rate`，单位为字节每秒。按用户、client ID 或两者组合配置具体值与默认值。优先应用用户加客户端的具体规则，其次按 Kafka 的用户、客户端和默认值顺序匹配。Broker 对匹配到同一实际身份范围的连接合并计量，并在 Produce、Fetch 和 ShareFetch 响应中返回节流时间。当前每个 Broker 独立计量，一秒突发额度，单次带宽节流上限 30 秒。请求线程占用和控制器变更配额尚待实现。

```bash
kafka-configs.sh --bootstrap-server kafka.example.com:9092 --command-config admin.properties \
  --alter --entity-type users --entity-name orders-app \
  --add-config producer_byte_rate=1048576,consumer_byte_rate=2097152
kafka-configs.sh --bootstrap-server kafka.example.com:9092 --command-config admin.properties \
  --describe --entity-type users --entity-name orders-app
kafka-configs.sh --bootstrap-server kafka.example.com:9092 --command-config admin.properties \
  --alter --entity-type ips --entity-name 192.0.2.40 --add-config connection_creation_rate=100
```

`connection_creation_rate` 单位为每秒新连接数，支持具体 IPv4、IPv6 或默认 IP 实体。Broker 在 TLS 握手前执行，此时已建立的连接继续服务。设为 0 会拒绝该来源的新连接。集群 Broker 在应用配置后开始使用新配额。查询需要 Cluster `DescribeConfigs`，修改需要 Cluster `AlterConfigs`。 [Kafka 配额 API](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/134745533/KIP-546%2BAdd%2BClient%2BQuota%2BAPIs%2Bto%2Bthe%2BAdmin%2BClient)

## Share Groups

集群模式支持 Kafka 4.4 的 `KafkaShareConsumer`。客户端设置 `group.id` 后订阅 Topic。使用 `share.acknowledgement.mode=explicit` 可逐条选择 ACCEPT、RELEASE 或 REJECT，再通过 `commitSync` 提交确认。v2 客户端也可续租交付锁。同一个分区可由多个成员并行领取。ACL 模式要求对应 Group 和 Topic 的 Read 权限。

新游标默认从首次领取时的日志末尾初始化。需要读取历史记录时，可在空组通过 `Admin.alterShareGroupOffsets` 设置为目标分区的有效 offset，再启动消费者。可查询、重置或删除空组的 offset。重复领取保留原 offset 并增加交付次数。当前锁定 30 秒、最多交付 5 次，每分区最多 2,048 个在途 offset。成员和交付状态由控制器 Raft 持久化，Share 会话在 Broker 重启后由客户端重建。当前使用 `read_uncommitted`，Share 专属配置尚未开放。 [Apache Kafka Share Consumer](https://kafka.apache.org/44/javadoc/org/apache/kafka/clients/consumer/KafkaShareConsumer.html)

## 查询集群控制器仲裁

```bash
kafka-metadata-quorum.sh --bootstrap-server kafka.example.com:9092 \
  --command-config admin.properties describe --status
kafka-metadata-quorum.sh --bootstrap-server kafka.example.com:9092 \
  --command-config admin.properties describe --replication
```

集群启用 ACL 时需要 Cluster `Describe`。返回 SimdKafka 控制器 Raft 的选举任期、下一个待提交的日志位置及各投票节点当前日志末端。无法访问的节点返回 `-1` 日志末端。节点采用 Raft 推送复制，Kafka 的 Fetch 时间戳字段返回未知。投票集合由启动配置指定。 [Kafka DescribeQuorum 协议](https://kafka.apache.org/44/design/protocol/#The_Messages_DescribeQuorum)

## 单节点分层存储

挂载远端文件系统后，将其独立目录传给 `--tiered-storage-dir`。数据目录和归档目录之间不能互相包含。首次开启后，Broker 在两端保存同一个存储身份，以检查挂载位置。归档主题使用正常刷盘模式。

```bash
dotnet SimdKafka.dll --data /var/lib/simdkafka --tiered-storage-dir /mnt/kafka-archive

kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --topic archived-events \
  --partitions 1 --replication-factor 1 --config remote.storage.enable=true \
  --config segment.bytes=16777216 --config local.retention.bytes=67108864

kafka-configs.sh --bootstrap-server 127.0.0.1:9092 --entity-type topics --entity-name archived-events \
  --alter --add-config local.retention.bytes=134217728,local.retention.ms=3600000
```

`remote.storage.enable=true` 可在建 Topic 时设置，也可通过 `IncrementalAlterConfigs` 打开。归档以完整批次为边界，验证远端副本的 SHA-256 并持久化归档目录后，再通过文件系统稀疏块回收本地空间。支持稀疏块释放的 Linux 或 Windows 文件系统会实际释放已归档空间。普通 Kafka Fetch 会按需读取归档和本地尾部，重启时从两层数据重建索引。原生 `ListOffsets` 区分全局最早、本地最早和最新归档位置。v11 还能查询最早待归档 offset。未开启分层的 Topic 查询归档位置时返回未找到。v10+ 的请求可为分层时间查找指定超时，超时检查在文件系统调用前后执行，正在进行的阻塞读取依赖挂载后端返回。 [Kafka ListOffsets 协议](https://kafka.apache.org/44/design/protocol/#The_Messages_ListOffsets)

`local.retention.ms` 按批次最大记录时间戳判断，只归档连续的合格前缀。满足时间阈值时，低流量分区可以归档低于 `segment.bytes` 的完整批次。两个本地阈值启用时，满足其一即可归档。局部配置设为 `-1` 表示无限，设为 `-2` 继承当前无限的全局日志保留策略，删除局部配置后恢复启动时的默认值。

现有实现保留远端段和目录，包括逻辑删记录或删除 Topic 后的归档对象。在线关闭分层、远端保留和回收、对象存储插件及集群 WAL 分层恢复尚未实现。 [Apache Kafka 分层存储](https://kafka.apache.org/44/operations/tiered-storage/) · [Kafka Topic 配置](https://kafka.apache.org/44/configuration/topic-configs/)
