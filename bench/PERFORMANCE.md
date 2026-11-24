# SimdKafka vs. Apache Kafka 4.4.0

简体中文 · [English](#english)

## 测试条件

2026 年 11 月 21 至 23 日，两种 Broker 依次使用同一台 8 vCPU、约 7.75 GiB 内存的 Linux 主机，两台独立客户端主机共发起 16 路生产请求和 4 路消费。测试使用原生 Kafka TCP、4 个分区、单副本、1 KiB 消息值、每批 64 条、无压缩和 `acks=all`。SimdKafka 合并刷盘后确认。Kafka 使用官方 JVM 发行包、1 GiB JVM 堆及 `log.flush.interval.messages=1`。

SimdKafka Release DLL SHA-256 为 `c3feb4b3f65b53df3ffe4dc8ec08f6f5087f274f932ed004103e35146717c01c`。

| 阶段 | 负载 | SimdKafka（条/秒） | Kafka 4.4.0（条/秒） |
| --- | --- | ---: | ---: |
| 快速 | 纯生产 | 18,173 | 10,229 |
| 快速 | 热缓存消费 | 776,033 | 671,361 |
| 快速 | 边写边读 | 23,049 | 12,588 |
| 10 分钟 | 边写边读 | 19,765 | 11,718 |
| 24 小时 | 边写边读 | 19,947 | 10,362 |

| 24 小时边写边读 | SimdKafka | Kafka 4.4.0 |
| --- | ---: | ---: |
| 实际持续时间（秒） | 86,402.068 | 86,414.078 |
| 平均 Broker CPU（核） | 0.251 | 0.119 |
| 峰值 Broker RSS（MiB） | 107.3 | 1,218.3 |
| 单轮生产请求 P99 的中位数（ms） | 243.813 | 524.409 |
| 单轮拉取请求 P99 的中位数（ms） | 193.328 | 345.376 |
| 通过 offset 和 SHA-256 校验的消息数 | 1,711,144,960 | 885,391,360 |

混合吞吐按写入且读回的记录数计算，每条记录计一次。吞吐与 CPU 按有效负载时间加权，定时阶段的持续时间包含 Topic 创建和清理。两段长测的已确认和读回数据均通过分区、连续 offset 和批次 SHA-256 对账。Kafka 记录到 80 个批次末端超过当次 Fetch 报告水位的观测，SimdKafka 为 0。这些批次均通过最终对账。

结论适用于这个单副本、明文 TCP、未压缩和持久化配置下的负载。测试脚本见[原生协议负载生成器](distributed_kafka_bench.py)和[对比编排](compare_broker_runs.sh)。

## English

The brokers ran sequentially on the same Linux host with 8 vCPUs and approximately 7.75 GiB of RAM. Two separate load generators drove a total of 16 producer streams and 4 fetch streams. The workload used native Kafka TCP, four partitions, one replica, 1 KiB record values, uncompressed batches of 64 records, and `acks=all`. SimdKafka acknowledged after grouped durable writes. Apache Kafka 4.4.0 ran the official JVM distribution with a 1 GiB heap and `log.flush.interval.messages=1`.

| Phase | Workload | SimdKafka (records/s) | Kafka 4.4.0 (records/s) |
| --- | --- | ---: | ---: |
| Quick | Produce | 18,173 | 10,229 |
| Quick | Hot-cache fetch | 776,033 | 671,361 |
| Quick | Mixed | 23,049 | 12,588 |
| 10 minutes | Mixed | 19,765 | 11,718 |
| 24 hours | Mixed | 19,947 | 10,362 |

| 24-hour mixed workload | SimdKafka | Kafka 4.4.0 |
| --- | ---: | ---: |
| Observed duration (s) | 86,402.068 | 86,414.078 |
| Average broker CPU (cores) | 0.251 | 0.119 |
| Peak broker RSS (MiB) | 107.3 | 1,218.3 |
| Median per-round produce request P99 (ms) | 243.813 | 524.409 |
| Median per-round fetch request P99 (ms) | 193.328 | 345.376 |
| Records reconciled by offset and SHA-256 | 1,711,144,960 | 885,391,360 |

Mixed throughput counts each record once after it is written and fetched. Throughput and CPU figures are weighted by workload time. Observed stage duration also includes topic creation and cleanup. The 24-hour runs reconciled acknowledged and fetched partitions, contiguous offsets, and batch SHA-256 digests. The harness logged 80 Kafka fetch batches whose end offsets exceeded the high watermark reported in those responses, and zero for SimdKafka. All of those batches reconciled successfully.

These results apply to the measured single-replica, plaintext, uncompressed durable workload. See the [wire load generator](distributed_kafka_bench.py) and the [comparison runner](compare_broker_runs.sh).
