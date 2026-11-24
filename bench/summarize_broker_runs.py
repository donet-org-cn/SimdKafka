"""Summarize verified comparison rounds, with explicit completion for each broker and tier."""

import argparse
import json
from pathlib import Path
from statistics import median


def read_stage(root, stage, broker):
    directory = root / f"{stage}-{broker}"
    file = directory / "results.jsonl"
    rows = []
    if file.exists():
        for line in file.read_text().splitlines():
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue  # A live writer may still be appending the last line.
    error = directory / "error.log"
    failure = error.exists() and error.stat().st_size != 0
    return rows, (directory / "completed").exists(), failure, directory.exists()


def load_metrics(rows, phase):
    samples = [row[phase] for row in rows if phase in row]
    if not samples:
        return None
    seconds = sum(s["seconds"] for s in samples)
    records = sum(s["records"] for s in samples)
    produce = [s["produce_rtt"]["p99_ms"] for s in samples if s["produce_rtt"]]
    fetch = [s["fetch_rtt"]["p99_ms"] for s in samples if s["fetch_rtt"]]
    return records / seconds, sum(s["broker_cpu_seconds"] for s in samples) / seconds, \
        max(s["rss_peak_mib"] for s in samples), median(produce) if produce else None, median(fetch) if fetch else None


def stage_status(rows, finished, failure, started):
    totals = next((row["endurance"] for row in rows if "endurance" in row), None)
    if finished and totals:
        return f"完成，{totals['seconds_observed']:,.1f} 秒 / {totals['verified_records']:,} 条"
    batches = sum(row.get("verified_batches", 0) for row in rows)
    state = "完成" if finished else "失败" if failure else "运行中" if started else "待开始"
    return f"{state}，已校验 {batches * 64:,} 条"


def render(root):
    out = ["# SimdKafka 与 Apache Kafka 4.4.0 性能对比", "",
           "两种 Broker 依次使用同一台机器。两个发压进程各发起 8 路生产请求，合计 4 路消费。"
           "负载固定为 4 个分区、单副本、1 KiB 消息值、64 条一批、无压缩、原生 TCP、acks=all。"
           "SimdKafka 合并刷盘后确认，Kafka 使用 1 GiB JVM 堆和 log.flush.interval.messages=1。", "",
           "只统计已完成 SHA-256 与 offset 校验的轮次。吞吐与 CPU 按有效负载时间加权。"
           "混合阶段每条记录写入一次并读回一次，吞吐列按一条记录计。延迟列为各轮 P99 的中位数，内存列为 Broker RSS 峰值。"
           "定时阶段观察时长包含每轮的 Topic 创建和清理。", "",
           "| 阶段 | 负载 | Broker | 条/秒 | 平均 CPU 核 | 峰值 RSS MiB | 生产 P99 ms | 拉取 P99 ms |",
           "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |"]
    status = []
    for stage, title in (("quick", "快速"), ("10m", "10 分钟"), ("long", "24 小时")):
        for broker, label in (("simdkafka", "SimdKafka"), ("kafka", "Kafka 4.4.0")):
            rows, finished, failure, started = read_stage(root, stage, broker)
            status.append(f"| {title} | {label} | {stage_status(rows, finished, failure, started)} |")
            for phase, workload in (("produce", "纯入队"), ("fetch", "热缓存出队"), ("mixed", "边写边读")):
                metrics = load_metrics(rows, phase)
                if metrics:
                    throughput, cpu, rss, put, fetch = metrics
                    status_label = label if finished else f"{label}（阶段未结束）"
                    out.append(f"| {title} | {workload} | {status_label} | {throughput:,.0f} | {cpu:.3f} | {rss:,.1f} | "
                               + " | ".join("—" if value is None else f"{value:.3f}" for value in (put, fetch)) + " |")
    out.extend(("", "| 阶段 | Broker | 完成状态 |", "| --- | --- | --- |", *status, ""))
    return "\n".join(out)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    markdown = render(args.directory)
    if args.output is None:
        print(markdown)
    else:
        temporary = args.output.with_suffix(".tmp")
        temporary.write_text(markdown, encoding="utf-8")
        temporary.replace(args.output)


if __name__ == "__main__":
    main()
