"""Coordinate two Linux load generators and verify every Kafka batch by SHA-256."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import math
import os
import shutil
import socket
import struct
import threading
import time
import uuid

from kafka_wire_bench import Peer, batch, create_topic, delete_topic, frame, process_sample, topic_routes, wait_for_leaders, wire_string


class Link:
    def __init__(self, sock):
        self.sock = sock
        self.reader = sock.makefile("rb")

    def send(self, message):
        self.sock.sendall(json.dumps(message, separators=(",", ":")).encode() + b"\n")

    def receive(self):
        line = self.reader.readline(8 * 1024 * 1024)
        if not line.endswith(b"\n"):
            raise RuntimeError("control channel closed or message exceeded limit")
        return json.loads(line)

    def close(self):
        self.reader.close()
        self.sock.close()


class ProcessMonitor:
    def __init__(self, pid):
        self.pid = pid
        self.cpu_start, *resident = process_sample(pid, include_breakdown=True)
        self.rss = [resident]
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.sample)
        self.thread.start()

    def sample(self):
        while not self.stop.wait(0.05):
            self.rss.append(process_sample(self.pid, include_breakdown=True)[1:])

    def finish(self):
        self.stop.set()
        self.thread.join()
        cpu_end, *resident = process_sample(self.pid, include_breakdown=True)
        self.rss.append(resident)
        total, anonymous, mapped = zip(*self.rss)
        return {"cpu_seconds": round(cpu_end - self.cpu_start, 3),
                "rss_mean_mib": round(sum(total) / len(total), 1),
                "rss_peak_mib": round(max(total), 1),
                "rss_anon_mean_mib": round(sum(anonymous) / len(anonymous), 1),
                "rss_file_mean_mib": round(sum(mapped) / len(mapped), 1)}


def digest(data):
    return hashlib.sha256(memoryview(data)[16:]).hexdigest()


def put(port, host, topic, p, encoded, hashes):
    name = wire_string(topic)
    prefix = struct.pack("!hhii", -1, -1, 10000, 1) + name + struct.pack("!ii", 1, p)
    results, samples = [], []
    peer = Peer(port, host)
    try:
        for records, checksum in zip(encoded, hashes, strict=True):
            request = frame(0, 8, prefix + struct.pack("!i", len(records)) + records)
            start = time.perf_counter_ns()
            response = peer.call(request)
            samples.append(time.perf_counter_ns() - start)
            index, error, offset = struct.unpack_from("!ihq", response, 8 + len(name))
            if error or index != p or offset < 0:
                raise RuntimeError(f"Produce partition {p} error {error}, offset {offset}")
            results.append((p, offset, checksum))
        return results, samples
    finally:
        peer.close()


def pull(port, host, topic, p, target, mixed=False, strict_watermark=True):
    name = wire_string(topic)
    max_bytes = 1024 * 1024
    body = (struct.pack("!iiiibi", -1, 200 if mixed else 0, 1, max_bytes, 0, 1) +
            name + struct.pack("!ii", 1, p))
    results, samples = [], []
    offset, watermark_lags = 0, 0
    deadline = time.monotonic() + 240
    peer = Peer(port, host)
    try:
        while offset < target:
            if time.monotonic() > deadline:
                raise TimeoutError(f"Fetch partition {p} at {offset} of {target}")
            request = frame(1, 5, body + struct.pack("!qqi", offset, -1, max_bytes))
            start = time.perf_counter_ns()
            response = peer.call(request)
            samples.append(time.perf_counter_ns() - start)
            index, error, high, _, _, aborted = struct.unpack_from("!ihqqqi", response, 12 + len(name))
            if (error or index != p or not 0 <= high <= target or (strict_watermark and high < offset)
                    or (not mixed and high != target)):
                raise RuntimeError(f"Fetch partition {p}, error {error}, watermark {high}, offset {offset}")
            pos = 46 + len(name) + max(0, aborted) * 16
            size = struct.unpack_from("!i", response, pos)[0]
            pos += 4
            end = pos + size
            if size < 0 or end != len(response):
                raise RuntimeError(f"Invalid Fetch payload size {size}")
            previous = offset
            while end - pos >= 12:
                base, length = struct.unpack_from("!qi", response, pos)
                if length < 49:
                    raise RuntimeError(f"Invalid batch length at {p}/{offset}")
                batch_end = pos + 12 + length
                if batch_end > end:
                    break
                count = struct.unpack_from("!i", response, pos + 57)[0]
                if base != offset or count <= 0 or offset + count > target:
                    raise RuntimeError(f"Invalid batch at {p}/{offset}, base={base}, count={count}, high={high}")
                if offset + count > high:
                    if strict_watermark or not mixed:
                        raise RuntimeError(f"Batch at {p}/{offset} ends above watermark {high}")
                    watermark_lags += 1
                results.append((p, base, digest(response[pos:batch_end])))
                offset += count
                pos = batch_end
            if offset == previous and (not mixed or size):
                raise RuntimeError(f"No complete batch at {p}/{offset}")
        return results, samples, watermark_lags
    finally:
        peer.close()


def connect(args):
    until = time.monotonic() + args.connect_timeout_seconds
    while True:
        try:
            sock = socket.create_connection((args.host, args.control_port), timeout=2)
            sock.settimeout(360)
            return Link(sock)
        except OSError:
            if time.monotonic() > until:
                raise
            time.sleep(0.2)


def work(args):
    link = connect(args)
    try:
        link.send({"node": args.node, "host": socket.gethostname(),
                   "idle_rss_mib": process_sample(args.pid)[1] if args.pid else None})
        plan = link.receive()
        batches, workers = plan["batches"], plan["workers"]
        records, size = plan["records"], plan["size"]
        encoded, hashes = [], []
        for w in range(workers):
            work_batches = []
            for i in range(batches):
                tag = struct.pack("!iii", args.node, w, i)
                work_batches.append(batch(records, size, tag + b"X" * (size - len(tag))))
            encoded.append(work_batches)
            hashes.append([digest(b) for b in work_batches])
        link.send({"ready": True})

        while True:
            instruction = link.receive()
            mode = instruction["phase"]
            if mode == "stop":
                return
            topic = instruction["topic"].encode()
            count = instruction["batches"]
            partitions = plan["partitions"]
            target = 2 * workers // partitions * count * records
            routes = instruction.get("leaders", {})
            monitor = ProcessMonitor(args.pid) if args.pid else None
            try:
                check_disk(args.storage_root, plan.get("reserve_mib", 0))
                with ThreadPoolExecutor(max_workers=workers + partitions) as pool:
                    puts, pulls = [], []
                    if mode in ("produce", "mixed"):
                        for w in range(workers):
                            p = (args.node * workers + w) % partitions
                            host, port = routes.get(str(p), (args.host, args.port))
                            puts.append(pool.submit(put, port, host, topic, p,
                                                    encoded[w][:count], hashes[w][:count]))
                    if mode in ("fetch", "mixed"):
                        for p in range(args.node, partitions, 2):
                            host, port = routes.get(str(p), (args.host, args.port))
                            pulls.append(pool.submit(pull, port, host, topic, p, target,
                                                     mode == "mixed", plan["strict_watermark"]))
                    written = [job.result() for job in puts]
                    read = [job.result() for job in pulls]
                link.send({"done": True})
                if not link.receive().get("phase_end"):
                    raise RuntimeError("missing end of phase")
                broker = monitor.finish() if monitor else None
                monitor = None
                link.send({"written": [row for w, _ in written for row in w],
                           "read": [row for r, _, _ in read for row in r],
                           "put_ns": [ns for _, times in written for ns in times],
                           "pull_ns": [ns for _, times, _ in read for ns in times],
                           "watermark_lags": sum(lags for _, _, lags in read), "broker": broker,
                           "node": args.node})
            except Exception as error:
                link.send({"error": str(error)})
                raise
            finally:
                if monitor:
                    monitor.finish()
    finally:
        link.close()


def stats(values):
    if not values:
        return {}
    values.sort()
    return {f"p{p}_ms": round(values[math.ceil(p * len(values) / 100) - 1] / 1e6, 3)
            for p in (50, 95, 99)}


def phase(args, links, pool, topic, name, count, leaders):
    monitor = ProcessMonitor(args.pid)
    start = time.monotonic()
    try:
        for link in links:
            link.send({"phase": name, "topic": topic, "batches": count, "leaders": leaders})
        for done in pool.map(lambda link: link.receive(), links):
            if not done.get("done"):
                raise RuntimeError(f"load generator failed: {done}")
        elapsed = time.monotonic() - start
        for link in links:
            link.send({"phase_end": True})
    finally:
        broker = monitor.finish()
    results = list(pool.map(lambda link: link.receive(), links))
    written = [r for node in results for r in node["written"]]
    read = [r for node in results for r in node["read"]]
    n = 2 * args.workers * count * args.records
    measure = {
        "seconds": round(elapsed, 4), "records": n, "records_per_sec": round(n / elapsed),
        "mib_per_sec": round(n * args.message_bytes / elapsed / 2**20, 2),
        "broker_cpu_cores": round(broker["cpu_seconds"] / elapsed, 3),
        "broker_cpu_seconds": broker["cpu_seconds"],
        "rss_mean_mib": broker["rss_mean_mib"], "rss_peak_mib": broker["rss_peak_mib"],
        "produce_rtt": stats([ns for node in results for ns in node["put_ns"]]),
        "fetch_rtt": stats([ns for node in results for ns in node["pull_ns"]]),
        "watermark_lags": sum(node["watermark_lags"] for node in results),
    }
    if args.sample_all_brokers:
        members = {args.host: broker}
        members.update((args.clients[node["node"]], node["broker"]) for node in results)
        cpu = sum(node["cpu_seconds"] for node in members.values())
        measure["cluster"] = {"cpu_cores": round(cpu / elapsed, 3), "cpu_seconds": round(cpu, 3),
                              "rss_sum_mean_mib": round(sum(node["rss_mean_mib"] for node in members.values()), 1),
                              "rss_sum_of_peaks_mib": round(sum(node["rss_peak_mib"] for node in members.values()), 1),
                              "rss_sum_anon_mean_mib": round(sum(node["rss_anon_mean_mib"] for node in members.values()), 1),
                              "rss_sum_file_mean_mib": round(sum(node["rss_file_mean_mib"] for node in members.values()), 1),
                              "members": members}
    return measure, written, read


def verify(args, count, written, read):
    expected = 2 * args.workers * count
    if len(written) != expected or sorted(written) != sorted(read):
        raise RuntimeError(f"Batch digest comparison failed, expected {expected}, wrote {len(written)}, read {len(read)}")
    per_partition = expected // args.partitions
    positions = [(p, offset) for p, offset, _ in sorted(written)]
    if positions != [(p, i * args.records) for p in range(args.partitions) for i in range(per_partition)]:
        raise RuntimeError("Missing or duplicate acknowledged offset")


def create_and_route(args, topic):
    peer = Peer(args.port, args.host)
    try:
        if args.replication_factor == 1:
            create_topic(peer, topic, args.partitions)
            wait_for_leaders(peer, topic, args.partitions)
            return {}
        deadline = time.monotonic() + 25
        controller = None
        while controller is None and time.monotonic() < deadline:
            controller, _ = topic_routes(peer, topic)
            if controller is None:
                time.sleep(0.1)
        if controller is None:
            raise RuntimeError("No cluster controller found")
        host, port = controller
        admin = Peer(port, host)
        try:
            create_topic(admin, topic, args.partitions, args.replication_factor)
        finally:
            admin.close()
        deadline = time.monotonic() + 30
        stable_since = time.monotonic()
        previous = {}
        while time.monotonic() < deadline:
            _, leaders = topic_routes(peer, topic)
            if leaders != previous:
                stable_since = time.monotonic()
                previous = leaders
            if len(leaders) == args.partitions and time.monotonic() - stable_since >= 1:
                return leaders
            time.sleep(0.1)
        raise RuntimeError(f"Replicated partitions did not elect leaders for {topic.decode()}")
    finally:
        peer.close()


def cleanup_topic(args, topic):
    bootstrap = Peer(args.port, args.host)
    deadline = time.monotonic() + 20
    try:
        while time.monotonic() < deadline:
            controller, _ = topic_routes(bootstrap, topic)
            if controller is not None:
                admin = Peer(controller[1], controller[0])
                try:
                    error = delete_topic(admin, topic)
                finally:
                    admin.close()
                if error in (0, 3):
                    return
                if error not in (7, 41):
                    raise RuntimeError(f"DeleteTopics error {error} for {topic.decode()}")
            time.sleep(.2)
        raise TimeoutError(f"Could not delete verified test topic {topic.decode()}")
    finally:
        bootstrap.close()


def stage_plan(args):
    yield False
    if args.duration_seconds:
        deadline = time.monotonic() + args.duration_seconds
        while time.monotonic() < deadline:
            yield True
    else:
        if not args.mixed_only:
            yield from [False] * args.rounds
        if args.mixed or args.mixed_only:
            yield True


def check_disk(root, reserve_mib):
    if reserve_mib and shutil.disk_usage(root).free < reserve_mib * 2**20:
        raise RuntimeError(f"Available disk space at {root} dropped below {reserve_mib} MiB")


def control(args):
    with socket.socket() as listener, ThreadPoolExecutor(max_workers=2) as pool:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((args.host, args.control_port))
        listener.listen(2)
        listener.settimeout(75)
        links = []
        clients = {}
        reserve_mib = args.reserve_disk_mib if args.duration_seconds else 0
        try:
            for _ in range(2):
                sock, address = listener.accept()
                sock.settimeout(360)
                link = Link(sock)
                links.append(link)
                hello = link.receive()
                node = hello["node"]
                if node not in (0, 1) or node in clients or address[0] != args.clients[node]:
                    raise RuntimeError(f"Unexpected load generator {address[0]}")
                clients[node] = {"ip": address[0], "hostname": hello["host"]}
                if args.sample_all_brokers:
                    if hello.get("idle_rss_mib") is None:
                        raise RuntimeError(f"Missing broker PID sample on {address[0]}")
                    clients[node]["broker_idle_rss_mib"] = hello["idle_rss_mib"]
                link.send({"batches": args.batches, "workers": args.workers, "records": args.records, "reserve_mib": reserve_mib,
                           "partitions": args.partitions, "size": args.message_bytes,
                           "strict_watermark": args.broker != "Kafka-4.4.0"})
            if not all(reply.get("ready") for reply in pool.map(lambda c: c.receive(), links)):
                raise RuntimeError("load generators did not prepare")
            idle_rss = process_sample(args.pid)[1]
            run = uuid.uuid4().hex[:10]
            started = None
            measured_rounds = verified_records = 0
            for i, mixed in enumerate(stage_plan(args)):
                if i == 1:
                    started = time.monotonic()
                check_disk(args.storage_root, reserve_mib)
                topic = f"remote-{run}-{i}"
                leaders = create_and_route(args, topic.encode())
                count = args.batches if i else min(32, args.batches)
                if mixed:
                    combined, written, read = phase(args, links, pool, topic, "mixed", count, leaders)
                    metrics = {"mixed": combined}
                else:
                    produce, written, _ = phase(args, links, pool, topic, "produce", count, leaders)
                    fetch, _, read = phase(args, links, pool, topic, "fetch", count, leaders)
                    metrics = {"produce": produce, "fetch": fetch}
                verify(args, count, written, read)
                if i:
                    measured_rounds += 1
                    verified_records += len(read) * args.records
                    digest_set = hashlib.sha256(json.dumps(sorted(read), separators=(",", ":")).encode()).hexdigest()
                    print(json.dumps({"broker": args.broker, "round": i, "topic": topic, "idle_rss_mib": idle_rss,
                                      "clients": clients, "verified_batches": len(read),
                                      "verified_sha256": digest_set,
                                      "replication_factor": args.replication_factor, "leaders": leaders, **metrics}), flush=True)
                if args.duration_seconds:
                    cleanup_topic(args, topic.encode())
            if args.duration_seconds:
                print(json.dumps({"broker": args.broker, "endurance": {"run": run, "seconds_requested": args.duration_seconds,
                                  "seconds_observed": round(time.monotonic() - started, 3), "rounds": measured_rounds,
                                  "verified_records": verified_records}}), flush=True)
            for link in links:
                link.send({"phase": "stop"})
        finally:
            for link in links:
                link.close()


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("role", choices=("control", "work"))
    cli.add_argument("--host", default="127.0.0.1")
    cli.add_argument("--port", type=int, default=29092)
    cli.add_argument("--control-port", type=int, default=29100)
    cli.add_argument("--pid", type=int)
    cli.add_argument("--sample-all-brokers", action="store_true")
    cli.add_argument("--broker", default="test")
    cli.add_argument("--node", type=int, choices=(0, 1))
    cli.add_argument("--clients", nargs=2, default=["127.0.0.1", "127.0.0.1"])
    cli.add_argument("--workers", type=int, default=8)
    cli.add_argument("--partitions", type=int, default=4)
    cli.add_argument("--replication-factor", type=int, default=1)
    cli.add_argument("--batches", type=int, default=256)
    cli.add_argument("--records", type=int, default=64)
    cli.add_argument("--message-bytes", type=int, default=1024)
    cli.add_argument("--rounds", type=int, default=3)
    cli.add_argument("--mixed", action="store_true")
    cli.add_argument("--mixed-only", action="store_true")
    cli.add_argument("--duration-seconds", type=int, default=0,
                     help="Sustain verified mixed batches for this duration and delete each verified test topic")
    cli.add_argument("--storage-root", default=".", help="Path on the local broker data volume")
    cli.add_argument("--reserve-disk-mib", type=int, default=1024, help="Minimum free space on each node for endurance runs")
    cli.add_argument("--connect-timeout-seconds", type=int, default=60, help="Worker connection deadline")
    args = cli.parse_args()
    if args.role == "control":
        if (not args.pid or args.partitions < 2 or args.partitions % 2 or args.workers < 1 or
                2 * args.workers % args.partitions or args.message_bytes < 12 or
                args.message_bytes * args.records > 900_000 or
                min(args.rounds, args.batches, args.records, args.replication_factor) < 1 or
                args.duration_seconds < 0 or args.reserve_disk_mib < 0):
            cli.error("invalid control settings")
        control(args)
    else:
        if args.node is None or args.connect_timeout_seconds < 1:
            cli.error("work requires a node and a positive connect timeout")
        work(args)


if __name__ == "__main__":
    main()
