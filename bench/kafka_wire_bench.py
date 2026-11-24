"""Run identical Kafka requests against a dedicated local broker on Linux."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from functools import cache
import json
import math
import os
from pathlib import Path
import socket
import struct
import threading
import time
import uuid


def wire_string(value):
    return struct.pack("!h", len(value)) + value


def frame(api, version, body=b""):
    request = struct.pack("!hhi", api, version, 1) + wire_string(b"same-wire-bench") + body
    return struct.pack("!i", len(request)) + request


class Peer:
    def __init__(self, port, host="127.0.0.1"):
        self.sock = socket.create_connection((host, port), timeout=20)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.size = bytearray(4)
        self.buffer = bytearray(2 * 1024 * 1024)

    def close(self):
        self.sock.close()

    def read_exact(self, view):
        while view:
            size = self.sock.recv_into(view)
            if not size:
                raise EOFError("broker disconnected")
            view = view[size:]

    def call(self, request):
        self.sock.sendall(request)
        self.read_exact(memoryview(self.size))
        size = struct.unpack("!i", self.size)[0]
        if not 4 <= size <= len(self.buffer):
            raise ValueError(f"invalid response size {size}")
        view = memoryview(self.buffer)[:size]
        self.read_exact(view)
        if struct.unpack_from("!i", view)[0] != 1:
            raise ValueError("incorrect correlation ID")
        return view[4:]


@cache
def crc_table():
    table = []
    for i in range(256):
        for _ in range(8):
            i = (i >> 1) ^ (0x82F63B78 if i & 1 else 0)
        table.append(i)
    return table


def crc32c(data):
    table = crc_table()
    crc = 0xFFFFFFFF
    for byte in data:
        crc = (crc >> 8) ^ table[(crc ^ byte) & 255]
    return crc ^ 0xFFFFFFFF


def varint(value):
    bits = (value << 1) ^ (value >> 63)
    result = bytearray()
    while bits > 127:
        result.append((bits & 127) | 128)
        bits >>= 7
    result.append(bits)
    return bytes(result)


def batch(count, size, value=None, producer=-1, sequence=-1):
    stamp = time.time_ns() // 1_000_000
    body = bytearray(struct.pack("!hiqqqhii", 0, count - 1, stamp, stamp, producer,
                                 -1 if producer < 0 else 0, sequence, count))
    value = b"X" * size if value is None else value
    for index in range(count):
        record = b"\0\0" + varint(index) + varint(-1) + varint(size) + value + b"\0"
        body.extend(varint(len(record)))
        body.extend(record)
    return struct.pack("!qiibI", 0, len(body) + 9, -1, 2, crc32c(body)) + body


def create_topic(peer, topic, partitions, replication_factor=1):
    name = wire_string(topic)
    body = struct.pack("!i", 1) + name + struct.pack("!ihiiib", partitions, replication_factor, 0, 0, 8000, 0)
    response = peer.call(frame(19, 4, body))
    error = struct.unpack_from("!h", response, 8 + len(name))[0]
    if error:
        raise RuntimeError(f"CreateTopics error {error}, topic {topic.decode()}")


def delete_topic(peer, topic):
    name = wire_string(topic)
    request = struct.pack("!i", 1) + name + struct.pack("!i", 10000)
    response = peer.call(frame(20, 3, request))
    count = struct.unpack_from("!i", response, 4)[0]
    if count != 1 or bytes(response[8:8 + len(name)]) != name:
        raise RuntimeError(f"Unexpected DeleteTopics response for {topic.decode()}")
    return struct.unpack_from("!h", response, 8 + len(name))[0]


def topic_routes(peer, topic):
    request = struct.pack("!i", 1) + wire_string(topic) + b"\0\0\0"
    response = peer.call(frame(3, 8, request))
    position = 4

    def read(fmt):
        nonlocal position
        value = struct.unpack_from(fmt, response, position)[0]
        position += struct.calcsize(fmt)
        return value

    def name():
        nonlocal position
        length = read("!h")
        if length < 0:
            return None
        result = bytes(response[position:position + length])
        position += length
        return result

    def skip_nodes():
        nonlocal position
        count = read("!i")
        position += count * 4

    members = {}
    for _ in range(read("!i")):
        identity, host, port = read("!i"), name(), read("!i")
        name()  # rack
        members[identity] = (host.decode(), port)
    name()  # cluster ID
    controller = members.get(read("!i"))
    leaders = {}
    for _ in range(read("!i")):
        error, topic_name = read("!h"), name()
        read("!b")  # internal topic
        for _ in range(read("!i")):
            partition_error, partition, leader = read("!h"), read("!i"), read("!i")
            read("!i")  # leader epoch
            for _ in range(3):
                skip_nodes()
            if topic_name == topic and not error and not partition_error and leader in members:
                leaders[partition] = members[leader]
        read("!i")  # authorised topic operations
    return controller, leaders


def wait_for_leaders(peer, topic, partitions):
    # Kafka can advertise partition leaders before their local logs have opened.
    name = wire_string(topic)
    offsets = b"".join(struct.pack("!iiq", p, -1, -1) for p in range(partitions))
    body = struct.pack("!ibi", -1, 0, 1) + name + struct.pack("!i", partitions) + offsets
    request = frame(2, 5, body)
    partition_response = struct.Struct("!ihqqi")
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        response = peer.call(request)
        position = 12 + len(name)
        entries = list(partition_response.iter_unpack(response[position:]))
        if len(entries) == partitions and all(error == 0 and offset == 0 for _, error, _, offset, _ in entries):
            return
        time.sleep(0.05)
    raise RuntimeError(f"partition leaders did not become ready for {topic.decode()}")


def produce(peer, topic, partition, records, batches, count):
    name = wire_string(topic)
    body = (struct.pack("!hhii", -1, -1, 10000, 1) + name +
            struct.pack("!iii", 1, partition, len(records)) + records)
    request = frame(0, 8, body)
    latencies = []
    for i in range(batches):
        start = time.perf_counter_ns()
        response = peer.call(request)
        elapsed = time.perf_counter_ns() - start
        index, error, offset = struct.unpack_from("!ihq", response, 8 + len(name))
        if error or index != partition or offset != i * count:
            raise RuntimeError(f"Produce partition {partition}, error {error}, offset {offset}, batch {i}")
        latencies.append(elapsed)
    return latencies


def fetch(peer, topic, partition, count):
    name = wire_string(topic)
    limit = 1024 * 1024
    body = struct.pack("!iiiibi", -1, 0, 1, limit, 0, 1) + name + struct.pack("!ii", 1, partition)
    latencies = []
    offset = 0
    while offset < count:
        request = frame(1, 5, body + struct.pack("!qqi", offset, -1, limit))
        start = time.perf_counter_ns()
        response = peer.call(request)
        elapsed = time.perf_counter_ns() - start
        index, error, high, _, _, aborted = struct.unpack_from("!ihqqqi", response, 12 + len(name))
        if error or index != partition or high != count:
            raise RuntimeError(f"Fetch partition {partition}, error {error}, high watermark {high}")
        pos = 46 + len(name) + max(0, aborted) * 16
        size = struct.unpack_from("!i", response, pos)[0]
        pos += 4
        end = pos + size
        if size <= 0 or end != len(response):
            raise RuntimeError(f"Invalid Fetch payload size {size}")
        first_offset = offset
        while end - pos >= 12:
            base, length = struct.unpack_from("!qi", response, pos)
            if length < 49:
                raise RuntimeError(f"Invalid batch length {length} at offset {offset}")
            batch_end = pos + 12 + length
            # Kafka may cut the final batch at MaxBytes. The next Fetch starts at that batch.
            if batch_end > end:
                break
            delta = struct.unpack_from("!i", response, pos + 23)[0]
            batch_count = struct.unpack_from("!i", response, pos + 57)[0]
            if base != offset or batch_count <= 0 or delta + 1 != batch_count:
                raise RuntimeError(f"Invalid returned batch at offset {offset}")
            offset += batch_count
            pos = batch_end
        if offset == first_offset:
            raise RuntimeError("Fetch returned no complete batch")
        latencies.append(elapsed)
    if offset != count:
        raise RuntimeError(f"Expected {count} records, got {offset}")
    return latencies


def process_sample(pid, include_breakdown=False):
    proc = Path(f"/proc/{pid}")
    fields = (proc / "stat").read_text().rsplit(")", 1)[1].split()
    status = (proc / "status").read_text().splitlines()
    cpu = (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")
    rss = next(int(line.split()[1]) for line in status if line.startswith("VmRSS:")) / 1024
    if not include_breakdown:
        return cpu, rss
    resident = {name: int(amount) / 1024 for name, amount, *_ in (line.split() for line in status if
                line.startswith(("RssAnon:", "RssFile:")))}
    return cpu, rss, resident.get("RssAnon:", 0), resident.get("RssFile:", 0)


def phase(args, count, worker):
    rss = []
    stop = threading.Event()

    def sample():
        while not stop.wait(0.05):
            rss.append(process_sample(args.pid)[1])

    with ExitStack() as sockets, ThreadPoolExecutor(max_workers=args.partitions) as pool:
        peers = [Peer(args.port) for _ in range(args.partitions)]
        for peer in peers:
            sockets.callback(peer.close)
        cpu_start, rss_start = process_sample(args.pid)
        rss.append(rss_start)
        client_start = time.process_time()
        start = time.perf_counter()
        monitor = threading.Thread(target=sample)
        monitor.start()
        try:
            jobs = [pool.submit(worker, peer, i) for i, peer in enumerate(peers)]
            elapsed_ns = [latency for job in jobs for latency in job.result()]
        finally:
            wall = time.perf_counter() - start
            client_cpu = time.process_time() - client_start
            stop.set()
            monitor.join()
        cpu_end, rss_end = process_sample(args.pid)
        rss.append(rss_end)
    elapsed_ns.sort()

    def percentile(p):
        return round(elapsed_ns[math.ceil(p * len(elapsed_ns)) - 1] / 1_000_000, 3)

    cpu = cpu_end - cpu_start
    return {
        "messages": count,
        "wall_sec": round(wall, 4),
        "msg_per_sec": round(count / wall),
        "mib_per_sec": round(count * args.message_bytes / wall / 2**20, 2),
        "broker_cpu_pct_one_core": round(cpu / wall * 100, 2),
        "broker_cpu_seconds": round(cpu, 4),
        "client_cpu_pct_one_core": round(client_cpu / wall * 100, 2),
        "broker_rss_mean_mib": round(sum(rss) / len(rss), 1),
        "broker_rss_peak_mib": round(max(rss), 1),
        "rtt_p50_ms": percentile(0.50),
        "rtt_p95_ms": percentile(0.95),
        "rtt_p99_ms": percentile(0.99),
        "requests": len(elapsed_ns),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--broker", required=True)
    parser.add_argument("--mode", required=True)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--partitions", type=int, default=4)
    parser.add_argument("--batches", type=int, default=512)
    parser.add_argument("--batch-records", type=int, default=128)
    parser.add_argument("--message-bytes", type=int, default=1024)
    args = parser.parse_args()
    run = uuid.uuid4().hex[:12]
    records = batch(args.batch_records, args.message_bytes)
    if min(args.rounds, args.partitions, args.batches, args.batch_records, args.message_bytes) <= 0 or len(records) > 1024**2:
        parser.error("all sizes must be positive and each batch must be at most 1 MiB")
    idle_rss = round(process_sample(args.pid)[1], 1)
    for round_number in range(args.rounds + 1):
        topic = f"bench-{run}-{round_number}".encode()
        peer = Peer(args.port)
        try:
            create_topic(peer, topic, args.partitions)
            wait_for_leaders(peer, topic, args.partitions)
        finally:
            peer.close()
        batches = min(args.batches, 64) if round_number == 0 else args.batches
        count = batches * args.batch_records
        production = phase(args, count * args.partitions,
                           lambda peer, p: produce(peer, topic, p, records, batches, args.batch_records))
        consumption = phase(args, count * args.partitions, lambda peer, p: fetch(peer, topic, p, count))
        if round_number:
            print(json.dumps({"broker": args.broker, "mode": args.mode, "round": round_number,
                              "idle_rss_mib": idle_rss, "topic": topic.decode(),
                              "produce": production, "fetch": consumption}), flush=True)


if __name__ == "__main__":
    main()
