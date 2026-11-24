"""Build and verify a ledger for an idempotent three-broker failover test."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import struct
import time

from distributed_kafka_bench import digest, pull
from kafka_wire_bench import Peer, batch, create_topic, frame, topic_routes, wire_string


def find_route(args, topic, kind):
    for host in args.hosts:
        peer = None
        try:
            peer = Peer(args.port, host)
            peer.sock.settimeout(3)
            controller, leaders = topic_routes(peer, topic)
            route = controller if kind == "controller" else leaders.get(0)
            if route is not None:
                return route
        except (OSError, EOFError):
            continue
        finally:
            if peer is not None:
                peer.close()
    raise ConnectionError("No Kafka route available")


def allocate_id(args):
    for host in args.hosts:
        peer = None
        try:
            peer = Peer(args.port, host)
            response = peer.call(frame(22, 1, struct.pack("!hi", -1, -1)))
            error, identity, epoch = struct.unpack_from("!hqh", response, 4)
            if not error and identity > 0 and epoch == 0:
                return identity
        except (OSError, EOFError):
            continue
        finally:
            if peer is not None:
                peer.close()
    raise RuntimeError("Could not allocate an idempotent producer")


def send(args, topic, worker):
    identity = allocate_id(args)
    name = wire_string(topic)
    prefix = struct.pack("!hhii", -1, -1, 5000, 1) + name + struct.pack("!ii", 1, 0)
    pending = []
    for number in range(args.batches):
        message = f"{args.node}/{worker}/{number:05d}".encode().ljust(args.size, b".")
        encoded = batch(args.records, args.size, message, producer=identity, sequence=number * args.records)
        pending.append((frame(0, 8, prefix + struct.pack("!i", len(encoded)) + encoded), digest(encoded)))
    if time.time() >= args.start_at:
        raise RuntimeError("Producer setup missed the scheduled start time")
    time.sleep(max(0, args.start_at - time.time()))
    started = time.time()
    retries = 0
    deadline = time.monotonic() + 100
    acknowledgements = []
    peer = None
    try:
        for request, checksum in pending:
            while True:
                if time.monotonic() >= deadline:
                    raise TimeoutError("Idempotent producer timed out")
                try:
                    if peer is None:
                        host, port = find_route(args, topic, "leader")
                        peer = Peer(port, host)
                        peer.sock.settimeout(3)
                    response = peer.call(request)
                    index, error, offset = struct.unpack_from("!ihq", response, 8 + len(name))
                    if not error and index == 0 and offset >= 0:
                        acknowledgements.append((0, offset, checksum))
                        break
                    if error not in (5, 6, 7):
                        raise RuntimeError(f"Unexpected Produce response {index}/{error}/{offset}")
                except (OSError, EOFError):
                    pass
                if peer is not None:
                    peer.close()
                    peer = None
                retries += 1
                time.sleep(0.06)
        return {"producer": identity, "started": started, "finished": time.time(),
                "retries": retries, "acks": acknowledgements}
    finally:
        if peer is not None:
            peer.close()


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("role", choices=("create", "write", "read", "verify"))
    cli.add_argument("--topic", required=True)
    cli.add_argument("--hosts", nargs="+", required=True, help="Addresses of the brokers under test")
    cli.add_argument("--port", type=int, default=39092)
    cli.add_argument("--node", type=int, choices=(0, 1))
    cli.add_argument("--start-at", type=float)
    cli.add_argument("--workers", type=int, default=4)
    cli.add_argument("--batches", type=int, default=128)
    cli.add_argument("--records", type=int, default=64)
    cli.add_argument("--size", type=int, default=256)
    cli.add_argument("--fault-at", type=float)
    cli.add_argument("--files", nargs=3, type=Path)
    args = cli.parse_args()
    topic = args.topic.encode()
    total_batches = 2 * args.workers * args.batches
    if args.role == "create":
        host, port = find_route(args, topic, "controller")
        peer = Peer(port, host)
        try:
            create_topic(peer, topic, 1, replication_factor=3)
        finally:
            peer.close()
        print(json.dumps({"leader": find_route(args, topic, "leader")}))
    elif args.role == "write":
        if args.node is None or args.start_at is None:
            cli.error("write requires --node and --start-at")
        with ThreadPoolExecutor(max_workers=args.workers) as workers:
            writers = list(workers.map(lambda worker: send(args, topic, worker), range(args.workers)))
        print(json.dumps(writers))
    elif args.role == "read":
        if len(args.hosts) != 1:
            cli.error("read requires exactly one host")
        target = total_batches * args.records
        records, _, _ = pull(args.port, args.hosts[0], topic, 0, target)
        print(json.dumps(records))
    else:
        if args.files is None or args.fault_at is None:
            cli.error("verify requires --files and --fault-at")
        first, second, observed = [json.loads(file.read_text(encoding="utf-8")) for file in args.files]
        writers = first + second
        confirmed = sorted(ack for writer in writers for ack in writer["acks"])
        retries = sum(writer["retries"] for writer in writers)
        overlaps = sum(writer["started"] < args.fault_at < writer["finished"] for writer in writers)
        offsets = list(range(0, total_batches * args.records, args.records))
        if (len(confirmed) != total_batches or [ack[1] for ack in confirmed] != offsets or
                sorted(observed) != confirmed or retries <= 0 or overlaps < 1):
            raise RuntimeError(f"Ledger or fault mismatch, acks={len(confirmed)}, retries={retries}, overlaps={overlaps}")
        print(json.dumps({"verified_records": total_batches * args.records, "verified_batches": total_batches,
                          "retries": retries, "writers_active_at_fault": overlaps}))


if __name__ == "__main__":
    main()
