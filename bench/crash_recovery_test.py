"""Exercise crash, torn-write, corrupt-data and write-failure recovery on an isolated broker."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import os
import resource
import signal
import socket
import struct
import subprocess
import time
import uuid

from distributed_kafka_bench import digest, pull
from kafka_wire_bench import Peer, batch, crc32c, create_topic, frame, wait_for_leaders, wire_string

TOPIC = b"recovery"
RECORDS = 16


def producer_id(peer):
    response = peer.call(frame(22, 1, struct.pack("!hi", -1, -1)))
    error, identifier, epoch = struct.unpack_from("!hqh", response, 4)
    if error or epoch != 0:
        raise RuntimeError(f"producer ID error {error}, epoch {epoch}")
    return identifier


def produce(peer, partition, records):
    name = wire_string(TOPIC)
    body = (struct.pack("!hhii", -1, -1, 10000, 1) + name +
            struct.pack("!iii", 1, partition, len(records)) + records)
    return struct.unpack_from("!hq", peer.call(frame(0, 8, body)), 12 + len(name))


def high(peer, partition):
    name = wire_string(TOPIC)
    body = (struct.pack("!ibi", -1, 0, 1) + name + struct.pack("!iiiq", 1, partition, -1, -1))
    error, offset = struct.unpack_from("!h8xq", peer.call(frame(2, 5, body)), 16 + len(name))
    if error:
        raise RuntimeError(f"ListOffsets error {error}")
    return offset


def commit(peer, offset):
    group = wire_string(b"recovery-check")
    name = wire_string(TOPIC)
    body = (group + struct.pack("!ih", -1, 0) + struct.pack("!i", 1) + name +
            struct.pack("!iiqi", 1, 0, offset, 0) + wire_string(b"durable"))
    error = struct.unpack_from("!h", peer.call(frame(8, 6, body)), 16 + len(name))[0]
    if error:
        raise RuntimeError(f"OffsetCommit error {error}")


def committed(peer):
    group = wire_string(b"recovery-check")
    name = wire_string(TOPIC)
    body = group + struct.pack("!i", 1) + name + struct.pack("!ii", 1, 0)
    response = peer.call(frame(9, 5, body))
    offset, epoch = struct.unpack_from("!qi", response, 16 + len(name))
    if epoch != 0 or bytes(response[30 + len(name):37 + len(name)]) != b"durable":
        raise RuntimeError("Committed offset metadata changed")
    return offset


def start(args, root, byte_limit=None, expect_failure=False):
    logfile = root / "broker.log"
    root.mkdir(exist_ok=True)

    def ignore_file_size_signal():
        signal.signal(signal.SIGXFSZ, signal.SIG_IGN)

    with logfile.open("ab") as log:
        process = subprocess.Popen(
            ["dotnet", args.dll, "--listen", f"127.0.0.1:{args.port}", "--data", str(root / "data"),
             "--no-auto-create"], stdout=log, stderr=log,
            preexec_fn=ignore_file_size_signal if byte_limit else None)
    if expect_failure:
        try:
            if process.wait(timeout=8) == 0:
                raise RuntimeError("Corrupt log was accepted")
        except subprocess.TimeoutExpired:
            stop(process, hard=True)
            raise RuntimeError("Broker started with a corrupt committed batch") from None
        return None
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and process.poll() is None:
        try:
            with socket.create_connection(("127.0.0.1", args.port), timeout=0.2):
                if byte_limit:
                    resource.prlimit(process.pid, resource.RLIMIT_FSIZE, (byte_limit, byte_limit))
                return process
        except OSError:
            time.sleep(0.03)
    stop(process, hard=True)
    raise RuntimeError(f"Broker exited with {process.returncode}: {logfile.read_text()[-3000:]}")


def stop(process, hard=False):
    if process is None or process.poll() is not None:
        return
    if hard:
        process.kill()
    else:
        process.terminate()
    try:
        process.wait(timeout=8)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)
        raise


def path_for(root, partition):
    data = root / "data"
    document = json.loads((data / "catalog.json").read_text())
    state = document.get("State", document)
    identifier = (state.get("TopicIds") or {}).get(TOPIC.decode())
    key = uuid.UUID(identifier).bytes_le if identifier else TOPIC
    return data / hashlib.sha256(key).hexdigest().upper() / f"{partition}.log"


def checked_read(args, partitions):
    peer = Peer(args.port)
    try:
        limits = [high(peer, p) for p in range(partitions)]
    finally:
        peer.close()
    return [row for p, target in enumerate(limits)
            for row in pull(args.port, "127.0.0.1", TOPIC, p, target)[0]]


def crash_round(args, index):
    root = args.directory / f"kill-{index}"
    process = start(args, root)
    acknowledged, attempted = [], set()
    try:
        peer = Peer(args.port)
        try:
            create_topic(peer, TOPIC, 4)
            wait_for_leaders(peer, TOPIC, 4)
            producers = [producer_id(peer) for _ in range(8)]
            payloads, checksums = [], []
            for i, producer in enumerate(producers):
                items = [batch(RECORDS, 1024, struct.pack("!ii", i, sequence) + b"A" * 1016,
                               producer, sequence * RECORDS) for sequence in range(64)]
                payloads.append(items)
                checksums.append([digest(item) for item in items])
                error, offset = produce(peer, i % 4, items[0])
                if error:
                    raise RuntimeError(f"initial append error {error}")
                acknowledged.append((i % 4, offset, checksums[i][0]))
                attempted.add((i % 4, checksums[i][0]))
            commit(peer, 2 * RECORDS)
        finally:
            peer.close()

        def writer(i):
            connection = Peer(args.port)
            try:
                for sequence in range(1, len(payloads[i])):
                    p = i % 4
                    checksum = checksums[i][sequence]
                    attempted.add((p, checksum))
                    error, offset = produce(connection, p, payloads[i][sequence])
                    if error:
                        raise RuntimeError(f"append error {error}")
                    acknowledged.append((p, offset, checksum))
            except (OSError, EOFError):
                return
            finally:
                connection.close()

        with ThreadPoolExecutor(max_workers=8) as clients:
            jobs = [clients.submit(writer, i) for i in range(8)]
            until = time.monotonic() + 8
            while len(acknowledged) < 24 and time.monotonic() < until:
                time.sleep(0.001)
            active = sum(not job.done() for job in jobs)
            if not active or len(acknowledged) < 24:
                raise RuntimeError("Could not interrupt active writes")
            stop(process, hard=True)
            for job in jobs:
                job.result()

        process = start(args, root)
        recovered = checked_read(args, 4)
        recovered_digests = {(p, checksum) for p, _, checksum in recovered}
        if not set(acknowledged) <= set(recovered) or len(recovered_digests) != len(recovered) or not recovered_digests <= attempted:
            raise RuntimeError("An acknowledged or recovered batch changed after SIGKILL")
        peer = Peer(args.port)
        try:
            if committed(peer) != 2 * RECORDS or producer_id(peer) <= max(producers):
                raise RuntimeError("Catalog metadata changed after SIGKILL")
            candidate = next(row for row in reversed(recovered) if row[0] == 0)
            original = next(item for items in payloads for item in items if digest(item) == candidate[2])
            if produce(peer, 0, original) != (0, candidate[1]):
                raise RuntimeError("Idempotent retry did not return original offset")
            conflicting = bytearray(original)
            conflicting[-2] ^= 1
            struct.pack_into("!I", conflicting, 17, crc32c(conflicting[21:]))
            if produce(peer, 0, bytes(conflicting)) != (45, -1):
                raise RuntimeError("Conflicting retry was accepted")
        finally:
            peer.close()
    finally:
        stop(process)

    path = path_for(root, 0)
    valid_length = path.stat().st_size
    ending = (1 + max(offset // RECORDS for p, offset, _ in recovered if p == 0)) * RECORDS
    damaged_tail = bytearray(batch(RECORDS, 1024))
    struct.pack_into("!qi", damaged_tail, 0, ending, len(damaged_tail) - 12)
    struct.pack_into("!i", damaged_tail, 12, 0)
    with path.open("ab") as file:
        file.write(damaged_tail[:-17])
        file.flush()
        os.fsync(file.fileno())
    process = start(args, root)
    try:
        if path.stat().st_size != valid_length or checked_read(args, 4) != recovered:
            raise RuntimeError("Incomplete tail recovery changed committed data")
        with path.open("r+b") as file:
            file.seek(90)
            old = file.read(1)
            file.seek(90)
            file.write(bytes([old[0] ^ 1]))
            file.flush()
            os.fsync(file.fileno())
        peer = Peer(args.port)
        try:
            name = wire_string(TOPIC)
            limit = 1024 * 1024
            body = (struct.pack("!iiiibi", -1, 0, 1, limit, 0, 1) + name +
                    struct.pack("!iiqqi", 1, 0, 0, -1, limit))
            error = struct.unpack_from("!h", peer.call(frame(1, 5, body)), 16 + len(name))[0]
            if error != 56 or produce(peer, 0, batch(RECORDS, 1024)) != (56, -1):
                raise RuntimeError("Broker did not fail closed after detecting committed corruption")
        finally:
            peer.close()
    finally:
        stop(process)

    start(args, root, expect_failure=True)
    print(json.dumps({"round": index, "active_producers_at_kill": active,
                      "acknowledged_batches": len(acknowledged), "recovered_batches": len(recovered),
                      "offset_commit": "passed", "idempotent_retry": "passed",
                      "torn_tail": "passed", "live_corruption_rejected": True,
                      "committed_corruption_rejected": True}), flush=True)


def write_failure(args):
    root = args.directory / "write-limit"
    process = start(args, root, byte_limit=100_000)
    try:
        peer = Peer(args.port)
        try:
            create_topic(peer, TOPIC, 1)
            wait_for_leaders(peer, TOPIC, 1)
            body = batch(RECORDS, 1024)
            accepted = []
            for i in range(16):
                error, offset = produce(peer, 0, body)
                if error:
                    if error != 56:
                        raise RuntimeError(f"Unexpected write failure {error}")
                    break
                accepted.append((0, offset, digest(body)))
            else:
                raise RuntimeError("Write limit did not cause a fault")
            if not accepted or produce(peer, 0, body) != (56, -1):
                raise RuntimeError("Faulted log still accepted requests")
        finally:
            peer.close()
    finally:
        stop(process)
    process = start(args, root)
    try:
        if checked_read(args, 1) != accepted:
            raise RuntimeError("Write failure damaged a prior acknowledgement")
    finally:
        stop(process)
    print(json.dumps({"simulated_write_failure": "passed", "preserved_acknowledged_batches": len(accepted)}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("dll")
    parser.add_argument("--port", type=int, default=29102)
    parser.add_argument("--rounds", type=int, default=3)
    args = parser.parse_args()
    args.directory.mkdir()
    for i in range(args.rounds):
        crash_round(args, i + 1)
    write_failure(args)


if __name__ == "__main__":
    main()
