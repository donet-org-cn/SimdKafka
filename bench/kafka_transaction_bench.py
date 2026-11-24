"""Exercise Kafka transaction protocol v1 and verify every returned batch and marker."""

import argparse
from contextlib import ExitStack
import hashlib
import json
from pathlib import Path
import struct
import time

from kafka_wire_bench import Peer, batch, create_topic, crc32c, frame, topic_routes, wire_string


def endpoint(value):
    host, port = value.rsplit(":", 1)
    return host, int(port)


def connect(address):
    host, port = address
    return Peer(port, host)


def check(error, api):
    if error:
        raise RuntimeError(f"{api} returned {error}")


def coordinator(peer, name):
    request = frame(10, 2, wire_string(name) + b"\x01")
    until = time.monotonic() + 25
    while True:
        response = peer.call(request)
        _, error, message_length = struct.unpack_from("!ihh", response)
        if error not in (14, 15, 16) or time.monotonic() >= until:
            check(error, "FindCoordinator")
            address = 8 + max(0, message_length)
            _, length = struct.unpack_from("!ih", response, address)
            address += 6
            return bytes(response[address:address + length]).decode(), struct.unpack_from("!i", response, address + length)[0]
        time.sleep(0.1)


def find_routes(peer, topic, partitions):
    until = time.monotonic() + 30
    while time.monotonic() < until:
        _, routes = topic_routes(peer, topic)
        if len(routes) == partitions:
            return routes
        time.sleep(0.05)
    raise RuntimeError(f"Topic {topic!r} did not establish {partitions} partition leaders")


def create(peer, topic, partitions, replicas):
    until = time.monotonic() + 20
    while time.monotonic() < until:
        controller, _ = topic_routes(peer, topic)
        if controller is not None:
            target = connect(controller)
            try:
                create_topic(target, topic, partitions, replicas)
                return
            except RuntimeError as error:
                if "error 41," not in str(error):
                    raise
            finally:
                target.close()
        time.sleep(0.05)
    raise RuntimeError("No topic controller became available")

def identity(peer, name, expiration):
    request = frame(22, 1, wire_string(name) + struct.pack("!i", expiration))
    until = time.monotonic() + 25
    while True:
        _, error, producer, epoch = struct.unpack("!ihqh", peer.call(request))
        if error not in (14, 15, 16, 51) or time.monotonic() >= until:
            check(error, "InitProducerId")
            return producer, epoch
        time.sleep(0.1)


def enlist(peer, name, topic, producer, epoch, partitions):
    request = wire_string(name) + struct.pack("!qhi", producer, epoch, 1) + wire_string(topic)
    request += struct.pack("!i", partitions) + b"".join(struct.pack("!i", p) for p in range(partitions))
    wire = frame(24, 2, request)
    until = time.monotonic() + 20
    while True:
        response = peer.call(wire)
        count = struct.unpack_from("!i", response, 8 + len(wire_string(topic)))[0]
        if count != partitions:
            raise RuntimeError(f"Wrong AddPartitionsToTxn response count {count}")
        errors = {error for _, error in struct.iter_unpack("!ih", response[12 + len(wire_string(topic)):])} - {0}
        if not errors:
            return
        if errors - {51, 55} or time.monotonic() >= until:
            check(min(errors), "AddPartitionsToTxn")
        time.sleep(0.01)


def end(peer, name, producer, epoch, commit):
    request = wire_string(name) + struct.pack("!qh?", producer, epoch, commit)
    return struct.unpack("!ih", peer.call(frame(26, 2, request)))[1]


def describe(peer, name):
    if len(name) >= 127:
        raise ValueError("The transaction ID must fit the compact test payload")
    header = struct.pack("!hhi", 65, 0, 1) + wire_string(b"txn-integrity-check") + b"\0"
    request = header + bytes([2, len(name) + 1]) + name + b"\0"
    response = peer.call(struct.pack("!i", len(request)) + request)
    if response[0] != 0 or response[5] != 2:
        raise RuntimeError("Unexpected DescribeTransactions response")
    check(struct.unpack_from("!h", response, 6)[0], "DescribeTransactions")
    state_offset = 8 + response[8]
    return bytes(response[state_offset + 1:state_offset + response[state_offset]]).decode()

def make_batch(producer, epoch, first, count, value):
    records = bytearray(batch(count, len(value), value=value, producer=producer, sequence=first))
    struct.pack_into("!h", records, 21, 16)
    struct.pack_into("!h", records, 51, epoch)
    struct.pack_into("!I", records, 17, crc32c(memoryview(records)[21:]))
    return records


def append(peer, name, topic, part, records):
    request = wire_string(name) + struct.pack("!hii", -1, 10000, 1) + wire_string(topic)
    request += struct.pack("!iii", 1, part, len(records)) + records
    response = peer.call(frame(0, 8, request))
    _, error, offset = struct.unpack_from("!ihq", response, 8 + len(wire_string(topic)))
    check(error, "Produce")
    return offset


def fetch(peer, topic, partition, offset, committed):
    limit = 1024 * 1024
    request = struct.pack("!iiiibi", -1, 0, 0, limit, int(committed), 1) + wire_string(topic)
    request += struct.pack("!iiqqi", 1, partition, offset, -1, limit)
    response = peer.call(frame(1, 5, request))
    start = 12 + len(wire_string(topic))
    p, error, high, lso, _, aborted = struct.unpack_from("!ihqqqi", response, start)
    check(error, "Fetch")
    if p != partition:
        raise RuntimeError(f"Expected partition {partition}, received {p}")
    start += 34
    index = list(struct.iter_unpack("!qq", response[start:start + max(0, aborted) * 16]))
    start += max(0, aborted) * 16
    size = struct.unpack_from("!i", response, start)[0]
    if size != len(response) - start - 4:
        raise RuntimeError("Invalid Kafka records length")
    return high, lso, index, response[start + 4:]


def latest_offset(peer, topic, partition, committed):
    name = wire_string(topic)
    request = struct.pack("!ibi", -1, int(committed), 1) + name
    request += struct.pack("!iiiq", 1, partition, -1, -1)
    response = peer.call(frame(2, 5, request))
    actual, error, _, offset, _ = struct.unpack_from("!ihqqi", response, 12 + len(name))
    check(error, "ListOffsets")
    if actual != partition or offset < 0:
        raise RuntimeError("Unexpected latest offset")
    return offset


def verify(peer, topic, partition, ledger, producer, count, start_offset=0, allow_later=False):
    offset, digest = start_offset, hashlib.sha256()
    aborted_seen = set()
    expected_end = start_offset + len(ledger) * (count + 1)
    aborted_expected = {(producer, start_offset + i * (count + 1))
                        for i, (commit, _) in enumerate(ledger) if not commit}
    until = time.monotonic() + 20
    while offset < expected_end:
        high, stable, aborted, remaining = fetch(peer, topic, partition, offset, True)
        if not allow_later and (high > expected_end or stable > expected_end):
            raise RuntimeError(f"Expected final watermarks {expected_end}, observed {high}/{stable}")
        if high < expected_end or stable < expected_end:
            if time.monotonic() < until:
                time.sleep(0.01)
                continue
            raise RuntimeError(f"Expected final watermarks {expected_end}, observed {high}/{stable}")
        aborted_seen.update(entry for entry in aborted if start_offset <= entry[1] < expected_end)
        progressed = False
        while len(remaining) >= 12 and offset < expected_end:
            base, length = struct.unpack_from("!qi", remaining)
            if length + 12 > len(remaining):
                break
            current = remaining[:length + 12]
            if base != offset or crc32c(current[21:]) != struct.unpack_from("!I", current, 17)[0]:
                raise RuntimeError(f"Bad offset or CRC at {offset}")
            transaction, record = divmod(offset - start_offset, count + 1)
            commit, expected = ledger[transaction]
            if record == 0:
                actual = hashlib.sha256(current[16:]).hexdigest()
                if actual != expected or current[22] != 16:
                    raise RuntimeError(f"Wrong transaction payload at {base}")
                digest.update(bytes.fromhex(actual))
                offset += count
            else:
                if current[22] != 48 or current[69] != int(commit):
                    raise RuntimeError(f"Wrong commit/abort marker at {base}")
                digest.update(bytes([current[69]]))
                offset += 1
            remaining = remaining[length + 12:]
            progressed = True
        if not progressed:
            raise RuntimeError(f"Read stalled at offset {offset}")
    if aborted_seen != aborted_expected:
        raise RuntimeError("Aborted transaction index did not match the expected ledger")
    return digest.hexdigest()


def produce_ledger(peer, topic, name, routes, args, stage_only):
    with ExitStack() as resources:
        coord = connect(coordinator(peer, name))
        resources.callback(coord.close)
        identity_value = identity(coord, name, args.expiration)
        writers = {address: connect(address) for address in set(routes.values())}
        for writer in writers.values():
            resources.callback(writer.close)
        offsets = dict.fromkeys(routes, 0)
        if args.reuse_topic:
            for part, address in routes.items():
                high = latest_offset(writers[address], topic, part, False)
                if latest_offset(writers[address], topic, part, True) != high:
                    raise RuntimeError(f"Reused topic {topic!r} has unresolved transactions")
                offsets[part] = high
        ledger, timings = [], []
        if args.start_at is not None:
            remaining = args.start_at - time.time()
            if remaining <= 0:
                raise RuntimeError("Scheduled start passed before the producer was ready")
            time.sleep(remaining)
        started = time.time_ns()
        start = time.perf_counter()
        transactions = 1 if stage_only else args.transactions
        for turn in range(transactions):
            count = args.records
            value = hashlib.sha256(topic + struct.pack("!i", turn)).digest()
            value = (value * ((args.size + 31) // 32))[:args.size]
            records = make_batch(*identity_value, turn * count, count, value)
            single = time.perf_counter_ns()
            enlist(coord, name, topic, *identity_value, len(routes))
            enlisted = time.perf_counter_ns()
            previous = enlisted
            per_partition = []
            for part, address in routes.items():
                if append(writers[address], name, topic, part, records) != offsets[part] + turn * (count + 1):
                    raise RuntimeError("Unexpected transactional Produce offset")
                finished = time.perf_counter_ns()
                per_partition.append((part, (finished - previous) / 1e6))
                previous = finished
            commit = turn % 3 != 2
            if not stage_only:
                check(end(coord, name, *identity_value, commit), "EndTxn")
                finished = time.perf_counter_ns()
                timings.append({"enlist_ms": (enlisted - single) / 1e6, "produce_ms": per_partition,
                                "end_ms": (finished - previous) / 1e6, "total_ms": (finished - single) / 1e6})
            ledger.append((commit, hashlib.sha256(records[16:]).hexdigest()))
        if stage_only:
            for part, address in routes.items():
                high, stable, _, data = fetch(writers[address], topic, part, offsets[part], True)
                if high != offsets[part] + args.records or stable != offsets[part] or len(data):
                    raise RuntimeError("Open transaction was visible before its decision")
        return identity_value, ledger, timings, offsets, time.perf_counter() - start, started, time.time_ns()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["run", "stage", "commit", "describe", "verify"])
    parser.add_argument("--bootstrap", required=True, type=endpoint)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--partitions", type=int, default=2)
    parser.add_argument("--replicas", type=int, default=3)
    parser.add_argument("--transactions", type=int, default=24)
    parser.add_argument("--records", type=int, default=32)
    parser.add_argument("--size", type=int, default=256)
    parser.add_argument("--expiration", type=int, default=180_000)
    parser.add_argument("--reuse-topic", action="store_true")
    parser.add_argument("--allow-later", action="store_true", help="Verify the saved offset range even after later appends")
    parser.add_argument("--verify-replica", type=endpoint, help="Read the saved ranges directly from this broker in verify mode")
    parser.add_argument("--start-at", type=float)
    parser.add_argument("--state", required=True, type=Path)
    args = parser.parse_args()
    if args.verify_replica and args.mode != "verify":
        parser.error("--verify-replica is only available in verify mode")
    topic = args.topic.encode()
    name = topic + b"-producer"
    with ExitStack() as resources:
        peer = connect(args.bootstrap)
        resources.callback(peer.close)
        if args.mode in ("run", "stage"):
            if not args.reuse_topic:
                create(peer, topic, args.partitions, args.replicas)
            routes = find_routes(peer, topic, args.partitions)
            identity_value, ledger, timings, offsets, elapsed, started, finished = produce_ledger(
                peer, topic, name, routes, args, args.mode == "stage")
            state = {"topic": args.topic, "identity": identity_value, "ledger": ledger, "routes": routes,
                     "coordinator": coordinator(peer, name), "records_per_batch": args.records,
                     "records_written": len(ledger) * args.records * args.partitions, "seconds": elapsed,
                     "started_ns": started, "finished_ns": finished, "start_offsets": offsets}
            if timings:
                duration = sorted(row["total_ms"] for row in timings)
                state["latencies_ms"] = duration
                state["timings_ms"] = timings
                state.update({"p50_ms": duration[len(duration) // 2], "p99_ms": duration[(len(duration) * 99 - 1) // 100]})
            args.state.write_text(json.dumps(state))
            if args.mode == "stage":
                print(json.dumps(state))
                return
        else:
            state = json.loads(args.state.read_text())
            if state["topic"] != args.topic:
                raise RuntimeError("Topic does not match the staged state")
            identity_value, ledger = state["identity"], state["ledger"]
            routes = (dict.fromkeys(map(int, state["routes"]), args.verify_replica) if args.verify_replica
                      else find_routes(peer, topic, len(state["routes"])))
        if args.mode in ("commit", "describe"):
            address = coordinator(peer, name)
            coord = connect(address)
            resources.callback(coord.close)
            result = {"error": end(coord, name, *identity_value, True)} if args.mode == "commit" else {"state": describe(coord, name)}
            print(json.dumps({"coordinator": address, **result}))
            return
        digests = {}
        for part, address in routes.items():
            reader = connect(address)
            resources.callback(reader.close)
            offsets = state.get("start_offsets", {})
            digests[part] = verify(reader, topic, part, ledger, identity_value[0], state["records_per_batch"],
                                   offsets.get(str(part), offsets.get(part, 0)), args.allow_later)
        state["verified_digests"] = digests
        print(json.dumps({key: value for key, value in state.items()
                          if key not in ("ledger", "latencies_ms", "timings_ms")}, sort_keys=True))


if __name__ == "__main__":
    main()
