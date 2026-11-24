#!/usr/bin/env bash
set -euo pipefail

directory=$(cd -- "${1:?benchmark directory required}" && pwd)
broker=${2:?simdkafka or kafka}
mode=${3:?page-cache or fsync}
port=${BENCH_PORT:-29092}
host=${BENCH_HOST:-127.0.0.1}
controller_port=$((port + 1))
run="$directory/$broker-$mode${BENCH_RUN_SUFFIX:-}"
if [[ "$mode" != page-cache && "$mode" != fsync ]]; then
    echo "Expected mode page-cache or fsync" >&2
    exit 2
fi
mkdir -- "$run"
mkdir -- "$run/data"
pid=

finish() {
    if [[ -n "$pid" ]]; then
        kill -TERM "$pid" 2>/dev/null || true
        wait "$pid" || true
    fi
    find "$run/data" -type f -exec sync -d {} +
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

case "$broker" in
    simdkafka)
        flush=()
        if [[ "$mode" == page-cache ]]; then
            flush=(--unsafe-no-fsync)
        fi
        dotnet "${BENCH_APP_DIR:-$directory}/SimdKafka.dll" --listen "$host:$port" --no-auto-create \
            --data "$run/data" "${flush[@]}" >"$run/broker.log" 2>&1 &
        pid=$!
        name=SimdKafka
        ;;
    kafka)
        kafka="$directory/kafka_2.13-4.4.0"
        export JAVA_HOME="$directory/jre"
        export LOG_DIR="$run"
        config="$run/server.properties"
        cat >"$config" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=127.0.0.1:$controller_port
listeners=PLAINTEXT://$host:$port,CONTROLLER://127.0.0.1:$controller_port
advertised.listeners=PLAINTEXT://$host:$port
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs=$run/data
default.replication.factor=1
min.insync.replicas=1
auto.create.topics.enable=false
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
EOF
        if [[ "$mode" == fsync ]]; then
            echo "log.flush.interval.messages=1" >>"$config"
        fi
        cluster_id=$("$kafka/bin/kafka-storage.sh" random-uuid)
        "$kafka/bin/kafka-storage.sh" format --standalone -t "$cluster_id" -c "$config" >"$run/format.log" 2>&1
        "$kafka/bin/kafka-server-start.sh" "$config" >"$run/broker.log" 2>&1 &
        pid=$!
        name=Kafka-4.4.0
        ;;
    *)
        echo "Expected broker simdkafka or kafka" >&2
        exit 2
        ;;
esac

echo "$pid" >"$run/broker.pid"
python3 - "$host" "$port" "$pid" <<'PY'
import os
import socket
import sys
from time import monotonic, sleep

host = sys.argv[1]
port, pid = map(int, sys.argv[2:])
deadline = monotonic() + 60
while monotonic() < deadline:
    os.kill(pid, 0)
    try:
        with socket.create_connection((host, port), timeout=0.25):
            break
    except OSError:
        sleep(0.15)
else:
    raise SystemExit("broker never opened TCP port")
PY
sleep 2
if [[ "${BENCH_CLIENT_MODE:-local}" == distributed ]]; then
    python3 "$directory/distributed_kafka_bench.py" control --host "$host" --port "$port" \
        --pid "$pid" --broker "${BENCH_LABEL:-$name}" "${@:4}" | tee "$run/results.jsonl"
else
    python3 "$directory/kafka_wire_bench.py" --port "$port" --pid "$pid" --broker "$name" --mode "$mode" \
        "${@:4}" | tee "$run/results.jsonl"
fi
