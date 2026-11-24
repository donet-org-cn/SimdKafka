#!/usr/bin/env bash
set -euo pipefail
umask 077

directory=$(cd -- "${1:?run directory required}" && pwd)
role=${2:?control or work required}
host=${BENCH_HOST:-127.0.0.1}
clients=("${BENCH_CLIENT_1:-127.0.0.1}" "${BENCH_CLIENT_2:-127.0.0.1}")
port=${BENCH_PORT:-39700}
control_port=${BENCH_CONTROL_PORT:-39710}
base=${BENCH_ASSETS_DIR:-$(dirname "$directory")}
image=${BENCH_JRE_IMAGE:-apache/kafka:4.4.0}
duration_10m=${BENCH_10M_SECONDS:-600}
duration_long=${BENCH_LONG_SECONDS:-86400}
stages=(quick:simdkafka quick:kafka 10m:simdkafka 10m:kafka long:simdkafka long:kafka)
program="$directory/distributed_kafka_bench.py"
broker_pid=
container=

finish() {
    if [[ -n "$container" ]]; then
        docker stop --time 25 "$container" >/dev/null 2>&1 || true
        container=
    elif [[ -n "$broker_pid" ]]; then
        kill -TERM "$broker_pid" 2>/dev/null || true
        wait "$broker_pid" || true
    fi
    broker_pid=
}

note() {
    printf '%s %s\n' "$(date -u +%FT%TZ)" "$*"
}

if [[ "$role" == work ]]; then
    node=${3:?worker node is required}
    for index in "${!stages[@]}"; do
        note "waiting ${stages[$index]}"
        python3 "$program" work --node "$node" --host "$host" --port "$port" \
            --control-port "$((control_port + index))" --connect-timeout-seconds 300
        note "completed ${stages[$index]}"
    done
    exit
fi

[[ "$role" == control ]]
trap finish EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
sha256sum "$directory/SimdKafka.dll"
test -r "$base/official-kafka/libs/kafka-clients-4.4.0.jar"
docker image inspect "$image" --format 'JRE container {{.Id}}'
nproc
free -m
df -m "$directory"

launch() {
    local broker=$1 run=$2 identifier=$3 controller_port=$((port + 1))
    mkdir -- "$run" "$run/data"
    if [[ "$broker" == simdkafka ]]; then
        dotnet "$directory/SimdKafka.dll" --listen "$host:$port" --no-auto-create \
            --data "$run/data" >"$run/broker.log" 2>&1 &
        broker_pid=$!
    else
        local config="$run/server.properties"
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
log.flush.interval.messages=1
log.segment.delete.delay.ms=1000
default.replication.factor=1
min.insync.replicas=1
auto.create.topics.enable=false
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
EOF
        local -a docker_args=(--network host --user 0:0 -e "LOG_DIR=$run" -e 'KAFKA_HEAP_OPTS=-Xms1g -Xmx1g'
            -v "$base/official-kafka:/opt/kafka44:ro" -v "$run:$run")
        local id
        id=$(docker run --rm --network none -v "$base/official-kafka:/opt/kafka44:ro" \
            --entrypoint /opt/kafka44/bin/kafka-storage.sh "$image" random-uuid)
        docker run --rm "${docker_args[@]}" --entrypoint /opt/kafka44/bin/kafka-storage.sh "$image" \
            format --standalone -t "$id" -c "$config" >"$run/format.log" 2>&1
        container="simdkafka-cmp-$identifier"
        docker run --rm --detach --name "$container" "${docker_args[@]}" \
            --entrypoint /opt/kafka44/bin/kafka-server-start.sh "$image" "$config" >"$run/container.id"
        broker_pid=$(docker inspect --format '{{.State.Pid}}' "$container")
    fi
    python3 - "$host" "$port" "$broker_pid" <<'PY'
import os
import socket
import sys
import time

host, port, pid = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
deadline = time.monotonic() + 60
while time.monotonic() < deadline:
    os.kill(pid, 0)
    try:
        with socket.create_connection((host, port), timeout=.25):
            break
    except OSError:
        time.sleep(.25)
else:
    raise SystemExit("Broker listener did not become ready")
PY
    sleep 3
}

for index in "${!stages[@]}"; do
    label=${stages[$index]%:*}
    broker=${stages[$index]#*:}
    directory_name="$label-$broker"
    run="$directory/$directory_name"
    name=SimdKafka
    [[ "$broker" == simdkafka ]] || name=Kafka-4.4.0
    options=(--rounds 3 --mixed)
    if [[ "$label" == 10m || "$label" == long ]]; then
        seconds=$duration_10m
        [[ "$label" != long ]] || seconds=$duration_long
        options=(--duration-seconds "$seconds" --storage-root "$run/data" --reserve-disk-mib 4096)
    fi
    note "starting $directory_name"
    launch "$broker" "$run" "$(basename "$directory")-$index"
    set +e
    python3 "$program" control --host "$host" --port "$port" --control-port "$((control_port + index))" \
        --pid "$broker_pid" --broker "$name" --workers 8 --partitions 4 --batches 128 --records 64 \
        --message-bytes 1024 --clients "${clients[@]}" "${options[@]}" >"$run/results.jsonl" 2>"$run/error.log"
    result=$?
    set -e
    if [[ -n "$container" ]]; then docker logs --tail 300 "$container" >"$run/broker.log" 2>&1 || true; fi
    finish
    if (( result != 0 )); then
        note "failed $directory_name"
        cat "$run/error.log" >&2
        python3 "$directory/summarize_broker_runs.py" "$directory" --output "$directory/comparison.md"
        exit "$result"
    fi
    date -u +%FT%TZ >"$run/completed"
    python3 "$directory/summarize_broker_runs.py" "$directory" --output "$directory/comparison.md"
    note "completed $directory_name"
done
note "suite complete"
