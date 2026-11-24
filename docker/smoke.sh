#!/usr/bin/env bash
set -euo pipefail

image=$1
client_image=apache/kafka:4.4.0
work=$(mktemp -d)
broker="simdkafka-smoke-${work##*.}"
volume="$broker-data"
topic=smoke-records
message=hello-simdkafka

cleanup() {
    local status=$?
    if (( status != 0 )); then
        docker logs --tail 50 "$broker" >&2 || true
    fi
    docker rm -f "$broker" >/dev/null 2>&1 || true
    docker volume rm "$volume" >/dev/null 2>&1 || true
    rm -f "$work/broker.log"
    rmdir "$work" || true
    exit "$status"
}
trap cleanup EXIT

start_broker() {
    docker run -d --pull never --name "$broker" \
        -e KAFKA_LISTENERS=PLAINTEXT://:9092 \
        -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092 \
        -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false \
        -v "$volume:/var/lib/kafka/data" "$image" >/dev/null
    for _ in {1..80}; do
        docker logs "$broker" >"$work/broker.log" 2>&1
        if grep -q 'SimdKafka listening' "$work/broker.log"; then
            return 0
        fi
        [[ $(docker inspect --format '{{.State.Running}}' "$broker") == true ]] || return 1
        sleep 0.25
    done
    printf 'Broker did not start\n' >&2
    return 1
}

kafka() {
    local command=$1
    shift
    timeout 60s docker run --rm -i --network "container:$broker" \
        --entrypoint "/opt/kafka/bin/$command" "$client_image" \
        --bootstrap-server 127.0.0.1:9092 "$@"
}

read_message() {
    kafka kafka-console-consumer.sh --topic "$topic" \
        --partition 0 --offset earliest --max-messages 1 --timeout-ms 20000 </dev/null
}

docker pull "$client_image"
docker volume create "$volume" >/dev/null
start_broker
[[ $(docker exec "$broker" id -u) == 1654 ]]

kafka kafka-topics.sh --create --topic "$topic" --partitions 1 --replication-factor 1 </dev/null
printf '%s\n' "$message" | kafka kafka-console-producer.sh --topic "$topic" --command-property acks=all
[[ $(read_message) == "$message" ]]

docker stop -t 30 "$broker" >/dev/null
[[ $(docker inspect --format '{{.State.ExitCode}}' "$broker") == 0 ]]
docker rm "$broker" >/dev/null
start_broker
[[ $(read_message) == "$message" ]]
printf 'Docker smoke passed for %s\n' "$image"
