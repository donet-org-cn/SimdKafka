#!/bin/bash
set -euo pipefail

fail() {
    printf 'SimdKafka: %s\n' "$*" >&2
    exit 1
}

broker=/opt/simdkafka/SimdKafka.dll
for arg in "$@"; do
    if [[ $arg == --help ]]; then
        exec dotnet "$broker" "$@"
    fi
done

for key in CLUSTER_ID KAFKA_PROCESS_ROLES KAFKA_CONTROLLER_QUORUM_VOTERS \
    KAFKA_CONTROLLER_QUORUM_BOOTSTRAP_SERVERS KAFKA_CONTROLLER_LISTENER_NAMES \
    KAFKA_INTER_BROKER_LISTENER_NAME; do
    [[ -z ${!key-} ]] || fail "$key is a KRaft setting. Use SIMDKAFKA_CLUSTER_CONFIG for SimdKafka clusters."
done

# Capture the listener name, host and decimal port from one Kafka-style endpoint.
parse_listener() {
    local key=$1 value=$2
    local pattern='^([A-Za-z][A-Za-z0-9_-]*)://(\[[0-9A-Fa-f:.]+\]|[A-Za-z0-9_.-]*):([0-9]{1,5})$'
    [[ $value =~ $pattern ]] || fail "$key must specify one NAME://HOST:PORT listener"
    endpoint_name=${BASH_REMATCH[1]^^}
    endpoint_host=${BASH_REMATCH[2]}
    endpoint_port=$((10#${BASH_REMATCH[3]}))
    (( endpoint_port >= 1 && endpoint_port <= 65535 )) || fail "$key port must be between 1 and 65535"
}

listen_host=0.0.0.0
listen_port=9092
listener_name=
if [[ -n ${KAFKA_LISTENERS-} ]]; then
    parse_listener KAFKA_LISTENERS "$KAFKA_LISTENERS"
    listener_name=$endpoint_name
    listen_host=${endpoint_host:-0.0.0.0}
    if [[ $listen_host == localhost ]]; then
        listen_host=127.0.0.1
    fi
    listen_port=$endpoint_port
fi

public_host=$listen_host
public_host=${public_host#\[}
public_host=${public_host%\]}
if [[ $public_host == 0.0.0.0 || $public_host == :: ]]; then
    public_host=localhost
fi
advertised=()
if [[ -n ${KAFKA_ADVERTISED_LISTENERS-} ]]; then
    parse_listener KAFKA_ADVERTISED_LISTENERS "$KAFKA_ADVERTISED_LISTENERS"
    if [[ -n $listener_name && $endpoint_name != "$listener_name" ]]; then
        fail "KAFKA_ADVERTISED_LISTENERS name must match KAFKA_LISTENERS"
    fi
    listener_name=$endpoint_name
    public_host=${endpoint_host#\[}
    public_host=${public_host%\]}
    [[ -n $public_host && $public_host != 0.0.0.0 && $public_host != :: ]] ||
        fail "KAFKA_ADVERTISED_LISTENERS needs a client-reachable host"
    advertised=(--advertise-port "$endpoint_port")
fi

data=${KAFKA_LOG_DIRS:-${KAFKA_LOG_DIR:-/var/lib/kafka/data}}
[[ $data != *,* ]] || fail "KAFKA_LOG_DIRS accepts one data directory"
options=(--listen "$listen_host:$listen_port" --advertise-host "$public_host"
    "${advertised[@]}" --data "$data" --partitions "${KAFKA_NUM_PARTITIONS:-1}")

auto_create=${KAFKA_AUTO_CREATE_TOPICS_ENABLE:-true}
case ${auto_create,,} in
    true) ;;
    false) options+=(--no-auto-create) ;;
    *) fail "KAFKA_AUTO_CREATE_TOPICS_ENABLE must be true or false" ;;
esac

append_option() {
    if [[ -n $2 ]]; then
        options+=("$1" "$2")
    fi
}

# Relative Kafka-style certificate filenames are mounted under /etc/kafka/secrets.
key_file=${SIMDKAFKA_TLS_PFX-}
if [[ -z $key_file && -n ${KAFKA_SSL_KEYSTORE_FILENAME-} ]]; then
    key_type=${KAFKA_SSL_KEYSTORE_TYPE:-PKCS12}
    [[ ${key_type^^} == PKCS12 ]] || fail "The TLS keystore must be PKCS12"
    key_file=$KAFKA_SSL_KEYSTORE_FILENAME
    [[ $key_file == /* ]] || key_file="/etc/kafka/secrets/$key_file"
fi
password_file=${SIMDKAFKA_TLS_PASSWORD_FILE-}
if [[ -z $password_file && -n ${KAFKA_SSL_KEYSTORE_CREDENTIALS-} ]]; then
    password_file=$KAFKA_SSL_KEYSTORE_CREDENTIALS
    [[ $password_file == /* ]] || password_file="/etc/kafka/secrets/$password_file"
fi
append_option --tls-pfx "$key_file"
append_option --tls-password-file "$password_file"
append_option --tls-client-ca "${SIMDKAFKA_TLS_CLIENT_CA-}"
append_option --sasl-plain-users "${SIMDKAFKA_SASL_PLAIN_USERS-}"
append_option --sasl-scram-users "${SIMDKAFKA_SASL_SCRAM_USERS-}"
append_option --sasl-oauth-config "${SIMDKAFKA_SASL_OAUTH_CONFIG-}"
append_option --cluster-config "${SIMDKAFKA_CLUSTER_CONFIG-}"

# CLI flags come last so the normal broker parser can apply native overrides.
# Track transport and mode after combining both sources.
combined=("${options[@]}" "$@")
tls=false
mtls=false
sasl=false
cluster=false
for (( i=0; i<${#combined[@]}; i++ )); do
    case ${combined[i]} in
        --cluster-config) cluster=true ;;
        --tls-pfx) tls=true ;;
        --tls-client-ca) mtls=true ;;
        --sasl-plain-users|--sasl-scram-users|--sasl-oauth-config) sasl=true ;;
        --sasl-dynamic-scram) sasl=true; continue ;;
        --no-auto-create|--unsafe-no-fsync) continue ;;
    esac
    i=$((i + 1))
done

if $cluster; then
    append_option --node-id "${KAFKA_NODE_ID-}"
    [[ -z ${KAFKA_DEFAULT_REPLICATION_FACTOR-} ]] ||
        fail "Set the cluster replica count when creating a topic with --replication-factor"
else
    [[ -z ${KAFKA_NODE_ID-} || $KAFKA_NODE_ID == 1 ]] || fail "Standalone mode advertises node ID 1"
    [[ ${KAFKA_DEFAULT_REPLICATION_FACTOR:-1} == 1 ]] || fail "Standalone mode supports replication factor 1"
fi

client_auth=${KAFKA_SSL_CLIENT_AUTH:-none}
case ${client_auth,,} in
    none) ;;
    required) $mtls || fail "KAFKA_SSL_CLIENT_AUTH=required needs SIMDKAFKA_TLS_CLIENT_CA or --tls-client-ca" ;;
    *) fail "KAFKA_SSL_CLIENT_AUTH supports none and required" ;;
esac

if [[ -n $listener_name ]]; then
    protocol=$listener_name
    IFS=, read -r -a mappings <<< "${KAFKA_LISTENER_SECURITY_PROTOCOL_MAP-}"
    for mapping in "${mappings[@]}"; do
        if [[ ${mapping%%:*} == "$listener_name" ]]; then
            protocol=${mapping#*:}
            break
        fi
    done
    actual_protocol=PLAINTEXT
    if $sasl; then
        actual_protocol=SASL_SSL
    elif $tls; then
        actual_protocol=SSL
    fi
    [[ $protocol == "$actual_protocol" ]] ||
        fail "Listener $listener_name requires $protocol, but broker options select $actual_protocol"
fi

exec dotnet "$broker" "${options[@]}" "$@"
