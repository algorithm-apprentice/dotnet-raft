#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: smoke-durable-kv-cluster.sh <repository-root>" >&2
  exit 2
fi

repo="$(cd "$1" && pwd -P)"
dll="$repo/examples/DotnetRaft.KvCluster/bin/Release/net10.0/DotnetRaft.KvCluster.dll"

require_command() {
  local command="$1"
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "durable KV cluster smoke requires $command" >&2
    exit 1
  fi
}

for command in curl jq python3; do
  require_command "$command"
done

if [[ ! -f "$dll" ]]; then
  echo "missing Release host at $dll" >&2
  echo "run: dotnet build examples/DotnetRaft.KvCluster -c Release" >&2
  exit 1
fi

python3 - <<'PY'
import socket
import sys

sockets = []
try:
    for port in range(17101, 17104):
        current = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        current.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        current.bind(("127.0.0.1", port))
        current.listen(1)
        sockets.append(current)
    for port in range(17201, 17204):
        current = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        current.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        current.bind(("127.0.0.1", port))
        current.listen(1)
        sockets.append(current)
except OSError as error:
    print(f"durable KV cluster smoke requires unused loopback ports: {error}", file=sys.stderr)
    sys.exit(1)
finally:
    for current in sockets:
        current.close()
PY

temp="$(mktemp -d)"
pids=()

cleanup() {
  local status="$?"
  for id in "${!pids[@]}"; do
    pid="${pids[$id]}"
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
      for _ in $(seq 1 50); do
        if ! kill -0 "$pid" >/dev/null 2>&1; then
          break
        fi
        sleep 0.1
      done
      if kill -0 "$pid" >/dev/null 2>&1; then
        kill -KILL "$pid" >/dev/null 2>&1 || true
      fi
      wait "$pid" >/dev/null 2>&1 || true
    fi
  done
  if [[ "$status" -ne 0 ]]; then
    for log in "$temp"/node-*.log; do
      if [[ -f "$log" ]]; then
        echo "===== $(basename "$log") =====" >&2
        cat "$log" >&2
      fi
    done
  fi
  rm -rf "$temp"
}
trap cleanup EXIT

http_port() {
  echo $((17100 + $1))
}

grpc_port() {
  echo $((17200 + $1))
}

start_node() {
  local id="$1"
  local http
  local grpc
  http="$(http_port "$id")"
  grpc="$(grpc_port "$id")"
  mkdir -p "$temp/node-$id"
  dotnet "$dll" \
    "--Raft:NodeId=$id" \
    "--Raft:HttpPort=$http" \
    "--Raft:GrpcPort=$grpc" \
    "--Raft:DataDirectory=$temp/node-$id" \
    "--Raft:TickIntervalMilliseconds=50" \
    "--Raft:RequestTimeoutSeconds=5" \
    "--Raft:TransportTimeoutMilliseconds=250" \
    "--Raft:SnapshotThresholdEntries=4" \
    "--Raft:Peers:1=http://127.0.0.1:17201" \
    "--Raft:Peers:2=http://127.0.0.1:17202" \
    "--Raft:Peers:3=http://127.0.0.1:17203" \
    >"$temp/node-$id.log" 2>&1 &
  pids["$id"]=$!
}

stop_node() {
  local id="$1"
  local pid="${pids[$id]}"
  kill "$pid"
  for _ in $(seq 1 50); do
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      break
    fi
    sleep 0.1
  done
  if kill -0 "$pid" >/dev/null 2>&1; then
    kill -KILL "$pid"
  fi
  wait "$pid" >/dev/null 2>&1 || true
  unset 'pids[$id]'
}

crash_node() {
  local id="$1"
  local pid="${pids[$id]}"
  kill -KILL "$pid"
  wait "$pid" >/dev/null 2>&1 || true
  unset 'pids[$id]'
}

curl_bounded() {
  curl \
    --connect-timeout 1 \
    --max-time 10 \
    -fsS \
    "$@"
}

curl_retrying_mutation() {
  local operation="$1"
  shift
  local response="$temp/curl-response.txt"
  local attempt
  local curl_exit
  local status
  for attempt in $(seq 1 3); do
    : >"$response"
    if status="$(
      curl \
        --connect-timeout 1 \
        --max-time 10 \
        -sS \
        -o "$response" \
        -w '%{http_code}' \
        "$@"
    )"; then
      curl_exit=0
    else
      curl_exit="$?"
    fi

    if [[ "$curl_exit" -eq 0 && "$status" == 2?? ]]; then
      return
    fi

    if [[ "$attempt" -lt 3 ]] \
      && {
        [[ "$curl_exit" -ne 0 ]] \
          || [[ "$status" =~ ^(408|429|500|502|503|504)$ ]];
      }; then
      sleep 0.5
      continue
    fi

    echo "$operation failed after attempt $attempt (curl=$curl_exit, HTTP=$status)" >&2
    if [[ -s "$response" ]]; then
      cat "$response" >&2
      echo >&2
    fi
    return 1
  done
}

wait_http() {
  local id="$1"
  local url="http://127.0.0.1:$(http_port "$id")/"
  for _ in $(seq 1 100); do
    if curl_bounded "$url" >/dev/null 2>&1; then
      return
    fi
    sleep 0.1
  done
  cat "$temp/node-$id.log" >&2
  echo "node $id did not start" >&2
  exit 1
}

wait_leader_view() {
  local id="$1"
  local expected="$2"
  local url="http://127.0.0.1:$(http_port "$id")/status"
  for _ in $(seq 1 100); do
    if [[ "$(curl_bounded "$url" 2>/dev/null | jq -r '.leaderId // 0')" == "$expected" ]]; then
      return
    fi
    sleep 0.1
  done
  echo "node $id did not observe leader $expected" >&2
  exit 1
}

wait_any_leader() {
  for _ in $(seq 1 100); do
    for id in "$@"; do
      local url="http://127.0.0.1:$(http_port "$id")/status"
      if [[ "$(curl_bounded "$url" 2>/dev/null | jq -r '.leaderId // 0')" == "$id" ]]; then
        echo "$id"
        return
      fi
    done
    sleep 0.1
  done
  echo "no candidate became leader" >&2
  exit 1
}

put_value() {
  local id="$1"
  local key="$2"
  local value="$3"
  local request_id="$4"
  curl_retrying_mutation \
    "PUT $key through node $id" \
    -X PUT \
    "http://127.0.0.1:$(http_port "$id")/kv/$key" \
    -H 'content-type: application/json' \
    -d "{\"value\":\"$value\",\"requestId\":\"$request_id\"}"
}

put_file() {
  local id="$1"
  local key="$2"
  local file="$3"
  curl_retrying_mutation \
    "PUT $key through node $id" \
    -X PUT \
    "http://127.0.0.1:$(http_port "$id")/kv/$key" \
    -H 'content-type: application/json' \
    --data-binary "@$file"
}

wait_value() {
  local id="$1"
  local key="$2"
  local expected="$3"
  local url="http://127.0.0.1:$(http_port "$id")/kv/$key"
  for _ in $(seq 1 100); do
    if [[ "$(curl_bounded "$url" 2>/dev/null | jq -r '.value // empty')" == "$expected" ]]; then
      return
    fi
    sleep 0.1
  done
  echo "node $id did not return $key=$expected" >&2
  exit 1
}

wait_value_length() {
  local id="$1"
  local key="$2"
  local expected="$3"
  local url="http://127.0.0.1:$(http_port "$id")/kv/$key"
  for _ in $(seq 1 100); do
    if [[ "$(curl_bounded "$url" 2>/dev/null | jq -r '.value | length')" == "$expected" ]]; then
      return
    fi
    sleep 0.1
  done
  echo "node $id did not return $key length $expected" >&2
  exit 1
}

for id in 1 2 3; do
  start_node "$id"
done
for id in 1 2 3; do
  wait_http "$id"
done

curl_bounded -X POST "http://127.0.0.1:17101/campaign" >/dev/null
leader="$(wait_any_leader 1 2 3)"
for id in 1 2 3; do
  wait_leader_view "$id" "$leader"
done
writer=1
if [[ "$writer" == "$leader" ]]; then
  writer=2
fi
put_value "$writer" color blue 11111111-2222-3333-4444-555555555555
wait_value 3 color blue

lagger=1
if [[ "$lagger" == "$leader" ]]; then
  lagger=2
fi
large_writer=1
for id in 1 2 3; do
  if [[ "$id" != "$leader" && "$id" != "$lagger" ]]; then
    large_writer="$id"
  fi
done
crash_node "$lagger"
python3 - "$temp/large.json" <<'PY'
import json
import sys

with open(sys.argv[1], "w", encoding="utf-8") as output:
    json.dump(
        {
            "value": "x" * (31 * 1024 * 1024),
            "requestId": "33333333-4444-5555-6666-777777777777",
        },
        output,
        separators=(",", ":"),
    )
PY
put_file "$large_writer" large "$temp/large.json"
put_value "$large_writer" checkpoint-a a 44444444-5555-6666-7777-888888888888
put_value "$large_writer" checkpoint-b b 55555555-6666-7777-8888-999999999999
put_value "$large_writer" checkpoint-c c 66666666-7777-8888-9999-aaaaaaaaaaaa
start_node "$lagger"
wait_http "$lagger"
wait_value_length "$lagger" large 32505856

crash_node "$leader"
survivors=()
for id in 1 2 3; do
  if [[ "$id" != "$leader" ]]; then
    survivors+=("$id")
  fi
done
curl_bounded -X POST "http://127.0.0.1:$(http_port "${survivors[0]}")/campaign" >/dev/null
new_leader="$(wait_any_leader "${survivors[@]}")"
for id in "${survivors[@]}"; do
  wait_leader_view "$id" "$new_leader"
done
surviving_writer="${survivors[0]}"
if [[ "$surviving_writer" == "$new_leader" ]]; then
  surviving_writer="${survivors[1]}"
fi
put_value "$surviving_writer" shape circle aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
wait_value "$new_leader" shape circle

start_node "$leader"
wait_http "$leader"
put_value "$surviving_writer" resumed yes 22222222-3333-4444-5555-666666666666
wait_value "$leader" color blue
wait_value "$leader" shape circle
wait_value "$leader" resumed yes

for id in 1 2 3; do
  stop_node "$id"
done
for id in 1 2 3; do
  start_node "$id"
done
for id in 1 2 3; do
  wait_http "$id"
done

curl_bounded -X POST "http://127.0.0.1:17103/campaign" >/dev/null
leader="$(wait_any_leader 1 2 3)"
for id in 1 2 3; do
  wait_leader_view "$id" "$leader"
done
wait_value 2 color blue
wait_value 2 shape circle

echo "durable KV cluster smoke passed"
