#!/usr/bin/env bash
#
# Benchmark microdb with redis-benchmark, and compare against a real redis-server
# on :6379 if one is reachable. Builds microdb, starts it on :6380, runs the
# SET/GET/INCR/PING suite both pipelined and not, then shuts it down.
#
# Usage: benchmarks/microdb_bench.sh [requests] [clients] [pipeline]
#   defaults: 500000 requests, 50 clients, pipeline depth 16
set -euo pipefail
cd "$(dirname "$0")/.."

N="${1:-500000}"
C="${2:-50}"
P="${3:-16}"
PORT=6380

command -v redis-benchmark >/dev/null || { echo "redis-benchmark not found (apt install redis-tools)"; exit 1; }

make microdb >/dev/null
./microdb -p "$PORT" >/tmp/microdb_bench.log 2>&1 &
MDB=$!
trap 'kill $MDB 2>/dev/null || true' EXIT
for _ in $(seq 1 40); do [ "$(redis-cli -p "$PORT" ping 2>/dev/null)" = PONG ] && break; done

run() {  # $1=port $2=label $3=extra-args
    echo "== $2 =="
    # redis-benchmark redraws progress with carriage returns; split on them and
    # keep only the final per-test summary lines.
    redis-benchmark -p "$1" -t ping,set,get,incr -n "$N" -c "$C" $3 -q 2>/dev/null \
        | tr '\r' '\n' | grep -E 'requests per second'
}

echo "### Non-pipelined (-c $C) ###"
run "$PORT" "microdb" ""
redis-cli -p 6379 ping >/dev/null 2>&1 && run 6379 "redis" "" || true

echo "### Pipelined (-P $P) ###"
run "$PORT" "microdb" "-P $P"
redis-cli -p 6379 ping >/dev/null 2>&1 && run 6379 "redis" "-P $P" || true
