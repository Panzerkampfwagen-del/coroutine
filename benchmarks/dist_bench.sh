#!/usr/bin/env bash
#
# Benchmark the distributed microdb. Measures the cost of the proxy hop and
# sharded fan-out by comparing, under identical redis-benchmark load:
#
#   (a) a single standalone microdb node   (the baseline)
#   (b) the same workload through microdb-proxy in front of 2 shards, each a
#       primary + one replica  (writes -> primary, reads -> replica)
#
# Everything runs on loopback, so absolute numbers track machine load; the
# point is the (b)/(a) ratio - how much the routing/framing layer costs.
#
# Usage: benchmarks/dist_bench.sh [requests] [clients] [pipeline]
#   defaults: 300000 requests, 50 clients, pipeline depth 16
set -euo pipefail
cd "$(dirname "$0")/.."

N="${1:-300000}"
C="${2:-50}"
P="${3:-16}"

command -v redis-benchmark >/dev/null || {
    echo "redis-benchmark not found (apt install redis-tools)"; exit 1; }

make microdb microdb_proxy >/dev/null

# Ports: standalone baseline, two shards (primary,replica), and the proxy.
BASE=6410
S0P=6401; S0R=6402
S1P=6403; S1R=6404
PXY=6400

PIDS=()
cleanup() { for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done; }
trap cleanup EXIT

start() { "$@" >/dev/null 2>&1 & PIDS+=($!); }
ready() { for _ in $(seq 1 50); do
    [ "$(redis-cli -p "$1" ping 2>/dev/null)" = PONG ] && return 0; done; return 1; }

start ./microdb -p "$BASE"
start ./microdb -p "$S0P"
start ./microdb -p "$S0R" -r "127.0.0.1:$S0P"
start ./microdb -p "$S1P"
start ./microdb -p "$S1R" -r "127.0.0.1:$S1P"
for port in "$BASE" "$S0P" "$S0R" "$S1P" "$S1R"; do ready "$port" || {
    echo "node $port failed to start"; exit 1; }; done

start ./microdb_proxy -p "$PXY" \
    -s "127.0.0.1:$S0P,127.0.0.1:$S0R" \
    -s "127.0.0.1:$S1P,127.0.0.1:$S1R"
ready "$PXY" || { echo "proxy failed to start"; exit 1; }

run() {  # $1=port $2=label $3=extra-args
    printf '%-26s' "$2"
    redis-benchmark -p "$1" -t set,get,incr -n "$N" -c "$C" $3 -q 2>/dev/null \
        | tr '\r' '\n' | grep -E 'requests per second' \
        | sed -E 's/ requests per second.*//; s/:/ =/' | paste -sd '   ' -
}

echo "### Non-pipelined (-c $C, n=$N) ###"
run "$BASE" "direct single node" ""
run "$PXY"  "via proxy (2 shards)" ""

echo "### Pipelined (-P $P) ###"
run "$BASE" "direct single node" "-P $P"
run "$PXY"  "via proxy (2 shards)" "-P $P"
