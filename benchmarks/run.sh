#!/usr/bin/env bash
#
# Build the echo server and benchmark it. bench.py starts the server itself
# (via --spawn), drives the load, then shuts it down. Results are printed and
# written to benchmarks/results.json.
#
# Usage: benchmarks/run.sh [connections] [duration_seconds] [port]
#   defaults: 1000 connections, 5 seconds, port 8080
set -euo pipefail

cd "$(dirname "$0")/.."

CONNS="${1:-1000}"
DURATION="${2:-5}"
PORT="${3:-8080}"

echo "Building echo_server..."
make echo_server >/dev/null

echo "Benchmarking: ${CONNS} connections for ${DURATION}s on port ${PORT}"
exec python3 benchmarks/bench.py \
    --spawn ./echo_server \
    --conns "${CONNS}" \
    --duration "${DURATION}" \
    --port "${PORT}"
