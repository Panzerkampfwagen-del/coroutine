#!/usr/bin/env bash
#
# Build and run both fuzz targets for a number of iterations:
#   channel_fuzz - model-checks channel FIFO ordering against a reference queue
#   sched_fuzz   - model-checks scheduler wake-ups (no lost item / wake / update)
#
# Usage: tests/fuzz/run.sh [iterations] [seed]
#   defaults: 20000 iterations, fixed seed
set -euo pipefail

cd "$(dirname "$0")/../.."

ITERS="${1:-20000}"
SEED="${2:-}"

make libcoro.a >/dev/null

CC="${CC:-cc}"
FLAGS="-O2 -g -Wall -Wextra -std=c17 -Iinclude"

$CC $FLAGS tests/fuzz/channel_fuzz.c -o /tmp/channel_fuzz -L. -lcoro
$CC $FLAGS tests/fuzz/sched_fuzz.c   -o /tmp/sched_fuzz   -L. -lcoro

echo "Fuzzing channels:  ${ITERS} iterations"
/tmp/channel_fuzz "${ITERS}" ${SEED}
echo "Fuzzing scheduler: ${ITERS} iterations"
/tmp/sched_fuzz "${ITERS}" ${SEED}
