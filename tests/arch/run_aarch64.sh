#!/usr/bin/env bash
#
# Cross-compile the core runtime for aarch64 and run the examples and stress
# test under qemu-user. This exercises the AArch64 context switch
# (src/context.S) on a non-host ISA. The io_uring layer is excluded because it
# needs an aarch64 build of liburing; everything else (scheduler, channels,
# mutex) is pure C plus the hand-written switch.
#
# Requires: gcc-aarch64-linux-gnu, qemu-user-static.
set -euo pipefail
cd "$(dirname "$0")/../.."

CC="${ARMCC:-aarch64-linux-gnu-gcc}"
QEMU="${QEMU:-qemu-aarch64-static}"
command -v "$CC"   >/dev/null || { echo "missing $CC (apt install gcc-aarch64-linux-gnu)"; exit 1; }
command -v "$QEMU" >/dev/null || { echo "missing $QEMU (apt install qemu-user-static)"; exit 1; }

CORE="src/context.S src/coro.c src/channel.c src/sync.c"
FLAGS="-static -O2 -g -Wall -Wextra -std=c17 -Iinclude"
out="$(mktemp -d)"
trap 'rm -rf "$out"' EXIT

run() {  # run <source.c> [args...]
    local src="$1"; shift
    "$CC" $FLAGS $CORE "$src" -o "$out/t"
    "$QEMU" "$out/t" "$@"
}

echo "target: $(file -b "$($CC $FLAGS $CORE examples/hello.c -o "$out/h"; echo "$out/h")" | cut -d, -f1-2)"
echo "== hello ==";    run examples/hello.c | tail -1
echo "== pingpong =="; run examples/pingpong.c
echo "== stress ==";   run tests/stress/stress.c 3
echo "aarch64: OK"
