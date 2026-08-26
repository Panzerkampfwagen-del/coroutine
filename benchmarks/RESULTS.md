# Measured results

Single box, Linux x86-64, gcc 15.2, default `-O2`. Numbers are reproducible
via the commands shown; treat them as indicative of this machine only.

## Primitive micro-benchmarks (`make bench` → `build/bench_prim`)

| primitive | ops | ops/s |
|---|---|---|
| switch (2-coroutine ping-pong, full yield round-trip) | 4,000,000 | ~91M |
| chan_rendezvous (cap-1 ring, both sides park every item) | 500,000 | ~60M |
| scale_1000coros (1000 coros × 1000 yields round-robin) | 1,000,000 | ~60M |

A context switch — pop/push on the run queue plus the six-pop register swap —
costs ~11 ns. A channel rendezvous with mandatory parking costs ~17 ns.

## TCP echo load test, 1000 concurrent connections, 4–5 s windows
(`benchmarks/bench.py`; uring via `make run-bench`, nonblock via
`build/echo_nb.sh` wrapper passing `--no-uring`)

| metric | io_uring | nonblock+yield |
|---|---|---|
| connections/s | **12,102** | 8,151 |
| requests/s (64 B echo) | 51,430 | **72,575** |
| unloaded p50 RTT | 64.1 µs | **36.4 µs** |
| unloaded p99 RTT | 215.1 µs | **284.4 µs** |
| echo mismatches | 0 | 0 |

Honest read: for tiny messages over hot loopback, `EAGAIN`-retry costs one
userspace swap per event while io_uring pays a kernel submission/completion
round-trip per op — so the classic backend wins raw req/s and p50 here. The
io_uring backend wins connection ramp-up (~1.5×) and its advantage grows
where syscalls would block for real (slower peers, larger reads, accept
storms); it also removes the busy retry-yield pattern entirely. Choose per
workload; both backends share every other layer of the runtime.
