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


## microdb vs Redis (same box, same client: redis-benchmark)

Redis 7.4-stable built from source (MALLOC=libc), --save ''; microdb as
committed. 50 connections, 50k ops per test, loopback. redis-benchmark
drives BOTH servers, so the client is identical.

| test | Redis 7.4 | microdb | ratio |
|---|---|---|---|
| SET, unpipelined | 71,124/s | 58,548/s | 0.82x |
| GET, unpipelined | 60,827/s | 46,904/s | 0.77x |
| SET, pipeline 16 | 735,294/s | 909,091/s | 1.24x |
| GET, pipeline 16 | 595,238/s | 909,091/s | 1.53x |

Honest read: unpipelined, microdb holds ~80% of Redis -- respectable for
~700 lines against a two-decade-old C server. Under pipelining it pulls
AHEAD: with 16 commands per round trip, per-command overhead dominates and
microdb's coroutine-per-connection + io_uring path has fewer layers between
the network and the hash table than Redis's readiness loop. Caveats: string
workload only (no zsets/Lua/TTL-sweeper threads/etc.), no persistence on
either side, and Redis carries features this demo deliberately lacks.
Reproduce:

    make run-microdb &          # :6380
    redis-benchmark -p 6380 -t set,get -n 50000 -c 50 -P 16 --csv
