# coro

**A high-performance cooperative coroutine runtime for Linux, with hand-written
assembly context switching (x86-64 and aarch64) and `io_uring` async I/O - and a
Redis-compatible store on top that keeps pace with Redis at ~1% of the code.**

[![CI](https://github.com/Panzerkampfwagen-del/coroutine/actions/workflows/ci.yml/badge.svg)](https://github.com/Panzerkampfwagen-del/coroutine/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
![Platform](https://img.shields.io/badge/platform-linux%20%7C%20x86--64%20%7C%20aarch64-lightgrey)

> A full design write-up is in [docs/design.md](docs/design.md).

---

## Quick start

```sh
git clone https://github.com/Panzerkampfwagen-del/coroutine.git coro && cd coro
sudo apt-get install -y liburing-dev   # Linux 5.1+
make                                   # libcoro.a, examples, echo_server, microdb
make check                             # build and run the test suite

./microdb -p 6380                      # a Redis-compatible store on the runtime
# in another shell (needs redis-tools):
redis-cli -p 6380 set hello world
redis-cli -p 6380 get hello

./echo_server -p 8080                  # or the simpler demo TCP echo server
echo "hello" | nc localhost 8080
```

---

## Motivation

Async I/O on Linux used to mean a readiness loop (`epoll`) stitched onto
callback spaghetti or a heavyweight thread per connection. `io_uring` changed
the model to true asynchronous submission/completion, and coroutines let you
write code that *looks* sequential while the runtime multiplexes thousands of
in-flight operations on a single thread. This project builds that stack from the
ground up: the register-level context switch, the scheduler, and the
`io_uring` event loop are all here, in a few hundred readable lines.

It exists mainly to **learn and to teach** - to show exactly how a coroutine
runtime suspends and resumes a call stack, how a guard page catches overflow,
and how completion events get routed back to the coroutine that was waiting.
Existing libraries hide all of this; here it is the point.

---

## Architecture

Single OS thread, cooperative scheduling - a coroutine runs until it yields,
blocks, or exits.

- **Context switch (`src/context.S`)** - `coro_context_switch(from, to)` saves
  the callee-saved registers into `from`, loads them from `to`, and returns.
  On x86-64 that is `rbx`, `rbp`, `r12`-`r15`, `rsp`; on aarch64 it is
  `x19`-`x28`, `fp`, `lr`, `sp`, and the callee-saved FP registers `d8`-`d15`.
  A fresh coroutine's stack is seeded with a trampoline (and, on x86-64,
  `coro_exit`), deliberately aligned so the user function and `coro_exit` enter
  with an ABI-correct stack. The architecture seam is three small `#if` blocks;
  everything else is portable C.
- **Stacks** - one `mmap` region per coroutine (1 MiB default), with a
  `PROT_NONE` guard page at the bottom so an overflow faults instead of
  corrupting the neighbour.
- **Round-robin scheduler (`src/coro.c`)** - a central loop switches into each
  ready coroutine; control returns to the loop on yield/block/exit. When the run
  queue drains, the loop waits on `io_uring` completions.
- **io_uring event loop (`src/io.c`)** - one shared ring (queue depth 64). A
  call (`coro_read`/`write`/`accept`/`connect`/`sleep`) submits an SQE tagged
  with the coroutine id, parks the coroutine, and yields; the scheduler harvests
  completions and wakes the owner by id. `coro_connect` lets a coroutine act as a
  client, which is what the distributed layer (below) is built on.
- **Channels (`src/channel.c`)** - a ring buffer plus sender/receiver wait
  queues; senders block when full, receivers block when empty.
- **Mutex (`src/sync.c`)** - a held flag plus a FIFO of waiters; `unlock` hands
  ownership directly to the longest waiter (no spinning, no futex).

```
   coro_run()  ── scheduler loop ──┐
        │  switch in               │ switch back on yield/block/exit
        ▼                          │
   ┌─────────┐  coro_read/…   ┌──────────┐  submit SQE   ┌───────────┐
   │coroutine│ ─────────────▶ │  io.c    │ ────────────▶ │ io_uring  │
   │ (stack) │ ◀───────────── │ park+id  │ ◀──────────── │   ring    │
   └─────────┘   wake by id   └──────────┘  reap CQE     └───────────┘
```

---

## Benchmarks

Loopback echo benchmark: persistent connections each send a 64-byte message and
await the echo. `benchmarks/bench.py` drives the load and writes
`benchmarks/results.json`.

| Metric | Result |
| --- | --- |
| Connection setup rate | **~12,600 conn/s** |
| Request rate (64 B echo) | **~28,900 req/s** |
| Round-trip latency, p50 / p99 | **84 µs / 238 µs** |
| Round-trip latency, p99.9 / max | 342 µs / 921 µs |
| Server-side echo latency (measured in-process) | **~8.5 µs** |
| Echo mismatches | 0 |

**Methodology.** Single machine, loopback (`127.0.0.1`). 1000 concurrent
connections, 64-byte messages, 5-second window; latency percentiles come from a
separate single-connection probe so they reflect the server, not load-generator
queuing. Kernel `6.6.114.1-microsoft-standard-WSL2`, 12 vCPUs, 7.6 GiB RAM,
Python 3.11 asyncio client.

> Caveat: the round-trip latency includes the Python asyncio client's per-call
> overhead; the server's own read-completion-to-write-completion latency,
> measured in-process, is about **8.5 µs**. Numbers are illustrative of a WSL2
> dev box - reproduce with `benchmarks/run.sh`.

Reproduce:

```sh
benchmarks/run.sh 1000 5      # 1000 connections, 5 seconds -> results.json
```

---

## Built on the runtime: microdb vs Redis

[`apps/microdb`](apps/microdb) is a Redis-compatible in-memory store - RESP wire
protocol, a hash-table keyspace with TTLs, ~20 commands, plus replication
(`PSYNC`/`REPLICAOF`/`WAIT`) - written entirely on this runtime (one coroutine
per connection, async `io_uring` I/O, graceful shutdown). The store is
**~1,200 lines** (the sharding/failover proxy below adds ~920 more, and both
share a ~260-line RESP/buffer header); Redis is ~150,000+ lines of C. Driven by
the same `redis-benchmark`, the single node keeps pace:

| `redis-benchmark -P 16` (pipelined) | microdb | Redis 7.0.15 |
| --- | --- | --- |
| SET | ~1.95M req/s | ~2.00M req/s |
| GET | ~2.03M req/s | ~2.31M req/s |
| INCR | ~2.10M req/s | ~2.27M req/s |

So within ~10% of Redis on pipelined GET/SET/INCR at roughly 1% of the code.
(Non-pipelined, microdb runs ~70% of Redis's rate; Redis's hand-tuned event loop
pulls ahead when each request is its own syscall.) Numbers vary on a shared WSL2
box - reproduce against a local `redis-server`:

```sh
benchmarks/microdb_bench.sh 300000 50 16     # -> benchmarks/microdb_results.txt
```

---

## Going distributed: sharding, replication, failover

A single store is one node. [`apps/microdb`](apps/microdb) also scales *out*: the
same runtime powers replication, a sharding proxy, and primary failover - and in
the process exercises the runtime as a **client** (dialing backends), not just a
server. That needed one new primitive, [`coro_connect`](include/io.h), the
outbound twin of `coro_accept`.

- **Replication.** Start a node with `-r host:port` (or `REPLICAOF` at runtime).
  It `PSYNC`s its primary, loads a snapshot, then applies the live write stream.
  Replicas are read-only and chain. `WAIT n timeout` blocks until `n` replicas
  have the write (the consistency/latency knob).
- **Sharding proxy** ([`microdb_proxy`](apps/microdb/microdb_proxy.c)). Speaks
  RESP, routes `key -> fnv1a(key) % nshards`, sends writes to the primary and
  reads to a replica, and fans `MGET`/`MSET`/`DEL` out across shards. It pipelines
  and coalesces writes to each backend, so a pipelined client keeps its
  throughput through the hop.
- **Failover.** The proxy health-checks each primary and promotes a replica
  (`REPLICAOF NO ONE`) on failure. This is coordinator-driven, *not* consensus -
  the split-brain and data-loss caveats are stated up front in
  [`docs/design.md` §11](docs/design.md).

```sh
# 2 shards, each a primary + one replica, behind the proxy
./microdb -p 6401 &                       # shard 0 primary
./microdb -p 6402 -r 127.0.0.1:6401 &     # shard 0 replica
./microdb -p 6403 &                       # shard 1 primary
./microdb -p 6404 -r 127.0.0.1:6403 &     # shard 1 replica
./microdb_proxy -p 7000 \
    -s 127.0.0.1:6401,127.0.0.1:6402 \
    -s 127.0.0.1:6403,127.0.0.1:6404 &
redis-cli -p 7000 set user:42 alice       # routed to a shard, replicated
redis-cli -p 7000 get user:42             # served from a replica

make dist-check                           # end-to-end: replication + sharding + failover
```

A proxy hop costs throughput (it does ~2x the socket I/O of a node on one
thread): routing through the 2-shard proxy retains ~60% of single-node rate
non-pipelined and ~45-50% pipelined (`benchmarks/dist_bench.sh`). That gap is the
classic argument for client-side sharding - which this hash rule would also allow.

---

## API

Full Doxygen-style documentation lives in the headers under `include/`.

**Spawn a coroutine and run the scheduler:**

```c
#include "coro.h"

static void worker(void *arg) {
    printf("hello from coroutine %ld\n", (long)arg);
    coro_yield();                 // cooperatively give up the CPU
}

int main(void) {
    for (long i = 0; i < 5; i++)
        coro_create(worker, (void *)i, 0);   // 0 = default 1 MiB stack
    coro_run();                                // returns when all finish
}
```

**Async I/O (looks blocking, scheduler stays busy):**

```c
#include "io.h"

char buf[4096];
int n = coro_read(fd, buf, sizeof buf);   // parks this coroutine, runs others
coro_write(fd, buf, n);
coro_sleep(100);                          // suspend ~100 ms, async
```

**Channels:**

```c
#include "channel.h"

channel_t *ch = channel_create(/*capacity=*/16, sizeof(int));
int v = 42;
channel_send(ch, &v);     // blocks if full
channel_recv(ch, &v);     // blocks if empty
channel_destroy(ch);
```

**Mutex:**

```c
#include "sync.h"

coro_mutex_t m;
coro_mutex_init(&m);
coro_mutex_lock(&m);
/* critical section */
coro_mutex_unlock(&m);
```

---

## Building & testing

```sh
make            # libcoro.a + examples + echo_server + microdb + microdb_proxy
make test       # the canonical examples (hello, pingpong)
make check      # full suite: examples + stress + channel & scheduler fuzzers
make clean

make debug      # -O0 -g3 with AddressSanitizer + UBSan
make release    # -O2 -march=native -DNDEBUG

make stress         # ./stress       - thousands of coroutines + channels + mutex
make channel_fuzz   # ./channel_fuzz - model-checked random channel ops
make sched_fuzz     # ./sched_fuzz   - model-checked scheduler wake-ups
tests/fuzz/run.sh 20000

make aarch64-check  # cross-compile + run the runtime on aarch64 under qemu-user
make dist-check     # end-to-end replication + sharding + failover (needs python3)

# Code quality
make format         # apply clang-format (LLVM, 4-space, 80 col)
make format-check   # CI gate: fail if unformatted
make tidy           # clang-tidy (bug-finding checks)
make cppcheck       # cppcheck (warning/performance/portability)
```

Custom stacks are made AddressSanitizer-safe via the fiber-switch annotations in
`coro_swap`, so `make debug` runs the whole suite cleanly under ASan + UBSan.
`make aarch64-check` additionally runs the scheduler, channels, mutex, and a
floating-point register test on aarch64 (needs `gcc-aarch64-linux-gnu` and
`qemu-user-static`).

Requirements: a C17 compiler, GNU `as`, Linux 5.1+, and `liburing`. Only the
`io_uring` consumers (`echo`, `echo_server`, `microdb`, `microdb_proxy`) link
`-luring`; `hello`/`pingpong` have no external dependency. `make dist-check`
needs `python3` (it speaks RESP over raw sockets - no `redis-cli` required).

---

## Limitations

- **Cooperative only.** A coroutine that never yields or blocks starves the
  others; there is no preemption.
- **Single-threaded.** No work stealing, no multi-core parallelism - the whole
  point is one thread multiplexing many coroutines.
- **Linux on x86-64 or aarch64.** The context switch is hand-written assembly
  for those two ISAs and the I/O layer is `io_uring`; no other architecture or
  OS is supported.
- **Not signal-safe / not thread-safe.** Intended for a single thread.
- **Basic I/O surface.** `read`/`write`/`accept`/`connect`/timer; no TLS, no full
  cancellation, no per-file offsets beyond `read(2)`/`write(2)` semantics.
  Per-operation deadlines/cancellation and an M:N threaded scheduler are the
  natural next steps (see [docs/design.md](docs/design.md#13-limitations-and-future-work)).
- **The distributed layer is demo-grade.** Async replication (stale replica
  reads), a full snapshot on every resync, fixed `hash % nshards` routing, and
  coordinator failover *without consensus* (split-brain/data-loss caveats). The
  trade-offs and the path to production are spelled out in
  [docs/design.md §11](docs/design.md#11-distribution-replication-sharding-and-failover).

---

## License & contributing

MIT licensed - see [LICENSE](LICENSE).

This is primarily a learning project, but issues and pull requests are welcome.
Before sending a PR, please run `make check`, `make format-check`, `make tidy`,
and `make cppcheck` (all of which CI enforces).
