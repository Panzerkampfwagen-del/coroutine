# coro — a stackful coroutine runtime in C17

A cooperative single-threaded coroutine runtime built from the registers up:
a hand-written x86-64 context switch, mmap'd stacks with `PROT_NONE` guard
pages, buffered channels with deterministic FIFO delivery, a direct-handoff
FIFO mutex, an **io_uring I/O layer** (submit-and-park: `coro_read/write/
accept/connect/sleep`), two scheduling **fuzzers**, and a dual-backend
coroutine-per-connection TCP echo server.

No dependencies beyond a C17 compiler and GNU `as`. The same suite passes on
three interchangeable scheduling backends: the hand-written assembly switch,
an ASan+UBSan instrumented build, and a portable `ucontext` fallback that
documents the semantics in C.

## Why this is interesting

Every production async runtime hides the same three tricks. This project
implements them where you can read them:

1. **A context switch is six pops.** Only callee-saved registers (`rbx`,
   `rbp`, `r12–r15`) live beyond a call boundary under the SysV ABI — so
   saving `%rsp` into the target and popping six quadwords *is* the switch.
   Caller-saved registers are dead by ABI contract anyway.
2. **A fresh coroutine is a forged stack frame.** Seven seeded quadwords make
   a new coroutine "return" into its trampoline with the ABI-correct
   `%rsp ≡ 8 (mod 16)` alignment, `%r12` carrying the coroutine pointer.
3. **`yield` and `park` are different primitives.** Yield requeues the
   caller; park suspends without requeueing because the waker adds exactly
   one run-queue entry. Confusing them produces double-resume
   use-after-free — the bug class ASan exists to catch.

## Layout

```
include/coro.h        public API (scheduler, channels, mutex)
include/io.h          io_uring-backed I/O API (optional; auto-detected)
src/switch.S          coro_swap: callee-saved-register context switch (x86-64)
src/coro.c            scheduler, stacks (mmap + PROT_NONE guard page),
                      park/wake protocol, reap-list lifecycle, weak I/O hooks
src/io.c              shared-ring submit-and-park I/O (liburing)
src/channel.c         ring buffer + sender/receiver wait queues
src/mutex.c           FIFO direct-handoff mutex (no futex, no atomics)
examples/echo_server.c  concurrent TCP echo server, io_uring OR nonblock
tests/test_all.c      assert-based runner: 8 tests across 3 backends
tests/fuzz/           sched_fuzz + channel_fuzz (model-based)
benchmarks/           primitive micro-bench + TCP echo load generator
```

## Build & run

```sh
make test            # suite on the hand-written asm switch (8 tests)
make sanitize        # same suite under ASan + UBSan
make ucontext-test   # portable swapcontext backend instead of asm
make fuzz            # scheduler fuzzer (20k random workloads) +
                     # model-based channel fuzzer (20k inputs)
make bench           # primitive micro-benchmarks (switch/chan/scale)
make run-bench       # TCP echo load test, 1000 conns (see benchmarks/)
make run-echo        # echo server on :8080; try: nc 127.0.0.1 8080
                     #   --no-uring forces the portable fallback backend
make clean
```

Requirements: gcc ≥ 12 with GNU as, Linux. Nothing else.

## Public API

```c
int      coro_init(void);              /* once, before coro_create     */
coro_t  *coro_create(coro_fn fn, void *arg);
void     coro_run(void);               /* until every coroutine exits  */
void     coro_yield(void);             /* suspend current, REQUEUE it  */
void     coro_park(void);              /* suspend WITHOUT requeueing   */
void     runq_wake(coro_t *c);         /* wake a parked coroutine once */
coro_t  *coro_current(void);
int      coro_alive_count(void);

chan_t  *chan_new(size_t capacity);    /* capacity >= 1                */
int      chan_send(chan_t *ch, void *v);   /* parks when full           */
int      chan_recv(chan_t *ch, void **v);  /* parks when empty          */
void     chan_free(chan_t *ch);

coro_mutex_t *mutex_new(void);
int    mutex_lock(coro_mutex_t *m);    /* FIFO, direct handoff         */
void   mutex_unlock(coro_mutex_t *m);  /* wakes head waiter only       */
```

## Semantics worth reading the code for

- **Channel FIFO invariant.** Receivers park only when the ring is empty
  *and* no sender is parked; senders park only when the ring is full and no
  receiver waits. Because both wait queues are never simultaneously
  non-empty, draining the buffer before rendezvousing with parked senders
  makes delivery order provably match send order.
- **Waiter nodes live on the waiter's own stack.** A parked coroutine's
  stack stays alive while suspended, so its channel/mutex node needs no
  heap allocation — and can never leak.
- **Mutex handoff without barging.** `unlock` transfers ownership to the
  head waiter *before* waking it; an unlocking coroutine that immediately
  relocks queues behind everyone it just unblocked.
- **Guard pages.** Each stack is an mmap region with its lowest page
  `mprotect(PROT_NONE)`d: overflow faults immediately instead of silently
  corrupting the neighbouring allocation.
- **Reap-list lifecycle.** A dying coroutine must not free itself before
  switching away — it would write its saved context into freed memory.
  Exit retires onto a reap list; `coro_run()` frees after every switch back.

## Test suite

Exit code 0 iff every assertion passes; the identical binary passes on all
three backends (asm, ASan+UBSan, ucontext).

| # | Test | Assertions | Proves |
|---|------|-----------|--------|
| 1 | pingpong | strict event order | yield switches cor-to-cor deterministically |
| 2 | many_coros | counter lands; alive == 0 | fairness across 100 coroutines; reaping |
| 3 | chan_fifo | producer→consumer exact at cap 4 | buffering + parking under contention |
| 4 | chan_block | values 1,2,3 in order; sender resumed | park/wake-once; buffer-first priority |
| 5 | mutex | zero CS violations with yields inside | mutual exclusion + direct handoff |
| 6 | stress | 50 × 10k yields, all reaped | stability at scale, no leaks |
| 7 | io_sleep | short sleep finishes first despite starting second | coro_run services parked timers after the run queue drains |
| 8 | io_socketpair | 3 payloads byte-intact through uring read/write | submit→park→completion→resume round-trip |

Tests 7–8 skip themselves gracefully where io_uring is unavailable (e.g. CI
sandboxes with `kernel.io_uring_disabled`).

## Echo server

Single thread, one coroutine per connection, two interchangeable I/O
backends chosen at startup:

- **io_uring** (default where liburing exists): `accept/read/write` submit to
  a shared ring and park the caller; the scheduler harvests completions when
  the run queue empties. Sockets stay blocking — the kernel waits instead.
- **nonblock** (`--no-uring` or no liburing): the original `EAGAIN` →
  retry-and-yield pattern.

Measured head-to-head (1000 conns): io_uring ramps connections ~1.5× faster;
the nonblock backend posts higher raw req/s and lower p50 on hot loopback,
because an `EAGAIN` retry is one userspace swap while uring is a kernel
round-trip per op. Full numbers and reproduction commands:
[benchmarks/RESULTS.md](benchmarks/RESULTS.md).

```sh
make run-echo &
printf 'hello\n' | nc 127.0.0.1 8080     # -> hello
```

## microdb -- a Redis-compatible store on the runtime

`apps/microdb/` is a single-file Redis-compatible string store (~700 lines
plus the RESP parser) served coroutine-per-connection over the io_uring
layer: chained hash table with incremental rehash, TTL expiry (EX/PX/EXAT/
PXAT, EXPIRE/TTL/PERSIST), INCR/DECR, APPEND/MGET/MSET, KEYS *, DBSIZE --
enough that `redis-cli`, `redis-benchmark`, and real clients work against
it. The full-scale sibling adds replication and a sharding proxy; this port
deliberately keeps the core.

Measured against real Redis 7.4 with the same benchmark client: ~80% of
Redis unpipelined, and 1.2-1.5x FASTER under pipeline 16 (per-command
overhead dominates once syscalls are batched). Numbers and caveats:
[benchmarks/RESULTS.md](benchmarks/RESULTS.md).

```sh
make run-microdb                        # :6380
redis-cli -p 6380 set hello world
redis-cli -p 6380 get hello             # -> "world"
```

## Fuzzers

Two randomized checkers complement the unit suite:

- **sched_fuzz** builds random producer/consumer workloads (random capacity,
  producer/consumer counts, item counts, yield/mutex interleavings) and
  verifies three invariants per iteration: every item consumed exactly once,
  every coroutine reaches its end (lost wake-ups surface as uncompleted
  coroutines, not hangs), and mutex-protected counters match attempted
  increments exactly.
- **channel_fuzz** replays pseudo-random send/receive sequences against a
  reference FIFO, catching any ordering or buffering bug; compiles as a
  libFuzzer target with `-DFUZZ_LIBFUZZER`.

```sh
make fuzz    # 20000 iterations each, seeded and reproducible
```

## Limitations

- Cooperative only: a coroutine that never yields starves the rest; there
  is no preemption and no multi-core work stealing.
- Linux x86-64 (asm backend) or any arch with the ucontext backend; not
  signal-safe, not thread-safe — the point is one thread done well.
- Channels are pointer-sized slots; no select/timeout composition yet.
