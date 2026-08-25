# coro — a stackful coroutine runtime in C17

A cooperative single-threaded coroutine runtime built from the registers up:
a hand-written x86-64 context switch, mmap'd stacks with `PROT_NONE` guard
pages, buffered channels with deterministic FIFO delivery, a direct-handoff
FIFO mutex, and a coroutine-per-connection TCP echo server on non-blocking
sockets.

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
src/switch.S          coro_swap: callee-saved-register context switch (x86-64)
src/coro.c            scheduler, stacks (mmap + PROT_NONE guard page),
                      park/wake protocol, reap-list lifecycle
src/channel.c         ring buffer + sender/receiver wait queues
src/mutex.c           FIFO direct-handoff mutex (no futex, no atomics)
examples/echo_server.c  single-threaded concurrent TCP echo server
tests/test_all.c      assert-based runner: 6 tests, 12 assertions
```

## Build & run

```sh
make test            # suite on the hand-written asm switch
make sanitize        # same suite under ASan + UBSan
make ucontext-test   # portable swapcontext backend instead of asm
make run-echo        # echo server on :8080; try: nc 127.0.0.1 8080
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

## Echo server

Single thread, one coroutine per connection. The listener accepts until
`EAGAIN`, then yields; handlers read/write with the same retry-and-yield
pattern, so thousands of sockets multiplex without threads or epoll loops.

```sh
make run-echo &
printf 'hello\n' | nc 127.0.0.1 8080     # -> hello
```

## Limitations

- Cooperative only: a coroutine that never yields starves the rest; there
  is no preemption and no multi-core work stealing.
- Linux x86-64 (asm backend) or any arch with the ucontext backend; not
  signal-safe, not thread-safe — the point is one thread done well.
- Channels are pointer-sized slots; no select/timeout composition yet.
