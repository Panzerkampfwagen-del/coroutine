# coro, Design Document

A userspace cooperative coroutine runtime for Linux: hand-written x86-64
context switching, an `io_uring` submit-and-park I/O layer, channels, a
FIFO mutex, both scheduling fuzzers, and a Redis-compatible store running
on top. This document explains how it works, why it is built the way it
is, and what was deliberately left out.

- **Audience:** someone extending the runtime or evaluating the design.
- **Status:** stable. Linux x86-64 (asm) or any arch via `ucontext`.
- **Companion code:** [`src/`](../src), [`include/`](../include),
  [`apps/microdb/`](../apps/microdb), [`tests/fuzz/`](../tests/fuzz),
  [`benchmarks/`](../benchmarks) (measured numbers in
  [RESULTS.md](../benchmarks/RESULTS.md)).

---

## 1. Goals and non-goals

**Goals**

- Show, in a few hundred readable lines per layer, how a coroutine runtime
  suspends and resumes a native call stack.
- Make async I/O look synchronous while one thread multiplexes thousands of
  in-flight operations through `io_uring`, with a portable non-blocking
  fallback so every build runs everywhere.
- Be correct under scrutiny: ASan/UBSan builds, two randomized fuzzers,
  and the same test suite across three interchangeable backends.
- Be useful: microdb, a Redis-compatible store, runs on top at ~80% of
  Redis unpipelined and faster than Redis under pipelining (measured with the same
  client; see RESULTS.md).

**Non-goals**

- Multi-core parallelism (no M:N scheduling). One OS thread.
- Preemption. A coroutine runs until it yields, parks, or exits.
- Replication / sharding / persistence in microdb (see §9).
- aarch64 assembly (the `ucontext` backend covers other architectures).

The first two non-goals are what keep the synchronization story trivial:
with one thread and no preemption, the run queue, channels, and mutex need
no atomics, locks, or memory barriers at all.

---

## 2. System overview

```
              main thread, g_main context
   ┌───────────────────────────────────────────────────────┐
   │                     coro_run()                        │
   │   while (runq || io_pending):                         │
   │      pop coroutine ── coro_swap into it               │
   │      ...or, queue empty: harvest io_uring completions │
   └───────▲───────────────────────────────┬───────────────┘
           │ swap back on finish           │ swap into next
   ┌───────┴──────────┐            ┌───────┴────────────┐
   │  coroutine A     │  yield     │  coroutine B       │
   │  own mmap stack  │ ─────────▶ │ own mmap stack     │
   │  (requeued)      │  park      │ (parked: NO requeue│
   └──────────────────┘            │  waker requeues)   │
                                   └────────┬───────────┘
                                            │ coro_read/write/
                                            │ accept/sleep
                                   ┌────────▼───────────┐
                                   │  src/io.c          │
                                   │ submit SQE tagged  │
                                   │ with coro pointer, │
                                   │ park on pending    │
                                   │ list               │
                                   └────────▲───────────┘
                                            │ completion cqe
                                   ┌────────┴───────────┐
                                   │ coro_io_reap():    │
                                   │ match cqe → waiter,│
                                   │ store res, wake    │
                                   └────────────────────┘
```

Design choice worth noting up front: **resumption is main-mediated**. The
scheduler never switches coroutine-to-coroutine directly; a yielding or
parking coroutine always returns control to `coro_run()` (the main stack),
which pops the next runnable coroutine and swaps into it. A finishing
coroutine likewise swaps straight back to `g_main`. The cost is one extra
context hop per scheduling decision versus symmetric switching; the benefit
is that the scheduler state machine is trivially inspectable, there is
exactly one place (`switch_away_to_next`) where "who runs next?" is
answered, exactly one place where a dying coroutine is retired, and no
possibility of a coroutine resuming another coroutine whose stack frame it
holds pointers into by accident.

---

## 3. Context switching

### 3.1 What must be saved

`coro_swap(from, to)` is entered like an ordinary C function, so the
compiler has already spilled any live **caller-saved** registers around the
call. Under the System V AMD64 ABI only six registers live across calls:

```
rbx, rbp, r12, r13, r14, r15  , plus %rsp itself
```

SSE state is entirely caller-saved on x86-64, so nothing FP needs saving.

### 3.2 The switch: six pushes and a pointer copy

[`src/switch.S`](../src/switch.S) uses a *push-based* scheme rather than
writing registers into a context struct:

```asm
coro_swap:                  # rdi = from, rsi = to
    push %rbp %rbx %r12 %r13 %r14 %r15
    mov  %rsp, (%rdi)       # saving %rsp captures everything
    mov  (%rsi), %rsp       # adopt the target's saved frame
    pop  %r15 %r14 %r13 %r12 %rbx %rbp
    ret                     # resume where the target last left off
```

Each register is pushed onto the *outgoing* coroutine's own stack, so the
single word at `*from` is the whole saved context. Restoring is the exact
inverse followed by `ret`; the instruction pointer rides on the return
address at the top of the destination frame. Six pushes, two moves, seven
pops: a context switch is genuinely this small.

### 3.3 Starting a fresh coroutine: the forged frame

A brand-new coroutine has no "place it left off," so `coro_create`
synthesizes one. Seven quadwords are written just below the chosen stack
top:

```
slot[0..2]  → 0          (r15, r14, r13)
slot[3]     → coro_t*    (lands in r12; the trampoline reads it)
slot[4..5]  → 0          (rbx, rbp)
slot[6]     → trampoline_asm_entry   (the "return address")
```

After `mov (%rsi),%rsp; pop ×6; ret`, control arrives in
`trampoline_asm_entry` with `%r12` holding the coroutine pointer. The
SysV ABI requires `%rsp ≡ 8 (mod 16)` at function entry (just after a
`call` pushed its 8-byte return address); the create path nudges the slot
base down until `slot[6]` is 16-byte aligned, which makes entry alignment
correct by construction.

The trampoline runs the user function, then retires the coroutine and swaps
directly back to `g_main`, it never touches the run queue.

### 3.4 The portable backend

`make ucontext-test` rebuilds everything against `swapcontext`/
`makecontext`. It exists as executable documentation: the same suite passing
on both backends demonstrates that the semantics live in the scheduler, not
in the assembly. (It also means the runtime ports to non-x86-64 hosts with
zero new assembly.)

---

## 4. Stacks

Each stack is one anonymous `mmap` of `CORO_STACK_SIZE (256 KiB)` plus a
guard region, page-aligned. The lowest pages are `mprotect(PROT_NONE)`: the
first out-of-bounds write below the stack faults immediately instead of
silently corrupting neighbouring heap allocations. Stacks are freed by
`munmap` from the reap list (§5), never by the dying coroutine itself.

Why mmap rather than malloc: guard pages need page granularity, and unmapping
on reclamation returns the memory to the OS immediately instead of leaving it
to allocator heuristics.

---

## 5. The scheduler

### 5.1 Queue discipline

One intrusive singly-linked list serves as the run queue (`coro::next`).
A coroutine occupies exactly one of these states:

| state | meaning | requeued by |
|---|---|---|
| ready | on the run queue |, |
| running | executing now |, |
| parked | suspended, NOT queued | its waker, exactly once |
| finished | retired onto the reap list | never |

The **yield/park distinction** is the load-bearing invariant:

- `coro_yield()` pushes the caller back onto the run queue, then switches away.
- `coro_park()` switches away *without* requeueing; whoever unblocks the
  coroutine later adds exactly one run-queue entry via `runq_wake()`.

Confusing them produces double-resume use-after-free: a parked coroutine that
also requeues itself gets resumed twice, and the second resume executes on a
freed stack. Channels, the mutex, and `io.c` all build on park/wake; none of
them ever touch the run queue directly.

### 5.2 Reaping

A dying coroutine cannot free its own stack: `retire()` marks it finished and
chains it onto a reap list; `coro_run()` frees stacks after every swap back to
the main context. Freeing your own stack mid-swap would write saved registers
into unmapped memory.

### 5.3 Deadlock detection, I/O-aware

When a coroutine yields or parks and the run queue is empty, the old answer
was "abort." With I/O in the picture the correct question is: *is anyone
waiting on a completion that could still arrive?*

```
while nothing runnable:
    if no I/O pending  → genuine deadlock, abort
    coro_io_reap(1)    → block on the ring until a completion wakes someone
```

This contains a hard-won subtlety: `io_uring_wait_cqe` can fail with
`EINTR` when a signal (SIGINT from Ctrl-C, say) lands mid-wait. Treating a
failed reap as fatal produced spurious `deadlock` aborts on clean shutdown -
fixed by making the wait a retry loop and letting the *pending count*, not
any single reap result, decide life and death. Genuine deadlocks (everyone
yielded/parked on channels with no I/O outstanding) still abort loudly.

---

## 6. The io_uring layer

### 6.1 Submit-and-park

Every operation in [`include/io.h`](../include/io.h), `coro_read`,
`coro_write`, `coro_accept`, `coro_connect`, `coro_sleep`, follows one
pattern:

1. fetch a submission queue entry (flushing once if full),
2. tag it with the calling coroutine's pointer (`user_data`),
3. submit,
4. link self onto the pending list,
5. `coro_park()`,
6. on resume, return `self->io_result`, the kernel's completion status.

The pending list threads through `coro::next`. That reuse is safe for the
same reason park/wake works: a coroutine parked on I/O sits on no other
queue, so the link belongs to us until the completion wakes it. Completions
are matched by walking the list for the `user_data` pointer, storing
`cqe->res`, and calling `runq_wake`.

### 6.2 Weak hooks: linking with or without io_uring

The scheduler never calls into `io.c` directly. Instead [`coro.c`](../src/coro.c)
defines weak defaults,

```c
__attribute__((weak)) int coro_io_pending(void) { return 0; }
__attribute__((weak)) int coro_io_reap(int block) { return 0; }
```

and `io.o` overrides them when linked. Builds without liburing behave
exactly as the runtime did before I/O existed, the run queue alone drives
`coro_run()`, and tests gate themselves on `coro_io_probe()`. One translation
unit, zero #ifdefs outside the Makefile probe.

### 6.3 Why not epoll?

epoll answers "which fds are ready"; you still perform blocking-looking
syscalls yourself and must keep them non-blocking. io_uring answers "here are
your completed operations", which maps *exactly* onto park/wake. The
measured trade-off is real, though: for tiny hot-loopback echoes the classic
`EAGAIN`-retry backend posts higher req/s (a retry costs one userspace swap;
an uring op costs a kernel round trip), while uring wins connection ramp-up
~1.5× and wins outright wherever syscalls would block for real. Both backends
ship in the echo server, selected at startup; numbers in
[RESULTS.md](../benchmarks/RESULTS.md).

---

## 7. Channels and mutex

**Channels** ([`src/channel.c`](../src/channel.c)) are ring buffers of
pointer slots with sender and receiver wait queues. The FIFO invariant rests
on a simple observation: receivers park only when the ring is empty *and* no
sender is parked, and senders park only when the ring is full and no receiver
waits, the two wait queues are therefore never simultaneously non-empty.
Draining buffered items before rendezvousing with parked senders makes
delivery order provably equal to send order. `channel_fuzz` checks this
against a reference FIFO model on pseudo-random send/receive sequences;
`sched_fuzz` hammers the parking paths with random producer/consumer fleets
and verifies lost-item, lost-wake-up, and mutex-update invariants.

Waiter nodes live on the *waiter's own stack*. A suspended coroutine's stack
stays valid, so enqueueing a wait node needs no allocation and can never leak.

**Mutex** ([`src/mutex.c`](../src/mutex.c)) is FIFO with direct handoff:
`unlock` transfers ownership to the head waiter *before* waking it, and an
unlocking coroutine that immediately relocks queues behind everyone it just
unblocked, no barging, so `sched_fuzz`'s "counter == increments attempted"
invariant holds under arbitrary yield storms inside critical sections.

---

## 8. microdb: the proof of usefulness

[`apps/microdb/`](../apps/microdb) is ~700 lines of Redis-compatible string
store on the runtime: RESP multibulk + inline parsing, a chained hash table
with load-factor-triggered doubling rehash, lazy TTL expiry (one liveness
predicate shared by GET/DEL/DBSIZE/KEYS so they can never disagree about
expired-but-unevicted keys), and coroutine-per-connection serving where each
connection reads a batch, executes every complete command in it, and writes
all replies back: looking blocking while being multiplexed.

Measured against Redis 7.4 with `redis-benchmark` driving both servers:
~80% of Redis unpipelined and 1.2 to 1.5× faster at pipeline depth 16, where
per-command overhead dominates and syscall batching favours the thinner
stack. Full table: [RESULTS.md](../benchmarks/RESULTS.md).

Shutdown is signal-driven: SIGINT sets a flag, the supervisor drains
connections, and the accept error path unwinds cleanly. That path is
precisely where the EINTR deadlock bug of §5.3 lived.

---

## 9. Deliberately left out, and what adding it would look like

These were cut for focus; knowing *where they would go* is most of their
design value:

**Replication.** A primary would keep a monotonically growing command offset;
every write is appended to an in-memory backlog and fanned out over one
feeder coroutine per connected replica (each feeder is just
`write_all(backlog[offset:])` followed by streaming appends). A replica connects
with `PSYNC <replid> <offset>`, receives a snapshot of the current keyspace
followed by the backlog tail, then applies commands verbatim. Relative TTLs
are rewritten to absolute deadlines (`SET ... PXAT`) during propagation so
replicas never re-evaluate `now` differently. The runtime already provides
everything needed: `coro_connect` for the master link, `coro_sleep(300)`
for reconnect backoff, park/wake for flow control.

**Sharding proxy.** A coroutine-per-client front end routes by
`fnv1a(key) % N` (the same hash the keyspace already uses) over persistent
upstream connections, with MOVED-style redirects on topology change. The
interesting problems are all operational (rebalancing, resharding, failure
detection), not concurrency, because the runtime already removed shared-
state hazards by construction.

Neither requires changing a line of the scheduler; both would be apps/ the
way microdb is.

---

## 10. Limitations

- Cooperative only: a coroutine that never yields starves everything.
- One OS thread: throughput ceiling is one core, by design.
- Not signal-safe beyond the shutdown path; not thread-safe, the point is
  one thread done well.
- Channels carry pointer-sized slots; values larger than a pointer travel
  by ownership transfer or encoding (microdb and the fuzzers encode ints
  in the pointer).
- No select/join composition yet; `coro_sleep` is the only timer primitive.
