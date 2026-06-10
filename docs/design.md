# coro - Design Document

A userspace cooperative coroutine runtime for Linux, with hand-written
context switching, an `io_uring` event loop, channels, and a mutex. This
document explains how it works, why it is built the way it is, and what was
deliberately left out.

- **Audience:** someone extending the runtime or evaluating the design.
- **Status:** stable. x86-64 and aarch64.
- **Companion code:** [`src/`](../src), [`include/`](../include); apps in
  [`apps/`](../apps) and [`examples/`](../examples).

---

## 1. Goals and non-goals

**Goals**

- Show, in a few hundred readable lines, exactly how a coroutine runtime
  suspends and resumes a native call stack.
- Make async I/O look synchronous to user code while a single thread drives
  thousands of in-flight operations through `io_uring`.
- Be correct under scrutiny: sanitizers, fuzzers, and a second ISA.
- Be useful: a real application (a Redis-compatible store) runs on top and is
  competitive with Redis on simple workloads.

**Non-goals**

- Multi-core parallelism (no M:N threading). The runtime is one OS thread.
- Preemption. Scheduling is cooperative; a coroutine runs until it yields,
  blocks, or exits.
- Portability beyond Linux/x86-64/aarch64.

These non-goals are what keep the synchronization story trivial: with one
thread and no preemption, the run queue, channels, and mutex need no atomics
or locks at all.

---

## 2. System overview

```
            ┌──────────────────────────────────────────────────────────┐
            │                      coro_run() loop                       │
            │                  (the scheduler, main stack)               │
            └───────┬───────────────────────────────────────┬──────────┘
        switch in   │ (coro_context_switch)                  │ run queue empty
        a coroutine ▼                                        │      │
            ┌───────────────┐  coro_yield / block / exit     │      ▼
            │  coroutine A  │ ───────────────────────────────┘   ┌──────────┐
            │  (own 1 MiB   │ ◀──────── switch back               │ io_uring │
            │   mmap stack) │                                     │  reap CQ │
            └──────┬────────┘                                     └────┬─────┘
                   │ coro_read/write/accept/sleep                       │
                   ▼                                                    │
            ┌───────────────┐   submit SQE (user_data = coro id)        │
            │     io.c      │ ─────────────────────────────────────────┘
            │  park + table │ ◀──── completion wakes the owning coroutine
            └───────────────┘
```

A coroutine is a struct plus an `mmap`'d stack. The scheduler is an ordinary
function running on the main stack. Control moves between them with
`coro_context_switch`, a hand-written assembly routine. When the run queue
drains, the scheduler blocks in the kernel waiting for `io_uring` completions,
each of which makes one parked coroutine runnable again.

---

## 3. Context switching

### 3.1 What must be saved

`coro_context_switch(from, to)` is called like a normal C function, so the
compiler has already spilled any live **caller-saved** registers around the
call. The switch therefore only needs to preserve **callee-saved** state:

| ABI | Callee-saved registers saved |
| --- | --- |
| System V AMD64 (x86-64) | `rbx`, `rbp`, `r12`-`r15`, `rsp` |
| AAPCS64 (aarch64) | `x19`-`x28`, `fp`(x29), `lr`(x30), `sp`, `d8`-`d15` |

Note the asymmetry: on x86-64 the SSE registers are all caller-saved, so none
are stored; on aarch64 the low 64 bits of `v8`-`v15` are callee-saved, so
`d8`-`d15` **must** be preserved or floating-point state leaks between
coroutines. (There is a regression test for exactly this.)

The instruction pointer is never stored explicitly. On x86-64 it rides on the
return address that `ret` pops off the destination stack; on aarch64 it is the
saved `lr`.

### 3.2 The switch itself

```
coro_context_switch(from, to):       # from=%rdi/x0, to=%rsi/x1
    save callee-saved regs  -> *from
    load callee-saved regs  <- *to
    ret                      # resume *to where it last left off
```

Because `rsp`/`sp` is part of the saved set, swapping it *is* the stack switch;
the trailing `ret` then resumes the destination. The struct layout in
[`coro.h`](../include/coro.h) is mirrored by byte offsets in
[`context.S`](../src/context.S); `_Static_assert`s in
[`coro.c`](../src/coro.c) fail the build if the two ever drift.

### 3.3 Starting a fresh coroutine: the trampoline and stack alignment

A brand-new coroutine has no "place it left off." We synthesize one. The
initial context resumes in `coro_trampoline`, which calls `coro_entry` (which
reads the running coroutine from a global and calls its user function) and then
falls into `coro_exit` when the user function returns.

The subtle part is ABI stack alignment. The System V AMD64 ABI requires that
`(%rsp + 8)` be a multiple of 16 at the point control enters a function (i.e.
just after a `call` pushes the 8-byte return address). Two consecutive `ret`s
into adjacent stack slots land at addresses 8 apart, so they cannot both be
canonically aligned. The trampoline sidesteps this by entering the user
function and `coro_exit` through a `call`/`ret` pair rather than two raw `ret`s:

```
              x86-64 initial stack (grows down)
   top (16-aligned) ─ 8  ┌───────────────────┐
                         │  coro_exit        │  ← returned into when fn() returns
   ctx.rsp ───────────►  │  coro_trampoline  │  ← first resume target
                         └───────────────────┘
   coro_create picks ctx.rsp ≡ 8 (mod 16) so that, after the trampoline's
   `call coro_entry` pushes a return address, fn() sees rsp ≡ 8 (mod 16), and
   the trampoline's `ret` then enters coro_exit at the same alignment.
```

aarch64 is simpler: the return address lives in `lr`, not on the stack, so
there is nothing to plant. `coro_create` just points `sp` at a 16-aligned top
and sets `lr` to the trampoline, which does `bl coro_entry; b coro_exit`.

### 3.4 Stacks and the guard page

Each coroutine stack is a single `mmap(MAP_PRIVATE|MAP_ANONYMOUS)` region (1 MiB
by default; 64-256 KiB for connection handlers). The lowest page is
`mprotect(PROT_NONE)` - a guard page - so a stack overflow faults precisely
instead of silently scribbling on the neighbouring mapping. The mapping is
never `malloc`'d: anonymous `mmap` gives page-aligned, demand-zeroed memory and
lets the guard page be a separate protection.

---

## 4. The scheduler

### 4.1 Model: a central loop, not symmetric switching

There are two common cooperative designs:

1. **Symmetric** - a yielding coroutine picks the next coroutine and switches
   directly to it.
2. **Central loop** - every yield/block switches back to one scheduler context,
   which decides what runs next.

This runtime uses the **central loop** (`coro_run`). The cost is one extra
context switch per scheduling decision (coroutine → scheduler → next coroutine
instead of coroutine → next coroutine). The benefit is that there is exactly
one place that owns "what runs next," which is where `io_uring` completion
reaping naturally lives: when the run queue empties, the loop - and only the
loop - blocks on the kernel. This keeps the I/O integration to a dozen lines
and avoids every coroutine having to know how to poll.

```
coro_run():
    loop:
        c = runq.pop()
        if c == NULL:
            if no I/O pending: return        # everything finished
            io_reap(block=true); continue    # wait for a completion, retry
        switch_into(c)                        # runs until it yields back
        on return, inspect c.state:
            READY   -> runq.push(c)           # voluntary yield
            BLOCKED -> (do nothing)           # parked on a wait queue
            DONE    -> free(c)
```

### 4.2 Coroutine states and the one-queue invariant

A coroutine is in exactly one of `READY`, `RUNNING`, `BLOCKED`, `DONE`. The key
invariant that keeps the data structures tiny: **a coroutine is on at most one
queue at a time** - either the run queue, or a single wait queue (a channel's,
the mutex's, or the I/O pending table) - never two. That lets every queue reuse
the same intrusive `next` pointer inside `struct coro`, so enqueue/dequeue are
allocation-free and the whole "wait queue" type is a two-pointer struct with
push/pop helpers.

### 4.3 yield vs block

`coro_yield` and `coro_block` are the same two instructions - set state, switch
to the scheduler - differing only in the state they set. `yield` sets `READY`
(the scheduler re-enqueues it); `block` sets `BLOCKED` (the scheduler leaves it
alone, because something else - a channel, the mutex, an I/O completion - holds
it on a wait queue and will call `coro_unblock` later). There are no spurious
wake-ups: a `BLOCKED` coroutine becomes runnable only through an explicit
`coro_unblock`.

---

## 5. Blocking primitives

### 5.1 Channels

A bounded channel is a ring buffer plus two wait queues (senders, receivers).
`send` to a full channel parks the caller on the sender queue and yields;
`recv` from an empty channel parks on the receiver queue. Each completed `send`
wakes one waiting receiver and vice versa. Waiters re-check the condition in a
`while` loop after waking, so the code is correct even if another coroutine
races in between the wake and the rescheduling (which, being cooperative, it
cannot here - but the loop keeps it robust and obviously-correct).

### 5.2 Mutex

Cooperative mutual exclusion needs no atomics or futex: a held flag plus a FIFO
of waiters. `unlock` performs a **direct hand-off** - it does not clear the
flag; it pops the longest-waiting coroutine and marks it runnable, transferring
ownership. The woken coroutine therefore does not re-check the flag (it already
owns the lock), which gives strict FIFO fairness and rules out barging.

---

## 6. io_uring integration

### 6.1 The park/submit/reap protocol

```
coro_read/write/accept/sleep:
    sqe = get_sqe(ring)
    prep the op on sqe
    sqe->user_data = current coroutine id
    io_uring_submit(ring)
    park current coroutine in g_pending[id]   # hashed by id
    coro_block()                              # -> scheduler
    return self->io_result                    # filled in by the reaper

scheduler idle path (run queue empty):
    io_uring_wait_cqe(ring, &cqe)             # block in the kernel
    for each available cqe:
        coro = g_pending.take(cqe->user_data)
        coro->io_result = cqe->res
        coro_unblock(coro)
        cqe_seen(cqe)
```

A submitted operation carries the coroutine **id** in `user_data`; on
completion the reaper uses it to find the parked coroutine in a small hash
table (chained through the same intrusive `next`, per the one-queue invariant),
stores `cqe->res`, and makes it runnable. The coroutine resumes inside its
`coro_read`/etc. and returns the result, having looked perfectly synchronous.

`coro_sleep` is the same machinery with `IORING_OP_TIMEOUT`; it is what lets the
servers print periodic stats and notice shutdown without a busy loop.

### 6.2 The weak-symbol seam

The scheduler must call into the I/O layer (to test for pending I/O and to
reap), but programs that never do I/O (`hello`, `pingpong`) should not drag in
`liburing`. The two hooks - `coro_io_pending` and `coro_io_reap` - are defined
**`__attribute__((weak))`** in `coro.c` as no-ops, and **strongly** in `io.c`.
The linker pulls `io.o` out of the archive only when something references a real
I/O symbol (`coro_read`, ...), at which point the strong definitions win. So the
I/O layer is pay-for-what-you-use at link time, with no `#ifdef`s.

---

## 7. AddressSanitizer and custom stacks

ASan keeps shadow memory and, for stack-use-after-return detection, a "fake
stack" per real stack. Hand-rolled coroutine stacks confuse it: switching
stacks under ASan's nose looks like corruption. The fix is the sanitizer's
fiber API. Every switch is bracketed:

```
__sanitizer_start_switch_fiber(save_slot_or_NULL, to_stack_bottom, to_stack_size)
coro_context_switch(from, to)
__sanitizer_finish_switch_fiber(saved, ...)
```

`start` tells ASan we are leaving a fiber (saving its fake stack, unless the
coroutine is exiting, in which case we pass `NULL` to discard it) and where the
destination's stack lives; `finish` completes the move on the other side. A
fresh coroutine calls `finish` first thing in `coro_entry`, which is also where
the scheduler's own stack bounds are learned. All of this compiles to nothing
unless `__SANITIZE_ADDRESS__` is defined, so release builds pay zero cost. The
result: the full suite, including the stress and scheduler fuzzers, runs clean
under `-fsanitize=address,undefined`.

---

## 8. Portability: the architecture seam

Only three things are architecture-specific, and each is guarded by
`#if defined(__x86_64__)` / `#elif defined(__aarch64__)`:

1. `coro_context_t` - the saved register set ([`coro.h`](../include/coro.h)).
2. The two routines in [`context.S`](../src/context.S) (the file is a `.S`, so
   the C preprocessor selects the right assembly).
3. The ~5 lines of initial-stack seeding in `coro_create`.

Everything else - scheduler, channels, mutex, I/O, the apps - is portable C.
The aarch64 port is exercised by cross-compiling the core and running the
examples, the stress test, and the scheduler fuzzer under `qemu-user`
(`make aarch64-check`). The `io_uring` layer is portable C too, but the cross
test omits it only because an aarch64 build of `liburing` is not installed in
CI.

---

## 9. Testing strategy

| Layer | What it checks | Where |
| --- | --- | --- |
| Examples | context switch, scheduler, channels, I/O end to end | `examples/` |
| Stress | thousands of coroutines, channel backpressure, mutex contention; hard invariants | `tests/stress/` |
| Channel fuzzer | channel FIFO ordering vs a reference model, random op streams | `tests/fuzz/channel_fuzz.c` |
| Scheduler fuzzer | no lost item / no lost wake-up / no lost mutex update, over random workloads | `tests/fuzz/sched_fuzz.c` |
| Sanitizers | UAF / overflow / UB across all of the above | `make debug` |
| Cross-arch | the AArch64 context switch (incl. FP regs) | `make aarch64-check` |
| Static analysis | `clang-tidy`, `cppcheck`, `clang-format` | CI |

The fuzzers are model-checking, not coverage-chasing: each builds a workload
that is correct *by construction* (balanced producers/consumers, a reference
FIFO), so any invariant violation is a real bug, and the failing seed is
printed for replay.

---

## 10. Performance

The demo store, **microdb**, speaks RESP and is driven by `redis-benchmark`.
On a 12-vCPU WSL2 box the single node reaches roughly 88-97% of Redis 7.0.15's
pipelined throughput on `SET`/`GET`/`INCR`, in ~1,000 lines of runtime plus a
store-with-replication of ~1,200 lines (see
[`benchmarks/microdb_results.txt`](../benchmarks/microdb_results.txt)). The
sharding proxy's overhead is covered in §11.7.

Where the time goes, and the trade-offs taken:

- **One coroutine per connection.** Simple and fast; each handler is a tiny
  state-free loop. The cost is one 64-256 KiB stack per connection.
- **Batched syscalls.** A handler does one `read`, parses *all* pipelined
  commands in the buffer, and writes *all* replies in one `write`. This is why
  pipelined throughput is close to Redis; the non-pipelined gap reflects
  Redis's more aggressive event-loop and syscall batching.
- **Central-loop overhead.** The extra switch per yield is a real cost; it buys
  a one-line I/O integration. For an I/O-bound server it is in the noise.

---

## 11. Distribution: replication, sharding, and failover

microdb began as a single node. The distributed layer turns a set of nodes into
one partitioned, replicated key/value service - and, crucially, exercises the
runtime in a direction the single-node store never did: **a coroutine as a
client**. Every coroutine so far has been a server (accept, read a request,
write a reply); a replica link and the proxy instead *dial outward* and fan
across backends. That is the one capability the runtime was missing, so the
layer starts with a new primitive.

### 11.1 The missing primitive: `coro_connect`

[`coro_connect`](../include/io.h) is the outbound complement of `coro_accept`:
it submits an `IORING_OP_CONNECT`, parks the coroutine, and resumes it when the
connection completes. With it, the same park/submit/reap machinery (§6) drives
client sockets, so a coroutine can hold connections to many backends and let the
scheduler interleave them - no callbacks, no state machine.

### 11.2 Architecture

```
              clients (redis-cli / redis-benchmark, RESP)
                                |
                        +---------------+
                        | microdb-proxy |   hash(key) % nshards
                        |  (1 coroutine |   writes -> primary
                        |  per client)  |   reads  -> replica (round-robin)
                        +---------------+
                          /            \
                 shard 0 /              \ shard 1
            +-----------+             +-----------+
            |  primary  |  PSYNC      |  primary  |
            |   :6401   |<------+     |   :6403   |<------+
            +-----------+       |     +-----------+       |
                                |                         |
            +-----------+       |     +-----------+       |
            |  replica  |-------+     |  replica  |-------+
            |   :6402   |             |   :6404   |
            +-----------+             +-----------+
```

Each node is a separate process - the single-thread, no-locks model is preserved
*within* a node, and processes (not threads) give a genuinely distributed
failure model.

### 11.3 Replication protocol

A replica is started with `-r host:port` (or retargeted live with `REPLICAOF`).
Its master-link coroutine runs:

```
replica -> primary:  *1\r\n$5\r\nPSYNC\r\n
primary -> replica:  +FULLRESYNC <replid> <offset>\r\n
                     $<n>\r\n<n bytes of snapshot>     (RDB-style bulk)
                     ... then the live write-command stream, forever
```

The snapshot is **itself a sequence of RESP `SET` commands** (with absolute
`PXAT` expiries), so the replica applies the snapshot and the live stream through
the identical command path. There is no separate "load" format to maintain.

**Snapshot/stream consistency** hinges on the cooperative model. When a replica
connects, the primary (in one coroutine, *without yielding*) registers the feed,
then serializes the snapshot. Because no other coroutine runs during that window,
the snapshot is a consistent point-in-time image, and every write that happens
*after* it - i.e. after the first yield in `coro_write` - lands in the feed's
backlog. No write is lost or duplicated across the snapshot/stream boundary. This
is the same "a non-yielding region is atomic" property the whole codebase leans
on (§5).

**The feeder.** Each connected replica has a per-feed backlog buffer and a
1-slot channel used as a wake-up semaphore. When a primary applies a write it
re-encodes the command and appends it to every feed's backlog; if a feeder is
parked it signals the channel. The feeder coroutine drains the backlog to the
socket and parks on the channel when caught up. This reuses the public channel
primitive (§5.1) rather than inventing a new wait mechanism, and the
"append-then-signal" runs without yielding so there is no lost-wake-up race.

**Read-only replicas / chained replication.** A replica rejects client writes
(`-READONLY`) but applies its master's stream; since *applying* a write also
propagates it to any of that node's own feeds, chained replication (replica of a
replica) works with no extra code.

### 11.4 `WAIT` and the consistency knob

Replication is asynchronous: a primary acknowledges a write before its replicas
have it, so a replica read can be stale. `WAIT <n> <timeout>` exposes the knob -
it blocks until `n` replicas have the current offset (or the timeout fires),
turning "fast but maybe stale" into "slower but durable to `n` copies".

The accounting is deliberately simple: the primary counts live-stream bytes as a
monotonic offset, and each feed tracks how far it has been **flushed**. `WAIT`
polls until enough feeds have flushed past the target offset. This is
**ack-on-flush** (the bytes reached the replica's socket), which is weaker than
Redis's **ack-on-apply** (the replica ran the command and replied with
`REPLCONF ACK`). It needs no back-channel and is honest about what it proves;
upgrading to ack-on-apply would add a reader on the replica link and a real ACK
offset. This is called out as a limitation rather than hidden.

### 11.5 Sharding, routing, and pipelining

The proxy speaks RESP, so stock clients drive it unchanged. Routing is
`shard = fnv1a(key) % nshards`; writes go to the shard's primary and reads
round-robin its replicas (or the primary if a shard has none). Multi-key
commands (`MGET`/`MSET`/`DEL`/`EXISTS`) fan out per key and the proxy reassembles
one reply in the original order, so a key on another shard still works.

A naive proxy that sends one command and waits for its reply destroys client
pipelining. Two design choices avoid that:

- **Pipelined dispatch.** A whole batch of simple single-key commands is
  dispatched to the backends first, then their replies are drained in command
  order. Because each backend processes its commands in order, the i-th reply
  from a backend matches the i-th request sent to it, so cross-shard
  interleaving stays correct.
- **Write coalescing.** Within a batch, requests to the same backend are staged
  in a buffer and flushed with a single `write` per backend, not one per command.

The batch is bounded (`MAX_PIPELINE`) so in-flight requests *and* replies always
fit in the socket buffers - otherwise a large enough burst could deadlock with
the proxy blocked writing requests while the backend blocks writing replies.
Local commands (`PING`, `INFO`, ...) and multi-key fan-outs drain the pipeline
first to preserve ordering, then run synchronously.

### 11.6 Failover - and why not consensus

A health-check coroutine in the proxy pings each primary; after
`HEALTH_THRESHOLD` consecutive misses it promotes the shard's first replica
(`REPLICAOF NO ONE`) and repoints routing at it. Reads keep working *through* a
primary outage because they were already going to the (still-live) replica;
writes recover once promotion completes.

This is **coordinator-driven failover, not consensus**, and the trade-offs are
stated plainly rather than papered over:

- **No fencing of the old primary** - if it returns, two nodes believe they are
  primary (split brain). A real system needs fencing/epochs.
- **Bounded data loss** - async replication means writes the old primary
  acknowledged but had not yet streamed are lost on promotion. `WAIT` narrows
  this window; it does not close it.
- **The proxy is a single coordinator** - fine for a demo, a single point of
  truth for routing in production.

Implementing Raft (leader election + log matching + commit index + snapshots)
was explicitly **out of scope**: it is a multi-week correctness problem in its
own right, and a half-correct consensus layer would be worse than an honest
coordinator-with-caveats. The runtime is a fine substrate for it later (each
node is already a single-threaded event loop), but it is a separate project.

### 11.7 Performance of the proxy hop

The proxy does roughly double the socket I/O of a single node (client side plus
backend side) on one thread, so it trades throughput for the routing it
provides. On the same WSL2 box (`benchmarks/dist_bench.sh`), routing through the
2-shard proxy retains about **60% of single-node throughput non-pipelined and
~45-50% pipelined** (~1.0-1.1M vs ~2.2M req/s on `SET`/`GET`/`INCR -P16`; see
[`benchmarks/dist_results.txt`](../benchmarks/dist_results.txt)). That gap is the
textbook motivation for client-side sharding (skip the hop) - which this design
could also support, since the routing rule is just a hash the client could
compute itself.

---

## 12. Trade-offs and alternatives considered

- **Central loop vs symmetric switching** (§4.1): chose the central loop for a
  single, simple I/O integration point, accepting one extra switch per yield.
- **id in `user_data` vs pointer in `user_data`** (§6.1): storing the coroutine
  *id* and looking it up in a table is marginally slower than stashing the
  pointer directly, but it keeps `user_data` opaque and makes use-after-free of
  a stale completion impossible to act on (an unknown id is simply ignored).
- **Cooperative mutex hand-off vs test-and-set** (§5.2): hand-off gives FIFO
  fairness for free in a cooperative world; a spin/test-and-set design would be
  pointless without preemption.
- **No preemption / no M:N** (§1): the deliberate simplification. It removes all
  locking from the core. Adding M:N threading would mean making the run queue,
  channels, mutex, and pending table thread-safe and adding cross-thread
  wake-ups - a different project (see Future work).
- **Scale out by processes, not threads** (§11): rather than make one node
  multi-core (M:N), the distributed layer runs many single-threaded nodes and
  routes between them. This keeps every node lock-free and gives a real
  distributed failure model, at the cost of a proxy hop (§11.7).
- **Coordinator failover vs consensus** (§11.6): chose an honest
  health-check-and-promote coordinator with stated split-brain/data-loss caveats
  over a half-implemented Raft.

---

## 13. Limitations and future work

- **Cooperative only / single-threaded** - a CPU-bound coroutine starves
  others; no multi-core scaling.
- **Basic I/O surface** - read/write/accept/connect/timeout; no TLS, no
  per-operation cancellation, no per-file offsets beyond `read(2)`/`write(2)`
  semantics.
- **Future: cancellation and deadlines** - per-operation timeouts via
  `IORING_OP_LINK_TIMEOUT` and cooperative teardown of a coroutine parked on
  I/O (`IORING_OP_ASYNC_CANCEL`). This fits the current architecture and is the
  most valuable next step.
- **Future: M:N threading** - a pool of worker threads, each with its own ring
  and a work-stealing run queue. This is the large, design-changing direction
  (it reintroduces real concurrency and locking).
- **Distributed layer (§11) is deliberately minimal.** Acknowledged gaps, each a
  natural next step: ack-on-apply `WAIT` (a real `REPLCONF ACK` back-channel);
  consensus-based failover with fencing (Raft) instead of the coordinator;
  consistent hashing and online resharding instead of a fixed `hash % nshards`;
  partial resync (a replication backlog) instead of a full snapshot on every
  reconnect; and client-side sharding to skip the proxy hop.

---

## 14. References

- System V Application Binary Interface, AMD64 Architecture Processor
  Supplement (the psABI) - callee-saved registers and the 16-byte stack
  alignment rule.
- Procedure Call Standard for the Arm 64-bit Architecture (AAPCS64) - `x19`-`x28`,
  `d8`-`d15` callee-saved rules.
- J. Axboe, *Efficient IO with io_uring* (the io_uring design document); kernel
  sources `io_uring/` and `include/uapi/linux/io_uring.h`; the `liburing`
  library.
- AddressSanitizer fiber annotations - `sanitizer/common_interface_defs.h`
  (`__sanitizer_start_switch_fiber` / `__sanitizer_finish_switch_fiber`) and the
  ASan "fiber"/"ucontext" documentation.
- Prior art in stackful coroutines/green threads: `boost.context`, Russ Cox's
  `libtask`, the GNU Pth library.
- A. Adya et al., *Cooperative Task Management without Manual Stack Management*
  (USENIX ATC 2002) - the cooperative-vs-event-driven framing.
- Redis serialization protocol (RESP) specification.
