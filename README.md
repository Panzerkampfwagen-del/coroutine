# scoped_down/coroutine

A minimal stackful coroutine runtime in C17, mirroring the core ideas of
`~/coroutine/coroutine` at undergraduate scope: hand-written x86-64 context
switch, cooperative round-robin scheduler, buffered channels with FIFO wait
queues, direct-handoff mutex, guard-paged stacks, and a coroutine-per-connection
TCP echo server on non-blocking sockets.

## Layout

```
include/coro.h      public API (scheduler, channels, mutex)
src/switch.S        coro_swap: callee-saved-register context switch (x86-64)
src/coro.c          scheduler, stacks (mmap + PROT_NONE guard page), parking
src/channel.c       ring-buffer channel + sender/receiver wait queues
src/mutex.c         FIFO-handoff mutex (no futex, no atomics)
examples/echo_server.c   single-threaded concurrent TCP echo server
tests/test_all.c    assert-based test runner (6 tests, 12 assertions)
```

## Build & run

```sh
make test          # build + run suite with the hand-written asm switch
make sanitize      # same suite under ASan + UBSan
make ucontext-test # portable backend (swapcontext) instead of asm
make run-echo      # start echo server on :8080; try `nc 127.0.0.1 8080`
```

## Key concepts demonstrated

- Context switching = save/restore callee-saved registers only; the fake initial
  frame seeded by `coro_create()` makes a new coroutine "return" into its entry
  point with the SysV ABI alignment intact.
- Cooperative blocking: channels/mutexes park coroutines on their own wait lists;
  `coro_park()` suspends without requeueing (each wake adds exactly one run-queue
  entry — getting this wrong causes double-resume use-after-free).
- Guard pages catch stack overflow as SIGSEGV.
- Echo server: `read/write` returning `EAGAIN` yields instead of blocking,
  giving single-threaded concurrency across connections.
