#ifndef CORO_INTERNAL_H
#define CORO_INTERNAL_H

/* Shared between src/coro.c and the io_uring backend (src/io.c). Not part
 * of the public API: coro_t stays opaque to user code. */

#include <ucontext.h>

#include "coro.h"

struct coro {
    ucontext_t  ctx;           /* CORO_USE_UCONTEXT builds            */
    coro_ctx_t  regs;          /* hand-written asm switch builds      */
    void       *stack;
    coro_fn     fn;
    void       *arg;
    int         finished;
    int         started;
    int         io_result;     /* completion result of the last uring op */
    struct coro *next;         /* run-queue / pending-I/O link: a coroutine
                                  sits on at most one queue at a time, so a
                                  parked coroutine may reuse it            */
};

/* Scheduler services shared with src/io.c. */
void runq_wake(coro_t *c);   /* requeue a parked coroutine exactly once */

#endif /* CORO_INTERNAL_H */
