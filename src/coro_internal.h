#ifndef CORO_INTERNAL_H
#define CORO_INTERNAL_H

#include "coro.h"
#include <stddef.h>

/* Lifecycle states. A coroutine sits in exactly one queue at a time, so the
   intrusive `next` link is shared between the run queue and any wait queue. */
typedef enum {
    CORO_READY,   /* on the run queue, waiting for a turn */
    CORO_RUNNING, /* currently executing */
    CORO_BLOCKED, /* parked on a channel/mutex/I/O wait queue */
    CORO_DONE,    /* finished; awaiting reclamation by the scheduler */
} coro_state_t;

struct coro {
    coro_context_t ctx; /* saved registers; must be first */
    void (*fn)(void *); /* user entry function */
    void *arg;          /* user argument */
    void *stack_base;   /* mmap base (guard page included) */
    size_t stack_size;  /* total mmap length */
    int id;             /* unique id, used as io_uring user_data */
    coro_state_t state;
    int io_result;     /* result of the most recent I/O completion */
    struct coro *next; /* run queue / wait queue link */
};

/* Helpers for coro_waitq_t (declared in coro.h). The queue is threaded through
   coro::next; a coroutine is only ever on one queue at a time (the run queue
   or a single wait queue), so sharing the link is safe. Used by the scheduler,
   channels, the mutex and io.c. */
static inline void coro_waitq_push(coro_waitq_t *q, coro_t *c) {
    c->next = NULL;
    if (q->tail)
        q->tail->next = c;
    else
        q->head = c;
    q->tail = c;
}

static inline coro_t *coro_waitq_pop(coro_waitq_t *q) {
    coro_t *c = q->head;
    if (!c)
        return NULL;
    q->head = c->next;
    if (!q->head)
        q->tail = NULL;
    c->next = NULL;
    return c;
}

/* Hand-written context switch in context.S: save callee-saved registers into
 *from, load them from *to, and resume *to at its stored return address. */
extern void coro_context_switch(coro_context_t *from, coro_context_t *to);

/* First C function a fresh coroutine runs (called from coro_trampoline in
   context.S). Has external linkage so the assembly can reach it. */
void coro_entry(void);

/* Scheduler services shared with channel.c, sync.c and io.c. */
coro_t *coro_current(void); /* the running coroutine */
void coro_block(void);      /* park the running coroutine; returns when woken */
void coro_unblock(coro_t *c); /* mark a parked coroutine runnable */

/* I/O integration points. Weakly stubbed in coro.c and overridden by io.c so
   the scheduler links cleanly whether or not the io_uring layer is present. */
int coro_io_pending(void);   /* nonzero if any coroutine awaits I/O */
int coro_io_reap(int block); /* harvest completions; returns count made ready */

#endif /* CORO_INTERNAL_H */
