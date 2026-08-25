#include "coro.h"

#include <stdlib.h>

/*
 * FIFO-handoff mutex: unlock() hands ownership directly to the oldest
 * waiter instead of releasing it publicly. No futex, no atomics -- the
 * cooperative scheduler guarantees we are never preempted mid-critical-
 * section except at explicit yields, so plain fields suffice.
 */

typedef struct mutex_waiter {
    coro_t            *co;
    struct mutex_waiter *next;
} mutex_waiter_t;

struct coro_mutex {
    coro_t         *owner;
    mutex_waiter_t *head, *tail;    /* FIFO of waiting coros */
};

coro_mutex_t *mutex_new(void)
{
    return calloc(1, sizeof(coro_mutex_t));
}

void mutex_free(coro_mutex_t *m) { free(m); }

int mutex_lock(coro_mutex_t *m)
{
    if (!m->owner) {
        m->owner = coro_current();
        return 0;
    }
    /* park; the node lives on OUR stack, which stays valid while we are
     * suspended inside this frame */
    mutex_waiter_t self = { .co = coro_current(), .next = NULL };
    if (m->tail) m->tail->next = &self;
    else         m->head       = &self;
    m->tail = &self;

    /* direct handoff: we are woken exactly once, already as owner */
    coro_park();
    return 0;
}

void mutex_unlock(coro_mutex_t *m)
{
    mutex_waiter_t *w = m->head;
    if (w) {
        m->head = w->next;
        if (!m->head) m->tail = NULL;
        m->owner = w->co;            /* direct handoff to oldest waiter */
        runq_wake(w->co);
    } else {
        m->owner = NULL;
    }
}
