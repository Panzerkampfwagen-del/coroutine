#include "sync.h"
#include "coro_internal.h"

void coro_mutex_init(coro_mutex_t *m) {
    m->locked = 0;
    m->waiters.head = NULL;
    m->waiters.tail = NULL;
}

void coro_mutex_lock(coro_mutex_t *m) {
    if (m->locked) {
        /* Park until unlock hands ownership to us; the lock stays held across
           the handoff, so on wake we already own it. */
        coro_waitq_push(&m->waiters, coro_current());
        coro_block();
        return;
    }
    m->locked = 1;
}

void coro_mutex_unlock(coro_mutex_t *m) {
    coro_t *next = coro_waitq_pop(&m->waiters);
    if (next) {
        /* Direct handoff: leave `locked` set and wake the longest waiter,
           which becomes the new owner. */
        coro_unblock(next);
    } else {
        m->locked = 0;
    }
}
