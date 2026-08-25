#include "coro.h"

#include <stdlib.h>
#include <string.h>

/*
 * Cooperative channel: because scheduling only happens at explicit yield
 * points, we need no atomics or futexes -- a waiter that is queued simply
 * yields until someone wakes it.
 */

typedef struct wait_node {
    coro_t          *co;
    void            *val;
    int              done;      /* sender: value was taken */
    struct wait_node *next;
} wait_node_t;

struct chan {
    void       **buf;
    size_t       cap, head, tail, count;

    wait_node_t *recv_q_head, *recv_q_tail;
    wait_node_t *send_q_head, *send_q_tail;
};

static void wq_push(wait_node_t **head, wait_node_t **tail, wait_node_t *n)
{
    n->next = NULL;
    if (*tail) (*tail)->next = n;
    else       *head = n;
    *tail = n;
}

static wait_node_t *wq_pop(wait_node_t **head, wait_node_t **tail)
{
    wait_node_t *n = *head;
    if (!n) return NULL;
    *head = n->next;
    if (!*head) *tail = NULL;
    n->next = NULL;
    return n;
}

chan_t *chan_new(size_t capacity)
{
    chan_t *ch = calloc(1, sizeof(*ch));
    if (!ch) abort();
    ch->cap = capacity ? capacity : 1;   /* cap>=1 keeps semantics simple */
    ch->buf = calloc(ch->cap, sizeof(void *));
    if (!ch->buf) abort();
    return ch;
}

void chan_free(chan_t *ch)
{
    free(ch->buf);
    free(ch);
}

int chan_send(chan_t *ch, void *v)
{
    /* waiting receiver? rendezvous: hand over directly and wake it */
    wait_node_t *r = wq_pop(&ch->recv_q_head, &ch->recv_q_tail);
    if (r) {
        r->val  = v;
        r->done = 1;
        runq_wake(r->co);            /* implemented in coro.c */
        return 0;
    }
    if (ch->count < ch->cap) {       /* room in the ring buffer */
        ch->buf[ch->tail] = v;
        ch->tail = (ch->tail + 1) % ch->cap;
        ch->count++;
        return 0;
    }
    /* full: park this sender until a recv drains a slot */
    wait_node_t self = { .co = coro_current(), .val = v, .done = 0 };
    wq_push(&ch->send_q_head, &ch->send_q_tail, &self);
    coro_park();                 /* woken exactly once via runq_wake */
    return 0;
}

int chan_recv(chan_t *ch, void **v)
{
    if (ch->count > 0) {
        /* drain the buffer first -- it holds the OLDEST items */
        *v = ch->buf[ch->head];
        ch->head = (ch->head + 1) % ch->cap;
        ch->count--;

        /* freed a slot: pull the oldest parked sender's value in */
        wait_node_t *s = wq_pop(&ch->send_q_head, &ch->send_q_tail);
        if (s) {
            s->done = 1;
            ch->buf[ch->tail] = s->val;
            ch->tail = (ch->tail + 1) % ch->cap;
            ch->count++;
            runq_wake(s->co);
        }
        return 0;
    }
    /* buffer empty: a parked sender hands its value over directly */
    wait_node_t *s = wq_pop(&ch->send_q_head, &ch->send_q_tail);
    if (s) {
        s->done = 1;
        runq_wake(s->co);
        *v = s->val;
        return 0;
    }
    /* empty: park this receiver until a send arrives */
    wait_node_t self = { .co = coro_current(), .val = NULL, .done = 0 };
    wq_push(&ch->recv_q_head, &ch->recv_q_tail, &self);
    coro_park();                 /* woken exactly once via runq_wake */
    *v = self.val;
    return 0;
}
