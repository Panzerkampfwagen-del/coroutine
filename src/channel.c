#include "channel.h"
#include "coro_internal.h"

#include <stdlib.h>
#include <string.h>

struct channel {
    char *buf;            /* capacity * item_size bytes of ring storage */
    size_t capacity;      /* maximum number of buffered items */
    size_t item_size;     /* bytes per item */
    size_t count;         /* items currently buffered */
    size_t head;          /* slot index of the oldest item */
    size_t tail;          /* slot index of the next free slot */
    coro_waitq_t senders; /* coroutines blocked because the channel was full */
    coro_waitq_t
        receivers; /* coroutines blocked because the channel was empty */
};

channel_t *channel_create(size_t capacity, size_t item_size) {
    if (capacity == 0 || item_size == 0)
        return NULL;

    channel_t *ch = calloc(1, sizeof *ch);
    if (!ch)
        return NULL;

    ch->buf = malloc(capacity * item_size);
    if (!ch->buf) {
        free(ch);
        return NULL;
    }
    ch->capacity = capacity;
    ch->item_size = item_size;
    return ch;
}

void channel_send(channel_t *ch, const void *item) {
    /* Park on the sender queue while full; recheck on wake, since another
       sender may have refilled the slot before we were scheduled. */
    while (ch->count == ch->capacity) {
        coro_waitq_push(&ch->senders, coro_current());
        coro_block();
    }

    memcpy(ch->buf + ch->tail * ch->item_size, item, ch->item_size);
    ch->tail = (ch->tail + 1) % ch->capacity;
    ch->count++;

    coro_t *r = coro_waitq_pop(&ch->receivers);
    if (r)
        coro_unblock(r);
}

void channel_recv(channel_t *ch, void *out) {
    /* Park on the receiver queue while empty; recheck on wake. */
    while (ch->count == 0) {
        coro_waitq_push(&ch->receivers, coro_current());
        coro_block();
    }

    memcpy(out, ch->buf + ch->head * ch->item_size, ch->item_size);
    ch->head = (ch->head + 1) % ch->capacity;
    ch->count--;

    coro_t *s = coro_waitq_pop(&ch->senders);
    if (s)
        coro_unblock(s);
}

void channel_destroy(channel_t *ch) {
    if (!ch)
        return;
    free(ch->buf);
    free(ch);
}
