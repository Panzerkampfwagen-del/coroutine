#define _GNU_SOURCE
#include "io.h"
#include "coro_internal.h"

#include <errno.h>
#include <liburing.h>
#include <stdint.h>

#define IO_DEPTH 64         /* io_uring submission/completion queue depth */
#define PENDING_BUCKETS 128 /* power of two; hash of in-flight coroutines */

static struct io_uring g_ring;
static int g_ring_ready;
static int g_pending_count;

/* Coroutines awaiting completion, hashed by id and chained through coro::next.
   A coroutine parked here is on no other queue, so reusing the link is safe. */
static coro_t *g_pending[PENDING_BUCKETS];

static inline unsigned pending_slot(int id) {
    return (unsigned)id & (PENDING_BUCKETS - 1);
}

static void pending_add(coro_t *c) {
    unsigned b = pending_slot(c->id);
    c->next = g_pending[b];
    g_pending[b] = c;
    g_pending_count++;
}

static coro_t *pending_take(int id) {
    coro_t **link = &g_pending[pending_slot(id)];
    for (coro_t *c = *link; c; link = &c->next, c = c->next) {
        if (c->id == id) {
            *link = c->next;
            c->next = NULL;
            g_pending_count--;
            return c;
        }
    }
    return NULL;
}

static int io_init(void) {
    if (g_ring_ready)
        return 0;
    int rc = io_uring_queue_init(IO_DEPTH, &g_ring, 0);
    if (rc < 0)
        return rc; /* -errno */
    g_ring_ready = 1;
    return 0;
}

/* Fetch a submission queue entry, flushing the queue once if it is full. */
static struct io_uring_sqe *io_get_sqe(void) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&g_ring);
    if (!sqe) {
        io_uring_submit(&g_ring);
        sqe = io_uring_get_sqe(&g_ring);
    }
    return sqe;
}

/* Tag the request with the caller's id, submit it, and park the caller until
   the scheduler harvests the matching completion. Returns cqe->res, or the
   submission error if io_uring_submit fails. */
static int io_submit_and_wait(struct io_uring_sqe *sqe) {
    coro_t *self = coro_current();
    io_uring_sqe_set_data64(sqe, (uint64_t)(unsigned)self->id);

    int rc = io_uring_submit(&g_ring);
    if (rc < 0)
        return rc; /* -errno; nothing was queued */

    pending_add(self);
    coro_block();
    return self->io_result;
}

int coro_read(int fd, void *buf, size_t len) {
    int rc = io_init();
    if (rc < 0)
        return rc;
    struct io_uring_sqe *sqe = io_get_sqe();
    if (!sqe)
        return -EBUSY;
    /* offset -1: use and advance the file position, mirroring read(2). */
    io_uring_prep_read(sqe, fd, buf, (unsigned)len, (uint64_t)-1);
    return io_submit_and_wait(sqe);
}

int coro_write(int fd, const void *buf, size_t len) {
    int rc = io_init();
    if (rc < 0)
        return rc;
    struct io_uring_sqe *sqe = io_get_sqe();
    if (!sqe)
        return -EBUSY;
    io_uring_prep_write(sqe, fd, buf, (unsigned)len, (uint64_t)-1);
    return io_submit_and_wait(sqe);
}

int coro_accept(int fd, struct sockaddr *addr, socklen_t *addrlen) {
    int rc = io_init();
    if (rc < 0)
        return rc;
    struct io_uring_sqe *sqe = io_get_sqe();
    if (!sqe)
        return -EBUSY;
    io_uring_prep_accept(sqe, fd, addr, addrlen, 0);
    return io_submit_and_wait(sqe);
}

int coro_connect(int fd, const struct sockaddr *addr, socklen_t addrlen) {
    int rc = io_init();
    if (rc < 0)
        return rc;
    struct io_uring_sqe *sqe = io_get_sqe();
    if (!sqe)
        return -EBUSY;
    io_uring_prep_connect(sqe, fd, addr, addrlen);
    return io_submit_and_wait(sqe);
}

int coro_sleep(uint64_t ms) {
    int rc = io_init();
    if (rc < 0)
        return rc;
    struct io_uring_sqe *sqe = io_get_sqe();
    if (!sqe)
        return -EBUSY;

    struct __kernel_timespec ts = {
        .tv_sec = (long long)(ms / 1000),
        .tv_nsec = (long long)(ms % 1000) * 1000000LL,
    };
    /* The kernel copies ts during submission, so the stack copy is safe. A
       timeout that elapses completes with -ETIME, which we treat as success. */
    io_uring_prep_timeout(sqe, &ts, 0, 0);
    io_submit_and_wait(sqe);
    return 0;
}

/* Wake the coroutine that owns a completion and recycle the CQE. */
static int reap_one(struct io_uring_cqe *cqe) {
    int id = (int)(unsigned)io_uring_cqe_get_data64(cqe);
    coro_t *c = pending_take(id);
    if (c) {
        c->io_result = cqe->res;
        coro_unblock(c);
    }
    io_uring_cqe_seen(&g_ring, cqe);
    return c ? 1 : 0;
}

/* Strong overrides of the weak hooks in coro.c. */

int coro_io_pending(void) {
    return g_pending_count > 0;
}

int coro_io_reap(int block) {
    if (!g_ring_ready || g_pending_count == 0)
        return 0;

    struct io_uring_cqe *cqe;
    int woken = 0;

    if (block) {
        if (io_uring_wait_cqe(&g_ring, &cqe) < 0)
            return 0;
        woken += reap_one(cqe);
    }
    /* Drain any further completions without blocking. */
    while (io_uring_peek_cqe(&g_ring, &cqe) == 0)
        woken += reap_one(cqe);

    return woken;
}
