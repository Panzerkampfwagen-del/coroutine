/* io_uring backend: submit-and-park I/O for coroutines.
 *
 * Ported from the full-scale sibling with two deliberate simplifications:
 * user_data carries the coroutine POINTER (not a synthetic id), and the
 * pending set is one intrusive list threaded through coro::next. Both are
 * safe for the same reason: a coroutine parked on I/O sits on no other
 * queue, so the link is ours to reuse until the completion wakes it.
 *
 * The scheduler sees only coro_io_pending()/coro_io_reap(); without this
 * translation unit linked, weak defaults in coro.c keep every build working.
 */
#define _GNU_SOURCE
#include "coro_internal.h"
#include "io.h"

#include <errno.h>
#include <liburing.h>

#define IO_DEPTH 64 /* submission/completion queue depth */

static struct io_uring g_ring;
static int g_ring_ready;
static int g_pending_count;
static coro_t *g_pending; /* intrusive singly-linked list of waiters */

static int io_init(void)
{
    if (g_ring_ready)
        return 0;
    int rc = io_uring_queue_init(IO_DEPTH, &g_ring, 0);
    if (rc < 0)
        return rc; /* -errno */
    g_ring_ready = 1;
    return 0;
}

/* Fetch a submission queue entry, flushing once if the queue is full. */
static struct io_uring_sqe *io_get_sqe(void)
{
    struct io_uring_sqe *sqe = io_uring_get_sqe(&g_ring);
    if (!sqe) {
        io_uring_submit(&g_ring);
        sqe = io_uring_get_sqe(&g_ring);
    }
    return sqe;
}

/* Tag the request with the caller's pointer, submit, park until harvested. */
static int io_submit_and_wait(struct io_uring_sqe *sqe)
{
    coro_t *self = coro_current();
    io_uring_sqe_set_data(sqe, self);

    int rc = io_uring_submit(&g_ring);
    if (rc < 0)
        return rc; /* -errno; nothing was queued */

    self->next = g_pending;
    g_pending = self;
    g_pending_count++;

    coro_park();               /* resumes via coro_io_reap -> runq_wake */
    return self->io_result;
}

int coro_io_probe(void) { return io_init(); }

int coro_read(int fd, void *buf, size_t len)
{
    int rc = io_init();
    if (rc < 0)
        return rc;
    struct io_uring_sqe *sqe = io_get_sqe();
    if (!sqe)
        return -EBUSY;
    io_uring_prep_read(sqe, fd, buf, (unsigned)len, (uint64_t)-1);
    return io_submit_and_wait(sqe);
}

int coro_write(int fd, const void *buf, size_t len)
{
    int rc = io_init();
    if (rc < 0)
        return rc;
    struct io_uring_sqe *sqe = io_get_sqe();
    if (!sqe)
        return -EBUSY;
    io_uring_prep_write(sqe, fd, buf, (unsigned)len, (uint64_t)-1);
    return io_submit_and_wait(sqe);
}

int coro_accept(int fd, struct sockaddr *addr, socklen_t *addrlen)
{
    int rc = io_init();
    if (rc < 0)
        return rc;
    struct io_uring_sqe *sqe = io_get_sqe();
    if (!sqe)
        return -EBUSY;
    io_uring_prep_accept(sqe, fd, addr, addrlen, 0);
    return io_submit_and_wait(sqe);
}

int coro_connect(int fd, const struct sockaddr *addr, socklen_t addrlen)
{
    int rc = io_init();
    if (rc < 0)
        return rc;
    struct io_uring_sqe *sqe = io_get_sqe();
    if (!sqe)
        return -EBUSY;
    io_uring_prep_connect(sqe, fd, addr, addrlen);
    return io_submit_and_wait(sqe);
}

int coro_sleep(uint64_t ms)
{
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
    /* The kernel copies ts at submission, so a stack copy is fine. An elapsed
       timeout completes with -ETIME, which callers treat as success. */
    io_uring_prep_timeout(sqe, &ts, 0, 0);
    io_submit_and_wait(sqe);
    return 0;
}

int coro_io_pending(void) { return g_pending_count > 0; }

static void reap_one(struct io_uring_cqe *cqe)
{
    coro_t *c = (coro_t *)io_uring_cqe_get_data(cqe);
    /* The waiter must still be on the pending list: it parks only after the
       SQE is submitted, so a completion can never precede registration. */
    coro_t **link = &g_pending;
    while (*link && *link != c)
        link = &(*link)->next;
    if (*link) {
        *link = c->next;
        c->next = NULL;
        g_pending_count--;
        c->io_result = cqe->res;
        runq_wake(c);
    }
    io_uring_cqe_seen(&g_ring, cqe);
}

int coro_io_reap(int block)
{
    if (!g_ring_ready || g_pending_count == 0)
        return 0;

    struct io_uring_cqe *cqe;
    int woken = 0;

    if (block) {
        if (io_uring_wait_cqe(&g_ring, &cqe) < 0)
            return 0;
        reap_one(cqe);
        woken++;
    }
    while (io_uring_peek_cqe(&g_ring, &cqe) == 0) {
        reap_one(cqe);
        woken++;
    }
    return woken;
}
