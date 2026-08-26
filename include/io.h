#ifndef CORO_IO_H
#define CORO_IO_H

#include <stddef.h>
#include <stdint.h>
#include <sys/socket.h>

/**
 * Coroutine-aware I/O backed by one shared io_uring instance.
 *
 * Each call submits one request, parks the calling coroutine, and the
 * scheduler resumes it when the completion is harvested. Other ready
 * coroutines run meanwhile, so calls are non-blocking to the program while
 * looking synchronous to the caller.
 *
 * The scheduler integrates through two hooks (weakly stubbed in coro.c and
 * overridden here): coro_io_pending() lets coro_run() keep servicing parked
 * I/O after the run queue drains, and coro_io_reap() harvests completions.
 */

/* 0 if io_uring works here (initializes the shared ring); else -errno.
   Call early to fail fast where io_uring is disabled or sandboxed. */
int coro_io_probe(void);

int coro_read(int fd, void *buf, size_t len);            /* read(2) semantics */
int coro_write(int fd, const void *buf, size_t len);     /* write(2)          */
int coro_accept(int fd, struct sockaddr *addr, socklen_t *addrlen);
int coro_connect(int fd, const struct sockaddr *addr, socklen_t addrlen);

/* Suspend the caller for at least ms milliseconds; other coroutines run. */
int coro_sleep(uint64_t ms);

#endif /* CORO_IO_H */
