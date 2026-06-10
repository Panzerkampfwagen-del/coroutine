#ifndef IO_H
#define IO_H

#include <stddef.h>
#include <stdint.h>
#include <sys/socket.h>

/**
 * @file io.h
 * @brief Coroutine-aware I/O backed by a single shared io_uring instance.
 *
 * Each call submits one request, parks the calling coroutine, and resumes it
 * once the completion is harvested by the scheduler. Other ready coroutines run
 * in the meantime, so these calls are non-blocking with respect to the whole
 * program while looking synchronous to the caller.
 *
 * The read/write/accept calls return the kernel result of the operation: a byte
 * count (>= 0), a new file descriptor for accept, or a negative errno on
 * failure.
 */

/**
 * @brief Check whether the io_uring backend is usable in this environment.
 *
 * Initializes the shared ring on success, so later I/O reuses it. This lets a
 * program fail fast with a clear message where io_uring is unavailable - a
 * kernel with `kernel.io_uring_disabled` set, or a restrictive sandbox - rather
 * than appear to start and then never make progress.
 *
 * @return 0 if io_uring is available, or a negative errno if it is not.
 */
int coro_io_probe(void);

/**
 * @brief Read from a file descriptor (read(2)/recv semantics).
 * @param fd   File descriptor to read from.
 * @param buf  Destination buffer.
 * @param len  Maximum number of bytes to read.
 * @return Bytes read (0 at EOF), or a negative errno.
 */
int coro_read(int fd, void *buf, size_t len);

/**
 * @brief Write to a file descriptor (write(2)/send semantics).
 * @param fd   File descriptor to write to.
 * @param buf  Source buffer.
 * @param len  Maximum number of bytes to write.
 * @return Bytes written, or a negative errno.
 */
int coro_write(int fd, const void *buf, size_t len);

/**
 * @brief Accept one connection on a listening socket.
 * @param fd        Listening socket.
 * @param addr      Optional output for the peer address (may be NULL).
 * @param addrlen   In/out length for @p addr (may be NULL).
 * @return A new connected socket descriptor, or a negative errno.
 */
int coro_accept(int fd, struct sockaddr *addr, socklen_t *addrlen);

/**
 * @brief Connect a socket to a peer (connect(2) semantics).
 *
 * The complement of coro_accept: lets a coroutine act as a client, dialling out
 * to another server while the rest of the program keeps running. Used by
 * microdb's replica link and sharding proxy.
 *
 * @param fd        A socket created with socket(2) (need not be non-blocking).
 * @param addr      Peer address to connect to.
 * @param addrlen   Length of @p addr.
 * @return 0 on success, or a negative errno on failure.
 */
int coro_connect(int fd, const struct sockaddr *addr, socklen_t addrlen);

/**
 * @brief Suspend the calling coroutine for at least @p ms milliseconds.
 *
 * Other coroutines run while this one sleeps. Backed by an io_uring timeout.
 *
 * @param ms  Minimum sleep duration in milliseconds.
 * @return 0 on success, or a negative errno if the timeout could not be queued.
 */
int coro_sleep(uint64_t ms);

#endif /* IO_H */
