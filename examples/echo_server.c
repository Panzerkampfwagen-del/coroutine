/*
 * echo_server -- single-threaded coroutine-per-connection TCP echo server,
 * with two interchangeable I/O backends selected at startup:
 *
 *   io_uring    (default where available)  accept/read/write submit to a shared
 *               ring and PARK the calling coroutine; the scheduler resumes it
 *               when the completion is harvested. Sockets stay blocking; the
 *               kernel waits, the coroutine sleeps, other connections run.
 *   nonblock    EAGAIN-driven retry-and-yield on O_NONBLOCK sockets -- the
 *               portable fallback, and what the runtime used before the
 *               io_uring layer landed.
 *
 * Build:  make            (io_uring backend included when liburing exists)
 * Run:    ./build/echo_server [-p port] [--no-uring]
 * Try:    nc 127.0.0.1 8080   (type, see it echoed)
 * Bench:  python3 benchmarks/bench.py --spawn build/echo_server --conns 1000
 */
#define _GNU_SOURCE
#include "../include/coro.h"
#ifdef CORO_HAVE_IO_URING
#include "../include/io.h"
#endif

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int g_backend_uring; /* set in main() once the backend is chosen */

static int set_nonblock(int fd)
{
    int fl = fcntl(fd, F_GETFL, 0);
    return fcntl(fd, F_SETFL, fl | O_NONBLOCK);
}

#ifdef CORO_HAVE_IO_URING
/* ------------------------------------------------ io_uring backend ------ */

static int ur_write_all(int fd, const void *buf, size_t n)
{
    const char *p = buf;
    while (n > 0) {
        int r = coro_write(fd, p, n);
        if (r <= 0)
            return -1;
        p += r;
        n -= (size_t)r;
    }
    return 0;
}

static void handle_conn_ur(void *arg)
{
    int fd = (int)(intptr_t)arg;
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    char buf[4096];
    for (;;) {
        int r = coro_read(fd, buf, sizeof(buf));
        if (r <= 0)
            break;                         /* EOF or error */
        if (ur_write_all(fd, buf, (size_t)r) < 0)
            break;
    }
    close(fd);
}

static void acceptor_ur(void *arg)
{
    int lfd = (int)(intptr_t)arg;
    for (;;) {
        int cfd = coro_accept(lfd, NULL, NULL);
        if (cfd < 0) {
            perror("coro_accept");
            return;
        }
        coro_create(handle_conn_ur, (void *)(intptr_t)cfd);
    }
}
#endif /* CORO_HAVE_IO_URING */

/* ---------------------------------------------- non-blocking backend ---- */

static ssize_t nb_read(int fd, void *buf, size_t n)
{
    for (;;) {
        ssize_t r = read(fd, buf, n);
        if (r >= 0)
            return r;
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            coro_yield();
            continue;
        }
        return -1;
    }
}

static int nb_write_all(int fd, const void *buf, size_t n)
{
    const char *p = buf;
    while (n > 0) {
        ssize_t r = write(fd, p, n);
        if (r > 0) {
            p += r;
            n -= (size_t)r;
            continue;
        }
        if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            coro_yield();
            continue;
        }
        return -1;
    }
    return 0;
}

static void handle_conn_nb(void *arg)
{
    int fd = (int)(intptr_t)arg;
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    char buf[4096];
    for (;;) {
        ssize_t r = nb_read(fd, buf, sizeof(buf));
        if (r <= 0)
            break;
        if (nb_write_all(fd, buf, (size_t)r) < 0)
            break;
    }
    close(fd);
}

static void acceptor_nb(void *arg)
{
    int lfd = (int)(intptr_t)arg;
    for (;;) {
        int cfd = accept(lfd, NULL, NULL);
        if (cfd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                coro_yield();
                continue;
            }
            perror("accept");
            return;
        }
        set_nonblock(cfd);
        coro_create(handle_conn_nb, (void *)(intptr_t)cfd);
    }
}

/* --------------------------------------------------------------- main --- */

int main(int argc, char **argv)
{
    int port = 8080;
    int force_nb = 0;

    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "-p") && i + 1 < argc) {
            port = atoi(argv[++i]);
        } else if (!strcmp(argv[i], "--no-uring")) {
            force_nb = 1;
        } else {
            char *end = NULL;
            long v = strtol(argv[i], &end, 10);
            if (!argv[i][0] || (end && *end) || v < 1 || v > 65535) {
                fprintf(stderr, "usage: %s [-p port] [--no-uring]\n", argv[0]);
                return 1;
            }
            port = (int)v;
        }
    }

#ifdef CORO_HAVE_IO_URING
    g_backend_uring = !force_nb && (coro_io_probe() == 0);
#else
    g_backend_uring = 0;
    (void)force_nb;
#endif

    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) {
        perror("socket");
        return 1;
    }
    int one = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr = {0};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port        = htons((uint16_t)port);
    if (bind(lfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        return 1;
    }
    if (listen(lfd, 512) < 0) {
        perror("listen");
        return 1;
    }
    if (!g_backend_uring)
        set_nonblock(lfd);

    printf("echo_server: listening on 127.0.0.1:%d (%s backend)\n", port,
           g_backend_uring ? "io_uring" : "nonblock");
    fflush(stdout);

    coro_init();
    if (g_backend_uring) {
#ifdef CORO_HAVE_IO_URING
        coro_create(acceptor_ur, (void *)(intptr_t)lfd);
#endif
    } else {
        coro_create(acceptor_nb, (void *)(intptr_t)lfd);
    }
    coro_run();
    return 0;
}
