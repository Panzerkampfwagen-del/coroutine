/*
 * echo_server -- single-threaded coroutine-per-connection TCP echo server.
 *
 * Sockets are O_NONBLOCK; a read/write that would block returns EAGAIN and
 * the connection coroutine yields, letting other connections make progress.
 * This is the cooperative-scheduling analog of what io_uring integration
 * gives the full project.
 *
 * Build:  make
 * Run:    ./build/echo_server [port]
 * Try:    nc 127.0.0.1 8080   (type, see it echoed)
 */
#define _GNU_SOURCE
#include "../include/coro.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int set_nonblock(int fd)
{
    int fl = fcntl(fd, F_GETFL, 0);
    return fcntl(fd, F_SETFL, fl | O_NONBLOCK);
}

static ssize_t coro_read(int fd, void *buf, size_t n)
{
    for (;;) {
        ssize_t r = read(fd, buf, n);
        if (r >= 0) return r;
        if (errno == EAGAIN || errno == EWOULDBLOCK) { coro_yield(); continue; }
        return -1;
    }
}

static ssize_t coro_write_all(int fd, const void *buf, size_t n)
{
    const char *p = buf;
    while (n > 0) {
        ssize_t r = write(fd, p, n);
        if (r > 0) { p += r; n -= (size_t)r; continue; }
        if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            coro_yield(); continue;
        }
        return -1;
    }
    return 0;
}

static void handle_conn(void *arg)
{
    int fd = (int)(intptr_t)arg;
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    char buf[4096];
    for (;;) {
        ssize_t r = coro_read(fd, buf, sizeof(buf));
        if (r <= 0) break;                    /* EOF or error */
        if (coro_write_all(fd, buf, (size_t)r) < 0) break;
    }
    close(fd);
}

static void acceptor(void *arg)
{
    int lfd = (int)(intptr_t)arg;

    for (;;) {
        int cfd = accept(lfd, NULL, NULL);
        if (cfd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) { coro_yield(); continue; }
            perror("accept");
            return;
        }
        set_nonblock(cfd);
        coro_create(handle_conn, (void *)(intptr_t)cfd);
    }
}

int main(int argc, char **argv)
{
    int port = 8080;
    if (argc > 1) {
        char *end = NULL;
        long v = strtol(argv[1], &end, 10);
        if (!argv[1][0] || (end && *end) || v < 1 || v > 65535) {
            fprintf(stderr, "usage: %s [port 1-65535]\n", argv[0]);
            return 1;
        }
        port = (int)v;
    }

    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) { perror("socket"); return 1; }

    int one = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr = {0};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port        = htons((uint16_t)port);
    if (bind(lfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); return 1;
    }
    if (listen(lfd, 512) < 0) { perror("listen"); return 1; }
    set_nonblock(lfd);

    fprintf(stderr, "echo_server: listening on 127.0.0.1:%d\n", port);

    coro_init();
    coro_create(acceptor, (void *)(intptr_t)lfd);
    coro_run();          /* returns only if the acceptor dies on a fatal
                            accept error (non-EAGAIN); see acceptor() */
    return 0;
}
