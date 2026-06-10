/* echo_server - a coroutine-per-connection TCP echo server.
 *
 * Demonstrates the coro library end to end: every accept, read and write goes
 * through the library's io_uring-backed wrappers, so the single OS thread never
 * blocks on I/O. The server prints periodic statistics and shuts down cleanly
 * on SIGINT, draining in-flight connections instead of dropping them.
 *
 * Build:  make echo_server
 * Run:    ./echo_server -p 8080
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "coro.h"
#include "io.h"

/* Per-connection stack: small, because the echo buffer is heap-allocated. */
#define CONN_STACK (64UL * 1024)
#define BUF_SIZE 16384
#define STATS_INTERVAL_MS 5000 /* print stats every 5 s */
#define TICK_MS 200            /* supervisor wakeup granularity */

/* Active connections, tracked so shutdown can drain them. Each node lives on
   its handler coroutine's stack and is valid while that coroutine is alive. */
typedef struct conn {
    int fd;
    struct conn *prev;
    struct conn *next;
} conn_t;

static conn_t *g_conns; /* head of the active-connection list */
static int g_active;    /* number of active connection coroutines */
static int g_listen_fd = -1;

static volatile sig_atomic_t g_shutdown; /* set by SIGINT handler */

/* Counters. Single-threaded, so plain ints need no synchronization. */
static struct {
    uint64_t accepted, closed;
    uint64_t bytes_rx, bytes_tx;
    uint64_t win_bytes;  /* rx+tx since last stats print (for throughput) */
    uint64_t lat_sum_us; /* sum of echo latencies since last print */
    uint64_t lat_count;  /* number of latency samples since last print */
} g_stats;

static void on_sigint(int sig) {
    (void)sig;
    g_shutdown = 1; /* noticed by the supervisor within one tick */
}

static uint64_t now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ull + (uint64_t)ts.tv_nsec / 1000;
}

static void conn_register(conn_t *c) {
    c->prev = NULL;
    c->next = g_conns;
    if (g_conns)
        g_conns->prev = c;
    g_conns = c;
    g_active++;
}

static void conn_unregister(conn_t *c) {
    if (c->prev)
        c->prev->next = c->next;
    else
        g_conns = c->next;
    if (c->next)
        c->next->prev = c->prev;
    g_active--;
}

/* One coroutine per connection: echo bytes until EOF or error. */
static void echo_conn(void *arg) {
    int fd = (int)(intptr_t)arg;
    conn_t self = {.fd = fd};
    conn_register(&self);

    char *buf = malloc(BUF_SIZE);
    if (buf) {
        for (;;) {
            int n = coro_read(fd, buf, BUF_SIZE);
            if (n <= 0)
                break; /* peer closed (0), shutdown, or error (<0) */

            uint64_t t0 = now_us();
            g_stats.bytes_rx += (uint64_t)n;
            g_stats.win_bytes += (uint64_t)n;

            int off = 0;
            while (off < n) {
                int w = coro_write(fd, buf + off, (size_t)(n - off));
                if (w <= 0)
                    goto done;
                off += w;
                g_stats.bytes_tx += (uint64_t)w;
                g_stats.win_bytes += (uint64_t)w;
            }
            /* Echo latency: read completion -> write completion for this chunk.
             */
            g_stats.lat_sum_us += now_us() - t0;
            g_stats.lat_count++;
        }
    }
done:
    free(buf);
    close(fd);
    conn_unregister(&self);
    g_stats.closed++;
}

/* Accept loop: spawn a handler per connection. */
static void acceptor(void *arg) {
    int lfd = (int)(intptr_t)arg;
    while (!g_shutdown) {
        int cfd = coro_accept(lfd, NULL, NULL);
        if (g_shutdown) { /* woken to stop accepting */
            if (cfd >= 0)
                close(cfd);
            break;
        }
        if (cfd < 0) {
            fprintf(stderr, "accept: %s\n", strerror(-cfd));
            break;
        }
        int one = 1;
        setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
        g_stats.accepted++;
        if (!coro_create(echo_conn, (void *)(intptr_t)cfd, CONN_STACK))
            close(cfd); /* out of memory: drop the connection */
    }
}

static void print_stats(uint64_t window_us) {
    double secs = (double)window_us / 1e6;
    double mbps = secs > 0 ? ((double)g_stats.win_bytes / 1e6) / secs : 0.0;
    double avg_lat = g_stats.lat_count ? (double)g_stats.lat_sum_us /
                                             (double)g_stats.lat_count
                                       : 0.0;
    fprintf(stderr,
            "[stats] accepted=%" PRIu64 " closed=%" PRIu64 " active=%d | "
            "rx=%" PRIu64 "KB tx=%" PRIu64 "KB | "
            "%.2f MB/s | avg echo latency %.1f us\n",
            g_stats.accepted, g_stats.closed, g_active, g_stats.bytes_rx / 1024,
            g_stats.bytes_tx / 1024, mbps, avg_lat);

    g_stats.win_bytes = 0;
    g_stats.lat_sum_us = 0;
    g_stats.lat_count = 0;
}

/* Prints stats on a timer and orchestrates graceful shutdown. */
static void supervisor(void *arg) {
    (void)arg;
    uint64_t last = now_us();

    while (!g_shutdown) {
        coro_sleep(TICK_MS);
        uint64_t t = now_us();
        if (t - last >= (uint64_t)STATS_INTERVAL_MS * 1000ull) {
            print_stats(t - last);
            last = t;
        }
    }

    /* Drain: unblock the acceptor's pending accept and every parked read.
       shutdown() makes those io_uring operations complete (with error/EOF), so
       the affected coroutines wake, run to completion and free themselves. */
    fprintf(stderr, "\n[shutdown] draining %d active connection(s)...\n",
            g_active);
    if (g_listen_fd >= 0)
        shutdown(g_listen_fd, SHUT_RDWR);
    for (conn_t *c = g_conns; c; c = c->next)
        shutdown(c->fd, SHUT_RDWR);
}

static int make_listener(int port) {
    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) {
        perror("socket");
        return -1;
    }
    int opt = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof opt);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)port);

    if (bind(lfd, (struct sockaddr *)&addr, sizeof addr) < 0) {
        perror("bind");
        close(lfd);
        return -1;
    }
    if (listen(lfd, 1024) < 0) {
        perror("listen");
        close(lfd);
        return -1;
    }
    return lfd;
}

int main(int argc, char **argv) {
    int port = 8080;
    int opt;
    while ((opt = getopt(argc, argv, "p:h")) != -1) {
        switch (opt) {
        case 'p':
            port = atoi(optarg);
            if (port <= 0 || port > 65535) {
                fprintf(stderr, "invalid port: %s\n", optarg);
                return 1;
            }
            break;
        case 'h':
            printf("usage: %s [-p port]   (default port 8080)\n", argv[0]);
            return 0;
        default:
            fprintf(stderr, "usage: %s [-p port]\n", argv[0]);
            return 1;
        }
    }

    /* io_uring write to a closed socket reports -EPIPE; don't also raise the
       SIGPIPE signal. SIGINT triggers graceful shutdown. */
    signal(SIGPIPE, SIG_IGN);
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = on_sigint; /* no SA_RESTART: wakes a blocked wait */
    sigaction(SIGINT, &sa, NULL);

    g_listen_fd = make_listener(port);
    if (g_listen_fd < 0)
        return 1;

    printf("echo_server listening on port %d (pid %d) - Ctrl-C to stop\n", port,
           (int)getpid());
    fflush(stdout);

    if (!coro_create(supervisor, NULL, 0) ||
        !coro_create(acceptor, (void *)(intptr_t)g_listen_fd, 0)) {
        fprintf(stderr, "failed to create startup coroutines\n");
        close(g_listen_fd);
        return 1;
    }
    coro_run();

    close(g_listen_fd);
    printf("[shutdown] complete: accepted=%" PRIu64 " closed=%" PRIu64
           " rx=%" PRIu64 "B tx=%" PRIu64 "B\n",
           g_stats.accepted, g_stats.closed, g_stats.bytes_rx,
           g_stats.bytes_tx);
    return 0;
}
