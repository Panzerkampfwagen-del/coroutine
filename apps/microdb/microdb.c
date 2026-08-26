/* microdb -- a Redis-compatible string store on the coro runtime.
 *
 * Core-only port from the full-scale sibling: keyspace, command execution,
 * and coroutine-per-connection serving over the io_uring I/O layer. The
 * sibling's replication engine and sharding proxy (~1,600 lines) are
 * deliberately omitted here; see that repo for them.
 *
 * Supported commands: PING, ECHO, SET (EX/PX/EXAT/PXAT), GET, GETSET, DEL,
 * EXISTS, INCR/DECR/INCRBY, APPEND, STRLEN, MGET/MSET, EXPIRE/PEXPIREAT,
 * TTL, PERSIST, TYPE, DBSIZE, FLUSHDB/FLUSHALL, KEYS *, plus the stubs
 * real clients probe with (SELECT/COMMAND/CONFIG/INFO). Speaks RESP multibulk
 * and inline protocols.
 */
#define _GNU_SOURCE
#include "../../include/coro.h"
#include "../../include/io.h"
#include "resp.h"

#include <arpa/inet.h>
#include <errno.h>
#include <getopt.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <time.h>
#include <unistd.h>

/* ------------------------------------------------------------------------- *
 * Keyspace: a chained hash table of string -> string with optional expiry.
 * ------------------------------------------------------------------------- */
typedef struct entry {
    struct entry *next;
    uint64_t expire_ms; /* 0 = persistent */
    size_t klen, vlen;
    char *key;
    char *val;
} entry;

static entry **g_buckets;
static size_t g_nbuckets;
static size_t g_count;

static uint64_t now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)ts.tv_nsec / 1000000ull;
}

/* Single liveness predicate so DBSIZE, KEYS, and lookups can never disagree
   about expired-but-not-yet-evicted entries. */
static int entry_is_expired(const entry *e, uint64_t now)
{
    return e->expire_ms && now >= e->expire_ms;
}

static void store_init(size_t nbuckets)
{
    g_nbuckets = nbuckets;
    g_buckets = calloc(nbuckets, sizeof(entry *));
}

static void store_rehash(void)
{
    size_t n = g_nbuckets * 2;
    entry **nb = calloc(n, sizeof(entry *));
    if (!nb)
        return;
    for (size_t i = 0; i < g_nbuckets; i++) {
        for (entry *e = g_buckets[i]; e;) {
            entry *next = e->next;
            size_t b = fnv1a(e->key, e->klen) & (n - 1);
            e->next = nb[b];
            nb[b] = e;
            e = next;
        }
    }
    free(g_buckets);
    g_buckets = nb;
    g_nbuckets = n;
}

static entry *entry_find(const char *key, size_t klen, entry ***slot)
{
    size_t b = fnv1a(key, klen) & (g_nbuckets - 1);
    entry **link = &g_buckets[b];
    for (entry *e = *link; e; link = &e->next, e = e->next) {
        if (e->klen == klen && memcmp(e->key, key, klen) == 0) {
            if (slot)
                *slot = link;
            return e;
        }
    }
    if (slot)
        *slot = link;
    return NULL;
}

static void entry_unlink_free(entry **link, entry *e)
{
    *link = e->next;
    free(e->key);
    free(e->val);
    free(e);
    g_count--;
}

/* Look up a live key, lazily expiring it if its TTL has passed. */
static entry *store_get(const char *key, size_t klen)
{
    entry **link;
    entry *e = entry_find(key, klen, &link);
    if (!e)
        return NULL;
    if (entry_is_expired(e, now_ms())) {
        entry_unlink_free(link, e);
        return NULL;
    }
    return e;
}

static void store_set(const char *key, size_t klen, const char *val,
                      size_t vlen, uint64_t expire_ms)
{
    entry **link;
    entry *e = entry_find(key, klen, &link);
    if (e) {
        char *nv = malloc(vlen ? vlen : 1);
        if (!nv)
            return;
        memcpy(nv, val, vlen);
        free(e->val);
        e->val = nv;
        e->vlen = vlen;
        e->expire_ms = expire_ms;
        return;
    }
    e = calloc(1, sizeof *e);
    if (!e)
        return;
    e->key = malloc(klen ? klen : 1);
    e->val = malloc(vlen ? vlen : 1);
    if (!e->key || !e->val) {
        free(e->key);
        free(e->val);
        free(e);
        return;
    }
    memcpy(e->key, key, klen);
    memcpy(e->val, val, vlen);
    e->klen = klen;
    e->vlen = vlen;
    e->expire_ms = expire_ms;
    e->next = *link;
    *link = e;
    g_count++;
    if (g_count > g_nbuckets) /* load factor > 1 */
        store_rehash();
}

static int store_del(const char *key, size_t klen)
{
    entry **link;
    entry *e = entry_find(key, klen, &link);
    if (!e)
        return 0;
    /* An expired-but-unevicted key is logically absent: DEL counts it as a
       miss (same predicate as store_get). */
    int was_live = !entry_is_expired(e, now_ms());
    entry_unlink_free(link, e);
    return was_live;
}

static void store_flush(void)
{
    for (size_t i = 0; i < g_nbuckets; i++) {
        for (entry *e = g_buckets[i]; e;) {
            entry *next = e->next;
            free(e->key);
            free(e->val);
            free(e);
            e = next;
        }
        g_buckets[i] = NULL;
    }
    g_count = 0;
}

/* ------------------------------------------------------------------------- *
 * Command execution.
 * ------------------------------------------------------------------------- */
static struct {
    uint64_t commands;
    uint64_t connections;
    uint64_t active;
} g_stats;

/* Parse an arg as a base-10 long long. Returns 1 on success. */
static int arg_to_ll(const arg_t *a, long long *out)
{
    if (a->len == 0 || a->len > 20)
        return 0;
    char tmp[24];
    memcpy(tmp, a->ptr, a->len);
    tmp[a->len] = 0;
    char *end;
    errno = 0;
    long long v = strtoll(tmp, &end, 10);
    if (errno || *end != 0)
        return 0;
    *out = v;
    return 1;
}

static int do_incrby(buf_t *o, const arg_t *key, long long delta)
{
    entry *e = store_get(key->ptr, key->len);
    long long cur = 0;
    if (e) {
        arg_t a = {e->val, e->vlen};
        if (!arg_to_ll(&a, &cur)) {
            reply_error(o, "ERR value is not an integer or out of range");
            return 0;
        }
    }
    cur += delta;
    char tmp[24];
    int k = snprintf(tmp, sizeof tmp, "%lld", cur);
    uint64_t exp = e ? e->expire_ms : 0;
    store_set(key->ptr, key->len, tmp, (size_t)k, exp);
    reply_int(o, cur);
    return 1;
}

/* Execute one parsed command, appending the RESP reply to o.
   Returns 1 normally, 0 if the connection should close (QUIT). */
static int exec_cmd(arg_t *argv, int argc, buf_t *o)
{
    g_stats.commands++;
    const arg_t *cmd = &argv[0];

    if (arg_eq(cmd, "ping")) {
        if (argc >= 2)
            reply_bulk(o, argv[1].ptr, argv[1].len);
        else
            reply_simple(o, "PONG");
    } else if (arg_eq(cmd, "echo") && argc == 2) {
        reply_bulk(o, argv[1].ptr, argv[1].len);
    } else if (arg_eq(cmd, "set") && argc >= 3) {
        uint64_t exp = 0;
        for (int i = 3; i + 1 < argc; i += 2) {
            long long n;
            if (arg_eq(&argv[i], "ex") && arg_to_ll(&argv[i + 1], &n))
                exp = now_ms() + (uint64_t)n * 1000;
            else if (arg_eq(&argv[i], "px") && arg_to_ll(&argv[i + 1], &n))
                exp = now_ms() + (uint64_t)n;
            else if (arg_eq(&argv[i], "exat") && arg_to_ll(&argv[i + 1], &n))
                exp = (uint64_t)n * 1000;
            else if (arg_eq(&argv[i], "pxat") && arg_to_ll(&argv[i + 1], &n))
                exp = (uint64_t)n;
        }
        store_set(argv[1].ptr, argv[1].len, argv[2].ptr, argv[2].len, exp);
        reply_simple(o, "OK");
    } else if (arg_eq(cmd, "get") && argc == 2) {
        entry *e = store_get(argv[1].ptr, argv[1].len);
        if (e)
            reply_bulk(o, e->val, e->vlen);
        else
            reply_nil(o);
    } else if (arg_eq(cmd, "getset") && argc == 3) {
        entry *e = store_get(argv[1].ptr, argv[1].len);
        if (e)
            reply_bulk(o, e->val, e->vlen);
        else
            reply_nil(o);
        store_set(argv[1].ptr, argv[1].len, argv[2].ptr, argv[2].len, 0);
    } else if (arg_eq(cmd, "del")) {
        long long removed = 0;
        for (int i = 1; i < argc; i++)
            removed += store_del(argv[i].ptr, argv[i].len);
        reply_int(o, removed);
    } else if (arg_eq(cmd, "exists")) {
        long long found = 0;
        for (int i = 1; i < argc; i++)
            found += store_get(argv[i].ptr, argv[i].len) != NULL;
        reply_int(o, found);
    } else if (arg_eq(cmd, "incr") && argc == 2) {
        do_incrby(o, &argv[1], 1);
    } else if (arg_eq(cmd, "decr") && argc == 2) {
        do_incrby(o, &argv[1], -1);
    } else if (arg_eq(cmd, "incrby") && argc == 3) {
        long long d;
        if (arg_to_ll(&argv[2], &d)) {
            do_incrby(o, &argv[1], d);
        } else {
            reply_error(o, "ERR value is not an integer or out of range");
        }
    } else if (arg_eq(cmd, "append") && argc == 3) {
        entry *e = store_get(argv[1].ptr, argv[1].len);
        if (!e) {
            store_set(argv[1].ptr, argv[1].len, argv[2].ptr, argv[2].len, 0);
            reply_int(o, (long long)argv[2].len);
        } else {
            size_t nl = e->vlen + argv[2].len;
            char *nv = malloc(nl ? nl : 1);
            if (nv) {
                memcpy(nv, e->val, e->vlen);
                memcpy(nv + e->vlen, argv[2].ptr, argv[2].len);
                free(e->val);
                e->val = nv;
                e->vlen = nl;
            }
            reply_int(o, (long long)nl);
        }
    } else if (arg_eq(cmd, "strlen") && argc == 2) {
        entry *e = store_get(argv[1].ptr, argv[1].len);
        reply_int(o, e ? (long long)e->vlen : 0);
    } else if (arg_eq(cmd, "mget")) {
        reply_array_header(o, argc - 1);
        for (int i = 1; i < argc; i++) {
            entry *e = store_get(argv[i].ptr, argv[i].len);
            if (e)
                reply_bulk(o, e->val, e->vlen);
            else
                reply_nil(o);
        }
    } else if (arg_eq(cmd, "mset") && argc >= 3 && !(argc & 1)) {
        for (int i = 1; i + 1 < argc; i += 2)
            store_set(argv[i].ptr, argv[i].len, argv[i + 1].ptr,
                      argv[i + 1].len, 0);
        reply_simple(o, "OK");
    } else if (arg_eq(cmd, "expire") && argc == 3) {
        long long sec;
        entry *e = store_get(argv[1].ptr, argv[1].len);
        if (e && arg_to_ll(&argv[2], &sec)) {
            e->expire_ms = now_ms() + (uint64_t)sec * 1000;
            reply_int(o, 1);
        } else {
            reply_int(o, 0);
        }
    } else if (arg_eq(cmd, "pexpireat") && argc == 3) {
        long long ms;
        entry *e = store_get(argv[1].ptr, argv[1].len);
        if (e && arg_to_ll(&argv[2], &ms)) {
            e->expire_ms = (uint64_t)ms;
            reply_int(o, 1);
        } else {
            reply_int(o, 0);
        }
    } else if (arg_eq(cmd, "ttl") && argc == 2) {
        entry *e = store_get(argv[1].ptr, argv[1].len);
        if (!e)
            reply_int(o, -2);
        else if (!e->expire_ms)
            reply_int(o, -1);
        else {
            uint64_t now = now_ms();
            reply_int(o, e->expire_ms > now
                             ? (long long)((e->expire_ms - now + 999) / 1000)
                             : 0);
        }
    } else if (arg_eq(cmd, "persist") && argc == 2) {
        entry *e = store_get(argv[1].ptr, argv[1].len);
        if (e && e->expire_ms) {
            e->expire_ms = 0;
            reply_int(o, 1);
        } else {
            reply_int(o, 0);
        }
    } else if (arg_eq(cmd, "type") && argc == 2) {
        entry *e = store_get(argv[1].ptr, argv[1].len);
        reply_simple(o, e ? "string" : "none");
    } else if (arg_eq(cmd, "dbsize")) {
        /* Live count, not raw g_count: expired-but-unevicted entries must not
           make DBSIZE disagree with KEYS *. */
        uint64_t now = now_ms();
        long long live = 0;
        for (size_t i = 0; i < g_nbuckets; i++)
            for (entry *e = g_buckets[i]; e; e = e->next)
                if (!entry_is_expired(e, now))
                    live++;
        reply_int(o, live);
    } else if (arg_eq(cmd, "flushdb") || arg_eq(cmd, "flushall")) {
        store_flush();
        reply_simple(o, "OK");
    } else if (arg_eq(cmd, "keys") && argc == 2) {
        /* Only the "*" (match everything) pattern is supported. Count live
           keys first so the RESP array header matches what we emit -- an
           over-count leaves clients waiting for elements that never come. */
        if (argv[1].len == 1 && argv[1].ptr[0] == '*') {
            uint64_t now = now_ms();
            long long live = 0;
            for (size_t i = 0; i < g_nbuckets; i++)
                for (entry *e = g_buckets[i]; e; e = e->next)
                    if (!entry_is_expired(e, now))
                        live++;
            reply_array_header(o, live);
            for (size_t i = 0; i < g_nbuckets; i++)
                for (entry *e = g_buckets[i]; e; e = e->next)
                    if (!entry_is_expired(e, now))
                        reply_bulk(o, e->key, e->klen);
        } else {
            reply_array_header(o, 0);
        }
    } else if (arg_eq(cmd, "select")) {
        reply_simple(o, "OK");
    } else if (arg_eq(cmd, "command")) {
        reply_array_header(o, 0); /* enough to satisfy clients that probe */
    } else if (arg_eq(cmd, "config")) {
        if (argc >= 2 && arg_eq(&argv[1], "get"))
            reply_array_header(o, 0);
        else
            reply_simple(o, "OK");
    } else if (arg_eq(cmd, "info")) {
        char info[192];
        int k = snprintf(info, sizeof info,
                         "# Server\r\nredis_version:microdb-0.2-lite\r\n"
                         "# Stats\r\ntotal_commands_processed:%llu\r\n"
                         "connected_clients:%llu\r\n",
                         (unsigned long long)g_stats.commands,
                         (unsigned long long)g_stats.active);
        reply_bulk(o, info, (size_t)k);
    } else if (arg_eq(cmd, "quit")) {
        reply_simple(o, "OK");
        return 0;
    } else {
        reply_error(o, "ERR unknown command");
        return 1;
    }
    return 1;
}

/* ------------------------------------------------------------------------- *
 * Connection handling and the server loop.
 * ------------------------------------------------------------------------- */
static volatile sig_atomic_t g_shutdown;
static int g_listen_fd = -1;

/* Active connections, tracked so shutdown can drain them. Each node lives on
   its handler coroutine's stack, valid while that coroutine is alive; the
   single-threaded scheduler needs no locking. */
typedef struct conn_node {
    int fd;
    struct conn_node *prev;
    struct conn_node *next;
} conn_node_t;

static conn_node_t *g_conns;

static void conn_register(conn_node_t *c)
{
    c->prev = NULL;
    c->next = g_conns;
    if (g_conns)
        g_conns->prev = c;
    g_conns = c;
}

static void conn_unregister(conn_node_t *c)
{
    if (c->prev)
        c->prev->next = c->next;
    else
        g_conns = c->next;
    if (c->next)
        c->next->prev = c->prev;
}

static void on_sigint(int sig)
{
    (void)sig;
    g_shutdown = 1;
}

static int write_all(int fd, const char *p, size_t n)
{
    size_t off = 0;
    while (off < n) {
        int w = coro_write(fd, p + off, n - off);
        if (w <= 0)
            return -1;
        off += (size_t)w;
    }
    return 0;
}

/* One coroutine per client connection: read a batch, execute every complete
   command in it, write all replies back, repeat. Looks blocking; is not. */
static void conn(void *arg)
{
    int fd = (int)(intptr_t)arg;
    conn_node_t self = {.fd = fd};
    conn_register(&self);
    g_stats.active++;

    buf_t in = {0}, out = {0};
    arg_t argv[MAX_ARGS];

    for (;;) {
        if (buf_reserve(&in, 16384) != 0)
            break;
        int n = coro_read(fd, in.data + in.len, in.cap - in.len);
        if (n <= 0)
            break;
        in.len += (size_t)n;

        size_t pos = 0;
        int keep_open = 1;
        for (;;) {
            int argc;
            size_t consumed;
            int r = parse_cmd(in.data + pos, in.len - pos, argv, &argc,
                              &consumed);
            if (r == 0)
                break; /* need more data */
            if (r < 0) {
                reply_error(&out, "ERR Protocol error");
                pos = in.len;
                keep_open = 0;
                break;
            }
            pos += consumed;
            if (argc <= 0)
                continue;
            if (exec_cmd(argv, argc, &out) == 0)
                keep_open = 0;
        }
        buf_consume(&in, pos);

        if (out.len) {
            if (write_all(fd, out.data, out.len) != 0)
                break;
            out.len = 0;
        }
        if (!keep_open)
            break;
    }

    buf_free(&in);
    buf_free(&out);
    close(fd);
    conn_unregister(&self);
    g_stats.active--;
}

static void acceptor(void *arg)
{
    int lfd = (int)(intptr_t)arg;
    while (!g_shutdown) {
        int cfd = coro_accept(lfd, NULL, NULL);
        if (g_shutdown) {
            if (cfd >= 0)
                close(cfd);
            break;
        }
        if (cfd < 0)
            break;
        int one = 1;
        setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
        g_stats.connections++;
        coro_create(conn, (void *)(intptr_t)cfd);
    }
}

/* Periodic stats banner; also drives cooperative shutdown detection. */
static void supervisor(void *arg)
{
    (void)arg;
    uint64_t last_cmds = 0;
    uint64_t last = now_ms();
    while (!g_shutdown) {
        coro_sleep(200);
        uint64_t t = now_ms();
        if (t - last >= 5000) {
            uint64_t dc = g_stats.commands - last_cmds;
            fprintf(stderr,
                    "[stats] keys=%zu clients=%llu total_conns=%llu cmds=%llu"
                    " (%.0f cmd/s)\n",
                    g_count, (unsigned long long)g_stats.active,
                    (unsigned long long)g_stats.connections,
                    (unsigned long long)g_stats.commands,
                    (double)dc * 1000.0 / (double)(t - last));
            last_cmds = g_stats.commands;
            last = t;
        }
    }
    fprintf(stderr, "\n[shutdown] draining %llu client(s)...\n",
            (unsigned long long)g_stats.active);
    if (g_listen_fd >= 0)
        shutdown(g_listen_fd, SHUT_RDWR);
    for (conn_node_t *c = g_conns; c; c = c->next)
        shutdown(c->fd, SHUT_RDWR);
}

static int make_listener(int port)
{
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

int main(int argc, char **argv)
{
    int port = 6380;
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
        default:
            fprintf(stderr, "usage: %s [-p port] (default 6380)\n", argv[0]);
            return opt == 'h' ? 0 : 1;
        }
    }

    /* Fail fast where io_uring is unavailable instead of binding the port
       and then never serving a request. */
    if (coro_io_probe() < 0) {
        fprintf(stderr, "fatal: io_uring unavailable "
                        "(check kernel.io_uring_disabled); microdb needs it\n");
        return 1;
    }

    signal(SIGPIPE, SIG_IGN);
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = on_sigint;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    store_init(1024);
    if (!g_buckets) {
        fprintf(stderr, "fatal: out of memory initializing keyspace\n");
        return 1;
    }
    g_listen_fd = make_listener(port);
    if (g_listen_fd < 0)
        return 1;

    printf("microdb listening on port %d (pid %d) - RESP/Redis compatible\n",
           port, (int)getpid());
    fflush(stdout);

    coro_init();
    coro_create(supervisor, NULL);
    coro_create(acceptor, (void *)(intptr_t)g_listen_fd);
    coro_run();

    close(g_listen_fd);
    store_flush();
    free(g_buckets);
    printf("[shutdown] complete: %llu commands served\n",
           (unsigned long long)g_stats.commands);
    return 0;
}
