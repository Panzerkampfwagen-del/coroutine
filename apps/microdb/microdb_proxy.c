/* microdb-proxy - a sharding + read-routing proxy for microdb, built on the
 * same coro runtime. It speaks RESP, so `redis-cli` and `redis-benchmark` drive
 * it unmodified, and it turns a set of independent microdb nodes into one
 * horizontally-partitioned key/value service.
 *
 * Each coroutine here is a *client* of the backend nodes - the mirror image of
 * microdb's server coroutines - which is what the runtime's coro_connect exists
 * to support.
 *
 *   - Routing:  key -> shard = fnv1a(key) % nshards.
 *   - Writes go to the shard's primary; reads round-robin its replicas (or the
 *     primary when a shard has none). This is the consistency/latency knob:
 *     replica reads can be stale under asynchronous replication.
 *   - Multi-key commands (MGET/MSET/DEL/EXISTS) fan out per key and the proxy
 *     reassembles one reply, so a key that lives on another shard still works.
 *   - Failover: a health-check coroutine pings each primary; after a few misses
 *     it promotes a replica (REPLICAOF NO ONE) and repoints routing at it. This
 *     is coordinator-style failover WITHOUT consensus - it can lose un-flushed
 *     writes and risks split brain if the old primary returns. See
 * docs/design.md.
 *
 * Build:  make microdb_proxy
 * Run:    ./microdb_proxy -p 7000 \
 *             -s 127.0.0.1:6401,127.0.0.1:6402 \
 *             -s 127.0.0.1:6403,127.0.0.1:6404
 *         redis-cli -p 7000 set foo bar
 */
#define _GNU_SOURCE
#include <ctype.h>
#include <getopt.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

#include "coro.h"
#include "io.h"
#include "resp.h" /* buf_t, RESP parser/replies, fnv1a, arg_t, arg_eq */

/* ------------------------------------------------------------------------- *
 * Topology: shards, each a primary plus zero or more replicas.
 * ------------------------------------------------------------------------- */
#define MAX_SHARDS 64
#define MAX_REPLICAS 8
#define HEALTH_INTERVAL_MS 300
#define HEALTH_THRESHOLD 3 /* consecutive PING misses before promotion */

typedef struct {
    char host[64];
    int port;
} node_t;

typedef struct {
    node_t primary;
    node_t replicas[MAX_REPLICAS];
    int nrep;
    unsigned
        rr; /* round-robin cursor for replica reads (unsigned: wraps cleanly) */
    int fails;     /* consecutive failed health checks of the primary */
    int health_fd; /* persistent health-check connection (-1 when down) */
} shard_t;

static shard_t g_shards[MAX_SHARDS];
static int g_nshards;

static int shard_of(const char *key, size_t klen) {
    if (g_nshards <= 1)
        return 0;
    return (int)(fnv1a(key, klen) % (uint64_t)g_nshards);
}

/* ------------------------------------------------------------------------- *
 * RESP reply framing (proxy-specific; the request parser and reply builders
 * come from resp.h).
 * ------------------------------------------------------------------------- */

/* Length of one complete RESP reply in b[0..n), 0 if more bytes are needed.
   Backends are our own microdb, so malformed input is not expected. */
static long resp_reply_len(const char *b, size_t n) {
    if (n < 1)
        return 0;
    switch (b[0]) {
    case '+':
    case '-':
    case ':': {
        size_t i = 1;
        while (i + 1 < n && !(b[i] == '\r' && b[i + 1] == '\n'))
            i++;
        if (i + 1 < n && b[i] == '\r' && b[i + 1] == '\n')
            return (long)(i + 2);
        return 0;
    }
    case '$': {
        size_t i = 1;
        int neg = 0;
        if (i < n && b[i] == '-') {
            neg = 1;
            i++;
        }
        size_t ds = i;
        long len = 0;
        while (i < n && b[i] >= '0' && b[i] <= '9') {
            len = len * 10 + (b[i] - '0');
            i++;
        }
        if (i + 1 >= n || b[i] != '\r' || b[i + 1] != '\n')
            return 0;
        if (neg)
            return (long)(i + 2); /* $-1\r\n */
        if (ds == i)
            return 0;
        size_t total = i + 2 + (size_t)len + 2;
        return total > n ? 0 : (long)total;
    }
    case '*': {
        size_t i = 1;
        int neg = 0;
        if (i < n && b[i] == '-') {
            neg = 1;
            i++;
        }
        size_t ds = i;
        long cnt = 0;
        while (i < n && b[i] >= '0' && b[i] <= '9') {
            cnt = cnt * 10 + (b[i] - '0');
            i++;
        }
        if (i + 1 >= n || b[i] != '\r' || b[i + 1] != '\n')
            return 0;
        if (neg)
            return (long)(i + 2); /* *-1\r\n */
        if (ds == i)
            return 0;
        size_t off = i + 2;
        for (long k = 0; k < cnt; k++) {
            long sub = resp_reply_len(b + off, n - off);
            if (sub <= 0)
                return 0;
            off += (size_t)sub;
        }
        return (long)off;
    }
    default:
        return 0;
    }
}

/* ------------------------------------------------------------------------- *
 * Per-client backend connection cache. A client coroutine keeps one open
 * connection per backend node it has talked to, keyed by host:port.
 * ------------------------------------------------------------------------- */
#define MAX_BCONN (MAX_SHARDS * (MAX_REPLICAS + 1))

typedef struct {
    node_t node;
    int fd;        /* -1 when not connected */
    buf_t in;      /* bytes read from the backend, not yet framed */
    size_t in_off; /* read cursor into `in`; avoids a memmove per reply */
    buf_t out;     /* staged requests, coalesced into one write per drain */
} bconn;

typedef struct {
    bconn conns[MAX_BCONN];
    int n;
} client_state;

static volatile sig_atomic_t g_shutdown;

static int write_all(int fd, const char *p, size_t n) {
    size_t off = 0;
    while (off < n) {
        int w = coro_write(fd, p + off, n - off);
        if (w <= 0)
            return -1;
        off += (size_t)w;
    }
    return 0;
}

static int dial(const node_t *node) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;
    struct sockaddr_in a;
    memset(&a, 0, sizeof a);
    a.sin_family = AF_INET;
    a.sin_port = htons((uint16_t)node->port);
    if (inet_pton(AF_INET, node->host, &a.sin_addr) != 1) {
        if (strcmp(node->host, "localhost") == 0)
            a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        else {
            close(fd);
            return -1;
        }
    }
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    if (coro_connect(fd, (struct sockaddr *)&a, sizeof a) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static int node_eq(const node_t *a, const node_t *b) {
    return a->port == b->port && strcmp(a->host, b->host) == 0;
}

/* Find (or open) this client's connection to a node. NULL on dial failure. */
static bconn *get_bconn(client_state *cs, const node_t *node) {
    /* Snapshot the node: it usually points into g_shards[], which the failover
       coroutine can rewrite while dial() below is parked in coro_connect. */
    node_t target = *node;
    node = &target;
    for (int i = 0; i < cs->n; i++)
        if (cs->conns[i].fd >= 0 && node_eq(&cs->conns[i].node, node))
            return &cs->conns[i];
    /* Reuse a closed slot if one exists. */
    bconn *slot = NULL;
    for (int i = 0; i < cs->n; i++)
        if (cs->conns[i].fd < 0) {
            slot = &cs->conns[i];
            break;
        }
    if (!slot) {
        if (cs->n >= MAX_BCONN)
            return NULL;
        slot = &cs->conns[cs->n++];
        slot->in = (buf_t){0};
        slot->out = (buf_t){0};
        slot->fd =
            -1; /* set before dial: a failed dial leaves a reusable slot */
    }
    int fd = dial(node);
    if (fd < 0)
        return NULL;
    slot->node = *node;
    slot->fd = fd;
    slot->in.len = 0;
    slot->in_off = 0;
    slot->out.len = 0;
    return slot;
}

static void drop_bconn(bconn *bc) {
    if (bc->fd >= 0)
        close(bc->fd);
    bc->fd = -1;
    bc->in.len = 0;
    bc->in_off = 0;
    bc->out.len = 0;
}

/* Read exactly one framed RESP reply from a backend and append it to *out.
   Returns 0 on success, -1 on backend failure (the connection is dropped). */
static int backend_read_one(bconn *bc, buf_t *out) {
    if (bc->fd < 0)
        return -1;
    for (;;) {
        size_t avail = bc->in.len - bc->in_off;
        /* The `bc->in.data &&` guard also tells the analyzer the pointer is
           non-NULL on the rl > 0 path. */
        long rl = (bc->in.data && avail)
                      ? resp_reply_len(bc->in.data + bc->in_off, avail)
                      : 0;
        if (rl > 0) {
            buf_append(out, bc->in.data + bc->in_off, (size_t)rl);
            bc->in_off += (size_t)rl;
            if (bc->in_off == bc->in.len) /* fully drained: rewind to front */
                bc->in.len = bc->in_off = 0;
            return 0;
        }
        /* Need more bytes: drop the already-consumed prefix (one memmove per
           refill, not one per reply), then read. */
        if (bc->in_off > 0) {
            buf_consume(&bc->in, bc->in_off);
            bc->in_off = 0;
        }
        if (buf_reserve(&bc->in, 16384) != 0) {
            drop_bconn(bc);
            return -1;
        }
        int n = coro_read(bc->fd, bc->in.data + bc->in.len,
                          bc->in.cap - bc->in.len);
        if (n <= 0) {
            drop_bconn(bc);
            return -1;
        }
        bc->in.len += (size_t)n;
    }
}

/* Send a command to a node and append its single reply to *out (the synchronous
   path, used by the multi-key helpers). Returns 0 on success, -1 on failure. */
static int backend_call(client_state *cs, const node_t *node, const char *cmd,
                        size_t cmdlen, buf_t *out) {
    bconn *bc = get_bconn(cs, node);
    if (!bc)
        return -1;
    if (write_all(bc->fd, cmd, cmdlen) != 0) {
        drop_bconn(bc);
        return -1;
    }
    return backend_read_one(bc, out);
}

/* Read one reply into a scratch buffer (used by helpers that must inspect it
   rather than forward it verbatim). */
static int backend_call_scratch(client_state *cs, const node_t *node,
                                const char *cmd, size_t cmdlen,
                                buf_t *scratch) {
    scratch->len = 0;
    return backend_call(cs, node, cmd, cmdlen, scratch);
}

/* ------------------------------------------------------------------------- *
 * Routing.
 * ------------------------------------------------------------------------- */
static int cmd_is_write(const arg_t *cmd) {
    return arg_eq(cmd, "set") || arg_eq(cmd, "del") || arg_eq(cmd, "incr") ||
           arg_eq(cmd, "decr") || arg_eq(cmd, "incrby") ||
           arg_eq(cmd, "append") || arg_eq(cmd, "mset") ||
           arg_eq(cmd, "getset") || arg_eq(cmd, "expire") ||
           arg_eq(cmd, "persist");
}

/* Commands the proxy answers itself rather than forwarding. */
static int is_local_cmd(const arg_t *c) {
    return arg_eq(c, "ping") || arg_eq(c, "quit") || arg_eq(c, "select") ||
           arg_eq(c, "config") || arg_eq(c, "command") || arg_eq(c, "info") ||
           arg_eq(c, "dbsize") || arg_eq(c, "flushall") || arg_eq(c, "flushdb");
}

/* Commands that span keys and must fan out across shards. */
static int is_multikey_cmd(const arg_t *c) {
    return arg_eq(c, "mget") || arg_eq(c, "mset") || arg_eq(c, "del") ||
           arg_eq(c, "exists");
}

/* A "simple" command is a single-key command forwarded verbatim to one backend.
   These are the ones the connection can pipeline; everything else is handled
   synchronously (after the pipeline drains). */
static int is_simple_cmd(const arg_t *cmd, int argc) {
    return argc >= 2 && !is_local_cmd(cmd) && !is_multikey_cmd(cmd);
}

/* Pick the node that should serve a single-key command: writes to the primary,
   reads to a round-robin replica (or the primary if there are none). */
static const node_t *route(int shard, int is_write) {
    shard_t *s = &g_shards[shard];
    if (is_write || s->nrep == 0)
        return &s->primary;
    unsigned idx = s->rr++ % (unsigned)s->nrep;
    return &s->replicas[idx];
}

/* True if a framed reply is a RESP error ("-ERR ...\r\n"). */
static int reply_is_error(const buf_t *b) {
    return b->len > 0 && b->data[0] == '-';
}

/* Parse the first integer reply ":N\r\n" in b; returns N (0 if not an int). */
static long long parse_int_reply(const char *b, size_t n) {
    if (n < 1 || b[0] != ':')
        return 0;
    long long v = 0;
    int neg = 0;
    size_t i = 1;
    if (i < n && b[i] == '-') {
        neg = 1;
        i++;
    }
    for (; i < n && b[i] >= '0' && b[i] <= '9'; i++)
        v = v * 10 + (b[i] - '0');
    return neg ? -v : v;
}

/* MGET k1..kn: fan out one GET per key to its shard's read node, then assemble
   the array reply in the original key order. */
static void do_mget(client_state *cs, arg_t *argv, int argc, buf_t *out,
                    buf_t *scratch) {
    reply_array_header(out, argc - 1);
    buf_t cmd = {0}; /* reused across keys, freed once below */
    for (int i = 1; i < argc; i++) {
        int sh = shard_of(argv[i].ptr, argv[i].len);
        const node_t *node = route(sh, 0);
        arg_t sub[2] = {{"GET", 3}, {argv[i].ptr, argv[i].len}};
        cmd.len = 0;
        resp_encode_command(&cmd, sub, 2);
        if (backend_call_scratch(cs, node, cmd.data, cmd.len, scratch) == 0 &&
            !reply_is_error(scratch))
            buf_append(out, scratch->data, scratch->len);
        else
            buf_append(out, "$-1\r\n", 5);
    }
    buf_free(&cmd);
}

/* MSET k1 v1 ..: route each pair to its key's primary; reply +OK if all stuck.
 */
static void do_mset(client_state *cs, arg_t *argv, int argc, buf_t *out,
                    buf_t *scratch) {
    if (argc < 3 || !(argc & 1)) {
        reply_error(out, "ERR wrong number of arguments for 'mset'");
        return;
    }
    int ok = 1;
    buf_t cmd = {0};
    for (int i = 1; i + 1 < argc; i += 2) {
        int sh = shard_of(argv[i].ptr, argv[i].len);
        arg_t sub[3] = {{"SET", 3},
                        {argv[i].ptr, argv[i].len},
                        {argv[i + 1].ptr, argv[i + 1].len}};
        cmd.len = 0;
        resp_encode_command(&cmd, sub, 3);
        if (backend_call_scratch(cs, &g_shards[sh].primary, cmd.data, cmd.len,
                                 scratch) != 0 ||
            reply_is_error(scratch))
            ok = 0;
    }
    buf_free(&cmd);
    if (ok)
        reply_simple(out, "OK");
    else
        reply_error(out, "ERR one or more shards failed");
}

/* DEL/EXISTS k1..kn: per-key fan-out, summing the integer replies. */
static void do_keysum(client_state *cs, arg_t *argv, int argc, buf_t *out,
                      buf_t *scratch, const char *op, int is_write) {
    long long total = 0;
    int failed = 0;
    size_t oplen = strlen(op);
    buf_t cmd = {0};
    for (int i = 1; i < argc; i++) {
        int sh = shard_of(argv[i].ptr, argv[i].len);
        const node_t *node = route(sh, is_write);
        arg_t sub[2] = {{op, oplen}, {argv[i].ptr, argv[i].len}};
        cmd.len = 0;
        resp_encode_command(&cmd, sub, 2);
        if (backend_call_scratch(cs, node, cmd.data, cmd.len, scratch) == 0 &&
            !reply_is_error(scratch))
            total += parse_int_reply(scratch->data, scratch->len);
        else
            failed = 1;
    }
    buf_free(&cmd);
    if (failed)
        reply_error(out, "ERR one or more shards failed");
    else
        reply_int(out, total);
}

/* Broadcast a no-key command (e.g. FLUSHALL) to every shard's primary. */
static void do_broadcast(client_state *cs, const char *raw, size_t rawlen,
                         buf_t *out, buf_t *scratch) {
    int ok = 1;
    for (int i = 0; i < g_nshards; i++)
        if (backend_call_scratch(cs, &g_shards[i].primary, raw, rawlen,
                                 scratch) != 0 ||
            reply_is_error(scratch))
            ok = 0;
    if (ok)
        reply_simple(out, "OK");
    else
        reply_error(out, "ERR one or more shards failed");
}

/* DBSIZE: sum across shard primaries. */
static void do_dbsize(client_state *cs, buf_t *out, buf_t *scratch) {
    long long total = 0;
    arg_t one[1] = {{"DBSIZE", 6}};
    buf_t cmd = {0};
    resp_encode_command(&cmd, one, 1);
    for (int i = 0; i < g_nshards; i++)
        if (backend_call_scratch(cs, &g_shards[i].primary, cmd.data, cmd.len,
                                 scratch) == 0)
            total += parse_int_reply(scratch->data, scratch->len);
    buf_free(&cmd);
    reply_int(out, total);
}

/* Route and execute one client command, appending the reply to *out.
   Returns 1 normally, 0 if the connection should close. */
static int handle_command(client_state *cs, arg_t *argv, int argc,
                          const char *raw, size_t rawlen, buf_t *out,
                          buf_t *scratch) {
    const arg_t *cmd = &argv[0];

    /* Commands the proxy answers itself. */
    if (arg_eq(cmd, "ping")) {
        if (argc >= 2)
            reply_bulk(out, argv[1].ptr, argv[1].len);
        else
            reply_simple(out, "PONG");
        return 1;
    }
    if (arg_eq(cmd, "quit")) {
        reply_simple(out, "OK");
        return 0;
    }
    if (arg_eq(cmd, "select") || arg_eq(cmd, "config")) {
        if (arg_eq(cmd, "config") && argc >= 2 && arg_eq(&argv[1], "get"))
            reply_array_header(out, 0);
        else
            reply_simple(out, "OK");
        return 1;
    }
    if (arg_eq(cmd, "command")) {
        reply_array_header(out, 0);
        return 1;
    }
    if (arg_eq(cmd, "info")) {
        char info[256];
        int k = snprintf(info, sizeof info,
                         "# Server\r\nredis_version:microdb-proxy-0.1\r\n"
                         "# Cluster\r\nshards:%d\r\n",
                         g_nshards);
        reply_bulk(out, info, (size_t)k);
        return 1;
    }
    if (arg_eq(cmd, "dbsize")) {
        do_dbsize(cs, out, scratch);
        return 1;
    }
    if (arg_eq(cmd, "flushall") || arg_eq(cmd, "flushdb")) {
        do_broadcast(cs, raw, rawlen, out, scratch);
        return 1;
    }

    /* Multi-key commands fan out across shards. */
    if (arg_eq(cmd, "mget") && argc >= 2) {
        do_mget(cs, argv, argc, out, scratch);
        return 1;
    }
    if (arg_eq(cmd, "mset")) {
        do_mset(cs, argv, argc, out, scratch);
        return 1;
    }
    if (arg_eq(cmd, "del") && argc >= 2) {
        do_keysum(cs, argv, argc, out, scratch, "DEL", 1);
        return 1;
    }
    if (arg_eq(cmd, "exists") && argc >= 2) {
        do_keysum(cs, argv, argc, out, scratch, "EXISTS", 0);
        return 1;
    }

    /* Single-key commands: route by argv[1] and forward verbatim. */
    if (argc < 2) {
        reply_error(out, "ERR unsupported command for this proxy");
        return 1;
    }
    int sh = shard_of(argv[1].ptr, argv[1].len);
    const node_t *node = route(sh, cmd_is_write(cmd));
    if (backend_call(cs, node, raw, rawlen, out) != 0)
        reply_error(out, "ERR shard unavailable");
    return 1;
}

/* ------------------------------------------------------------------------- *
 * Client connection coroutine.
 *
 * Simple single-key commands are pipelined: a whole batch is dispatched to the
 * backends (each write flushed immediately), then the replies are drained in
 * the original command order. Because every backend processes its commands in
 * order, the i-th reply from a backend matches the i-th command sent to it, so
 * interleaving across shards stays correct. This is what lets a pipelined
 * client (redis-benchmark -P) keep its throughput through the proxy. The batch
 * is bounded so requests and replies always fit in the socket buffers (no
 * pipeline deadlock). Local and multi-key commands drain the pipeline first to
 * preserve ordering, then run synchronously.
 * ------------------------------------------------------------------------- */
#define MAX_CONNS 65536
#define MAX_PIPELINE 256
static int g_conn_fds[MAX_CONNS];
static int g_conn_n;
static int g_listen_fd = -1;

/* Flush every backend's staged requests with a single write each, then read one
   reply per pending entry in order, appending to *out. A NULL entry marks a
   command whose dispatch already failed (e.g. the dial failed). */
static void drain_pipeline(client_state *cs, bconn **pend, int *np,
                           buf_t *out) {
    for (int i = 0; i < cs->n; i++) {
        bconn *bc = &cs->conns[i];
        if (bc->fd >= 0 && bc->out.len > 0) {
            if (write_all(bc->fd, bc->out.data, bc->out.len) != 0)
                drop_bconn(bc); /* pending reads on it will report the error */
            bc->out.len = 0;
        }
    }
    for (int i = 0; i < *np; i++) {
        if (!pend[i] || backend_read_one(pend[i], out) != 0)
            reply_error(out, "ERR shard unavailable");
    }
    *np = 0;
}

static void on_sigint(int sig) {
    (void)sig;
    g_shutdown = 1;
}

static void client(void *arg) {
    int fd = (int)(intptr_t)arg;
    int slot = g_conn_n < MAX_CONNS ? g_conn_n : -1;
    if (slot >= 0)
        g_conn_fds[g_conn_n++] = fd;

    client_state cs;
    cs.n = 0;
    buf_t in = {0}, out = {0}, scratch = {0};
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
        bconn *
            pend[MAX_PIPELINE]; /* simple commands awaiting replies, in order */
        int np = 0;

        for (;;) {
            int argc;
            size_t consumed;
            int r =
                parse_cmd(in.data + pos, in.len - pos, argv, &argc, &consumed);
            if (r == 0)
                break;
            if (r < 0) {
                drain_pipeline(&cs, pend, &np, &out);
                reply_error(&out, "ERR Protocol error");
                pos = in.len;
                keep_open = 0;
                break;
            }
            const char *raw = in.data + pos;
            size_t rawlen = consumed;
            pos += consumed;
            if (argc <= 0)
                continue;

            const arg_t *cmd = &argv[0];
            if (is_simple_cmd(cmd, argc)) {
                /* Stage the request on its backend; it is flushed (coalesced
                   with the batch's other requests to that backend) and its
                   reply collected at drain time. */
                int sh = shard_of(argv[1].ptr, argv[1].len);
                bconn *bc = get_bconn(&cs, route(sh, cmd_is_write(cmd)));
                if (bc)
                    buf_append(&bc->out, raw, rawlen);
                pend[np++] = bc; /* NULL bc => error emitted on drain */
                if (np == MAX_PIPELINE)
                    drain_pipeline(&cs, pend, &np, &out);
            } else {
                /* Ordering: a local/multi-key command must see all earlier
                   replies first. */
                drain_pipeline(&cs, pend, &np, &out);
                if (handle_command(&cs, argv, argc, raw, rawlen, &out,
                                   &scratch) == 0)
                    keep_open = 0;
            }
        }
        drain_pipeline(&cs, pend, &np, &out);
        buf_consume(&in, pos);

        if (out.len) {
            if (write_all(fd, out.data, out.len) != 0)
                break;
            out.len = 0;
        }
        if (!keep_open)
            break;
    }

    for (int i = 0; i < cs.n; i++)
        drop_bconn(&cs.conns[i]);
    for (int i = 0; i < cs.n; i++) {
        buf_free(&cs.conns[i].in);
        buf_free(&cs.conns[i].out);
    }
    buf_free(&in);
    buf_free(&out);
    buf_free(&scratch);
    close(fd);
    if (slot >= 0)
        g_conn_fds[slot] = -1;
}

/* ------------------------------------------------------------------------- *
 * Failover: health-check each primary; promote a replica after repeated misses.
 * ------------------------------------------------------------------------- */
/* PING over an already-open connection; 1 if it answered (+PONG), else 0. The
   connection is reused across ticks, so a healthy primary costs no reconnects.
 */
static int ping_fd(int fd) {
    if (write_all(fd, "*1\r\n$4\r\nPING\r\n", 14) != 0)
        return 0;
    char b[64];
    int n = coro_read(fd, b, sizeof b);
    return n >= 1 && b[0] == '+'; /* +PONG\r\n */
}

static void promote_replica(const node_t *node) {
    int fd = dial(node);
    if (fd < 0)
        return;
    if (write_all(fd, "*3\r\n$9\r\nREPLICAOF\r\n$2\r\nNO\r\n$3\r\nONE\r\n",
                  37) == 0) {
        char b[64];
        coro_read(fd, b, sizeof b); /* expect +OK */
    }
    close(fd);
}

static void health_check(void *arg) {
    (void)arg;
    while (!g_shutdown) {
        coro_sleep(HEALTH_INTERVAL_MS);
        for (int i = 0; i < g_nshards; i++) {
            shard_t *s = &g_shards[i];
            if (s->health_fd < 0)
                s->health_fd = dial(&s->primary); /* (re)connect lazily */
            if (s->health_fd >= 0 && ping_fd(s->health_fd)) {
                s->fails = 0;
                continue;
            }
            if (s->health_fd >=
                0) { /* the probe failed: drop it, redial later */
                close(s->health_fd);
                s->health_fd = -1;
            }
            if (++s->fails < HEALTH_THRESHOLD || s->nrep == 0)
                continue;
            /* Promote the first replica and repoint routing at it. The stale
               health_fd is already closed, so the next tick probes the new
               primary. */
            node_t newp = s->replicas[0];
            promote_replica(&newp);
            fprintf(stderr,
                    "[failover] shard %d: primary %s:%d down -> promoting "
                    "%s:%d\n",
                    i, s->primary.host, s->primary.port, newp.host, newp.port);
            s->primary = newp;
            for (int j = 0; j < s->nrep - 1; j++)
                s->replicas[j] = s->replicas[j + 1];
            s->nrep--;
            s->fails = 0;
        }
    }
    for (int i = 0; i < g_nshards; i++)
        if (g_shards[i].health_fd >= 0)
            close(g_shards[i].health_fd);
}

static void acceptor(void *arg) {
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
        if (!coro_create(client, (void *)(intptr_t)cfd, 256UL * 1024))
            close(cfd);
    }
}

static void supervisor(void *arg) {
    (void)arg;
    while (!g_shutdown)
        coro_sleep(200);
    if (g_listen_fd >= 0)
        shutdown(g_listen_fd, SHUT_RDWR);
    for (int i = 0; i < g_conn_n; i++)
        if (g_conn_fds[i] >= 0)
            shutdown(g_conn_fds[i], SHUT_RDWR);
}

/* ------------------------------------------------------------------------- *
 * Configuration parsing and startup.
 * ------------------------------------------------------------------------- */
static int parse_node(const char *s, node_t *out) {
    const char *colon = strrchr(s, ':');
    if (!colon || colon == s || (size_t)(colon - s) >= sizeof out->host)
        return -1;
    size_t hlen = (size_t)(colon - s);
    memcpy(out->host, s, hlen);
    out->host[hlen] = 0;
    out->port = atoi(colon + 1);
    return (out->port > 0 && out->port <= 65535) ? 0 : -1;
}

/* A shard spec is "primary[,replica1,replica2,...]". */
static int parse_shard(char *spec) {
    if (g_nshards >= MAX_SHARDS)
        return -1;
    shard_t *s = &g_shards[g_nshards];
    memset(s, 0, sizeof *s);
    s->health_fd = -1; /* memset left it 0, which is a valid fd */
    char *save = NULL;
    char *tok = strtok_r(spec, ",", &save);
    if (!tok || parse_node(tok, &s->primary) != 0)
        return -1;
    while ((tok = strtok_r(NULL, ",", &save))) {
        if (s->nrep >= MAX_REPLICAS)
            return -1;
        if (parse_node(tok, &s->replicas[s->nrep]) != 0)
            return -1;
        s->nrep++;
    }
    g_nshards++;
    return 0;
}

static int make_listener(int port) {
    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0)
        return -1;
    int opt = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof opt);
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)port);
    if (bind(lfd, (struct sockaddr *)&addr, sizeof addr) < 0 ||
        listen(lfd, 1024) < 0) {
        close(lfd);
        return -1;
    }
    return lfd;
}

int main(int argc, char **argv) {
    int port = 7000;
    int opt;
    while ((opt = getopt(argc, argv, "p:s:h")) != -1) {
        switch (opt) {
        case 'p':
            port = atoi(optarg);
            if (port <= 0 || port > 65535) {
                fprintf(stderr, "invalid port: %s\n", optarg);
                return 1;
            }
            break;
        case 's':
            if (parse_shard(optarg) != 0) {
                fprintf(stderr, "invalid shard spec: %s\n", optarg);
                return 1;
            }
            break;
        case 'h':
        default:
            fprintf(stderr,
                    "usage: %s -p port -s primary[,replica...] "
                    "[-s primary[,replica...]] ...\n"
                    "  e.g. %s -p 7000 -s 127.0.0.1:6401,127.0.0.1:6402 "
                    "-s 127.0.0.1:6403,127.0.0.1:6404\n",
                    argv[0], argv[0]);
            return opt == 'h' ? 0 : 1;
        }
    }
    if (g_nshards == 0) {
        fprintf(stderr, "no shards configured (need at least one -s)\n");
        return 1;
    }

    /* The proxy dials backends with io_uring; fail fast if it is unavailable.
     */
    if (coro_io_probe() < 0) {
        fprintf(stderr,
                "fatal: io_uring is unavailable (check "
                "kernel.io_uring_disabled); microdb-proxy requires it\n");
        return 1;
    }

    signal(SIGPIPE, SIG_IGN);
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = on_sigint;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    g_listen_fd = make_listener(port);
    if (g_listen_fd < 0) {
        perror("listen");
        return 1;
    }

    printf("microdb-proxy listening on port %d (pid %d) - %d shard(s)\n", port,
           (int)getpid(), g_nshards);
    for (int i = 0; i < g_nshards; i++) {
        printf("  shard %d: primary %s:%d", i, g_shards[i].primary.host,
               g_shards[i].primary.port);
        for (int j = 0; j < g_shards[i].nrep; j++)
            printf(", replica %s:%d", g_shards[i].replicas[j].host,
                   g_shards[i].replicas[j].port);
        printf("\n");
    }
    fflush(stdout);

    if (!coro_create(supervisor, NULL, 0) ||
        !coro_create(health_check, NULL, 0) ||
        !coro_create(acceptor, (void *)(intptr_t)g_listen_fd, 0)) {
        fprintf(stderr, "failed to start coroutines\n");
        return 1;
    }
    coro_run();

    close(g_listen_fd);
    printf("[shutdown] proxy stopped\n");
    return 0;
}
