/* microdb - a small Redis-compatible in-memory key/value store built on the
 * coro runtime. It speaks RESP (the Redis wire protocol), so `redis-cli` and
 * `redis-benchmark` talk to it unmodified. One coroutine handles each
 * connection; the whole server is a single OS thread, so the keyspace needs no
 * locking. The point is to show the runtime doing real work - and to be
 * competitive with Redis on simple GET/SET/INCR at a fraction of the code.
 *
 * Supported: PING ECHO SET GET DEL EXISTS INCR DECR INCRBY APPEND STRLEN
 *            MGET MSET GETSET EXPIRE TTL PERSIST TYPE DBSIZE FLUSHDB FLUSHALL
 *            KEYS(*) CONFIG(stub) COMMAND(stub) INFO SELECT QUIT
 * Replication: REPLICAOF/SLAVEOF, PSYNC, REPLCONF, WAIT  (see the Replication
 *            section below and docs/design.md).
 *
 * Build:  make microdb
 * Run:    ./microdb -p 6380                      # a primary
 *         ./microdb -p 6381 -r 127.0.0.1:6380    # a replica of it
 *         redis-cli -p 6380 set foo bar
 *         redis-benchmark -p 6380 -t set,get,incr -n 100000 -q
 */
#define _GNU_SOURCE
#include <ctype.h>
#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

#include "channel.h"
#include "coro.h"
#include "io.h"
#include "resp.h" /* buf_t, RESP parser/replies, fnv1a, arg_t, arg_eq */

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

typedef struct {
    entry **buckets;
    size_t nbuckets;
    size_t count;
} store_t;

static store_t g_store;

static uint64_t now_ms(void) {
    struct timespec ts;
    /* Wall clock, not CLOCK_MONOTONIC: EXAT/PXAT take absolute Unix-epoch
       timestamps from clients, and snapshot PXAT values must mean the same
       instant on the primary and its replicas (possibly different hosts). */
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)ts.tv_nsec / 1000000ull;
}

static void store_init(size_t nbuckets) {
    g_store.nbuckets = nbuckets;
    g_store.count = 0;
    g_store.buckets = calloc(nbuckets, sizeof(entry *));
}

static void store_rehash(void) {
    size_t n = g_store.nbuckets * 2;
    entry **nb = calloc(n, sizeof(entry *));
    if (!nb)
        return;
    for (size_t i = 0; i < g_store.nbuckets; i++) {
        entry *e = g_store.buckets[i];
        while (e) {
            entry *next = e->next;
            size_t b = fnv1a(e->key, e->klen) & (n - 1);
            e->next = nb[b];
            nb[b] = e;
            e = next;
        }
    }
    free(g_store.buckets);
    g_store.buckets = nb;
    g_store.nbuckets = n;
}

static entry *entry_find(const char *key, size_t klen, entry ***slot) {
    size_t b = fnv1a(key, klen) & (g_store.nbuckets - 1);
    entry **link = &g_store.buckets[b];
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

static void entry_unlink_free(entry **link, entry *e) {
    *link = e->next;
    free(e->key);
    free(e->val);
    free(e);
    g_store.count--;
}

/* Look up a live key, lazily expiring it if its TTL has passed. */
static entry *store_get(const char *key, size_t klen) {
    entry **link;
    entry *e = entry_find(key, klen, &link);
    if (!e)
        return NULL;
    if (e->expire_ms && now_ms() >= e->expire_ms) {
        entry_unlink_free(link, e);
        return NULL;
    }
    return e;
}

static void store_set(const char *key, size_t klen, const char *val,
                      size_t vlen, uint64_t expire_ms) {
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
    g_store.count++;
    if (g_store.count > g_store.nbuckets) /* load factor > 1 */
        store_rehash();
}

static int store_del(const char *key, size_t klen) {
    entry **link;
    entry *e = entry_find(key, klen, &link);
    if (!e)
        return 0;
    entry_unlink_free(link, e);
    return 1;
}

static void store_flush(void) {
    for (size_t i = 0; i < g_store.nbuckets; i++) {
        entry *e = g_store.buckets[i];
        while (e) {
            entry *next = e->next;
            free(e->key);
            free(e->val);
            free(e);
            e = next;
        }
        g_store.buckets[i] = NULL;
    }
    g_store.count = 0;
}

/* ------------------------------------------------------------------------- *
 * Replication.
 *
 * A node is either a primary (the default) or a replica of some primary. The
 * protocol is a deliberately small subset of Redis's:
 *
 *   replica -> primary:  *1\r\n$5\r\nPSYNC\r\n
 *   primary -> replica:  +FULLRESYNC <replid> <offset>\r\n
 *                        $<n>\r\n<n bytes of snapshot>      (RDB-style bulk)
 *                        ... then a live stream of write commands, forever
 *
 * The snapshot is itself a concatenation of RESP `SET` commands, so the replica
 * applies the snapshot and the live stream through the exact same code path.
 * Every write a primary accepts is re-encoded and appended to each connected
 * replica's backlog; a per-replica feeder coroutine drains the backlog to the
 * socket. Applying a command on a replica re-propagates it to that replica's
 * own sub-replicas, so chained replication works for free.
 * ------------------------------------------------------------------------- */
#define REPL_BACKLOG_MAX                                                       \
    (64UL * 1024 * 1024) /* drop a replica this far behind */
#define REPL_SNAPSHOT_MAX                                                      \
    (4UL * 1024 * 1024 * 1024) /* reject an absurd/overflowed snapshot length  \
                                */

/* One connected replica, as seen from the primary. */
typedef struct repl_feed {
    int fd;
    buf_t backlog;   /* live-stream bytes not yet written to the socket */
    channel_t *wake; /* 1-slot semaphore: signalled when backlog grows */
    int waiting;     /* feeder is parked in channel_recv on an empty backlog */
    int alive;       /* cleared on write error or backlog overflow */
    int synced;      /* snapshot fully sent: only then does it count for WAIT */
    long sent_offset; /* master offset this replica has been flushed up to */
    struct repl_feed *next;
} repl_feed;

static repl_feed *g_feeds; /* primary side: list of connected replicas */
static long
    g_master_offset; /* total live-stream bytes propagated since start */
static char g_replid[41];

/* Replica side. */
static int g_is_replica;
static int g_link_started; /* the master-link coroutine has been spawned */
static char g_master_host[256];
static int g_master_port;
static int g_master_link_up;
static int g_master_fd = -1; /* current link socket, so promotion can cut it */
static int g_master_gen;     /* bumped on REPLICAOF to retarget the link */

/* exec_cmd context: where replies go and whether the command came from our
   master (which bypasses the read-only guard). */
typedef struct {
    buf_t *out;
    int from_master;
} client_t;

/* Commands that mutate the keyspace and therefore must be replicated. */
static int cmd_is_write(const arg_t *cmd) {
    return arg_eq(cmd, "set") || arg_eq(cmd, "del") || arg_eq(cmd, "incr") ||
           arg_eq(cmd, "decr") || arg_eq(cmd, "incrby") ||
           arg_eq(cmd, "append") || arg_eq(cmd, "mset") ||
           arg_eq(cmd, "getset") || arg_eq(cmd, "expire") ||
           arg_eq(cmd, "pexpireat") || arg_eq(cmd, "persist") ||
           arg_eq(cmd, "flushdb") || arg_eq(cmd, "flushall");
}

/* Serialize the whole live keyspace as a run of SET commands (with absolute
   PXAT expiries). Does not yield, so the snapshot is a consistent point-in-time
   image. */
static void store_snapshot(buf_t *o) {
    uint64_t now = now_ms();
    char tmp[24];
    for (size_t i = 0; i < g_store.nbuckets; i++) {
        for (entry *e = g_store.buckets[i]; e; e = e->next) {
            if (e->expire_ms && e->expire_ms <= now)
                continue; /* already dead */
            arg_t a[5];
            a[0].ptr = "SET";
            a[0].len = 3;
            a[1].ptr = e->key;
            a[1].len = e->klen;
            a[2].ptr = e->val;
            a[2].len = e->vlen;
            int ac = 3;
            if (e->expire_ms) {
                a[3].ptr = "PXAT";
                a[3].len = 4;
                int k = snprintf(tmp, sizeof tmp, "%llu",
                                 (unsigned long long)e->expire_ms);
                a[4].ptr = tmp;
                a[4].len = (size_t)k;
                ac = 5;
            }
            resp_encode_command(o, a, ac);
        }
    }
}

/* Append already-encoded bytes to every replica's backlog and wake any idle
   feeder. Runs to completion without yielding, so the feed list is stable. */
static void propagate_raw(const char *data, size_t n) {
    g_master_offset += (long)n;
    for (repl_feed *f = g_feeds; f; f = f->next) {
        if (!f->alive)
            continue;
        buf_append(&f->backlog, data, n);
        if (f->backlog.len > REPL_BACKLOG_MAX)
            f->alive = 0; /* too far behind; the woken feeder will drop it */
        /* Wake even when we just cleared alive, so a parked feeder unparks and
           runs its cleanup instead of sleeping forever on a dead feed. */
        if (f->waiting) {
            f->waiting = 0;
            char d = 1;
            channel_send(f->wake, &d); /* empty 1-slot channel: never blocks */
        }
    }
}

static void propagate(const arg_t *argv, int argc) {
    if (!g_feeds)
        return;
    /* Reused across calls: propagate() runs to completion without yielding in a
       single-threaded runtime, so one scratch buffer avoids a malloc/free per
       replicated write. */
    static buf_t t;
    t.len = 0;
    resp_encode_command(&t, argv, argc);
    propagate_raw(t.data, t.len);
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
static int arg_to_ll(const arg_t *a, long long *out) {
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

/* Returns 1 if the keyspace was mutated, 0 if the command was rejected (so the
   caller knows whether to replicate it). */
static int do_incrby(buf_t *o, const arg_t *key, long long delta) {
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

/* Forward declaration: WAIT and REPLICAOF need these. */
static void start_master_link(void);
static int count_acked(long target);

/* Execute one parsed command, appending the RESP reply to cl->out.
 * Returns 1 normally, 0 if the connection should close (QUIT). */
static int exec_cmd(arg_t *argv, int argc, client_t *cl) {
    g_stats.commands++;
    buf_t *o = cl->out;
    const arg_t *cmd = &argv[0];

    /* A replica refuses client writes; only its master link may mutate it. */
    if (g_is_replica && !cl->from_master && cmd_is_write(cmd)) {
        reply_error(o, "READONLY You can't write against a read only replica.");
        return 1;
    }
    int dirty = 0; /* set by any branch that mutated the keyspace */
    /* Replication override: a branch whose verbatim form is non-deterministic
       (relative expiry) fills rov/roc with an absolute-time equivalent to
       propagate instead. rovbuf must outlive the propagate() call at the end.
     */
    arg_t rov[5];
    int roc = 0;
    char rovbuf[24];

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
        dirty = 1;
        if (exp) {
            /* Replicate the absolute expiry so replicas don't recompute a
               relative TTL against their own clock: SET key val PXAT <exp>. */
            int kk = snprintf(rovbuf, sizeof rovbuf, "%llu",
                              (unsigned long long)exp);
            rov[0] = (arg_t){"SET", 3};
            rov[1] = argv[1];
            rov[2] = argv[2];
            rov[3] = (arg_t){"PXAT", 4};
            rov[4] = (arg_t){rovbuf, (size_t)kk};
            roc = 5;
        }
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
        dirty = 1;
    } else if (arg_eq(cmd, "del")) {
        long long removed = 0;
        for (int i = 1; i < argc; i++)
            removed += store_del(argv[i].ptr, argv[i].len);
        reply_int(o, removed);
        dirty = removed > 0; /* nothing removed -> nothing to replicate */
    } else if (arg_eq(cmd, "exists")) {
        long long found = 0;
        for (int i = 1; i < argc; i++)
            found += store_get(argv[i].ptr, argv[i].len) != NULL;
        reply_int(o, found);
    } else if (arg_eq(cmd, "incr") && argc == 2) {
        dirty = do_incrby(o, &argv[1], 1);
    } else if (arg_eq(cmd, "decr") && argc == 2) {
        dirty = do_incrby(o, &argv[1], -1);
    } else if (arg_eq(cmd, "incrby") && argc == 3) {
        long long d;
        if (arg_to_ll(&argv[2], &d)) {
            dirty = do_incrby(o, &argv[1], d);
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
        dirty = 1;
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
    } else if (arg_eq(cmd, "mset") && argc >= 3 && (argc & 1)) {
        for (int i = 1; i + 1 < argc; i += 2)
            store_set(argv[i].ptr, argv[i].len, argv[i + 1].ptr,
                      argv[i + 1].len, 0);
        reply_simple(o, "OK");
        dirty = 1;
    } else if (arg_eq(cmd, "expire") && argc == 3) {
        long long sec;
        entry *e = store_get(argv[1].ptr, argv[1].len);
        if (e && arg_to_ll(&argv[2], &sec)) {
            e->expire_ms = now_ms() + (uint64_t)sec * 1000;
            reply_int(o, 1);
            dirty = 1;
            /* Replicate the absolute deadline (PEXPIREAT) rather than the
               relative seconds, which the replica would re-evaluate later. */
            int kk = snprintf(rovbuf, sizeof rovbuf, "%llu",
                              (unsigned long long)e->expire_ms);
            rov[0] = (arg_t){"PEXPIREAT", 9};
            rov[1] = argv[1];
            rov[2] = (arg_t){rovbuf, (size_t)kk};
            roc = 3;
        } else {
            reply_int(o, 0);
        }
    } else if (arg_eq(cmd, "pexpireat") && argc == 3) {
        /* Absolute expiry in ms (used for replication of EXPIRE; also a real
           command). Deterministic, so it replicates verbatim. */
        long long ms;
        entry *e = store_get(argv[1].ptr, argv[1].len);
        if (e && arg_to_ll(&argv[2], &ms)) {
            e->expire_ms = (uint64_t)ms;
            reply_int(o, 1);
            dirty = 1;
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
            dirty = 1;
        } else {
            reply_int(o, 0);
        }
    } else if (arg_eq(cmd, "type") && argc == 2) {
        entry *e = store_get(argv[1].ptr, argv[1].len);
        reply_simple(o, e ? "string" : "none");
    } else if (arg_eq(cmd, "dbsize")) {
        reply_int(o, (long long)g_store.count);
    } else if (arg_eq(cmd, "flushdb") || arg_eq(cmd, "flushall")) {
        store_flush();
        reply_simple(o, "OK");
        dirty = 1;
    } else if (arg_eq(cmd, "keys") && argc == 2) {
        /* Only the "*" (match everything) pattern is supported. */
        if (argv[1].len == 1 && argv[1].ptr[0] == '*') {
            reply_array_header(o, (long long)g_store.count);
            uint64_t now = now_ms();
            for (size_t i = 0; i < g_store.nbuckets; i++)
                for (entry *e = g_store.buckets[i]; e; e = e->next)
                    if (!e->expire_ms || e->expire_ms > now)
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
        int slaves = 0;
        for (repl_feed *f = g_feeds; f; f = f->next)
            slaves += f->alive;
        char info[512];
        int k = snprintf(info, sizeof info,
                         "# Server\r\nredis_version:microdb-0.1\r\n"
                         "# Replication\r\nrole:%s\r\nconnected_slaves:%d\r\n"
                         "master_host:%s\r\nmaster_port:%d\r\n"
                         "master_link_status:%s\r\nmaster_repl_offset:%ld\r\n",
                         g_is_replica ? "slave" : "master", slaves,
                         g_is_replica ? g_master_host : "",
                         g_is_replica ? g_master_port : 0,
                         g_master_link_up ? "up" : "down", g_master_offset);
        reply_bulk(o, info, (size_t)k);
    } else if ((arg_eq(cmd, "replicaof") || arg_eq(cmd, "slaveof")) &&
               argc == 3) {
        if (arg_eq(&argv[1], "no") && arg_eq(&argv[2], "one")) {
            /* Promote to primary: stop following, and cut the link so the
               master-link coroutine notices immediately. */
            g_is_replica = 0;
            g_master_link_up = 0;
            g_master_gen++;
            if (g_master_fd >= 0)
                shutdown(g_master_fd, SHUT_RDWR);
        } else if (argv[1].len < sizeof g_master_host) {
            long long port;
            if (!arg_to_ll(&argv[2], &port) || port <= 0 || port > 65535) {
                reply_error(o, "ERR Invalid master port");
                return 1;
            }
            memcpy(g_master_host, argv[1].ptr, argv[1].len);
            g_master_host[argv[1].len] = 0;
            g_master_port = (int)port;
            g_is_replica = 1;
            g_master_link_up = 0;
            g_master_gen++;
            if (g_master_fd >= 0)
                shutdown(g_master_fd, SHUT_RDWR); /* retarget */
            start_master_link();
        } else {
            reply_error(o, "ERR Invalid master host");
            return 1;
        }
        reply_simple(o, "OK");
    } else if (arg_eq(cmd, "replconf")) {
        reply_simple(o, "OK"); /* listening-port / capa / ack handshake noise */
    } else if (arg_eq(cmd, "wait") && argc == 3) {
        long long want, timeout;
        if (!arg_to_ll(&argv[1], &want) || !arg_to_ll(&argv[2], &timeout) ||
            want < 0 || timeout < 0) {
            reply_error(o, "ERR value is not an integer or out of range");
            return 1;
        }
        long target = g_master_offset;
        uint64_t deadline = now_ms() + (uint64_t)(timeout > 0 ? timeout : 0);
        for (;;) {
            int n = count_acked(target);
            if (n >= want) {
                reply_int(o, n);
                break;
            }
            if (timeout > 0 && now_ms() >= deadline) {
                reply_int(o, n);
                break;
            }
            coro_sleep(20); /* let feeders flush; they run while we sleep */
        }
    } else if (arg_eq(cmd, "quit")) {
        reply_simple(o, "OK");
        return 0;
    } else {
        reply_error(o, "ERR unknown command");
        return 1; /* unmatched: nothing to replicate */
    }

    /* A matched write is now applied locally; mirror it to any replicas (and,
       on a replica, on to its own sub-replicas). A branch that needs a
       deterministic (absolute-time) form supplies it in rov/roc. */
    if (dirty) {
        if (roc)
            propagate(rov, roc);
        else
            propagate(argv, argc);
    }
    return 1;
}

/* ------------------------------------------------------------------------- *
 * Connection handling and the server loop.
 * ------------------------------------------------------------------------- */
static volatile sig_atomic_t g_shutdown;
static int g_listen_fd = -1;

/* Active connection fds, so shutdown can drain them (mirrors echo_server). */
#define MAX_CONNS 65536
static int g_conn_fds[MAX_CONNS];
static int g_conn_n;

static void on_sigint(int sig) {
    (void)sig;
    g_shutdown = 1;
}

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

/* ------------------------------------------------------------------------- *
 * Replication runtime: the primary's per-replica feeder and the replica's
 * link back to its primary.
 * ------------------------------------------------------------------------- */

/* WAIT: how many replicas have been flushed up to a given master offset. This
   is ack-on-flush (the bytes reached the replica's socket), which is weaker
   than Redis's ack-on-apply but needs no back-channel from the replica. */
static int count_acked(long target) {
    int n = 0;
    for (repl_feed *f = g_feeds; f; f = f->next)
        if (f->alive && f->synced && f->sent_offset >= target)
            n++;
    return n;
}

static void feed_link(repl_feed *f) {
    f->next = g_feeds;
    g_feeds = f;
}

static void feed_unlink(repl_feed *f) {
    for (repl_feed **link = &g_feeds; *link; link = &(*link)->next) {
        if (*link == f) {
            *link = f->next;
            return;
        }
    }
}

/* Take over a client connection that issued PSYNC: register it as a replica,
   ship the snapshot, then stream the live backlog until the socket dies. */
static void run_replica_feed(int fd) {
    repl_feed *f = calloc(1, sizeof *f);
    if (!f) {
        close(fd);
        return;
    }
    f->fd = fd;
    f->wake = channel_create(1, 1);
    if (!f->wake) {
        free(f);
        close(fd);
        return;
    }
    f->alive = 1;
    f->sent_offset = g_master_offset; /* the live stream begins here */
    feed_link(f); /* register before snapshotting: no yield in between */

    buf_t snap = {0};
    store_snapshot(&snap);
    /* "+FULLRESYNC " + 40-char replid + " " + offset + "\r\n$" + snaplen +
       "\r\n": large offsets/snapshots push this past a 96-byte buffer, and
       snprintf would return the untruncated length, over-reading on write. */
    char hdr[128];
    int hn = snprintf(hdr, sizeof hdr, "+FULLRESYNC %s %ld\r\n$%zu\r\n",
                      g_replid, g_master_offset, snap.len);
    int ok = write_all(fd, hdr, (size_t)hn) == 0 &&
             (snap.len == 0 || write_all(fd, snap.data, snap.len) == 0);
    buf_free(&snap);
    /* The snapshot covers everything up to sent_offset; only now may WAIT count
       this replica (before this point it has none of the data on its socket).
     */
    f->synced = ok;

    while (ok && f->alive && !g_shutdown) {
        if (f->backlog.len == 0) {
            f->waiting = 1;
            char d;
            channel_recv(f->wake, &d); /* parks until propagate() signals */
            f->waiting = 0;
            continue;
        }
        /* Swap the backlog out so propagate() can keep appending while we
           write the bytes we already have. */
        buf_t s = f->backlog;
        f->backlog = (buf_t){0};
        if (write_all(fd, s.data, s.len) != 0) {
            buf_free(&s);
            break;
        }
        f->sent_offset += (long)s.len;
        buf_free(&s);
    }

    f->alive = 0;
    feed_unlink(f);
    channel_destroy(f->wake);
    buf_free(&f->backlog);
    free(f);
    close(fd);
}

/* --- replica side ----------------------------------------------------------
 */

static buf_t g_apply_scratch; /* throwaway reply sink for applied commands */

/* Apply every complete command in in[0..limit) and return bytes consumed. */
static size_t apply_commands(buf_t *in, size_t limit) {
    client_t cl = {&g_apply_scratch, 1};
    arg_t argv[MAX_ARGS];
    size_t pos = 0;
    for (;;) {
        int argc;
        size_t used;
        int r = parse_cmd(in->data + pos, limit - pos, argv, &argc, &used);
        if (r <= 0)
            break;
        pos += used;
        g_apply_scratch.len = 0;
        if (argc > 0)
            exec_cmd(argv, argc, &cl);
    }
    return pos;
}

static int repl_fill(int fd, buf_t *b) {
    if (buf_reserve(b, 16384) != 0)
        return -1;
    int n = coro_read(fd, b->data + b->len, b->cap - b->len);
    if (n > 0)
        b->len += (size_t)n;
    return n;
}

/* Consume one CRLF-terminated line from the front of b (its contents are not
   needed - just skipped). */
static int repl_read_line(int fd, buf_t *b) {
    for (;;) {
        for (size_t i = 0; i + 1 < b->len; i++)
            if (b->data[i] == '\r' && b->data[i + 1] == '\n') {
                buf_consume(b, i + 2);
                return 0;
            }
        if (repl_fill(fd, b) <= 0)
            return -1;
    }
}

/* Parse and consume a "$<n>\r\n" bulk header from the front of b. */
static int repl_read_bulklen(int fd, buf_t *b, long *out) {
    for (;;) {
        if (b->len >= 1) {
            if (b->data[0] != '$')
                return -1;
            size_t i = 1;
            while (i + 1 < b->len &&
                   !(b->data[i] == '\r' && b->data[i + 1] == '\n'))
                i++;
            if (i + 1 < b->len && b->data[i] == '\r' &&
                b->data[i + 1] == '\n') {
                long v = 0;
                for (size_t j = 1; j < i; j++) {
                    if (b->data[j] < '0' || b->data[j] > '9')
                        return -1;
                    v = v * 10 + (b->data[j] - '0');
                }
                *out = v;
                buf_consume(b, i + 2);
                return 0;
            }
        }
        if (repl_fill(fd, b) <= 0)
            return -1;
    }
}

static int repl_dial(const char *host, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;
    struct sockaddr_in a;
    memset(&a, 0, sizeof a);
    a.sin_family = AF_INET;
    a.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &a.sin_addr) != 1) {
        if (strcmp(host, "localhost") == 0)
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

/* The replica's link to its primary: (re)connect, PSYNC, load the snapshot,
   then apply the live command stream. Retries on disconnect, and stops
   following promptly when REPLICAOF NO ONE cuts g_master_fd. */
static void master_link(void *arg) {
    (void)arg;
    buf_t in = {0};
    while (!g_shutdown) {
        if (!g_is_replica) {
            coro_sleep(200);
            continue;
        }
        int gen = g_master_gen;
        char host[256];
        snprintf(host, sizeof host, "%s", g_master_host);
        int port = g_master_port;

        int fd = repl_dial(host, port);
        if (fd < 0) {
            g_master_link_up = 0;
            coro_sleep(500);
            continue;
        }
        g_master_fd = fd;
        in.len = 0;

        const char *psync = "*1\r\n$5\r\nPSYNC\r\n";
        long snlen = -1;
        int alive = write_all(fd, psync, strlen(psync)) == 0 &&
                    repl_read_line(fd, &in) == 0 && /* +FULLRESYNC line */
                    repl_read_bulklen(fd, &in, &snlen) == 0 && snlen >= 0 &&
                    (unsigned long)snlen <= REPL_SNAPSHOT_MAX;

        if (alive) {
            while (in.len < (size_t)snlen)
                if (repl_fill(fd, &in) <= 0) {
                    alive = 0;
                    break;
                }
        }
        if (alive) {
            apply_commands(&in, (size_t)snlen); /* load the snapshot */
            buf_consume(&in, (size_t)snlen);
            g_master_link_up = 1;
            for (;;) {
                buf_consume(&in, apply_commands(&in, in.len));
                if (g_shutdown || !g_is_replica || gen != g_master_gen)
                    break;
                if (repl_fill(fd, &in) <= 0)
                    break;
            }
        }

        g_master_fd = -1;
        g_master_link_up = 0;
        close(fd);
        if (!g_shutdown && g_is_replica)
            coro_sleep(300); /* backoff before reconnecting */
    }
    buf_free(&in);
}

static void start_master_link(void) {
    if (g_link_started)
        return;
    g_link_started = 1;
    coro_create(master_link, NULL, 256UL * 1024);
}

static void conn(void *arg) {
    int fd = (int)(intptr_t)arg;
    int slot = g_conn_n < MAX_CONNS ? g_conn_n : -1;
    if (slot >= 0)
        g_conn_fds[g_conn_n++] = fd;
    g_stats.active++;

    buf_t in = {0}, out = {0};
    arg_t argv[MAX_ARGS];
    client_t cl = {&out, 0};

    for (;;) {
        if (buf_reserve(&in, 16384) != 0)
            break;
        int n = coro_read(fd, in.data + in.len, in.cap - in.len);
        if (n <= 0)
            break;
        in.len += (size_t)n;

        /* Parse and execute every complete command in the buffer. */
        size_t pos = 0;
        int keep_open = 1;
        for (;;) {
            int argc;
            size_t consumed;
            int r =
                parse_cmd(in.data + pos, in.len - pos, argv, &argc, &consumed);
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
            /* PSYNC turns this connection into a replication feed: flush any
               pending reply, release our connection bookkeeping, and hand the
               socket to the feeder (which owns it from here, close included).
             */
            if (arg_eq(&argv[0], "psync") || arg_eq(&argv[0], "sync")) {
                if (out.len)
                    write_all(fd, out.data, out.len);
                buf_free(&in);
                buf_free(&out);
                if (slot >= 0)
                    g_conn_fds[slot] = -1;
                g_stats.active--;
                run_replica_feed(fd);
                return;
            }
            if (exec_cmd(argv, argc, &cl) == 0)
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
    if (slot >= 0)
        g_conn_fds[slot] = -1;
    g_stats.active--;
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
        g_stats.connections++;
        if (!coro_create(conn, (void *)(intptr_t)cfd, 256UL * 1024))
            close(cfd);
    }
}

static void supervisor(void *arg) {
    (void)arg;
    uint64_t last_cmds = 0;
    uint64_t last = now_ms();
    while (!g_shutdown) {
        coro_sleep(200);
        uint64_t t = now_ms();
        if (t - last >= 5000) {
            uint64_t dc = g_stats.commands - last_cmds;
            fprintf(stderr,
                    "[stats] keys=%zu clients=%" PRIu64 " total_conns=%" PRIu64
                    " cmds=%" PRIu64 " (%.0f cmd/s)\n",
                    g_store.count, g_stats.active, g_stats.connections,
                    g_stats.commands, (double)dc * 1000.0 / (double)(t - last));
            last_cmds = g_stats.commands;
            last = t;
        }
    }
    fprintf(stderr, "\n[shutdown] draining %" PRIu64 " client(s)...\n",
            g_stats.active);
    if (g_listen_fd >= 0)
        shutdown(g_listen_fd, SHUT_RDWR);
    for (int i = 0; i < g_conn_n; i++)
        if (g_conn_fds[i] >= 0)
            shutdown(g_conn_fds[i], SHUT_RDWR);
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

/* Fill g_replid with 40 hex digits derived from pid+time, like Redis's runid.
 */
static void make_replid(void) {
    unsigned long s = (unsigned long)getpid() ^ (now_ms() << 1);
    static const char hex[] = "0123456789abcdef";
    for (int i = 0; i < 40; i++) {
        s = s * 6364136223846793005UL + 1442695040888963407UL;
        g_replid[i] = hex[(s >> 33) & 0xF];
    }
    g_replid[40] = 0;
}

int main(int argc, char **argv) {
    int port = 6380;
    const char *replicaof = NULL; /* "host:port" if started as a replica */
    int opt;
    while ((opt = getopt(argc, argv, "p:r:h")) != -1) {
        switch (opt) {
        case 'p':
            port = atoi(optarg);
            if (port <= 0 || port > 65535) {
                fprintf(stderr, "invalid port: %s\n", optarg);
                return 1;
            }
            break;
        case 'r':
            replicaof = optarg;
            break;
        case 'h':
        default:
            fprintf(stderr,
                    "usage: %s [-p port] [-r master_host:port]\n"
                    "       (default port 6380; -r makes this a replica)\n",
                    argv[0]);
            return opt == 'h' ? 0 : 1;
        }
    }

    make_replid();

    /* -r host:port starts this node as a replica of the given primary. */
    if (replicaof) {
        const char *colon = strrchr(replicaof, ':');
        if (!colon || colon == replicaof ||
            (size_t)(colon - replicaof) >= sizeof g_master_host) {
            fprintf(stderr, "invalid -r value '%s' (want host:port)\n",
                    replicaof);
            return 1;
        }
        size_t hlen = (size_t)(colon - replicaof);
        memcpy(g_master_host, replicaof, hlen);
        g_master_host[hlen] = 0;
        g_master_port = atoi(colon + 1);
        if (g_master_port <= 0 || g_master_port > 65535) {
            fprintf(stderr, "invalid master port in '%s'\n", replicaof);
            return 1;
        }
        g_is_replica = 1;
    }

    signal(SIGPIPE, SIG_IGN);
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = on_sigint;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    store_init(1024);
    g_listen_fd = make_listener(port);
    if (g_listen_fd < 0)
        return 1;

    printf("microdb listening on port %d (pid %d) - RESP/Redis compatible%s\n",
           port, (int)getpid(), g_is_replica ? " [replica]" : "");
    if (g_is_replica)
        printf("replicating from %s:%d\n", g_master_host, g_master_port);
    fflush(stdout);

    if (!coro_create(supervisor, NULL, 0) ||
        !coro_create(acceptor, (void *)(intptr_t)g_listen_fd, 0)) {
        fprintf(stderr, "failed to start coroutines\n");
        return 1;
    }
    if (g_is_replica)
        start_master_link();
    coro_run();

    close(g_listen_fd);
    store_flush();
    free(g_store.buckets);
    printf("[shutdown] complete: %" PRIu64 " commands served\n",
           g_stats.commands);
    return 0;
}
