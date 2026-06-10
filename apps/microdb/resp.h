#ifndef MICRODB_RESP_H
#define MICRODB_RESP_H

/*
 * Shared RESP wire helpers and a tiny growable buffer, used by both microdb and
 * microdb-proxy. A single home for the protocol parser means a fix lands in one
 * place rather than two.
 *
 * Everything here is `static inline`: each translation unit that includes the
 * header gets its own copy with no link conflicts, and an unused inline (e.g. a
 * reply builder one of the two programs never calls) draws no warning.
 */

#include <ctype.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------------- *
 * Growable byte buffer (binary-safe, like a tiny SDS).
 * ------------------------------------------------------------------------- */
typedef struct {
    char *data;
    size_t len;
    size_t cap;
} buf_t;

static inline int buf_reserve(buf_t *b, size_t extra) {
    if (b->len + extra <= b->cap)
        return 0;
    size_t cap = b->cap ? b->cap : 256;
    while (cap < b->len + extra)
        cap *= 2;
    char *p = realloc(b->data, cap);
    if (!p)
        return -1;
    b->data = p;
    b->cap = cap;
    return 0;
}

static inline void buf_append(buf_t *b, const void *p, size_t n) {
    if (buf_reserve(b, n) == 0) {
        memcpy(b->data + b->len, p, n);
        b->len += n;
    }
}

static inline void buf_puts(buf_t *b, const char *s) {
    buf_append(b, s, strlen(s));
}

static inline void buf_free(buf_t *b) {
    free(b->data);
    b->data = NULL;
    b->len = b->cap = 0;
}

/* Drop the first n consumed bytes, sliding the remainder to the front. */
static inline void buf_consume(buf_t *b, size_t n) {
    if (n >= b->len) {
        b->len = 0;
    } else {
        memmove(b->data, b->data + n, b->len - n);
        b->len -= n;
    }
}

/* ------------------------------------------------------------------------- *
 * FNV-1a, used for both keyspace bucketing and shard routing.
 * ------------------------------------------------------------------------- */
static inline uint64_t fnv1a(const char *s, size_t n) {
    uint64_t h = 1469598103934665603ull;
    for (size_t i = 0; i < n; i++) {
        h ^= (unsigned char)s[i];
        h *= 1099511628211ull;
    }
    return h;
}

/* ------------------------------------------------------------------------- *
 * RESP request parsing.
 * ------------------------------------------------------------------------- */
#define MAX_ARGS 1024
#define MAX_BULK (256L * 1024 * 1024)

typedef struct {
    const char *ptr;
    size_t len;
} arg_t;

/* Parse "<digits>\r\n" starting at *p. 1=ok, 0=incomplete, -1=error. */
static inline int read_long(const char *buf, size_t n, size_t *p, long *out) {
    size_t i = *p;
    int neg = 0, any = 0;
    long v = 0;
    if (i < n && buf[i] == '-') {
        neg = 1;
        i++;
    }
    while (i < n && buf[i] >= '0' && buf[i] <= '9') {
        v = v * 10 + (buf[i] - '0');
        i++;
        any = 1;
    }
    if (i >= n)
        return 0; /* need at least the '\r' */
    if (buf[i] != '\r')
        return -1;
    if (i + 1 >= n)
        return 0; /* need the '\n' */
    if (buf[i + 1] != '\n' || !any)
        return -1;
    *p = i + 2;
    *out = neg ? -v : v;
    return 1;
}

/* Parse one command from buf[0..n); returns 1 on a complete command (filling
   argv/argc/consumed), 0 if more data is needed, -1 on a protocol error.
   Handles both RESP multibulk (*) and inline space-separated commands. */
static inline int parse_cmd(const char *buf, size_t n, arg_t *argv, int *argc,
                            size_t *consumed) {
    if (n == 0)
        return 0;

    if (buf[0] != '*') {
        /* Inline command: a single \n-terminated line split on spaces. */
        size_t i = 0;
        while (i < n && buf[i] != '\n')
            i++;
        if (i == n)
            return 0;
        size_t end = (i > 0 && buf[i - 1] == '\r') ? i - 1 : i;
        int ac = 0;
        size_t j = 0;
        while (j < end) {
            while (j < end && buf[j] == ' ')
                j++;
            if (j >= end)
                break;
            size_t s = j;
            while (j < end && buf[j] != ' ')
                j++;
            if (ac < MAX_ARGS) {
                argv[ac].ptr = buf + s;
                argv[ac].len = j - s;
                ac++;
            }
        }
        *argc = ac;
        *consumed = i + 1;
        return ac > 0 ? 1 : -1;
    }

    /* Multibulk: *<count>\r\n  then count x  $<len>\r\n<bytes>\r\n */
    size_t p = 1;
    long count;
    int r = read_long(buf, n, &p, &count);
    if (r <= 0)
        return r;
    if (count < 0 || count > MAX_ARGS)
        return -1;

    int ac = 0;
    for (long k = 0; k < count; k++) {
        if (p >= n)
            return 0;
        if (buf[p] != '$')
            return -1;
        p++;
        long len;
        r = read_long(buf, n, &p, &len);
        if (r <= 0)
            return r;
        if (len < 0 || len > MAX_BULK)
            return -1;
        if (p + (size_t)len + 2 > n)
            return 0; /* bytes + trailing CRLF not all here yet */
        argv[ac].ptr = buf + p;
        argv[ac].len = (size_t)len;
        ac++;
        p += (size_t)len;
        if (buf[p] != '\r' || buf[p + 1] != '\n')
            return -1;
        p += 2;
    }
    *argc = ac;
    *consumed = p;
    return 1;
}

/* ------------------------------------------------------------------------- *
 * RESP reply builders.
 * ------------------------------------------------------------------------- */
static inline void reply_simple(buf_t *o, const char *s) {
    buf_append(o, "+", 1);
    buf_puts(o, s);
    buf_append(o, "\r\n", 2);
}

static inline void reply_error(buf_t *o, const char *s) {
    buf_append(o, "-", 1);
    buf_puts(o, s);
    buf_append(o, "\r\n", 2);
}

static inline void reply_int(buf_t *o, long long n) {
    char tmp[32];
    int k = snprintf(tmp, sizeof tmp, ":%lld\r\n", n);
    buf_append(o, tmp, (size_t)k);
}

static inline void reply_nil(buf_t *o) {
    buf_append(o, "$-1\r\n", 5);
}

static inline void reply_bulk(buf_t *o, const char *p, size_t n) {
    char hdr[32];
    int k = snprintf(hdr, sizeof hdr, "$%zu\r\n", n);
    buf_append(o, hdr, (size_t)k);
    buf_append(o, p, n);
    buf_append(o, "\r\n", 2);
}

static inline void reply_array_header(buf_t *o, long long n) {
    char hdr[32];
    int k = snprintf(hdr, sizeof hdr, "*%lld\r\n", n);
    buf_append(o, hdr, (size_t)k);
}

/* Re-encode a parsed command back into RESP multibulk form. */
static inline void resp_encode_command(buf_t *o, const arg_t *argv, int argc) {
    char h[32];
    int k = snprintf(h, sizeof h, "*%d\r\n", argc);
    buf_append(o, h, (size_t)k);
    for (int i = 0; i < argc; i++) {
        k = snprintf(h, sizeof h, "$%zu\r\n", argv[i].len);
        buf_append(o, h, (size_t)k);
        buf_append(o, argv[i].ptr, argv[i].len);
        buf_append(o, "\r\n", 2);
    }
}

/* ------------------------------------------------------------------------- *
 * Argument helpers.
 * ------------------------------------------------------------------------- */
/* Case-insensitive compare of an argument against a NUL-terminated keyword. */
static inline int arg_eq(const arg_t *a, const char *s) {
    size_t n = strlen(s);
    if (a->len != n)
        return 0;
    for (size_t i = 0; i < n; i++)
        if (tolower((unsigned char)a->ptr[i]) != tolower((unsigned char)s[i]))
            return 0;
    return 1;
}

#endif /* MICRODB_RESP_H */
