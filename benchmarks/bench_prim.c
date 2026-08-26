/* bench_prim -- micro-benchmarks for the runtime's core primitives.
 *
 * Three workloads, each printing one CSV line:
 *
 *   switch   two coroutines ping-ponging via coro_yield: measures a full
 *            context switch + scheduler round-trip (pop/push/swap).
 *   chan     one producer, one consumer, capacity-1 channel: every item is
 *            a rendezvous (both sides park), so ops/s reflects park/wake.
 *   scale    N coroutines yielding K times each: total yields/s and a check
 *            that every coroutine ran to completion under round-robin.
 *
 * Usage: bench_prim [switch_ops] [chan_ops] [scale_coros] [scale_iters]
 */
#define _GNU_SOURCE
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "../include/coro.h"

static double now_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1e6 + (double)ts.tv_nsec / 1e3;
}

/* ------------------------------------------------------------ switch ---- */

static volatile int g_turn;

static void sw_a(void *arg)
{
    long n = (long)arg;
    for (long i = 0; i < n; i++) {
        g_turn = 1;
        coro_yield();
    }
}

static void sw_b(void *arg)
{
    long n = (long)arg;
    for (long i = 0; i < n; i++) {
        g_turn = 0;
        coro_yield();
    }
}

/* -------------------------------------------------------------- chan ---- */

static chan_t *g_bench_ch;

static void ch_prod(void *arg)
{
    long n = (long)arg;
    for (long i = 0; i < n; i++)
        chan_send(g_bench_ch, (void *)(intptr_t)(i + 1));
}

static void ch_cons(void *arg)
{
    long n = (long)arg;
    void *v;
    long sum = 0; /* consume everything: no dead code */
    for (long i = 0; i < n; i++) {
        chan_recv(g_bench_ch, &v);
        sum += (intptr_t)v;
    }
    if (sum != n * (n + 1) / 2) {
        fprintf(stderr, "bench_prim: chan checksum mismatch\n");
        exit(1);
    }
}

/* ------------------------------------------------------------- scale ---- */

static long g_scale_yields;
static int g_scale_done;

static void scaler(void *arg)
{
    long iters = (long)arg;
    for (long i = 0; i < iters; i++) {
        coro_yield();
        g_scale_yields++;
    }
    g_scale_done++;
}

int main(int argc, char **argv)
{
    long sw_ops     = (argc > 1) ? atol(argv[1]) : 2000000;
    long chan_ops   = (argc > 2) ? atol(argv[2]) : 500000;
    long scale_n    = (argc > 3) ? atol(argv[3]) : 1000;
    long scale_iter = (argc > 4) ? atol(argv[4]) : 1000;

    coro_init();
    printf("primitive,ops,seconds,ops_per_sec\n");

    /* switch: n ping-pong rounds = 2n switches. */
    {
        double t0 = now_us();
        coro_create(sw_a, (void *)sw_ops);
        coro_create(sw_b, (void *)sw_ops);
        coro_run();
        double dt = (now_us() - t0) / 1e6;
        long total = 2 * sw_ops;
        printf("switch,%ld,%.4f,%.0f\n", total, dt, total / dt);
    }

    /* chan: n items through a capacity-1 ring = n parked handoffs. */
    {
        g_bench_ch = chan_new(1);
        double t0 = now_us();
        coro_create(ch_prod, (void *)chan_ops);
        coro_create(ch_cons, (void *)chan_ops);
        coro_run();
        double dt = (now_us() - t0) / 1e6;
        printf("chan_rendezvous,%ld,%.4f,%.0f\n", chan_ops, dt,
               chan_ops / dt);
        chan_free(g_bench_ch);
    }

    /* scale: N coros x K yields each. */
    {
        g_scale_yields = 0;
        g_scale_done = 0;
        double t0 = now_us();
        for (long i = 0; i < scale_n; i++)
            coro_create(scaler, (void *)scale_iter);
        coro_run();
        double dt = (now_us() - t0) / 1e6;
        long total = scale_n * scale_iter;
        if (g_scale_yields != total || g_scale_done != scale_n) {
            fprintf(stderr, "bench_prim: lost coroutines under scale\n");
            return 1;
        }
        printf("scale_%ldcoros,%ld,%.4f,%.0f\n", scale_n, total, dt,
               total / dt);
    }

    return 0;
}
