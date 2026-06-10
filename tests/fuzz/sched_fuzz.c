/* Scheduler fuzzer.
 *
 * Each iteration builds a random producer/consumer workload - random channel
 * capacity, random numbers of producers and consumers, random item counts, and
 * random interleaving of yields and mutex-protected increments - then runs it
 * to completion and checks invariants that a correct cooperative scheduler must
 * uphold:
 *
 *   1. Every produced item is consumed exactly once (no lost item).
 *   2. Every coroutine reaches its end (no lost wake-up / no deadlock). The
 *      scheduler returns when nothing is runnable, so a lost wake-up shows up
 * as a coroutine that never completed rather than as a hang.
 *   3. The mutex-protected counter equals the number of increments attempted
 *      (no lost update across the yields held inside the critical section).
 *
 * The workload is deadlock-free by construction (balanced sends/receives with a
 * sentinel per consumer), so any invariant violation is a scheduler bug. This
 * extends the channel fuzzer from data-structure checking to control-flow
 * checking of yield / block / wake.
 *
 * Usage: sched_fuzz [iterations] [seed]
 */
#define _GNU_SOURCE
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "channel.h"
#include "coro.h"
#include "sync.h"

#define SENTINEL (-1)

typedef struct {
    uint64_t s;
} rng;

static uint64_t rnd(rng *r) {
    uint64_t x = r->s;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    return r->s = x;
}

/* Per-run shared state. */
static channel_t *g_ch;
static coro_mutex_t g_mtx;
static uint64_t g_seed;
static int g_nprod, g_ncons, g_items;
static int g_producers_done;
static long g_consumed;
static long g_counter;  /* incremented under the mutex */
static long g_expected; /* increments attempted (no lock) */
static int g_completed; /* coroutines that reached their end */

/* A mutex-protected increment with a yield held inside the critical section:
   correct only if the mutex truly excludes other coroutines. */
static void do_inc(rng *r) {
    g_expected++; /* exact: plain op, no yield around it */
    coro_mutex_lock(&g_mtx);
    long t = g_counter;
    if (rnd(r) & 1)
        coro_yield();
    g_counter = t + 1;
    coro_mutex_unlock(&g_mtx);
}

static void producer(void *arg) {
    rng r = {g_seed ^ (0x9e3779b97f4a7c15ull * (uint64_t)(long)arg + 1)};
    for (int i = 0; i < g_items; i++) {
        int v = 1;
        channel_send(g_ch, &v);
        if (rnd(&r) % 3 == 0)
            coro_yield();
        if (rnd(&r) % 4 == 0)
            do_inc(&r);
    }
    /* The last producer to finish closes the channel for each consumer. */
    if (++g_producers_done == g_nprod) {
        int s = SENTINEL;
        for (int j = 0; j < g_ncons; j++)
            channel_send(g_ch, &s);
    }
    g_completed++;
}

static void consumer(void *arg) {
    rng r = {g_seed ^ (0xd1b54a32d192ed03ull * (uint64_t)(long)arg + 7)};
    for (;;) {
        int v;
        channel_recv(g_ch, &v);
        if (v == SENTINEL)
            break;
        g_consumed++;
        if (rnd(&r) % 3 == 0)
            coro_yield();
        if (rnd(&r) % 4 == 0)
            do_inc(&r);
    }
    g_completed++;
}

/* Returns 0 on success, or a nonzero code identifying the failed invariant. */
static int run_one(uint64_t seed) {
    rng r = {seed ? seed : 1};
    g_seed = seed;
    int cap = 1 + (int)(rnd(&r) % 8);
    g_nprod = 1 + (int)(rnd(&r) % 8);
    g_ncons = 1 + (int)(rnd(&r) % 8);
    g_items = 1 + (int)(rnd(&r) % 16);
    g_producers_done = 0;
    g_consumed = 0;
    g_counter = 0;
    g_expected = 0;
    g_completed = 0;

    g_ch = channel_create((size_t)cap, sizeof(int));
    if (!g_ch)
        return -1;
    coro_mutex_init(&g_mtx);

    for (long i = 0; i < g_nprod; i++)
        coro_create(producer, (void *)i, 48 * 1024);
    for (long i = 0; i < g_ncons; i++)
        coro_create(consumer, (void *)i, 48 * 1024);
    coro_run();
    channel_destroy(g_ch);

    long total_items = (long)g_nprod * g_items;
    int total_coros = g_nprod + g_ncons;
    if (g_consumed != total_items)
        return 1; /* lost or duplicated item */
    if (g_completed != total_coros)
        return 2; /* a coroutine never woke up */
    if (g_counter != g_expected)
        return 3; /* mutex lost an update */
    return 0;
}

int main(int argc, char **argv) {
    int iters = (argc > 1) ? atoi(argv[1]) : 20000;
    uint64_t seed =
        (argc > 2) ? strtoull(argv[2], NULL, 10) : 0x0123456789abcdefull;
    if (iters <= 0)
        iters = 1;

    rng r = {seed ? seed : 1};
    for (int it = 0; it < iters; it++) {
        uint64_t s = rnd(&r);
        int rc = run_one(s);
        if (rc != 0) {
            fprintf(
                stderr,
                "sched_fuzz: FAIL at iteration %d (seed %llu), invariant %d\n",
                it, (unsigned long long)s, rc);
            return 1;
        }
    }
    printf("sched_fuzz: PASS  %d iterations\n", iters);
    return 0;
}
