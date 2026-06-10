/* Stress test: many coroutines bouncing messages through a bounded channel and
 * hammering a shared mutex under contention, repeated for several rounds. It
 * checks hard invariants - every produced item is consumed exactly once and
 * every lock-protected increment lands - so a deadlock or lost wakeup shows up
 * as a mismatch (the scheduler returns once everything is blocked) rather than
 * a hang. Designed to also run cleanly under AddressSanitizer.
 *
 * Usage: stress [rounds]   (default 20)
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "channel.h"
#include "coro.h"
#include "sync.h"

#define PRODUCERS 400
#define CONSUMERS 100
#define ITEMS_PER_PRODUCER 200
#define CONTENDERS 500
#define LOCKS_PER_CONTENDER 200
#define CHAN_CAP 64
#define STACK (64 * 1024)
#define SENTINEL (-1)

static channel_t *g_chan;
static int g_producers_done;
static long g_consumed;
static coro_mutex_t g_mtx;
static long g_counter;

static void producer(void *arg) {
    long id = (long)arg;
    for (int i = 0; i < ITEMS_PER_PRODUCER; i++) {
        int v = (int)((id * 1000 + i) & 0x3fffffff); /* positive, != SENTINEL */
        channel_send(g_chan, &v);
    }
    /* The producer that finishes last closes the channel for each consumer. */
    if (++g_producers_done == PRODUCERS) {
        int s = SENTINEL;
        for (int j = 0; j < CONSUMERS; j++)
            channel_send(g_chan, &s);
    }
}

static void consumer(void *arg) {
    (void)arg;
    for (;;) {
        int v;
        channel_recv(g_chan, &v);
        if (v == SENTINEL)
            break;
        g_consumed++;
        if ((g_consumed & 7) == 0)
            coro_yield(); /* extra interleaving */
    }
}

static void contender(void *arg) {
    (void)arg;
    for (int i = 0; i < LOCKS_PER_CONTENDER; i++) {
        coro_mutex_lock(&g_mtx);
        long t = g_counter;
        coro_yield(); /* force contention inside the critical section */
        g_counter = t + 1;
        coro_mutex_unlock(&g_mtx);
    }
}

static int run_round(void) {
    g_producers_done = 0;
    g_consumed = 0;
    g_counter = 0;

    g_chan = channel_create(CHAN_CAP, sizeof(int));
    if (!g_chan) {
        fprintf(stderr, "channel_create failed\n");
        return -1;
    }
    coro_mutex_init(&g_mtx);

    for (long i = 0; i < PRODUCERS; i++)
        coro_create(producer, (void *)i, STACK);
    for (long i = 0; i < CONSUMERS; i++)
        coro_create(consumer, NULL, STACK);
    for (long i = 0; i < CONTENDERS; i++)
        coro_create(contender, NULL, STACK);

    coro_run();
    channel_destroy(g_chan);
    g_chan = NULL;

    long exp_items = (long)PRODUCERS * ITEMS_PER_PRODUCER;
    long exp_locks = (long)CONTENDERS * LOCKS_PER_CONTENDER;
    if (g_consumed != exp_items) {
        fprintf(stderr, "FAIL: consumed %ld, expected %ld\n", g_consumed,
                exp_items);
        return -1;
    }
    if (g_counter != exp_locks) {
        fprintf(stderr, "FAIL: counter %ld, expected %ld\n", g_counter,
                exp_locks);
        return -1;
    }
    return 0;
}

int main(int argc, char **argv) {
    int rounds = (argc > 1) ? atoi(argv[1]) : 20;
    if (rounds <= 0)
        rounds = 1;

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    long coros = 0, items = 0, locks = 0;
    for (int r = 0; r < rounds; r++) {
        if (run_round() != 0) {
            printf("stress: FAILED at round %d/%d\n", r + 1, rounds);
            return 1;
        }
        coros += PRODUCERS + CONSUMERS + CONTENDERS;
        items += (long)PRODUCERS * ITEMS_PER_PRODUCER;
        locks += (long)CONTENDERS * LOCKS_PER_CONTENDER;
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double secs = (double)(t1.tv_sec - t0.tv_sec) +
                  (double)(t1.tv_nsec - t0.tv_nsec) / 1e9;

    printf("stress: PASS  rounds=%d coroutines=%ld channel_items=%ld "
           "mutex_ops=%ld  (%.2fs)\n",
           rounds, coros, items, locks, secs);
    return 0;
}
