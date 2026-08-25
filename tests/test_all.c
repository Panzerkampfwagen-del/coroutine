/*
 * test_all -- assert-based test runner for the coroutine runtime.
 *
 *   make test        builds and runs this binary
 *   exit code 0      all tests passed
 */
#include "../include/coro.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int g_failures;

#define CHECK(cond, name)                                              \
    do {                                                               \
        if (cond) { printf("  PASS %s\n", name); }                     \
        else      { printf("  FAIL %s (line %d)\n", name, __LINE__);    \
                    g_failures++; }                                    \
    } while (0)

#define UNUSED(x) ((void)(x))

/* --------------------------------------------------------- helpers ------ */

#define MAX_EVENTS 4096
static int g_events[MAX_EVENTS];
static int g_nevents;

static void record(int x)
{
    if (g_nevents < MAX_EVENTS) g_events[g_nevents++] = x;
}

/* ------------------------------------------- test 1: ping-pong yields --- */

static void ping(void *arg)
{
    UNUSED(arg);
    for (int i = 0; i < 5; i++) {
        record(i * 2);          /* even numbers */
        coro_yield();
    }
}

static void pong(void *arg)
{
    UNUSED(arg);
    for (int i = 0; i < 5; i++) {
        record(i * 2 + 1);      /* odd numbers */
        coro_yield();
    }
}

static void test_pingpong(void)
{
    printf("[test_pingpong]\n");
    g_nevents = 0;
    coro_init();
    coro_create(ping, NULL);
    coro_create(pong, NULL);
    coro_run();

    CHECK(g_nevents == 10, "ten events recorded");
    int ok = 1;
    for (int i = 0; i < 10; i++)
        if (g_events[i] != i) ok = 0;
    CHECK(ok, "perfect alternation ping/pong/yield order");
}

/* ------------------------------- test 2: many coroutines, shared counter */

#define N_COROS 100
#define N_INCS  1000

static int g_counter;

static void counter_worker(void *arg)
{
    UNUSED(arg);
    for (int i = 0; i < N_INCS; i++) {
        g_counter++;            /* single-threaded: no atomics needed */
        if ((i & 7) == 0) coro_yield();
    }
}

static void test_many_coros(void)
{
    printf("[test_many_coros]\n");
    g_counter = 0;
    coro_init();
    for (int i = 0; i < N_COROS; i++)
        coro_create(counter_worker, NULL);
    coro_run();
    CHECK(g_counter == N_COROS * N_INCS, "all increments landed");
    CHECK(coro_alive_count() == 0, "all coroutines reaped");
}

/* ------------------------------------------------ test 3: channel FIFO -- */

static chan_t *g_chan;
#define CHAN_N 500

static void producer(void *arg)
{
    UNUSED(arg);
    for (int i = 0; i < CHAN_N; i++) {
        chan_send(g_chan, (void *)(intptr_t)(i + 1));
        coro_yield();           /* let consumer interleave */
    }
}

static void consumer(void *arg)
{
    UNUSED(arg);
    intptr_t v;
    int got = 0;
    while (got < CHAN_N) {
        chan_recv(g_chan, (void **)&v);
        if ((int)v != got + 1) {
            CHECK(0, "channel preserves FIFO order");
            return;
        }
        got++;
        coro_yield();
    }
    CHECK(got == CHAN_N, "consumer received every item in order");
}

static void test_channel_fifo(void)
{
    printf("[test_channel_fifo]\n");
    coro_init();
    g_chan = chan_new(4);         /* small capacity => real blocking */
    coro_create(producer, NULL);
    coro_create(consumer, NULL);
    coro_run();
    chan_free(g_chan);
}

/* --------------------------------------- test 4: channel rendezvous ----- */

/* cap 1 with a slow receiver: sender must block until space frees. */
static chan_t *g_sync_chan;
static int     g_send_returned;

static void blocking_sender(void *arg)
{
    UNUSED(arg);
    chan_send(g_sync_chan, (void *)1);
    chan_send(g_sync_chan, (void *)2);   /* must block until recv drains */
    g_send_returned = 2;
    chan_send(g_sync_chan, (void *)3);
    g_send_returned = 3;
}

static void draining_receiver(void *arg)
{
    UNUSED(arg);
    void *v;
    chan_recv(g_sync_chan, &v);          /* takes buffered item 1 */
    CHECK((intptr_t)v == 1, "first value = buffered item");
    chan_recv(g_sync_chan, &v);          /* drains parked sender's 2 */
    CHECK((intptr_t)v == 2, "second value correct");
    chan_recv(g_sync_chan, &v);          /* rendezvous with sender's 3 */
    CHECK((intptr_t)v == 3, "third value correct");
    /* by now the sender must have finished both sends and exited */
    CHECK(g_send_returned == 3, "sender fully resumed");
}

static void test_channel_blocking(void)
{
    printf("[test_channel_blocking]\n");
    coro_init();
    g_sync_chan = chan_new(1);
    g_send_returned = 0;
    /*
     * Deterministic parking: sender buffers item 1 (cap 1), then sends
     * item 2 -- channel full, so it parks without ever yielding between
     * the two sends. The receiver then drains: its first recv takes the
     * buffered item, the second must rendezvous with the parked sender.
     */
    coro_create(blocking_sender, NULL);
    coro_create(draining_receiver, NULL);
    coro_run();
    chan_free(g_sync_chan);
}

/* ------------------------------------------------------ test 5: mutex --- */

static coro_mutex_t *g_mtx;
static int g_cs_violations;
static int g_in_cs;
static int g_mtx_count;

static void mutex_user(void *arg)
{
    UNUSED(arg);
    for (int i = 0; i < 200; i++) {
        mutex_lock(g_mtx);
        if (g_in_cs) g_cs_violations++;   /* mutual exclusion broken! */
        g_in_cs++;
        if ((i & 3) == 0) coro_yield();   /* yield INSIDE critical section */
        g_in_cs--;
        mutex_unlock(g_mtx);
    }
    g_mtx_count++;
}

static void test_mutex_mutual_exclusion(void)
{
    printf("[test_mutex]\n");
    coro_init();
    g_mtx = mutex_new();
    g_in_cs = 0; g_cs_violations = 0; g_mtx_count = 0;
    for (int i = 0; i < 8; i++)
        coro_create(mutex_user, NULL);
    coro_run();
    CHECK(g_cs_violations == 0, "mutual exclusion held across yields");
    CHECK(g_mtx_count == 8, "all workers finished");
    mutex_free(g_mtx);
}

/* --------------------------------------------- test 6: stress + reap ---- */

static void spinner(void *arg)
{
    UNUSED(arg);
    for (int i = 0; i < 10000; i++) coro_yield();
}

static void test_stress_reap(void)
{
    printf("[test_stress]\n");
    coro_init();
    for (int i = 0; i < 50; i++) coro_create(spinner, NULL);
    coro_run();
    CHECK(coro_alive_count() == 0, "50 spinners x 10000 yields reaped");
}

int main(int argc, char **argv)
{
    int only = argc > 1 ? atoi(argv[1]) : 0;   /* run one test by number */

    struct { int id; const char *name; void (*fn)(void); } tests[] = {
        { 1, "pingpong",   test_pingpong },
        { 2, "many_coros", test_many_coros },
        { 3, "chan_fifo",  test_channel_fifo },
        { 4, "chan_block", test_channel_blocking },
        { 5, "mutex",      test_mutex_mutual_exclusion },
        { 6, "stress",     test_stress_reap },
    };

    for (size_t i = 0; i < sizeof(tests)/sizeof(tests[0]); i++) {
        if (only && tests[i].id != only) continue;
        tests[i].fn();
    }

    if (g_failures) {
        printf("\n%d FAILURE(S)\n", g_failures);
        return 1;
    }
    printf("\nALL TESTS PASSED\n");
    return 0;
}
