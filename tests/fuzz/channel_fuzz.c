/* Model-based fuzz target for channels (ported from the full-scale sibling,
 * adapted to this runtime's pointer-slot channel API).
 *
 * Each input byte is decoded into a send (with a small value) or a receive.
 * A reference FIFO mirrors the channel; every received value is checked
 * against what the model says should come out, so any ordering or buffering
 * bug trips a failure. To stay single-coroutine and never block, we only
 * send when the model says the ring is not full and only receive when it is
 * not empty -- parking paths are exercised by sched_fuzz instead.
 *
 * Values are encoded in the pointer slot: (void*)(intptr_t)value.
 *
 * Build standalone (default): a deterministic driver replays many pseudo-
 * random inputs. Build with -DFUZZ_LIBFUZZER to expose a libFuzzer entry
 * point instead.
 */
#define _GNU_SOURCE
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../include/coro.h"

#define CAP 32

static const uint8_t *g_data;
static size_t g_len;
static int g_fail;

int channel_fuzz_one(const uint8_t *data, size_t len);

#ifdef FUZZ_LIBFUZZER
int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    return channel_fuzz_one(data, size);
}
#endif

static void driver(void *arg)
{
    (void)arg;
    chan_t *ch = chan_new(CAP);
    if (!ch) {
        g_fail = 1;
        return;
    }

    int model[CAP]; /* reference FIFO mirroring the channel */
    memset(model, 0, sizeof(model));
    int head = 0, count = 0;

    for (size_t i = 0; i < g_len && !g_fail; i++) {
        uint8_t b = g_data[i];
        int want_send = b & 1;
        int val = (int)(b >> 1); /* 0..127 */

        if (want_send && count < CAP) {
            chan_send(ch, (void *)(intptr_t)val);
            model[(head + count) % CAP] = val;
            count++;
        } else if (count > 0) {
            void *out;
            chan_recv(ch, &out);
            int expected = model[head];
            head = (head + 1) % CAP;
            count--;
            if ((int)(intptr_t)out != expected)
                g_fail = 1;
        }
    }

    /* Drain whatever remains and confirm it comes out in order. */
    while (count > 0 && !g_fail) {
        void *out;
        chan_recv(ch, &out);
        int expected = model[head];
        head = (head + 1) % CAP;
        count--;
        if ((int)(intptr_t)out != expected)
            g_fail = 1;
    }
    chan_free(ch);
}

int channel_fuzz_one(const uint8_t *data, size_t len)
{
    g_data = data;
    g_len = len;
    g_fail = 0;
    coro_init();               /* idempotent across iterations */
    coro_create(driver, NULL);
    coro_run();
    return g_fail;
}

#ifndef FUZZ_LIBFUZZER
/* xorshift64 driver producing the pseudo-random inputs. */
static uint64_t next(uint64_t *s)
{
    uint64_t x = *s;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    return *s = x;
}

int main(int argc, char **argv)
{
    int iters = (argc > 1) ? atoi(argv[1]) : 20000;
    uint64_t seed =
        (argc > 2) ? strtoull(argv[2], NULL, 10) : 0xfeedface12345678ull;
    if (iters <= 0)
        iters = 1;

    static uint8_t buf[256];
    uint64_t s = seed ? seed : 1;
    for (int it = 0; it < iters; it++) {
        size_t len = 1 + (size_t)(next(&s) % sizeof(buf));
        for (size_t i = 0; i < len; i++)
            buf[i] = (uint8_t)next(&s);
        if (channel_fuzz_one(buf, len) != 0) {
            fprintf(stderr, "channel_fuzz: FAIL at iteration %d\n", it);
            return 1;
        }
    }
    printf("channel_fuzz: PASS  %d iterations\n", iters);
    return 0;
}
#endif /* !FUZZ_LIBFUZZER */
