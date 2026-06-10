/* Model-based fuzz target for channels.
 *
 * Each input byte is decoded into a send (with a small value) or a receive. A
 * reference FIFO mirrors the channel; every received value is checked against
 * what the model says should come out, so any ordering or buffering bug trips
 * an assertion. To stay single-coroutine and never block, we only send when the
 * channel is not full and only receive when it is not empty.
 *
 * Build standalone (default): a deterministic driver replays many pseudo-random
 * inputs. Build with -DFUZZ_LIBFUZZER to expose LLVMFuzzerTestOneInput instead.
 */
#define _GNU_SOURCE
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "channel.h"
#include "coro.h"

#define CAP 32

static const uint8_t *g_data;
static size_t g_len;
static int g_fail;

static void driver(void *arg) {
    (void)arg;
    channel_t *ch = channel_create(CAP, sizeof(int));
    if (!ch) {
        g_fail = 1;
        return;
    }

    int model[CAP]; /* reference FIFO mirroring the channel */
    int head = 0, count = 0;

    for (size_t i = 0; i < g_len && !g_fail; i++) {
        uint8_t b = g_data[i];
        int want_send = b & 1;
        int val = (int)(b >> 1); /* 0..127 */

        if (want_send && count < CAP) {
            channel_send(ch, &val);
            model[(head + count) % CAP] = val;
            count++;
        } else if (count > 0) {
            int out;
            channel_recv(ch, &out);
            int expected = model[head];
            head = (head + 1) % CAP;
            count--;
            if (out != expected)
                g_fail = 1;
        }
    }

    /* Drain whatever remains and confirm it comes out in order. */
    while (count > 0 && !g_fail) {
        int out;
        channel_recv(ch, &out);
        int expected = model[head];
        head = (head + 1) % CAP;
        count--;
        if (out != expected)
            g_fail = 1;
    }

    channel_destroy(ch);
}

/* Run one input through a fresh channel. Returns 0 on success, 1 on mismatch.
 */
int channel_fuzz_one(const uint8_t *data, size_t len) {
    g_data = data;
    g_len = len;
    g_fail = 0;
    coro_create(driver, NULL, 64 * 1024);
    coro_run();
    return g_fail;
}

#ifdef FUZZ_LIBFUZZER
int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    channel_fuzz_one(data, size);
    return 0;
}
#else
/* xorshift64: tiny deterministic PRNG so runs are reproducible from a seed. */
static uint64_t g_rng;
static uint64_t xorshift(void) {
    uint64_t x = g_rng;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    return g_rng = x;
}

int main(int argc, char **argv) {
    int iters = (argc > 1) ? atoi(argv[1]) : 5000;
    g_rng = (argc > 2) ? strtoull(argv[2], NULL, 10) : 0x1234567890abcdefULL;
    if (iters <= 0)
        iters = 1;

    uint8_t buf[512];
    for (int it = 0; it < iters; it++) {
        size_t len = (size_t)(xorshift() % sizeof buf);
        for (size_t j = 0; j < len; j++)
            buf[j] = (uint8_t)xorshift();
        if (channel_fuzz_one(buf, len) != 0) {
            fprintf(stderr, "channel_fuzz: FAIL at iteration %d\n", it);
            return 1;
        }
    }
    printf("channel_fuzz: PASS  %d iterations\n", iters);
    return 0;
}
#endif
