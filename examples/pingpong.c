/* Two coroutines bounce a counter back and forth across two channels. Each
   round-trip increments the counter once; after 1000 rounds the final value
   is printed. */
#include "channel.h"
#include "coro.h"
#include <stdio.h>

#define ROUNDS 1000

static channel_t *to_pong; /* ping -> pong */
static channel_t *to_ping; /* pong -> ping */

static void ping(void *arg) {
    (void)arg;
    int v = 0;
    for (int i = 0; i < ROUNDS; i++) {
        channel_send(to_pong, &v); /* hand the counter to pong */
        channel_recv(to_ping, &v); /* get it back, incremented */
    }
    printf("pingpong: final value = %d\n", v);
}

static void pong(void *arg) {
    (void)arg;
    int v;
    for (int i = 0; i < ROUNDS; i++) {
        channel_recv(to_pong, &v);
        v++;
        channel_send(to_ping, &v);
    }
}

int main(void) {
    to_pong = channel_create(1, sizeof(int));
    to_ping = channel_create(1, sizeof(int));

    coro_create(ping, NULL, 0);
    coro_create(pong, NULL, 0);
    coro_run();

    channel_destroy(to_pong);
    channel_destroy(to_ping);
    return 0;
}
