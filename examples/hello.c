/* Five coroutines that print and yield, demonstrating round-robin scheduling.
 */
#include "coro.h"
#include <stdio.h>

static void worker(void *arg) {
    long id = (long)arg;
    for (int step = 0; step < 3; step++) {
        printf("coroutine %ld: step %d\n", id, step);
        coro_yield();
    }
    printf("coroutine %ld: done\n", id);
}

int main(void) {
    for (long i = 1; i <= 5; i++)
        coro_create(worker, (void *)i, 0);

    coro_run();

    printf("all coroutines finished\n");
    return 0;
}
