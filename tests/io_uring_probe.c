/* Tiny standalone probe: exits 0 if io_uring can be initialized on this host,
 * 1 otherwise. CI uses it to decide whether to run the io_uring-dependent
 * distributed test or skip it - some sandboxes and hardened kernels disable
 * io_uring (e.g. via the kernel.io_uring_disabled sysctl).
 *
 * Build: cc tests/io_uring_probe.c -o io_uring_probe -luring
 */
#include <liburing.h>
#include <stdio.h>

int main(void) {
    struct io_uring ring;
    int rc = io_uring_queue_init(8, &ring, 0);
    if (rc < 0) {
        fprintf(stderr, "io_uring unavailable: io_uring_queue_init = %d\n", rc);
        return 1;
    }
    io_uring_queue_exit(&ring);
    printf("io_uring available\n");
    return 0;
}
