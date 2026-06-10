/* TCP echo server on port 8080. One coroutine accepts connections; each
   accepted connection is handled by its own coroutine that echoes bytes back
   using io_uring-backed reads and writes.

   Test with:  echo "hello" | nc localhost 8080 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "coro.h"
#include "io.h"

#define PORT 8080

static void echo_conn(void *arg) {
    int fd = (int)(intptr_t)arg;
    char buf[4096];

    for (;;) {
        int n = coro_read(fd, buf, sizeof buf);
        if (n <= 0)
            break; /* peer closed, or error */

        int off = 0;
        while (off < n) {
            int w = coro_write(fd, buf + off, (size_t)(n - off));
            if (w <= 0)
                goto done;
            off += w;
        }
    }
done:
    close(fd);
}

static void acceptor(void *arg) {
    int lfd = (int)(intptr_t)arg;
    for (;;) {
        int cfd = coro_accept(lfd, NULL, NULL);
        if (cfd < 0) {
            fprintf(stderr, "accept failed: %d\n", cfd);
            break;
        }
        coro_create(echo_conn, (void *)(intptr_t)cfd, 0);
    }
}

static int make_listener(void) {
    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) {
        perror("socket");
        return -1;
    }

    int opt = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof opt);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(PORT);

    if (bind(lfd, (struct sockaddr *)&addr, sizeof addr) < 0) {
        perror("bind");
        close(lfd);
        return -1;
    }
    if (listen(lfd, 64) < 0) {
        perror("listen");
        close(lfd);
        return -1;
    }
    return lfd;
}

int main(void) {
    int lfd = make_listener();
    if (lfd < 0)
        return 1;

    printf("echo server listening on port %d\n", PORT);
    fflush(stdout);

    coro_create(acceptor, (void *)(intptr_t)lfd, 0);
    coro_run();

    close(lfd);
    return 0;
}
