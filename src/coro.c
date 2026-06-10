#define _GNU_SOURCE
#include "coro.h"
#include "coro_internal.h"

#include <stdint.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <unistd.h>

/* Address Sanitizer needs to be told about stack switches, otherwise it
   mistakes our hand-rolled coroutine stacks for corruption. When ASan is on we
   bracket every context switch with the fiber-switch annotations; otherwise the
   wrapper compiles down to a bare coro_context_switch. */
#if defined(__SANITIZE_ADDRESS__)
#include <sanitizer/common_interface_defs.h>
#define CORO_ASAN 1
#endif

#define CORO_DEFAULT_STACK (1024UL * 1024) /* 1 MiB */
#define CORO_GUARD_SIZE 4096 /* one guard page at the stack bottom */

/* The assembly in context.S hard-codes these offsets; keep them in sync. */
#if defined(__x86_64__)
_Static_assert(offsetof(coro_context_t, rbx) == 0, "rbx offset");
_Static_assert(offsetof(coro_context_t, rbp) == 8, "rbp offset");
_Static_assert(offsetof(coro_context_t, r12) == 16, "r12 offset");
_Static_assert(offsetof(coro_context_t, r13) == 24, "r13 offset");
_Static_assert(offsetof(coro_context_t, r14) == 32, "r14 offset");
_Static_assert(offsetof(coro_context_t, r15) == 40, "r15 offset");
_Static_assert(offsetof(coro_context_t, rsp) == 48, "rsp offset");
#elif defined(__aarch64__)
_Static_assert(offsetof(coro_context_t, x19) == 0, "x19 offset");
_Static_assert(offsetof(coro_context_t, fp) == 80, "fp offset");
_Static_assert(offsetof(coro_context_t, lr) == 88, "lr offset");
_Static_assert(offsetof(coro_context_t, sp) == 96, "sp offset");
_Static_assert(offsetof(coro_context_t, d8) == 104, "d8 offset");
_Static_assert(offsetof(coro_context_t, d15) == 160, "d15 offset");
#endif

extern void coro_trampoline(void);

static coro_context_t g_sched_ctx; /* scheduler (main) context */
static coro_t *g_current; /* running coroutine; NULL inside scheduler */
static int g_next_id;     /* monotonically increasing id source */

/* FIFO run queue of READY coroutines. */
static coro_waitq_t g_runq;

static void runq_enqueue(coro_t *c) {
    coro_waitq_push(&g_runq, c);
}
static coro_t *runq_dequeue(void) {
    return coro_waitq_pop(&g_runq);
}

/* Usable stack region (above the guard page), reported to ASan on each switch.
 */
static const void *coro_stack_lo(const coro_t *c) {
    return (const char *)c->stack_base + CORO_GUARD_SIZE;
}
static size_t coro_stack_usable(const coro_t *c) {
    return c->stack_size - CORO_GUARD_SIZE;
}

#ifdef CORO_ASAN
static const void
    *g_sched_lo; /* scheduler stack bounds, learned on first entry */
static size_t g_sched_size;
#define CORO_SCHED_LO g_sched_lo
#define CORO_SCHED_SIZE g_sched_size
#else
#define CORO_SCHED_LO NULL
#define CORO_SCHED_SIZE 0
#endif

/* Switch from *from to *to. to_lo/to_size describe the destination's stack for
   ASan; `finishing` is set when the caller will never be resumed (coro_exit),
   so ASan can discard its fake stack. */
static void coro_swap(coro_context_t *from, coro_context_t *to,
                      const void *to_lo, size_t to_size, int finishing) {
#ifdef CORO_ASAN
    void *fake = NULL;
    __sanitizer_start_switch_fiber(finishing ? NULL : &fake, to_lo, to_size);
    coro_context_switch(from, to);
    __sanitizer_finish_switch_fiber(fake, NULL, NULL);
#else
    (void)to_lo;
    (void)to_size;
    (void)finishing;
    coro_context_switch(from, to);
#endif
}

coro_t *coro_current(void) {
    return g_current;
}

coro_t *coro_create(void (*fn)(void *), void *arg, size_t stack_size) {
    if (stack_size == 0)
        stack_size = CORO_DEFAULT_STACK;

    long pg = sysconf(_SC_PAGESIZE);
    if (pg <= 0)
        pg = 4096;
    stack_size = (stack_size + (size_t)pg - 1) & ~((size_t)pg - 1);
    if (stack_size <= CORO_GUARD_SIZE)
        stack_size += (size_t)pg; /* room above guard */

    coro_t *c = calloc(1, sizeof *c);
    if (!c)
        return NULL;

    void *base = mmap(NULL, stack_size, PROT_READ | PROT_WRITE,
                      MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (base == MAP_FAILED) {
        free(c);
        return NULL;
    }

    /* Guard page at the lowest address: a stack overflow faults here instead of
       silently corrupting the neighbouring mapping. */
    if (mprotect(base, CORO_GUARD_SIZE, PROT_NONE) != 0) {
        munmap(base, stack_size);
        free(c);
        return NULL;
    }

    c->fn = fn;
    c->arg = arg;
    c->stack_base = base;
    c->stack_size = stack_size;
    c->id = ++g_next_id;
    c->state = CORO_READY;

    /* Seed the initial context so the first switch resumes in coro_trampoline.
       The trampoline runs coro_entry (the user function) and then falls into
       coro_exit; coro_entry/coro_exit read the running coroutine from
       g_current, so no arguments need threading through registers. */
#if defined(__x86_64__)
    /* From a 16-aligned top, drop one pad slot so the trampoline slot lands at
       an address congruent to 8 (mod 16); the first `ret` into the trampoline
       then leaves rsp congruent to 0, and the trampoline's `call`/`ret` hand
       both coro_entry and coro_exit an ABI-aligned stack (rsp+8 a multiple of
       16 at entry). */
    uintptr_t top = ((uintptr_t)base + stack_size) & ~(uintptr_t)0xF;
    top -= 8;
    void **sp = (void **)top;
    *(--sp) = (void *)coro_exit;       /* coro_entry's return address */
    *(--sp) = (void *)coro_trampoline; /* first resume target */
    c->ctx.rsp = (void *)sp;
#elif defined(__aarch64__)
    /* AArch64 keeps the return address in lr, not on the stack, so we just
       point sp at a 16-aligned top and resume at the trampoline via lr. */
    uintptr_t top = ((uintptr_t)base + stack_size) & ~(uintptr_t)0xF;
    c->ctx.sp = (void *)top;
    c->ctx.lr = (void *)coro_trampoline;
#endif

    runq_enqueue(c);
    return c;
}

static void coro_free(coro_t *c) {
    munmap(c->stack_base, c->stack_size);
    free(c);
}

/* Entry point for every coroutine, reached from coro_trampoline. */
void coro_entry(void) {
#ifdef CORO_ASAN
    /* Complete the switch into this fresh fiber and, the first time around,
       record the scheduler's stack bounds (the fiber we came from). */
    const void *prev_lo;
    size_t prev_size;
    __sanitizer_finish_switch_fiber(NULL, &prev_lo, &prev_size);
    if (!g_sched_lo) {
        g_sched_lo = prev_lo;
        g_sched_size = prev_size;
    }
#endif
    coro_t *self = g_current;
    self->fn(self->arg);
    /* returns into coro_exit via the planted return address */
}

void coro_yield(void) {
    coro_t *self = g_current;
    self->state = CORO_READY;
    coro_swap(&self->ctx, &g_sched_ctx, CORO_SCHED_LO, CORO_SCHED_SIZE, 0);
}

void coro_block(void) {
    coro_t *self = g_current;
    self->state = CORO_BLOCKED;
    coro_swap(&self->ctx, &g_sched_ctx, CORO_SCHED_LO, CORO_SCHED_SIZE, 0);
}

void coro_unblock(coro_t *c) {
    c->state = CORO_READY;
    runq_enqueue(c);
}

void coro_exit(void) {
    coro_t *self = g_current;
    self->state = CORO_DONE;
    coro_swap(&self->ctx, &g_sched_ctx, CORO_SCHED_LO, CORO_SCHED_SIZE, 1);
    __builtin_unreachable();
}

/* Default no-op I/O hooks; io.c provides strong overrides when linked. */
__attribute__((weak)) int coro_io_pending(void) {
    return 0;
}
__attribute__((weak)) int coro_io_reap(int block) {
    (void)block;
    return 0;
}

void coro_run(void) {
    for (;;) {
        coro_t *c = runq_dequeue();
        if (!c) {
            /* Run queue drained: block on outstanding I/O, if any. The wait may
               be cut short by a signal, in which case we simply loop again. */
            if (!coro_io_pending())
                break;
            coro_io_reap(/*block=*/1);
            continue;
        }

        g_current = c;
        c->state = CORO_RUNNING;
        coro_swap(&g_sched_ctx, &c->ctx, coro_stack_lo(c), coro_stack_usable(c),
                  0);
        g_current = NULL;

        /* Control returned from the coroutine; act on why it yielded. */
        switch (c->state) {
        case CORO_READY:
            runq_enqueue(c);
            break; /* voluntary yield */
        case CORO_BLOCKED:
            break; /* parked on a wait queue */
        case CORO_DONE:
            coro_free(c);
            break; /* finished */
        default:
            break;
        }
    }
}
