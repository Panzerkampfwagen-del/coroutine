#define _GNU_SOURCE
#include "coro.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/mman.h>
#include <unistd.h>
#include <ucontext.h>

void coro_swap(coro_ctx_t *from, coro_ctx_t *to); /* src/switch.S */

struct coro {
    ucontext_t  ctx;           /* CORO_USE_UCONTEXT builds            */
    coro_ctx_t  regs;          /* hand-written asm switch builds      */
    void       *stack;
    coro_fn     fn;
    void       *arg;
    int         finished;
    int         started;
    struct coro *next;
};

/* ------------------------------------------------------------------ state */

static coro_t *g_main;
static coro_t *g_current;
static coro_t *g_runq_head;
static coro_t *g_runq_tail;
static coro_t *g_reap;      /* finished, awaiting free by the scheduler */
static int     g_alive;

static void stack_free(void *top);         /* fwd decl used by reaper */

static void runq_push(coro_t *c)
{
    c->next = NULL;
    if (g_runq_tail) g_runq_tail->next = c;
    else             g_runq_head = c;
    g_runq_tail = c;
}

static coro_t *runq_pop(void)
{
    coro_t *c = g_runq_head;
    if (!c) return NULL;
    g_runq_head = c->next;
    if (!g_runq_head) g_runq_tail = NULL;
    c->next = NULL;
    return c;
}

static void retire(coro_t *c)   /* called from a dying coroutine */
{
    c->finished = 1;
    g_alive--;
    c->next = g_reap;
    g_reap = c;
}

static void reap_finished(void) /* called from the scheduler */
{
    while (g_reap) {
        coro_t *c = g_reap;
        g_reap = c->next;
        c->next = NULL;
        stack_free(c->stack);
        free(c);
    }
}

/* --------------------------------------------------------------- stacks   */

static void *stack_alloc(void)
{
    size_t page  = (size_t)sysconf(_SC_PAGESIZE);
    size_t total = CORO_STACK_SIZE + CORO_GUARD_SIZE;
    total = (total + page - 1) & ~(page - 1);

    char *base = mmap(NULL, total, PROT_READ | PROT_WRITE,
                      MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (base == MAP_FAILED) { perror("mmap"); abort(); }

    /* first page is PROT_NONE: touching it segfaults => overflow caught */
    if (mprotect(base, CORO_GUARD_SIZE, PROT_NONE) != 0) {
        perror("mprotect"); abort();
    }
    return base + total;                 /* top of stack (grows down) */
}

static void stack_free(void *top)
{
    size_t page  = (size_t)sysconf(_SC_PAGESIZE);
    size_t total = CORO_STACK_SIZE + CORO_GUARD_SIZE;
    total = (total + page - 1) & ~(page - 1);
    munmap((char *)top - total, total);
}

/* ------------------------------------------------------------ entry glue  */

#ifdef CORO_USE_UCONTEXT
static void trampoline_uc(void)
{
    coro_t *self = g_current;
    self->fn(self->arg);
    retire(self);
    /* park forever: switch straight back to the scheduler */
    swapcontext(&self->ctx, &g_main->ctx);
}
#else
void trampoline_asm_entry(void);

/* On entry r12 holds the coro_t* seeded by coro_create.               */
__attribute__((noinline))
void trampoline_asm_entry(void)
{
    register coro_t *me __asm__("r12");
    me->fn(me->arg);
    retire(me);
    coro_swap(&me->regs, &g_main->regs);
    __builtin_unreachable();
}
#endif

/* --------------------------------------------------------------- create   */

coro_t *coro_create(coro_fn fn, void *arg)
{
    coro_t *c = calloc(1, sizeof(*c));
    if (!c) abort();
    c->fn   = fn;
    c->arg  = arg;
    c->stack = stack_alloc();
    g_alive++;

#ifdef CORO_USE_UCONTEXT
    getcontext(&c->ctx);
    c->ctx.uc_stack.ss_sp   = (char *)c->stack - CORO_STACK_SIZE;
    c->ctx.uc_stack.ss_size = CORO_STACK_SIZE;
    c->ctx.uc_link          = NULL;
    makecontext(&c->ctx, trampoline_uc, 0);
#else
    /*
     * Initial fake frame (descending addresses):
     *   slot[0..2] -> r15, r14, r13        (zero)
     *   slot[3]    -> r12 = c
     *   slot[4]    -> rbx                  (zero)
     *   slot[5]    -> rbp                  (zero)
     *   slot[6]    -> return address = trampoline_asm_entry
     * After `mov (%rsi),%rsp; pop x6; ret` we land in the trampoline.
     * The return-address slot is kept 16-byte aligned so that rsp at
     * trampoline entry satisfies the SysV ABI (rsp % 16 == 8).
     */
    uint64_t *top = (uint64_t *)c->stack;
    while (((uintptr_t)(top - 7)) % 16 != 0) top--;
    uint64_t *slots = top - 7;
    slots[0] = 0; slots[1] = 0; slots[2] = 0;
    slots[3] = (uint64_t)c;
    slots[4] = 0; slots[5] = 0;
    slots[6] = (uint64_t)&trampoline_asm_entry;
    c->regs.rsp = (uint64_t)slots;
#endif

    runq_push(c);
    return c;
}

int coro_init(void)
{
    if (g_main) return 0;
    g_main = calloc(1, sizeof(*g_main));
    if (!g_main) return -1;
    g_current = g_main;
    return 0;
}

int coro_alive_count(void) { return g_alive; }

coro_t *coro_current(void) { return g_current; }

/* Used by channels/mutexes to re-queue a coroutine that was parked on a
 * wait list outside the scheduler proper. */
void runq_wake(coro_t *c)
{
    runq_push(c);
}

/* ------------------------------------------------------------- switching  */

static void switch_away_to_next(coro_t *self)
{
    coro_t *next = runq_pop();
    if (!next) {
        fprintf(stderr, "coro deadlock: no runnable coroutine left\n");
        abort();
    }
    if (next == self) return;              /* sole runnable: keep going */
    g_current = next;
#ifdef CORO_USE_UCONTEXT
    swapcontext(&self->ctx, &next->ctx);
#else
    coro_swap(&self->regs, &next->regs);
#endif
}

void coro_yield(void)
{
    coro_t *self = g_current;
    if (self == g_main || !self) {
        fprintf(stderr, "coro_yield called outside a coroutine\n");
        abort();
    }
    runq_push(self);
    switch_away_to_next(self);
}

/* Parked coroutines are woken exactly once (each wake adds one run-queue
 * entry), so they must suspend WITHOUT adding themselves again. */
void coro_park(void)
{
    coro_t *self = g_current;
    if (self == g_main || !self) {
        fprintf(stderr, "coro_park called outside a coroutine\n");
        abort();
    }
    switch_away_to_next(self);
}

void coro_run(void)
{
    if (!g_main || g_current != g_main) {
        /* Nested or pre-init calls would swap from g_main's context while
         * clobbering g_main's saved state -- corrupt the scheduler. */
        fprintf(stderr,
                "coro_run(): must be called once from main after coro_init()\n");
        abort();
    }
    while (g_runq_head) {
        coro_t *next = runq_pop();
        g_current = next;
#ifdef CORO_USE_UCONTEXT
        swapcontext(&g_main->ctx, &next->ctx);
#else
        coro_swap(&g_main->regs, &next->regs);
#endif
        reap_finished();                   /* safe: dying coros parked here */
    }
    reap_finished();
    g_current = g_main;
}
