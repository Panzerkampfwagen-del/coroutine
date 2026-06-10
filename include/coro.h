#ifndef CORO_H
#define CORO_H

#include <stddef.h>

/**
 * @file coro.h
 * @brief Core coroutine API: creation, yielding, exit, and the scheduler.
 */

/**
 * @brief Saved machine state for one coroutine.
 *
 * Holds the callee-saved registers for the target ABI - the only state a
 * cooperative switch must preserve, since the caller has already spilled live
 * caller-saved registers around the call. The layout is architecture-specific
 * and must stay in sync with the offsets in context.S.
 *
 * - x86-64 (System V AMD64): rbx, rbp, r12-r15, rsp. The instruction pointer
 *   rides on the return address at the top of the stack.
 * - aarch64 (AAPCS64): x19-x28, fp (x29), lr (x30), sp, and the callee-saved
 *   floating-point registers d8-d15. lr carries the resume address.
 */
#if defined(__x86_64__)
typedef struct {
    void *rbx;
    void *rbp;
    void *r12;
    void *r13;
    void *r14;
    void *r15;
    void *rsp;
} coro_context_t;
#elif defined(__aarch64__)
typedef struct {
    void *x19, *x20, *x21, *x22, *x23, *x24, *x25, *x26, *x27, *x28;
    void *fp; /* x29 */
    void *lr; /* x30 - resume address */
    void *sp;
    void *d8, *d9, *d10, *d11, *d12, *d13, *d14, *d15;
} coro_context_t;
#else
#error "coro: unsupported architecture (x86-64 and aarch64 only)"
#endif

/** @brief Opaque coroutine handle. */
typedef struct coro coro_t;

/**
 * @brief An intrusive FIFO of coroutines.
 *
 * Used to build the wait queues inside the synchronization primitives (see
 * sync.h). Zero-initialize before first use.
 */
typedef struct {
    coro_t *head;
    coro_t *tail;
} coro_waitq_t;

/**
 * @brief Create a ready-to-run coroutine.
 *
 * The coroutine will execute @p fn(@p arg) on its own stack and is appended to
 * the back of the run queue. The lowest page of the stack is a PROT_NONE guard
 * page that turns a stack overflow into a fault instead of silent corruption.
 *
 * @param fn          Entry function for the coroutine.
 * @param arg         Argument passed to @p fn.
 * @param stack_size  Stack size in bytes; 0 selects the 1 MiB default. Rounded
 *                    up to a page boundary.
 * @return The new coroutine handle, or NULL on allocation failure.
 */
coro_t *coro_create(void (*fn)(void *), void *arg, size_t stack_size);

/**
 * @brief Cooperatively yield the CPU.
 *
 * The calling coroutine is placed at the back of the run queue and resumes once
 * every other ready coroutine has had a turn. Must be called from within a
 * coroutine.
 */
void coro_yield(void);

/**
 * @brief Terminate the running coroutine immediately.
 *
 * Never returns. Invoked automatically when a coroutine's function returns, so
 * calling it explicitly is optional.
 */
void coro_exit(void);

/**
 * @brief Run the scheduler until all coroutines finish.
 *
 * Switches into the first ready coroutine and keeps running coroutines
 * round-robin until the run queue drains and no I/O is outstanding, then
 * returns to the caller.
 */
void coro_run(void);

#endif /* CORO_H */
