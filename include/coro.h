#ifndef CORO_H
#define CORO_H

#include <stddef.h>
#include <stdint.h>

#define CORO_STACK_SIZE (256 * 1024)
#define CORO_GUARD_SIZE 4096

/* Saved CPU context. The hand-written switch keeps callee-saved registers
 * on the coroutine stack itself, so only the stack pointer is stored here.
 * UCONTEXT builds never use this struct. */
#ifdef CORO_USE_UCONTEXT
typedef struct { unsigned long long _pad; } coro_ctx_t;
#else
typedef struct { unsigned long long rsp; } coro_ctx_t;
#endif

typedef struct coro coro_t;

typedef void (*coro_fn)(void *arg);

/* Scheduler ---------------------------------------------------------------- */
int  coro_init(void);          /* call once from main before any coro_create */
void coro_run(void);           /* run until all coroutines finish */
void coro_yield(void);         /* suspend current, schedule next runnable */
void coro_park(void);          /* suspend WITHOUT requeueing; wake via
                                  runq_wake(). Used by channels/mutexes
                                  that keep their own wait lists */

coro_t *coro_create(coro_fn fn, void *arg);
coro_t *coro_current(void);
void    runq_wake(coro_t *c);  /* make a parked coroutine runnable again */
int     coro_alive_count(void);

/* Channel ------------------------------------------------------------------ */
typedef struct chan chan_t;

chan_t *chan_new(size_t capacity);
int     chan_send(chan_t *ch, void *v);   /* blocks (yields) if full   */
int     chan_recv(chan_t *ch, void **v);  /* blocks (yields) if empty  */
void    chan_free(chan_t *ch);

/* Mutex (FIFO handoff, no futex) ------------------------------------------- */
typedef struct coro_mutex coro_mutex_t;

coro_mutex_t *mutex_new(void);
int  mutex_lock(coro_mutex_t *m);         /* blocks (yields) while held */
void mutex_unlock(coro_mutex_t *m);
void mutex_free(coro_mutex_t *m);

#endif /* CORO_H */
