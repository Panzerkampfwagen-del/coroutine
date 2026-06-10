#ifndef SYNC_H
#define SYNC_H

#include "coro.h"

/**
 * @file sync.h
 * @brief A cooperative mutex for coroutines.
 *
 * Because coroutines never preempt one another, mutual exclusion needs only a
 * held flag plus a FIFO of blocked waiters: ownership is handed directly from
 * unlock to the next waiter, so there is nothing to spin on and no futex is
 * involved. Declare one by value and pass its address.
 */
typedef struct {
    int locked;           /**< nonzero while held */
    coro_waitq_t waiters; /**< coroutines blocked in coro_mutex_lock */
} coro_mutex_t;

/**
 * @brief Initialize a mutex to the unlocked state.
 * @param m  Mutex to initialize.
 */
void coro_mutex_init(coro_mutex_t *m);

/**
 * @brief Acquire the mutex, blocking the calling coroutine until it is free.
 * @param m  Mutex to lock.
 */
void coro_mutex_lock(coro_mutex_t *m);

/**
 * @brief Release the mutex.
 *
 * If a coroutine is waiting, ownership passes directly to the one that has
 * waited longest, which is made runnable.
 *
 * @param m  Mutex to unlock.
 */
void coro_mutex_unlock(coro_mutex_t *m);

#endif /* SYNC_H */
