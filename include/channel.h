#ifndef CHANNEL_H
#define CHANNEL_H

#include <stddef.h>

/**
 * @file channel.h
 * @brief Bounded, typed channels for passing fixed-size items between
 *        coroutines.
 *
 * Backed by a ring buffer. Safe only for cooperative (single OS thread) use,
 * like the rest of this library.
 */

/** @brief Opaque channel handle. */
typedef struct channel channel_t;

/**
 * @brief Create a channel.
 *
 * @param capacity   Maximum number of buffered items; must be at least 1.
 * @param item_size  Size in bytes of each item; must be at least 1.
 * @return The new channel, or NULL on invalid arguments or allocation failure.
 */
channel_t *channel_create(size_t capacity, size_t item_size);

/**
 * @brief Send an item into the channel.
 *
 * Copies @p item_size bytes from @p item. If the channel is full, the calling
 * coroutine blocks until space becomes available.
 *
 * @param ch    Target channel.
 * @param item  Pointer to the item to copy in.
 */
void channel_send(channel_t *ch, const void *item);

/**
 * @brief Receive the next item from the channel.
 *
 * Copies @p item_size bytes into @p out. If the channel is empty, the calling
 * coroutine blocks until an item arrives.
 *
 * @param ch   Source channel.
 * @param out  Destination buffer for the item.
 */
void channel_recv(channel_t *ch, void *out);

/**
 * @brief Destroy a channel and free its storage.
 *
 * The caller must ensure no coroutine is currently blocked on it. Passing NULL
 * is a no-op.
 *
 * @param ch  Channel to destroy.
 */
void channel_destroy(channel_t *ch);

#endif /* CHANNEL_H */
