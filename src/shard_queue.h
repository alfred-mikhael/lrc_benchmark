#pragma once
#include <span>
#include <optional>

typedef struct shard
{
    size_t shard_id;
    // Borrowed data: publication does not transfer ownership. Keep storage alive
    // and unchanged until the consumer has finished processing it.
    std::span<const std::byte> data;
} shard;

// Synchronized arrival queue for one repair operation. A producer may publish
// while a consumer decodes. Synchronization makes published data visible to
// the consumer; it does not protect subsequent mutations of the backing data.
class ShardQueue
{
public:
    ShardQueue();
    // Nonblocking; returns empty if no shard is currently available.
    std::optional<shard> try_pop();
    // Waits for an arrival or closure. Returns empty only when closed and drained.
    std::optional<shard> wait_pop();
    // Publishes a borrowed shard view and wakes a waiting consumer.
    // Requires an open queue.
    void push(shard const &shard);
    // Marks delivery complete and wakes waiting consumers. Already queued shards
    // remain available for consumption. No further pushes are permitted.
    void close();
};
