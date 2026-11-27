#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <vector>
#include "src/repair_dag.h"
#include "src/shard_queue.h"

enum class decode_status : bool
{
    success,
    failure,
};

typedef struct layout_info
{
    size_t num_parity_shards;
    size_t num_data_shards;
    size_t symbol_size; // Symbol size in bytes; shard lengths must be a multiple.
} layout_info;

/**
 * Interface for Locally Repairable Codes (LRC). All LRCs in this library implement this interface.
 *
 * There are three important parameters of an LRC, the message length (k) and the code length (n) and the local
 * reconstruction cost (l). A (k, l, r) LRC splits a message into `k` data shards and computes `r` global parity
 * shards. Each shard is made of some number of  `symbols` of type `T`. For example, each symbol might be a byte
 * of type `uint8_t`, in which case an X byte  file will be coded into shards of size `X / k` (the last data shard
 * may be padded).
 *
 * As opposed to normal erasure codes, LRCs are specially designed to minimze the amount of reads when recovering
 * from the failure of a single shard. To do so, the `k` shards are divided into `l` groups, and for each group a
 * local parity shard is computed. When a single shard goes down, one only reads to read `k / l` shards to repair,
 * as opposed to the usual `k + r`.
 */
class LRC
{
protected:
    std::unique_ptr<std::byte[]> workspace;
    uint16_t num_threads;

public:
    virtual ~LRC() = default;

    [[nodiscard]] virtual size_t num_data_shards() const noexcept = 0;
    [[nodiscard]] virtual size_t num_local_shards() const noexcept = 0;
    [[nodiscard]] virtual size_t num_global_shards() const noexcept = 0;

    // Counts agree with the shard-count getters; total parity is local + global.
    virtual layout_info get_layout_info() const noexcept = 0;

    // Requires the code's data/parity shard counts, equally sized shard buffers,
    // and byte lengths divisible by symbol_size. Input padding is initialized.
    // Output buffers are allocated by the caller and must not overlap inputs
    // or each other. On return, parity has been written and all workers finished.
    virtual void encode(std::vector<std::span<std::byte const>> const &data_shards, std::vector<std::span<std::byte>> const &parity_shards) const = 0;

    // Builds a DAG for shard_number, with node byte lengths derived from
    // shard_size (bytes per stored shard, including padding). Requires valid
    // shard indices and shard_size divisible by the code's symbol_size.
    // Indices are zero-based: data first, then parity. Retrieval inputs exclude
    // the target and all known unavailable shards; missing shards may still
    // appear as reconstructed intermediates. Other shards are assumed available.
    // Returns nullopt if no supported plan exists under these exclusions.
    // The caller may discover further failures and replan with a larger set.
    // Planning and optional DAG optimization happen outside the timed repair.
    // The plan owns its metadata and is not
    // retained as mutable planning state in this LRC.
    [[nodiscard]] virtual std::optional<RepairDAG> plan_repair(
        size_t shard_number,
        size_t shard_size,
        std::span<const size_t> unavailable_shards) const = 0;

    // Executes a valid DAG compatible with this code, for one extent.
    // May return success before delivery closes.
    // Success means output contains the reconstructed shard, while output is not meaningful
    // during failures. All decoding workers finish before return.
    // Uses preallocated workspace, assumed sufficient for all intermediate
    // calculation buffers.
    // The caller owns delivery threads and must join them before destroying the
    // queue or its backing data, including when repair returns early.
    [[nodiscard]] virtual decode_status repair(
        const RepairDAG &plan,
        ShardQueue &arrivals,
        std::span<std::byte> output) const = 0;
};
