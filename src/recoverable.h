#pragma once
#include <memory>
#include <filesystem>
#include <vector>
#include <span>
#include <optional>
#include "src/codes/lrc/lrc.h"

class StorageConditions;
class DeliveryScheduler;

/**
 * Interface layer between data and LRCs.
 * Owns shard storage. Borrowed views must not outlive their backing storage.
 * Writes and layout changes must not run concurrently with readers or repairs
 * using that storage.
 */
class Recoverable
{
private:
    size_t extent_size;
    size_t data_size;
    std::optional<layout_info> layout;
    std::vector<struct extent> extents;
    std::unique_ptr<std::byte[]> output;

    // Returns a borrowed view into extent storage without initiating repair or
    // scheduling delivery. Requires an installed layout, valid indices, and
    // availability already established by the caller. Delivery delays are
    // handled separately so helper-shard retrievals can overlap.
    std::span<const std::byte> read_available_shard(
        size_t extent_number, size_t shard_number) const;

    // Replans until all input shards are available. The target is known to be
    // unavailable; throws if the code cannot produce a supported repair plan.
    RepairDAG find_plan(const LRC &code, const StorageConditions &conditions,
                        size_t extent_number, size_t target, size_t bytes) const;

public:
    Recoverable(const std::filesystem::path &path, size_t extent_size);
    Recoverable(size_t data_size, size_t extent_size);

    // Requires an installed layout compatible with code; mismatch is undefined
    // behavior. On successful completion, parity represents the current data.
    void encode(const LRC &code);
    // Copies the layout. May be called after encoding; invalidates parity.
    // Changes to shard counts or symbol size must preserve the logical data
    // while repartitioning/padding storage as needed. May invalidate borrowed views.
    void set_layout(layout_info const &layout_info);

    // Replaces the logical data with the file contents; invalidates parity and
    // may invalidate borrowed views.
    void write(const std::filesystem::path &path);
    // Requires offset < data_size. Changes logical data and invalidates parity;
    // does not automatically recompute it.
    void write(std::byte byte, size_t offset);

    size_t get_num_extents() const { return extents.size(); };
    size_t get_extent_size() const { return extent_size; };

    // Reads a stored shard if its machine is alive; otherwise plans and executes
    // repair into output. Replans with newly discovered unavailable helpers until
    // a viable DAG is found. Helper reads do not recursively initiate repairs.
    // Indices are zero-based: data first, then parity at num_data_shards.
    // Requires valid indices and locations, a layout compatible with code, and
    // valid parity for repairs. Conditions remain unchanged throughout the call.
    // Throws std::runtime_error if no supported plan exists or repair fails.
    // Healthy reads borrow extent storage; repaired reads borrow output and are
    // overwritten by the next repair. Storage replacement may invalidate either.
    // Calls sharing output or code workspace must not repair concurrently.
    // Requires an idle scheduler, used exclusively for this call. Delivery is
    // finalized before return or exception, so the scheduler can be reused.
    std::span<const std::byte> read_shard(
        size_t extent_number, size_t shard_number,
        const LRC &code, const StorageConditions &conditions,
        DeliveryScheduler &scheduler);
    // Borrows the reusable output buffer; contents are meaningful after a
    // successful repair and may be overwritten by the next repair.
    std::span<const std::byte> read_output() const;
};

struct extent
{
    std::vector<std::unique_ptr<std::byte[]>> data_shards;
    std::vector<std::unique_ptr<std::byte[]>> parity_shards;
};
