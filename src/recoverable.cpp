#include "./recoverable.h"
#include "./delivery_scheduler.h"
#include "./storage_conditions/storage_conditions.h"
#include <algorithm>
#include <chrono>
#include <cmath>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <utility>

namespace
{
size_t divide_round_up(size_t value, size_t divisor)
{
    return value / divisor + (value % divisor != 0);
}

size_t shard_bytes(size_t extent_size, const layout_info &layout)
{
    if (layout.num_data_shards == 0 || layout.symbol_size == 0)
        throw std::invalid_argument("Data shard count and symbol size must be positive");
    const auto symbols = divide_round_up(
        divide_round_up(extent_size, layout.num_data_shards), layout.symbol_size);
    if (symbols > std::numeric_limits<size_t>::max() / layout.symbol_size)
        throw std::length_error("Shard size overflow");
    return symbols * layout.symbol_size;
}

std::vector<std::unique_ptr<std::byte[]>> allocate_shards(size_t count, size_t bytes)
{
    std::vector<std::unique_ptr<std::byte[]>> shards;
    shards.reserve(count);
    for (size_t i = 0; i < count; ++i)
        shards.push_back(std::make_unique<std::byte[]>(bytes));
    return shards;
}

void copy_extent_data(const extent &source, size_t source_bytes,
                      extent &destination, size_t destination_bytes, size_t count)
{
    size_t offset = 0;
    while (offset < count)
    {
        const auto amount = std::min({count - offset,
            source_bytes - offset % source_bytes,
            destination_bytes - offset % destination_bytes});
        std::copy_n(source.data_shards.at(offset / source_bytes).get() + offset % source_bytes,
                    amount, destination.data_shards.at(offset / destination_bytes).get()
                                + offset % destination_bytes);
        offset += amount;
    }
}

size_t checked_file_size(const std::filesystem::path &path)
{
    const auto bytes = std::filesystem::file_size(path);
    if (bytes > std::numeric_limits<size_t>::max())
        throw std::length_error("File is too large");
    return static_cast<size_t>(bytes);
}

void read_bytes(std::ifstream &input, std::byte *destination, size_t count)
{
    while (count != 0)
    {
        const auto amount = std::min(count,
            static_cast<size_t>(std::numeric_limits<std::streamsize>::max()));
        input.read(reinterpret_cast<char *>(destination), static_cast<std::streamsize>(amount));
        destination += amount;
        count -= amount;
    }
}

size_t machine_for(const StorageConditions &conditions, size_t extent_number, size_t shard_number)
{
    return conditions.get_shard_location({extent_number, shard_number});
}

using Clock = std::chrono::steady_clock;

Clock::duration retrieval_delay(const StorageConditions &conditions, size_t machine, size_t bytes)
{
    const double microseconds = conditions.get_retrieval_delay(machine, bytes);
    const auto maximum = std::chrono::duration<double, std::micro>(Clock::duration::max()).count();
    if (!std::isfinite(microseconds) || microseconds < 0 || microseconds >= maximum)
        throw std::invalid_argument("Retrieval delay must be finite, nonnegative and representable");
    return std::chrono::duration_cast<Clock::duration>(
        std::chrono::duration<double, std::micro>(microseconds));
}

}

RepairDAG Recoverable::find_plan(const LRC &code, const StorageConditions &conditions,
                                size_t extent_number, size_t target, size_t bytes) const
{
    std::vector<size_t> unavailable{target};
    for (;;)
    {
        auto plan = code.plan_repair(target, bytes, unavailable);
        if (!plan)
            throw std::runtime_error("No supported repair plan");
        const auto previous_count = unavailable.size();
        for (const auto &node : plan->nodes)
        {
            const auto *input = std::get_if<RepairInput>(&node.operation);
            if (!input)
                continue;
            if (std::find(unavailable.begin(), unavailable.end(), input->shard_id) != unavailable.end())
                throw std::runtime_error("Repair plan requests a known unavailable shard");
            if (!conditions.get_machine_status(machine_for(conditions, extent_number, input->shard_id)))
                unavailable.push_back(input->shard_id);
        }
        if (unavailable.size() == previous_count)
            return std::move(*plan);
    }
}

Recoverable::Recoverable(size_t data_size, size_t extent_size)
    : extent_size(extent_size), data_size(data_size)
{
    if (extent_size == 0)
        throw std::invalid_argument("Extent size must be positive");
    extents.resize(divide_round_up(data_size, extent_size));
    for (auto &item : extents)
        item.data_shards = allocate_shards(1, extent_size);
}

Recoverable::Recoverable(const std::filesystem::path &path, size_t extent_size)
    : Recoverable(checked_file_size(path), extent_size)
{
    std::ifstream input;
    input.exceptions(std::ios::failbit | std::ios::badbit);
    input.open(path, std::ios::binary);
    size_t remaining = data_size;
    for (auto &item : extents)
    {
        const auto amount = std::min(remaining, extent_size);
        read_bytes(input, item.data_shards.front().get(), amount);
        remaining -= amount;
    }
}

void Recoverable::set_layout(const layout_info &new_layout)
{
    const auto bytes = shard_bytes(extent_size, new_layout);
    const auto old_bytes = layout ? shard_bytes(extent_size, *layout) : extent_size;
    std::vector<extent> replacement(extents.size());
    size_t remaining = data_size;
    for (size_t i = 0; i < extents.size(); ++i)
    {
        replacement[i].data_shards = allocate_shards(new_layout.num_data_shards, bytes);
        replacement[i].parity_shards = allocate_shards(new_layout.num_parity_shards, bytes);
        const auto amount = std::min(remaining, extent_size);
        copy_extent_data(extents[i], old_bytes, replacement[i], bytes, amount);
        remaining -= amount;
    }
    auto new_output = std::make_unique<std::byte[]>(bytes);
    extents.swap(replacement);
    output.swap(new_output);
    layout = new_layout;
}

void Recoverable::encode(const LRC &code)
{
    const auto bytes = shard_bytes(extent_size, layout.value());
    std::vector<std::span<const std::byte>> data;
    std::vector<std::span<std::byte>> parity;
    data.reserve(layout->num_data_shards);
    parity.reserve(layout->num_parity_shards);
    for (auto &item : extents)
    {
        data.clear();
        parity.clear();
        for (const auto &buffer : item.data_shards)
            data.emplace_back(buffer.get(), bytes);
        for (const auto &buffer : item.parity_shards)
            parity.emplace_back(buffer.get(), bytes);
        code.encode(data, parity);
    }
}

void Recoverable::write(const std::filesystem::path &path)
{
    Recoverable replacement(path, extent_size);
    if (layout)
        replacement.set_layout(*layout);
    extents.swap(replacement.extents);
    output.swap(replacement.output);
    data_size = replacement.data_size;
}

void Recoverable::write(std::byte byte, size_t offset)
{
    if (offset >= data_size)
        throw std::out_of_range("Data offset is out of range");
    const auto bytes = layout ? shard_bytes(extent_size, *layout) : extent_size;
    const auto local = offset % extent_size;
    extents.at(offset / extent_size).data_shards.at(local / bytes)[local % bytes] = byte;
}

std::span<const std::byte> Recoverable::read_available_shard(
    size_t extent_number, size_t shard_number) const
{
    const auto bytes = shard_bytes(extent_size, layout.value());
    const auto &item = extents.at(extent_number);
    if (shard_number < layout->num_data_shards)
        return {item.data_shards.at(shard_number).get(), bytes};
    return {item.parity_shards.at(shard_number - layout->num_data_shards).get(), bytes};
}

std::span<const std::byte> Recoverable::read_shard(
    size_t extent_number, size_t shard_number,
    const LRC &code, const StorageConditions &conditions,
    DeliveryScheduler &scheduler)
{
    const auto stored = read_available_shard(extent_number, shard_number);
    const auto machine = machine_for(conditions, extent_number, shard_number);
    std::optional<RepairDAG> plan;
    std::vector<DeliveryScheduler::ScheduledShard> deliveries;
    if (conditions.get_machine_status(machine))
    {
        deliveries.push_back({retrieval_delay(conditions, machine, stored.size()), {shard_number, stored}});
    }
    else
    {
        plan = find_plan(code, conditions, extent_number, shard_number, stored.size());
        if (plan->target_shard != shard_number || plan->result >= plan->nodes.size()
            || plan->nodes[plan->result].byte_size != stored.size())
            throw std::runtime_error("Invalid repair plan result");
        for (const auto &node : plan->nodes)
        {
            const auto *input = std::get_if<RepairInput>(&node.operation);
            if (!input)
                continue;
            const auto data = read_available_shard(extent_number, input->shard_id);
            if (node.byte_size != data.size())
                throw std::runtime_error("Repair input size does not match stored shard");
            const auto source = machine_for(conditions, extent_number, input->shard_id);
            deliveries.push_back({retrieval_delay(conditions, source, data.size()), {input->shard_id, data}});
        }
    }
    ShardQueue arrivals;
    // A rejected start must not cancel an existing session. Once started,
    // always finalize delivery before destroying the local queue.
    scheduler.start(arrivals);
    try
    {
        scheduler.schedule_batch(std::move(deliveries));
        scheduler.finish_scheduling();
        std::span<const std::byte> result;
        if (plan)
        {
            const auto status = code.repair(*plan, arrivals, {output.get(), stored.size()});
            scheduler.cancel_and_wait();
            if (status != decode_status::success)
                throw std::runtime_error("Shard repair failed");
            result = read_output();
        }
        else
        {
            const auto received = arrivals.wait_pop();
            scheduler.cancel_and_wait();
            if (!received)
                throw std::runtime_error("Shard delivery failed");
            result = received->data;
        }
        return result;
    }
    catch (...)
    {
        try
        {
            scheduler.cancel_and_wait();
        }
        catch (...)
        {
            // Cleanup completes before a producer error is rethrown. Preserve
            // the original scheduling/decoding exception.
        }
        throw;
    }
}

std::span<const std::byte> Recoverable::read_output() const
{
    return {output.get(), shard_bytes(extent_size, layout.value())};
}
