#pragma once
#include <cstddef>
#include <cmath>
#include <stdexcept>
#include <functional>
#include <utility>
#include <vector>
#include <unordered_map>

struct machine_info
{
    /* data */
    bool alive;
    double base_latency_us;
    double link_speed; // Decimal kB per microsecond (1 kB = 1000 bytes).
};

struct shard_location_hash
{
    std::size_t operator()(const std::pair<std::size_t, std::size_t> &location) const noexcept
    {
        const auto extent_hash = std::hash<std::size_t>{}(location.first);
        const auto shard_hash = std::hash<std::size_t>{}(location.second);
        return extent_hash ^ (shard_hash + 0x9e3779b9U + (extent_hash << 6) + (extent_hash >> 2));
    }
};

class StorageConditions
{
    /* data */
    std::vector<machine_info> cluster_info;
    // Maps (extent number, shard number) to an index in cluster_info.
    std::unordered_map<std::pair<size_t, size_t>, size_t, shard_location_hash> shard_locations;

    static void validate_machine(const machine_info &machine)
    {
        if (!std::isfinite(machine.link_speed) || machine.link_speed <= 0.0
            || !std::isfinite(machine.base_latency_us) || machine.base_latency_us < 0.0)
            throw std::invalid_argument("Machine latency must be finite and nonnegative; link speed must be finite and positive");
    }

protected:
    const machine_info &get_machine_info(size_t machine_index) const
    {
        return cluster_info.at(machine_index);
    }

    // Returns baseline latency in microseconds for data_size bytes.
    double get_baseline_retrieval_delay(size_t machine_index, size_t data_size) const
    {
        const auto &machine = get_machine_info(machine_index);
        return machine.base_latency_us + (static_cast<double>(data_size) / 1000.0) / machine.link_speed;
    }

public:
    StorageConditions(
        std::vector<machine_info> machines,
        std::unordered_map<std::pair<size_t, size_t>, size_t, shard_location_hash> locations)
        : cluster_info(std::move(machines)), shard_locations(std::move(locations))
    {
        for (const auto &machine : cluster_info)
            validate_machine(machine);
    }

    virtual ~StorageConditions() = default;
    // Appends a machine and returns its stable index. Mutations must not overlap reads.
    size_t add_machine(const machine_info &machine)
    {
        validate_machine(machine);
        cluster_info.push_back(machine);
        return cluster_info.size() - 1;
    }

    // Adds or updates a shard location. Requires an existing machine index.
    void add_shard(std::pair<size_t, size_t> shard_index, size_t machine_index)
    {
        get_machine_info(machine_index);
        shard_locations.insert_or_assign(shard_index, machine_index);
    }

    // Changes availability only. Mutations must not overlap reads or repairs.
    void change_machine_status(size_t machine_index, bool alive)
    {
        cluster_info.at(machine_index).alive = alive;
    }

    // Updates an existing shard's location; does not simulate a data transfer.
    // Throws out_of_range for a missing shard or destination machine, without
    // changing the mapping. Mutations must not overlap reads or repairs.
    void move_shard(std::pair<size_t, size_t> shard_index, size_t machine_index)
    {
        get_machine_info(machine_index);
        shard_locations.at(shard_index) = machine_index;
    }

    size_t get_shard_location(std::pair<size_t, size_t> shard_index) const { return shard_locations.at(shard_index); };
    bool get_machine_status(size_t machine_index) const { return get_machine_info(machine_index).alive; };
    // Returns microseconds; data_size is measured in bytes.
    virtual double get_retrieval_delay(size_t machine_index, size_t data_size) const = 0;
};
