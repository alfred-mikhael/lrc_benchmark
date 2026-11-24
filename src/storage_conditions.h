#pragma once
#include <cstddef>
#include <functional>
#include <utility>
#include <vector>
#include <unordered_map>

struct machine_info
{
    /* data */
    bool alive;
    double base_latency_us;
    double link_speed; // KiB per microsecond
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

typedef struct storage_conditions
{
    /* data */
    std::vector<machine_info> cluster_info;
    // Maps (extent number, shard number) to an index in cluster_info.
    std::unordered_map<std::pair<size_t, size_t>, size_t, shard_location_hash> shard_locations;

    bool get_machine_status(size_t machine_index) const;
    double get_retrieval_delay(size_t machine_index, size_t data_size) const;

} storage_conditions;
