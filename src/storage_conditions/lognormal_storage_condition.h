#pragma once

#include "storage_conditions.h"
#include <cmath>
#include <cstdint>
#include <random>
#include <stdexcept>

class LognormalStorageCondition : public StorageConditions
{
    mutable std::mt19937_64 generator;
    mutable std::lognormal_distribution<double> distribution;

    static std::lognormal_distribution<double> make_distribution(double mu, double sigma)
    {
        if (!std::isfinite(mu) || !std::isfinite(sigma) || sigma <= 0.0)
            throw std::invalid_argument("Lognormal mu must be finite and sigma must be finite and positive");
        return std::lognormal_distribution<double>(mu, sigma);
    }

public:
    // mu and sigma describe the underlying normal distribution. Each sample
    // exp(N(mu, sigma)) adds microseconds to the no-congestion retrieval delay.
    LognormalStorageCondition(
        std::vector<machine_info> machines,
        std::unordered_map<std::pair<size_t, size_t>, size_t, shard_location_hash> locations,
        double mu, double sigma, std::uint64_t seed)
        : StorageConditions(std::move(machines), std::move(locations)),
          generator(seed), distribution(make_distribution(mu, sigma)) {}

    // Each call advances the random sequence, even for the same shard or machine.
    // Calls on one instance must be serialized. Equal seeds and call sequences
    // reproduce samples with the same standard-library implementation; exact
    // distribution results are not guaranteed to match across implementations.
    double get_retrieval_delay(size_t machine_index, size_t data_size) const override
    {
        const auto baseline = get_baseline_retrieval_delay(machine_index, data_size);
        return baseline + distribution(generator);
    }
};
