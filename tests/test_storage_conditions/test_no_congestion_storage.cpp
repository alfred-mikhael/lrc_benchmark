#include <gtest/gtest.h>
#include <limits>
#include <random>
#include <unordered_set>
#include "src/storage_conditions/no_congestion_storage_condition.h"

TEST(NoCongestionStorageTest, SingleServerDelay)
{
    NoCongestionStorageCondition conditions({{true, 10.0, 2.0}}, {});

    EXPECT_DOUBLE_EQ(conditions.get_retrieval_delay(0, 0), 10.0);
    EXPECT_DOUBLE_EQ(conditions.get_retrieval_delay(0, 500), 10.25);
    EXPECT_DOUBLE_EQ(conditions.get_retrieval_delay(0, 1000), 10.5);
    EXPECT_DOUBLE_EQ(conditions.get_retrieval_delay(0, 8000), 14.0);
    EXPECT_DOUBLE_EQ(conditions.get_retrieval_delay(0, 8000), 14.0);
}

TEST(NoCongestionStorageTest, MultipleServerDelay)
{
    NoCongestionStorageCondition conditions(
        {{true, 10.0, 2.0}, {true, 20.0, 4.0}, {true, 0.0, 0.5}}, {});

    EXPECT_DOUBLE_EQ(conditions.get_retrieval_delay(0, 4000), 12.0);
    EXPECT_DOUBLE_EQ(conditions.get_retrieval_delay(1, 4000), 21.0);
    EXPECT_DOUBLE_EQ(conditions.get_retrieval_delay(2, 4000), 8.0);
}

TEST(NoCongestionStorageTest, ZeroMachinesThrowsError)
{
    NoCongestionStorageCondition conditions({}, {});
    EXPECT_THROW(conditions.get_machine_status(0), std::out_of_range);
    EXPECT_THROW(conditions.get_retrieval_delay(0, 1000), std::out_of_range);
}

TEST(NoCongestionStorageTest, SingleServerShardLocations)
{
    NoCongestionStorageCondition conditions(
        {{true, 10.0, 2.0}}, {{{0, 0}, 0}, {{0, 1}, 0}, {{1, 0}, 0}});

    EXPECT_EQ(conditions.get_shard_location({0, 0}), 0u);
    EXPECT_EQ(conditions.get_shard_location({0, 1}), 0u);
    EXPECT_EQ(conditions.get_shard_location({1, 0}), 0u);
    EXPECT_THROW(conditions.get_shard_location({1, 1}), std::out_of_range);
}

TEST(NoCongestionStorageTest, MultipleServerShardLocations)
{
    NoCongestionStorageCondition conditions(
        {{true, 10.0, 2.0}, {true, 20.0, 4.0}, {true, 0.0, 0.5}},
        {{{0, 0}, 2}, {{0, 1}, 0}, {{1, 0}, 1}, {{1, 1}, 2}});

    EXPECT_EQ(conditions.get_shard_location({0, 0}), 2u);
    EXPECT_EQ(conditions.get_shard_location({0, 1}), 0u);
    EXPECT_EQ(conditions.get_shard_location({1, 0}), 1u);
    EXPECT_EQ(conditions.get_shard_location({1, 1}), 2u);
    EXPECT_THROW(conditions.get_shard_location({2, 0}), std::out_of_range);
}

TEST(NoCongestionStorageTest, FewShardHashCollisions)
{
    constexpr size_t input_count = 100000;
    std::mt19937_64 generator(42);
    std::uniform_int_distribution<size_t> distribution(0, std::numeric_limits<size_t>::max());
    std::unordered_set<std::pair<size_t, size_t>, shard_location_hash> inputs;
    std::unordered_set<size_t> hashes;
    inputs.reserve(input_count);
    hashes.reserve(input_count);
    size_t collisions = 0;
    while (inputs.size() < input_count)
    {
        const std::pair<size_t, size_t> input{distribution(generator), distribution(generator)};
        if (!inputs.insert(input).second)
            continue; // Repeated identical inputs are not hash collisions.
        if (!hashes.insert(shard_location_hash{}(input)).second)
            ++collisions;
    }
    EXPECT_LT(collisions, 10u);
}

TEST(NoCongestionStorageTest, AddMachine)
{
    NoCongestionStorageCondition conditions({}, {});
    const auto first = conditions.add_machine({true, 10.0, 2.0});
    const auto second = conditions.add_machine({false, 20.0, 4.0});
    EXPECT_EQ(first, 0u);
    EXPECT_EQ(second, 1u);
    EXPECT_TRUE(conditions.get_machine_status(first));
    EXPECT_FALSE(conditions.get_machine_status(second));
    EXPECT_DOUBLE_EQ(conditions.get_retrieval_delay(first, 1000), 10.5);
    EXPECT_DOUBLE_EQ(conditions.get_retrieval_delay(second, 1000), 20.25);
    EXPECT_THROW(conditions.add_machine({true, 0.0, 0.0}), std::invalid_argument);
    EXPECT_THROW(conditions.add_machine({true, -1.0, 1.0}), std::invalid_argument);
    EXPECT_THROW(conditions.add_machine({true, 0.0, std::numeric_limits<double>::infinity()}), std::invalid_argument);
    EXPECT_THROW(conditions.add_machine({true, std::numeric_limits<double>::quiet_NaN(), 1.0}), std::invalid_argument);
    EXPECT_EQ(conditions.add_machine({true, 0.0, 1.0}), 2u);
}

TEST(NoCongestionStorageTest, AddShard)
{
    NoCongestionStorageCondition conditions({}, {});
    EXPECT_THROW(conditions.add_shard({0, 0}, 0), std::out_of_range);
    EXPECT_THROW(conditions.get_shard_location({0, 0}), std::out_of_range);
    const auto first = conditions.add_machine({true, 10.0, 2.0});
    const auto second = conditions.add_machine({true, 20.0, 4.0});
    conditions.add_shard({0, 1}, first);
    conditions.add_shard({1, 0}, second);
    EXPECT_EQ(conditions.get_shard_location({0, 1}), first);
    EXPECT_EQ(conditions.get_shard_location({1, 0}), second);
    conditions.add_shard({0, 1}, second);
    EXPECT_EQ(conditions.get_shard_location({0, 1}), second);
    EXPECT_THROW(conditions.add_shard({0, 1}, 2), std::out_of_range);
    EXPECT_EQ(conditions.get_shard_location({0, 1}), second);
}

TEST(NoCongestionStorageTest, MachineStatus)
{
    NoCongestionStorageCondition conditions(
        {{true, 10.0, 2.0}, {false, 20.0, 4.0}, {true, 0.0, 0.5}}, {});

    EXPECT_TRUE(conditions.get_machine_status(0));
    EXPECT_FALSE(conditions.get_machine_status(1));
    EXPECT_TRUE(conditions.get_machine_status(2));
    EXPECT_THROW(conditions.get_machine_status(3), std::out_of_range);
}

TEST(NoCongestionStorageTest, ChangeMachineStatus)
{
    NoCongestionStorageCondition conditions(
        {{true, 10.0, 2.0}, {false, 20.0, 4.0}}, {{{0, 0}, 0}});

    conditions.change_machine_status(0, false);
    EXPECT_FALSE(conditions.get_machine_status(0));
    EXPECT_FALSE(conditions.get_machine_status(1));
    conditions.change_machine_status(0, true);
    conditions.change_machine_status(1, true);
    EXPECT_TRUE(conditions.get_machine_status(0));
    EXPECT_TRUE(conditions.get_machine_status(1));
    conditions.change_machine_status(1, true);
    EXPECT_TRUE(conditions.get_machine_status(1));
    EXPECT_EQ(conditions.get_shard_location({0, 0}), 0u);
    EXPECT_DOUBLE_EQ(conditions.get_retrieval_delay(0, 1000), 10.5);
    EXPECT_THROW(conditions.change_machine_status(2, false), std::out_of_range);
    EXPECT_TRUE(conditions.get_machine_status(0));
    EXPECT_TRUE(conditions.get_machine_status(1));
}

TEST(NoCongestionStorageTest, MoveShard)
{
    NoCongestionStorageCondition conditions(
        {{true, 10.0, 2.0}, {true, 20.0, 4.0}}, {{{0, 0}, 0}, {{0, 1}, 0}});

    conditions.move_shard({0, 0}, 1);
    EXPECT_EQ(conditions.get_shard_location({0, 0}), 1u);
    EXPECT_EQ(conditions.get_shard_location({0, 1}), 0u);
    EXPECT_DOUBLE_EQ(conditions.get_retrieval_delay(conditions.get_shard_location({0, 0}), 1000), 20.25);
    conditions.move_shard({0, 0}, 1);
    EXPECT_EQ(conditions.get_shard_location({0, 0}), 1u);
    EXPECT_THROW(conditions.move_shard({0, 0}, 2), std::out_of_range);
    EXPECT_EQ(conditions.get_shard_location({0, 0}), 1u);
    EXPECT_THROW(conditions.move_shard({1, 0}, 0), std::out_of_range);
    EXPECT_THROW(conditions.get_shard_location({1, 0}), std::out_of_range);
}
