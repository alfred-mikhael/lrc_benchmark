#include <gtest/gtest.h>
#include <cmath>
#include <limits>
#include "src/storage_conditions/lognormal_storage_condition.h"

TEST(LognormalStorageTest, AddsCongestionToBaseline)
{
    // Identical seeds isolate the deterministic baseline from the random delay.
    LognormalStorageCondition zero_baseline({{true, 0.0, 1.0}}, {}, 0.0, 0.5, 42);
    LognormalStorageCondition conditions(
        {{true, 10.0, 2.0}, {true, 20.0, 4.0}}, {}, 0.0, 0.5, 42);
    for (size_t i = 0; i < 100; ++i)
    {
        const auto sample = zero_baseline.get_retrieval_delay(0, 0);
        ASSERT_TRUE(std::isfinite(sample));
        ASSERT_GT(sample, 0.0);
        const auto baseline = i % 2 == 0 ? 10.25 : 20.125;
        EXPECT_DOUBLE_EQ(conditions.get_retrieval_delay(i % 2, 500), baseline + sample);
    }
}

TEST(LognormalStorageTest, SameSeedReproducesSequence)
{
    LognormalStorageCondition first({{true, 10.0, 2.0}}, {}, -0.5, 0.75, 123);
    LognormalStorageCondition second({{true, 10.0, 2.0}}, {}, -0.5, 0.75, 123);
    for (size_t i = 0; i < 100; ++i)
        EXPECT_DOUBLE_EQ(first.get_retrieval_delay(0, i * 100),
                         second.get_retrieval_delay(0, i * 100));
}

TEST(LognormalStorageTest, DifferentSeedsProduceDifferentSequences)
{
    LognormalStorageCondition first({{true, 0.0, 1.0}}, {}, 0.0, 1.0, 123);
    LognormalStorageCondition second({{true, 0.0, 1.0}}, {}, 0.0, 1.0, 456);
    bool differs = false;
    for (size_t i = 0; i < 100; ++i)
    {
        const auto a = first.get_retrieval_delay(0, 0);
        const auto b = second.get_retrieval_delay(0, 0);
        differs = differs || a != b;
    }
    EXPECT_TRUE(differs);
}

TEST(LognormalStorageTest, LogSamplesHaveRequestedMeanAndVariance)
{
    constexpr size_t count = 100000;
    constexpr double mu = 0.7;
    constexpr double sigma = 0.4;
    LognormalStorageCondition conditions({{true, 0.0, 1.0}}, {}, mu, sigma, 42);
    double sum = 0.0;
    double squared_sum = 0.0;
    for (size_t i = 0; i < count; ++i)
    {
        const auto sample = conditions.get_retrieval_delay(0, 0);
        ASSERT_TRUE(std::isfinite(sample));
        ASSERT_GT(sample, 0.0);
        const auto value = std::log(sample);
        sum += value;
        squared_sum += value * value;
    }
    const auto mean = sum / count;
    const auto variance = squared_sum / count - mean * mean;
    // Broad statistical tolerances (over seven standard errors), with a fixed
    // seed, avoid relying on implementation-specific distribution sequences.
    EXPECT_NEAR(mean, mu, 0.01);
    EXPECT_NEAR(variance, sigma * sigma, 0.006);
}

TEST(LognormalStorageTest, InvalidDistributionParametersThrow)
{
    const auto infinity = std::numeric_limits<double>::infinity();
    const auto nan = std::numeric_limits<double>::quiet_NaN();
    for (const auto mu : {infinity, -infinity, nan})
        EXPECT_THROW((LognormalStorageCondition({}, {}, mu, 1.0, 42)), std::invalid_argument);
    for (const auto sigma : {0.0, -1.0, infinity, -infinity, nan})
        EXPECT_THROW((LognormalStorageCondition({}, {}, 0.0, sigma, 42)), std::invalid_argument);
    EXPECT_NO_THROW((LognormalStorageCondition({}, {}, -1.0, 0.5, 42)));
}

TEST(LognormalStorageTest, InvalidMachineAccessDoesNotConsumeSample)
{
    LognormalStorageCondition conditions({{true, 10.0, 2.0}}, {}, 0.0, 0.5, 42);
    LognormalStorageCondition reference({{true, 10.0, 2.0}}, {}, 0.0, 0.5, 42);
    EXPECT_THROW(conditions.get_retrieval_delay(1, 1000), std::out_of_range);
    EXPECT_DOUBLE_EQ(conditions.get_retrieval_delay(0, 1000), reference.get_retrieval_delay(0, 1000));
    LognormalStorageCondition empty({}, {}, 0.0, 0.5, 42);
    EXPECT_THROW(empty.get_machine_status(0), std::out_of_range);
    EXPECT_THROW(empty.get_retrieval_delay(0, 1000), std::out_of_range);
}

TEST(LognormalStorageTest, SupportsMachineAndShardChanges)
{
    LognormalStorageCondition conditions({}, {}, 0.0, 0.5, 42);
    const auto first = conditions.add_machine({true, 10.0, 2.0});
    const auto second = conditions.add_machine({true, 20.0, 4.0});
    conditions.add_shard({0, 1}, first);
    EXPECT_EQ(conditions.get_shard_location({0, 1}), first);
    conditions.change_machine_status(first, false);
    EXPECT_FALSE(conditions.get_machine_status(first));
    conditions.move_shard({0, 1}, second);
    EXPECT_EQ(conditions.get_shard_location({0, 1}), second);
    EXPECT_TRUE(conditions.get_machine_status(second));
    EXPECT_GT(conditions.get_retrieval_delay(second, 1000), 20.25);
}
