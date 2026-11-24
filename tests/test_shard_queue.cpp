#include "src/shard_queue.h"
#include <thread>
#include <future>
#include <latch>
#include <gtest/gtest.h>

TEST(ShardQueueTest, EmptyQueueReturnsNoShard)
{
    ShardQueue queue;
    EXPECT_FALSE(queue.try_pop().has_value());
}

TEST(ShardQueueTest, WaitPopDrainsPendingShards)
{
    ShardQueue queue;
    queue.push(shard{1, {}});
    queue.push(shard{2, {}});
    queue.close();
    auto first = queue.wait_pop();
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->shard_id, 1u);
    auto second = queue.wait_pop();
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->shard_id, 2u);
    EXPECT_FALSE(queue.wait_pop().has_value());
}

TEST(ShardQueueTest, TryPopDrainsPendingShards)
{
    ShardQueue queue;
    queue.push(shard{1, {}});
    queue.push(shard{2, {}});
    queue.close();
    auto first = queue.try_pop();
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->shard_id, 1u);
    auto second = queue.try_pop();
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->shard_id, 2u);
    EXPECT_FALSE(queue.try_pop().has_value());
}

TEST(ShardQueueTest, WaitPopCalledBeforePush)
{
    ShardQueue queue;
    std::latch started{1};
    auto consumer = std::async(std::launch::async, [&]
                               {
        started.count_down();
        return queue.wait_pop(); });
    // Synchronizes consumer startup, not the exact entry into cv.wait().
    started.wait();
    queue.push(shard{3, {}});
    auto result = consumer.get();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->shard_id, 3u);
}

TEST(ShardQueueTest, TryPopCalledBeforePush)
{
    ShardQueue queue;
    EXPECT_FALSE(queue.try_pop().has_value());
    queue.push(shard{3, {}});
    auto result = queue.try_pop();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->shard_id, 3u);
    EXPECT_FALSE(queue.try_pop().has_value());
}

TEST(ShardQueueTest, TryPopUnlocked)
{
    ShardQueue queue;
    queue.push(shard{4, {}});
    auto result = queue.try_pop();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->shard_id, 4u);
    EXPECT_FALSE(queue.try_pop().has_value());
}

TEST(ShardQueueTest, WaitPopUnlocked)
{
    ShardQueue queue;
    queue.push(shard{5, {}});
    auto result = queue.wait_pop();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->shard_id, 5u);
    EXPECT_FALSE(queue.try_pop().has_value());
}

TEST(ShardQueueTest, WaitPopExitsOnClose)
{
    ShardQueue queue;
    std::latch started{1};
    auto consumer = std::async(std::launch::async, [&]
                               {
        started.count_down();
        return queue.wait_pop(); });
    started.wait();
    queue.close();
    EXPECT_FALSE(consumer.get().has_value());
}

TEST(ShardQueueTest, TryPopDoesNothingOnClosed)
{
    ShardQueue queue;
    queue.close();
    EXPECT_FALSE(queue.try_pop().has_value());
    EXPECT_FALSE(queue.try_pop().has_value());
}
