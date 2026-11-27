#include "src/delivery_scheduler.h"
#include <array>
#include <chrono>
#include <stdexcept>
#include <vector>
#include <gtest/gtest.h>

namespace
{
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

// Call only after submissions finish; process-level timeouts catch deadlocks.
std::vector<size_t> drain_ids(ShardQueue &queue)
{
    std::vector<size_t> ids;
    while (auto item = queue.wait_pop())
        ids.push_back(item->shard_id);
    return ids;
}
}

TEST(DeliverySchedulerTest, EmptySessionClosesQueue)
{
    ShardQueue queue;
    DeliveryScheduler scheduler;
    scheduler.start(queue);
    scheduler.finish_scheduling();
    EXPECT_FALSE(queue.wait_pop().has_value());
    scheduler.wait();
}

TEST(DeliverySchedulerTest, WaitEndsSubmissionsAndDeliversBorrowedView)
{
    std::array data{std::byte{0x12}, std::byte{0x34}};
    ShardQueue queue;
    DeliveryScheduler scheduler;
    scheduler.start(queue);
    scheduler.schedule({7, data}, 0ms);
    scheduler.wait();

    auto item = queue.wait_pop();
    ASSERT_TRUE(item.has_value());
    EXPECT_EQ(item->shard_id, 7u);
    EXPECT_EQ(item->data.data(), data.data());
    ASSERT_EQ(item->data.size(), data.size());
    EXPECT_EQ(item->data[0], data[0]);
    EXPECT_EQ(item->data[1], data[1]);
    EXPECT_FALSE(queue.wait_pop().has_value());
}

TEST(DeliverySchedulerTest, RelativeDelayDoesNotDeliverEarly)
{
    ShardQueue queue;
    DeliveryScheduler scheduler;
    scheduler.start(queue);
    const auto before = Clock::now();
    scheduler.schedule({1, {}}, 20ms);
    scheduler.finish_scheduling();
    auto item = queue.wait_pop();
    const auto received = Clock::now();
    scheduler.wait();
    ASSERT_TRUE(item.has_value());
    EXPECT_EQ(item->shard_id, 1u);
    EXPECT_GE(received - before, 20ms);
}

TEST(DeliverySchedulerTest, PublishesByDeadlineAndPreservesEqualDeadlineOrder)
{
    ShardQueue queue;
    DeliveryScheduler scheduler;
    scheduler.start(queue);
    const auto first = Clock::now() + 100ms;
    scheduler.schedule_at({3, {}}, first + 100ms);
    scheduler.schedule_at({1, {}}, first);
    scheduler.schedule_at({2, {}}, first);
    scheduler.finish_scheduling();
    EXPECT_EQ(drain_ids(queue), (std::vector<size_t>{1, 2, 3}));
    scheduler.wait();
}

TEST(DeliverySchedulerTest, PastDeadlineIsImmediatelyEligible)
{
    ShardQueue queue;
    DeliveryScheduler scheduler;
    scheduler.start(queue);
    scheduler.schedule_at({9, {}}, Clock::now() - 1s);
    scheduler.wait();
    EXPECT_EQ(drain_ids(queue), (std::vector<size_t>{9}));
}

TEST(DeliverySchedulerTest, AcceptsMoreDeliveriesAfterPendingQueueDrains)
{
    ShardQueue queue;
    DeliveryScheduler scheduler;
    scheduler.start(queue);
    scheduler.schedule({1, {}}, 0ms);
    auto first = queue.wait_pop();
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->shard_id, 1u);
    scheduler.schedule({2, {}}, 0ms);
    scheduler.finish_scheduling();
    EXPECT_EQ(drain_ids(queue), (std::vector<size_t>{2}));
    scheduler.wait();
}

TEST(DeliverySchedulerTest, EarlierSubmissionInterruptsLongDeadline)
{
    ShardQueue queue;
    DeliveryScheduler scheduler;
    scheduler.start(queue);
    scheduler.schedule({99, {}}, 1h);
    scheduler.schedule({1, {}}, 0ms);
    auto first = queue.wait_pop();
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->shard_id, 1u);
    // After consuming an arrival, add another while the distant one is pending.
    // Exact worker wait entry is intentionally not assumed by this test.
    scheduler.schedule({2, {}}, 0ms);
    auto second = queue.wait_pop();
    scheduler.cancel_and_wait();
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->shard_id, 2u);
    EXPECT_FALSE(queue.wait_pop().has_value());
}

TEST(DeliverySchedulerTest, CancellationDiscardsPendingDeliveriesAndClosesQueue)
{
    ShardQueue queue;
    DeliveryScheduler scheduler;
    scheduler.start(queue);
    scheduler.schedule({1, {}}, 1h);
    scheduler.schedule({2, {}}, 2h);
    scheduler.finish_scheduling();
    scheduler.cancel_and_wait();
    EXPECT_FALSE(queue.wait_pop().has_value());
}

TEST(DeliverySchedulerTest, CancellationClosesEmptyAcceptingSession)
{
    ShardQueue queue;
    DeliveryScheduler scheduler;
    scheduler.start(queue);
    scheduler.cancel_and_wait();
    EXPECT_FALSE(queue.wait_pop().has_value());
}

TEST(DeliverySchedulerTest, DestructorCancelsActiveDelivery)
{
    ShardQueue queue;
    {
        DeliveryScheduler scheduler;
        scheduler.start(queue);
        scheduler.schedule({1, {}}, 1h);
    }
    EXPECT_FALSE(queue.wait_pop().has_value());
}

TEST(DeliverySchedulerTest, ReusesWorkerAfterCompletionAndCancellation)
{
    // Queues outlive the scheduler, including on assertion failures.
    ShardQueue first;
    ShardQueue cancelled;
    ShardQueue last;
    DeliveryScheduler scheduler;
    scheduler.start(first);
    scheduler.schedule({1, {}}, 0ms);
    scheduler.wait();
    EXPECT_EQ(drain_ids(first), (std::vector<size_t>{1}));
    scheduler.start(cancelled);
    scheduler.schedule({2, {}}, 1h);
    scheduler.cancel_and_wait();
    EXPECT_FALSE(cancelled.wait_pop().has_value());
    scheduler.start(last);
    scheduler.schedule({3, {}}, 0ms);
    scheduler.wait();
    EXPECT_EQ(drain_ids(last), (std::vector<size_t>{3}));
}

TEST(DeliverySchedulerTest, RejectsSubmissionsOutsideAcceptingSession)
{
    ShardQueue queue;
    ShardQueue other;
    DeliveryScheduler scheduler;
    EXPECT_THROW(scheduler.schedule({1, {}}, 0ms), std::logic_error);
    EXPECT_THROW(scheduler.schedule_at({1, {}}, Clock::now()), std::logic_error);
    EXPECT_THROW(scheduler.finish_scheduling(), std::logic_error);
    scheduler.start(queue);
    EXPECT_THROW(scheduler.start(other), std::logic_error);
    scheduler.schedule({2, {}}, 1h);
    scheduler.finish_scheduling();
    EXPECT_THROW(scheduler.schedule({3, {}}, 0ms), std::logic_error);
    EXPECT_THROW(scheduler.schedule_at({3, {}}, Clock::now()), std::logic_error);
    scheduler.cancel_and_wait();
    EXPECT_FALSE(queue.wait_pop().has_value());
}

TEST(DeliverySchedulerTest, RejectsInvalidDelaysWithoutLosingSession)
{
    ShardQueue queue;
    DeliveryScheduler scheduler;
    scheduler.start(queue);
    EXPECT_THROW(scheduler.schedule({1, {}}, -1ms), std::invalid_argument);
    EXPECT_THROW(scheduler.schedule({2, {}}, Clock::duration::max()), std::invalid_argument);
    scheduler.schedule({3, {}}, 0ms);
    scheduler.wait();
    EXPECT_EQ(drain_ids(queue), (std::vector<size_t>{3}));
}

TEST(DeliverySchedulerTest, IdleWaitAndCancellationAreHarmless)
{
    DeliveryScheduler scheduler;
    EXPECT_NO_THROW(scheduler.wait());
    EXPECT_NO_THROW(scheduler.cancel_and_wait());
}
