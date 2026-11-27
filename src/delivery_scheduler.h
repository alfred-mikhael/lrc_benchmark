#pragma once

#include "shard_queue.h"
#include <chrono>
#include <condition_variable>
#include <exception>
#include <mutex>
#include <stop_token>
#include <thread>
#include <vector>

/**
 * Publishes shard views using one reusable worker thread and one active session.
 * Construct before timing to exclude thread creation from measurements.
 * The caller computes delays; the scheduler handles deadlines and queue closure.
 * Public calls must be serialized; publication and queue consumption run
 * concurrently. Only the scheduler's worker pushes into the arrival queue.
 *
 * The queue must live until wait() or cancel_and_wait() finishes, even when it
 * reports an error. Shard data must remain alive and unchanged until publication
 * and all consuming decoding tasks have finished. The scheduler owns no data.
 */
class DeliveryScheduler
{
    using Clock = std::chrono::steady_clock;

    struct PendingDelivery
    {
        shard value;
        Clock::time_point deadline;
    };

    enum class State { idle, accepting, draining, completed };

    // All session state is protected by mutex. Pending deliveries are ordered
    // by deadline, with submission order preserved for equal deadlines.
    std::mutex mutex;
    std::condition_variable_any changed;
    std::vector<PendingDelivery> pending;
    ShardQueue *arrivals = nullptr;
    State state = State::idle;
    bool cancelled = false;
    std::exception_ptr error;

    // Last member so it is destroyed before the state it accesses.
    std::jthread worker;
    void run(std::stop_token stop);

public:
    struct ScheduledShard
    {
        Clock::duration delay;
        shard value;
    };

    DeliveryScheduler();
    // Cancels outstanding delivery and joins the worker without throwing.
    ~DeliveryScheduler();
    DeliveryScheduler(const DeliveryScheduler &) = delete;
    DeliveryScheduler &operator=(const DeliveryScheduler &) = delete;
    DeliveryScheduler(DeliveryScheduler &&) = delete;
    DeliveryScheduler &operator=(DeliveryScheduler &&) = delete;

    // Begins a session with a fresh, open, empty queue and no other producer.
    // The previous session must have been finalized by wait/cancel_and_wait.
    // An empty pending list does not close the queue while submissions are open.
    void start(ShardQueue &queue);

    // Schedules one shard after delay, relative to this call's clock reading.
    // Accepts chrono durations such as milliseconds{10} or microseconds{250}.
    // A zero delay is immediately eligible; negative/overflowing delays are
    // rejected. Requires an accepting session. Copies only the descriptor.
    void schedule(shard value, Clock::duration delay);

    // Schedules at an absolute deadline, allowing several calls to share one
    // reference time. Past deadlines are immediately eligible. Inserting an
    // earlier deadline wakes the worker to reconsider its current wait.
    // Actual delivery can be late due to OS scheduling.
    void schedule_at(shard value, Clock::time_point deadline);

    // Schedules a batch relative to one clock reading after sorting by delay.
    // Equal delays preserve input order. Requires an accepting session, even
    // for an empty batch. Validates all delays before submitting any shard.
    // Allocation failures may leave a partially submitted batch; the caller
    // must still finalize the session. Does not end submissions.
    void schedule_batch(std::vector<ScheduledShard> deliveries);

    // Ends submissions without blocking. The worker closes the queue once all
    // pending deliveries have been published, including for an empty session.
    // Call before blocking in a decoder that may need end-of-input to finish.
    void finish_scheduling();

    // Ends submissions if still open, waits for delivery and queue closure, and
    // returns the scheduler to idle. Rethrows any producer error after cleanup.
    // Does not wait for decoding workers or terminate the reusable worker.
    void wait();

    // Discards pending deliveries, interrupts deadline waits, and waits for
    // publication to stop and the queue to close. Already queued shards remain.
    // A publication racing with cancellation may finish before return.
    // Returns to idle and rethrows any producer error after cleanup.
    void cancel_and_wait();
};
