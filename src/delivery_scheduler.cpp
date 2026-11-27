#include "delivery_scheduler.h"
#include <algorithm>
#include <stdexcept>
#include <utility>

DeliveryScheduler::DeliveryScheduler()
    : worker([this](std::stop_token stop) { run(stop); }) {}

DeliveryScheduler::~DeliveryScheduler()
{
    try
    {
        cancel_and_wait();
    }
    catch (...)
    {
        // Completion and queue closure precede propagation of producer errors.
    }
    worker.request_stop();
    changed.notify_all();
    // The jthread member joins before the remaining members are destroyed.
}

void DeliveryScheduler::start(ShardQueue &queue)
{
    std::lock_guard guard(mutex);
    if (state != State::idle)
        throw std::logic_error("Finalize the previous delivery session before starting another");
    arrivals = &queue;
    cancelled = false;
    error = nullptr;
    state = State::accepting;
    changed.notify_all();
}

void DeliveryScheduler::schedule(shard value, Clock::duration delay)
{
    const auto now = Clock::now();
    if (delay < Clock::duration::zero())
        throw std::invalid_argument("Delivery delay must be nonnegative");
    if (now.time_since_epoch() > Clock::duration::max() - delay)
        throw std::invalid_argument("Delivery deadline overflow");
    schedule_at(value, now + delay);
}

void DeliveryScheduler::schedule_at(shard value, Clock::time_point deadline)
{
    std::lock_guard guard(mutex);
    if (state != State::accepting)
        throw std::logic_error("Delivery session is not accepting submissions");
    const auto position = std::upper_bound(pending.begin(), pending.end(), deadline,
        [](auto time, const PendingDelivery &delivery) { return time < delivery.deadline; });
    pending.insert(position, PendingDelivery{value, deadline});
    changed.notify_all();
}

void DeliveryScheduler::schedule_batch(std::vector<ScheduledShard> deliveries)
{
    {
        std::lock_guard guard(mutex);
        if (state != State::accepting)
            throw std::logic_error("Delivery session is not accepting submissions");
    }
    std::stable_sort(deliveries.begin(), deliveries.end(),
        [](const auto &a, const auto &b) { return a.delay < b.delay; });
    const auto start = Clock::now();
    for (const auto &delivery : deliveries)
    {
        if (delivery.delay < Clock::duration::zero())
            throw std::invalid_argument("Delivery delay must be nonnegative");
        if (start.time_since_epoch() > Clock::duration::max() - delivery.delay)
            throw std::invalid_argument("Delivery deadline overflow");
    }
    for (const auto &delivery : deliveries)
        schedule_at(delivery.value, start + delivery.delay);
}

void DeliveryScheduler::finish_scheduling()
{
    std::lock_guard guard(mutex);
    if (state == State::idle)
        throw std::logic_error("No delivery session has been started");
    if (state == State::accepting)
        state = State::draining;
    changed.notify_all();
}

void DeliveryScheduler::wait()
{
    std::unique_lock guard(mutex);
    if (state == State::idle)
        return;
    if (state == State::accepting)
        state = State::draining;
    changed.notify_all();
    changed.wait(guard, [this] { return state == State::completed; });
    const auto failure = std::exchange(error, nullptr);
    arrivals = nullptr;
    state = State::idle;
    guard.unlock();
    if (failure)
        std::rethrow_exception(failure);
}

void DeliveryScheduler::cancel_and_wait()
{
    {
        std::lock_guard guard(mutex);
        if (state == State::idle)
            return;
        cancelled = true;
        changed.notify_all();
    }
    wait();
}

void DeliveryScheduler::run(std::stop_token stop)
{
    std::unique_lock guard(mutex);
    while (!stop.stop_requested())
    {
        changed.wait(guard, stop, [this] {
            return state == State::accepting || state == State::draining;
        });
        if (stop.stop_requested())
            break;
        if (cancelled || (state == State::draining && pending.empty()))
        {
            pending.clear();
            arrivals->close();
            state = State::completed;
            changed.notify_all();
            continue;
        }
        if (pending.empty())
        {
            changed.wait(guard, stop, [this] {
                return cancelled || state != State::accepting || !pending.empty();
            });
            continue;
        }
        const auto deadline = pending.front().deadline;
        if (Clock::now() < deadline)
        {
            changed.wait_until(guard, stop, deadline, [this, deadline] {
                return cancelled || pending.front().deadline < deadline;
            });
            continue;
        }
        try
        {
            // Queue push is nonblocking apart from its short mutex acquisition.
            // Holding our mutex serializes publication with cancellation.
            arrivals->push(pending.front().value);
            pending.erase(pending.begin());
        }
        catch (...)
        {
            error = std::current_exception();
            cancelled = true;
        }
    }
}
