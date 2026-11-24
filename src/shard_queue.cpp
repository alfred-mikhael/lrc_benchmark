#include "src/shard_queue.h"
#include <mutex>
#include <condition_variable>

ShardQueue::ShardQueue() : arrivals(std::queue<shard>()), open(true) {}

std::optional<shard> ShardQueue::try_pop()
{
    auto mtx = std::unique_lock<std::mutex>(lock, std::try_to_lock);
    if (!mtx.owns_lock())
        return std::nullopt;

    if (arrivals.empty())
        return std::nullopt;
    shard s = arrivals.front();
    arrivals.pop();
    return s;
}

std::optional<shard> ShardQueue::wait_pop()
{
    auto mtx = std::unique_lock<std::mutex>(lock);
    cv.wait(mtx, [this]
            { return !arrivals.empty() || !open; });
    if (arrivals.empty())
        return std::nullopt;

    shard s = arrivals.front();
    arrivals.pop();
    return s;
}

void ShardQueue::push(shard const &s)
{
    {
        auto mtx = std::scoped_lock<std::mutex>(lock);
        if (!open)
            open = true;
        arrivals.push(s);
    }
    cv.notify_one();
}

void ShardQueue::close()
{
    {
        auto mtx = std::scoped_lock<std::mutex>(lock);
        open = false;
    }
    cv.notify_all();
}
