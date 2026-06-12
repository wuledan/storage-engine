#pragma once
#include "coro_primitives.h"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <coroutine>
#include <mutex>
#include <queue>
#include <vector>

namespace storage::runtime {

using Clock = std::chrono::steady_clock;
using TimePoint = Clock::time_point;
using Duration = Clock::duration;

// Timer node in min-heap
struct TimerNode {
    TimePoint deadline;
    size_t source_queue_idx{SIZE_MAX};     // 原优先级队列索引
    size_t source_worker_id{SIZE_MAX};     // 原 worker ID
    std::coroutine_handle<> handle{};       // 待恢复协程
    
    // min-heap: earlier deadline = higher priority
    bool operator<(const TimerNode& o) const noexcept {
        return deadline > o.deadline;  // reversed for std::priority_queue (max-heap by default)
    }
};

// Per-worker timer state (single worker, no lock needed)
class WorkerTimerState {
public:
    void insert(TimerNode node) {
        pending_.push(std::move(node));
    }
    
    // Pop all expired timers
    std::vector<TimerNode> expire(TimePoint now) {
        std::vector<TimerNode> expired;
        while (!pending_.empty() && pending_.top().deadline <= now) {
            expired.push_back(pending_.top());
            pending_.pop();
        }
        return expired;
    }
    
    // Next deadline (or max if empty) — for optional sleep optimization
    TimePoint next_deadline() const noexcept {
        if (pending_.empty()) return TimePoint::max();
        return pending_.top().deadline;
    }
    
    bool empty() const noexcept { return pending_.empty(); }
    size_t size() const noexcept { return pending_.size(); }

private:
    std::priority_queue<TimerNode> pending_;
};

// ── Offline (shared) timer wheel ──
// Single instance shared across all offline workers in a WorkStealingExecutor.
// Thread-safe: mutex protects the min-heap.
class OfflineTimerWheel {
public:
    void insert(TimerNode node) {
        std::lock_guard<std::mutex> lock(mutex_);
        pending_.push(std::move(node));
        cv_.notify_one();
    }

    // Pop all expired timers. Thread-safe.
    std::vector<TimerNode> expire(TimePoint now) {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<TimerNode> expired;
        while (!pending_.empty() && pending_.top().deadline <= now) {
            expired.push_back(pending_.top());
            pending_.pop();
        }
        return expired;
    }

    // Next deadline, or TimePoint::max() if empty.
    TimePoint next_deadline() const noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        if (pending_.empty()) return TimePoint::max();
        return pending_.top().deadline;
    }

    // Block until either:
    //   - a new timer is inserted (cv_ notified), or
    //   - the earliest deadline is reached.
    // Returns when the caller should re-check for expired timers.
    void wait_for_work_or_deadline() {
        std::unique_lock<std::mutex> lock(mutex_);
        if (pending_.empty()) {
            cv_.wait(lock);
        } else {
            cv_.wait_until(lock, pending_.top().deadline);
        }
    }

    // Wake up any thread blocked in wait_for_work_or_deadline().
    void notify() { cv_.notify_all(); }

private:
    std::priority_queue<TimerNode> pending_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
};

// ── co_sleep awaiter ──

struct SleepAwaiter {
    TimerNode node;
    
    bool await_ready() noexcept { return false; }
    
    // Defined in timer.cc (needs complete OnlineWorker type)
    void await_suspend(std::coroutine_handle<> h) noexcept;
    
    void await_resume() noexcept {}
};

inline SleepAwaiter co_sleep_for(Duration dur) {
    return {TimerNode{Clock::now() + dur}};
}

inline SleepAwaiter co_sleep_until(TimePoint deadline) {
    return {TimerNode{deadline}};
}

}  // namespace storage::runtime
