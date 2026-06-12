#pragma once
#include "coro_primitives.h"
#include <chrono>
#include <condition_variable>
#include <coroutine>
#include <functional>
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

    // Optional callback used by timed baton wait: if set, timer expiry
    // calls on_expire() instead of resuming `handle` directly.
    // The callback is responsible for posting the baton.
    std::function<void()> on_expire;

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
// Global static instance shared across all WorkStealingExecutors.
// Thread-safe: mutex protects the min-heap.
//
// The timer thread uses sleep_until_next() to block on a condition_variable
// until the earliest deadline, avoiding busy-wait.  insert() calls
// notify_one() when a new shorter deadline appears, waking the timer thread
// early.
class OfflineTimerWheel {
public:
    // Insert a timer node.  If the new node becomes the new earliest deadline,
    // notify the sleeping timer thread so it can re-adjust its wake-up time.
    void insert(TimerNode node) {
        bool need_wake = false;
        {
            std::lock_guard<std::mutex> lk(mutex_);
            TimePoint prev_top =
                pending_.empty() ? TimePoint::max() : pending_.top().deadline;
            pending_.push(std::move(node));
            need_wake = !pending_.empty() && pending_.top().deadline < prev_top;
        }
        if (need_wake) cv_.notify_one();
    }

    // Pop all expired timers. Thread-safe.
    std::vector<TimerNode> expire(TimePoint now) {
        std::lock_guard<std::mutex> lk(mutex_);
        std::vector<TimerNode> expired;
        while (!pending_.empty() && pending_.top().deadline <= now) {
            expired.push_back(pending_.top());
            pending_.pop();
        }
        return expired;
    }

    // Block the calling thread (timer thread) until the earliest deadline is
    // reached.  If the heap is empty, wait indefinitely until a timer is
    // inserted (and insert() calls notify_one()).
    //
    // Must be called WITHOUT holding mutex_ — this method acquires the lock
    // internally.
    void sleep_until_next() {
        std::unique_lock<std::mutex> lk(mutex_);
        if (pending_.empty()) {
            cv_.wait(lk);
        } else {
            cv_.wait_until(lk, pending_.top().deadline);
        }
    }

    // Wake the timer thread (e.g. on stop).
    void notify_one() { cv_.notify_one(); }

private:
    std::priority_queue<TimerNode> pending_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
};

// ── Global offline timer ──
// Shared across all WorkStealingExecutors with a single timer thread that
// sleeps via condition_variable until the next deadline.
void start_global_timer_thread();
void stop_global_timer_thread();
OfflineTimerWheel& global_offline_timer();

// ── co_sleep awaiter ──

struct SleepAwaiter {
    TimerNode node;
    
    bool await_ready() noexcept { return false; }
    
    // Defined in timer.cc (needs complete OnlineWorker type)
    void await_suspend(std::coroutine_handle<> h) noexcept;
    
    void await_resume() noexcept {}
};

inline SleepAwaiter co_sleep_for(Duration dur) {
    TimerNode tn{};
    tn.deadline = Clock::now() + dur;
    return {tn};
}

inline SleepAwaiter co_sleep_until(TimePoint deadline) {
    TimerNode tn{};
    tn.deadline = deadline;
    return {tn};
}

}  // namespace storage::runtime
