#pragma once
#include <coroutine>
#include <cstddef>
#include "worker.h"
#include "work_queue.h"
#include "work_item.h"

namespace storage::runtime {

// Yield: suspend the current coroutine and re-enqueue its handle
// to the specified queue index. The Scheduler will resume it when
// that queue is drained.
struct yield_awaiter {
    size_t queue_idx;

    bool await_ready() const noexcept { return false; }

    void await_suspend(std::coroutine_handle<> h) noexcept {
        auto* w = current_worker();
        if (!w) return;
        auto* q = w->get_queue(queue_idx);
        if (q) {
            q->enqueue(WorkItem::make_coro(h));
        }
    }

    void await_resume() const noexcept {}
};

inline yield_awaiter yield_to(size_t queue_idx) { return {queue_idx}; }

// Simple yield: suspend and re-enqueue to the current worker's default queue.
// For OnlineWorker, this is the engine queue (P2).
// Equivalent to std::this_thread::yield() — cooperative, non-blocking.
//
// Usage:  co_await yield();
//
inline yield_awaiter yield() {
    auto* w = current_worker();
    return { w ? w->default_queue_idx() : 0 };
}

}  // namespace storage::runtime
