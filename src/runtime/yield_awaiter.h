#pragma once
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include "worker.h"
#include "work_queue.h"
#include "work_item.h"
#include "scheduler.h"

namespace storage::runtime {

// Yield: suspend the current coroutine and re-enqueue its handle
// to the specified queue index. The Scheduler will resume it when
// that queue is drained.
struct yield_awaiter {
    size_t queue_idx;

    bool await_ready() const noexcept { return false; }

    void await_suspend(std::coroutine_handle<> h) noexcept {
        auto* w = current_worker();
        WorkQueue* q = nullptr;
        if (w) {
            q = w->get_queue(queue_idx);
        } else {
            // Before the worker loop starts (e.g. during init), current_worker()
            // is null.  Fall back to tls_source_queue which is set alongside
            // tls_source_queue_idx by the Scheduler (or by init_io_backend).
            q = tls_source_queue;
        }
        if (q) {
            q->enqueue(WorkItem::make_coro(h));
        }
    }

    void await_resume() const noexcept {}
};

inline yield_awaiter yield_to(size_t queue_idx) { return {queue_idx}; }

// Simple yield: suspend and re-enqueue to the source queue of the current
// work item.  The source queue is set by the Scheduler via
// tls_source_queue_idx before each execute().
//
// Fallback order (should never reach fallbacks in practice):
//   1. tls_source_queue_idx  (set by Scheduler before execute)
//   2. current_worker()->default_queue_idx()  (engine queue for OnlineWorker)
//   3. 0  (last resort)
//
// Equivalent to std::this_thread::yield() — cooperative, non-blocking.
//
// Usage:  co_await yield();
//
inline yield_awaiter yield() {
    size_t idx = (tls_source_queue_idx != SIZE_MAX) ? tls_source_queue_idx : SIZE_MAX;
    if (idx == SIZE_MAX) {
        auto* w = current_worker();
        idx = w ? w->default_queue_idx() : 0;
    }
    return {idx};
}

}  // namespace storage::runtime
