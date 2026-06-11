// affinity_semaphore.h -- Counting semaphore with thread affinity
//
// Modeled after std::counting_semaphore but with coroutine support and
// thread affinity: waiters resume on the same worker thread where they
// originally co_await'ed (via current_worker_id() from the executor).
//
// Usage:
//   AffinitySemaphore sem(4);  // max 4 concurrent
//   co_await sem.acquire();    // wait for slot
//   // ... critical section ...
//   sem.release();             // return slot
//
#pragma once

#include "affinity_baton.h"
#include <atomic>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <new>

namespace storage::runtime::adapt {

// Counting semaphore with thread affinity.
// - acquire(): co_await to decrement count; suspends if count == 0
// - release(): increment count, wake one waiter
// - try_acquire(): non-blocking check
//
class AffinitySemaphore {
public:
    explicit AffinitySemaphore(size_t initial_count) noexcept
        : count_(initial_count) {}

    AffinitySemaphore(const AffinitySemaphore&) = delete;
    AffinitySemaphore& operator=(const AffinitySemaphore&) = delete;

    // ── Acquire awaiter ──
    //
    // The WaiterNode is NOT embedded in the awaiter. Instead it is
    // heap-allocated in await_suspend. This guarantees the node survives
    // regardless of where the compiler places the AcquireAwaiter temporary
    // (coroutine frame vs. real stack) — critical when the awaiter is
    // wrapped by external adapters such as folly::ViaIfAsyncAwaiter.
    struct AcquireAwaiter {
        AffinitySemaphore& sem;

        bool await_ready() const noexcept {
            return sem.try_acquire();
        }

        void await_suspend(std::coroutine_handle<> h) noexcept;
        void await_resume() const noexcept {}
    };

    AcquireAwaiter acquire() noexcept { return {*this}; }
    bool try_acquire() noexcept;
    void release() noexcept;

    size_t available() const noexcept {
        return count_.load(std::memory_order_acquire);
    }

private:
    static size_t current_worker_id() noexcept {
        return detail::get_current_worker_id();
    }

    // Wait queue: linked list of WaiterNode, protected by single CAS
    std::atomic<AffinityBaton::WaiterNode*> waiters_{nullptr};
    std::atomic<size_t> count_;
};

// ── Implementation ──

inline bool AffinitySemaphore::try_acquire() noexcept {
    size_t expected = count_.load(std::memory_order_acquire);
    while (expected > 0) {
        if (count_.compare_exchange_weak(expected, expected - 1,
                std::memory_order_acq_rel, std::memory_order_relaxed)) {
            return true;
        }
    }
    return false;
}

inline void AffinitySemaphore::AcquireAwaiter::await_suspend(
    std::coroutine_handle<> h) noexcept {
    // Heap-allocate the WaiterNode so it survives regardless of where the
    // AcquireAwaiter temporary lives (coroutine frame vs. real stack).
    auto* node = new (std::nothrow) AffinityBaton::WaiterNode{};
    if (!node) {
        // Allocation failure — resume immediately (safe fallback).
        h.resume();
        return;
    }
    node->handle = h;
    node->worker_id = sem.current_worker_id();
    node->next = nullptr;

    // CAS into waiters_ head
    auto* old = sem.waiters_.load(std::memory_order_acquire);
    do {
        // Re-check count — might have been released between await_ready and now
        if (sem.try_acquire()) {
            // Got a slot — don't suspend; free the allocated node.
            delete node;
            h.resume();
            return;
        }
        node->next = old;
    } while (!sem.waiters_.compare_exchange_weak(
        old, node, std::memory_order_release, std::memory_order_acquire));
}

inline void AffinitySemaphore::release() noexcept {
    count_.fetch_add(1, std::memory_order_release);

    // Dequeue one waiter and resume
    auto* old = waiters_.load(std::memory_order_acquire);
    while (old) {
        if (waiters_.compare_exchange_weak(old, old->next,
                std::memory_order_acquire, std::memory_order_relaxed)) {
            // Resume the waiter inline, then free the heap-allocated node.
            auto* node = old;
            node->handle.resume();
            delete node;
            return;
        }
    }
}

}  // namespace storage::runtime::adapt
