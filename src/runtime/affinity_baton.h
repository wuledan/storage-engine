// affinity_baton.h -- Executor-routed Baton with thread affinity
//
// Complete replacement for folly::coro::Baton that routes post() through
// the WorkStealingExecutor to guarantee thread affinity: each waiter
// is resumed on the worker thread where it originally co_await'ed.
//
// Design: intrusive linked list of WaiterNode, each recording the
// waiter's worker_id. post() atomically drains the list and calls
// executor.add_to_worker(id, handle) for each waiter.
#pragma once

#include <atomic>
#include <chrono>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>

namespace storage::runtime::adapt {

// Duration alias matching runtime::Duration (steady_clock::duration)
using Duration = std::chrono::steady_clock::duration;

// RouteFunc: routes a coroutine handle to the specified worker.
//   worker_id: target worker's ID (from WaiterNode.worker_id)
//   handle:    coroutine to resume on that worker
// The implementation must deliver `handle` to the scheduler of worker `worker_id`.
using RouteFunc = std::function<void(size_t worker_id, std::coroutine_handle<>)>;

namespace detail {
    extern size_t (*get_current_worker_id)();
}

// ── AffinityBaton ──
//
// Multi-waiter Baton with executor-based routing.
//
// API parity with folly::coro::Baton:
//   co_await baton          -- suspend until posted
//   baton.ready()           -- query posted state
//   baton.try_wait()        -- non-blocking check
//   baton.reset()           -- reset to not-ready
//   baton.post(executor)    -- resume waiters via executor (affinity!)
//   baton.post_direct()     -- resume waiters inline (no executor)
//
class AffinityBaton {
public:
    AffinityBaton() noexcept = default;

    ~AffinityBaton() {
        // If there are still waiters (baton was destroyed before post),
        // resume them inline so they can continue and eventually notice.
        auto* waiters = waiters_.exchange(nullptr, std::memory_order_acq_rel);
        if (waiters) {
            // Clear the posted bit (kPostedBit = 1) which may be encoded in
            // waiters_ after post() has been called.  Without clearing, we'd
            // pass an invalid pointer (e.g. 0x1) into resume_chain.
            resume_chain(clear_posted(waiters), nullptr);
        }
    }

    AffinityBaton(const AffinityBaton&) = delete;
    AffinityBaton& operator=(const AffinityBaton&) = delete;

    // ── Query ──

    bool ready() const noexcept {
        auto v = waiters_.load(std::memory_order_acquire);
        return reinterpret_cast<uintptr_t>(v) & kPostedBit;
    }

    bool try_wait() const noexcept {
        return ready();
    }

    // ── Reset (only safe when no waiters exist) ──

    void reset() noexcept {
        // Clear posted bit; only safe with no waiters
        auto* v = waiters_.load(std::memory_order_relaxed);
        waiters_.store(v, std::memory_order_release);
    }

    // ── Intrusive waiter node ──

    struct WaiterNode {
        std::coroutine_handle<> handle;
        size_t worker_id;     // waiter's worker_id (SIZE_MAX = external thread)
        WaiterNode* next;
    };

    // ── Awaiter (returned by operator co_await) ──

    struct Awaiter {
        AffinityBaton& baton;
        WaiterNode node;

        bool await_ready() const noexcept {
            return baton.ready();
        }

        void await_suspend(std::coroutine_handle<> handle) noexcept {
            node.handle = handle;
            node.worker_id = current_worker_id();
            node.next = nullptr;

            // Single CAS: atomically checks posted bit AND enqueues.
            // No window between "check state" and "CAS into waiters".
            auto* old = baton.waiters_.load(std::memory_order_acquire);

            do {
                if (reinterpret_cast<uintptr_t>(old) & kPostedBit) {
                    // Already posted — don't suspend
                    handle.resume();
                    return;
                }
                node.next = clear_posted(old);
            } while (!baton.waiters_.compare_exchange_weak(
                old, &node,
                std::memory_order_release,
                std::memory_order_acquire));
        }

        void await_resume() const noexcept {}
    };

    // ── Timed wait ──

    enum class WaitResult { kSignaled, kTimeout };

    // Shared state between TimedAwaiter and the timer expiry callback.
    struct TimedWaitState {
        std::atomic<bool> timed_out{false};
    };

    struct TimedAwaiter {
        AffinityBaton& baton;
        Duration timeout;
        WaiterNode node;
        std::shared_ptr<TimedWaitState> state;
        WaitResult result{WaitResult::kTimeout};

        bool await_ready() const noexcept {
            return baton.ready();
        }

        // Defined in worker.cc (needs timer.h, online_worker.h, etc.)
        void await_suspend(std::coroutine_handle<> h) noexcept;

        WaitResult await_resume() const noexcept {
            return state->timed_out.load(std::memory_order_acquire)
                ? WaitResult::kTimeout : WaitResult::kSignaled;
        }
    };

    TimedAwaiter wait_for(Duration timeout) noexcept {
        return {*this, timeout, {}, std::make_shared<TimedWaitState>(), WaitResult::kTimeout};
    }

    Awaiter operator co_await() noexcept {
        return Awaiter{*this, WaiterNode{}};
    }

    // ── Post with routing ──
    //
    // Atomically drains the waiter chain and routes each waiter's
    // continuation through the provided RouteFunc. The caller is
    // responsible for delivering the coroutine handle to the correct
    // worker thread (e.g. via add_to_worker or a work-stealing executor).
    void post(RouteFunc route);

    // ── Direct post (no routing) ──
    //
    // Resume all waiters inline on the calling thread.
    // Use for: destruction cleanup, test code, non-executor contexts.
    void post_direct() noexcept;

private:
    static size_t current_worker_id();

    static void resume_chain(WaiterNode* waiters,
                             RouteFunc* route);

    // Posted bit encoded in waiters_ pointer low bit (pointer is 4/8-byte aligned).
    // This merges state_ and waiters_ into a single atomic, eliminating the
    // "check state_ then CAS waiters_" race window in await_suspend.
    static constexpr uintptr_t kPostedBit = 1;
    static WaiterNode* clear_posted(WaiterNode* p) noexcept {
        return reinterpret_cast<WaiterNode*>(
            reinterpret_cast<uintptr_t>(p) & ~kPostedBit);
    }

    std::atomic<WaiterNode*> waiters_{nullptr};
};

}  // namespace storage::runtime::adapt
