// affinity_mutex.h -- Executor-routed Mutex with thread affinity
//
// Complete replacement for folly::coro::Mutex that routes unlock()
// through the WorkStealingExecutor to guarantee thread affinity: each
// waiter is resumed on the worker thread where it originally co_await'ed.
//
// Design: single atomic state word encoding lock flag + waiter list.
//   state_ == 0           : unlocked, no waiters
//   state_ == kLockedFlag : locked, no waiters
//   state_ & ~kLockedFlag : locked, with waiters (pointer to Waiter stack)
//
// This avoids all races between lock release and waiter enqueue because
// the state transition is atomic.
#pragma once

#include <atomic>
#include <coroutine>
#include <cstddef>
#include <cstdint>

#include "affinity_baton.h"

namespace storage::runtime::adapt {

struct ScopedLockAwaiter;

// ── AffinityMutex ──
//
// Coroutine-friendly mutex with thread-affine wake-up.
//
// API parity with folly::coro::Mutex:
//   co_await mutex.co_lock()   -- acquire exclusive lock (coroutine)
//   mutex.co_scoped_lock()     -- acquire and return RAII guard
//   mutex.try_lock()           -- non-blocking try
//   mutex.unlock()             -- release (called by guard)
//
class AffinityMutex {
public:
    AffinityMutex() noexcept = default;

    ~AffinityMutex() {
        // If there are still waiters (mutex destroyed while locked with
        // waiters), resume them inline so they can continue and eventually
        // notice. This should not happen in correct code.
        auto state = state_.load(std::memory_order_acquire);
        if (auto* waiters = extract_waiters(state)) {
            while (waiters) {
                auto* next = waiters->next;
                waiters->handle.resume();
                waiters = next;
            }
        }
    }

    AffinityMutex(const AffinityMutex&) = delete;
    AffinityMutex& operator=(const AffinityMutex&) = delete;

    // ── Intrusive waiter node ──

    struct Waiter {
        std::coroutine_handle<> handle;
        size_t worker_id;  // waiter's worker_id (SIZE_MAX = external thread)
        Waiter* next;
    };

    // ── Awaiter for co_lock() ──

    struct LockAwaiter {
        AffinityMutex& mutex;
        Waiter node;

        bool await_ready() const noexcept {
            // Try to acquire without suspending: CAS state_ from 0 to kLockedFlag
            uintptr_t expected = 0;
            return mutex.state_.compare_exchange_strong(
                expected, kLockedFlag,
                std::memory_order_acquire,
                std::memory_order_relaxed);
        }

        bool await_suspend(std::coroutine_handle<> handle) noexcept {
            node.handle = handle;
            node.worker_id = current_worker_id();
            node.next = nullptr;

            // We failed to acquire the lock (await_ready returned false).
            // Enqueue ourselves onto the waiter stack.
            auto* waiter_ptr = &node;

            // CAS loop to push onto the waiter stack.
            auto old_state = mutex.state_.load(std::memory_order_acquire);

            do {
                // Double-check: if lock became free, try to acquire directly
                if (old_state == 0) {
                    if (mutex.state_.compare_exchange_strong(
                            old_state, kLockedFlag,
                            std::memory_order_acquire,
                            std::memory_order_relaxed)) {
                        // Got the lock, don't suspend
                        return false;
                    }
                    // CAS failed, someone else grabbed it. old_state is updated.
                    continue;
                }

                // Lock is held. Push our waiter onto the stack.
                // Existing waiters list is at (old_state & ~kLockedFlag).
                auto* old_waiters = reinterpret_cast<Waiter*>(
                    old_state & ~kLockedFlag);
                waiter_ptr->next = old_waiters;

                uintptr_t new_state = reinterpret_cast<uintptr_t>(waiter_ptr)
                                      | kLockedFlag;

                if (mutex.state_.compare_exchange_weak(
                        old_state, new_state,
                        std::memory_order_release,
                        std::memory_order_acquire)) {
                    // Successfully enqueued; suspend.
                    return true;
                }
                // CAS failed, state changed. old_state is updated. Retry.
                // Note: waiter_ptr->next may be stale now, but it will be
                // re-set on the next iteration.
            } while (true);
        }

        void await_resume() const noexcept {
            // We now hold the lock
        }
    };

    // ── co_lock(): acquire exclusive lock ──

    LockAwaiter co_lock() noexcept {
        return LockAwaiter{*this, Waiter{}};
    }

    // ── Non-coroutine lock — std::lock_guard / std::unique_lock compatible ──
    //
    // SAFETY: Only safe when caller and lock holder are on the SAME worker thread.
    // In multi-worker (offline) scenarios, use co_await mutex.co_lock() instead.
    // This method spin-waits — calling it cross-worker will deadlock.
    //
    void lock() noexcept {
        while (!try_lock()) {
            while (state_.load(std::memory_order_relaxed) & kLockedFlag) {
                __builtin_ia32_pause();
            }
        }
    }

    // ── RAII lock guard ──

    class AffinityMutexLock {
    public:
        explicit AffinityMutexLock(AffinityMutex& mutex) noexcept
            : mutex_(&mutex) {}

        ~AffinityMutexLock() {
            if (mutex_) {
                mutex_->unlock();
            }
        }

        AffinityMutexLock(const AffinityMutexLock&) = delete;
        AffinityMutexLock& operator=(const AffinityMutexLock&) = delete;

        AffinityMutexLock(AffinityMutexLock&& other) noexcept
            : mutex_(other.mutex_) {
            other.mutex_ = nullptr;
        }

        AffinityMutexLock& operator=(AffinityMutexLock&& other) noexcept {
            if (this != &other) {
                if (mutex_) {
                    mutex_->unlock();
                }
                mutex_ = other.mutex_;
                other.mutex_ = nullptr;
            }
            return *this;
        }

    private:
        AffinityMutex* mutex_;
    };

    // ── co_scoped_lock(): acquire and return RAII guard via co_await ──
    // Usage: auto guard = co_await mutex.co_scoped_lock();
    ScopedLockAwaiter co_scoped_lock() noexcept;

    // ── try_lock(): non-blocking try ──

    bool try_lock() noexcept {
        uintptr_t expected = 0;
        return state_.compare_exchange_strong(
            expected, kLockedFlag,
            std::memory_order_acquire,
            std::memory_order_relaxed);
    }

    // ── unlock(): release lock, wake next waiter if any ──

    void unlock();

    // ── 设置路由回调（用于线程亲和唤醒） ──

    void set_route(RouteFunc route) { route_ = std::move(route); }

private:
    static size_t current_worker_id();

    static constexpr uintptr_t kLockedFlag = 1;

    static Waiter* extract_waiters(uintptr_t state) noexcept {
        return reinterpret_cast<Waiter*>(state & ~kLockedFlag);
    }

    // Combined state: either 0 (unlocked), kLockedFlag (locked, no waiters),
    // or (waiter_ptr | kLockedFlag) (locked, with waiters).
    // Waiter pointers are always aligned (at least 2-byte aligned for
    // struct with pointer + size_t members), so bit 0 is safe for the flag.
    std::atomic<uintptr_t> state_{0};

    RouteFunc route_;  // 路由回调，unlock() 中唤醒 waiter 时使用
};

// ── Scoped RAII guard ──
// Works with both blocking lock/unlock and co_await co_scoped_lock().
// Movable, default-constructible (null mutex).
struct AffinityScopedLock {
    AffinityMutex* mtx{nullptr};

    AffinityScopedLock() = default;
    explicit AffinityScopedLock(AffinityMutex& m) noexcept : mtx(&m) {}

    AffinityScopedLock(AffinityScopedLock&& other) noexcept : mtx(other.mtx) {
        other.mtx = nullptr;
    }
    AffinityScopedLock& operator=(AffinityScopedLock&& other) noexcept {
        if (mtx) mtx->unlock();
        mtx = other.mtx;
        other.mtx = nullptr;
        return *this;
    }
    AffinityScopedLock(const AffinityScopedLock&) = delete;

    ~AffinityScopedLock() {
        if (mtx) mtx->unlock();
    }

    void unlock() noexcept {
        if (mtx) { mtx->unlock(); mtx = nullptr; }
    }
};

// ── Awaiter for co_await mutex.co_scoped_lock() ──
struct ScopedLockAwaiter {
    AffinityMutex& mtx;
    AffinityMutex::LockAwaiter inner;

    bool await_ready() const noexcept { return inner.await_ready(); }
    void await_suspend(std::coroutine_handle<> h) noexcept { inner.await_suspend(h); }
    AffinityScopedLock await_resume() noexcept { return AffinityScopedLock(mtx); }
};

// Out-of-line definition of co_scoped_lock() (needs ScopedLockAwaiter complete)
inline ScopedLockAwaiter AffinityMutex::co_scoped_lock() noexcept {
    return {*this, {*this, {}}};
}

}  // namespace storage::runtime::adapt
