#include <gtest/gtest.h>
#include "runtime/coro_task.h"
#include "runtime/affinity_mutex.h"

using namespace storage::runtime::adapt;

// ── co_scoped_lock() RAII guard ──

TEST(AffinityMutex, ScopedLockGuard) {
    AffinityMutex mtx;
    blockingWait([&]() -> Task<void> {
        {
            auto guard = co_await mtx.co_scoped_lock();
            // Critical section
        }
        // Should not deadlock (guard destroyed, mutex unlocked)
        co_return;
    }());
}

// ── Scoped lock ownership / unlock / re-lock ──

TEST(AffinityMutex, ScopedLockOwnership) {
    AffinityMutex mtx;
    blockingWait([&]() -> Task<void> {
        AffinityScopedLock lock = co_await mtx.co_scoped_lock();
        EXPECT_NE(lock.mtx, nullptr);  // owns lock

        lock.unlock();
        EXPECT_EQ(lock.mtx, nullptr);  // no longer owns lock

        // Re-acquire
        lock = co_await mtx.co_scoped_lock();
        EXPECT_NE(lock.mtx, nullptr);  // owns lock again
        co_return;
    }());
}

// ── Non-blocking try-lock via LockAwaiter::await_ready() ──
// LockAwaiter::await_ready() uses a CAS to acquire the lock without
// suspending — it is the coroutine-friendly equivalent of try_lock().

TEST(AffinityMutex, TryLock) {
    AffinityMutex mtx;

    // First attempt should succeed (CAS from 0 → kLockedFlag)
    auto awaiter1 = mtx.co_lock();
    EXPECT_TRUE(awaiter1.await_ready());

    // Second attempt should fail (already locked)
    auto awaiter2 = mtx.co_lock();
    EXPECT_FALSE(awaiter2.await_ready());

    mtx.unlock();

    // Third attempt should succeed after unlock
    auto awaiter3 = mtx.co_lock();
    EXPECT_TRUE(awaiter3.await_ready());
    mtx.unlock();
}

// ── Deferred locking: default-construct guard, then acquire ──

TEST(AffinityMutex, DeferLock) {
    AffinityMutex mtx;
    blockingWait([&]() -> Task<void> {
        // Default-constructed AffinityScopedLock owns nothing
        AffinityScopedLock guard;
        EXPECT_EQ(guard.mtx, nullptr);

        // Now acquire
        guard = co_await mtx.co_scoped_lock();
        EXPECT_NE(guard.mtx, nullptr);
        co_return;
    }());
}
