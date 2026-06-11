#include <gtest/gtest.h>
#include "runtime/affinity_mutex.h"
#include <mutex>  // for std::lock_guard, std::unique_lock

using namespace storage::runtime::adapt;

TEST(AffinityMutex, StdLockGuard) {
    AffinityMutex mtx;
    {
        std::lock_guard<AffinityMutex> guard(mtx);
        // Critical section
    }
    // Should not deadlock
}

TEST(AffinityMutex, StdUniqueLock) {
    AffinityMutex mtx;
    std::unique_lock<AffinityMutex> lock(mtx);
    EXPECT_TRUE(lock.owns_lock());
    lock.unlock();
    EXPECT_FALSE(lock.owns_lock());
    lock.lock();
    EXPECT_TRUE(lock.owns_lock());
}

TEST(AffinityMutex, TryLock) {
    AffinityMutex mtx;
    EXPECT_TRUE(mtx.try_lock());
    EXPECT_FALSE(mtx.try_lock());  // already locked
    mtx.unlock();
    EXPECT_TRUE(mtx.try_lock());
    mtx.unlock();
}

TEST(AffinityMutex, DeferLock) {
    AffinityMutex mtx;
    std::unique_lock<AffinityMutex> lock(mtx, std::defer_lock);
    EXPECT_FALSE(lock.owns_lock());
    lock.lock();
    EXPECT_TRUE(lock.owns_lock());
}
