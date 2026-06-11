#include <gtest/gtest.h>
#include "runtime/affinity_semaphore.h"
#include <thread>
#include <atomic>
#include <chrono>
#include <vector>
#include <coroutine>

using namespace storage::runtime::adapt;

// ============================================================================
// Non-coroutine tests
// ============================================================================

TEST(AffinitySemaphore, TryAcquireDecrements) {
    AffinitySemaphore sem(3);
    EXPECT_TRUE(sem.try_acquire());
    EXPECT_TRUE(sem.try_acquire());
    EXPECT_TRUE(sem.try_acquire());
    EXPECT_FALSE(sem.try_acquire());
    EXPECT_EQ(sem.available(), 0);
}

TEST(AffinitySemaphore, ReleaseIncrements) {
    AffinitySemaphore sem(1);
    EXPECT_TRUE(sem.try_acquire());
    EXPECT_EQ(sem.available(), 0);
    sem.release();
    EXPECT_EQ(sem.available(), 1);
    EXPECT_TRUE(sem.try_acquire());
}

TEST(AffinitySemaphore, InitialCount) {
    AffinitySemaphore sem(0);
    EXPECT_EQ(sem.available(), 0);
    EXPECT_FALSE(sem.try_acquire());
    sem.release();
    EXPECT_TRUE(sem.try_acquire());
}

TEST(AffinitySemaphore, MultipleReleases) {
    AffinitySemaphore sem(0);
    sem.release();
    sem.release();
    sem.release();
    EXPECT_EQ(sem.available(), 3);
    EXPECT_TRUE(sem.try_acquire());
    EXPECT_TRUE(sem.try_acquire());
    EXPECT_TRUE(sem.try_acquire());
    EXPECT_FALSE(sem.try_acquire());
}

TEST(AffinitySemaphore, OverflowToZero) {
    AffinitySemaphore sem(1);
    sem.release();
    sem.release();
    sem.release();
    EXPECT_EQ(sem.available(), 4);
    EXPECT_TRUE(sem.try_acquire());
    EXPECT_TRUE(sem.try_acquire());
    EXPECT_TRUE(sem.try_acquire());
    EXPECT_TRUE(sem.try_acquire());
    EXPECT_FALSE(sem.try_acquire());
}

TEST(AffinitySemaphore, ReleaseDoesNotCrash) {
    AffinitySemaphore sem(SIZE_MAX);
    sem.release();
    SUCCEED();
}

// ============================================================================
// Coroutine test — non-suspending (eager coroutine, count > 0)
// ============================================================================

struct EagerTask {
    struct promise_type {
        EagerTask get_return_object() noexcept { return {}; }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        void unhandled_exception() noexcept { std::terminate(); }
    };
};

TEST(AffinitySemaphore, CoroutineAcquireRelease) {
    AffinitySemaphore sem(2);
    std::atomic<int> counter{0};

    [&]() -> EagerTask {
        co_await sem.acquire();
        counter.fetch_add(1);
        sem.release();
    }();

    EXPECT_EQ(counter.load(), 1);
    EXPECT_EQ(sem.available(), 2);
}

TEST(AffinitySemaphore, CoroutineMultiAcquireRelease) {
    AffinitySemaphore sem(2);
    std::atomic<int> counter{0};

    [&]() -> EagerTask {
        co_await sem.acquire();
        counter.fetch_add(1);
        sem.release();

        co_await sem.acquire();
        counter.fetch_add(10);
        sem.release();

        co_await sem.acquire();
        counter.fetch_add(100);
        sem.release();
    }();

    EXPECT_EQ(counter.load(), 111);
}

// ── Coroutine suspension via a standalone function ──
// Using a free function with global pointers avoids GCC's coroutine
// frame capture corruption (the compiler misplaces captured variables
// after suspension/resume in some GCC versions).

namespace {
    AffinitySemaphore* g_sem = nullptr;
    bool* g_woke = nullptr;
}

static EagerTask test_task_impl() {
    co_await g_sem->acquire();
    *g_woke = true;
    g_sem->release();
}

TEST(AffinitySemaphore, CoroutineSuspendAndWakeup) {
    AffinitySemaphore sem(0);
    bool woke = false;
    g_sem = &sem;
    g_woke = &woke;

    // Start the coroutine; it suspends at acquire (count == 0).
    auto task = test_task_impl();
    (void)task;

    EXPECT_FALSE(woke);
    EXPECT_EQ(sem.available(), 0);

    // Release a slot — wakes the coroutine. The coroutine runs,
    // sets woke = true, and calls release() again (count 0->1->2).
    sem.release();

    EXPECT_TRUE(woke);
    // count was 0, release() in test -> 1, release() in coroutine -> 2
    EXPECT_EQ(sem.available(), 2);

    g_sem = nullptr;
    g_woke = nullptr;
}

// ============================================================================
// Multithreaded concurrency tests
// ============================================================================

TEST(AffinitySemaphore, ConcurrentLimitWithThreads) {
    constexpr int kMaxConcurrent = 3;
    constexpr int kTotalTasks = 20;
    AffinitySemaphore sem(kMaxConcurrent);
    std::atomic<int> max_concurrent{0};
    std::atomic<int> in_flight{0};
    std::atomic<int> completed{0};

    std::vector<std::thread> threads;
    threads.reserve(kTotalTasks);

    for (int i = 0; i < kTotalTasks; ++i) {
        threads.emplace_back([&]() {
            while (!sem.try_acquire()) {
                std::this_thread::yield();
            }

            int cur = in_flight.fetch_add(1) + 1;
            int prev = max_concurrent.load();
            while (cur > prev) {
                max_concurrent.compare_exchange_weak(prev, cur);
            }

            for (int x = 0; x < 5000; ++x) {
                asm volatile("" : "+r"(x));
            }

            in_flight.fetch_sub(1);
            sem.release();
            completed.fetch_add(1);
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_LE(max_concurrent.load(), kMaxConcurrent);
    EXPECT_EQ(completed.load(), kTotalTasks);
}

TEST(AffinitySemaphore, ThreadSafety) {
    constexpr int kIterations = 10000;
    constexpr int kNumThreads = 4;
    AffinitySemaphore sem(1);
    std::atomic<int> counter{0};

    std::vector<std::thread> threads;
    threads.reserve(kNumThreads);

    for (int t = 0; t < kNumThreads; ++t) {
        threads.emplace_back([&]() {
            for (int i = 0; i < kIterations; ++i) {
                while (!sem.try_acquire()) {
                    std::this_thread::yield();
                }
                int val = counter.load();
                counter.store(val + 1);
                sem.release();
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(counter.load(), kIterations * kNumThreads);
}
