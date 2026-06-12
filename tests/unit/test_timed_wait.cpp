#include <gtest/gtest.h>
#include "runtime/affinity_baton.h"
#include "runtime/timer.h"
#include "runtime/coro_api.h"
#include "runtime/online_worker.h"
#include "runtime/work_stealing_executor.h"
#include "runtime/work_item.h"
#include <thread>
#include <atomic>
#include <chrono>

using namespace storage::runtime;
using namespace storage::runtime::adapt;
using namespace std::chrono_literals;

using WaitResult = AffinityBaton::WaitResult;

// ── Coroutine task type ──
// initial_suspend = always   → submitted via handle, never runs inline
// final_suspend   = never    → coroutine frame destroyed after co_return
struct WaitTask {
    struct promise_type {
        WaitTask get_return_object() {
            return WaitTask{
                std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_always initial_suspend() noexcept { return {}; }
        std::suspend_never  final_suspend()   noexcept { return {}; }
        void return_void()   noexcept {}
        void unhandled_exception() noexcept { std::terminate(); }
    };
    std::coroutine_handle<promise_type> handle;
};

// ============================================================================
// Non-coroutine basics
// ============================================================================

TEST(TimedWait, AlreadyPosted) {
    AffinityBaton baton;
    // Post before wait
    baton.post_direct();
    // wait_for(1h) should return kSignaled immediately because baton is ready
    // We test await_ready directly:
    auto awaiter = baton.wait_for(1h);
    EXPECT_TRUE(awaiter.await_ready());
}

TEST(TimedWait, NotPosted) {
    AffinityBaton baton;
    auto awaiter = baton.wait_for(1h);
    EXPECT_FALSE(awaiter.await_ready());
}

// ============================================================================
// OnlineWorker: timed wait with actual timer expiry
// ============================================================================

TEST(TimedWait, OnlineTimeoutExpires) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    OnlineWorker w(cfg);
    w.scheduler().set_busy_poll(true);
    w.start();

    static AffinityBaton baton;
    static std::atomic<WaitResult> result{WaitResult::kSignaled};
    static std::atomic<bool> done{false};

    auto task = []() -> WaitTask {
        result.store(co_await baton.wait_for(10ms));
        done.store(true);
    }();

    w.submit_engine(WorkItem::make_coro(task.handle));

    // Do NOT post the baton — let the timer expire
    int spins = 0;
    while (!done.load() && spins < 500) {
        std::this_thread::sleep_for(1ms);
        ++spins;
    }

    EXPECT_TRUE(done.load()) << "Timed wait did not complete within timeout";
    EXPECT_EQ(result.load(), WaitResult::kTimeout);

    w.stop();
    w.join();
}

TEST(TimedWait, OnlineSignaledBeforeTimeout) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    OnlineWorker w(cfg);
    w.scheduler().set_busy_poll(true);
    w.start();

    static AffinityBaton baton;
    static std::atomic<WaitResult> result{WaitResult::kTimeout};
    static std::atomic<bool> done{false};
    static std::atomic<bool> posted{false};

    auto task = []() -> WaitTask {
        result.store(co_await baton.wait_for(100ms));
        done.store(true);
    }();

    w.submit_engine(WorkItem::make_coro(task.handle));

    // Post the baton after a short delay (well before 100ms timeout)
    std::thread poster([&]() {
        std::this_thread::sleep_for(2ms);
        posted.store(true);
        baton.post_direct();
    });

    int spins = 0;
    while (!done.load() && spins < 500) {
        std::this_thread::sleep_for(1ms);
        ++spins;
    }

    EXPECT_TRUE(done.load()) << "Timed wait did not complete after post";
    EXPECT_TRUE(posted.load());
    EXPECT_EQ(result.load(), WaitResult::kSignaled);

    poster.join();
    w.stop();
    w.join();
}

TEST(TimedWait, OnlineZeroTimeout) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    OnlineWorker w(cfg);
    w.scheduler().set_busy_poll(true);
    w.start();

    static AffinityBaton baton;
    static std::atomic<WaitResult> result{WaitResult::kSignaled};
    static std::atomic<bool> done{false};

    auto task = []() -> WaitTask {
        // Zero timeout on an unposted baton should immediately time out
        result.store(co_await baton.wait_for(0ms));
        done.store(true);
    }();

    w.submit_engine(WorkItem::make_coro(task.handle));

    int spins = 0;
    while (!done.load() && spins < 500) {
        std::this_thread::sleep_for(1ms);
        ++spins;
    }

    EXPECT_TRUE(done.load()) << "Zero timeout did not complete";
    EXPECT_EQ(result.load(), WaitResult::kTimeout);

    w.stop();
    w.join();
}

// ============================================================================
// WorkStealingExecutor: timed wait
// ============================================================================

TEST(TimedWait, WseTimeoutExpires) {
    WorkStealingExecutor::Config cfg;
    cfg.num_workers = 2;
    cfg.pin_cpus = false;
    WorkStealingExecutor exec(cfg);
    exec.start();

    static AffinityBaton baton;
    static std::atomic<WaitResult> result{WaitResult::kSignaled};
    static std::atomic<bool> done{false};

    auto task = []() -> WaitTask {
        result.store(co_await baton.wait_for(15ms));
        done.store(true);
    }();

    exec.add(WorkItem::make_coro(task.handle));

    int spins = 0;
    while (!done.load() && spins < 500) {
        std::this_thread::sleep_for(1ms);
        ++spins;
    }

    EXPECT_TRUE(done.load()) << "WSE timed wait did not complete within timeout";
    EXPECT_EQ(result.load(), WaitResult::kTimeout);

    exec.stop();
    exec.join();
}

TEST(TimedWait, WseSignaledBeforeTimeout) {
    WorkStealingExecutor::Config cfg;
    cfg.num_workers = 2;
    cfg.pin_cpus = false;
    WorkStealingExecutor exec(cfg);
    exec.start();

    static AffinityBaton baton;
    static std::atomic<WaitResult> result{WaitResult::kTimeout};
    static std::atomic<bool> done{false};

    auto task = []() -> WaitTask {
        result.store(co_await baton.wait_for(200ms));
        done.store(true);
    }();

    exec.add(WorkItem::make_coro(task.handle));

    std::thread poster([&]() {
        std::this_thread::sleep_for(2ms);
        baton.post_direct();
    });

    int spins = 0;
    while (!done.load() && spins < 500) {
        std::this_thread::sleep_for(1ms);
        ++spins;
    }

    EXPECT_TRUE(done.load()) << "WSE timed wait did not complete after post";
    EXPECT_EQ(result.load(), WaitResult::kSignaled);

    poster.join();
    exec.stop();
    exec.join();
}

// ============================================================================
// Multiple timed waiters on the same baton
// ============================================================================

TEST(TimedWait, OnlineMultipleWaitersOneTimesOut) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    OnlineWorker w(cfg);
    w.scheduler().set_busy_poll(true);
    w.start();

    static AffinityBaton baton;
    static std::atomic<int> signaled_count{0};
    static std::atomic<int> timeout_count{0};
    static std::atomic<int> done_count{0};

    auto waiter1 = []() -> WaitTask {
        auto r = co_await baton.wait_for(5ms);   // short timeout → will time out
        if (r == WaitResult::kSignaled) signaled_count.fetch_add(1);
        else timeout_count.fetch_add(1);
        done_count.fetch_add(1);
    }();

    auto waiter2 = []() -> WaitTask {
        auto r = co_await baton.wait_for(200ms);  // long timeout → post comes first
        if (r == WaitResult::kSignaled) signaled_count.fetch_add(1);
        else timeout_count.fetch_add(1);
        done_count.fetch_add(1);
    }();

    w.submit_engine(WorkItem::make_coro(waiter1.handle));
    w.submit_engine(WorkItem::make_coro(waiter2.handle));

    // Wait a bit for waiter1 to time out, then post
    std::this_thread::sleep_for(30ms);
    baton.post_direct();

    int spins = 0;
    while (done_count.load() < 2 && spins < 500) {
        std::this_thread::sleep_for(1ms);
        ++spins;
    }

    EXPECT_EQ(done_count.load(), 2);
    // waiter1 should have timed out (5ms timeout, we waited 30ms before posting)
    // waiter2 should have been signaled (200ms timeout, we posted at 30ms)
    EXPECT_GE(timeout_count.load(), 1);
    EXPECT_GE(signaled_count.load(), 1);

    w.stop();
    w.join();
}
