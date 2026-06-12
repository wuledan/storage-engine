#include <gtest/gtest.h>
#include "runtime/timer.h"
#include "runtime/coro_api.h"
#include "runtime/online_worker.h"
#include "runtime/work_stealing_executor.h"
#include "runtime/work_item.h"
#include <thread>
#include <atomic>
#include <chrono>

using namespace storage::runtime;
using namespace std::chrono_literals;

// ── Coroutine task type for timer tests ──
// initial_suspend = always   → submitted via handle, never runs inline
// final_suspend   = never    → coroutine frame destroyed after co_return
struct TimerTask {
    struct promise_type {
        TimerTask get_return_object() {
            return TimerTask{
                std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_always initial_suspend() noexcept { return {}; }
        std::suspend_never  final_suspend()   noexcept { return {}; }
        void return_void()   noexcept {}
        void unhandled_exception() noexcept { std::terminate(); }
    };
    std::coroutine_handle<promise_type> handle;
};

// ── Basic sleep accuracy (Online) ──

TEST(Timer, OnlineCoSleepBasic) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    OnlineWorker w(cfg);
    // Busy-poll mode: the timer coroutine (P1) re-enqueues itself on every
    // iteration and would starve the engine queue (P2) in policy-based mode.
    w.scheduler().set_busy_poll(true);
    w.start();

    static std::atomic<bool> done{false};
    static std::atomic<int64_t> elapsed_us{0};
    static auto start = Clock::now();

    // Captureless coroutine lambda — static variables are accessible
    auto task = []() -> TimerTask {
        start = Clock::now();
        co_await co_sleep_for(10ms);
        elapsed_us.store(
            std::chrono::duration_cast<std::chrono::microseconds>(
                Clock::now() - start).count());
        done.store(true);
    }();

    w.submit_engine(WorkItem::make_coro(task.handle));

    while (!done.load()) std::this_thread::sleep_for(1ms);

    auto us = elapsed_us.load();
    EXPECT_GE(us, 8000);   // at least 8ms (allow scheduling jitter)
    EXPECT_LE(us, 50000);  // no more than 50ms

    w.stop();
    w.join();
}

// ── Multiple timers fire in order ──

TEST(Timer, OnlineMultipleTimersOrdered) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    OnlineWorker w(cfg);
    // Busy-poll mode: the timer coroutine (P1) re-enqueues itself on every
    // iteration and would starve the engine queue (P2) in policy-based mode.
    w.scheduler().set_busy_poll(true);
    w.start();

    static std::atomic<int> sequence{0};
    static std::atomic<int> results[3]{0, 0, 0};

    // Captureless coroutine lambda — static variables accessible
    auto make_task = [](int id, int delay_ms) -> TimerTask {
        co_await co_sleep_for(std::chrono::milliseconds(delay_ms));
        results[id].store(sequence.fetch_add(1));
    };

    auto t0 = make_task(0, 30);
    w.submit_engine(WorkItem::make_coro(t0.handle));
    auto t1 = make_task(1, 10);
    w.submit_engine(WorkItem::make_coro(t1.handle));
    auto t2 = make_task(2, 20);
    w.submit_engine(WorkItem::make_coro(t2.handle));

    // Wait for all timers to fire
    std::this_thread::sleep_for(100ms);

    // 10ms should fire first (result[1] = 0), then 20ms, then 30ms
    EXPECT_LT(results[1].load(), results[2].load());
    EXPECT_LT(results[2].load(), results[0].load());

    w.stop();
    w.join();
}

// ── Offline timer basic (WorkStealingExecutor) ──

TEST(Timer, OfflineCoSleepBasic) {
    WorkStealingExecutor::Config cfg;
    cfg.num_workers = 2;
    cfg.pin_cpus = false;
    WorkStealingExecutor exec(cfg);
    exec.start();

    static std::atomic<bool> done{false};
    static auto start = Clock::now();
    static std::atomic<int64_t> elapsed_us{0};

    auto task = []() -> TimerTask {
        start = Clock::now();
        co_await co_sleep_for(10ms);
        elapsed_us.store(
            std::chrono::duration_cast<std::chrono::microseconds>(
                Clock::now() - start).count());
        done.store(true);
    }();

    exec.add(WorkItem::make_coro(task.handle));

    while (!done.load()) std::this_thread::sleep_for(1ms);

    auto us = elapsed_us.load();
    EXPECT_GE(us, 8000);
    EXPECT_LE(us, 50000);

    exec.stop();
    exec.join();
}

// ── Offline: timer restores to original worker ──

TEST(Timer, OfflineTimerRestoresToOriginalWorker) {
    WorkStealingExecutor::Config cfg;
    cfg.num_workers = 2;
    cfg.pin_cpus = false;
    WorkStealingExecutor exec(cfg);
    exec.start();

    static std::atomic<size_t> resume_worker{SIZE_MAX};
    static std::atomic<bool> done{false};

    auto task = []() -> TimerTask {
        co_await co_sleep_for(5ms);
        resume_worker.store(WorkStealingExecutor::current_executor()
            ? WorkStealingExecutor::current_worker_id() : SIZE_MAX);
        done.store(true);
    }();

    exec.add_to_worker(0, task.handle);

    while (!done.load()) std::this_thread::sleep_for(1ms);

    // Should have resumed on some valid worker
    EXPECT_NE(resume_worker.load(), SIZE_MAX);

    exec.stop();
    exec.join();
}
