#include <gtest/gtest.h>
#include "runtime/work_stealing_executor.h"
#include "runtime/work_item.h"
#include <thread>
#include <atomic>
#include <chrono>

using namespace storage::runtime;

// ── Global counters for function-pointer-based WorkItem ──
static std::atomic<int> g_counter{0};
static void inc_counter() { g_counter.fetch_add(1); }
static void spin_a_bit() { for (volatile int x = 0; x < 100; ++x); }

// ============================================================================
// Test basic start/stop
// ============================================================================
TEST(WorkStealingExecutor, StartStop) {
    WorkStealingExecutor::Config cfg;
    cfg.num_workers = 2;
    cfg.pin_cpus = false;
    WorkStealingExecutor exec(cfg);
    exec.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    exec.stop();
    exec.join();
}

// ============================================================================
// Test folly::Executor interface — add(folly::Func) via base class pointer
// ============================================================================
TEST(WorkStealingExecutor, FollyExecutorAdd) {
    WorkStealingExecutor::Config cfg;
    cfg.num_workers = 2;
    cfg.pin_cpus = false;
    WorkStealingExecutor exec(cfg);
    exec.start();

    std::atomic<int> counter{0};
    constexpr int N = 500;

    for (int i = 0; i < N; ++i) {
        // Dispatch through folly::Executor base class pointer
        folly::Executor* base = &exec;
        base->add([&counter] { counter.fetch_add(1, std::memory_order_relaxed); });
    }

    // Wait for all tasks to complete
    while (counter.load() < N) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(counter.load(), N);

    exec.stop();
    exec.join();
}

// ============================================================================
// Test that WorkStealingExecutor::current_folly_executor() returns non-null
// on worker threads
// ============================================================================
TEST(WorkStealingExecutor, CurrentFollyExecutor) {
    WorkStealingExecutor::Config cfg;
    cfg.num_workers = 1;
    cfg.pin_cpus = false;
    WorkStealingExecutor exec(cfg);
    exec.start();

    std::atomic<bool> checked{false};

    exec.add(WorkItem::make_func([]() {
        // On a WSE worker thread, current_folly_executor() should be non-null
        EXPECT_NE(WorkStealingExecutor::current_folly_executor(), nullptr);
        // current_executor() should match
        EXPECT_EQ(WorkStealingExecutor::current_executor(),
                  dynamic_cast<WorkStealingExecutor*>(
                      WorkStealingExecutor::current_folly_executor()));
    }));

    // Also submit via folly::Executor interface and verify the TLS is set
    folly::Executor* base = &exec;
    base->add([&checked] {
        EXPECT_NE(WorkStealingExecutor::current_folly_executor(), nullptr);
        checked.store(true, std::memory_order_release);
    });

    while (!checked.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    exec.stop();
    exec.join();
}

// ============================================================================
// Test submit and execute via add() (global queue)
// ============================================================================
TEST(WorkStealingExecutor, SubmitTask) {
    WorkStealingExecutor::Config cfg;
    cfg.num_workers = 2;
    cfg.pin_cpus = false;
    WorkStealingExecutor exec(cfg);
    exec.start();

    g_counter = 0;
    constexpr int N = 1000;

    for (int i = 0; i < N; ++i) {
        exec.add(WorkItem::make_func(inc_counter));
    }

    // Wait for all tasks to complete
    while (g_counter.load() < N) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(g_counter.load(), N);

    exec.stop();
    exec.join();
}

// ============================================================================
// Test that tasks submitted directly to a worker's local deque get executed
// (equivalent to add_to_worker but with function-based WorkItem)
// ============================================================================
TEST(WorkStealingExecutor, WorkerLocalDeque) {
    WorkStealingExecutor::Config cfg;
    cfg.num_workers = 2;
    cfg.pin_cpus = false;
    WorkStealingExecutor exec(cfg);
    exec.start();

    g_counter = 0;
    // Enqueue directly to worker 0's local ring via public WorkerState API
    for (int i = 0; i < 100; ++i) {
        exec.worker(0).local_deque->enqueue(WorkItem::make_func(inc_counter));
    }
    exec.worker(0).has_work.store(true, std::memory_order_release);
    exec.worker(0).park.notify();

    while (g_counter.load() < 100) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(g_counter.load(), 100);

    exec.stop();
    exec.join();
}

// ============================================================================
// Test work stealing — submit all work to worker 0, worker 1 should steal
// ============================================================================
TEST(WorkStealingExecutor, WorkStealing) {
    WorkStealingExecutor::Config cfg;
    cfg.num_workers = 2;
    cfg.pin_cpus = false;
    cfg.steal_attempts = 8;
    WorkStealingExecutor exec(cfg);
    exec.start();

    // Enqueue tasks to worker 0's local ring only
    for (int i = 0; i < 50; ++i) {
        exec.worker(0).local_deque->enqueue(WorkItem::make_func(spin_a_bit));
    }
    exec.worker(0).has_work.store(true, std::memory_order_release);
    exec.worker(0).park.notify();

    // Also add some via global queue so worker 1 has a chance to grab those
    for (int i = 0; i < 10; ++i) {
        exec.add(WorkItem::make_func(spin_a_bit));
    }

    // Wait long enough for work to be distributed
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Both workers should have executed something
    EXPECT_GT(exec.worker(0).tasks_executed.load(), 0u);
    EXPECT_GT(exec.worker(1).tasks_executed.load(), 0u);

    exec.stop();
    exec.join();
}
