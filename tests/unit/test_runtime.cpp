#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>
#include "runtime/online_group.h"
#include "runtime/runtime.h"

using namespace storage::runtime;

// NOTE: The full Runtime class requires adapt/WorkStealingExecutor for the
// OfflineWorkerGroup, which depends on external quant_invest headers not
// available in this build. Therefore we test the online runtime lifecycle
// through OnlineWorkerGroup directly, which exercises the same
// start/stop/task-submission path used by Runtime::online_group().
//
// The Runtime class itself is a thin container; its start() calls
// online_group_.start() and offline_group_.start(), and its stop() calls
// the corresponding stop() methods. Testing OnlineWorkerGroup lifecycle
// validates the core runtime behavior.

static std::atomic<int> g_rt_counter{0};

// ============================================================================
// OnlineWorkerGroup lifecycle: start/stop （模拟 Runtime 生命周期）
// ============================================================================
TEST(RuntimeTest, StartStop) {
    OnlineWorkerGroup::Config cfg;
    cfg.num_workers = 2;
    OnlineWorkerGroup group(cfg);

    g_rt_counter = 0;
    group.submit_to(1,
        WorkItem::make_func(+[]() { g_rt_counter.fetch_add(1); }));
    group.submit_to(2,
        WorkItem::make_func(+[]() { g_rt_counter.fetch_add(10); }));

    group.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    group.stop();

    EXPECT_GE(g_rt_counter.load(), 11);
}

// ============================================================================
// Start/stop with no tasks
// ============================================================================
TEST(RuntimeTest, StartStopEmpty) {
    OnlineWorkerGroup::Config cfg;
    cfg.num_workers = 1;
    OnlineWorkerGroup group(cfg);
    EXPECT_NO_THROW(group.start());
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_NO_THROW(group.stop());
}

// ============================================================================
// Single worker group
// ============================================================================
TEST(RuntimeTest, SingleWorkerConfig) {
    OnlineWorkerGroup::Config cfg;
    cfg.num_workers = 1;
    OnlineWorkerGroup group(cfg);

    g_rt_counter = 0;
    group.submit_to(0,
        WorkItem::make_func(+[]() { g_rt_counter = 42; }));

    group.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    group.stop();

    EXPECT_EQ(g_rt_counter.load(), 42);
}

// ============================================================================
// Multiple start/stop cycles
// ============================================================================
TEST(RuntimeTest, StartStopMultipleTimes) {
    OnlineWorkerGroup::Config cfg;
    cfg.num_workers = 2;
    OnlineWorkerGroup group(cfg);

    static std::atomic<int> s_phase{0};
    for (int i = 0; i < 3; ++i) {
        s_phase = i + 1;
        g_rt_counter = 0;
        group.submit_to(i,
            WorkItem::make_func(+[]() { g_rt_counter.store(s_phase.load()); }));

        EXPECT_NO_THROW(group.start());
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        EXPECT_NO_THROW(group.stop());

        EXPECT_EQ(g_rt_counter.load(), i + 1);
    }
}

// ============================================================================
// Stats accessible after stop
// ============================================================================
TEST(RuntimeTest, StatsAccessibleAfterStop) {
    OnlineWorkerGroup::Config cfg;
    cfg.num_workers = 2;
    OnlineWorkerGroup group(cfg);

    g_rt_counter = 0;
    group.submit_to(1,
        WorkItem::make_func(+[]() { g_rt_counter.fetch_add(1); }));

    group.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    group.stop();

    auto s = group.stats();
    EXPECT_EQ(s.worker_count, 2u);
    EXPECT_GE(s.total_tasks_executed, 1u);
}

// ============================================================================
// Multiple workers process tasks concurrently
// ============================================================================
TEST(RuntimeTest, MultipleWorkersConcurrent) {
    OnlineWorkerGroup::Config cfg;
    cfg.num_workers = 4;
    OnlineWorkerGroup group(cfg);

    static std::atomic<int> s_count{0};
    s_count = 0;

    // Submit many tasks distributed across workers
    for (int i = 0; i < 100; ++i) {
        group.submit_to(i,
            WorkItem::make_func(+[]() { s_count.fetch_add(1); }));
    }

    group.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    group.stop();

    EXPECT_EQ(s_count.load(), 100);
}

// ============================================================================
// RuntimeConfig verification
// ============================================================================
TEST(RuntimeTest, RuntimeConfigValues) {
    RuntimeConfig cfg;
    EXPECT_EQ(cfg.online_cfg.num_workers, 4u);
    EXPECT_EQ(cfg.offline_cfg.num_workers, 2u);

    RuntimeConfig custom_cfg;
    custom_cfg.online_cfg.num_workers = 8;
    EXPECT_EQ(custom_cfg.online_cfg.num_workers, 8u);
}
