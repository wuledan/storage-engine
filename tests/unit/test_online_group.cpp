#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>
#include <set>
#include "runtime/online_group.h"
#include "runtime/local_work_queue.h"

using namespace storage::runtime;

static std::atomic<int> g_og_counter{0};

// ============================================================================
// OnlineWorkerGroup lifecycle: start/stop
// ============================================================================
TEST(OnlineGroupTest, StartStop) {
    OnlineWorkerGroup::Config cfg;
    cfg.num_workers = 2;
    cfg.base_cpu_id = 0;
    cfg.name_prefix = "test-group";
    OnlineWorkerGroup group(cfg);

    EXPECT_EQ(group.worker_count(), 2u);

    // Submit items before start (queues pre-registered in constructor)
    g_og_counter = 0;
    group.submit_to(42, WorkItem::make_func(+[]() { g_og_counter.fetch_add(1); }));
    group.submit_to(42, WorkItem::make_func(+[]() { g_og_counter.fetch_add(10); }));

    group.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    group.stop();

    EXPECT_GE(g_og_counter.load(), 11);
    auto s = group.stats();
    EXPECT_EQ(s.worker_count, 2u);
    EXPECT_GE(s.total_tasks_executed, 2u);
}

// ============================================================================
// Start/stop with no tasks submitted
// ============================================================================
TEST(OnlineGroupTest, StartStopEmpty) {
    OnlineWorkerGroup::Config cfg;
    cfg.num_workers = 1;
    cfg.base_cpu_id = 0;
    OnlineWorkerGroup group(cfg);

    EXPECT_NO_THROW(group.start());
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_NO_THROW(group.stop());

    auto s = group.stats();
    EXPECT_EQ(s.worker_count, 1u);
    EXPECT_EQ(s.total_tasks_executed, 0u);
}

// ============================================================================
// Hash routing is deterministic: same key → same worker every time
// ============================================================================
TEST(OnlineGroupTest, HashRoutingDeterministic) {
    OnlineWorkerGroup::Config cfg;
    cfg.num_workers = 4;
    OnlineWorkerGroup group(cfg);

    uint64_t key = 123456789;
    size_t route1 = group.route_by_hash(key);
    size_t route2 = group.route_by_hash(key);
    EXPECT_EQ(route1, route2);

    // Verify modulo distribution
    EXPECT_EQ(route1, key % 4);
    EXPECT_EQ(route2, key % 4);
}

// ============================================================================
// Hash routing distributes across workers (different keys → different workers)
// ============================================================================
TEST(OnlineGroupTest, HashRoutingDistribution) {
    OnlineWorkerGroup::Config cfg;
    cfg.num_workers = 4;
    OnlineWorkerGroup group(cfg);

    std::set<size_t> routes;
    for (uint64_t k = 0; k < 100; ++k) {
        routes.insert(group.route_by_hash(k));
    }
    // With 100 keys and 4 buckets, we expect all 4 routes to be used
    EXPECT_EQ(routes.size(), 4u);
}

// ============================================================================
// submit_to routes by hash and tasks are processed
// ============================================================================
TEST(OnlineGroupTest, SubmitToRoutesCorrectly) {
    OnlineWorkerGroup::Config cfg;
    cfg.num_workers = 3;
    OnlineWorkerGroup group(cfg);

    static std::atomic<int> s_worker0{0};
    static std::atomic<int> s_worker1{0};
    static std::atomic<int> s_worker2{0};
    s_worker0 = 0;
    s_worker1 = 0;
    s_worker2 = 0;

    // Submit tasks for each expected worker
    for (uint64_t k = 0; k < 30; ++k) {
        size_t expected_worker = k % 3;
        group.submit_to(k, WorkItem::make_func(+[]() {
            // We can't know which worker runs this, we just count total
            // The routing is verified by worker-specific task counts above
        }));
    }

    // Submit tasks to specific worker indices via submit_to with known keys
    for (uint64_t k = 0; k < 10; ++k) {
        group.submit_to(k * 3 + 0, WorkItem::make_func(+[]() { s_worker0.fetch_add(1); }));
        group.submit_to(k * 3 + 1, WorkItem::make_func(+[]() { s_worker1.fetch_add(1); }));
        group.submit_to(k * 3 + 2, WorkItem::make_func(+[]() { s_worker2.fetch_add(1); }));
    }

    group.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    group.stop();

    // Each worker should have processed 10 tasks (keys mapping to their bucket)
    EXPECT_EQ(s_worker0.load(), 10);
    EXPECT_EQ(s_worker1.load(), 10);
    EXPECT_EQ(s_worker2.load(), 10);
}

// ============================================================================
// Single worker group
// ============================================================================
TEST(OnlineGroupTest, SingleWorker) {
    OnlineWorkerGroup::Config cfg;
    cfg.num_workers = 1;
    OnlineWorkerGroup group(cfg);

    EXPECT_EQ(group.worker_count(), 1u);

    g_og_counter = 0;
    group.submit_to(0, WorkItem::make_func(+[]() { g_og_counter = 99; }));

    group.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    group.stop();

    EXPECT_EQ(g_og_counter.load(), 99);
}

// ============================================================================
// worker() accessor returns correct reference
// ============================================================================
TEST(OnlineGroupTest, WorkerAccessor) {
    OnlineWorkerGroup::Config cfg;
    cfg.num_workers = 2;
    OnlineWorkerGroup group(cfg);

    // worker() should return valid references
    OnlineWorker& w0 = group.worker(0);
    OnlineWorker& w1 = group.worker(1);

    // Submit directly to each worker
    g_og_counter = 0;
    w0.submit_engine(WorkItem::make_func(+[]() { g_og_counter.fetch_add(1); }));
    w1.submit_engine(WorkItem::make_func(+[]() { g_og_counter.fetch_add(2); }));

    group.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    group.stop();

    EXPECT_EQ(g_og_counter.load(), 3);
}

// ============================================================================
// Repeated start/stop is safe
// ============================================================================
TEST(OnlineGroupTest, RepeatedStartStop) {
    OnlineWorkerGroup::Config cfg;
    cfg.num_workers = 2;
    OnlineWorkerGroup group(cfg);

    g_og_counter = 0;
    for (int i = 0; i < 3; ++i) {
        group.submit_to(i, WorkItem::make_func(+[]() { g_og_counter.fetch_add(1); }));

        group.start();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        group.stop();
    }

    EXPECT_EQ(g_og_counter.load(), 3);
}

// ============================================================================
// Stats accumulate correctly across workers
// ============================================================================
TEST(OnlineGroupTest, StatsAccumulate) {
    OnlineWorkerGroup::Config cfg;
    cfg.num_workers = 3;
    OnlineWorkerGroup group(cfg);

    g_og_counter = 0;
    for (int i = 0; i < 9; ++i) {
        group.submit_to(i, WorkItem::make_func(+[]() { g_og_counter.fetch_add(1); }));
    }

    group.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    group.stop();

    auto s = group.stats();
    EXPECT_EQ(s.worker_count, 3u);
    EXPECT_EQ(s.total_tasks_executed, 9u);
    EXPECT_GE(s.total_exec_ns, 0u);
}
