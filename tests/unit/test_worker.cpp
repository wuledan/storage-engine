#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>
#include "runtime/worker.h"
#include "runtime/batched_spsc_work_queue.h"

using namespace storage::runtime;

static std::atomic<int> g_w_counter{0};

static void reset_counter() { g_w_counter = 0; }

// ============================================================================
// Worker lifecycle: create, start, stop, join
// ============================================================================
TEST(WorkerTest, Lifecycle) {
    reset_counter();
    Worker::Config cfg;
    cfg.cpu_id = 0;  // no pinning
    Worker worker(cfg);

    // Register a queue with some items
    auto q = std::make_unique<BatchedSPSCWorkQueue>(
        QueueType::kEngine, Priority::kCritical, "test_q");
    WorkItem items[3] = {
        WorkItem::make_func(+[]() { g_w_counter.fetch_add(1); }),
        WorkItem::make_func(+[]() { g_w_counter.fetch_add(10); }),
        WorkItem::make_func(+[]() { g_w_counter.fetch_add(100); }),
    };
    q->push_batch(items, 3);
    worker.add_queue(std::move(q));

    worker.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    worker.stop();
    worker.join();

    auto s = worker.stats();
    EXPECT_EQ(s.tasks_executed, 3u);
    EXPECT_EQ(g_w_counter.load(), 111);
}

// ============================================================================
// Add queue before start
// ============================================================================
TEST(WorkerTest, AddQueueBeforeStart) {
    reset_counter();
    Worker::Config cfg;
    Worker worker(cfg);

    // Add a queue before starting
    auto q = std::make_unique<BatchedSPSCWorkQueue>(
        QueueType::kEngine, Priority::kCritical, "pre_q");
    WorkItem items[2] = {
        WorkItem::make_func(+[]() { g_w_counter.fetch_add(1); }),
        WorkItem::make_func(+[]() { g_w_counter.fetch_add(2); }),
    };
    q->push_batch(items, 2);
    worker.add_queue(std::move(q));

    // Add another queue after
    auto q2 = std::make_unique<BatchedSPSCWorkQueue>(
        QueueType::kTimer, Priority::kHigh, "pre_q2");
    WorkItem items2[1] = {
        WorkItem::make_func(+[]() { g_w_counter.fetch_add(4); }),
    };
    q2->push_batch(items2, 1);
    worker.add_queue(std::move(q2));

    worker.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    worker.stop();
    worker.join();

    EXPECT_EQ(g_w_counter.load(), 1 + 2 + 4);
}

// ============================================================================
// Notify wakes idle worker
// ============================================================================
TEST(WorkerTest, NotifyWakesIdle) {
    Worker::Config cfg;
    Worker worker(cfg);

    // Start with an empty queue — worker will idle
    worker.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Worker is now idle (parked). Notify should wake it, but it will
    // just go idle again since there's no work. Then request_stop.
    worker.notify();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    worker.stop();
    worker.join();

    auto s = worker.stats();
    EXPECT_GE(s.total_polls, 1u);
    EXPECT_GE(s.total_idles, 1u);
}

// ============================================================================
// Stats accumulate correctly
// ============================================================================
TEST(WorkerTest, StatsAccumulate) {
    reset_counter();
    Worker::Config cfg;
    Worker worker(cfg);

    auto q = std::make_unique<BatchedSPSCWorkQueue>(
        QueueType::kEngine, Priority::kCritical, "stats_q");
    WorkItem items[5];
    for (int i = 0; i < 5; ++i) {
        items[i] = WorkItem::make_func(+[]() { g_w_counter.fetch_add(1); });
    }
    q->push_batch(items, 5);
    worker.add_queue(std::move(q));

    worker.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    worker.stop();
    worker.join();

    auto s = worker.stats();
    EXPECT_EQ(s.tasks_executed, 5u);
    EXPECT_GE(s.total_exec_ns, 0u);
    EXPECT_GE(s.total_polls, 1u);
    EXPECT_EQ(g_w_counter.load(), 5);
}

// ============================================================================
// Multiple workers operate independently
// ============================================================================
TEST(WorkerTest, IndependentWorkers) {
    reset_counter();
    Worker::Config cfg1, cfg2;
    Worker w1(cfg1);
    Worker w2(cfg2);

    auto make_q = [](std::string name, int base) {
        auto q = std::make_unique<BatchedSPSCWorkQueue>(
            QueueType::kEngine, Priority::kCritical, std::move(name));
        WorkItem items[3] = {
            WorkItem::make_func(+[]() { g_w_counter.fetch_add(1); }),
            WorkItem::make_func(+[]() { g_w_counter.fetch_add(1); }),
            WorkItem::make_func(+[]() { g_w_counter.fetch_add(1); }),
        };
        q->push_batch(items, 3);
        return q;
    };

    w1.add_queue(make_q("w1", 0));
    w2.add_queue(make_q("w2", 0));

    w1.start();
    w2.start();

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    w1.stop();
    w1.join();
    w2.stop();
    w2.join();

    EXPECT_EQ(w1.stats().tasks_executed, 3u);
    EXPECT_EQ(w2.stats().tasks_executed, 3u);
    EXPECT_EQ(g_w_counter.load(), 6);
}
