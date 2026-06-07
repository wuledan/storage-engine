#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>
#include <mutex>
#include <vector>
#include "runtime/scheduler.h"
#include "runtime/strict_priority_policy.h"
#include "runtime/adaptive_idle.h"
#include "runtime/batched_spsc_work_queue.h"
#include "runtime/affine_work_queue.h"

using namespace storage::runtime;

// ============================================================================
// Helpers: thread-safe counters for function-pointer WorkItems
// ============================================================================
static std::atomic<int> g_counter{0};
static std::vector<int> g_order;
static std::mutex g_order_mutex;

static void reset_globals() {
    g_counter = 0;
    g_order.clear();
}

// ============================================================================
// Register queue and poll — scheduler processes enqueued items
// ============================================================================
TEST(SchedulerTest, RegisterAndPoll) {
    reset_globals();

    AdaptiveIdle idle;
    StrictPriorityPolicy policy(64);
    Scheduler scheduler(&policy, &idle);

    {
        auto queue = std::make_unique<BatchedSPSCWorkQueue>(
            QueueType::kEngine, Priority::kCritical, "test");

        WorkItem items[3] = {
            WorkItem::make_func(+[]() { g_counter.fetch_add(1); }),
            WorkItem::make_func(+[]() { g_counter.fetch_add(10); }),
            WorkItem::make_func(+[]() { g_counter.fetch_add(100); }),
        };
        queue->push_batch(items, 3);
        scheduler.register_queue(std::move(queue));
    }

    std::thread t([&]() { scheduler.run(); });

    // Give scheduler time to process all items
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    scheduler.request_stop();
    t.join();

    EXPECT_EQ(scheduler.stats().total_tasks_executed, 3u);
    EXPECT_EQ(g_counter.load(), 111);
}

// ============================================================================
// Multi-queue priority scheduling: P0 items processed before P1
// ============================================================================
TEST(SchedulerTest, MultiQueuePriorityOrder) {
    reset_globals();

    AdaptiveIdle idle;
    StrictPriorityPolicy policy(64);
    Scheduler scheduler(&policy, &idle);

    // P1 queue (lower priority, registered first)
    {
        auto low_q = std::make_unique<BatchedSPSCWorkQueue>(
            QueueType::kDiskIO, Priority::kHigh, "low");

        WorkItem low_items[2] = {
            WorkItem::make_func(+[]() {
                std::lock_guard<std::mutex> l(g_order_mutex);
                g_order.push_back(1);
            }),
            WorkItem::make_func(+[]() {
                std::lock_guard<std::mutex> l(g_order_mutex);
                g_order.push_back(2);
            }),
        };
        low_q->push_batch(low_items, 2);
        scheduler.register_queue(std::move(low_q));
    }

    // P0 queue (higher priority, registered second)
    {
        auto high_q = std::make_unique<BatchedSPSCWorkQueue>(
            QueueType::kNetIO, Priority::kCritical, "high");

        WorkItem high_items[2] = {
            WorkItem::make_func(+[]() {
                std::lock_guard<std::mutex> l(g_order_mutex);
                g_order.push_back(10);
            }),
            WorkItem::make_func(+[]() {
                std::lock_guard<std::mutex> l(g_order_mutex);
                g_order.push_back(20);
            }),
        };
        high_q->push_batch(high_items, 2);
        scheduler.register_queue(std::move(high_q));
    }

    std::thread t([&]() { scheduler.run(); });

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    scheduler.request_stop();
    t.join();

    // Both queues should have been fully processed
    ASSERT_EQ(g_order.size(), 4u);

    // StrictPriorityPolicy always picks P0 (index 1) over P1 (index 0)
    // when both have items.  So P0 items (10, 20) come first.
    EXPECT_EQ(g_order[0], 10);
    EXPECT_EQ(g_order[1], 20)
        << "P0 items should be dequeued before P1 items";
}

// ============================================================================
// Stop exits the run loop
// ============================================================================
TEST(SchedulerTest, StopExitsLoop) {
    AdaptiveIdle idle;
    StrictPriorityPolicy policy(64);
    Scheduler scheduler(&policy, &idle);

    auto queue = std::make_unique<BatchedSPSCWorkQueue>(
        QueueType::kEngine, Priority::kCritical, "q");
    scheduler.register_queue(std::move(queue));

    std::thread t([&]() { scheduler.run(); });

    // Give it a moment then stop
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    scheduler.request_stop();
    t.join();

    // run() must have returned → stats reflect at least one poll attempt
    EXPECT_GE(scheduler.stats().total_polls, 1u);
}

// ============================================================================
// Idle notify wakeup: scheduler with empty queue
// ============================================================================
TEST(SchedulerTest, IdleWakeup) {
    AdaptiveIdle idle;
    StrictPriorityPolicy policy(64);
    Scheduler scheduler(&policy, &idle);

    // Register an empty queue → scheduler will immediately idle
    auto queue = std::make_unique<BatchedSPSCWorkQueue>(
        QueueType::kTimer, Priority::kMedium, "idle_q");
    scheduler.register_queue(std::move(queue));

    std::thread t([&]() { scheduler.run(); });

    // Wait long enough for the scheduler to reach idle-park state
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // request_stop triggers notify internally, waking the scheduler
    scheduler.request_stop();
    t.join();

    // The scheduler did at least one poll cycle and one idle cycle
    EXPECT_GE(scheduler.stats().total_polls, 1u);
    EXPECT_GE(scheduler.stats().total_idles, 1u);
}

// ============================================================================
// Multi-queue with mixed empty/non-empty
// ============================================================================
TEST(SchedulerTest, MixedQueues) {
    reset_globals();

    AdaptiveIdle idle;
    StrictPriorityPolicy policy(64);
    Scheduler scheduler(&policy, &idle);

    // Empty P0 queue
    {
        auto q = std::make_unique<BatchedSPSCWorkQueue>(
            QueueType::kEngine, Priority::kCritical, "p0_empty");
        scheduler.register_queue(std::move(q));
    }

    // Non-empty P1 queue
    {
        auto q = std::make_unique<BatchedSPSCWorkQueue>(
            QueueType::kEngine, Priority::kHigh, "p1_nonempty");
        WorkItem items[2] = {
            WorkItem::make_func(+[]() { g_counter.fetch_add(1); }),
            WorkItem::make_func(+[]() { g_counter.fetch_add(2); }),
        };
        q->push_batch(items, 2);
        scheduler.register_queue(std::move(q));
    }

    std::thread t([&]() { scheduler.run(); });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    scheduler.request_stop();
    t.join();

    // P1 items should still be processed despite P0 being empty
    EXPECT_EQ(scheduler.stats().total_tasks_executed, 2u);
    EXPECT_EQ(g_counter.load(), 3);
}

// ============================================================================
// BatchedSPSCWorkQueue with scheduler (approx_count reports correctly)
// ============================================================================
TEST(SchedulerTest, BatchedQueueWork) {
    reset_globals();

    AdaptiveIdle idle;
    StrictPriorityPolicy policy(64);
    Scheduler scheduler(&policy, &idle);

    auto queue = std::make_unique<BatchedSPSCWorkQueue>(
        QueueType::kEngine, Priority::kCritical, "batched");
    WorkItem items[5];
    for (int i = 0; i < 5; ++i) {
        items[i] = WorkItem::make_func(+[]() { g_counter.fetch_add(1); });
    }
    queue->push_batch(items, 5);
    scheduler.register_queue(std::move(queue));

    std::thread t([&]() { scheduler.run(); });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    scheduler.request_stop();
    t.join();

    EXPECT_EQ(scheduler.stats().total_tasks_executed, 5u);
    EXPECT_EQ(g_counter.load(), 5);
}
