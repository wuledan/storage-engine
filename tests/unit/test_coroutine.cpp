#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>
#include "runtime/scheduler.h"
#include "runtime/strict_priority_policy.h"
#include "runtime/adaptive_idle.h"
#include "runtime/batched_spsc_work_queue.h"
#include "runtime/local_work_queue.h"

using namespace storage::runtime;

static std::atomic<int> g_coro_count{0};

// ============================================================================
// 测试：Scheduler run() 可作为协程启动（仅编译期验证 Task 类型）
// ============================================================================
TEST(CoroutineTest, SchedulerRunAsCoroutine) {
    AdaptiveIdle idle;
    StrictPriorityPolicy policy;
    Scheduler scheduler(&policy, &idle);
    scheduler.register_queue(std::make_unique<LocalWorkQueue>(
        QueueType::kEngine, Priority::kMedium, "test"));

    SUCCEED();
}

// ============================================================================
// 测试：blockingWait 调度循环可正常退出
// ============================================================================
TEST(CoroutineTest, BlockingWaitAndStop) {
    AdaptiveIdle idle;
    StrictPriorityPolicy policy;
    Scheduler scheduler(&policy, &idle);
    scheduler.register_queue(std::make_unique<LocalWorkQueue>(
        QueueType::kEngine, Priority::kMedium, "test"));

    std::thread t([&]() { scheduler.run(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    scheduler.request_stop();
    t.join();
    SUCCEED();
}

// ============================================================================
// 测试：任务在协程环境中执行
// ============================================================================
TEST(CoroutineTest, TasksExecuteInCoroutine) {
    g_coro_count = 0;
    AdaptiveIdle idle;
    StrictPriorityPolicy policy;
    Scheduler scheduler(&policy, &idle);

    auto q = std::make_unique<BatchedSPSCWorkQueue>(
        QueueType::kEngine, Priority::kCritical, "test");
    auto* raw_q = q.get();
    scheduler.register_queue(std::move(q));

    // 预先投递 3 个任务
    WorkItem items[3] = {
        WorkItem::make_func(+[]() { g_coro_count.fetch_add(1); }),
        WorkItem::make_func(+[]() { g_coro_count.fetch_add(10); }),
        WorkItem::make_func(+[]() { g_coro_count.fetch_add(100); }),
    };
    raw_q->push_batch(items, 3);

    std::thread t([&]() { scheduler.run(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    scheduler.request_stop();
    t.join();

    EXPECT_EQ(g_coro_count.load(), 111);
    EXPECT_EQ(scheduler.stats().total_tasks_executed, 3u);
}
