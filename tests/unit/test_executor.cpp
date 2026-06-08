#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>
#include "runtime/strategy_executor.h"
#include "runtime/dispatch_poller.h"
#include "runtime/online_worker.h"
#include "runtime/affine_work_queue.h"
#include "runtime/local_work_queue.h"

using namespace storage::runtime;

// ============================================================================
// 测试1: 记录决策历史
// ============================================================================
TEST(ExecutorTest, RecordDecision) {
    StrategyExecutor exec;
    DispatchPlan from{DispatchStrategy::kDispatchSingle, 0, 1, 4};
    DispatchPlan to{DispatchStrategy::kDirectAll, 4, 0, 0};
    RealTimeCapacity cap;
    cap.utilization = 0.85;
    cap.avg_latency_us = 4.0;

    exec.record_decision(from, to, cap, "overload");
    EXPECT_EQ(exec.decision_count(), 1);
    EXPECT_EQ(exec.history().size(), 1);
    EXPECT_EQ(exec.history()[0].reason, "overload");
}

// ============================================================================
// 测试2: OnlineWorker 热切换（swap_engine_queue）
// ============================================================================
TEST(ExecutorTest, QueueHotSwap) {
    Worker::Config cfg;
    OnlineWorker w(cfg);

    // 默认是 BatchedSPSC (kSPSC)
    auto* old_q = w.get_queue(w.idx_engine_);
    ASSERT_NE(old_q, nullptr);
    EXPECT_EQ(old_q->semantic(), QueueSemantic::kSPSC);

    // 切换到 LocalWorkQueue (kDirect)
    auto new_q = std::make_unique<LocalWorkQueue>(
        QueueType::kEngine, Priority::kMedium, "engine-swapped", 256);
    bool ok = w.swap_engine_queue(std::move(new_q));
    EXPECT_TRUE(ok);

    // 验证队列已替换
    auto* swapped_q = w.get_queue(w.idx_engine_);
    ASSERT_NE(swapped_q, nullptr);
    EXPECT_EQ(swapped_q->semantic(), QueueSemantic::kLocal);
}

// 全局 counter (WorkItem::Func 是 void(*)()，不能捕获 lambda)
static std::atomic<int> g_exec_count{0};
static void inc_exec_count() { g_exec_count.fetch_add(1); }

// ============================================================================
// 测试3: submit_engine 在两种模式下均可用
// ============================================================================
TEST(ExecutorTest, SubmitEngineBothModes) {
    // Indirect 模式（默认 BatchedSPSCWorkQueue）
    {
        Worker::Config cfg;
        OnlineWorker w(cfg);
        w.start();
        g_exec_count = 0;

        for (int i = 0; i < 10; ++i) {
            w.submit_engine(WorkItem::make_func(inc_exec_count));
        }
        w.notify();

        // 等待任务执行
        for (int i = 0; i < 50 && g_exec_count.load() < 10; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
            w.notify();
        }
        EXPECT_GE(g_exec_count.load(), 10);
        w.stop();
        w.join();
    }

    // Direct 模式（LocalWorkQueue，start 前 swap）
    {
        Worker::Config cfg;
        OnlineWorker w(cfg);
        w.swap_engine_queue(std::make_unique<LocalWorkQueue>(
            QueueType::kEngine, Priority::kMedium, "engine-direct", 256));
        g_exec_count = 0;
        w.start();

        for (int i = 0; i < 10; ++i) {
            w.submit_engine(WorkItem::make_func(inc_exec_count));
        }
        w.notify();

        for (int i = 0; i < 50 && g_exec_count.load() < 10; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
            w.notify();
        }
        EXPECT_GE(g_exec_count.load(), 10);
        w.stop();
        w.join();
    }
}

// ============================================================================
// 测试4: DispatchPoller 生命周期
// ============================================================================
TEST(ExecutorTest, DispatchPollerLifecycle) {
    DispatchPoller::Config cfg;
    cfg.poller_id = 1;
    // DispatchFn 是 std::function，可以捕获
    std::atomic<size_t> called{0};
    auto fn = [&called](uint64_t /*key*/, WorkItem /*item*/) { called++; };

    DispatchPoller poller(cfg, std::move(fn));
    poller.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(35));
    poller.stop();
    EXPECT_GT(poller.packets_received(), 0);
}
