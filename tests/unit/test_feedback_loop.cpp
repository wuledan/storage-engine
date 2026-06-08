#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include "runtime/feedback_loop.h"
#include "runtime/online_group.h"
#include "runtime/online_worker.h"

using namespace storage::runtime;

// ============================================================================
// 测试1: FeedbackLoop 基本生命周期
// ============================================================================
TEST(FeedbackLoopTest, BasicLifecycle) {
    MetricsCollector collector;
    StrategyDecider decider;
    StrategyExecutor executor;

    FeedbackLoop loop(collector, decider, executor);
    EXPECT_EQ(loop.tick_count(), 0);

    // 没有绑定 workers，tick 不会执行策略变更
    bool changed = loop.tick();
    EXPECT_FALSE(changed);
    EXPECT_EQ(loop.tick_count(), 1);
}

// ============================================================================
// 测试2: 绑定 workers + tick
// ============================================================================
TEST(FeedbackLoopTest, BindWorkersAndTick) {
    WorkerPerf perf(0);
    perf.set_level(PerfLevel::kCount);

    MetricsCollector collector;
    collector.attach_worker(0, &perf);

    StrategyDecider decider;
    StrategyExecutor executor;

    FeedbackLoop loop(collector, decider, executor);

    // 模拟一些计数
    perf.record_enqueue(QueueType::kEngine, 0);
    perf.record_enqueue(QueueType::kEngine, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // 第一次 tick 为 baseline
    loop.tick();
    // 第二次 tick 有 delta
    bool changed = loop.tick();
    // 正常状态 → 无变更
    EXPECT_EQ(loop.tick_count(), 2);
    EXPECT_FALSE(changed);
}

// ============================================================================
// 测试3: 防抖动（连续N次同结论才执行）
// ============================================================================
TEST(FeedbackLoopTest, AntiFlapping) {
    WorkerPerf perf(0);
    perf.set_level(PerfLevel::kCount);
    MetricsCollector collector;
    collector.attach_worker(0, &perf);
    StrategyDecider decider;
    StrategyExecutor executor;
    FeedbackLoop::Config cfg;
    cfg.consecutive_samples = 3;
    FeedbackLoop loop(collector, decider, executor, cfg);

    // 单次 tick 不应触发放切换
    for (int i = 0; i < 2; ++i) {
        loop.tick();
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    // 需要 consecutive_samples(3) 次相同结论才切换
    EXPECT_EQ(loop.switch_count(), 0);
}

// ============================================================================
// 测试4: 决策历史
// ============================================================================
TEST(FeedbackLoopTest, DecisionHistory) {
    StrategyExecutor executor;
    DispatchPlan from{DispatchStrategy::kDispatchSingle, 0, 1, 4};
    DispatchPlan to{DispatchStrategy::kDirectAll, 4, 0, 0};
    RealTimeCapacity cap;
    executor.record_decision(from, to, cap, "test_reason");

    EXPECT_EQ(executor.decision_count(), 1);
    const auto& h = executor.history();
    EXPECT_EQ(h.size(), 1);
    EXPECT_EQ(h[0].from.strategy, DispatchStrategy::kDispatchSingle);
    EXPECT_EQ(h[0].to.strategy, DispatchStrategy::kDirectAll);
}
