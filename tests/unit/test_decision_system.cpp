#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include "runtime/dispatch_types.h"
#include "runtime/metrics_collector.h"
#include "runtime/capacity_estimator.h"
#include "runtime/strategy_decider.h"

using namespace storage::runtime;

// ============================================================================
// 测试1: DispatchPlan 相等比较
// ============================================================================
TEST(DecisionSystemTest, DispatchPlanEquality) {
    DispatchPlan a{DispatchStrategy::kDirectAll, 4, 0, 0};
    DispatchPlan b{DispatchStrategy::kDirectAll, 4, 0, 0};
    DispatchPlan c{DispatchStrategy::kDispatchSingle, 0, 1, 4};
    EXPECT_EQ(a, b);
    EXPECT_NE(a, c);
}

// ============================================================================
// 测试2: BottleneckAnalysis 计算
// ============================================================================
TEST(DecisionSystemTest, BottleneckIdentification) {
    auto ba = BottleneckAnalysis::compute(1000, 2000, 3000);
    EXPECT_EQ(ba.bottleneck, BottleneckAnalysis::kNet);

    auto bb = BottleneckAnalysis::compute(2000, 1000, 3000);
    EXPECT_EQ(bb.bottleneck, BottleneckAnalysis::kEngine);

    auto bc = BottleneckAnalysis::compute(2000, 2000, 2000);
    EXPECT_EQ(bc.bottleneck, BottleneckAnalysis::kBalanced);
}

// ============================================================================
// 测试3: MetricsCollector 基本采集
// ============================================================================
TEST(DecisionSystemTest, MetricsCollectorBasic) {
    WorkerPerf perf(0);
    perf.set_level(PerfLevel::kCount);
    perf.record_enqueue(QueueType::kEngine, 0);
    perf.record_enqueue(QueueType::kEngine, 0);

    MetricsCollector mc;
    mc.attach_worker(0, &perf);

    // 第一次采集为 baseline
    auto metrics = mc.collect();
    EXPECT_EQ(metrics.elapsed_seconds, 0.0);

    // 第二次采集应有 delta
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    auto metrics2 = mc.collect();
    EXPECT_GT(metrics2.elapsed_seconds, 0.0);
}

// ============================================================================
// 测试4: CapacityEstimator
// ============================================================================
TEST(DecisionSystemTest, CapacityEstimator) {
    RuntimeMetrics m;
    m.elapsed_seconds = 1.0;
    m.total_enqueued_delta = 10000;
    m.total_executed_delta = 8000;

    auto cap = RealTimeCapacity::estimate(m, 1);
    EXPECT_NEAR(cap.arrival_rate, 10000.0, 1.0);
    EXPECT_NEAR(cap.service_rate_per_core, 8000.0, 1.0);
}

// ============================================================================
// 测试5: StrategyDecider 过载触发 DirectAll
// ============================================================================
TEST(DecisionSystemTest, OverloadTriggersDirectAll) {
    StrategyDecider decider;
    HardwareTopology hw;
    hw.nic_queues = 4;
    hw.cpu_cores = 4;

    RealTimeCapacity cap;
    cap.utilization = 0.90; // 过载
    cap.avg_latency_us = 5.0;

    DispatchPlan current{DispatchStrategy::kDispatchSingle, 0, 1, 4};
    auto plan = decider.decide(cap, current, hw);
    EXPECT_TRUE(plan.has_value());
    EXPECT_EQ(plan->strategy, DispatchStrategy::kDirectAll);
}

// ============================================================================
// 测试6: StrategyDecider 健康态不调整
// ============================================================================
TEST(DecisionSystemTest, HealthyNoChange) {
    StrategyDecider decider;
    HardwareTopology hw;
    hw.nic_queues = 4;

    RealTimeCapacity cap;
    cap.utilization = 0.60;  // 正常
    cap.avg_latency_us = 1.5;

    DispatchPlan current{DispatchStrategy::kDirectAll, 4, 0, 0};
    auto plan = decider.decide(cap, current, hw);
    EXPECT_FALSE(plan.has_value());
}

// ============================================================================
// 测试7: 延迟异常触发策略切换
// ============================================================================
TEST(DecisionSystemTest, LatencySpikeTriggersChange) {
    StrategyDecider::Config cfg;
    cfg.latency_p99_degrade = 2.0;
    StrategyDecider decider(cfg);
    HardwareTopology hw;
    hw.nic_queues = 4;

    // 先建立基线
    RealTimeCapacity baseline;
    baseline.utilization = 0.5;
    baseline.avg_latency_us = 1.0;
    DispatchPlan current;
    decider.decide(baseline, current, hw);

    // 延迟飙升
    RealTimeCapacity cap;
    cap.utilization = 0.65;
    cap.avg_latency_us = 5.0;  // 5x baseline
    auto plan = decider.decide(cap, current, hw);
    EXPECT_TRUE(plan.has_value());
}

// ============================================================================
// 测试8: HardwareTopology.probe() 基本结构
// ============================================================================
TEST(DecisionSystemTest, HardwareTopologyProbe) {
    auto hw = HardwareTopology::probe();
    EXPECT_GT(hw.cpu_cores, 0);
    EXPECT_GT(hw.nic_queues, 0);
    EXPECT_GT(hw.nic_bandwidth_gbps, 0);
}
