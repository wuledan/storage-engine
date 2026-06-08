#pragma once
#include "capacity_estimator.h"
#include "dispatch_types.h"
#include <optional>

namespace storage::runtime {

// ── 策略决策器 ──
//
// 根据实时容量评估和当前配置，自动选择最优的 dispatch 策略。
struct StrategyDeciderConfig {
    double utilization_high{0.85};
    double utilization_low{0.30};
    double latency_p99_degrade{2.0};
    double hysteresis{0.1};
};

class StrategyDecider {
public:
    using Config = StrategyDeciderConfig;

    explicit StrategyDecider(const Config& cfg = Config{}) : cfg_(cfg) {}

    // 返回新的 plan，nullopt = 无需调整
    std::optional<DispatchPlan> decide(
        const RealTimeCapacity& cap,
        const DispatchPlan& current,
        const HardwareTopology& hw);

private:
    Config cfg_;
    double baseline_latency_us_{0};  // 首次测量的基线延迟
};

inline std::optional<DispatchPlan> StrategyDecider::decide(
    const RealTimeCapacity& cap,
    const DispatchPlan& current,
    const HardwareTopology& hw) {

    // 初始化基线（首次调用）
    if (baseline_latency_us_ == 0 && cap.avg_latency_us > 0) {
        baseline_latency_us_ = cap.avg_latency_us;
    }

    // 健康态：有余量，不调整
    if (cap.utilization > cfg_.utilization_low + cfg_.hysteresis
        && cap.utilization < cfg_.utilization_high - cfg_.hysteresis
        && cap.avg_latency_us < baseline_latency_us_ * cfg_.latency_p99_degrade) {
        return std::nullopt;
    }

    // 过载 → 尝试 DirectAll（减少跨线程开销）
    if (cap.utilization > cfg_.utilization_high) {
        if (hw.nic_queues >= current.direct_workers + current.dispatch_threads + 1) {
            DispatchPlan plan;
            plan.strategy = DispatchStrategy::kDirectAll;
            plan.direct_workers = hw.nic_queues;
            plan.dispatch_threads = 0;
            plan.consumers_per_dispatch = 1;
            return plan;
        }
        // nic_queues 不够 DirectAll，保持当前策略但报警
        return std::nullopt;
    }

    // 低载 → 尝试 DispatchSingle（节省 polling 开销）
    if (cap.utilization < cfg_.utilization_low) {
        DispatchPlan plan;
        plan.strategy = DispatchStrategy::kDispatchSingle;
        plan.direct_workers = 0;
        plan.dispatch_threads = 1;
        plan.consumers_per_dispatch = hw.nic_queues;
        return plan;
    }

    // 延迟异常且利用率正常 → 切换到 DirectAll
    if (baseline_latency_us_ > 0
        && cap.avg_latency_us > baseline_latency_us_ * cfg_.latency_p99_degrade) {
        DispatchPlan plan;
        plan.strategy = DispatchStrategy::kDirectAll;
        plan.direct_workers = hw.nic_queues;
        plan.dispatch_threads = 0;
        plan.consumers_per_dispatch = 1;
        return plan;
    }

    return std::nullopt;
}

}  // namespace storage::runtime
