#pragma once
#include "dispatch_types.h"
#include "metrics_collector.h"
#include "capacity_estimator.h"
#include "strategy_decider.h"
#include "strategy_executor.h"
#include <optional>
#include <cstdint>

namespace storage::runtime {

class OnlineWorkerGroup;

// ── FeedbackLoop 配置 ──
struct FeedbackLoopConfig {
    uint64_t tick_interval_ms{5000};     // 决策间隔
    size_t consecutive_samples{3};        // 连续 N 次同结论才执行
};

// ── FeedbackLoop ──
//
// 闭环控制器：周期性采集指标 → 容量评估 → 策略决策 → 执行变更。
// 内置防抖动机制，连续 N 次同结论才执行切换。
class FeedbackLoop {
public:
    using Config = FeedbackLoopConfig;

    FeedbackLoop(MetricsCollector& collector,
                 StrategyDecider& decider,
                 StrategyExecutor& executor,
                 const Config& cfg = Config{});

    // 绑定 Worker 组（用于执行策略变更）
    void bind_workers(OnlineWorkerGroup* workers);

    // 单次 tick（由 timer 或 scheduler 周期性调用）
    // 返回 true 表示执行了策略切换
    bool tick();

    // 查询
    const DispatchPlan& current_plan() const { return current_plan_; }
    size_t tick_count() const { return tick_count_; }
    size_t switch_count() const { return executor_.decision_count(); }

private:
    // 执行策略变更
    bool apply_plan(const DispatchPlan& plan);

    MetricsCollector& collector_;
    StrategyDecider& decider_;
    StrategyExecutor& executor_;
    OnlineWorkerGroup* workers_{nullptr};
    Config cfg_;

    DispatchPlan current_plan_;
    HardwareTopology hw_;

    // 防抖动
    std::optional<DispatchPlan> pending_plan_;
    size_t consecutive_same_{0};

    uint64_t last_tick_ns_{0};
    size_t tick_count_{0};
};

}  // namespace storage::runtime
