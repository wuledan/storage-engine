#pragma once
#include "dispatch_types.h"
#include "capacity_estimator.h"
#include <vector>
#include <string>
#include <cstdint>
#include <chrono>

namespace storage::runtime {

// ── 策略执行器 ──
//
// 记录所有策略变更决策及其上下文，供审计和分析使用。
// 实际的 worker 操作由 FeedbackLoop 通过 OnlineWorkerGroup 完成。
class StrategyExecutor {
public:
    struct DecisionRecord {
        uint64_t timestamp_ns;
        DispatchPlan from;
        DispatchPlan to;
        double utilization_before;
        double p99_latency_before;
        std::string reason;
    };

    void record_decision(const DispatchPlan& from, const DispatchPlan& to,
                         const RealTimeCapacity& cap_at_decision,
                         const std::string& reason = "") {
        auto now = std::chrono::steady_clock::now();
        DecisionRecord rec;
        rec.timestamp_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            now.time_since_epoch()).count();
        rec.from = from;
        rec.to = to;
        rec.utilization_before = cap_at_decision.utilization;
        rec.p99_latency_before = cap_at_decision.avg_latency_us;
        rec.reason = reason;
        history_.push_back(rec);
    }

    const std::vector<DecisionRecord>& history() const { return history_; }
    size_t decision_count() const { return history_.size(); }

private:
    std::vector<DecisionRecord> history_;
};

}  // namespace storage::runtime
