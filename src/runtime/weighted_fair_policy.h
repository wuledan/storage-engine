#pragma once
#include "scheduling_policy.h"
#include <vector>

namespace storage::runtime {

class WeightedFairPolicy : public SchedulingPolicy {
public:
    // weights: 每个队列的权重，默认全部 1.0
    explicit WeightedFairPolicy(std::vector<double> weights = {}, size_t max_batch = 64);

    std::string_view name() const noexcept override {
        return "weighted_fair";
    }

    ScheduleDecision decide(
        const std::vector<QueueSnapshot>& queues,
        const SchedulerStats& stats) noexcept override;

private:
    std::vector<double> virtual_time_;   // 每个队列的虚拟时间
    std::vector<double> weights_;        // 每个队列的权重
    size_t max_batch_;
};

}  // namespace storage::runtime
