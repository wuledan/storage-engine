#pragma once
#include "scheduling_policy.h"

namespace storage::runtime {

class RoundRobinPolicy : public SchedulingPolicy {
public:
    explicit RoundRobinPolicy(size_t max_batch = 64);

    std::string_view name() const noexcept override {
        return "round_robin";
    }

    ScheduleDecision decide(
        const std::vector<QueueSnapshot>& queues,
        const SchedulerStats& stats) noexcept override;

private:
    size_t last_index_{0};
    size_t max_batch_;
};

}  // namespace storage::runtime
