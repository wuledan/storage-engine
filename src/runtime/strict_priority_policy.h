#pragma once
#include "scheduling_policy.h"

namespace storage::runtime {

class StrictPriorityPolicy : public SchedulingPolicy {
public:
    explicit StrictPriorityPolicy(size_t max_batch = 64);

    std::string_view name() const noexcept override {
        return "strict_priority";
    }

    ScheduleDecision decide(
        const std::vector<QueueSnapshot>& queues,
        const SchedulerStats& stats) noexcept override;

private:
    size_t max_batch_;
    // 同一优先级内 round-robin: 上次选择的队列索引
    size_t last_prio_{SIZE_MAX};
    size_t last_idx_{0};
};

}  // namespace storage::runtime
