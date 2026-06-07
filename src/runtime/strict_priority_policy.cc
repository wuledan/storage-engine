#include "strict_priority_policy.h"
#include <algorithm>

namespace storage::runtime {

StrictPriorityPolicy::StrictPriorityPolicy(size_t max_batch)
    : max_batch_(max_batch) {}

ScheduleDecision StrictPriorityPolicy::decide(
    const std::vector<QueueSnapshot>& queues,
    const SchedulerStats& stats) noexcept {
    // 按优先级 P0→P3 顺序扫描
    for (int prio = 0; prio <= 3; ++prio) {
        for (size_t i = 0; i < queues.size(); ++i) {
            if (queues[i].priority == static_cast<Priority>(prio) &&
                queues[i].approx_count > 0) {
                return {
                    i,
                    std::min(queues[i].approx_count, max_batch_),
                    false,
                    true};
            }
        }
    }

    // 全部空
    return {0, 0, true, true};
}

}  // namespace storage::runtime
