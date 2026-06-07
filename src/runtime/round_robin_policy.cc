#include "round_robin_policy.h"
#include <algorithm>

namespace storage::runtime {

RoundRobinPolicy::RoundRobinPolicy(size_t max_batch)
    : max_batch_(max_batch) {}

ScheduleDecision RoundRobinPolicy::decide(
    const std::vector<QueueSnapshot>& queues,
    const SchedulerStats& /*stats*/) noexcept {
    const size_t n = queues.size();
    if (n == 0) {
        return {0, 0, true, true};
    }

    // 从上次位置下一个开始轮询，跳过空队列
    for (size_t offset = 1; offset <= n; ++offset) {
        const size_t idx = (last_index_ + offset) % n;
        if (queues[idx].approx_count > 0) {
            last_index_ = idx;
            return {
                idx,
                std::min(queues[idx].approx_count, max_batch_),
                false,
                true
            };
        }
    }

    // 全部空
    return {0, 0, true, true};
}

}  // namespace storage::runtime
