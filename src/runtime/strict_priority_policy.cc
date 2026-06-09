#include "strict_priority_policy.h"
#include <algorithm>

namespace storage::runtime {

StrictPriorityPolicy::StrictPriorityPolicy(size_t max_batch)
    : max_batch_(max_batch) {}

ScheduleDecision StrictPriorityPolicy::decide(
    const std::vector<QueueSnapshot>& queues,
    const SchedulerStats& stats) noexcept {
    const size_t n = queues.size();
    if (n == 0) {
        return {0, 0, true, true};
    }

    // 按优先级 P0→P3 顺序扫描
    for (int prio = 0; prio <= 3; ++prio) {
        // 统计该优先级有多少非空队列
        size_t count = 0;
        for (size_t i = 0; i < n; ++i) {
            if (queues[i].priority == static_cast<Priority>(prio) &&
                queues[i].approx_count > 0) {
                ++count;
            }
        }
        if (count == 0) continue;

        // 同一优先级内 round-robin: 从 last_idx_+1 开始扫描
        // 如果优先级变了, 重置 last_idx_ 到 -1 (相当于从 0 开始)
        if (last_prio_ != static_cast<size_t>(prio)) {
            last_prio_ = static_cast<size_t>(prio);
            last_idx_ = 0;  // 会 wrap 到实际第一个
        }

        for (size_t offset = 1; offset <= n; ++offset) {
            size_t idx = (last_idx_ + offset) % n;
            if (queues[idx].priority == static_cast<Priority>(prio) &&
                queues[idx].approx_count > 0) {
                last_idx_ = idx;
                return {
                    idx,
                    std::min(queues[idx].approx_count, max_batch_),
                    false,
                    true};
            }
        }
    }

    // 全部空
    last_prio_ = SIZE_MAX;
    last_idx_ = 0;
    return {0, 0, true, true};
}

}  // namespace storage::runtime
