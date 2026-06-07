#include "weighted_fair_policy.h"
#include <algorithm>
#include <vector>

namespace storage::runtime {

WeightedFairPolicy::WeightedFairPolicy(std::vector<double> weights, size_t max_batch)
    : weights_(std::move(weights)), max_batch_(max_batch) {}

ScheduleDecision WeightedFairPolicy::decide(
    const std::vector<QueueSnapshot>& queues,
    const SchedulerStats& stats) noexcept {
    const size_t n = queues.size();

    // 第一次调用或队列数量变化时初始化
    if (virtual_time_.size() != n) {
        virtual_time_.assign(n, 0.0);
    }
    if (weights_.size() != n) {
        weights_.assign(n, 1.0);
    }

    // 找到 virtual_time 最小的非空队列
    size_t best_idx = 0;
    bool found = false;
    for (size_t i = 0; i < n; ++i) {
        if (queues[i].approx_count > 0) {
            if (!found || virtual_time_[i] < virtual_time_[best_idx]) {
                best_idx = i;
                found = true;
            }
        }
    }

    if (!found) {
        return {0, 0, true, true};
    }

    // 计算 batch_size
    size_t batch = std::min(queues[best_idx].approx_count, max_batch_);

    // 更新虚拟时间
    virtual_time_[best_idx] += static_cast<double>(batch) / weights_[best_idx];

    return {best_idx, batch, false, true};
}

}  // namespace storage::runtime
