#pragma once
#include "scheduling_policy.h"
#include <memory>
#include <string>
#include <vector>

namespace storage::runtime {

struct PolicyConfig {
    std::string name;              // "strict_priority" | "weighted_fair" | "round_robin"
    std::vector<double> weights;   // WeightedFair 权重 (可选)
    size_t max_batch{64};
};

// 工厂函数：根据配置创建对应的调度策略
std::unique_ptr<SchedulingPolicy> make_policy(const PolicyConfig& cfg);

}  // namespace storage::runtime
