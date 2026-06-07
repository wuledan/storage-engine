#include "policy_factory.h"
#include "strict_priority_policy.h"
#include "weighted_fair_policy.h"
#include "round_robin_policy.h"
#include <stdexcept>

namespace storage::runtime {

std::unique_ptr<SchedulingPolicy> make_policy(const PolicyConfig& cfg) {
    if (cfg.name == "strict_priority") {
        return std::make_unique<StrictPriorityPolicy>(cfg.max_batch);
    }
    if (cfg.name == "weighted_fair") {
        return std::make_unique<WeightedFairPolicy>(cfg.weights, cfg.max_batch);
    }
    if (cfg.name == "round_robin") {
        return std::make_unique<RoundRobinPolicy>(cfg.max_batch);
    }
    throw std::runtime_error("Unknown scheduling policy: " + cfg.name);
}

}  // namespace storage::runtime
