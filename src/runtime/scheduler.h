#pragma once
#include "work_queue.h"
#include "scheduling_policy.h"
#include "adaptive_idle.h"
#include "work_item.h"
#include <atomic>
#include <memory>
#include <vector>
#include <cstdint>

namespace storage::runtime {

class Scheduler {
public:
    Scheduler(SchedulingPolicy* policy, AdaptiveIdle* idle);

    size_t register_queue(std::unique_ptr<WorkQueue> queue);

    void run();
    void request_stop();

    const SchedulerStats& stats() const noexcept { return stats_; }
    void reset_stats() noexcept { stats_ = SchedulerStats{}; }

private:
    std::vector<std::unique_ptr<WorkQueue>> queues_;
    SchedulingPolicy* policy_;
    AdaptiveIdle* idle_;
    std::atomic<bool> running_{false};
    SchedulerStats stats_;
    std::vector<uint64_t> last_poll_times_;
    std::vector<uint64_t> total_dequeued_;
};

}  // namespace storage::runtime
