#pragma once
#include "work_queue.h"
#include "scheduling_policy.h"
#include "adaptive_idle.h"
#include "work_item.h"
#include "worker_perf.h"
#include <atomic>
#include <memory>
#include <vector>
#include <cstdint>
#include <folly/coro/Task.h>

namespace storage::runtime {

class Scheduler {
public:
    Scheduler(SchedulingPolicy* policy, AdaptiveIdle* idle);

    size_t register_queue(std::unique_ptr<WorkQueue> queue);
    WorkQueue* get_queue(size_t idx) const;

    folly::coro::Task<void> run();
    void request_stop();

    void set_policy(SchedulingPolicy* policy) noexcept { policy_ = policy; }
    void set_idle(AdaptiveIdle* idle) noexcept { idle_ = idle; }
    void set_perf(WorkerPerf* perf) noexcept { perf_ = perf; }
    void reset_stop() noexcept { stop_requested_.store(false, std::memory_order_release); }

    // 热替换指定索引的队列（仅允许在 worker 未运行时调用）
    bool replace_queue(size_t idx, std::unique_ptr<WorkQueue> new_queue) {
        if (idx >= queues_.size()) return false;
        queues_[idx] = std::move(new_queue);
        return true;
    }

    const SchedulerStats& stats() const noexcept { return stats_; }
    void reset_stats() noexcept { stats_ = SchedulerStats{}; }

private:
    std::vector<std::unique_ptr<WorkQueue>> queues_;
    SchedulingPolicy* policy_;
    AdaptiveIdle* idle_;
    WorkerPerf* perf_{nullptr};
    std::atomic<bool> running_{false};
    std::atomic<bool> stop_requested_{false};
    SchedulerStats stats_;
    std::vector<uint64_t> last_poll_times_;
    std::vector<uint64_t> total_dequeued_;
};

}  // namespace storage::runtime
