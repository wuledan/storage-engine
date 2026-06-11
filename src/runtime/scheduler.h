#pragma once
#include "work_queue.h"
#include "scheduling_policy.h"
#include "adaptive_idle.h"
#include "work_item.h"
#include "worker_perf.h"
#include "io/io_backend.h"
#include <atomic>
#include <memory>
#include <vector>
#include <cstdint>
#include <atomic>

namespace storage::runtime {

// Thread-local: set by Scheduler before each coroutine/func execution
// to indicate which queue the current work item was dequeued from.
// Used by yield() to route the coroutine back to its source queue.
// Consumer: yield_awaiter in yield_awaiter.h
extern thread_local size_t tls_source_queue_idx;

// Thread-local pointer to the source WorkQueue, set alongside the index
// above.  Provides a direct queue pointer for yield_awaiter::await_suspend
// when current_worker() is not available (e.g. during initialization).
extern thread_local WorkQueue* tls_source_queue;

class Scheduler {
public:
    Scheduler(SchedulingPolicy* policy, AdaptiveIdle* idle);

    size_t register_queue(std::unique_ptr<WorkQueue> queue);
    WorkQueue* get_queue(size_t idx) const;

    void run();
    void request_stop();

    void set_policy(SchedulingPolicy* policy) noexcept { policy_ = policy; }
    void set_idle(AdaptiveIdle* idle) noexcept { idle_ = idle; }
    void set_perf(WorkerPerf* perf) noexcept { perf_ = perf; }
    void set_io_backend(storage::io::IIOBackend* backend) noexcept { io_backend_ = backend; }
    void set_affine_idx(size_t idx) noexcept { affine_idx_ = idx; }
    void set_busy_poll(bool v) noexcept { busy_poll_ = v; }
    void set_disk_io_idx(size_t idx) noexcept { disk_io_idx_ = idx; }
    void reset_stop() noexcept { stop_requested_.store(false, std::memory_order_release); }

    // 热替换指定索引的队列（仅允许在 worker 未运行时调用）
    bool replace_queue(size_t idx, std::unique_ptr<WorkQueue> new_queue) {
        if (idx >= queues_.size()) return false;
        queues_[idx] = std::move(new_queue);
        return true;
    }

    const SchedulerStats& stats() const noexcept { return stats_; }
    void reset_stats() noexcept { stats_ = SchedulerStats{}; }

    // 探针：累计各阶段耗时 (rdtsc cycles)
    struct Probe { uint64_t io_poll{0}, drain_p0{0}, snapshot{0}, decide{0}, dequeue_exec{0}, drain_p0b{0}, iter{0}; };
    Probe probe;
    size_t probe_count{0};
    void reset_probe() { probe = Probe{}; probe_count = 0; }

private:
    void drain_p0(std::vector<WorkItem>& batch, size_t max_batch);

    std::vector<std::unique_ptr<WorkQueue>> queues_;
    SchedulingPolicy* policy_;
    AdaptiveIdle* idle_;
    WorkerPerf* perf_{nullptr};
    size_t affine_idx_{SIZE_MAX};
    size_t disk_io_idx_{SIZE_MAX};
    storage::io::IIOBackend* io_backend_{nullptr};
    std::atomic<bool> running_{false};
    std::atomic<bool> stop_requested_{false};
    bool busy_poll_{false};
    void run_busy();
    SchedulerStats stats_;
    std::vector<uint64_t> last_poll_times_;
    std::vector<uint64_t> total_dequeued_;
};

}  // namespace storage::runtime
