#pragma once
#include "work_item.h"
#include "work_stealing_executor.h"
#include <memory>
#include <cstddef>
#include <vector>

namespace storage::runtime {

struct OfflineGroupStats {
    size_t worker_count{0};
    uint64_t tasks_submitted{0};
    uint64_t tasks_completed{0};
    uint64_t coro_resumes{0};
    uint64_t steals_success{0};
    uint64_t steals_failed{0};
    uint64_t parks{0};
    double utilization{0.0};
};

class OfflineWorkerGroup {
public:
    struct Config {
        size_t num_workers{0};       // 0 = auto
        std::string name_prefix{"offline"};
        bool pin_cpus{true};
        bool numa_aware{true};
    };

    explicit OfflineWorkerGroup(const Config& cfg);
    ~OfflineWorkerGroup() = default;

    void start();
    void stop();

    // Submit task (fire-and-forget)
    void submit(WorkItem item);

    // Submit to specific worker (affinity routing)
    void submit_to_worker(size_t worker_id, WorkItem item);

    size_t worker_count() const noexcept { return executor_->num_workers(); }
    OfflineGroupStats stats() const;

private:
    std::unique_ptr<WorkStealingExecutor> executor_;
};

}  // namespace storage::runtime
