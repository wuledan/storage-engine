#include "offline_group.h"
// work_stealing_executor.h has been removed (dead code)
// #include "adapt/work_stealing_executor.h"
#include "work_item.h"

namespace storage::runtime {

OfflineWorkerGroup::OfflineWorkerGroup(const Config& cfg)
    : cfg_(cfg),
      executor_(std::make_unique<adapt::WorkStealingExecutor>(
          cfg_.num_workers, cfg_.name_prefix)) {}

OfflineWorkerGroup::~OfflineWorkerGroup() = default;

void OfflineWorkerGroup::start() {
    executor_->start();
}

void OfflineWorkerGroup::stop() {
    executor_->stop();
}

void OfflineWorkerGroup::submit(WorkItem item) {
    executor_->add([item = std::move(item)]() mutable {
        item.execute();
    });
}

void OfflineWorkerGroup::submit_to_worker(size_t worker_id, WorkItem item) {
    executor_->add_to_worker(worker_id, [item = std::move(item)]() mutable {
        item.execute();
    });
}

OfflineGroupStats OfflineWorkerGroup::stats() const {
    auto ws = executor_->stats();
    return {
        ws.tasks_submitted,
        ws.tasks_completed,
        ws.local_pops,
        ws.local_steals,
        ws.failed_steals,
        ws.park_count,
        ws.utilization,
    };
}

bool OfflineWorkerGroup::is_running() const noexcept {
    return executor_->is_running();
}

}  // namespace storage::runtime
