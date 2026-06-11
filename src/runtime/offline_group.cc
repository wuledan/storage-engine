#include "offline_group.h"

namespace storage::runtime {

OfflineWorkerGroup::OfflineWorkerGroup(const Config& cfg) {
    WorkStealingExecutor::Config exec_cfg;
    exec_cfg.num_workers = cfg.num_workers;
    exec_cfg.name_prefix = cfg.name_prefix;
    exec_cfg.pin_cpus = cfg.pin_cpus;
    exec_cfg.numa_aware_stealing = cfg.numa_aware;
    executor_ = std::make_unique<WorkStealingExecutor>(exec_cfg);
}

void OfflineWorkerGroup::start() { executor_->start(); }
void OfflineWorkerGroup::stop() { executor_->stop(); executor_->join(); }

void OfflineWorkerGroup::submit(WorkItem item) { executor_->add(std::move(item)); }

void OfflineWorkerGroup::submit_to_worker(size_t worker_id, WorkItem item) {
    auto& ws = executor_->worker(worker_id);
    ws.local_deque->push(std::move(item));
    ws.has_work.store(true, std::memory_order_release);
    ws.park.notify();
}

OfflineGroupStats OfflineWorkerGroup::stats() const {
    OfflineGroupStats s;
    s.worker_count = executor_->num_workers();
    for (size_t i = 0; i < executor_->num_workers(); ++i) {
        auto& ws = executor_->worker(i);
        s.tasks_completed += ws.tasks_executed.load();
        s.steals_success += ws.steals_success.load();
        s.steals_failed += ws.steals_failed.load();
        s.parks += ws.parks.load();
    }
    // Estimate utilization
    if (s.worker_count > 0 && s.tasks_completed > s.steals_success) {
        s.utilization = 1.0 - (double)s.parks / (double)(s.tasks_completed + s.parks + 1);
    }
    return s;
}

}  // namespace storage::runtime
