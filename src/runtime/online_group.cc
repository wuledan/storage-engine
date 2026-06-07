#include "online_group.h"

namespace storage::runtime {

OnlineWorkerGroup::OnlineWorkerGroup(const Config& cfg)
    : cfg_(cfg) {
    for (size_t i = 0; i < cfg_.num_workers; ++i) {
        Worker::Config wcfg;
        wcfg.cpu_id = cfg_.base_cpu_id + static_cast<uint32_t>(i);
        wcfg.numa_node = 0;
        wcfg.policy_cfg.name = "strict_priority";
        workers_.push_back(std::make_unique<OnlineWorker>(wcfg));
    }
}

void OnlineWorkerGroup::start() {
    for (auto& w : workers_) {
        w->start();
    }
}

void OnlineWorkerGroup::stop() {
    for (auto& w : workers_) {
        w->stop();
        w->join();
    }
}

size_t OnlineWorkerGroup::route_by_hash(uint64_t key) const noexcept {
    if (workers_.empty()) {
        return 0;
    }
    return key % workers_.size();
}

void OnlineWorkerGroup::submit_to(uint64_t key, WorkItem item) {
    size_t idx = route_by_hash(key);
    workers_[idx]->submit_engine(std::move(item));
}

OnlineWorker& OnlineWorkerGroup::worker(size_t index) {
    return *workers_[index];
}

OnlineGroupStats OnlineWorkerGroup::stats() const {
    OnlineGroupStats s;
    s.worker_count = workers_.size();
    for (const auto& w : workers_) {
        auto ws = w->stats();
        s.total_tasks_executed += ws.tasks_executed;
        s.total_exec_ns += ws.total_exec_ns;
    }
    return s;
}

}  // namespace storage::runtime
