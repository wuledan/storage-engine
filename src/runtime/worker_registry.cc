#include "worker_registry.h"
#include "online_worker.h"
#include <algorithm>
#include <random>

namespace storage::runtime {

WorkerRegistry& WorkerRegistry::instance() {
    static WorkerRegistry registry;
    return registry;
}

void WorkerRegistry::register_worker(OnlineWorker* w, size_t global_id, int numa_node) {
    std::lock_guard<std::mutex> lock(mutex_);
    // Ensure all_ is large enough
    if (global_id >= all_.size()) {
        all_.resize(global_id + 1, nullptr);
    }
    all_[global_id] = w;

    // Ensure by_numa_ has entry for this NUMA node
    size_t nid = static_cast<size_t>(numa_node);
    if (nid >= by_numa_.size()) {
        by_numa_.resize(nid + 1);
    }
    by_numa_[nid].push_back(w);
}

size_t WorkerRegistry::total_workers() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return all_.size();
}

OnlineWorker* WorkerRegistry::get_by_id(size_t global_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return (global_id < all_.size()) ? all_[global_id] : nullptr;
}

OnlineWorker* WorkerRegistry::get_random_from_numa(int numa_node) const {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t nid = static_cast<size_t>(numa_node);
    if (nid >= by_numa_.size() || by_numa_[nid].empty()) {
        return nullptr;
    }
    // Simple round-robin-like: pick a random index
    static thread_local std::mt19937 gen(std::random_device{}());
    const auto& v = by_numa_[nid];
    std::uniform_int_distribution<size_t> dist(0, v.size() - 1);
    return v[dist(gen)];
}

const std::vector<OnlineWorker*>& WorkerRegistry::get_by_numa(int numa_node) const {
    std::lock_guard<std::mutex> lock(mutex_);
    static const std::vector<OnlineWorker*> empty;
    size_t nid = static_cast<size_t>(numa_node);
    return (nid < by_numa_.size()) ? by_numa_[nid] : empty;
}

const std::vector<OnlineWorker*>& WorkerRegistry::get_all() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return all_;
}

}  // namespace storage::runtime
