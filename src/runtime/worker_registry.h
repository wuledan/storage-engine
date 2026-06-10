#pragma once
#include <cstddef>
#include <mutex>
#include <vector>

namespace storage::runtime {

class OnlineWorker;

// Global singleton registry of all OnlineWorker instances.
// Indexed by global worker ID and NUMA node.
// Thread-safe for registration, const-access for queries.
class WorkerRegistry {
public:
    static WorkerRegistry& instance();

    // Register a worker (called by OnlineWorkerGroup during construction)
    void register_worker(OnlineWorker* w, size_t global_id, int numa_node);

    // ── Queries ──
    size_t total_workers() const;
    OnlineWorker* get_by_id(size_t global_id) const;
    OnlineWorker* get_random_from_numa(int numa_node) const;

    // Get all workers on a specific NUMA node
    const std::vector<OnlineWorker*>& get_by_numa(int numa_node) const;

    // Flat list of all workers (global index order)
    const std::vector<OnlineWorker*>& get_all() const;

private:
    WorkerRegistry() = default;
    mutable std::mutex mutex_;
    std::vector<OnlineWorker*> all_;                // global index → worker
    std::vector<std::vector<OnlineWorker*>> by_numa_; // NUMA node → workers
};

}  // namespace storage::runtime
