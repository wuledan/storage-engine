#pragma once
#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "work_item.h"

namespace storage::runtime {

class OnlineWorker;
class WorkStealingExecutor;

// ── WorkerHandle ──
//
// Unified handle for routing work to any worker (online or offline).
// Online workers use enqueue_affine to deliver a coroutine handle;
// offline workers push the WorkItem to the per-worker affine queue.
struct WorkerHandle {
    enum Type : uint8_t { kOnline, kOffline };
    Type type;
    union {
        OnlineWorker* online;
        struct {
            WorkStealingExecutor* exec;
            size_t local_id;
        } offline;
    };

    // Deliver a WorkItem to this worker's affine queue.
    // For online workers the coroutine handle is extracted and passed
    // to enqueue_affine(); for offline workers the WorkItem is pushed
    // directly into the per-worker affine RingWorkQueue (MPSC).
    void push_affine(WorkItem item);
};

// Global singleton registry of all OnlineWorker instances.
// Indexed by global worker ID and NUMA node.
// Thread-safe for registration, const-access for queries.
class WorkerRegistry {
public:
    static WorkerRegistry& instance();

    // Register an online worker with a pre-assigned global ID
    // (called by OnlineWorkerGroup during construction).
    void register_worker(OnlineWorker* w, size_t global_id, int numa_node);

    // Register an online worker with an auto-generated global ID.
    // Returns the assigned global ID.
    size_t register_online_worker(OnlineWorker* w, int numa_node);

    // Register an offline worker with an auto-generated global ID.
    // Returns the assigned global ID.
    size_t register_offline_worker(WorkStealingExecutor* exec,
                                   size_t local_id, int numa_node);

    // ── Queries ──
    size_t total_workers() const;
    OnlineWorker* get_by_id(size_t global_id) const;
    OnlineWorker* get_random_from_numa(int numa_node) const;

    // Get all workers on a specific NUMA node
    const std::vector<OnlineWorker*>& get_by_numa(int numa_node) const;

    // Flat list of all workers (global index order)
    const std::vector<OnlineWorker*>& get_all() const;

    // Get a WorkerHandle for any registered worker (online or offline).
    // Returns nullptr if the global_id is unknown.
    WorkerHandle* get_handle(size_t global_id);

    // Look up the global ID for an offline worker by (exec, local_id) pair.
    // Returns SIZE_MAX if not found.
    size_t get_global_id_for_offline(WorkStealingExecutor* exec,
                                     size_t local_id) const;

    // Remove all offline workers belonging to a given executor.
    // Called by WorkStealingExecutor destructor.
    void remove_executor(WorkStealingExecutor* exec);

private:
    WorkerRegistry() = default;
    mutable std::mutex mutex_;
    std::vector<OnlineWorker*> all_;                // global index → worker
    std::vector<std::vector<OnlineWorker*>> by_numa_; // NUMA node → workers

    // Unified handle storage indexed by global_id.
    // Online workers registered via register_worker() also get a handle.
    std::vector<std::unique_ptr<WorkerHandle>> handles_;

    // Offline-specific: NUMA groups indexed by global_id (not OnlineWorker*).
    std::vector<std::vector<size_t>> offline_by_numa_;

    // Reverse mapping: (exec, local_id) → global_id for offline workers.
    // Populated in register_offline_worker(); queried by get_global_id_for_offline().
    struct OfflineKey {
        WorkStealingExecutor* exec;
        size_t local_id;
        bool operator==(const OfflineKey& o) const noexcept {
            return exec == o.exec && local_id == o.local_id;
        }
    };
    struct OfflineKeyHash {
        size_t operator()(const OfflineKey& k) const noexcept {
            return std::hash<WorkStealingExecutor*>{}(k.exec) ^ (k.local_id << 1);
        }
    };
    std::unordered_map<OfflineKey, size_t, OfflineKeyHash> offline_global_id_;

    // Monotonic counter for auto-generated global IDs.
    std::atomic<size_t> next_global_id_{0};
};

}  // namespace storage::runtime
