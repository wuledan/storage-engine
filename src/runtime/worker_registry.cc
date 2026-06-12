#include "worker_registry.h"
#include "online_worker.h"
#include "work_stealing_executor.h"
#include "ring_work_queue.h"
#include <algorithm>
#include <random>

namespace storage::runtime {

// ── WorkerHandle ──

void WorkerHandle::push_affine(WorkItem item) {
    if (type == kOnline) {
        online->enqueue_affine(item.coro);
    } else if (offline.exec) {
        auto& ws = offline.exec->worker(offline.local_id);
        ws.affine_queue->enqueue(std::move(item));
        ws.has_work.store(true, std::memory_order_release);
        ws.park.notify();
    }
    // else: stale handle (executor destroyed) — drop the item
}

// ── WorkerRegistry ──

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

    // Create a WorkerHandle for unified lookup
    if (global_id >= handles_.size()) {
        handles_.resize(global_id + 1);
    }
    auto handle = std::make_unique<WorkerHandle>();
    handle->type = WorkerHandle::kOnline;
    handle->online = w;
    handles_[global_id] = std::move(handle);
}

size_t WorkerRegistry::register_online_worker(OnlineWorker* w, int numa_node) {
    size_t gid = next_global_id_.fetch_add(1, std::memory_order_relaxed);
    register_worker(w, gid, numa_node);
    return gid;
}

size_t WorkerRegistry::register_offline_worker(WorkStealingExecutor* exec,
                                                size_t local_id, int numa_node) {
    size_t gid = next_global_id_.fetch_add(1, std::memory_order_relaxed);

    std::lock_guard<std::mutex> lock(mutex_);
    if (gid >= handles_.size()) {
        handles_.resize(gid + 1);
    }
    auto handle = std::make_unique<WorkerHandle>();
    handle->type = WorkerHandle::kOffline;
    handle->offline.exec = exec;
    handle->offline.local_id = local_id;
    handles_[gid] = std::move(handle);

    // Track by NUMA node
    size_t nid = static_cast<size_t>(numa_node);
    if (nid >= offline_by_numa_.size()) {
        offline_by_numa_.resize(nid + 1);
    }
    offline_by_numa_[nid].push_back(gid);

    // Store reverse mapping for get_global_id_for_offline()
    offline_global_id_[{exec, local_id}] = gid;

    return gid;
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

WorkerHandle* WorkerRegistry::get_handle(size_t global_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (global_id < handles_.size()) {
        return handles_[global_id].get();
    }
    return nullptr;
}

size_t WorkerRegistry::get_global_id_for_offline(WorkStealingExecutor* exec,
                                                  size_t local_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = offline_global_id_.find({exec, local_id});
    if (it != offline_global_id_.end()) {
        return it->second;
    }
    return SIZE_MAX;
}

void WorkerRegistry::remove_executor(WorkStealingExecutor* exec) {
    std::lock_guard<std::mutex> lock(mutex_);
    // Collect global IDs to remove
    std::vector<size_t> to_remove;
    for (const auto& [key, gid] : offline_global_id_) {
        if (key.exec == exec) {
            to_remove.push_back(gid);
        }
    }
    // Remove handles and NUMA mappings
    for (size_t gid : to_remove) {
        if (gid < handles_.size()) {
            handles_[gid].reset();  // nullify the handle
        }
        // Remove from offline_by_numa_
        for (auto& vec : offline_by_numa_) {
            auto it = std::find(vec.begin(), vec.end(), gid);
            if (it != vec.end()) {
                vec.erase(it);
                break;
            }
        }
    }
    // Remove from offline_global_id_ map
    for (auto it = offline_global_id_.begin(); it != offline_global_id_.end(); ) {
        if (it->first.exec == exec) {
            it = offline_global_id_.erase(it);
        } else {
            ++it;
        }
    }
}

}  // namespace storage::runtime
