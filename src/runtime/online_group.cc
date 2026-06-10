#include "online_group.h"
#include <hwloc.h>
#include <algorithm>
#include <numeric>

namespace storage::runtime {

// ── NUMA topology discovery (reuses pattern from WorkStealingExecutor) ──

void OnlineWorkerGroup::discover_topology() {
    if (!cfg_.auto_discover_numa && cfg_.per_numa_workers.empty()) {
        return;
    }

    hwloc_topology_t topo;
    hwloc_topology_init(&topo);
    hwloc_topology_load(topo);

    int numa_count = hwloc_get_nbobjs_by_type(topo, HWLOC_OBJ_NUMANODE);
    if (numa_count < 1) numa_count = 1;

    // Determine workers per NUMA node
    std::vector<size_t> per_numa;
    if (!cfg_.per_numa_workers.empty()) {
        per_numa = cfg_.per_numa_workers;
        // Pad with 0s if needed
        per_numa.resize(static_cast<size_t>(numa_count), 0);
    } else {
        size_t total = cfg_.num_workers;
        if (total == 0) {
            // Auto: 1 worker per physical core total
            for (int n = 0; n < numa_count; ++n) {
                hwloc_obj_t node = hwloc_get_obj_by_type(topo, HWLOC_OBJ_NUMANODE, n);
                unsigned n_pus = hwloc_get_nbobjs_inside_cpuset_by_type(
                    topo, node->cpuset, HWLOC_OBJ_PU);
                size_t phys_cores = 0;
                for (unsigned i = 0; i < n_pus; ++i) {
                    hwloc_obj_t pu = hwloc_get_obj_inside_cpuset_by_type(
                        topo, node->cpuset, HWLOC_OBJ_PU, i);
                    hwloc_obj_t core = hwloc_get_ancestor_obj_by_type(
                        topo, HWLOC_OBJ_CORE, pu);
                    if (core && pu == core->children[0]) phys_cores++;
                }
                per_numa.push_back(phys_cores);
            }
            total = std::accumulate(per_numa.begin(), per_numa.end(), size_t(0));
        } else {
            // Distribute evenly across NUMA nodes
            size_t base_per_numa = total / static_cast<size_t>(numa_count);
            size_t remainder = total % static_cast<size_t>(numa_count);
            for (int n = 0; n < numa_count; ++n) {
                per_numa.push_back(base_per_numa + (static_cast<size_t>(n) < remainder ? 1 : 0));
            }
        }
        // Store for reference
        const_cast<Config&>(cfg_).per_numa_workers = per_numa;
    }

    // Collect physical PUs per NUMA node
    std::vector<std::vector<int>> per_numa_pus(static_cast<size_t>(numa_count));
    for (int n = 0; n < numa_count; ++n) {
        hwloc_obj_t node = hwloc_get_obj_by_type(topo, HWLOC_OBJ_NUMANODE, n);
        unsigned n_pus = hwloc_get_nbobjs_inside_cpuset_by_type(
            topo, node->cpuset, HWLOC_OBJ_PU);
        
        for (unsigned i = 0; i < n_pus; ++i) {
            hwloc_obj_t pu = hwloc_get_obj_inside_cpuset_by_type(
                topo, node->cpuset, HWLOC_OBJ_PU, i);
            
            if (!cfg_.use_ht_siblings) {
                hwloc_obj_t core = hwloc_get_ancestor_obj_by_type(
                    topo, HWLOC_OBJ_CORE, pu);
                // Skip HT siblings — only first PU per core
                if (!core || pu != core->children[0]) continue;
            }
            per_numa_pus[n].push_back(pu->os_index);
        }
    }

    // Create workers distributed across NUMA nodes
    size_t global_id = 0;
    size_t total_per_numa_count = std::accumulate(per_numa.begin(), per_numa.end(), size_t(0));
    workers_.reserve(total_per_numa_count);
    numa_worker_ranges_.resize(static_cast<size_t>(numa_count), 0);
    numa_worker_lists_.resize(static_cast<size_t>(numa_count));

    for (int n = 0; n < numa_count; ++n) {
        numa_worker_ranges_[n] = workers_.size(); // start index for this NUMA
        
        size_t count = (n < static_cast<int>(per_numa.size())) ? per_numa[n] : 0;
        for (size_t w = 0; w < count; ++w) {
            Worker::Config wcfg;
            wcfg.policy_cfg.name = "strict_priority";
            
            // CPU assignment: round-robin across available PUs in this NUMA node
            const auto& pus = per_numa_pus[n];
            if (!pus.empty() && cfg_.pin_cpu) {
                wcfg.cpu_id = static_cast<uint32_t>(pus[w % pus.size()]) + 1;  // +1 because 0 means "no pin"
            } else if (cfg_.base_cpu_id > 0) {
                wcfg.cpu_id = cfg_.base_cpu_id + static_cast<uint32_t>(global_id);
            } else {
                wcfg.cpu_id = 0;  // no pinning
            }
            
            // NUMA node
            wcfg.numa_node = cfg_.bind_memory ? static_cast<uint32_t>(n + 1) : 0;  // +1 because 0 means "no bind"
            
            auto worker = std::make_unique<OnlineWorker>(wcfg);
            
            // Register globally
            if (cfg_.register_global) {
                WorkerRegistry::instance().register_worker(worker.get(), global_id, n);
            }
            
            // Track in per-NUMA lists
            numa_worker_lists_[n].push_back(worker.get());
            
            workers_.push_back(std::move(worker));
            ++global_id;
        }
    }

    hwloc_topology_destroy(topo);
}

OnlineWorkerGroup::OnlineWorkerGroup(const Config& cfg)
    : cfg_(cfg) {
    if (cfg_.auto_discover_numa || !cfg_.per_numa_workers.empty()) {
        discover_topology();
    } else {
        // Legacy: sequential CPU IDs
        workers_.reserve(cfg_.num_workers);
        for (size_t i = 0; i < cfg_.num_workers; ++i) {
            Worker::Config wcfg;
            wcfg.cpu_id = cfg_.base_cpu_id + static_cast<uint32_t>(i) + (cfg_.pin_cpu ? 1 : 0);
            wcfg.numa_node = cfg_.bind_memory ? 1 : 0;  // default NUMA 0
            wcfg.policy_cfg.name = "strict_priority";
            auto worker = std::make_unique<OnlineWorker>(wcfg);
            if (cfg_.register_global) {
                WorkerRegistry::instance().register_worker(worker.get(), i, 0);
            }
            workers_.push_back(std::move(worker));
        }
    }
}

OnlineWorkerGroup::~OnlineWorkerGroup() {
    // Workers are destroyed automatically via unique_ptr
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
    if (workers_.empty()) return 0;
    return key % workers_.size();
}

size_t OnlineWorkerGroup::route_by_numa_hash(uint64_t key, int numa_node) const noexcept {
    auto nid = static_cast<size_t>(numa_node);
    if (nid >= numa_worker_lists_.size() || numa_worker_lists_[nid].empty()) {
        return route_by_hash(key);  // fallback to global hash
    }
    return key % numa_worker_lists_[nid].size();
}

void OnlineWorkerGroup::submit_to(uint64_t key, WorkItem item) {
    size_t idx = route_by_hash(key);
    workers_[idx]->submit_engine(std::move(item));
}

void OnlineWorkerGroup::submit_to_numa(int numa_node, WorkItem item) {
    auto* w = WorkerRegistry::instance().get_random_from_numa(numa_node);
    if (w) {
        w->submit_engine(std::move(item));
    }
}

OnlineWorker& OnlineWorkerGroup::worker(size_t index) {
    return *workers_[index];
}

const std::vector<OnlineWorker*>& OnlineWorkerGroup::get_numa_workers(int numa_node) const noexcept {
    static const std::vector<OnlineWorker*> empty;
    auto nid = static_cast<size_t>(numa_node);
    return (nid < numa_worker_lists_.size()) ? numa_worker_lists_[nid] : empty;
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
