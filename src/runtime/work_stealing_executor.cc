#include "work_stealing_executor.h"
#include <hwloc.h>
#include <algorithm>
#include <cstdlib>
#include <mutex>
#include <numeric>

namespace storage::runtime {

thread_local size_t WorkStealingExecutor::tl_worker_id_ = SIZE_MAX;
thread_local WorkStealingExecutor* WorkStealingExecutor::tl_executor_ = nullptr;

namespace {
std::atomic<size_t> g_add_counter{0};
}  // anonymous namespace

WorkStealingExecutor::WorkStealingExecutor(const Config& cfg) : cfg_(cfg) {
    hwloc_topology_t topo;
    hwloc_topology_init(&topo);
    hwloc_topology_load(topo);

    int numa_count = hwloc_get_nbobjs_by_type(topo, HWLOC_OBJ_NUMANODE);
    if (numa_count < 1) numa_count = 1;

    // ── Determine workers per NUMA node ──
    std::vector<size_t> per_numa(static_cast<size_t>(numa_count), 0);
    size_t total_workers = cfg_.num_workers;

    if (total_workers == 0) {
        // Auto: 1 worker per physical core
        for (int n = 0; n < numa_count; ++n) {
            hwloc_obj_t node = hwloc_get_obj_by_type(topo, HWLOC_OBJ_NUMANODE, n);
            if (!node) {
                per_numa[static_cast<size_t>(n)] = 1;
                continue;
            }
            unsigned n_pus = hwloc_get_nbobjs_inside_cpuset_by_type(
                topo, node->cpuset, HWLOC_OBJ_PU);
            size_t phys_cores = 0;
            for (unsigned i = 0; i < n_pus; ++i) {
                hwloc_obj_t pu = hwloc_get_obj_inside_cpuset_by_type(
                    topo, node->cpuset, HWLOC_OBJ_PU, i);
                hwloc_obj_t core = hwloc_get_ancestor_obj_by_type(
                    topo, HWLOC_OBJ_CORE, pu);
                if (core && pu == core->children[0]) ++phys_cores;
            }
            per_numa[static_cast<size_t>(n)] = phys_cores ? phys_cores : 1;
        }
        total_workers = std::accumulate(per_numa.begin(), per_numa.end(), size_t(0));
    } else if (!cfg_.numa_aware_stealing) {
        // Distribute evenly across NUMA nodes
        size_t base = total_workers / static_cast<size_t>(numa_count);
        size_t rem = total_workers % static_cast<size_t>(numa_count);
        for (int n = 0; n < numa_count; ++n) {
            per_numa[static_cast<size_t>(n)] = base + (static_cast<size_t>(n) < rem ? 1 : 0);
        }
    } else {
        // NUMA-aware: distribute proportional to physical cores
        std::vector<size_t> phys_per_numa(static_cast<size_t>(numa_count), 0);
        size_t total_phys = 0;
        for (int n = 0; n < numa_count; ++n) {
            hwloc_obj_t node = hwloc_get_obj_by_type(topo, HWLOC_OBJ_NUMANODE, n);
            if (!node) {
                phys_per_numa[static_cast<size_t>(n)] = 1;
                total_phys += 1;
                continue;
            }
            unsigned n_pus = hwloc_get_nbobjs_inside_cpuset_by_type(
                topo, node->cpuset, HWLOC_OBJ_PU);
            size_t phys = 0;
            for (unsigned i = 0; i < n_pus; ++i) {
                hwloc_obj_t pu = hwloc_get_obj_inside_cpuset_by_type(
                    topo, node->cpuset, HWLOC_OBJ_PU, i);
                hwloc_obj_t core = hwloc_get_ancestor_obj_by_type(
                    topo, HWLOC_OBJ_CORE, pu);
                if (core && pu == core->children[0]) ++phys;
            }
            phys_per_numa[static_cast<size_t>(n)] = phys ? phys : 1;
            total_phys += phys_per_numa[static_cast<size_t>(n)];
        }
        size_t allocated = 0;
        for (int n = 0; n < numa_count; ++n) {
            size_t share = (n == numa_count - 1)
                ? (total_workers - allocated)
                : (total_workers * phys_per_numa[static_cast<size_t>(n)] / total_phys);
            per_numa[static_cast<size_t>(n)] = share;
            allocated += share;
        }
    }

    if (total_workers == 0) {
        total_workers = 1;
        per_numa[0] = 1;
    }

    // ── Collect PUs per NUMA node for CPU pinning ──
    std::vector<std::vector<int>> per_numa_pus(static_cast<size_t>(numa_count));
    for (int n = 0; n < numa_count; ++n) {
        hwloc_obj_t node = hwloc_get_obj_by_type(topo, HWLOC_OBJ_NUMANODE, n);
        if (!node) continue;
        unsigned n_pus = hwloc_get_nbobjs_inside_cpuset_by_type(
            topo, node->cpuset, HWLOC_OBJ_PU);
        for (unsigned i = 0; i < n_pus; ++i) {
            hwloc_obj_t pu = hwloc_get_obj_inside_cpuset_by_type(
                topo, node->cpuset, HWLOC_OBJ_PU, i);
            if (!cfg_.use_ht_siblings) {
                hwloc_obj_t core = hwloc_get_ancestor_obj_by_type(
                    topo, HWLOC_OBJ_CORE, pu);
                if (!core || pu != core->children[0]) continue;
            }
            per_numa_pus[n].push_back(pu->os_index);
        }
    }

    // ── Create WorkerState objects ──
    workers_.reserve(total_workers);
    std::vector<int> worker_numa_node;
    worker_numa_node.reserve(total_workers);

    size_t global_id = 0;
    for (int n = 0; n < numa_count; ++n) {
        size_t count = per_numa[static_cast<size_t>(n)];
        for (size_t w = 0; w < count && global_id < total_workers; ++w) {
            auto ws = std::make_unique<WorkerState>();
            ws->id = global_id;
            ws->numa_node = n;

            // CPU assignment
            const auto& pus = per_numa_pus[n];
            if (!pus.empty() && cfg_.pin_cpus) {
                ws->hwloc_cpu = pus[w % pus.size()];
            } else {
                ws->hwloc_cpu = -1;
            }

            ws->local_deque = std::make_unique<WorkStealingDeque>();
            ws->yield_queue = std::make_unique<LocalWorkQueue>(
                QueueType::kEngine, Priority::kMedium, "wse_yield");
            ws->running.store(false, std::memory_order_relaxed);
            ws->has_work.store(false, std::memory_order_relaxed);

            worker_numa_node.push_back(n);
            workers_.push_back(std::move(ws));
            ++global_id;
        }
    }

    hwloc_topology_destroy(topo);

    // ── Build NUMA peer lists ──
    numa_peers_.resize(workers_.size());
    for (size_t i = 0; i < workers_.size(); ++i) {
        for (size_t j = 0; j < workers_.size(); ++j) {
            if (i == j) continue;
            if (!cfg_.numa_aware_stealing || worker_numa_node[i] == worker_numa_node[j]) {
                numa_peers_[i].push_back(j);
            }
        }
        // Copy peer list into the WorkerState for lock-free access in worker_loop
        workers_[i]->numa_peers = numa_peers_[i];
    }

    // ── Create global submission queue ──
    global_queue_ = std::make_unique<RingWorkQueue>(
        QueueType::kEngine, Priority::kMedium,
        cfg_.name_prefix + "_global", 4096);
}

WorkStealingExecutor::~WorkStealingExecutor() {
    stop();
    join();
}

void WorkStealingExecutor::start() {
    for (auto& ws_ptr : workers_) {
        ws_ptr->running.store(true, std::memory_order_release);
        WorkerState* ws = ws_ptr.get();
        ws_ptr->thread = std::thread([this, ws]() {
            // CPU pinning
            if (ws->hwloc_cpu >= 0 && cfg_.pin_cpus) {
                hwloc_topology_t topo;
                hwloc_topology_init(&topo);
                hwloc_topology_load(topo);
                hwloc_cpuset_t cpuset = hwloc_bitmap_alloc();
                hwloc_bitmap_set(cpuset, static_cast<unsigned>(ws->hwloc_cpu));
                hwloc_set_cpubind(topo, cpuset, HWLOC_CPUBIND_THREAD);
                hwloc_bitmap_free(cpuset);
                hwloc_topology_destroy(topo);
            }
            worker_loop(*ws);
        });
    }
}

void WorkStealingExecutor::stop() {
    for (auto& ws_ptr : workers_) {
        ws_ptr->running.store(false, std::memory_order_release);
        ws_ptr->park.notify();
    }
}

void WorkStealingExecutor::join() {
    for (auto& ws_ptr : workers_) {
        if (ws_ptr->thread.joinable()) {
            ws_ptr->thread.join();
        }
    }
}

void WorkStealingExecutor::add(WorkItem item) {
    global_queue_->enqueue(std::move(item));
    // Wake a worker via round-robin to distribute wake-up load
    if (!workers_.empty()) {
        size_t idx = g_add_counter.fetch_add(1, std::memory_order_relaxed) % workers_.size();
        workers_[idx]->park.notify();
    }
}

void WorkStealingExecutor::add_to_worker(size_t worker_id, std::coroutine_handle<> h) {
    if (worker_id >= workers_.size()) return;
    auto& ws = *workers_[worker_id];
    ws.local_deque->push(WorkItem::make_coro(h));
    ws.has_work.store(true, std::memory_order_release);
    ws.park.notify();
}

adapt::RouteFunc WorkStealingExecutor::make_route_func() {
    return [this](size_t worker_id, std::coroutine_handle<> h) {
        this->add_to_worker(worker_id, h);
    };
}

void WorkStealingExecutor::worker_loop(WorkerState& ws) {
    // Set up TLS
    tl_worker_id_ = ws.id;
    tl_executor_ = this;
    adapt::detail::get_current_worker_id = []() -> size_t { return tl_worker_id_; };
    tls_source_queue_idx = SIZE_MAX;
    tls_source_queue = nullptr;

    ws.running.store(true, std::memory_order_release);

    while (ws.running.load(std::memory_order_acquire)) {
        WorkItem item;

        // 0. Drain yield queue (coroutines that yielded back to this worker)
        if (ws.yield_queue->try_dequeue(item)) {
            tls_source_queue = ws.yield_queue.get();
            tls_source_queue_idx = SIZE_MAX;  // tell yield() to use tls_source_queue
            item.execute();
            ws.tasks_executed.fetch_add(1, std::memory_order_relaxed);
            continue;
        }

        // 1. Pop from own local deque (LIFO — best cache locality)
        if (ws.local_deque->pop(item)) {
            tls_source_queue = ws.yield_queue.get();
            tls_source_queue_idx = SIZE_MAX;
            item.execute();
            ws.tasks_executed.fetch_add(1, std::memory_order_relaxed);
            continue;
        }

        // 2. Try to dequeue from global queue
        //    Note: RingWorkQueue dequeue is single-consumer only, so we serialize
        //    with a mutex.  Contention is low because most tasks are stolen locally.
        {
            std::lock_guard<std::mutex> lock(global_mutex_);
            if (global_queue_->try_dequeue(item)) {
                tls_source_queue = ws.yield_queue.get();
                tls_source_queue_idx = SIZE_MAX;
                item.execute();
                ws.tasks_executed.fetch_add(1, std::memory_order_relaxed);
                continue;
            }
        }

        // 3. Steal from another worker (NUMA-local if enabled)
        bool stolen = false;
        const auto& peers = ws.numa_peers;
        if (!peers.empty()) {
            size_t attempts = cfg_.steal_attempts;
            // Simple random start to avoid all workers stealing from same victim
            size_t start_idx = ws.id * 2654435761ULL % peers.size();
            for (size_t i = 0; i < attempts && !stolen; ++i) {
                size_t victim_idx = peers[(start_idx + i) % peers.size()];
                auto& victim = *workers_[victim_idx];
                if (victim.local_deque->steal(item)) {
                    tls_source_queue = ws.yield_queue.get();
                    tls_source_queue_idx = SIZE_MAX;
                    item.execute();
                    ws.tasks_executed.fetch_add(1, std::memory_order_relaxed);
                    ws.steals_success.fetch_add(1, std::memory_order_relaxed);
                    stolen = true;
                    break;
                }
                ws.steals_failed.fetch_add(1, std::memory_order_relaxed);
            }
        }
        if (stolen) continue;

        // 4. Nothing found — park (adaptive idle: spin → yield → cv wait)
        ws.parks.fetch_add(1, std::memory_order_relaxed);
        ws.park.enter_idle();
    }
}

size_t WorkStealingExecutor::current_worker_id() {
    return tl_worker_id_;
}

WorkStealingExecutor* WorkStealingExecutor::current_executor() {
    return tl_executor_;
}

}  // namespace storage::runtime
