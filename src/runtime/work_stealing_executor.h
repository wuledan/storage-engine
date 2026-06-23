#pragma once
#include "coro_primitives.h"
#include "timer.h"
#include "adaptive_idle.h"
#include "local_work_queue.h"
#include "ring_work_queue.h"
#include <atomic>
#include <memory>
#include <thread>
#include <vector>
#include <string>
#include <functional>

namespace storage::runtime {

class WorkStealingExecutor {
public:
    struct Config {
        size_t num_workers{0};            // 0 = auto (1 per physical core)
        std::string name_prefix{"ws"};
        bool pin_cpus{true};              // CPU affinity
        bool use_ht_siblings{false};
        bool numa_aware_stealing{true};   // only steal from same NUMA node
        size_t steal_attempts{4};         // number of steal attempts before backoff
    };

    // ── Worker state ──
    struct WorkerState {
        size_t id;
        int numa_node{0};
        int hwloc_cpu{-1};

        // Work queues
        std::unique_ptr<RingWorkQueue> local_deque;  // MPMC ring, FIFO, lock-free
        std::unique_ptr<LocalWorkQueue> yield_queue;     // yield() target for this worker

        // Thread + coordination
        std::thread thread;
        std::atomic<bool> running{false};
        AdaptiveIdle park;               // for parking when idle
        std::atomic<bool> has_work{false};  // set when work arrives (used by park predicate)

        // Affinity routing
        std::vector<size_t> numa_peers;   // worker IDs in same NUMA node

        // Stats
        std::atomic<uint64_t> tasks_executed{0};
        std::atomic<uint64_t> coro_resumes{0};    // coroutine resumes (may yield, count each resume)
        std::atomic<uint64_t> steals_success{0};
        std::atomic<uint64_t> steals_failed{0};
        std::atomic<uint64_t> parks{0};
    };

    explicit WorkStealingExecutor(const Config& cfg);
    ~WorkStealingExecutor();

    WorkStealingExecutor(const WorkStealingExecutor&) = delete;
    WorkStealingExecutor& operator=(const WorkStealingExecutor&) = delete;

    // ── Submission ──

    // Submit from any thread (external or worker). Pushes to a worker's local deque.
    void add(WorkItem item);

    // Convenience: wraps any callable in a coroutine and submits it as WorkItem.
    void add(std::function<void()> func);

    // Route a coroutine handle to a specific worker (affinity resume).
    void add_to_worker(size_t worker_id, std::coroutine_handle<> h);

    // ── Lifecycle ──

    void start();
    void stop();
    void join();

    // ── Query ──

    size_t num_workers() const noexcept { return workers_.size(); }
    WorkerState& worker(size_t idx) { return *workers_[idx]; }
    const WorkerState& worker(size_t idx) const { return *workers_[idx]; }

    // Create a RouteFunc for affinity primitives (Baton, Mutex)
    adapt::RouteFunc make_route_func();

    // Static: current worker on this executor (thread-local)
    static size_t current_worker_id();
    static WorkStealingExecutor* current_executor();

private:
    void worker_loop(WorkerState& ws);

    Config cfg_;
    std::vector<std::unique_ptr<WorkerState>> workers_;

    // Global MPMC ring for external submissions (via add()).
    std::unique_ptr<RingWorkQueue> global_entry_;

    // TLS
    static thread_local size_t tl_worker_id_;
    static thread_local WorkStealingExecutor* tl_executor_;

    // NUMA peer lists
    std::vector<std::vector<size_t>> numa_peers_;  // per worker: peer indices

};

}  // namespace storage::runtime
