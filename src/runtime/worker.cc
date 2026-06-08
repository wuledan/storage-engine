#include "worker.h"
#include "policy_factory.h"
#include "adapt/affinity_baton.h"
#include <hwloc.h>
#include <folly/coro/BlockingWait.h>

namespace storage::runtime {

namespace {
thread_local size_t tls_worker_id = SIZE_MAX;

// Default worker-id resolver (returns "no affinity")
size_t default_worker_id() { return SIZE_MAX; }
}

// Define the function pointer declared in adapt/affinity_baton.h.
// adapt/*.cc files are excluded from this build, so we provide the
// definition here instead.
namespace adapt::detail {
    size_t (*get_current_worker_id)() = default_worker_id;
}

// ── AffinityBaton method implementations ──
// adapt/*.cc files are excluded from the build, so we also provide
// the AffinityBaton method bodies here (they would normally live in
// adapt/affinity_baton.cc, which includes a quant_invest header).
namespace adapt {

size_t AffinityBaton::current_worker_id() {
    return detail::get_current_worker_id();
}

void AffinityBaton::post(RouteFunc route) {
    auto* old = waiters_.exchange(
        reinterpret_cast<WaiterNode*>(kPostedBit),
        std::memory_order_acq_rel);
    resume_chain(clear_posted(old), &route);
}

void AffinityBaton::post_direct() noexcept {
    auto* old = waiters_.exchange(
        reinterpret_cast<WaiterNode*>(kPostedBit),
        std::memory_order_acq_rel);
    resume_chain(clear_posted(old), nullptr);
}

void AffinityBaton::resume_chain(WaiterNode* waiters, RouteFunc* route) {
    while (waiters) {
        auto* next = waiters->next;
        auto handle = waiters->handle;
        auto worker_id = waiters->worker_id;

        if (route && worker_id != SIZE_MAX) {
            (*route)(worker_id, handle);
        } else {
            handle.resume();
        }

        waiters = next;
    }
}

// ── AffinityMutex method implementations ──
// adapt/affinity_mutex.cc is excluded (needs quant_invest headers),
// so we provide the bodies here.
size_t AffinityMutex::current_worker_id() {
    return detail::get_current_worker_id();
}

folly::coro::Task<AffinityMutex::AffinityMutexLock>
AffinityMutex::co_scoped_lock() {
    co_await co_lock();
    co_return AffinityMutexLock{*this};
}

void AffinityMutex::unlock() {
    auto old_state = state_.load(std::memory_order_relaxed);
    Waiter* waiters;

    do {
        waiters = extract_waiters(old_state);
        uintptr_t new_state;
        if (waiters) {
            auto* next = waiters->next;
            new_state = next
                ? (reinterpret_cast<uintptr_t>(next) | kLockedFlag)
                : 0;
        } else {
            new_state = 0;
        }
        if (state_.compare_exchange_weak(
                old_state, new_state,
                std::memory_order_release,
                std::memory_order_relaxed)) {
            break;
        }
    } while (true);

    if (waiters) {
        if (route_ && waiters->worker_id != SIZE_MAX) {
            route_(waiters->worker_id, waiters->handle);
        } else {
            waiters->handle.resume();
        }
    }
}

}  // namespace adapt

std::atomic<size_t> Worker::next_id_{0};

Worker::Worker(Config cfg)
    : id_(next_id_++),
      cfg_(std::move(cfg)),
      idle_(std::make_unique<AdaptiveIdle>(cfg_.idle_cfg)),
      policy_(make_policy(cfg_.policy_cfg)),
      scheduler_(policy_.get(), idle_.get()),
      mem_pool_(std::make_unique<adapt::QuantMemoryResource>()),
      work_item_pool_(std::make_unique<adapt::ObjectPool<WorkItem>>()),
      perf_(id_) {
    scheduler_.set_perf(&perf_);
}

size_t Worker::add_queue(std::unique_ptr<WorkQueue> q) {
    return scheduler_.register_queue(std::move(q));
}

void Worker::set_policy(std::unique_ptr<SchedulingPolicy> p) {
    scheduler_.set_policy(p.get());
    policy_ = std::move(p);
}

void Worker::start() {
    scheduler_.reset_stop();
    thread_ = std::thread([this] {
        worker_loop();
    });
    if (cfg_.cpu_id > 0) {
        hwloc_topology_t topo;
        hwloc_topology_init(&topo);
        hwloc_topology_load(topo);
        hwloc_cpuset_t cpuset = hwloc_bitmap_alloc();
        hwloc_bitmap_set(cpuset, cfg_.cpu_id - 1);
        hwloc_set_cpubind(topo, cpuset, HWLOC_CPUBIND_THREAD);
        hwloc_bitmap_free(cpuset);
        hwloc_topology_destroy(topo);
    }
}

void Worker::stop() {
    scheduler_.request_stop();
}

void Worker::join() {
    if (thread_.joinable()) {
        thread_.join();
    }
}

void Worker::notify() {
    idle_->notify();
}

WorkerStats Worker::stats() const {
    WorkerStats s;
    const auto& sched_stats = scheduler_.stats();
    s.tasks_executed = sched_stats.total_tasks_executed;
    s.total_exec_ns = sched_stats.total_exec_ns;
    s.total_polls = sched_stats.total_polls;
    s.total_idles = sched_stats.total_idles;
    return s;
}

void Worker::worker_loop() {
    tls_worker_id = id_;
    adapt::detail::get_current_worker_id = []() -> size_t { return tls_worker_id; };
    on_worker_start();
    folly::coro::blockingWait(scheduler_.run());
    tls_worker_id = SIZE_MAX;
    adapt::detail::get_current_worker_id = []() -> size_t { return SIZE_MAX; };
}

}  // namespace storage::runtime
