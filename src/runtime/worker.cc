#include "worker.h"
#include "online_worker.h"
#include "policy_factory.h"
#include "affinity_baton.h"
#include "work_stealing_executor.h"
#include "worker_registry.h"
#include "timer.h"
#include <hwloc.h>

namespace storage::runtime {

namespace {
thread_local size_t tls_worker_id = SIZE_MAX;
thread_local Worker* tls_current_worker = nullptr;

// Default worker-id resolver (returns "no affinity")
size_t default_worker_id() { return SIZE_MAX; }
}

// Define the function pointer declared in affinity_baton.h.
// adapt/*.cc files are excluded from this build, so we provide the
// definition here instead.
namespace adapt::detail {
    size_t (*get_current_worker_id)() = default_worker_id;
}

// ── AffinityBaton method implementations ──
// adapt/*.cc files are excluded from the build, so we also provide
// the AffinityBaton method bodies here (they would normally live in
// the dead affinity_baton.cc, which included a quant_invest header).
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
// The dead affinity_mutex.cc was excluded (needed quant_invest headers),
// so we provide the bodies here.
size_t AffinityMutex::current_worker_id() {
    return detail::get_current_worker_id();
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

// ── AffinityBaton::TimedAwaiter::await_suspend ──
// Registers with both the baton waiter chain AND the timer system.
// Timer expiry calls on_expire which sets timed_out and posts the baton.
void AffinityBaton::TimedAwaiter::await_suspend(std::coroutine_handle<> h) noexcept {
    node.handle = h;
    node.worker_id = detail::get_current_worker_id();
    node.next = nullptr;

    // 1. Register with baton (same CAS chain logic as Awaiter)
    auto* old = baton.waiters_.load(std::memory_order_acquire);
    do {
        if (reinterpret_cast<uintptr_t>(old) & kPostedBit) {
            // Already posted — don't suspend
            result = WaitResult::kSignaled;
            h.resume();
            return;
        }
        node.next = clear_posted(old);
    } while (!baton.waiters_.compare_exchange_weak(
        old, &node,
        std::memory_order_release,
        std::memory_order_acquire));

    // 2. Zero / negative timeout → immediate timeout
    if (timeout <= Duration::zero()) {
        state->timed_out.store(true, std::memory_order_release);
        baton.post_direct();
        return;
    }

    // 3. Create route function (for timer callback — preserves affinity)
    RouteFunc route;
    size_t source_worker = SIZE_MAX;
    if (auto* ow = current_online_worker()) {
        route = ow->make_route_func();
    } else if (auto* exec = WorkStealingExecutor::current_executor()) {
        route = exec->make_route_func();
        source_worker = WorkerRegistry::instance().get_global_id_for_offline(
            exec, node.worker_id);
    }
    // If neither, route stays empty → post_direct fallback

    // 4. Register with timer system
    Clock::time_point deadline = Clock::now() + timeout;

    TimerNode tn;
    tn.deadline = deadline;
    tn.handle = h;
    tn.source_queue_idx = (tls_source_queue_idx != SIZE_MAX)
        ? tls_source_queue_idx : SIZE_MAX;
    tn.source_worker_id = source_worker;

    auto state_copy = state;
    tn.on_expire = [state_copy, route = std::move(route), &baton = this->baton]() {
        state_copy->timed_out.store(true, std::memory_order_release);
        if (route) {
            baton.post(route);
        } else {
            baton.post_direct();
        }
    };

    if (auto* ow = current_online_worker()) {
        ow->timer_state().insert(std::move(tn));
    } else if (WorkStealingExecutor::current_executor()) {
        global_offline_timer().insert(std::move(tn));
    } else {
        // No timer context — signal timeout immediately
        state->timed_out.store(true, std::memory_order_release);
        baton.post_direct();
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
        if (cfg_.cpu_id > 0) {
            // 线程内部绑定 CPU + NUMA 内存
            hwloc_topology_t topo;
            hwloc_topology_init(&topo);
            hwloc_topology_load(topo);
            
            hwloc_cpuset_t cpuset = hwloc_bitmap_alloc();
            hwloc_bitmap_set(cpuset, cfg_.cpu_id - 1);
            hwloc_set_cpubind(topo, cpuset, HWLOC_CPUBIND_THREAD);
            hwloc_bitmap_free(cpuset);
            
            if (cfg_.numa_node > 0) {
                hwloc_bitmap_t nodeset = hwloc_bitmap_alloc();
                hwloc_bitmap_set(nodeset, cfg_.numa_node - 1);
                hwloc_set_membind(topo, nodeset,
                                  HWLOC_MEMBIND_BIND,
                                  HWLOC_MEMBIND_THREAD);
                hwloc_bitmap_free(nodeset);
            }
            hwloc_topology_destroy(topo);
        }
        worker_loop();
    });
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

// ── folly::Executor ──
// 遵循 quant::WorkStealingExecutor 模式
// folly::coro::Task 完成时通过此方法调度回 worker 的 P0 队列
namespace {
struct ExecutorTask {
    struct promise_type {
        folly::Func func;
        ExecutorTask get_return_object() {
            return ExecutorTask{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        void unhandled_exception() noexcept { std::terminate(); }
    };
    std::coroutine_handle<promise_type> handle;
};
}  // namespace

void Worker::add(folly::Func func) {
    auto task = [](folly::Func f) -> ExecutorTask {
        f();
        co_return;
    }(std::move(func));
    enqueue_affine(task.handle);
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
    tls_current_worker = this;
    adapt::detail::get_current_worker_id = []() -> size_t { return tls_worker_id; };
    on_worker_start();
    scheduler_.run();
    tls_current_worker = nullptr;
    tls_worker_id = SIZE_MAX;
    adapt::detail::get_current_worker_id = []() -> size_t { return SIZE_MAX; };
}

Worker* current_worker() { return tls_current_worker; }
OnlineWorker* current_online_worker() {
    return dynamic_cast<OnlineWorker*>(tls_current_worker);
}

}  // namespace storage::runtime
