#include "online_worker.h"
#include "ring_work_queue.h"
#include "mpmc_ring.h"
#include "ring_work_queue.h"
#include "batched_spsc_work_queue.h"
#include "local_work_queue.h"
#include "policy_factory.h"
#include "io/io_engine.h"

namespace storage::runtime {

namespace {

// ── Persistent IO poll coroutine ──
// Suspends via yield() which routes back to disk_io (P1) because
// tls_source_queue_idx is set by init_io_backend() before creation.
// Scheduler resumes via P1 dequeue → handle.resume().
//
// Callbacks are invoked inline inside the backend's poll() method,
// using the IORequest::CallbackFn function pointer + context (zero
// heap allocation). The IOCompletion array here only captures result
// data and is no longer used for callback dispatch.
static adapt::Task<void> io_poll_coro_fn(OnlineWorker* w) {
    io::IOCompletion io_comps[64];
    while (true) {
        auto* backend = w->io_backend();
        if (!backend) co_return;

        backend->flush_pending();
        backend->flush_submissions();
        backend->poll(io_comps, 64);
        // Callbacks already invoked inside poll() — no dispatch needed here.

        co_await yield();
    }
}

// ── Timer coroutine ──
// Runs on the timer (P1) queue: drains expired timers and re-enqueues
// the sleeping coroutines to their source queues, then yields.
//
// IMPORTANT: When no timers have expired, we use yield() (which routes
// back to the engine queue P2 via default_queue_idx()) instead of
// yield_to(idx_timer_) (which stays in P1).  This prevents the timer
// coroutine from starving lower-priority queues (P2/P3) under the
// StrictPriority scheduling policy: without this, the persistent timer
// loop keeps P1 non-empty forever and P2/P3 never get scheduled.
static adapt::Task<void> timer_coro_fn(OnlineWorker* w) {
    while (true) {
        auto& ts = w->timer_state();
        auto now = Clock::now();
        auto expired = ts.expire(now);

        if (!expired.empty()) {
            for (auto& node : expired) {
                if (node.on_expire) {
                    // Timed baton wait: callback sets timed_out and posts baton
                    node.on_expire();
                } else {
                    size_t qidx = (node.source_queue_idx != SIZE_MAX)
                        ? node.source_queue_idx : w->idx_engine_;
                    auto* q = w->get_queue(qidx);
                    if (q) {
                        q->enqueue(WorkItem::make_coro(node.handle));
                    }
                }
            }

            // More timers may have expired while we processed; stay in P1
            // for a prompt re-check.
            co_await yield_to(w->idx_timer_);
        } else {
            // No expired timers → yield to lower-priority queues so they
            // are not starved.  yield() uses default_queue_idx() (engine
            // queue / P2) when tls_source_queue_idx is unset, which is the
            // case in the non-busy_poll scheduler path.
            co_await yield();
        }
    }
}

}  // anonymous namespace

OnlineWorker::OnlineWorker(const Worker::Config& cfg)
    : Worker(cfg) {
    // Warmup 内存池
    memory_resource().warmup(4 * 1024 * 1024);  // 4MB
    work_item_pool().warmup(1024);
    idx_affine_ = add_queue(std::make_unique<RingWorkQueue>(
        QueueType::kAffine, Priority::kCritical, "affine", 65536));
    scheduler().set_affine_idx(idx_affine_);
    set_affine_q_idx(idx_affine_);
    idx_net_io_ = add_queue(std::make_unique<BatchedSPSCWorkQueue>(
        QueueType::kNetIO, Priority::kHigh, "net_io"));
    idx_disk_io_ = add_queue(std::make_unique<BatchedSPSCWorkQueue>(
        QueueType::kDiskIO, Priority::kHigh, "disk_io"));
    scheduler().set_disk_io_idx(idx_disk_io_);
    idx_engine_ = add_queue(std::make_unique<RingWorkQueue>(
        QueueType::kEngine, Priority::kMedium, "engine", 65536));
    idx_timer_ = add_queue(std::make_unique<AffineWorkQueue>(
        QueueType::kTimer, Priority::kHigh, "timer"));
    set_policy(make_policy(PolicyConfig{"strict_priority"}));

    // Launch timer coroutine in P4 (timer) queue.
    // Set TLS so that co_await yield_to(idx_timer_) inside timer_coro_fn
    // can enqueue the coroutine back to the timer queue even before the
    // worker loop starts (current_worker() returns null during construction).
    tls_source_queue_idx = idx_timer_;
    tls_source_queue = get_queue(idx_timer_);
    auto timer_coro = timer_coro_fn(this);
    tls_source_queue_idx = SIZE_MAX;
    tls_source_queue = nullptr;
    auto timer_h = timer_coro.release();
    timer_h.resume();  // Start the timer loop — will run until yield, then self-manage
}

void OnlineWorker::submit_engine(WorkItem item) {
    item.enqueue_ns = perf().record_enqueue(QueueType::kEngine, item.trace_id);
    auto* q = static_cast<RingWorkQueue*>(get_queue(idx_engine_));
    if (q) q->enqueue(std::move(item));
    // notify() removed — busy_poll Scheduler always awake
}

void OnlineWorker::submit_net_io(WorkItem item) {
    item.enqueue_ns = perf().record_enqueue(QueueType::kNetIO, item.trace_id);
    auto* q = static_cast<BatchedSPSCWorkQueue*>(get_queue(idx_net_io_));
    if (q) {
        q->push_batch(&item, 1);
    }
}

void OnlineWorker::submit_disk_io(WorkItem item) {
    item.enqueue_ns = perf().record_enqueue(QueueType::kDiskIO, item.trace_id);
    auto* q = static_cast<BatchedSPSCWorkQueue*>(get_queue(idx_disk_io_));
    if (q) {
        q->push_batch(&item, 1);
    }
}

void OnlineWorker::enqueue_affine(std::coroutine_handle<> h) {
    auto* q = static_cast<RingWorkQueue*>(get_queue(idx_affine_));
    assert(q != nullptr && "affine queue must exist in OnlineWorker");
    q->enqueue(WorkItem::make_coro(h));
    // notify() removed — busy_poll Scheduler always awake
}

adapt::RouteFunc OnlineWorker::make_route_func() {
    return adapt::RouteFunc{
        [](void* ctx, size_t /*worker_id*/, std::coroutine_handle<> h) {
            static_cast<OnlineWorker*>(ctx)->enqueue_affine(h);
        },
        this
    };
}

// ── IO Backend ──

void OnlineWorker::init_io_backend(const io::IOBackendConfig& cfg) {
    // RouteFn has been removed from IIOBackend. co_read/co_write use
    // each waiter's own route saved in the baton node. No routing
    // function needed at the backend level.
    io_backend_ = io::IOEngine::create(cfg);
    scheduler().set_io_backend(io_backend_.get());

    // Set tls_source_queue_idx + tls_source_queue so yield() inside
    // io_poll_coro_fn routes back to disk_io (P1) — avoids any
    // current_worker() dependency since this runs on the test/main thread
    // before the worker loop starts.
    tls_source_queue_idx = idx_disk_io_;
    tls_source_queue = get_queue(idx_disk_io_);
    auto io_coro = io_poll_coro_fn(this);
    tls_source_queue_idx = SIZE_MAX;  // reset
    tls_source_queue = nullptr;
    auto io_h = io_coro.release();
    io_h.resume();  // Start the IO poll loop — will run until yield, then self-manage
}

adapt::Task<io::IOCompletion> OnlineWorker::co_read(
    int fd, uint64_t offset, void* buf, size_t len) {
    if (!io_backend_) {
        throw std::runtime_error("IO backend not initialized");
    }
    co_return co_await io_backend_->co_read(fd, offset, buf, len);
}

adapt::Task<io::IOCompletion> OnlineWorker::co_write(
    int fd, uint64_t offset, const void* buf, size_t len) {
    if (!io_backend_) {
        throw std::runtime_error("IO backend not initialized");
    }
    co_return co_await io_backend_->co_write(fd, offset, buf, len);
}

}  // namespace storage::runtime
