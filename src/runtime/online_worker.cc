#include "online_worker.h"
#include "ring_work_queue.h"
#include "mpmc_ring.h"
#include "ring_work_queue.h"
#include "batched_spsc_work_queue.h"
#include "local_work_queue.h"
#include "policy_factory.h"
#include "io/io_engine.h"
#include "yield_awaiter.h"

namespace storage::runtime {

namespace {

// ── IO poll coroutine type ──
struct IOCoroTask {
    struct promise_type {
        IOCoroTask get_return_object() {
            return IOCoroTask{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        void unhandled_exception() noexcept { std::terminate(); }
    };
    std::coroutine_handle<promise_type> handle;
};

// ── Persistent IO poll coroutine ──
// Suspends via co_await yield_to(idx_disk_io_) — re-enqueues handle to P1.
// Scheduler resumes via drain_all → P1 dequeue → handle.resume().
static IOCoroTask io_poll_coro_fn(OnlineWorker* w) {
    io::IOCompletion io_comps[64];
    while (true) {
        auto* backend = w->io_backend();
        if (!backend) co_return;

        backend->flush_pending();
        backend->flush_submissions();
        size_t n = backend->poll(io_comps, 64);
        for (size_t j = 0; j < n; ++j) {
            if (io_comps[j].callback) {
                io_comps[j].callback(io_comps[j]);
            }
        }

        // Suspend: handle re-enqueued to P1, Scheduler will resume us
        co_await yield_to(w->idx_disk_io_);
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
    if (q) {
        q->enqueue(WorkItem::make_coro(h));
        // notify() removed — busy_poll Scheduler always awake
    }
}

adapt::RouteFunc OnlineWorker::make_route_func() {
    return [this](size_t /*worker_id*/, std::coroutine_handle<> h) {
        this->enqueue_affine(h);
    };
}

// ── IO Backend ──

void OnlineWorker::init_io_backend(const io::IOBackendConfig& cfg) {
    io_backend_ = io::IOEngine::create(cfg, make_route_func());
    scheduler().set_io_backend(io_backend_.get());

    // Launch persistent IO poll coroutine in P1 (disk_io) queue
    auto io_coro = io_poll_coro_fn(this);
    (void)io_coro;
}

folly::coro::Task<io::IOCompletion> OnlineWorker::co_read(
    int fd, uint64_t offset, void* buf, size_t len) {
    if (!io_backend_) {
        throw std::runtime_error("IO backend not initialized");
    }
    co_return co_await io_backend_->co_read(fd, offset, buf, len);
}

folly::coro::Task<io::IOCompletion> OnlineWorker::co_write(
    int fd, uint64_t offset, const void* buf, size_t len) {
    if (!io_backend_) {
        throw std::runtime_error("IO backend not initialized");
    }
    co_return co_await io_backend_->co_write(fd, offset, buf, len);
}

}  // namespace storage::runtime
