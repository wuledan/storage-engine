#include "online_worker.h"
#include "ring_work_queue.h"
#include "mpmc_ring.h"
#include "ring_work_queue.h"
#include "batched_spsc_work_queue.h"
#include "local_work_queue.h"
#include "policy_factory.h"
#include "io/io_engine.h"

namespace storage::runtime {

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
    notify();
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
        notify();  // 唤醒可能 idle 的 worker
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
