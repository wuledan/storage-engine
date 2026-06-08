#include "online_worker.h"
#include "affine_work_queue.h"
#include "batched_spsc_work_queue.h"
#include "local_work_queue.h"
#include "policy_factory.h"
#include <mutex>

namespace storage::runtime {

namespace {
    // 用于 co_submit_engine 的全局桥接状态
    // WorkItem::Func 是 void(*)()，不支持捕获 lambda，
    // 因此通过此全局 slot + 静态函数桥接。
    // 使用 mutex 保护，测试场景为顺序调用，无并发冲突。
    std::function<void()> g_engine_task;
    std::mutex g_engine_task_mutex;

    // 静态函数指针，作为 WorkItem::Func 提交
    void run_engine_task() {
        std::function<void()> task;
        {
            std::lock_guard<std::mutex> lock(g_engine_task_mutex);
            task = std::move(g_engine_task);
        }
        if (task) task();
    }
}

OnlineWorker::OnlineWorker(const Worker::Config& cfg)
    : Worker(cfg) {
    // Warmup 内存池
    memory_resource().warmup(4 * 1024 * 1024);  // 4MB
    work_item_pool().warmup(1024);
    idx_affine_ = add_queue(std::make_unique<AffineWorkQueue>(
        QueueType::kAffine, Priority::kCritical, "affine"));
    idx_net_io_ = add_queue(std::make_unique<BatchedSPSCWorkQueue>(
        QueueType::kNetIO, Priority::kHigh, "net_io"));
    idx_disk_io_ = add_queue(std::make_unique<BatchedSPSCWorkQueue>(
        QueueType::kDiskIO, Priority::kMedium, "disk_io"));
    idx_engine_ = add_queue(std::make_unique<LocalWorkQueue>(
        QueueType::kEngine, Priority::kMedium, "engine", 200000));
    idx_timer_ = add_queue(std::make_unique<LocalWorkQueue>(
        QueueType::kTimer, Priority::kHigh, "timer"));
    set_policy(make_policy(PolicyConfig{"strict_priority"}));
}

void OnlineWorker::submit_engine(WorkItem item) {
    item.enqueue_ns = perf().record_enqueue(QueueType::kEngine, item.trace_id);
    auto* q = get_queue(idx_engine_);
    if (!q) return;

    if (dispatch_mode_ == TaskDispatchMode::kDirect) {
        if (auto* lq = dynamic_cast<LocalWorkQueue*>(q)) {
            lq->try_enqueue(std::move(item));
        }
    } else {
        // Indirect: default path via BatchedSPSCWorkQueue / AffineWorkQueue
        if (auto* bq = dynamic_cast<BatchedSPSCWorkQueue*>(q)) {
            bq->push_batch(&item, 1);
        } else if (auto* aq = dynamic_cast<AffineWorkQueue*>(q)) {
            aq->enqueue(std::move(item));
            notify();
        }
    }
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
    auto* q = static_cast<AffineWorkQueue*>(get_queue(idx_affine_));
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

folly::coro::Task<void> OnlineWorker::co_submit_engine(std::function<void()> work) {
    auto baton = std::make_shared<adapt::AffinityBaton>();
    auto route = make_route_func();

    // 将 work + baton post 打包为 std::function，通过全局 slot 桥接
    {
        std::lock_guard<std::mutex> lock(g_engine_task_mutex);
        g_engine_task = [baton, work = std::move(work), route = std::move(route)]() {
            work();
            baton->post(route);
        };
    }

    submit_engine(WorkItem::make_func(run_engine_task));
    notify();  // 唤醒 worker，确保引擎队列任务被处理

    co_await *baton;
}

}  // namespace storage::runtime
