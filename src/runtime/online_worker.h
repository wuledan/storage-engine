#pragma once
#include "worker.h"
#include "dispatch_types.h"
#include "local_work_queue.h"
#include "affine_work_queue.h"
#include "ring_work_queue.h"
#include "io/io_backend.h"
#include "io/io_engine.h"
#include <functional>
#include <folly/coro/Task.h>
#include "adapt/affinity_baton.h"

namespace storage::runtime {

// 任务分发模式
enum class TaskDispatchMode : uint8_t {
    kIndirect,  // 通过 MPMC/Affine 队列间接分发（默认）
    kDirect,    // 直接写入 Local 队列
};

class OnlineWorker : public Worker {
public:
    explicit OnlineWorker(const Worker::Config& cfg);

    // 向特定队列投递任务
    void submit_engine(WorkItem item);
    void submit_net_io(WorkItem item);
    void submit_disk_io(WorkItem item);

    // 向 affine queue 投递协程句柄
    void enqueue_affine(std::coroutine_handle<> h) override;

    // 创建 RouteFunc，捕获 this 用于 enqueue_affine 路由
    adapt::RouteFunc make_route_func();

    // 热替换 engine 队列（用于 dispatch 策略切换）
    // 排空旧队列任务，迁移到新队列，更新 dispatch_mode_
    bool swap_engine_queue(std::unique_ptr<WorkQueue> new_queue) {
        auto* old_q = get_queue(idx_engine_);
        if (!old_q) return false;

        // 排空旧队列
        WorkItem item;
        while (old_q->try_dequeue(item)) {
            if (auto* lq = dynamic_cast<LocalWorkQueue*>(new_queue.get())) {
                lq->try_enqueue(std::move(item));
            } else if (auto* rq = dynamic_cast<RingWorkQueue*>(new_queue.get())) {
                rq->enqueue(std::move(item));
            } else if (auto* aq = dynamic_cast<AffineWorkQueue*>(new_queue.get())) {
                aq->enqueue(std::move(item));
            }
        }

        // 替换
        bool ok = scheduler().replace_queue(idx_engine_, std::move(new_queue));
        if (ok) {
            // 根据新队列语义自动设置 dispatch 模式
            auto* q = get_queue(idx_engine_);
            if (q && q->semantic() == QueueSemantic::kLocal) {
                dispatch_mode_ = TaskDispatchMode::kDirect;
            } else {
                dispatch_mode_ = TaskDispatchMode::kIndirect;
            }
        }
        return ok;
    }

    TaskDispatchMode dispatch_mode() const { return dispatch_mode_; }

    // ── IO Backend ──
    void init_io_backend(const io::IOBackendConfig& cfg);
    io::IIOBackend* io_backend() { return io_backend_.get(); }

    // 协程友好的 IO API
    folly::coro::Task<io::IOCompletion> co_read(
        int fd, uint64_t offset, void* buf, size_t len);
    folly::coro::Task<io::IOCompletion> co_write(
        int fd, uint64_t offset, const void* buf, size_t len);

    // 队列索引（在构造函数中注册，公开供外部轮询器和测试使用）
    size_t idx_engine_{0};
    size_t idx_net_io_{0};
    size_t idx_disk_io_{0};
    size_t idx_affine_{0};
    size_t idx_timer_{0};

    size_t default_queue_idx() const noexcept override { return idx_engine_; }

    std::unique_ptr<io::IIOBackend> io_backend_;

private:
    TaskDispatchMode dispatch_mode_{TaskDispatchMode::kDirect};
};

}  // namespace storage::runtime
