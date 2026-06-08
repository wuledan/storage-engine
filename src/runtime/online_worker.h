#pragma once
#include "worker.h"
#include <functional>
#include <folly/coro/Task.h>
#include "adapt/affinity_baton.h"

namespace storage::runtime {

class OnlineWorker : public Worker {
public:
    explicit OnlineWorker(const Worker::Config& cfg);

    // 向特定队列投递任务
    void submit_engine(WorkItem item);
    void submit_net_io(WorkItem item);
    void submit_disk_io(WorkItem item);

    // 向 affine queue 投递协程句柄
    void enqueue_affine(std::coroutine_handle<> h) override;

    // 提交一个协程任务：投递到 engine 队列执行，完成后自动通过 affine queue 恢复调用方协程
    folly::coro::Task<void> co_submit_engine(std::function<void()> work);

    // 创建 RouteFunc，捕获 this 用于 enqueue_affine 路由
    adapt::RouteFunc make_route_func();

    // 队列索引（在构造函数中注册）
    // protected 以允许测试子类访问
protected:
    size_t idx_engine_{0};
    size_t idx_net_io_{0};
    size_t idx_disk_io_{0};
    size_t idx_affine_{0};
    size_t idx_timer_{0};
};

}  // namespace storage::runtime
