#pragma once
#include "worker.h"

namespace storage::runtime {

class OnlineWorker : public Worker {
public:
    explicit OnlineWorker(const Worker::Config& cfg);

    // 向特定队列投递任务
    void submit_engine(WorkItem item);
    void submit_net_io(WorkItem item);
    void submit_disk_io(WorkItem item);

private:
    // 队列索引（在构造函数中注册）
    size_t idx_engine_{0};
    size_t idx_net_io_{0};
    size_t idx_disk_io_{0};
    size_t idx_affine_{0};
    size_t idx_timer_{0};
};

}  // namespace storage::runtime
