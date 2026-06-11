#pragma once
#include "types.h"
#include "work_item.h"
#include <cstddef>
#include <string>

namespace storage::runtime {

// 工作队列抽象基类
class WorkQueue {
public:
    virtual ~WorkQueue() = default;

    // 非阻塞 enqueue 单个任务
    virtual void enqueue(WorkItem item) noexcept = 0;

    // 非阻塞 dequeue 单个任务
    virtual bool try_dequeue(WorkItem& item) noexcept = 0;

    // 批量 dequeue，返回实际获取数 (≥0, ≤max_count)
    virtual size_t try_dequeue_batch(WorkItem* items, size_t max_count) noexcept = 0;

    // 队列中待处理任务的近似数量
    virtual size_t approx_count() const noexcept = 0;

    // 队列元信息
    virtual QueueType type() const noexcept = 0;
    virtual Priority base_priority() const noexcept = 0;
    virtual QueueSemantic semantic() const noexcept = 0;
    virtual const char* name() const noexcept = 0;

    // 通知有新任务（用于空闲唤醒），默认返回 false
    virtual bool has_pending_notify() noexcept { return false; }
};

}  // namespace storage::runtime
