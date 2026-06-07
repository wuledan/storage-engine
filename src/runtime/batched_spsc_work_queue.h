#pragma once
#include "work_queue.h"
#include "batched_spsc_queue.h"
#include <string>

namespace storage::runtime {

class BatchedSPSCWorkQueue : public WorkQueue {
public:
    explicit BatchedSPSCWorkQueue(QueueType type, Priority priority, std::string name,
                                  size_t capacity = 4096);

    // 生产者（IO Poller 线程调用）
    void push_batch(const WorkItem* items, size_t count) noexcept;

    // 消费者（Worker 线程调用）
    bool try_dequeue(WorkItem& item) noexcept override;        // 从本地缓存取
    size_t try_dequeue_batch(WorkItem* items, size_t max) noexcept override;

    size_t approx_count() const noexcept override;

    QueueType type() const noexcept override { return type_; }
    Priority base_priority() const noexcept override { return priority_; }
    QueueSemantic semantic() const noexcept override { return QueueSemantic::kSPSC; }
    const char* name() const noexcept override { return name_.c_str(); }

private:
    BatchedSPSCQueue<WorkItem> queue_;
    QueueType type_;
    Priority priority_;
    std::string name_;
};

}  // namespace storage::runtime
