#pragma once
#include "work_queue.h"
#include <folly/concurrency/UnboundedQueue.h>
#include <string>

namespace storage::runtime {

class AffineWorkQueue : public WorkQueue {
public:
    explicit AffineWorkQueue(QueueType type, Priority priority, std::string name,
                             size_t capacity = 8192);

    // 任意线程调用
    void enqueue(WorkItem item) noexcept;

    // Worker 线程调用
    bool try_dequeue(WorkItem& item) noexcept override;
    size_t try_dequeue_batch(WorkItem* items, size_t max_count) noexcept override;

    size_t approx_count() const noexcept override;

    QueueType type() const noexcept override { return type_; }
    Priority base_priority() const noexcept override { return priority_; }
    QueueSemantic semantic() const noexcept override { return QueueSemantic::kMPMC; }
    const char* name() const noexcept override { return name_.c_str(); }

private:
    // folly::UMPMCQueue<WorkItem, false, 6> — false=NonBlocking, 6=segment bits
    folly::UMPMCQueue<WorkItem, false, 6> queue_;
    QueueType type_;
    Priority priority_;
    std::string name_;
};

}  // namespace storage::runtime
