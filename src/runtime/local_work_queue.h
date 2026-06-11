#pragma once
#include "work_queue.h"
#include "local_queue.h"
#include <string>

namespace storage::runtime {

class LocalWorkQueue : public WorkQueue {
public:
    explicit LocalWorkQueue(QueueType type, Priority priority, std::string name,
                            size_t capacity = 1024);

    bool try_enqueue(WorkItem item) noexcept;

    void enqueue(WorkItem item) noexcept override {
        try_enqueue(std::move(item));
    }

    bool try_dequeue(WorkItem& item) noexcept override;
    size_t try_dequeue_batch(WorkItem* items, size_t max_count) noexcept override;
    size_t approx_count() const noexcept override;

    QueueType type() const noexcept override { return type_; }
    Priority base_priority() const noexcept override { return priority_; }
    QueueSemantic semantic() const noexcept override { return QueueSemantic::kLocal; }
    const char* name() const noexcept override { return name_.c_str(); }

private:
    LocalQueue<WorkItem> queue_;
    QueueType type_;
    Priority priority_;
    std::string name_;
};

}  // namespace storage::runtime
