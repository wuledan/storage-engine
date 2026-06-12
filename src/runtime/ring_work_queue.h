#pragma once
#include "work_queue.h"
#include "mpmc_ring.h"
#include <string>

namespace storage::runtime {

class RingWorkQueue : public WorkQueue {
public:
    RingWorkQueue(QueueType type, Priority prio, std::string name, size_t cap = 4096)
        : type_(type), priority_(prio), name_(std::move(name)), ring_(cap) {}

    void enqueue(WorkItem item) noexcept override { ring_.enqueue(item); }
    bool try_dequeue(WorkItem& item) noexcept override { return ring_.dequeue(item); }
    bool try_dequeue_mc(WorkItem& item) noexcept { return ring_.dequeue_mc(item); }
    size_t try_dequeue_batch(WorkItem* items, size_t max) noexcept override {
        return ring_.dequeue_bulk(items, max);
    }
    size_t approx_count() const noexcept override { return ring_.count(); }
    QueueType type() const noexcept override { return type_; }
    Priority base_priority() const noexcept override { return priority_; }
    QueueSemantic semantic() const noexcept override { return QueueSemantic::kMPMC; }
    const char* name() const noexcept override { return name_.c_str(); }

private:
    QueueType type_;
    Priority priority_;
    std::string name_;
    MPMCRing<WorkItem> ring_;
};

}  // namespace storage::runtime
