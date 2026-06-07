#include "batched_spsc_work_queue.h"

namespace storage::runtime {

BatchedSPSCWorkQueue::BatchedSPSCWorkQueue(QueueType type, Priority priority,
                                           std::string name, size_t /*capacity*/)
    : type_(type), priority_(priority), name_(std::move(name)) {}

void BatchedSPSCWorkQueue::push_batch(const WorkItem* items, size_t count) noexcept {
    queue_.push_batch(items, count);
}

bool BatchedSPSCWorkQueue::try_dequeue(WorkItem& item) noexcept {
    return queue_.try_dequeue_batch(&item, 1) == 1;
}

size_t BatchedSPSCWorkQueue::try_dequeue_batch(WorkItem* items, size_t max) noexcept {
    return queue_.try_dequeue_batch(items, max);
}

size_t BatchedSPSCWorkQueue::approx_count() const noexcept {
    return queue_.approx_count();
}

}  // namespace storage::runtime
