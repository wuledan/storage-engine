#include "local_work_queue.h"

namespace storage::runtime {

LocalWorkQueue::LocalWorkQueue(QueueType type, Priority priority, std::string name,
                               size_t /*capacity*/)
    : type_(type), priority_(priority), name_(std::move(name)) {}

bool LocalWorkQueue::try_enqueue(WorkItem item) noexcept {
    return queue_.try_enqueue(std::move(item));
}

bool LocalWorkQueue::try_dequeue(WorkItem& item) noexcept {
    return queue_.try_dequeue(item);
}

size_t LocalWorkQueue::try_dequeue_batch(WorkItem* items, size_t max_count) noexcept {
    return queue_.try_dequeue_batch(items, max_count);
}

size_t LocalWorkQueue::approx_count() const noexcept {
    return queue_.size();
}

}  // namespace storage::runtime
