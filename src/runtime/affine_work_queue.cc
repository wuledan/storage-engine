#include "affine_work_queue.h"

namespace storage::runtime {

AffineWorkQueue::AffineWorkQueue(QueueType type, Priority priority, std::string name,
                                 size_t capacity)
    : queue_(capacity), type_(type), priority_(priority), name_(std::move(name)) {}

void AffineWorkQueue::enqueue(WorkItem item) noexcept {
    queue_.enqueue(item);
}

bool AffineWorkQueue::try_dequeue(WorkItem& item) noexcept {
    return queue_.dequeue(item);
}

size_t AffineWorkQueue::try_dequeue_batch(WorkItem* items, size_t max_count) noexcept {
    return queue_.dequeue_bulk(items, max_count);
}

size_t AffineWorkQueue::approx_count() const noexcept {
    return queue_.count();
}

}  // namespace storage::runtime
