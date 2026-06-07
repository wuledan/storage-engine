#include "affine_work_queue.h"

namespace storage::runtime {

AffineWorkQueue::AffineWorkQueue(QueueType type, Priority priority, std::string name,
                                 size_t /*capacity*/)
    : type_(type), priority_(priority), name_(std::move(name)) {}

void AffineWorkQueue::enqueue(WorkItem item) noexcept {
    queue_.enqueue(std::move(item));
}

bool AffineWorkQueue::try_dequeue(WorkItem& item) noexcept {
    return queue_.try_dequeue(item);
}

size_t AffineWorkQueue::try_dequeue_batch(WorkItem* items, size_t max_count) noexcept {
    size_t count = 0;
    while (count < max_count && queue_.try_dequeue(items[count])) {
        ++count;
    }
    return count;
}

size_t AffineWorkQueue::approx_count() const noexcept {
    return 0;
}

}  // namespace storage::runtime
