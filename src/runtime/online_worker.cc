#include "online_worker.h"
#include "affine_work_queue.h"
#include "batched_spsc_work_queue.h"
#include "local_work_queue.h"
#include "policy_factory.h"

namespace storage::runtime {

OnlineWorker::OnlineWorker(const Worker::Config& cfg)
    : Worker(cfg) {
    idx_affine_ = add_queue(std::make_unique<AffineWorkQueue>(
        QueueType::kAffine, Priority::kCritical, "affine"));
    idx_net_io_ = add_queue(std::make_unique<BatchedSPSCWorkQueue>(
        QueueType::kNetIO, Priority::kHigh, "net_io"));
    idx_disk_io_ = add_queue(std::make_unique<BatchedSPSCWorkQueue>(
        QueueType::kDiskIO, Priority::kMedium, "disk_io"));
    idx_engine_ = add_queue(std::make_unique<LocalWorkQueue>(
        QueueType::kEngine, Priority::kMedium, "engine"));
    idx_timer_ = add_queue(std::make_unique<LocalWorkQueue>(
        QueueType::kTimer, Priority::kHigh, "timer"));
    set_policy(make_policy(PolicyConfig{"strict_priority"}));
}

void OnlineWorker::submit_engine(WorkItem item) {
    auto* q = static_cast<LocalWorkQueue*>(get_queue(idx_engine_));
    if (q) {
        q->try_enqueue(std::move(item));
    }
}

void OnlineWorker::submit_net_io(WorkItem item) {
    auto* q = static_cast<BatchedSPSCWorkQueue*>(get_queue(idx_net_io_));
    if (q) {
        q->push_batch(&item, 1);
    }
}

void OnlineWorker::submit_disk_io(WorkItem item) {
    auto* q = static_cast<BatchedSPSCWorkQueue*>(get_queue(idx_disk_io_));
    if (q) {
        q->push_batch(&item, 1);
    }
}

}  // namespace storage::runtime
