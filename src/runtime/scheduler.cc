#include "scheduler.h"
#include <chrono>
#include <algorithm>
#include <folly/coro/Task.h>

namespace storage::runtime {

static uint64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

Scheduler::Scheduler(SchedulingPolicy* policy, AdaptiveIdle* idle)
    : policy_(policy), idle_(idle) {}

size_t Scheduler::register_queue(std::unique_ptr<WorkQueue> queue) {
    size_t idx = queues_.size();
    queues_.push_back(std::move(queue));
    last_poll_times_.push_back(0);
    total_dequeued_.push_back(0);
    return idx;
}

WorkQueue* Scheduler::get_queue(size_t idx) const {
    if (idx < queues_.size()) {
        return queues_[idx].get();
    }
    return nullptr;
}

folly::coro::Task<void> Scheduler::run() {
    // Signal we're running, then check if stop was already requested.
    // Must NOT reset stop_requested_ here — doing so would undo a stop
    // that was already signaled before we started, causing the worker
    // thread to run forever in its loop (nobody calls request_stop again).
    running_.store(true, std::memory_order_release);
    if (stop_requested_.load(std::memory_order_acquire)) {
        running_.store(false, std::memory_order_release);
        if (idle_) idle_->notify();
        co_return;
    }

    constexpr size_t kMaxBatchSize = 64;

    std::vector<QueueSnapshot> snapshots;
    snapshots.reserve(queues_.size());
    std::vector<WorkItem> batch(kMaxBatchSize);

    while (running_.load(std::memory_order_acquire)) {
        snapshots.clear();
        for (size_t i = 0; i < queues_.size(); ++i) {
            QueueSnapshot snap;
            snap.type = queues_[i]->type();
            snap.priority = queues_[i]->base_priority();
            snap.approx_count = queues_[i]->approx_count();
            snap.last_poll_ns = last_poll_times_[i];
            snap.total_dequeued = total_dequeued_[i];
            snapshots.push_back(snap);
        }

        auto decision = policy_->decide(snapshots, stats_);
        stats_.total_polls++;

        if (decision.idle) {
            stats_.total_idles++;
            idle_->enter_idle();
            continue;
        }

        if (decision.queue_index >= queues_.size()) continue;
        auto& queue = queues_[decision.queue_index];
        size_t n = queue->try_dequeue_batch(
            batch.data(), std::min(decision.batch_size, kMaxBatchSize));

        if (n == 0) continue;

        last_poll_times_[decision.queue_index] = now_ns();
        for (size_t i = 0; i < n; ++i) {
            auto t0 = now_ns();
            batch[i].execute();
            auto t1 = now_ns();
            stats_.total_tasks_executed++;
            stats_.total_exec_ns += (t1 - t0);
            policy_->on_task_completed(queue->type(), t1 - t0);
        }
        total_dequeued_[decision.queue_index] += n;
    }

    co_return;
}

void Scheduler::request_stop() {
    stop_requested_.store(true, std::memory_order_release);
    running_.store(false, std::memory_order_release);
    if (idle_) idle_->notify();
}

}  // namespace storage::runtime
