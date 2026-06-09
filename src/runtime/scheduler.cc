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

void Scheduler::drain_p0(std::vector<WorkItem>& batch, size_t max_batch) {
    if (affine_idx_ >= queues_.size()) return;
    auto& q = queues_[affine_idx_];
    while (true) {
        size_t n = q->try_dequeue_batch(batch.data(), max_batch);
        if (n == 0) break;
        last_poll_times_[affine_idx_] = now_ns();
        for (size_t i = 0; i < n; ++i) {
            if (batch[i].tag == 1) {
                auto h = batch[i].coro;
                h.resume();
                if (!h.done()) h.resume();
            } else {
                auto t0 = now_ns();
                batch[i].execute();
                auto exec_ns = now_ns() - t0;
                stats_.total_tasks_executed++;
                stats_.total_exec_ns += exec_ns;
                if (perf_) perf_->record_exec(q->type(), exec_ns);
            }
        }
        total_dequeued_[affine_idx_] += n;
    }
}

folly::coro::Task<void> Scheduler::run() {
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
        // ── Step 1: 排空 P0（IO poll 由常驻协程处理） ──
        drain_p0(batch, kMaxBatchSize);

        // ── Step 2: 快照 + 策略决策 ──
        snapshots.clear();
        for (size_t i = 0; i < queues_.size(); ++i) {
            QueueSnapshot snap;
            snap.type = queues_[i]->type();
            snap.priority = queues_[i]->base_priority();
            snap.approx_count = (i == affine_idx_) ? 0 : queues_[i]->approx_count();
            snap.last_poll_ns = last_poll_times_[i];
            snap.total_dequeued = total_dequeued_[i];
            snapshots.push_back(snap);
        }

        auto decision = policy_->decide(snapshots, stats_);
        stats_.total_polls++;

        // ── Step 3: 全空 → idle ──
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
            if (perf_ && batch[i].enqueue_ns > 0) {
                perf_->record_dequeue(queue->type(), batch[i].enqueue_ns);
            }
            auto t0 = now_ns();
            batch[i].execute();
            auto exec_ns = now_ns() - t0;
            stats_.total_tasks_executed++;
            stats_.total_exec_ns += exec_ns;
            if (perf_) {
                perf_->record_exec(queue->type(), exec_ns);
            }
            policy_->on_task_completed(queue->type(), exec_ns);
        }
        total_dequeued_[decision.queue_index] += n;

        // Step 4: 任务执行可能产生新 P0 → 排空
        drain_p0(batch, kMaxBatchSize);
    }

    co_return;
}

void Scheduler::request_stop() {
    stop_requested_.store(true, std::memory_order_release);
    running_.store(false, std::memory_order_release);
    if (idle_) idle_->notify();
}

}  // namespace storage::runtime
