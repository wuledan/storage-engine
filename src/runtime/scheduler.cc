#include "scheduler.h"
#include "metric_counter.h"
#include <chrono>
#include <algorithm>

namespace storage::runtime {

namespace {
    metric::MetricCounter g_sched_iters;
    metric::MetricCounter g_drain_p0_ns;
    metric::MetricCounter g_drain_all_ns;
    metric::MetricCounter g_io_poll_ns;
    metric::MetricCounter g_drain_p0b_ns;
}

static uint64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

thread_local size_t tls_source_queue_idx = SIZE_MAX;
thread_local WorkQueue* tls_source_queue = nullptr;

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
    // Check perf level once outside the loop to avoid repeated atomic reads.
    auto lv = perf_ ? perf_->level() : PerfLevel::kNone;
    while (true) {
        size_t n = q->try_dequeue_batch(batch.data(), max_batch);
        if (n == 0) break;
        last_poll_times_[affine_idx_] = now_ns();
        for (size_t i = 0; i < n; ++i) {
            tls_source_queue_idx = affine_idx_;
            tls_source_queue = q.get();
            if (lv == PerfLevel::kNone) {
                // Fast path: no timing, just count
                batch[i].execute();
                stats_.total_tasks_executed++;
            } else {
                auto t0 = now_ns();
                batch[i].execute();
                auto exec_ns = now_ns() - t0;
                stats_.total_tasks_executed++;
                stats_.total_exec_ns += exec_ns;
                if (perf_ && lv >= PerfLevel::kTrace) {
                    perf_->record_exec(q->type(), exec_ns);
                }
            }
        }
        total_dequeued_[affine_idx_] += n;
    }
}

void Scheduler::run() {
    if (busy_poll_) { run_busy(); return; }

    running_.store(true, std::memory_order_release);
    if (stop_requested_.load(std::memory_order_acquire)) {
        running_.store(false, std::memory_order_release);
        if (idle_) idle_->notify();
        return;
    }

    constexpr size_t kMaxBatchSize = 64;
    std::vector<QueueSnapshot> snapshots;
    snapshots.reserve(queues_.size());
    std::vector<WorkItem> batch(kMaxBatchSize);

    while (running_.load(std::memory_order_acquire)) {
        uint64_t t_iter = __builtin_ia32_rdtsc();

        // P0: baton.post activated producers
        drain_p0(batch, kMaxBatchSize);
        uint64_t t1 = __builtin_ia32_rdtsc();

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
        uint64_t t4 = __builtin_ia32_rdtsc();

        // ── Step 3: 全空 → idle ──
        if (decision.idle) {
            // IO backend 活跃时持续 polling，不进入 idle 阻塞 Worker 线程
            if (io_backend_) {
                continue;
            }
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
        uint64_t t5 = __builtin_ia32_rdtsc();

        // 累计探针 (no inline IO poll — handled by coroutine via P1 drain)
        probe.io_poll     += 0;
        probe.drain_p0    += t1 - t_iter;
        probe.snapshot    += t4 - t1;
        probe.drain_p0b   += t5 - t4;
        probe.iter        += t5 - t_iter;
        probe_count++;
    }
}

void Scheduler::request_stop() {
    stop_requested_.store(true, std::memory_order_release);
    running_.store(false, std::memory_order_release);
    if (idle_) idle_->notify();
}

void Scheduler::run_busy() {
    running_.store(true, std::memory_order_release);
    if (stop_requested_.load(std::memory_order_acquire)) {
        running_.store(false, std::memory_order_release);
        return;
    }

    constexpr size_t kMaxBatchSize = 64;
    std::vector<WorkItem> p0_batch(kMaxBatchSize);

    while (running_.load(std::memory_order_acquire)) {
        uint64_t t_iter = __builtin_ia32_rdtsc();

        // P0: baton.post activated producers (always first)
        drain_p0(p0_batch, kMaxBatchSize);
        uint64_t t1 = __builtin_ia32_rdtsc();

        // P1: disk IO coroutine (poll CQEs, fire callbacks, then re-enqueue)
        auto lv = perf_ ? perf_->level() : PerfLevel::kNone;
        if (disk_io_idx_ < queues_.size()) {
            std::vector<WorkItem> io_batch(kMaxBatchSize);
            size_t n = queues_[disk_io_idx_]->try_dequeue_batch(io_batch.data(), kMaxBatchSize);
            for (size_t i = 0; i < n; ++i) {
                tls_source_queue_idx = disk_io_idx_;
                tls_source_queue = queues_[disk_io_idx_].get();
                if (lv == PerfLevel::kNone) {
                    io_batch[i].execute();
                    stats_.total_tasks_executed++;
                } else {
                    auto t0 = now_ns();
                    io_batch[i].execute();
                    auto exec_ns = now_ns() - t0;
                    stats_.total_tasks_executed++;
                    stats_.total_exec_ns += exec_ns;
                    if (lv >= PerfLevel::kTrace) {
                        perf_->record_exec(queues_[disk_io_idx_]->type(), exec_ns);
                    }
                }
            }
        }
        // Then: engine, net_io, timer (everything else)
        std::vector<WorkItem> other_batch(kMaxBatchSize);
        for (size_t qi = 0; qi < queues_.size(); ++qi) {
            if (qi == affine_idx_ || qi == disk_io_idx_) continue;
            size_t n = queues_[qi]->try_dequeue_batch(other_batch.data(), kMaxBatchSize);
            if (n == 0) continue;
            auto* qp = queues_[qi].get();
            for (size_t i = 0; i < n; ++i) {
                tls_source_queue_idx = qi;
                tls_source_queue = qp;
                if (lv == PerfLevel::kNone) {
                    other_batch[i].execute();
                    stats_.total_tasks_executed++;
                } else {
                    auto t0 = now_ns();
                    other_batch[i].execute();
                    auto exec_ns = now_ns() - t0;
                    stats_.total_tasks_executed++;
                    stats_.total_exec_ns += exec_ns;
                    if (lv >= PerfLevel::kTrace) {
                        perf_->record_exec(qp->type(), exec_ns);
                    }
                }
            }
        }
        uint64_t t2 = __builtin_ia32_rdtsc();

        // Second P0 drain (tasks may have produced new P0 work)
        drain_p0(p0_batch, kMaxBatchSize);
        uint64_t t3 = __builtin_ia32_rdtsc();

        // Probe stats (matching run() semantics)
        probe.drain_p0    += t1 - t_iter;
        probe.io_poll     += t2 - t1;       // IO + other queue processing
        probe.drain_p0b   += t3 - t2;       // Second P0 drain
        probe.iter        += t3 - t_iter;
        probe_count++;
        g_sched_iters << 1;
        g_drain_p0_ns << ((t1 - t_iter) / 3);   // rdtsc → ns (approx 3GHz)
        g_drain_all_ns << ((t2 - t1) / 3);
        g_io_poll_ns << ((t2 - t1) / 3);        // same as drain_all for backward compat
        g_drain_p0b_ns << ((t3 - t2) / 3);      // second P0 drain time
        // Cycle timing available via metrics API:
        //   avg_drain_p0  = g_drain_p0_ns  / g_sched_iters
        //   avg_io_poll   = g_io_poll_ns   / g_sched_iters
        //   avg_drain_p0b = g_drain_p0b_ns / g_sched_iters
    }
}

void register_scheduler_metrics() {
    using namespace metric;
    MetricRegistry::instance().register_counter("scheduler/iters", &g_sched_iters);
    MetricRegistry::instance().register_counter("scheduler/drain_p0_ns", &g_drain_p0_ns);
    MetricRegistry::instance().register_counter("scheduler/drain_all_ns", &g_drain_all_ns);
    MetricRegistry::instance().register_counter("scheduler/io_poll_ns", &g_io_poll_ns);
    MetricRegistry::instance().register_counter("scheduler/drain_p0b_ns", &g_drain_p0b_ns);
}

}  // namespace storage::runtime
