#pragma once
#include "perf_histogram.h"
#include "types.h"
#include <atomic>
#include <cstdint>
#include <vector>

namespace storage::runtime {

// ── Performance monitoring levels ──
enum class PerfLevel : uint8_t {
    kNone  = 0,   // 完全禁用
    kCount = 1,   // 仅计数和总量
    kTrace = 2,   // 记录直方图和时间戳
};

// ── 单队列性能数据 ──
struct QueuePerf {
    uint64_t enqueued_count{0};
    uint64_t total_wait_ns{0};
    uint64_t max_wait_ns{0};
    uint64_t total_exec_ns{0};
    Histogram wait_hist;
    Histogram exec_hist;
};

// ── 队列性能快照（对外输出） ──
struct QueueSnapshotPerf {
    QueueType type;
    uint64_t enqueued;
    uint64_t avg_wait_ns;
    uint64_t max_wait_ns;
    uint64_t avg_exec_ns;
    uint64_t p50_wait_ns;
    uint64_t p99_wait_ns;
};

// ── Worker 性能快照 ──
struct PerfSnapshot {
    size_t worker_id{0};
    std::vector<QueueSnapshotPerf> queues;
    uint64_t snapshot_ns{0};
};

// ── WorkerPerf ──
//
// Per-worker performance counters with three levels of detail.
// Cache-line aligned to avoid false sharing between workers.
class alignas(64) WorkerPerf {
public:
    explicit WorkerPerf(size_t worker_id) : worker_id_(worker_id) {}

    void set_level(PerfLevel level) noexcept {
        level_.store(level, std::memory_order_release);
    }
    PerfLevel level() const noexcept {
        return level_.load(std::memory_order_acquire);
    }

    // 入队记录 — 返回入队时间戳（仅 kTrace 时非零）
    uint64_t record_enqueue(QueueType q, uint32_t trace_id) noexcept {
        (void)trace_id;
        auto lv = level_.load(std::memory_order_acquire);
        if (lv == PerfLevel::kNone) [[likely]] return 0;
        queues_[static_cast<uint8_t>(q)].enqueued_count++;
        if (lv >= PerfLevel::kTrace) {
            return rdtsc_ns();
        }
        return 0;
    }

    // 出队记录
    void record_dequeue(QueueType q, uint64_t enqueue_ns) noexcept {
        auto lv = level_.load(std::memory_order_acquire);
        if (lv == PerfLevel::kNone) [[likely]] return;
        uint64_t now = rdtsc_ns();
        uint64_t wait = (enqueue_ns > 0) ? (now - enqueue_ns) : 0;
        auto& qp = queues_[static_cast<uint8_t>(q)];
        qp.total_wait_ns += wait;
        if (wait > qp.max_wait_ns) qp.max_wait_ns = wait;
        if (lv >= PerfLevel::kTrace && wait > 0) {
            qp.wait_hist.record(wait);
        }
    }

    // 执行记录
    void record_exec(QueueType q, uint64_t exec_ns) noexcept {
        auto lv = level_.load(std::memory_order_acquire);
        if (lv == PerfLevel::kNone) [[likely]] return;
        auto& qp = queues_[static_cast<uint8_t>(q)];
        qp.total_exec_ns += exec_ns;
        if (lv >= PerfLevel::kTrace) {
            qp.exec_hist.record(exec_ns);
        }
    }

    // 生成快照
    PerfSnapshot snapshot() const noexcept {
        PerfSnapshot snap;
        snap.worker_id = worker_id_;
        snap.snapshot_ns = rdtsc_ns();
        for (uint8_t i = 0; i < 8; ++i) {
            auto& qp = queues_[i];
            if (qp.enqueued_count == 0) continue;
            QueueSnapshotPerf qs;
            qs.type = static_cast<QueueType>(i);
            qs.enqueued = qp.enqueued_count;
            qs.max_wait_ns = qp.max_wait_ns;
            qs.avg_wait_ns = qp.enqueued_count > 0
                ? qp.total_wait_ns / qp.enqueued_count : 0;
            qs.avg_exec_ns = qp.enqueued_count > 0
                ? qp.total_exec_ns / qp.enqueued_count : 0;
            qs.p50_wait_ns = static_cast<uint64_t>(qp.wait_hist.percentile(0.50));
            qs.p99_wait_ns = static_cast<uint64_t>(qp.wait_hist.percentile(0.99));
            snap.queues.push_back(qs);
        }
        return snap;
    }

private:
    uint64_t rdtsc_ns() const noexcept {
        return __builtin_ia32_rdtsc();
    }

    size_t worker_id_;
    std::atomic<PerfLevel> level_{PerfLevel::kNone};
    QueuePerf queues_[8];
};

}  // namespace storage::runtime
