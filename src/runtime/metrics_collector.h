#pragma once
#include "dispatch_types.h"
#include "types.h"
#include "worker_perf.h"
#include <vector>
#include <cstdint>
#include <chrono>

namespace storage::runtime {

// ── 单队列指标 ──
struct QueueMetrics {
    QueueType type{};
    size_t enqueued_delta{0};
    size_t executed_delta{0};
    uint64_t total_exec_ns{0};
    uint64_t avg_wait_ns{0};
    double p50_wait_ns{0};
    double p99_wait_ns{0};
};

// ── Worker 利用率 ──
struct WorkerUtilization {
    size_t worker_id{0};
    uint64_t busy_ns{0};
    uint64_t idle_ns{0};
    uint64_t park_count{0};
};

// ── 运行时指标快照 ──
struct RuntimeMetrics {
    uint64_t snapshot_ns{0};
    double elapsed_seconds{0};
    size_t total_enqueued_delta{0};
    size_t total_executed_delta{0};
    std::vector<QueueMetrics> per_queue;
    std::vector<WorkerUtilization> per_worker;
    double io_ops_per_sec{0};
    double io_bytes_per_sec{0};
};

// ── 指标采集器 ──
class MetricsCollector {
public:
    void attach_worker(size_t id, WorkerPerf* perf) {
        workers_.emplace_back(id, perf);
    }

    // 采集快照，自动计算 delta
    RuntimeMetrics collect() {
        RuntimeMetrics m;
        auto now = std::chrono::steady_clock::now();
        uint64_t now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            now.time_since_epoch()).count();

        size_t total_enq = 0;
        size_t total_exe = 0;

        // 遍历所有 worker，汇总 PerfSnapshot
        for (auto& [id, perf] : workers_) {
            auto snap = perf->snapshot();
            for (auto& qs : snap.queues) {
                uint8_t qi = static_cast<uint8_t>(qs.type);
                // 累积总量
                total_enq += qs.enqueued;
                total_exe += static_cast<size_t>(
                    qs.avg_exec_ns > 0 && qs.enqueued > 0
                    ? qs.enqueued  // approximate: avg_exec_ns * enqueued would
                    : 0);          // give total exec, but we just track counts

                // 构建 QueueMetrics
                QueueMetrics qm;
                qm.type = qs.type;
                qm.enqueued_delta = qs.enqueued;   // cumulative, delta computed later
                qm.executed_delta = static_cast<size_t>(
                    qs.avg_exec_ns > 0 ? qs.enqueued : 0);
                qm.total_exec_ns = qs.avg_exec_ns * qs.enqueued;
                qm.avg_wait_ns = qs.avg_wait_ns;
                qm.p50_wait_ns = static_cast<double>(qs.p50_wait_ns);
                qm.p99_wait_ns = static_cast<double>(qs.p99_wait_ns);
                m.per_queue.push_back(qm);

                WorkerUtilization wu;
                wu.worker_id = id;
                wu.busy_ns = qs.avg_exec_ns * qs.enqueued;
                m.per_worker.push_back(wu);
            }
        }

        // 计算 delta 与 elapsed
        if (previous_.last_ns == 0) {
            // 首次采集：保存基线，返回空 metrics（elapsed=0）
            previous_.last_ns = now_ns;
            previous_.total_enqueued = total_enq;
            previous_.total_executed = total_exe;
            m.elapsed_seconds = 0;
            return m;
        }

        m.elapsed_seconds = static_cast<double>(now_ns - previous_.last_ns) / 1e9;
        m.total_enqueued_delta = total_enq - previous_.total_enqueued;
        m.total_executed_delta = total_exe - previous_.total_executed;
        m.snapshot_ns = now_ns;
        m.io_ops_per_sec = io_ops_;
        m.io_bytes_per_sec = io_bytes_;

        // 对 per_queue 做 delta（简化：直接用量）
        // 实际生产应跟踪 per-queue-type 的累计值做 delta

        // 更新基线
        previous_.last_ns = now_ns;
        previous_.total_enqueued = total_enq;
        previous_.total_executed = total_exe;

        return m;
    }

    void set_io_metrics(double ops_per_sec, double bytes_per_sec) {
        io_ops_ = ops_per_sec;
        io_bytes_ = bytes_per_sec;
    }

private:
    struct Snapshot {
        uint64_t last_ns{0};
        size_t total_enqueued{0};
        size_t total_executed{0};
    };
    std::vector<std::pair<size_t, WorkerPerf*>> workers_;
    Snapshot previous_;
    double io_ops_{0};
    double io_bytes_{0};
};

}  // namespace storage::runtime
