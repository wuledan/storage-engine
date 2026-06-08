#pragma once
#include "metrics_collector.h"
#include "dispatch_types.h"

namespace storage::runtime {

// ── 实时容量评估 ──
struct RealTimeCapacity {
    double arrival_rate{0};          // λ (req/s)
    double service_rate_per_core{0}; // μ (req/s/core)
    double utilization{0};           // ρ
    double avg_queue_depth{0};
    double avg_latency_us{0};
    double max_throughput{0};        // 当前配置极限

    BottleneckAnalysis::Stage bottleneck{};
    double headroom_pct{0};

    static RealTimeCapacity estimate(const RuntimeMetrics& m, size_t active_workers);
};

inline RealTimeCapacity RealTimeCapacity::estimate(
    const RuntimeMetrics& m, size_t active_workers) {
    RealTimeCapacity cap;

    if (m.elapsed_seconds <= 0 || active_workers == 0) {
        return cap;
    }

    // λ = total_enqueued_delta / elapsed_seconds
    cap.arrival_rate = static_cast<double>(m.total_enqueued_delta) / m.elapsed_seconds;

    // μ = total_executed_delta / (elapsed_seconds * active_workers)
    cap.service_rate_per_core =
        static_cast<double>(m.total_executed_delta) / m.elapsed_seconds
        / static_cast<double>(active_workers);

    // 利用率：从 per_queue 计算 busy_ns / (elapsed * workers * 1e9)
    uint64_t total_busy_ns = 0;
    for (auto& q : m.per_queue) {
        total_busy_ns += q.total_exec_ns;
    }
    cap.utilization = static_cast<double>(total_busy_ns)
        / (m.elapsed_seconds * 1e9 * static_cast<double>(active_workers));
    if (cap.utilization > 1.0) cap.utilization = 1.0;

    // 平均延迟
    double total_wait = 0;
    size_t wait_count = 0;
    for (auto& q : m.per_queue) {
        if (q.enqueued_delta > 0) {
            total_wait += q.avg_wait_ns * q.enqueued_delta;
            wait_count += q.enqueued_delta;
        }
    }
    cap.avg_latency_us = (wait_count > 0)
        ? (total_wait / wait_count) / 1000.0
        : 0;

    // 队列深度
    cap.avg_queue_depth = cap.utilization / (1.0 - cap.utilization + 0.0001);

    // 最大吞吐
    cap.max_throughput = cap.service_rate_per_core * static_cast<double>(active_workers);

    // headroom
    cap.headroom_pct = (1.0 - cap.utilization) * 100.0;

    // 瓶颈识别：
    // - 如果 engine queue P99 > net queue P99 * 2: Engine 瓶颈
    // - 如果 utilization > 0.8: CPU/Engine 瓶颈
    // - 否则: Balanced
    double max_p99 = 0;
    double engine_p99 = 0;
    double net_p99 = 0;
    for (auto& q : m.per_queue) {
        if (q.p99_wait_ns > max_p99) max_p99 = q.p99_wait_ns;
        if (q.type == QueueType::kEngine) engine_p99 = q.p99_wait_ns;
        if (q.type == QueueType::kNetIO) net_p99 = q.p99_wait_ns;
    }
    if (engine_p99 > net_p99 * 2 && net_p99 > 0) {
        cap.bottleneck = BottleneckAnalysis::kEngine;
    } else if (cap.utilization > 0.8) {
        cap.bottleneck = BottleneckAnalysis::kEngine;
    } else {
        cap.bottleneck = BottleneckAnalysis::kBalanced;
    }

    return cap;
}

}  // namespace storage::runtime
