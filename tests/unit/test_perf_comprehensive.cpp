#include <gtest/gtest.h>
#include "runtime/worker_perf.h"
#include "runtime/perf_histogram.h"
#include "runtime/trace_config.h"
#include "runtime/online_worker.h"
#include <chrono>
#include <thread>

using namespace storage::runtime;

// ===== 直方图精度测试 =====

// 已知值分布，验证百分位合理
TEST(PerfComprehensiveTest, HistogramPercentileAccuracy) {
    Histogram h;
    // 记录：1个1ns, 1个1us, 1个1ms, 1个1s
    h.record(1);
    h.record(1000);
    h.record(1000000);
    h.record(1000000000);

    EXPECT_EQ(h.count(), 4);
    // P0 ≈ 1ns
    EXPECT_NEAR(h.percentile(0.0), 1, 1);
    // P50 应该在 1000 附近（1us）
    EXPECT_LE(h.percentile(0.25), 1024);
    // P75 在 1000000 附近（1ms）
    EXPECT_GT(h.percentile(0.75), 500000);
}

// 大量同值记录
TEST(PerfComprehensiveTest, HistogramUniformValues) {
    Histogram h;
    for (int i = 0; i < 1000; ++i) {
        h.record(1000);
    }
    EXPECT_EQ(h.count(), 1000);
    // 所有值相同，P50 应该在同一个桶
    auto p50 = h.percentile(0.5);
    EXPECT_GE(p50, 512);
    EXPECT_LE(p50, 1024);
}

// ===== kNone 级别零开销验证 =====

// kNone 时 record_enqueue 返回 0
TEST(PerfComprehensiveTest, NoneLevelZeroOverhead) {
    WorkerPerf perf(0);
    perf.set_level(PerfLevel::kNone);
    for (int i = 0; i < 10000; ++i) {
        auto ts = perf.record_enqueue(QueueType::kEngine, i);
        EXPECT_EQ(ts, 0);
    }
    auto snap = perf.snapshot();
    // kNone 时所有队列计数器为 0
    for (auto& q : snap.queues) {
        EXPECT_EQ(q.enqueued, 0);
    }
}

// ===== kCount 计数器正确性 =====

TEST(PerfComprehensiveTest, CountLevelAccuracy) {
    WorkerPerf perf(0);
    perf.set_level(PerfLevel::kCount);

    constexpr size_t N = 50000;
    for (size_t i = 0; i < N; ++i) {
        perf.record_enqueue(QueueType::kEngine, 0);
    }

    auto snap = perf.snapshot();
    bool found = false;
    for (auto& q : snap.queues) {
        if (q.type == QueueType::kEngine) {
            EXPECT_EQ(q.enqueued, N);
            found = true;
        }
    }
    EXPECT_TRUE(found);
}

// ===== kTrace 级别全量记录 =====

TEST(PerfComprehensiveTest, TraceLevelRecordsWaitTime) {
    WorkerPerf perf(0);
    perf.set_level(PerfLevel::kTrace);

    auto ts = perf.record_enqueue(QueueType::kEngine, 42);
    EXPECT_GT(ts, 0);

    // 模拟等待 100ns
    std::this_thread::sleep_for(std::chrono::nanoseconds(100));
    perf.record_dequeue(QueueType::kEngine, ts);
    perf.record_exec(QueueType::kEngine, 500);

    auto snap = perf.snapshot();
    for (auto& q : snap.queues) {
        if (q.type == QueueType::kEngine) {
            EXPECT_GT(q.max_wait_ns, 0);
            EXPECT_GT(q.p50_wait_ns, 0);
        }
    }
}

// ===== TraceConfig 与 WorkerPerf 集成 =====

// 验证 TraceConfig 开关能控制 WorkerPerf 的行为
TEST(PerfComprehensiveTest, TraceConfigWithWorkerPerf) {
    TraceConfig::disable();

    WorkerPerf perf(0);
    // TraceConfig 禁用 ≠ WorkerPerf 级别，但可以在上层联动
    perf.set_level(PerfLevel::kTrace);

    uint32_t tid = TraceConfig::next_trace_id();
    bool should = TraceConfig::should_trace(tid);
    // 默认禁用，所以不 trace
    EXPECT_FALSE(should);
}

// ===== 多次 snapshot 一致性 =====

TEST(PerfComprehensiveTest, MultipleSnapshotsConsistent) {
    WorkerPerf perf(0);
    perf.set_level(PerfLevel::kCount);

    perf.record_enqueue(QueueType::kNetIO, 0);
    perf.record_enqueue(QueueType::kNetIO, 0);

    auto snap1 = perf.snapshot();
    auto snap2 = perf.snapshot();

    // 两次 snapshot 应该看到相同的计数（累计值不变）
    for (size_t i = 0; i < snap1.queues.size(); ++i) {
        EXPECT_EQ(snap1.queues[i].enqueued, snap2.queues[i].enqueued);
    }
}

// ===== 全链路：OnlineWorker + Perf 集成 =====

namespace {
    std::atomic<int> g_perf_done{0};
    void perf_integration_func() { g_perf_done.fetch_add(1); }
}

TEST(PerfComprehensiveTest, OnlineWorkerPerfIntegration) {
    TraceConfig::enable_all();

    Worker::Config cfg;
    OnlineWorker w(cfg);

    // 启用 trace level
    w.set_perf_level(PerfLevel::kTrace);

    w.start();

    g_perf_done.store(0);
    for (int i = 0; i < 100; ++i) {
        WorkItem item = WorkItem::make_func(perf_integration_func);
        item.trace_id = TraceConfig::next_trace_id();
        w.submit_engine(std::move(item));
    }

    // 等待任务完成
    while (g_perf_done.load() < 100) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }

    w.stop();
    w.join();

    auto snap = w.perf().snapshot();
    // 应该有 engine 队列的计数
    bool has_engine = false;
    for (auto& q : snap.queues) {
        if (q.type == QueueType::kEngine) {
            EXPECT_GE(q.enqueued, 100);
            has_engine = true;
        }
    }
    EXPECT_TRUE(has_engine);

    TraceConfig::disable();
}
