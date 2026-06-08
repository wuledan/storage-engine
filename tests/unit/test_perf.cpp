#include <gtest/gtest.h>
#include "runtime/worker_perf.h"
#include "runtime/perf_histogram.h"
#include "runtime/work_item.h"

using namespace storage::runtime;

// ============================================================================
// 测试1: Histogram 基本操作
// ============================================================================
TEST(PerfTest, HistogramRecord) {
    Histogram h;
    h.record(100);
    h.record(200);
    h.record(300);
    EXPECT_EQ(h.count(), 3);
    // 100~255 落在 bucket 7 (128), 200~300 落在 bucket 8 (256)
    EXPECT_NEAR(static_cast<uint64_t>(h.percentile(0.5)), 256, 256);
}

// ============================================================================
// 测试2: 禁用时 record_enqueue 返回 0
// ============================================================================
TEST(PerfTest, RecordEnqueueWhenDisabled) {
    WorkerPerf perf(0);
    perf.set_level(PerfLevel::kNone);
    auto ts = perf.record_enqueue(QueueType::kEngine, 0);
    EXPECT_EQ(ts, 0);
}

// ============================================================================
// 测试3: kCount 时 record_enqueue 返回 0（不记录时间戳）
// ============================================================================
TEST(PerfTest, RecordEnqueueAtCountLevel) {
    WorkerPerf perf(0);
    perf.set_level(PerfLevel::kCount);
    auto ts = perf.record_enqueue(QueueType::kEngine, 0);
    EXPECT_EQ(ts, 0);
    // 但计数应该递增
    auto snap = perf.snapshot();
    EXPECT_GT(snap.queues.size(), 0);
}

// ============================================================================
// 测试4: kTrace 时 record_enqueue 返回非零时间戳
// ============================================================================
TEST(PerfTest, RecordEnqueueAtTraceLevel) {
    WorkerPerf perf(0);
    perf.set_level(PerfLevel::kTrace);
    auto ts = perf.record_enqueue(QueueType::kEngine, 0);
    EXPECT_GT(ts, 0);
}

// ============================================================================
// 测试5: snapshot 聚合
// ============================================================================
TEST(PerfTest, SnapshotAggregation) {
    WorkerPerf perf(0);
    perf.set_level(PerfLevel::kTrace);
    auto ts = perf.record_enqueue(QueueType::kEngine, 42);
    perf.record_dequeue(QueueType::kEngine, ts);
    perf.record_exec(QueueType::kEngine, 1000);

    auto snap = perf.snapshot();
    ASSERT_GE(snap.queues.size(), 1);
    EXPECT_EQ(snap.queues[0].type, QueueType::kEngine);
    EXPECT_EQ(snap.queues[0].enqueued, 1);
}

// ============================================================================
// 测试6: WorkItem enqueue_ns 字段
// ============================================================================
TEST(PerfTest, WorkItemEnqueueNs) {
    WorkItem item;
    EXPECT_EQ(item.enqueue_ns, 0);
    item.enqueue_ns = 12345;
    EXPECT_EQ(item.enqueue_ns, 12345);
}

// ============================================================================
// 测试7: 多队列 perf 独立
// ============================================================================
TEST(PerfTest, MultipleQueuesIndependent) {
    WorkerPerf perf(0);
    perf.set_level(PerfLevel::kCount);
    perf.record_enqueue(QueueType::kEngine, 0);
    perf.record_enqueue(QueueType::kNetIO, 0);
    perf.record_enqueue(QueueType::kDiskIO, 0);
    auto snap = perf.snapshot();
    EXPECT_GE(snap.queues.size(), 3);
}

// ============================================================================
// 测试8: WorkItem reset 清除 enqueue_ns
// ============================================================================
TEST(PerfTest, ResetClearsEnqueueNs) {
    WorkItem item;
    item.enqueue_ns = 12345;
    item.reset();
    EXPECT_EQ(item.enqueue_ns, 0);
}
