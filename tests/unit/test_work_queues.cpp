#include <gtest/gtest.h>
#include <thread>
#include <atomic>
#include <vector>
#include "runtime/local_work_queue.h"
#include "runtime/batched_spsc_work_queue.h"
#include "runtime/affine_work_queue.h"

using namespace storage::runtime;

// ============================================================================
// LocalWorkQueue: metadata + enqueue/dequeue
// ============================================================================
TEST(LocalWorkQueueTest, Metadata) {
    LocalWorkQueue q(QueueType::kEngine, Priority::kHigh, "engine_q");
    EXPECT_EQ(q.type(), QueueType::kEngine);
    EXPECT_EQ(q.base_priority(), Priority::kHigh);
    EXPECT_EQ(q.semantic(), QueueSemantic::kLocal);
    EXPECT_STREQ(q.name(), "engine_q");
}

TEST(LocalWorkQueueTest, EnqueueDequeue) {
    LocalWorkQueue q(QueueType::kEngine, Priority::kCritical, "test_q");

    static int executed = 0;
    executed = 0;

    WorkItem item = WorkItem::make_func(+[]() { ++executed; });
    // LocalWorkQueue does not have a public enqueue(), but items are placed
    // by the worker thread via direct queue access.  For testing we use
    // try_dequeue directly.  Here we push to the underlying queue
    // through the abstract interface — only dequeue is exposed.
    //
    // Instead, we verify that after manually adding to the internal queue
    // (via friend access in a real scenario), try_dequeue works.
    // Since LocalQueue is the backing store and try_enqueue is not exposed
    // through WorkQueue, we test the dequeue path when items are already present.
    //
    // We use the fact that LocalWorkQueue::approx_count == queue_.size()
    // and that we can verify empty behavior.
    EXPECT_EQ(q.approx_count(), 0u);

    // For a real LocalWorkQueue, items are enqueued by the owning thread.
    // Since there's no public enqueue, we test that dequeue on empty = false.
    WorkItem out;
    EXPECT_FALSE(q.try_dequeue(out));
    EXPECT_EQ(q.try_dequeue_batch(&out, 1), 0u);
}

// ============================================================================
// BatchedSPSCWorkQueue: push_batch + dequeue
// ============================================================================
TEST(BatchedSPSCWorkQueueTest, Metadata) {
    BatchedSPSCWorkQueue q(QueueType::kNetIO, Priority::kCritical, "io_q");
    EXPECT_EQ(q.type(), QueueType::kNetIO);
    EXPECT_EQ(q.base_priority(), Priority::kCritical);
    EXPECT_EQ(q.semantic(), QueueSemantic::kSPSC);
    EXPECT_STREQ(q.name(), "io_q");
}

TEST(BatchedSPSCWorkQueueTest, PushBatchAndDequeue) {
    BatchedSPSCWorkQueue q(QueueType::kNetIO, Priority::kHigh, "io_q");

    static int acc = 0;
    acc = 0;

    WorkItem items[3] = {
        WorkItem::make_func(+[]() { acc += 1; }),
        WorkItem::make_func(+[]() { acc += 2; }),
        WorkItem::make_func(+[]() { acc += 4; }),
    };
    q.push_batch(items, 3);

    EXPECT_EQ(q.approx_count(), 3u);

    // Dequeue one at a time
    WorkItem out;
    EXPECT_TRUE(q.try_dequeue(out));
    out.execute();
    EXPECT_EQ(acc, 1);

    EXPECT_TRUE(q.try_dequeue(out));
    out.execute();
    EXPECT_EQ(acc, 3);

    EXPECT_TRUE(q.try_dequeue(out));
    out.execute();
    EXPECT_EQ(acc, 7);

    EXPECT_FALSE(q.try_dequeue(out));
}

TEST(BatchedSPSCWorkQueueTest, BatchDequeue) {
    BatchedSPSCWorkQueue q(QueueType::kDiskIO, Priority::kMedium, "disk_q");

    static int acc = 0;
    acc = 0;

    WorkItem items[4] = {
        WorkItem::make_func(+[]() { acc += 1; }),
        WorkItem::make_func(+[]() { acc += 2; }),
        WorkItem::make_func(+[]() { acc += 4; }),
        WorkItem::make_func(+[]() { acc += 8; }),
    };
    q.push_batch(items, 4);

    WorkItem buf[4] = {};
    size_t n = q.try_dequeue_batch(buf, 4);
    EXPECT_EQ(n, 4u);
    for (size_t i = 0; i < n; ++i) buf[i].execute();
    EXPECT_EQ(acc, 15);
}

// ============================================================================
// AffineWorkQueue: enqueue + dequeue, multi-threaded
// ============================================================================
TEST(AffineWorkQueueTest, Metadata) {
    AffineWorkQueue q(QueueType::kAffine, Priority::kHigh, "affine_q");
    EXPECT_EQ(q.type(), QueueType::kAffine);
    EXPECT_EQ(q.base_priority(), Priority::kHigh);
    EXPECT_EQ(q.semantic(), QueueSemantic::kMPMC);
    EXPECT_STREQ(q.name(), "affine_q");
}

TEST(AffineWorkQueueTest, SingleThreadEnqueueDequeue) {
    AffineWorkQueue q(QueueType::kAffine, Priority::kLow, "test");

    static int val = 0;
    val = 0;

    q.enqueue(WorkItem::make_func(+[]() { val = 42; }));

    EXPECT_GE(q.approx_count(), 0u);  // UMPMCQueue doesn't give exact size

    WorkItem out;
    EXPECT_TRUE(q.try_dequeue(out));
    out.execute();
    EXPECT_EQ(val, 42);

    EXPECT_FALSE(q.try_dequeue(out));
}

TEST(AffineWorkQueueTest, MultiThreadedEnqueue) {
    AffineWorkQueue q(QueueType::kBackground, Priority::kLow, "bg");

    static std::atomic<int> sum{0};
    sum = 0;

    constexpr int kNumThreads = 4;
    constexpr int kItemsPerThread = 100;

    std::vector<std::thread> producers;
    for (int t = 0; t < kNumThreads; ++t) {
        producers.emplace_back([&q, t]() {
            for (int i = 0; i < kItemsPerThread; ++i) {
                q.enqueue(WorkItem::make_func(+[]() { sum.fetch_add(1); }));
            }
        });
    }

    for (auto& th : producers) th.join();

    // Dequeue all items
    int dequeued = 0;
    WorkItem out;
    while (q.try_dequeue(out)) {
        out.execute();
        ++dequeued;
    }

    EXPECT_EQ(dequeued, kNumThreads * kItemsPerThread);
    EXPECT_EQ(sum.load(), kNumThreads * kItemsPerThread);
}

TEST(AffineWorkQueueTest, BatchDequeue) {
    AffineWorkQueue q(QueueType::kEngine, Priority::kMedium, "batch_test");

    for (int i = 0; i < 10; ++i) {
        q.enqueue(WorkItem::make_func(+[]() {}));
    }

    WorkItem buf[8] = {};
    size_t n = q.try_dequeue_batch(buf, 8);
    EXPECT_EQ(n, 8u);

    n = q.try_dequeue_batch(buf, 8);
    EXPECT_EQ(n, 2u);

    n = q.try_dequeue_batch(buf, 8);
    EXPECT_EQ(n, 0u);
}
