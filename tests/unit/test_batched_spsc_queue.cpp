#include <gtest/gtest.h>
#include <array>
#include "runtime/batched_spsc_queue.h"
#include "runtime/work_item.h"

using namespace storage::runtime;

// Use a small capacity for testing, must be power of 2
using SmallBatchedQueue = BatchedSPSCQueue<int, 8>;

// ============================================================================
// push_batch + try_dequeue_batch basic
// ============================================================================
TEST(BatchedSPSCQueueTest, BasicPushDequeue) {
    BatchedSPSCQueue<int> q;

    int input[3] = {10, 20, 30};
    q.push_batch(input, 3);

    EXPECT_EQ(q.approx_count(), 3u);

    int output[8] = {};
    size_t n = q.try_dequeue_batch(output, 8);
    EXPECT_EQ(n, 3u);
    EXPECT_EQ(output[0], 10);
    EXPECT_EQ(output[1], 20);
    EXPECT_EQ(output[2], 30);

    EXPECT_EQ(q.approx_count(), 0u);
}

// ============================================================================
// Multiple batches (cross batch boundary)
// ============================================================================
TEST(BatchedSPSCQueueTest, MultipleBatches) {
    BatchedSPSCQueue<int> q;

    int batch1[2] = {1, 2};
    int batch2[2] = {3, 4};
    q.push_batch(batch1, 2);
    q.push_batch(batch2, 2);

    EXPECT_EQ(q.approx_count(), 4u);

    int output[4] = {};
    size_t n = q.try_dequeue_batch(output, 4);
    EXPECT_EQ(n, 4u);
    EXPECT_EQ(output[0], 1);
    EXPECT_EQ(output[1], 2);
    EXPECT_EQ(output[2], 3);
    EXPECT_EQ(output[3], 4);
}

// ============================================================================
// Partial dequeue (smaller than available)
// ============================================================================
TEST(BatchedSPSCQueueTest, PartialDequeue) {
    BatchedSPSCQueue<int> q;

    int input[5] = {10, 20, 30, 40, 50};
    q.push_batch(input, 5);

    // Dequeue only 2
    int output[8] = {};
    size_t n = q.try_dequeue_batch(output, 2);
    EXPECT_EQ(n, 2u);
    EXPECT_EQ(output[0], 10);
    EXPECT_EQ(output[1], 20);
    EXPECT_EQ(q.approx_count(), 3u);

    // Dequeue remaining
    n = q.try_dequeue_batch(output, 8);
    EXPECT_EQ(n, 3u);
    EXPECT_EQ(output[0], 30);
    EXPECT_EQ(output[1], 40);
    EXPECT_EQ(output[2], 50);
}

// ============================================================================
// Empty dequeue
// ============================================================================
TEST(BatchedSPSCQueueTest, EmptyDequeue) {
    BatchedSPSCQueue<int> q;
    int output[4] = {};
    EXPECT_EQ(q.try_dequeue_batch(output, 4), 0u);
    EXPECT_EQ(q.approx_count(), 0u);
}

// ============================================================================
// approx_count approximate accuracy
// ============================================================================
TEST(BatchedSPSCQueueTest, ApproxCount) {
    BatchedSPSCQueue<int> q;

    EXPECT_EQ(q.approx_count(), 0u);

    q.push_batch(std::array<int, 3>{1, 2, 3}.data(), 3);
    EXPECT_EQ(q.approx_count(), 3u);

    int output[8] = {};
    q.try_dequeue_batch(output, 2);
    EXPECT_EQ(q.approx_count(), 1u);

    q.try_dequeue_batch(output, 8);
    EXPECT_EQ(q.approx_count(), 0u);
}

// ============================================================================
// Ring-buffer wrap-around
// ============================================================================
TEST(BatchedSPSCQueueTest, WrapAround) {
    SmallBatchedQueue q;  // capacity = 8

    // Fill with 7 items (headroom of 1 for SPSC safe operation)
    int input[7] = {1, 2, 3, 4, 5, 6, 7};
    q.push_batch(input, 7);

    // Dequeue 4
    int output[8] = {};
    EXPECT_EQ(q.try_dequeue_batch(output, 4), 4u);
    EXPECT_EQ(output[0], 1);
    EXPECT_EQ(output[3], 4);

    // Push more to force wrap-around
    int input2[4] = {8, 9, 10, 11};
    q.push_batch(input2, 4);

    // Dequeue remaining — should cross the wrap boundary
    EXPECT_EQ(q.try_dequeue_batch(output, 8), 7u);
    EXPECT_EQ(output[0], 5);
    EXPECT_EQ(output[1], 6);
    EXPECT_EQ(output[2], 7);
    EXPECT_EQ(output[3], 8);
    EXPECT_EQ(output[4], 9);
    EXPECT_EQ(output[5], 10);
    EXPECT_EQ(output[6], 11);
}

// ============================================================================
// WorkItem in BatchedSPSCQueue
// ============================================================================
TEST(BatchedSPSCQueueTest, WorkItemQueue) {
    BatchedSPSCQueue<WorkItem, 8> q;

    static int counter = 0;
    counter = 0;

    WorkItem items[3] = {
        WorkItem::make_func(+[]() { counter += 1; }),
        WorkItem::make_func(+[]() { counter += 10; }),
        WorkItem::make_func(+[]() { counter += 100; }),
    };
    q.push_batch(items, 3);
    EXPECT_EQ(q.approx_count(), 3u);

    WorkItem output[8] = {};
    size_t n = q.try_dequeue_batch(output, 8);
    EXPECT_EQ(n, 3u);

    for (size_t i = 0; i < n; ++i) {
        output[i].execute();
    }
    EXPECT_EQ(counter, 111);
}
