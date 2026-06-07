#include <gtest/gtest.h>
#include "runtime/local_queue.h"
#include "runtime/work_item.h"

using namespace storage::runtime;

// Use a small capacity (power-of-2) for full-queue tests.
using SmallQueue = LocalQueue<int, 4>;

// ============================================================================
// Basic try_enqueue / try_dequeue
// ============================================================================
TEST(LocalQueueTest, BasicEnqueueDequeue) {
    LocalQueue<int> q;

    EXPECT_TRUE(q.try_enqueue(42));
    EXPECT_TRUE(q.try_enqueue(100));

    int val = 0;
    EXPECT_TRUE(q.try_dequeue(val));
    EXPECT_EQ(val, 42);

    EXPECT_TRUE(q.try_dequeue(val));
    EXPECT_EQ(val, 100);
}

// ============================================================================
// Full queue — enqueue returns false
// ============================================================================
TEST(LocalQueueTest, FullQueueReturnsFalse) {
    SmallQueue q;  // Capacity = 4, so max 3 elements (head==tail means empty)

    // With capacity 4, indices 0..3.  head=0.  tail goes 0→1→2→3.
    // When tail wraps and catches head, the queue is full (next_tail == head).
    EXPECT_TRUE(q.try_enqueue(1));
    EXPECT_TRUE(q.try_enqueue(2));
    EXPECT_TRUE(q.try_enqueue(3));
    // Fourth enqueue should fail (next_tail=0 == head=0)
    EXPECT_FALSE(q.try_enqueue(4));
}

// ============================================================================
// Empty queue — dequeue returns false
// ============================================================================
TEST(LocalQueueTest, EmptyQueueReturnsFalse) {
    LocalQueue<int> q;

    int val = -1;
    EXPECT_FALSE(q.try_dequeue(val));
    EXPECT_EQ(val, -1);  // value unchanged
}

// ============================================================================
// Batch dequeue
// ============================================================================
TEST(LocalQueueTest, BatchDequeueCount) {
    LocalQueue<int> q;

    EXPECT_TRUE(q.try_enqueue(10));
    EXPECT_TRUE(q.try_enqueue(20));
    EXPECT_TRUE(q.try_enqueue(30));

    int buf[4] = {};
    size_t n = q.try_dequeue_batch(buf, 4);
    EXPECT_EQ(n, 3u);
    EXPECT_EQ(buf[0], 10);
    EXPECT_EQ(buf[1], 20);
    EXPECT_EQ(buf[2], 30);
}

TEST(LocalQueueTest, BatchDequeuePartial) {
    LocalQueue<int> q;

    EXPECT_TRUE(q.try_enqueue(1));
    EXPECT_TRUE(q.try_enqueue(2));
    EXPECT_TRUE(q.try_enqueue(3));

    int buf[2] = {};
    size_t n = q.try_dequeue_batch(buf, 2);
    EXPECT_EQ(n, 2u);
    EXPECT_EQ(buf[0], 1);
    EXPECT_EQ(buf[1], 2);

    // Remaining
    n = q.try_dequeue_batch(buf, 2);
    EXPECT_EQ(n, 1u);
    EXPECT_EQ(buf[0], 3);
}

TEST(LocalQueueTest, BatchDequeueEmpty) {
    LocalQueue<int> q;
    int buf[4] = {};
    EXPECT_EQ(q.try_dequeue_batch(buf, 4), 0u);
}

// ============================================================================
// size() and empty()
// ============================================================================
TEST(LocalQueueTest, SizeAndEmpty) {
    LocalQueue<int> q;

    EXPECT_TRUE(q.empty());
    EXPECT_EQ(q.size(), 0u);

    q.try_enqueue(1);
    EXPECT_FALSE(q.empty());
    EXPECT_EQ(q.size(), 1u);

    q.try_enqueue(2);
    EXPECT_EQ(q.size(), 2u);

    int val;
    q.try_dequeue(val);
    EXPECT_EQ(q.size(), 1u);

    q.try_dequeue(val);
    EXPECT_TRUE(q.empty());
    EXPECT_EQ(q.size(), 0u);
}

// ============================================================================
// Wrap-around test (ring buffer behavior)
// ============================================================================
TEST(LocalQueueTest, WrapAround) {
    SmallQueue q;  // capacity 4 → indices 0,1,2,3

    // Fill with 3 items (max before full)
    EXPECT_TRUE(q.try_enqueue(1));
    EXPECT_TRUE(q.try_enqueue(2));
    EXPECT_TRUE(q.try_enqueue(3));

    int v = 0;
    q.try_dequeue(v);  // remove 1  → head=1
    q.try_dequeue(v);  // remove 2  → head=2
    EXPECT_EQ(v, 2);

    // Now head=2, tail=3.  We can add one more.
    EXPECT_TRUE(q.try_enqueue(4));  // tail goes 3→0  (wrap)
    EXPECT_TRUE(q.try_enqueue(5));  // tail goes 0→1

    // Queue: [4, 5, 3] at indices [0, 1, 2]
    // Dequeue all
    EXPECT_EQ(q.size(), 3u);
    EXPECT_TRUE(q.try_dequeue(v));
    EXPECT_EQ(v, 3);
    EXPECT_TRUE(q.try_dequeue(v));
    EXPECT_EQ(v, 4);
    EXPECT_TRUE(q.try_dequeue(v));
    EXPECT_EQ(v, 5);
    EXPECT_TRUE(q.empty());
}

// ============================================================================
// WorkItem in LocalQueue
// ============================================================================
TEST(LocalQueueTest, WorkItemQueue) {
    LocalQueue<WorkItem, 8> q;

    static int executed = 0;
    executed = 0;

    WorkItem item = WorkItem::make_func(+[]() { ++executed; });
    EXPECT_TRUE(q.try_enqueue(item));

    EXPECT_FALSE(q.empty());
    EXPECT_EQ(q.size(), 1u);

    WorkItem out;
    EXPECT_TRUE(q.try_dequeue(out));
    out.execute();
    EXPECT_EQ(executed, 1);
    EXPECT_TRUE(q.empty());
}
