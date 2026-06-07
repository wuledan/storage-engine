#include <gtest/gtest.h>
#include <thread>
#include <atomic>
#include <vector>
#include <cstdint>
#include "runtime/affine_work_queue.h"
#include "runtime/batched_spsc_queue.h"
#include "runtime/work_item.h"

using namespace storage::runtime;

// Constants at namespace scope for use in captureless lambdas
static constexpr int kAffineProducers = 4;
static constexpr int kAffineItemsPerProducer = 100'000;
static constexpr int kAffineTotalItems =
    kAffineProducers * kAffineItemsPerProducer;

static constexpr int kSPSCProducers = 2;
static constexpr int kSPSCItemsPerProducer = 500'000;
static constexpr int kSPCSTotalItems =
    kSPSCProducers * kSPSCItemsPerProducer;
static constexpr int kSPSCBatchSize = 64;

static constexpr int kContentionThreads = 8;
static constexpr int kContentionItemsPerThread = 50'000;
static constexpr int kContentionTotal =
    kContentionThreads * kContentionItemsPerThread;

static constexpr int kLargeBatchSize = 128;
static constexpr int kLargeTotalItems = 1'000'000;
static constexpr int kLargeDequeueBufSize = 512;

// ============================================================================
// AffineWorkQueue: 4 threads concurrent enqueue 100k items,
// 1 thread dequeue → verify total count
// ============================================================================
TEST(ConcurrentTest, AffineQueueMultiProducerSingleConsumer) {
    AffineWorkQueue q(QueueType::kAffine, Priority::kCritical, "concurrent_affine");

    std::atomic<int64_t> produced_sum{0};

    // Producers: each enqueues 100k items
    std::vector<std::thread> producers;
    for (int t = 0; t < kAffineProducers; ++t) {
        producers.emplace_back([&q, &produced_sum]() {
            for (int i = 0; i < kAffineItemsPerProducer; ++i) {
                q.enqueue(WorkItem::make_func(
                    +[]() {}  // no-op, we just count items
                ));
                produced_sum.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    // Consumer: dequeue all items
    std::atomic<int64_t> consumed_count{0};
    std::thread consumer([&q, &consumed_count]() {
        WorkItem item;
        int64_t local_count = 0;
        while (local_count < kAffineTotalItems) {
            if (q.try_dequeue(item)) {
                item.execute();
                ++local_count;
            } else {
                // Brief yield when empty to let producers catch up
                std::this_thread::yield();
            }
        }
        consumed_count.store(local_count, std::memory_order_release);
    });

    for (auto& th : producers) {
        th.join();
    }
    consumer.join();

    EXPECT_EQ(produced_sum.load(), kAffineTotalItems);
    EXPECT_EQ(consumed_count.load(), kAffineTotalItems);
}

// ============================================================================
// BatchedSPSCQueue: 2 producers push_batch 1M items → verify total
//
// NOTE: BatchedSPSCQueue is SPSC (single producer, single consumer) by design.
// We use 2 separate queues (one per producer) and a single consumer that
// drains both queues, simulating a realistic multi-producer setup.
// ============================================================================
TEST(ConcurrentTest, BatchedSPSCMultiProducerTotal) {
    // Use 2 SPSC queues (one per producer)
    BatchedSPSCQueue<WorkItem, 65536> q1;
    BatchedSPSCQueue<WorkItem, 65536> q2;

    std::atomic<int64_t> produced_sum{0};

    // Producer 1
    std::thread p1([&q1, &produced_sum]() {
        WorkItem batch[kSPSCBatchSize];
        int remaining = kSPSCItemsPerProducer;
        while (remaining > 0) {
            int n = std::min(remaining, kSPSCBatchSize);
            for (int i = 0; i < n; ++i) {
                batch[i] = WorkItem::make_func(+[]() {});
            }
            q1.push_batch(batch, n);
            produced_sum.fetch_add(n, std::memory_order_relaxed);
            remaining -= n;
        }
    });

    // Producer 2
    std::thread p2([&q2, &produced_sum]() {
        WorkItem batch[kSPSCBatchSize];
        int remaining = kSPSCItemsPerProducer;
        while (remaining > 0) {
            int n = std::min(remaining, kSPSCBatchSize);
            for (int i = 0; i < n; ++i) {
                batch[i] = WorkItem::make_func(+[]() {});
            }
            q2.push_batch(batch, n);
            produced_sum.fetch_add(n, std::memory_order_relaxed);
            remaining -= n;
        }
    });

    // Consumer: drain both queues
    std::atomic<int64_t> consumed_count{0};
    std::thread consumer([&q1, &q2, &consumed_count]() {
        WorkItem buf[256];
        int64_t total = 0;
        while (total < kSPCSTotalItems) {
            size_t n1 = q1.try_dequeue_batch(buf, 256);
            for (size_t i = 0; i < n1; ++i) {
                buf[i].execute();
            }
            total += n1;

            size_t n2 = q2.try_dequeue_batch(buf, 256);
            for (size_t i = 0; i < n2; ++i) {
                buf[i].execute();
            }
            total += n2;

            if (n1 == 0 && n2 == 0) {
                std::this_thread::yield();
            }
        }
        consumed_count.store(total, std::memory_order_release);
    });

    p1.join();
    p2.join();
    consumer.join();

    EXPECT_EQ(produced_sum.load(), kSPCSTotalItems);
    EXPECT_EQ(consumed_count.load(), kSPCSTotalItems);
}

// ============================================================================
// AffineWorkQueue: concurrent correctness under high contention
// ============================================================================
TEST(ConcurrentTest, AffineQueueHighContention) {
    AffineWorkQueue q(QueueType::kAffine, Priority::kCritical, "contention");

    std::atomic<int64_t> enqueued{0};
    std::vector<std::thread> producers;

    for (int t = 0; t < kContentionThreads; ++t) {
        producers.emplace_back([&q, &enqueued]() {
            for (int i = 0; i < kContentionItemsPerThread; ++i) {
                q.enqueue(WorkItem::make_func(+[]() {}));
            }
            enqueued.fetch_add(kContentionItemsPerThread,
                               std::memory_order_relaxed);
        });
    }

    // Consumer dequeues concurrently
    std::atomic<int64_t> dequeued{0};
    std::thread consumer([&q, &dequeued]() {
        WorkItem item;
        int64_t local = 0;
        while (local < kContentionTotal) {
            if (q.try_dequeue(item)) {
                item.execute();
                ++local;
            } else {
                std::this_thread::yield();
            }
        }
        dequeued.store(local, std::memory_order_release);
    });

    for (auto& th : producers) {
        th.join();
    }
    consumer.join();

    EXPECT_EQ(enqueued.load(), kContentionTotal);
    EXPECT_EQ(dequeued.load(), kContentionTotal);
}

// ============================================================================
// BatchedSPSCQueue: single producer, single consumer large volume
// ============================================================================
TEST(ConcurrentTest, BatchedSPSCSingleProducerLargeVolume) {
    BatchedSPSCQueue<WorkItem, 65536> q;

    std::thread producer([&q]() {
        WorkItem batch[kLargeBatchSize];
        int remaining = kLargeTotalItems;
        while (remaining > 0) {
            int n = std::min(remaining, kLargeBatchSize);
            for (int i = 0; i < n; ++i) {
                batch[i] = WorkItem::make_func(+[]() {});
            }
            q.push_batch(batch, n);
            remaining -= n;
        }
    });

    std::atomic<int64_t> consumed{0};
    std::thread consumer([&q, &consumed]() {
        WorkItem buf[kLargeDequeueBufSize];
        int64_t total = 0;
        while (total < kLargeTotalItems) {
            size_t n = q.try_dequeue_batch(buf, kLargeDequeueBufSize);
            for (size_t i = 0; i < n; ++i) {
                buf[i].execute();
            }
            total += n;
            if (n == 0) {
                std::this_thread::yield();
            }
        }
        consumed.store(total, std::memory_order_release);
    });

    producer.join();
    consumer.join();

    EXPECT_EQ(consumed.load(), kLargeTotalItems);
}
