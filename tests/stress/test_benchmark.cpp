#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>
#include <vector>
#include <algorithm>
#include <numeric>
#include <cstdint>
#include <iostream>
#include <iomanip>
#include <mutex>
#include <folly/coro/BlockingWait.h>
#include "runtime/local_queue.h"
#include "runtime/batched_spsc_queue.h"
#include "runtime/affine_work_queue.h"
#include "runtime/scheduler.h"
#include "runtime/strict_priority_policy.h"
#include "runtime/adaptive_idle.h"
#include "runtime/worker.h"
#include "runtime/batched_spsc_work_queue.h"
#include "runtime/online_worker.h"
#include "runtime/online_group.h"

using namespace storage::runtime;

// Test fixture: prints benchmark header
class BenchmarkTest : public ::testing::Test {
protected:
    void SetUp() override {}

    void TearDown() override {
        std::cout << "----------------------------------------------------\n";
    }
};

// Helper: measure elapsed ns
static uint64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

// Helper: print throughput
static void print_throughput(const char* label, uint64_t count, uint64_t elapsed_ns) {
    double elapsed_s = static_cast<double>(elapsed_ns) / 1'000'000'000.0;
    double ops_per_s = static_cast<double>(count) / elapsed_s;
    std::cout << std::left << std::setw(50) << label
              << ": " << std::fixed << std::setprecision(0)
              << ops_per_s << " ops/s"
              << "  (count=" << count
              << ", time=" << elapsed_s << "s)\n";
}

// Helper: print latency stats
static void print_latency(const char* label,
                          const std::vector<uint64_t>& latencies_ns) {
    if (latencies_ns.empty()) return;

    std::vector<uint64_t> sorted = latencies_ns;
    std::sort(sorted.begin(), sorted.end());

    uint64_t p50 = sorted[sorted.size() * 50 / 100];
    uint64_t p90 = sorted[sorted.size() * 90 / 100];
    uint64_t p99 = sorted[sorted.size() * 99 / 100];
    uint64_t max_val = sorted.back();
    uint64_t min_val = sorted.front();
    double avg = std::accumulate(sorted.begin(), sorted.end(), 0.0) / sorted.size();

    std::cout << std::left << std::setw(50) << label
              << ": avg=" << std::fixed << std::setprecision(0) << avg
              << " ns  P50=" << p50
              << "  P90=" << p90
              << "  P99=" << p99
              << "  min=" << min_val
              << "  max=" << max_val
              << "  (samples=" << sorted.size() << ")\n";
}

// ============================================================================
// 1. LocalQueue enqueue/dequeue 1M throughput
// ============================================================================
TEST_F(BenchmarkTest, LocalQueueThroughput) {
    constexpr int kNumOps = 1'000'000;

    // --- Enqueue benchmark ---
    {
        LocalQueue<int, 65536> q;
        uint64_t t0 = now_ns();
        for (int i = 0; i < kNumOps; ++i) {
            q.try_enqueue(i);
        }
        uint64_t t1 = now_ns();
        print_throughput("LocalQueue enqueue 1M", kNumOps, t1 - t0);
    }

    // --- Dequeue benchmark ---
    {
        LocalQueue<int, 65536> q;
        for (int i = 0; i < kNumOps; ++i) {
            q.try_enqueue(i);
        }
        uint64_t t0 = now_ns();
        int val;
        for (int i = 0; i < kNumOps; ++i) {
            q.try_dequeue(val);
        }
        uint64_t t1 = now_ns();
        print_throughput("LocalQueue dequeue 1M", kNumOps, t1 - t0);
    }

    // --- Combined enqueue+dequeue ---
    {
        LocalQueue<int, 65536> q;
        uint64_t t0 = now_ns();
        int val;
        for (int i = 0; i < kNumOps; ++i) {
            q.try_enqueue(i);
            q.try_dequeue(val);
        }
        uint64_t t1 = now_ns();
        print_throughput("LocalQueue enqueue+dequeue 1M", kNumOps, t1 - t0);
    }
}

// ============================================================================
// 2. BatchedSPSCQueue batch push/dequeue throughput
// ============================================================================
TEST_F(BenchmarkTest, BatchedSPSCQueueThroughput) {
    constexpr int kTotalItems = 1'000'000;
    constexpr int kBatchSize = 64;

    // --- Push batch benchmark ---
    {
        BatchedSPSCQueue<int, 65536> q;
        int batch[kBatchSize];
        for (int i = 0; i < kBatchSize; ++i) batch[i] = i;

        uint64_t t0 = now_ns();
        int remaining = kTotalItems;
        while (remaining > 0) {
            int n = std::min(remaining, kBatchSize);
            q.push_batch(batch, n);
            remaining -= n;
        }
        uint64_t t1 = now_ns();
        print_throughput("BatchedSPSCQueue push_batch 1M", kTotalItems, t1 - t0);
    }

    // --- Dequeue batch benchmark ---
    {
        BatchedSPSCQueue<int, 65536> q;
        int batch[kBatchSize];
        for (int i = 0; i < kBatchSize; ++i) batch[i] = i;
        int remaining = kTotalItems;
        while (remaining > 0) {
            int n = std::min(remaining, kBatchSize);
            q.push_batch(batch, n);
            remaining -= n;
        }

        int output[512];
        uint64_t t0 = now_ns();
        int64_t total = 0;
        while (total < kTotalItems) {
            size_t n = q.try_dequeue_batch(output, 512);
            total += n;
        }
        uint64_t t1 = now_ns();
        print_throughput("BatchedSPSCQueue dequeue_batch 1M", kTotalItems, t1 - t0);
    }

    // --- Combined push+dequeue ---
    {
        BatchedSPSCQueue<int, 65536> q;
        int batch[kBatchSize];
        for (int i = 0; i < kBatchSize; ++i) batch[i] = i;
        int output[512];

        uint64_t t0 = now_ns();
        int remaining = kTotalItems;
        int64_t consumed = 0;
        while (consumed < kTotalItems) {
            if (remaining > 0) {
                int n = std::min(remaining, kBatchSize);
                q.push_batch(batch, n);
                remaining -= n;
            }
            size_t n = q.try_dequeue_batch(output, 512);
            consumed += n;
        }
        uint64_t t1 = now_ns();
        print_throughput("BatchedSPSCQueue push+dequeue 1M", kTotalItems, t1 - t0);
    }
}

// ============================================================================
// 3. AffineWorkQueue multi-thread throughput
// ============================================================================
TEST_F(BenchmarkTest, AffineWorkQueueThroughput) {
    constexpr int kTotalItems = 1'000'000;
    constexpr int kNumProducers = 4;
    constexpr int kItemsPerProducer = kTotalItems / kNumProducers;

    AffineWorkQueue q(QueueType::kAffine, Priority::kCritical, "bm_affine");

    // --- Multi-producer enqueue ---
    {
        uint64_t t0 = now_ns();
        std::vector<std::thread> producers;
        for (int t = 0; t < kNumProducers; ++t) {
            producers.emplace_back([&q]() {
                for (int i = 0; i < kItemsPerProducer; ++i) {
                    q.enqueue(WorkItem::make_func(+[]() {}));
                }
            });
        }
        for (auto& th : producers) th.join();
        uint64_t t1 = now_ns();
        print_throughput("AffineWorkQueue enqueue 1M (4P)", kTotalItems, t1 - t0);
    }

    // --- Single-consumer dequeue ---
    {
        uint64_t t0 = now_ns();
        WorkItem item;
        int64_t count = 0;
        while (count < kTotalItems) {
            if (q.try_dequeue(item)) {
                item.execute();
                ++count;
            } else {
                std::this_thread::yield();
            }
        }
        uint64_t t1 = now_ns();
        print_throughput("AffineWorkQueue dequeue 1M (1C)", kTotalItems, t1 - t0);
    }

    // --- Full pipeline: multi-producer + single-consumer concurrently ---
    {
        AffineWorkQueue q2(QueueType::kAffine, Priority::kCritical, "bm_affine2");
        std::atomic<int64_t> consumed{0};

        uint64_t t0 = now_ns();
        std::vector<std::thread> producers;
        for (int t = 0; t < kNumProducers; ++t) {
            producers.emplace_back([&q2]() {
                for (int i = 0; i < kItemsPerProducer; ++i) {
                    q2.enqueue(WorkItem::make_func(+[]() {}));
                }
            });
        }

        std::thread consumer([&q2, &consumed]() {
            WorkItem item;
            int64_t local = 0;
            while (local < kTotalItems) {
                if (q2.try_dequeue(item)) {
                    item.execute();
                    ++local;
                } else {
                    std::this_thread::yield();
                }
            }
            consumed.store(local, std::memory_order_release);
        });

        for (auto& th : producers) th.join();
        consumer.join();
        uint64_t t1 = now_ns();
        print_throughput("AffineWorkQueue pipeline 1M (4P1C)", kTotalItems, t1 - t0);
        EXPECT_EQ(consumed.load(), kTotalItems);
    }
}

// ============================================================================
// 4. Scheduler task processing throughput
//
// Pre-loads tasks, then starts scheduler to measure draining throughput.
// (Tasks cannot be submitted while scheduler is parked because there is
// no external notification path for the BatchedSPSC queue.)
// ============================================================================
TEST_F(BenchmarkTest, SchedulerThroughput) {
    constexpr int kNumTasks = 100'000;

    AdaptiveIdle idle;
    StrictPriorityPolicy policy(64);
    Scheduler scheduler(&policy, &idle);

    auto queue = std::make_unique<BatchedSPSCWorkQueue>(
        QueueType::kEngine, Priority::kCritical, "throughput_q", 200000);
    BatchedSPSCWorkQueue* q_ptr = queue.get();
    scheduler.register_queue(std::move(queue));

    static std::atomic<int64_t> s_count{0};
    s_count = 0;

    // Pre-load all tasks (before scheduler starts, so no idle park issue)
    constexpr int kBatchSize = 1024;
    {
        int remaining = kNumTasks;
        WorkItem batch[kBatchSize];
        while (remaining > 0) {
            int n = std::min(remaining, kBatchSize);
            for (int i = 0; i < n; ++i) {
                batch[i] = WorkItem::make_func(+[]() {
                    s_count.fetch_add(1, std::memory_order_relaxed);
                });
            }
            q_ptr->push_batch(batch, n);
            remaining -= n;
        }
    }

    uint64_t t0 = now_ns();
    std::thread scheduler_thread([&]() { folly::coro::blockingWait(scheduler.run()); });

    // Wait for all tasks to complete
    while (s_count.load() < kNumTasks) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    uint64_t t1 = now_ns();
    scheduler.request_stop();
    scheduler_thread.join();

    print_throughput("Scheduler throughput 100k tasks", kNumTasks, t1 - t0);
    EXPECT_EQ(s_count.load(), kNumTasks);
}

// ============================================================================
// 5. Worker startup latency
// ============================================================================
TEST_F(BenchmarkTest, WorkerStartupLatency) {
    constexpr int kNumSamples = 10;

    std::vector<uint64_t> startup_times_ns;
    startup_times_ns.reserve(kNumSamples);

    for (int i = 0; i < kNumSamples; ++i) {
        Worker::Config cfg;
        cfg.policy_cfg.name = "strict_priority";
        cfg.cpu_id = 0;
        Worker worker(cfg);

        // Add a queue with pre-loaded tasks so scheduler doesn't park
        auto q = std::make_unique<BatchedSPSCWorkQueue>(
            QueueType::kEngine, Priority::kCritical, "startup_q");
        WorkItem dummy[1] = { WorkItem::make_func(+[]() {}) };
        q->push_batch(dummy, 1);
        worker.add_queue(std::move(q));

        uint64_t t0 = now_ns();
        worker.start();
        worker.stop();
        worker.join();
        uint64_t t1 = now_ns();

        startup_times_ns.push_back(t1 - t0);
    }

    print_latency("Worker startup latency (start+stop+join)", startup_times_ns);
    EXPECT_GT(startup_times_ns.size(), 0u);
}

// ============================================================================
// 6. Runtime throughput: pre-load 50k tasks, measure processing time
// ============================================================================
TEST_F(BenchmarkTest, RuntimeThroughput) {
    constexpr int kNumTasks = 50'000;

    static std::atomic<int64_t> s_count{0};
    s_count = 0;

    // Use a Worker with large BatchedSPSCWorkQueue for thread-safe pre-load
    Worker::Config cfg;
    cfg.policy_cfg.name = "strict_priority";
    Worker worker(cfg);
    auto q = std::make_unique<BatchedSPSCWorkQueue>(
        QueueType::kEngine, Priority::kCritical, "rt_throughput", 200000);
    BatchedSPSCWorkQueue* q_ptr = q.get();
    worker.add_queue(std::move(q));

    // Pre-load tasks
    constexpr int kBatchSize = 1024;
    {
        int remaining = kNumTasks;
        WorkItem batch[kBatchSize];
        while (remaining > 0) {
            int n = std::min(remaining, kBatchSize);
            for (int i = 0; i < n; ++i) {
                batch[i] = WorkItem::make_func(+[]() {
                    s_count.fetch_add(1, std::memory_order_relaxed);
                });
            }
            q_ptr->push_batch(batch, n);
            remaining -= n;
        }
    }

    uint64_t t0 = now_ns();
    worker.start();

    // Wait for all tasks to complete
    while (s_count.load() < kNumTasks) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    uint64_t t1 = now_ns();
    worker.stop();
    worker.join();

    print_throughput("Runtime throughput 50k tasks", kNumTasks, t1 - t0);
    EXPECT_EQ(s_count.load(), kNumTasks);
}

// ============================================================================
// Summary header
// ============================================================================
class BenchmarkSummary : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        std::cout << "\n";
        std::cout << "====================================================\n";
        std::cout << "        STORAGE ENGINE BENCHMARK SUMMARY            \n";
        std::cout << "====================================================\n";
    }
};

TEST_F(BenchmarkSummary, PrintHeader) {
    std::cout << "Benchmarks executed. See individual test outputs above.\n";
    std::cout << "====================================================\n\n";
}


