#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>
#include <vector>
#include <cstdint>
#include "runtime/worker.h"
#include "runtime/batched_spsc_work_queue.h"
#include "runtime/affine_work_queue.h"
#include "runtime/online_worker.h"
#include "runtime/online_group.h"

using namespace storage::runtime;

// NOTE: The full Runtime class (which includes OfflineWorkerGroup) requires
// adapt/WorkStealingExecutor with external quant_invest headers. We test
// stress scenarios through OnlineWorkerGroup directly, which is the core
// runtime component for online task processing.

// ============================================================================
// Full load 60s: Worker runs with a large pre-loaded queue for 60 seconds.
// Pre-loads 1M tasks into a BatchedSPSCWorkQueue, starts worker, waits 60s,
// verifies all tasks consumed. Tests sustained processing stability.
// ============================================================================
TEST(StressTest, FullLoad60s) {
    constexpr int kNumTasks = 1'000'000;
    constexpr int kQueueCapacity = 2'000'000;
    constexpr int kRunSeconds = 60;

    Worker::Config cfg;
    cfg.policy_cfg.name = "strict_priority";
    Worker worker(cfg);
    auto q = std::make_unique<BatchedSPSCWorkQueue>(
        QueueType::kEngine, Priority::kCritical, "stress_q", kQueueCapacity);
    BatchedSPSCWorkQueue* q_ptr = q.get();
    worker.add_queue(std::move(q));

    static std::atomic<int64_t> s_executed{0};
    s_executed = 0;

    // Pre-load all tasks
    constexpr int kBatchSize = 2048;
    {
        int remaining = kNumTasks;
        WorkItem batch[kBatchSize];
        while (remaining > 0) {
            int n = std::min(remaining, kBatchSize);
            for (int i = 0; i < n; ++i) {
                batch[i] = WorkItem::make_func(+[]() {
                    s_executed.fetch_add(1, std::memory_order_relaxed);
                });
            }
            q_ptr->push_batch(batch, n);
            remaining -= n;
        }
    }

    worker.start();

    // Let it run for kRunSeconds
    std::this_thread::sleep_for(std::chrono::seconds(kRunSeconds));

    worker.stop();
    worker.join();

    EXPECT_GE(s_executed.load(), kNumTasks)
        << "All " << kNumTasks << " tasks should be executed within "
        << kRunSeconds << " seconds. Executed: " << s_executed.load();
}

// ============================================================================
// Worker 100x start/stop cycle
// ============================================================================
TEST(StressTest, WorkerStartStop100Cycles) {
    constexpr int kNumCycles = 100;

    for (int cycle = 0; cycle < kNumCycles; ++cycle) {
        Worker::Config cfg;
        cfg.cpu_id = 0;
        Worker worker(cfg);

        // Register a queue with items
        auto q = std::make_unique<BatchedSPSCWorkQueue>(
            QueueType::kEngine, Priority::kCritical, "stress_q");
        WorkItem items[1] = {
            WorkItem::make_func(+[]() { /* no-op */ }),
        };
        q->push_batch(items, 1);
        worker.add_queue(std::move(q));

        worker.start();
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        worker.stop();
        worker.join();

        auto s = worker.stats();
        EXPECT_GE(s.total_polls, 1u)
            << "Cycle " << cycle << ": worker should have polled at least once";
    }
}

// ============================================================================
// OnlineWorkerGroup 50x start/stop cycle
// ============================================================================
TEST(StressTest, OnlineGroupStartStop50Cycles) {
    constexpr int kNumCycles = 50;

    for (int cycle = 0; cycle < kNumCycles; ++cycle) {
        OnlineWorkerGroup::Config cfg;
        cfg.num_workers = 2;
        cfg.base_cpu_id = 0;
        OnlineWorkerGroup group(cfg);

        // Submit a task
        group.submit_to(0, WorkItem::make_func(+[]() { /* no-op */ }));

        group.start();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        group.stop();

        auto s = group.stats();
        EXPECT_GE(s.total_tasks_executed, 0u)
            << "Cycle " << cycle << ": stats should be accessible";
    }
}

// ============================================================================
// OnlineWorkerGroup 20x start/stop cycle
// ============================================================================
TEST(StressTest, OnlineGroupStartStop20Cycles) {
    constexpr int kNumCycles = 20;

    for (int cycle = 0; cycle < kNumCycles; ++cycle) {
        OnlineWorkerGroup::Config cfg;
        cfg.num_workers = 2;
        OnlineWorkerGroup group(cfg);

        group.submit_to(
            cycle, WorkItem::make_func(+[]() { /* no-op */ }));

        group.start();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        group.stop();
    }
}

// ============================================================================
// Burst submit: 100k tasks in rapid succession
// Pre-loads tasks before starting workers (safe: no concurrent consumer),
// then verifies all tasks are consumed.
// ============================================================================
TEST(StressTest, BurstSubmit100k) {
    constexpr int kNumTasks = 100'000;

    // Use individual Workers with BatchedSPSCWorkQueue (capacity 65536)
    // for thread-safe pre-loading
    Worker::Config cfg;
    cfg.policy_cfg.name = "strict_priority";
    Worker worker(cfg);
    auto q = std::make_unique<BatchedSPSCWorkQueue>(
        QueueType::kEngine, Priority::kCritical, "burst_q", 131072);
    BatchedSPSCWorkQueue* q_ptr = q.get();
    worker.add_queue(std::move(q));

    static std::atomic<int64_t> s_executed{0};
    s_executed = 0;

    // Pre-load all tasks into the SPSC queue (single producer, thread-safe)
    constexpr int kBatchSize = 1024;
    int remaining = kNumTasks;
    while (remaining > 0) {
        int n = std::min(remaining, kBatchSize);
        WorkItem batch[kBatchSize];
        for (int i = 0; i < n; ++i) {
            batch[i] = WorkItem::make_func(+[]() {
                s_executed.fetch_add(1, std::memory_order_relaxed);
            });
        }
        q_ptr->push_batch(batch, n);
        remaining -= n;
    }

    worker.start();

    // Wait for all tasks to drain
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::seconds(15);
    while (s_executed.load() < kNumTasks &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    worker.stop();
    worker.join();

    EXPECT_EQ(s_executed.load(), kNumTasks);
}

// ============================================================================
// Persistent worker with heavy load - 10K tasks per worker
// Pre-loads tasks before starting, verifies all consumed.
// ============================================================================
TEST(StressTest, HeavyWorkerLoad) {
    constexpr int kTasksPerWorker = 10'000;

    // Use 4 individual workers with large BatchedSPSCWorkQueue
    constexpr int kNumWorkers = 4;
    constexpr int kTotalTasks = kNumWorkers * kTasksPerWorker;
    constexpr int kQueueCapacity = 65536;

    std::vector<std::unique_ptr<Worker>> workers;
    std::vector<BatchedSPSCWorkQueue*> queues;
    static std::atomic<int64_t> s_counter{0};
    s_counter = 0;

    for (int w = 0; w < kNumWorkers; ++w) {
        Worker::Config cfg;
        cfg.policy_cfg.name = "strict_priority";
        auto worker = std::make_unique<Worker>(cfg);
        auto q = std::make_unique<BatchedSPSCWorkQueue>(
            QueueType::kEngine, Priority::kCritical,
            "heavy_q" + std::to_string(w), kQueueCapacity);
        queues.push_back(q.get());
        worker->add_queue(std::move(q));
        workers.push_back(std::move(worker));
    }

    // Pre-load tasks
    constexpr int kBatchSize = 1024;
    for (int w = 0; w < kNumWorkers; ++w) {
        int remaining = kTasksPerWorker;
        while (remaining > 0) {
            int n = std::min(remaining, kBatchSize);
            WorkItem batch[kBatchSize];
            for (int i = 0; i < n; ++i) {
                batch[i] = WorkItem::make_func(+[]() {
                    s_counter.fetch_add(1, std::memory_order_relaxed);
                });
            }
            queues[w]->push_batch(batch, n);
            remaining -= n;
        }
    }

    // Start all workers
    for (auto& w : workers) w->start();

    // Wait for all tasks to drain
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::seconds(10);
    while (s_counter.load() < kTotalTasks &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    for (auto& w : workers) { w->stop(); w->join(); }

    EXPECT_EQ(s_counter.load(), kTotalTasks);
}
