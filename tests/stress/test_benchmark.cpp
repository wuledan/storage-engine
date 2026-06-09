#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>
#include <vector>
#include <algorithm>
#include <numeric>
#include <cstdint>
#include <random>
#include <iostream>
#include <iomanip>
#include <cstdio>
#include <cstring>
#include <coroutine>
#include <sys/mman.h>
#include <mutex>
#include <folly/coro/Collect.h>
#include <x86intrin.h>
#include <emmintrin.h>
#include <fcntl.h>
#include <unistd.h>
#include <folly/coro/BlockingWait.h>
#include "runtime/local_queue.h"
#include "io/io_engine.h"
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
using namespace storage::io;

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

// ============================================================================
// 7. 协程提交 + 调度的端到端延迟
//    测量 submit_engine → task execute 的完整延迟
// ============================================================================
TEST(BenchmarkCoroutine, E2ECoroutineSubmitLatency) {
    using namespace std::chrono;
    Worker::Config cfg;
    OnlineWorker w(cfg);

    constexpr size_t kSamples = 10000;
    std::vector<uint64_t> latencies(kSamples);

    w.start();

    for (size_t i = 0; i < kSamples; ++i) {
        auto t0 = steady_clock::now();
        static std::atomic<bool> done{false};
        done = false;

        w.submit_engine(WorkItem::make_func([]() {
            done.store(true);
        }));
        w.notify();  // 唤醒 worker 处理任务

        // 轮询等待完成
        while (!done.load()) {
            // busy wait (调度器在同一个worker线程，需要yield)
            std::this_thread::yield();
        }

        auto t1 = steady_clock::now();
        latencies[i] = duration_cast<nanoseconds>(t1 - t0).count();
    }

    w.stop();
    w.join();

    std::sort(latencies.begin(), latencies.end());

    std::cout << "\n=== Coroutine E2E Submit Latency ===" << std::endl;
    std::cout << "  P50: " << latencies[kSamples/2] / 1000.0 << " us" << std::endl;
    std::cout << "  P99: " << latencies[kSamples*99/100] / 1000.0 << " us" << std::endl;
    std::cout << "  P999: " << latencies[kSamples*999/1000] / 1000.0 << " us" << std::endl;
}

// ============================================================================
// 8. OnlineWorker 批量任务吞吐
// ============================================================================
TEST(BenchmarkCoroutine, BulkTaskThroughput) {
    Worker::Config cfg;
    OnlineWorker w(cfg);
    constexpr size_t kTotal = 100000;
    static std::atomic<size_t> done_count{0};
    done_count = 0;

    w.start();

    auto t0 = std::chrono::steady_clock::now();

    // 批量 submit (BatchedSPSCWorkQueue 容量足够，不会丢任务)
    for (size_t i = 0; i < kTotal; ++i) {
        w.submit_engine(WorkItem::make_func([]() {
            done_count.fetch_add(1);
        }));
    }
    w.notify();  // 唤醒 worker 处理

    // 等待全部完成 (轮询)
    while (done_count.load() < kTotal) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
        w.notify();  // 轮询时持续唤醒，防止 worker 陷入 park
    }

    auto t1 = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
    double ops_per_sec = kTotal * 1e6 / elapsed;

    w.stop();
    w.join();

    std::cout << "\n=== OnlineWorker Bulk Task Throughput ===" << std::endl;
    std::cout << "  " << kTotal << " tasks in " << elapsed << " us" << std::endl;
    std::cout << "  Throughput: " << (ops_per_sec / 1e6) << " M ops/s" << std::endl;
}

// ============================================================================
// 9. Worker start/stop 生命周期延迟
// ============================================================================
TEST(BenchmarkCoroutine, WorkerLifecycleLatency) {
    constexpr size_t kCycles = 100;
    std::vector<uint64_t> start_latencies(kCycles);
    std::vector<uint64_t> stop_latencies(kCycles);

    for (size_t i = 0; i < kCycles; ++i) {
        Worker::Config cfg;
        cfg.cpu_id = 0; // 不绑定CPU
        OnlineWorker w(cfg);

        auto t0 = std::chrono::steady_clock::now();
        w.start();
        auto t1 = std::chrono::steady_clock::now();

        w.stop();
        w.join();
        auto t2 = std::chrono::steady_clock::now();

        start_latencies[i] = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
        stop_latencies[i] = std::chrono::duration_cast<std::chrono::nanoseconds>(t2 - t1).count();
    }

    std::sort(start_latencies.begin(), start_latencies.end());
    std::sort(stop_latencies.begin(), stop_latencies.end());

    std::cout << "\n=== Worker Lifecycle Latency (" << kCycles << " cycles) ===" << std::endl;
    std::cout << "  Start P50: " << start_latencies[kCycles/2] / 1000.0 << " us" << std::endl;
    std::cout << "  Start P99: " << start_latencies[kCycles*99/100] / 1000.0 << " us" << std::endl;
    std::cout << "  Stop  P50: " << stop_latencies[kCycles/2] / 1000.0 << " us" << std::endl;
    std::cout << "  Stop  P99: " << stop_latencies[kCycles*99/100] / 1000.0 << " us" << std::endl;
}

// ============================================================================
// BenchmarkDetail: 细分延迟分析 (使用 rdtsc 获取高精度时间戳)
// ============================================================================
#include "runtime/worker_perf.h"

// ── 全局桥接状态（单线程 benchmark，无竞争） ──
static uint64_t* g_bench_result{nullptr};
static std::atomic<bool> g_bench_done{false};

static void bench_record_elapsed() {
    if (g_bench_result) {
        *g_bench_result = __builtin_ia32_rdtsc();
    }
    g_bench_done.store(true);
}

static void bench_set_true() {
    g_bench_done.store(true);
}

// ===== 纯调度延迟（Worker 内部，无跨线程唤醒） =====
TEST(BenchmarkDetail, SchedulingLatencyInternal) {
    Worker::Config cfg;
    OnlineWorker w(cfg);

    constexpr size_t kSamples = 100000;
    std::vector<uint64_t> latencies(kSamples);

    w.start();

    for (size_t i = 0; i < kSamples; ++i) {
        uint64_t t0 = __builtin_ia32_rdtsc();
        g_bench_result = &latencies[i];
        g_bench_done = false;

        w.submit_engine(WorkItem::make_func(bench_record_elapsed));
        w.notify();

        while (!g_bench_done.load()) { _mm_pause(); }
        latencies[i] = latencies[i] - t0;
    }

    w.stop();
    w.join();

    std::sort(latencies.begin(), latencies.end());
    double ghz = 3.0;

    std::cout << "\n=== Internal Scheduling Latency (submit_engine \xE2\x86\x92 execute, active worker) ===" << std::endl;
    std::cout << "  P50:   " << (latencies[kSamples/2] / ghz) << " ns" << std::endl;
    std::cout << "  P99:   " << (latencies[kSamples*99/100] / ghz) << " ns" << std::endl;
    std::cout << "  P999:  " << (latencies[kSamples*999/1000] / ghz) << " ns" << std::endl;
    std::cout << "  P9999: " << (latencies[kSamples*9999/10000] / ghz) << " ns" << std::endl;
}

// ===== 排队延迟（用 perf counter） =====
TEST(BenchmarkDetail, QueueWaitLatency) {
    Worker::Config cfg;
    OnlineWorker w(cfg);
    w.set_perf_level(PerfLevel::kTrace);

    constexpr size_t kSamples = 50000;

    w.start();

    for (size_t i = 0; i < kSamples; ++i) {
        uint64_t t0 = __builtin_ia32_rdtsc();
        WorkItem item = WorkItem::make_func([]() {});
        item.enqueue_ns = t0;  // 手动记录入队时间

        w.submit_engine(std::move(item));
        w.notify();
    }

    // 等待调度器处理完所有任务
    g_bench_done = false;
    w.submit_engine(WorkItem::make_func(bench_set_true));
    w.notify();
    while (!g_bench_done.load()) { _mm_pause(); }

    w.stop();
    w.join();

    auto snap = w.perf().snapshot();
    for (auto& q : snap.queues) {
        if (q.type == QueueType::kEngine) {
            std::cout << "\n=== Queue Wait Latency (perf counter) ===" << std::endl;
            std::cout << "  enqueued: " << q.enqueued << std::endl;
            std::cout << "  avg_wait: " << q.avg_wait_ns << " ns" << std::endl;
            std::cout << "  max_wait: " << q.max_wait_ns << " ns" << std::endl;
            std::cout << "  P50_wait: " << q.p50_wait_ns / 3.0 << " ns (rdtsc)" << std::endl;
            std::cout << "  P99_wait: " << q.p99_wait_ns / 3.0 << " ns (rdtsc)" << std::endl;
        }
    }
}

// ===== 跨线程往返延迟 =====

// 场景 A: Worker 已激活（非 idle）
TEST(BenchmarkDetail, CrossThreadActiveWorker) {
    Worker::Config cfg;
    OnlineWorker w(cfg);
    w.start();

    // 先投递一个任务让 worker 脱离 idle 状态
    g_bench_done = false;
    w.submit_engine(WorkItem::make_func(bench_set_true));
    w.notify();
    while (!g_bench_done.load()) { _mm_pause(); }

    constexpr size_t kSamples = 10000;
    std::vector<uint64_t> latencies(kSamples);

    for (size_t i = 0; i < kSamples; ++i) {
        uint64_t t0 = __builtin_ia32_rdtsc();
        g_bench_result = &latencies[i];
        g_bench_done = false;

        w.submit_engine(WorkItem::make_func(bench_record_elapsed));
        w.notify();

        while (!g_bench_done.load()) { _mm_pause(); }
        latencies[i] = latencies[i] - t0;
    }

    w.stop();
    w.join();

    std::sort(latencies.begin(), latencies.end());
    double ghz = 3.0;

    std::cout << "\n=== Cross-Thread RTT (Worker ACTIVE) ===" << std::endl;
    std::cout << "  P50:  " << (latencies[kSamples/2] / ghz) << " ns" << std::endl;
    std::cout << "  P99:  " << (latencies[kSamples*99/100] / ghz) << " ns" << std::endl;
    std::cout << "  P999: " << (latencies[kSamples*999/1000] / ghz) << " ns" << std::endl;
}

// 场景 B: Worker 空闲（PARK 状态），测量唤醒延迟
TEST(BenchmarkDetail, CrossThreadIdleWorker) {
    Worker::Config cfg;
    OnlineWorker w(cfg);
    w.start();

    // 等待 worker 进入 idle (PARK)
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    constexpr size_t kSamples = 1000;
    std::vector<uint64_t> latencies(kSamples);

    for (size_t i = 0; i < kSamples; ++i) {
        uint64_t t0 = __builtin_ia32_rdtsc();
        g_bench_result = &latencies[i];
        g_bench_done = false;

        w.submit_engine(WorkItem::make_func(bench_record_elapsed));
        w.notify();

        while (!g_bench_done.load()) { _mm_pause(); }
        latencies[i] = latencies[i] - t0;

        // 等待再次 idle
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    w.stop();
    w.join();

    std::sort(latencies.begin(), latencies.end());
    double ghz = 3.0;

    std::cout << "\n=== Cross-Thread RTT (Worker IDLE/PARKED) ===" << std::endl;
    std::cout << "  P50:  " << (latencies[kSamples/2] / ghz) << " ns" << std::endl;
    std::cout << "  P99:  " << (latencies[kSamples*99/100] / ghz) << " ns" << std::endl;
    std::cout << "  P999: " << (latencies[kSamples*999/1000] / ghz) << " ns" << std::endl;
}

// ============================================================================
// Mode 1 (Same-Core) + Mode 2 (Cross-Thread) IO Benchmarks
// Mode 1: SimpleCoro 在 Worker 线程上通过 Scheduler 调度执行
// Mode 2: 测试线程直接 submit + Scheduler poll 完成
// ============================================================================

// 轻量级 C++20 协程，提交到 Scheduler 的 P0 affine 队列
struct SimpleCoro {
    struct promise_type {
        std::coroutine_handle<> continuation{std::noop_coroutine()};
        std::atomic<bool>* done{nullptr};

        SimpleCoro get_return_object() {
            return SimpleCoro{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_never initial_suspend() noexcept { return {}; }
        auto final_suspend() noexcept {
            struct FinalAw {
                bool await_ready() noexcept { return false; }
                void await_resume() noexcept {}
                std::coroutine_handle<> await_suspend(std::coroutine_handle<promise_type> h) noexcept {
                    if (h.promise().done) h.promise().done->store(true);
                    return std::noop_coroutine();
                }
            };
            return FinalAw{};
        }
        void return_void() noexcept {}
        void unhandled_exception() noexcept { std::terminate(); }
    };
    std::coroutine_handle<promise_type> handle;
    operator std::coroutine_handle<>() const noexcept { return handle; }
};

// 轻量级 IO awaitable: 提交 IO → co_await baton → baton.post(worker_route) → Scheduler 恢复
struct IOWriteAwaitable {
    storage::io::IIOBackend* backend;
    int fd; uint64_t offset; void* buf; size_t len;
    OnlineWorker* worker;

    bool await_ready() noexcept { return false; }

    void await_suspend(std::coroutine_handle<> h) noexcept {
        auto route = worker->make_route_func();

        storage::io::IORequest req;
        req.op = storage::io::IORequest::kWrite;
        req.fd = fd; req.offset = offset; req.buf = buf; req.len = len;
        req.callback = [h, route](storage::io::IOCompletion) {
            route(0, h);  // enqueue_affine → Scheduler drain_p0 → resume
        };
        backend->submit(std::move(req));
    }

    storage::io::IOCompletion await_resume() noexcept { return {0, 0, {}}; }
};
// 注意：以上是简化版，await_suspend 返回 void 后协程已暂停，
// baton.post 时通过 route → enqueue_affine → Scheduler → resume

// ============================================================================
// 协程流水线 — Worker 线程直接提交 + Scheduler poll
// 使用 IO backend 直接 submit/poll，通过 Worker 创建的文件描述符
// ============================================================================

TEST(BenchmarkIO, CoroutinePipeline) {
    std::vector<int> qds = {1, 4, 16, 64, 128};

    for (const auto& type : {"io_uring", "libaio"}) {
        // 直接创建 backend（不通过 Worker，防止并发 poll 冲突）
        std::unique_ptr<IIOBackend> backend;
        try { backend = IOEngine::create({"io_uring", 256}, nullptr); }
        catch(...) { continue; }

        std::string path = "/mnt/nvme_test/bench_coro_" + std::string(type);
        int fd = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_DIRECT, 0644);
        ASSERT_GE(fd, 0);
        void* fill; posix_memalign(&fill, 4096, 4096); memset(fill, 0, 4096);
        pwrite(fd, fill, 4096, 0); pwrite(fd, fill, 4096, 1024UL*1024*1024-4096); free(fill);
        void* buf; posix_memalign(&buf, 4096, 4096); memset(buf, 'X', 4096);

        std::cout << "\n=== " << type << " Direct Pipeline (standalone IO backend) ===" << std::endl;
        std::cout << "  QD    |  IOPS(K)  |  P50(us)  |  P99(us)  |  BW(MB/s)" << std::endl;
        std::cout << "  ------|-----------|-----------|-----------|----------" << std::endl;

        for (int qd : qds) {
            const size_t total_ops = (qd <= 16) ? 2000 : 10000;
            std::vector<uint64_t> latencies(total_ops, 0);
            std::atomic<size_t> submitted{0}, completed{0};

            auto t_start = std::chrono::steady_clock::now();
            std::thread io_thread([&]() {
                // 初始填充 QD
                for (size_t i = 0; i < (size_t)qd && i < total_ops; ++i) {
                    size_t s = submitted.fetch_add(1);
                    uint64_t t0 = __builtin_ia32_rdtsc();
                    IORequest req; req.op = IORequest::kWrite; req.fd = fd;
                    req.offset = s * 4096ULL; req.buf = buf; req.len = 4096;
                    req.callback = [&completed, &latencies, s, t0](IOCompletion c) {
                        if (c.result == (int64_t)4096) latencies[s] = __builtin_ia32_rdtsc() - t0;
                        completed.fetch_add(1);
                    };
                    backend->submit(std::move(req));
                }
                backend->flush_pending();
                backend->flush_submissions();

                IOCompletion comps[64];
                while (completed.load() < total_ops) {
                    size_t n = backend->poll(comps, 64);
                    for (size_t i = 0; i < n; ++i)
                        if (comps[i].callback) comps[i].callback(comps[i]);

                    // 保持 QD 流水线
                    while (submitted.load() - completed.load() < (size_t)qd && submitted.load() < total_ops) {
                        size_t s = submitted.fetch_add(1);
                        uint64_t t0 = __builtin_ia32_rdtsc();
                        IORequest req; req.op = IORequest::kWrite; req.fd = fd;
                        req.offset = s * 4096ULL; req.buf = buf; req.len = 4096;
                        req.callback = [&completed, &latencies, s, t0](IOCompletion c) {
                            if (c.result == (int64_t)4096) latencies[s] = __builtin_ia32_rdtsc() - t0;
                            completed.fetch_add(1);
                        };
                        backend->submit(std::move(req));
                    }
                    backend->flush_pending();
                    backend->flush_submissions();
                }
            });

            io_thread.join();

            auto t1 = std::chrono::steady_clock::now();
            double elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t_start).count();
            std::sort(latencies.begin(), latencies.end());
            double ghz = 3.0;
            size_t valid_n = 0;
            for (size_t i = 0; i < total_ops; ++i) if (latencies[i] > 0) valid_n = i + 1;
            if (valid_n == 0) continue;
            double iops = completed.load() / (elapsed_us / 1e6);
            double bw = iops * 4096 / (1024.0 * 1024.0);
            printf("  %-4d  |  %7.1f  |  %7.2f  |  %7.2f  |  %6.0f\n",
                   qd, iops/1000.0, (latencies[valid_n/2]/ghz/1000.0),
                   (latencies[valid_n*99/100]/ghz/1000.0), bw);
        }
        close(fd); unlink(path.c_str()); free(buf);
    }
}
