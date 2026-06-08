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
#include <cstdio>
#include <cstring>
#include <sys/mman.h>
#include <mutex>
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
// Block Size × Queue Depth 标准测试矩阵
// ============================================================================

struct QDResult {
    int queue_depth;
    double iops;
    uint64_t p50_ns;
    uint64_t p99_ns;
    uint64_t p999_ns;
    uint64_t avg_ns;
};

// 辅助：分配对齐缓冲区
void* alloc_aligned_buffer(size_t size) {
    void* buf = nullptr;
    posix_memalign(&buf, 4096, size);
    std::memset(buf, 'A', size);
    return buf;
}

// 辅助：为指定 block size 创建临时文件
int create_test_file(size_t block_size) {
    std::string path = "/tmp/bench_" + std::to_string(block_size);
    int fd = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return -1;
    void* buf = alloc_aligned_buffer(block_size);
    pwrite(fd, buf, block_size, 0);
    free(buf);
    fsync(fd);
    return fd;
}

// 运行单组 benchmark：(backend, block_size, queue_depth) → QDResult
static QDResult run_block_benchmark(OnlineWorker& w, int fd, size_t block_size,
                                     int queue_depth, size_t total_ops) {
    std::vector<uint64_t> latencies(total_ops);
    std::atomic<size_t> submitted{0};
    std::atomic<size_t> completed{0};
    std::atomic<bool> stop{false};

    void* buf = alloc_aligned_buffer(block_size);
    std::thread submitter([&]() {
        while (!stop.load() && submitted.load() < total_ops) {
            if (submitted.load() - completed.load() < static_cast<size_t>(queue_depth)) {
                size_t slot = submitted.fetch_add(1);
                if (slot >= total_ops) break;
                uint64_t t0 = __builtin_ia32_rdtsc();
                IORequest req;
                req.op = IORequest::kWrite;
                req.fd = fd;
                req.offset = (slot % 1000) * block_size;
                req.buf = buf;
                req.len = block_size;
                req.callback = [&completed, &latencies, slot, t0](IOCompletion c) {
                    if (c.result > 0) latencies[slot] = __builtin_ia32_rdtsc() - t0;
                    completed.fetch_add(1);
                };
                w.io_backend()->submit(std::move(req));
            } else {
                _mm_pause();
            }
        }
        stop.store(true);
    });

    while (completed.load() < total_ops) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    stop.store(true);
    submitter.join();
    free(buf);

    std::sort(latencies.begin(), latencies.end());
    double ghz = 3.0;
    QDResult r;
    r.queue_depth = queue_depth;
    uint64_t avg_lat = std::accumulate(latencies.begin(), latencies.end(), 0ULL) / total_ops;
    r.p50_ns = static_cast<uint64_t>(latencies[total_ops/2] / ghz);
    r.p99_ns = static_cast<uint64_t>(latencies[total_ops*99/100] / ghz);
    r.p999_ns = static_cast<uint64_t>(latencies[total_ops*999/1000] / ghz);
    r.avg_ns = static_cast<uint64_t>(avg_lat / ghz);
    r.iops = static_cast<double>(queue_depth) / (r.avg_ns / 1e9);
    return r;
}

// io_uring: block size × QD 矩阵 (IOPS)
TEST(BenchmarkIO, IoUringBlockSizeMatrix) {
    Worker::Config cfg;
    cfg.cpu_id = 1;
    OnlineWorker w(cfg);
    w.init_io_backend({"io_uring", 256});
    w.start();

    std::vector<size_t> sizes = {1024, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576};
    std::vector<int> qds = {1, 4, 16, 64, 128};

    std::cout << "\n=== io_uring Block Size × Queue Depth Matrix ===" << std::endl;
    std::cout << "  BS/QD  |";
    for (int qd : qds) std::cout << "  QD=" << qd << "  |";
    std::cout << "\n  -------|";
    for (size_t i = 0; i < qds.size(); ++i) std::cout << "---------|";
    std::cout << std::endl;

    for (size_t bs : sizes) {
        int fd = create_test_file(bs);
        ASSERT_GE(fd, 0);

        std::cout << "  " << std::setw(5) << (bs/1024) << "K  |";
        for (int qd : qds) {
            size_t ops = (qd <= 4) ? 500 : 2000;
            auto r = run_block_benchmark(w, fd, bs, qd, ops);
            double iops_k = r.iops / 1000.0;
            std::cout << std::setw(6) << std::fixed << std::setprecision(1)
                      << iops_k << "K |";
        }
        std::cout << std::endl;
        close(fd);
        std::string path = "/tmp/bench_" + std::to_string(bs);
        unlink(path.c_str());
    }

    w.stop();
    w.join();
}

// io_uring: 延迟矩阵 (P50/P99 by block size at QD=1 and QD=128)
TEST(BenchmarkIO, IoUringLatencyMatrix) {
    Worker::Config cfg;
    cfg.cpu_id = 1;
    OnlineWorker w(cfg);
    w.init_io_backend({"io_uring", 256});
    w.start();

    std::vector<size_t> sizes = {1024, 4096, 16384, 65536, 262144, 1048576};

    std::cout << "\n=== io_uring Latency by Block Size ===" << std::endl;
    std::cout << "  BS    |  QD  |  P50(us)  |  P99(us)  |  IOPS(K) |  BW(MB/s)" << std::endl;
    std::cout << "  ------|------|-----------|-----------|----------|----------" << std::endl;

    for (size_t bs : sizes) {
        for (int qd : {1, 128}) {
            int fd = create_test_file(bs);
            ASSERT_GE(fd, 0);
            size_t ops = (qd == 1) ? 500 : 2000;
            auto r = run_block_benchmark(w, fd, bs, qd, ops);
            double bw = (r.iops * bs) / (1024.0 * 1024.0);

            std::cout << "  " << std::setw(4) << (bs/1024) << "K |"
                      << "  " << std::setw(2) << qd << "  |"
                      << std::setw(8) << std::fixed << std::setprecision(2)
                      << (r.p50_ns/1000.0) << " |"
                      << std::setw(8) << (r.p99_ns/1000.0) << " |"
                      << std::setw(7) << std::setprecision(1) << (r.iops/1000.0) << " |"
                      << std::setw(7) << std::setprecision(1) << bw << std::endl;

            close(fd);
            std::string path = "/tmp/bench_" + std::to_string(bs);
            unlink(path.c_str());
        }
    }

    w.stop();
    w.join();
}

// libaio: block size × QD 矩阵
TEST(BenchmarkIO, LibaioBlockSizeMatrix) {
    Worker::Config cfg;
    cfg.cpu_id = 1;
    OnlineWorker w(cfg);
    IOBackendConfig io_cfg;
    io_cfg.type = "libaio";
    io_cfg.queue_depth = 256;
    bool created = false;
    try { w.init_io_backend(io_cfg); created = true; }
    catch (...) {}
    if (!created) { std::cout << "[Bench] libaio not available\n"; return; }
    w.start();

    std::vector<size_t> sizes = {1024, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576};
    std::vector<int> qds = {1, 4, 16, 64, 128};

    std::cout << "\n=== libaio Block Size × Queue Depth Matrix ===" << std::endl;
    std::cout << "  BS/QD  |";
    for (int qd : qds) std::cout << "  QD=" << qd << "  |";
    std::cout << "\n  -------|";
    for (size_t i = 0; i < qds.size(); ++i) std::cout << "---------|";
    std::cout << std::endl;

    for (size_t bs : sizes) {
        int fd = create_test_file(bs);
        ASSERT_GE(fd, 0);
        std::cout << "  " << std::setw(5) << (bs/1024) << "K  |";
        for (int qd : qds) {
            size_t ops = (qd <= 4) ? 500 : 2000;
            auto r = run_block_benchmark(w, fd, bs, qd, ops);
            double iops_k = r.iops / 1000.0;
            std::cout << std::setw(6) << std::fixed << std::setprecision(1)
                      << iops_k << "K |";
        }
        std::cout << std::endl;
        close(fd);
        std::string path = "/tmp/bench_" + std::to_string(bs);
        unlink(path.c_str());
    }

    w.stop();
    w.join();
}
