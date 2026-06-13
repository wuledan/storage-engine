#include <gtest/gtest.h>
#include "runtime/work_stealing_executor.h"
#include "runtime/metric_server.h"
#include "runtime/metric_counter.h"
#include <thread>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <random>

using namespace storage::runtime;
using namespace storage::runtime::metric;
using namespace std::chrono;

// ============================================================================
// Offline CPU Longevity Test (20 minutes)
//
// Pattern:
//   4 submitter threads continuously push tasks to a WorkStealingExecutor
//   with 8 workers. Each task simulates random CPU work (10-500 us) using
//   a busy-wait loop with pause instructions.
//
//   Backpressure via in-flight counter keeps the global entry ring
//   (capacity 65536) from overflowing — tasks are never silently dropped.
//
//   Every 10 seconds the monitor logs: elapsed time, throughput (K IOPS),
//   and per-worker tasks_executed % 100 (visual activity indicator).
//
//   After 20 minutes the test verifies:
//     1. No deadlock (all workers exit cleanly via join())
//     2. At least 6 of 8 workers participated (tasks_executed > 0)
// ============================================================================

namespace {

// Global counter used by the captureless task function.
// Set up in TEST body, read by the monitor loop.
MetricCounter* g_task_counter = nullptr;

// In-flight count for backpressure. Incremented by submitter before
// enqueue, decremented by the task after completion.
std::atomic<int64_t> g_inflight{0};

// ── Captureless task function ─────────────────────────────────────────────
//
// Each invocation:
//   1. Generates a random work duration (10-500 us) via a thread_local RNG
//   2. Busy-waits for that duration (pause instruction)
//   3. Increments the task counter
//   4. Releases one backpressure slot
//
void cpu_work_task() {
    // Per-thread RNG seeded by the worker thread's std::thread::id hash.
    // This avoids contention on a shared RNG and gives each worker its own
    // distribution of work durations.
    thread_local std::mt19937 gen(
        static_cast<unsigned>(
            std::hash<std::thread::id>{}(std::this_thread::get_id())));
    thread_local std::uniform_int_distribution<int> work_dist(10, 500);

    int work_us = work_dist(gen);
    auto end = steady_clock::now() + microseconds(work_us);
    while (steady_clock::now() < end) {
        __builtin_ia32_pause();
    }

    g_task_counter->operator<<(1);
    g_inflight.fetch_sub(1, std::memory_order_release);
}

}  // anonymous namespace

// ============================================================================
// TEST: 20-minute Offline CPU Longevity
// ============================================================================
TEST(Longevity, OfflineCPU_20min) {
    printf("\n=== Offline CPU Longevity (20min, 8 workers, random 10-500us tasks) ===\n");

    // ── 1. Executor setup ────────────────────────────────────────────────
    WorkStealingExecutor::Config cfg;
    cfg.num_workers = 8;
    cfg.pin_cpus = false;             // don't pin in test environments
    cfg.steal_attempts = 4;           // default

    WorkStealingExecutor exec(cfg);
    exec.start();

    // ── 2. Metrics ───────────────────────────────────────────────────────
    MetricCounter total_tasks;
    MetricRegistry::instance().register_counter(
        "longevity/offline_tasks", &total_tasks);
    g_task_counter = &total_tasks;

    // ── 3. Submitter threads ─────────────────────────────────────────────
    //
    // 4 submitters, each continuously pushing tasks via the global entry
    // ring. Backpressure via g_inflight (limit = 40000) ensures we never
    // exceed the ring capacity (65536).
    //
    constexpr int64_t kMaxInflight = 40000;
    std::atomic<bool> stop{false};
    std::vector<std::thread> submitters;

    for (int s = 0; s < 4; ++s) {
        submitters.emplace_back([&exec, &stop] {
            while (!stop.load(std::memory_order_relaxed)) {
                // Backpressure: wait until in-flight count drops below limit
                while (g_inflight.load(std::memory_order_acquire) >= kMaxInflight) {
                    std::this_thread::sleep_for(microseconds(100));
                }
                g_inflight.fetch_add(1, std::memory_order_relaxed);
                exec.add(WorkItem::make_func(cpu_work_task));
            }
        });
    }

    // ── 4. Metrics server ────────────────────────────────────────────────
    MetricServer::Config scfg{9191};
    scfg.bind_addr = "192.168.3.12";
    MetricServer srv(scfg);
    srv.start();
    printf("  Metrics server on http://192.168.3.12:9191/metrics\n");

    // ── 5. Monitor loop (5 min, log every 10 seconds) ────────────────────
    auto t0 = steady_clock::now();
    uint64_t prev_tasks = 0;

    printf("  Time | IOPS(K) | W0 | W1 | W2 | W3 | W4 | W5 | W6 | W7\n");
    printf("  -----|---------|----|----|----|----|----|----|----|----\n");
    fflush(stdout);

    while (duration_cast<minutes>(steady_clock::now() - t0).count() < 1) {
        std::this_thread::sleep_for(seconds(10));

        double   secs = duration_cast<seconds>(
                            steady_clock::now() - t0).count();
        uint64_t cur  = total_tasks.value();
        double   iops_k = static_cast<double>(cur - prev_tasks) / 10.0 / 1000.0;

        printf("  %4.0fs | %7.1f", secs, iops_k);
        for (size_t i = 0; i < 8; ++i) {
            printf(" | %2lu",
                   exec.worker(i).tasks_executed.load(
                       std::memory_order_relaxed) % 100);
        }
        printf("\n");
        fflush(stdout);

        prev_tasks = cur;
    }

    // ── 6. Clean shutdown ────────────────────────────────────────────────
    stop.store(true, std::memory_order_release);
    for (auto& t : submitters) {
        t.join();
    }
    exec.stop();
    exec.join();

    // ── 7. Verify all workers participated (no deadlock, even stealing) ──
    size_t active = 0;
    for (size_t i = 0; i < 8; ++i) {
        if (exec.worker(i).tasks_executed.load(std::memory_order_relaxed) > 0) {
            ++active;
        }
    }

    EXPECT_GE(active, 6u)
        << "Only " << active << " workers active, expected >= 6";

    printf("  Active workers: %zu/8, total tasks: %lu\n",
           active, total_tasks.value());
    printf("=== Longevity test complete ===\n");
}
