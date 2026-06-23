#include <gtest/gtest.h>
#include "runtime/work_stealing_executor.h"
#include "runtime/offline_group.h"
#include <thread>
#include <atomic>
#include <chrono>
#include <vector>
#include <algorithm>
#include <numeric>
#include <cstdio>

using namespace storage::runtime;
using namespace std::chrono;

// ── bthread-style work-stealing benchmark ──
//
// Test pattern: N external threads submit M tasks each to the executor.
// Each task does a tiny amount of work (atomic increment).
// Measures: total throughput, per-task latency, steal statistics.
//
// Similar to: brpc/docs/cn/bthread_benchmark.md

struct BenchStats {
    std::atomic<uint64_t> tasks_completed{0};
    std::atomic<uint64_t> total_latency_ns{0};
    std::atomic<bool> running{true};
};

// ── Steal-efficiency test helpers ──
// Global pointer used by the captureless function below so tasks pushed to a
// worker's local deque can bump a counter without capture.
static std::atomic<uint64_t>* g_steal_counter{nullptr};

static void steal_bench_task() {
    g_steal_counter->fetch_add(1, std::memory_order_relaxed);
}

// ── Uneven-load test helpers ──
static std::atomic<size_t>* g_ue_completed{nullptr};
static int g_ue_work_us{0};

static void uneven_load_task() {
    if (g_ue_work_us > 0) {
        auto end = std::chrono::high_resolution_clock::now() +
                   std::chrono::microseconds(g_ue_work_us);
        while (std::chrono::high_resolution_clock::now() < end)
            __builtin_ia32_pause();
    }
    g_ue_completed->fetch_add(1, std::memory_order_relaxed);
}

// Global in-flight counter for submitter backpressure.
// The global entry ring has capacity 65536; we keep in-flight ≤ 40000
// to avoid silently losing tasks when the queue overflows.
static std::atomic<int64_t> g_submit_inflight{0};

// Single submitter thread: repeatedly submits tasks via add(std::function)
// (lambdas with captures are allowed — they get wrapped in a coroutine internally).
static void submitter(WorkStealingExecutor* exec, BenchStats* stats,
                      size_t total_tasks, int work_us) {
    for (size_t i = 0; i < total_tasks && stats->running.load(); ++i) {
        // Backpressure: wait if too many tasks are queued but not yet consumed.
        // The global entry ring has capacity 65536. Keeping in-flight ≤ 40000
        // (with 4 concurrent submitters the worst-case overshoot is ~4) ensures
        // we never fill the ring and silently drop tasks.
        while (g_submit_inflight.load(std::memory_order_acquire) > 40000) {
            std::this_thread::sleep_for(microseconds(100));
        }
        g_submit_inflight.fetch_add(1, std::memory_order_relaxed);

        auto t0 = high_resolution_clock::now();
        exec->add(std::function<void()>{[stats, t0, work_us]() {
            auto t1 = high_resolution_clock::now();
            auto lat = duration_cast<nanoseconds>(t1 - t0).count();
            stats->total_latency_ns.fetch_add(lat, std::memory_order_relaxed);
            stats->tasks_completed.fetch_add(1, std::memory_order_relaxed);
            g_submit_inflight.fetch_sub(1, std::memory_order_release);

            // Simulate work
            if (work_us > 0) {
                auto end = t1 + microseconds(work_us);
                while (high_resolution_clock::now() < end)
                    __builtin_ia32_pause();
            }
        }});
    }
}

TEST(WorkStealingBench, ThroughputScaling) {
    printf("\n=== Work-Stealing Throughput Scaling ===\n");

    std::vector<int> workloads = {0, 10, 50, 100};  // us of work per task
    std::vector<size_t> worker_counts = {1, 2, 4, 8};

    for (int work_us : workloads) {
        printf("\n--- Work=%dus per task ---\n", work_us);
        fflush(stdout);
        printf("  Workers | Submitters | Tasks   | Time(s) | IOPS(K) | AvgLat(us) | Steal%%\n");
        printf("  --------|------------|---------|---------|----------|------------|-------\n");
        fflush(stdout);

        for (size_t nw : worker_counts) {
            WorkStealingExecutor::Config cfg;
            cfg.num_workers = nw;
            cfg.pin_cpus = false;
            cfg.steal_attempts = 2;

            WorkStealingExecutor exec(cfg);
            exec.start();

            const size_t total_tasks = 100000;
            const int submitter_count = 4;
            BenchStats stats;

            auto t0 = high_resolution_clock::now();

            std::vector<std::thread> submitters;
            for (int s = 0; s < submitter_count; ++s) {
                submitters.emplace_back(submitter, &exec, &stats,
                                        total_tasks / submitter_count, work_us);
            }
            for (auto& t : submitters) t.join();

            // Wait for all tasks to complete
            while (stats.tasks_completed.load() < total_tasks) {
                std::this_thread::sleep_for(milliseconds(1));
            }

            auto t1 = high_resolution_clock::now();
            stats.running.store(false);

            double wall_sec = duration_cast<microseconds>(t1 - t0).count() / 1e6;
            double iops = stats.tasks_completed.load() / wall_sec / 1000.0;
            uint64_t avg_lat = stats.total_latency_ns.load() / stats.tasks_completed.load() / 1000;

            // Steal stats
            uint64_t total_steals = 0;
            uint64_t total_task_count = 0;
            for (size_t i = 0; i < nw; ++i) {
                total_steals += exec.worker(i).steals_success.load();
                total_task_count += exec.worker(i).tasks_executed.load();
            }
            double steal_pct = total_task_count > 0 ? 100.0 * total_steals / total_task_count : 0;

            printf("  %-7zu | %-10d | %-7zu | %-7.2f | %-8.1f | %-10lu | %-5.1f\n",
                   nw, submitter_count, total_tasks, wall_sec, iops, avg_lat, steal_pct);
            fflush(stdout);

            exec.stop();
            exec.join();
        }
    }
}

TEST(WorkStealingBench, StealEfficiency) {
    printf("\n=== Steal Efficiency ===\n");
    fflush(stdout);

    WorkStealingExecutor::Config cfg;
    cfg.num_workers = 4;
    cfg.pin_cpus = false;
    cfg.steal_attempts = 4;

    WorkStealingExecutor exec(cfg);

    // NOTE: local deque (RingWorkQueue) capacity is 4096 (hardcoded in
    // WorkStealingExecutor ctor). We stay ≤ 4000 to avoid silent drops.
    const size_t total_tasks = 4000;
    std::atomic<uint64_t> completed{0};
    g_steal_counter = &completed;

    auto t0 = high_resolution_clock::now();

    // Submit ALL tasks to worker 0 only — other workers must steal.
    // Push tasks BEFORE start() so they're visible to all workers as soon
    // as they enter their loops (avoids the race where workers park before
    // we can notify them).
    for (size_t i = 0; i < total_tasks; ++i) {
        exec.worker(0).local_deque->enqueue(
            WorkItem::make_func(steal_bench_task));
    }
    // Now start workers — each thread begins worker_loop and will either
    // consume from its own deque, the global entry, or steal from peers.
    // Worker 0's deque has all the tasks so the other 3 workers must steal.
    // (start() spawns threads but does NOT guarantee they've entered the
    //  loop body by the time it returns; we notify all workers to be safe.)
    exec.start();
    for (size_t i = 0; i < exec.num_workers(); ++i) {
        exec.worker(i).park.notify();
    }

    // Wait for completion
    while (completed.load() < total_tasks) {
        std::this_thread::sleep_for(milliseconds(1));
    }

    auto t1 = high_resolution_clock::now();
    double wall_sec = duration_cast<microseconds>(t1 - t0).count() / 1e6;

    printf("  All %zu tasks pushed to worker-0 only\n", total_tasks);
    printf("  Time: %.2fs, IOPS: %.1fK\n",
           wall_sec, total_tasks / wall_sec / 1000.0);

    for (size_t i = 0; i < 4; ++i) {
        auto& ws = exec.worker(i);
        printf("  Worker-%zu: tasks=%lu steals=%lu park=%lu\n",
               i, ws.tasks_executed.load(), ws.steals_success.load(),
               ws.parks.load());
    }
    fflush(stdout);

    exec.stop();
    exec.join();
}

TEST(WorkStealingBench, UnevenLoadStealing) {
    printf("\n=== Uneven Load Stealing ===\n");
    fflush(stdout);

    WorkStealingExecutor::Config cfg;
    cfg.num_workers = 4;
    cfg.pin_cpus = false;
    cfg.steal_attempts = 4;

    WorkStealingExecutor exec(cfg);

    const size_t total_tasks = 1000;
    const int work_us = 200;  // significant work per task
    std::atomic<size_t> completed{0};
    g_ue_completed = &completed;
    g_ue_work_us = work_us;

    // Push ALL tasks to worker-0 only — other workers must steal.
    // Follow the established pattern: enqueue BEFORE start() so all tasks
    // are visible as soon as workers enter their loops.
    for (size_t i = 0; i < total_tasks; ++i) {
        exec.worker(0).local_deque->enqueue(
            WorkItem::make_func(uneven_load_task));
    }

    auto t0 = high_resolution_clock::now();

    exec.start();
    for (size_t i = 0; i < exec.num_workers(); ++i) {
        exec.worker(i).park.notify();
    }

    while (completed.load() < total_tasks)
        std::this_thread::sleep_for(milliseconds(1));

    auto t1 = high_resolution_clock::now();
    double wall_sec = duration_cast<microseconds>(t1 - t0).count() / 1e6;

    printf("  All %zu tasks (each %dus) pushed to worker-0\n", total_tasks, work_us);
    fflush(stdout);
    printf("  Time: %.2fs, IOPS: %.1fK\n",
           wall_sec, total_tasks / wall_sec / 1000.0);
    fflush(stdout);
    printf("  Worker | Tasks  | Steals | Parks\n");
    for (size_t i = 0; i < 4; ++i) {
        auto& ws = exec.worker(i);
        printf("  %-6zu | %-6lu | %-6lu | %-5lu\n",
               i, ws.tasks_executed.load(), ws.steals_success.load(), ws.parks.load());
    }
    fflush(stdout);
    fflush(stdout);

    // With heavy work, all workers should participate via stealing
    size_t participating = 0;
    for (size_t i = 0; i < 4; ++i)
        if (exec.worker(i).tasks_executed.load() > 0) ++participating;

    EXPECT_GE(participating, 2u) << "Expected at least 2 workers to participate via stealing";
    EXPECT_EQ(completed.load(), total_tasks);

    exec.stop();
    exec.join();
}
