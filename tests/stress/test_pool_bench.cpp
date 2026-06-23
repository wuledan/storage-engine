// test_pool_bench.cpp — MemoryPool throughput benchmarks
//
// Measures pure allocation, round-trip, random-size mix, multi-thread
// contention, and steady-state throughput of the lock-free memory pool.
//
// Build: see tests/stress/CMakeLists.txt
// Run:   ./test_pool_bench

#include <gtest/gtest.h>
#include "runtime/memory_pool.h"

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <random>
#include <thread>
#include <vector>

using namespace storage::runtime::adapt;

// =============================================================================
// Helpers
// =============================================================================

static uint64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

// =============================================================================
// Scenario 1: Single-thread allocate-only throughput
// =============================================================================

TEST(PoolBench, SingleThreadAllocOnly) {
    MemoryPool pool;
    pool.warmup(4 * 1024 * 1024);

    constexpr size_t N = 1'000'000;
    std::vector<void*> ptrs(N);

    auto t0 = now_ns();
    for (size_t i = 0; i < N; i++) {
        ptrs[i] = pool.allocate(64);
    }
    auto t1 = now_ns();

    double ns_per = (t1 - t0) / (double)N;
    printf("  [AllocOnly 64B] %zu ops in %.1fms \u2192 %.1f ns/op (%.0f Mops/s)\n",
           N, (t1 - t0) / 1e6, ns_per, 1000.0 / ns_per);

    // Compare with ::operator new
    t0 = now_ns();
    for (size_t i = 0; i < N; i++) {
        ptrs[i] = ::operator new(64);
    }
    t1 = now_ns();
    double baseline_ns = (t1 - t0) / (double)N;
    printf("  [Baseline ::new 64B] %.1f ns/op (%.0f Mops/s)\n",
           baseline_ns, 1000.0 / baseline_ns);
    printf("  Speedup: %.1fx\n", baseline_ns / ns_per);

    // Free
    for (auto p : ptrs) ::operator delete(p);
}

// =============================================================================
// Scenario 2: Single-thread round-trip (allocate + deallocate alternating)
// =============================================================================

TEST(PoolBench, SingleThreadRoundTrip) {
    MemoryPool pool;
    pool.warmup(4 * 1024 * 1024);

    constexpr size_t N = 1'000'000;

    auto t0 = now_ns();
    for (size_t i = 0; i < N; i++) {
        void* p = pool.allocate(32);
        pool.deallocate(p, 32);
    }
    auto t1 = now_ns();

    double ns_per = (t1 - t0) / (double)N;
    printf("  [RoundTrip 32B] %zu ops in %.1fms \u2192 %.1f ns/roundtrip (%.0f Mops/s)\n",
           N, (t1 - t0) / 1e6, ns_per, 1000.0 / ns_per);

    // Baseline
    t0 = now_ns();
    for (size_t i = 0; i < N; i++) {
        void* p = ::operator new(32);
        ::operator delete(p);
    }
    t1 = now_ns();
    double baseline_ns = (t1 - t0) / (double)N;
    printf("  [Baseline ::new/delete 32B] %.1f ns/roundtrip (%.0f Mops/s)\n",
           baseline_ns, 1000.0 / baseline_ns);
    printf("  Speedup: %.1fx\n", baseline_ns / ns_per);
}

// =============================================================================
// Scenario 3: Random-size allocation mix (simulates real-world load)
// =============================================================================

TEST(PoolBench, RandomSizeMix) {
    MemoryPool pool;
    pool.warmup(16 * 1024 * 1024);

    constexpr size_t N = 1'000'000;
    size_t sizes[] = {8, 16, 32, 64, 128, 256, 512, 1024};
    std::mt19937 rng(42);
    std::uniform_int_distribution<size_t> dist(0, 7);
    std::vector<std::pair<void*, size_t>> ptrs;
    ptrs.reserve(N);

    auto t0 = now_ns();
    for (size_t i = 0; i < N; i++) {
        size_t sz = sizes[dist(rng)];
        void* p = pool.allocate(sz);
        ptrs.emplace_back(p, sz);
    }
    auto t1 = now_ns();

    double ns_per = (t1 - t0) / (double)N;
    printf("  [RandomSize Alloc] %zu ops in %.1fms \u2192 %.1f ns/op (%.0f Mops/s)\n",
           N, (t1 - t0) / 1e6, ns_per, 1000.0 / ns_per);

    // Free all
    t0 = now_ns();
    for (auto& [p, sz] : ptrs) {
        pool.deallocate(p, sz);
    }
    t1 = now_ns();
    double free_ns = (t1 - t0) / (double)N;
    printf("  [RandomSize Free] %.1f ns/op (%.0f Mops/s)\n",
           free_ns, 1000.0 / free_ns);
}

// =============================================================================
// Scenario 4: Multi-thread contention (simulates per-NUMA shared pool)
// =============================================================================

TEST(PoolBench, MultiThreadContention) {
    MemoryPool pool;
    pool.warmup(64 * 1024 * 1024);

    constexpr size_t N = 200'000;
    constexpr int kThreads = 4;

    for (int t = 1; t <= kThreads; t++) {
        auto run = [&](int /*tid*/) {
            size_t per_thread = N / t;
            for (size_t i = 0; i < per_thread; i++) {
                void* p = pool.allocate(64);
                pool.deallocate(p, 64);
            }
        };

        auto t0 = now_ns();
        std::vector<std::thread> threads;
        for (int j = 0; j < t; j++)
            threads.emplace_back(run, j);
        for (auto& th : threads) th.join();
        auto t1 = now_ns();

        double ns_per = (t1 - t0) / (double)N;
        printf("  [%d threads] %zu ops in %.1fms \u2192 %.1f ns/roundtrip (%.0f Mops/s total, %.0f Mops/s/thread)\n",
               t, N, (t1 - t0) / 1e6, ns_per,
               1000.0 / ns_per, 1000.0 / ns_per * t);
    }
}

// =============================================================================
// Scenario 5: Steady-state throughput (simulates scheduler hot path)
// =============================================================================

TEST(PoolBench, SteadyStateThroughput) {
    // Simulate real-world scenario: 128 active items cycling in the pool.
    // Continuously alloc-free-alloc-free, measuring steady-state throughput.
    MemoryPool pool;
    pool.warmup(4 * 1024 * 1024);

    constexpr size_t kPoolSize = 128;    // active objects in flight
    constexpr size_t kIterations = 500'000;
    void* ptrs[kPoolSize];

    // Pre-warm: allocate the active set
    for (size_t i = 0; i < kPoolSize; i++) {
        ptrs[i] = pool.allocate(64);
    }

    auto t0 = now_ns();
    for (size_t iter = 0; iter < kIterations; iter++) {
        size_t idx = iter % kPoolSize;
        pool.deallocate(ptrs[idx], 64);
        ptrs[idx] = pool.allocate(64);
    }
    auto t1 = now_ns();

    double ns_per = (t1 - t0) / (double)kIterations;
    printf("  [SteadyState 64B, pool=%zu] %zu iters in %.1fms \u2192 %.1f ns/op (%.0f Mops/s)\n",
           kPoolSize, kIterations, (t1 - t0) / 1e6, ns_per, 1000.0 / ns_per);
}
