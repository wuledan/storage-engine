// test_pool_numa.cpp — Per-NUMA shared pool multi-thread correctness & scaling
//
// Validates that QuantMemoryResource is safe under concurrent multi-threaded
// access patterns that mimic a per-NUMA shared pool (one pool, many threads).
//
// Scenarios:
//   1. ConcurrentAllocFree  — 8 threads, random sizes, no double-free/leak
//   2. HoldAndRelease      — 4 threads, hold 1000 objects each, cycle 50×
//   3. ThroughputScaling   — 1–8 threads, mixed sizes, throughput measurement
//
// Build: see tests/stress/CMakeLists.txt (add_stress_test)
// Run:   ./test_pool_numa

#include <gtest/gtest.h>
#include "runtime/memory_pool.h"

#include <atomic>
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
// Test 1: Multi-thread concurrent allocate/deallocate (correctness)
// =============================================================================

TEST(PoolNUMA, ConcurrentAllocFree) {
    QuantMemoryResource pool;
    pool.warmup(64 * 1024 * 1024);

    constexpr int kThreads = 8;
    constexpr size_t kOpsPerThread = 100'000;
    std::atomic<size_t> errors{0};

    auto worker = [&](int tid) {
        std::mt19937 rng(tid * 12345 + 42);
        std::uniform_int_distribution<size_t> size_dist(0, 5);
        size_t sizes[] = {8, 16, 32, 64, 128, 256};

        for (size_t i = 0; i < kOpsPerThread; i++) {
            size_t sz = sizes[size_dist(rng)];
            void* p = pool.allocate(sz);
            if (!p) { errors++; continue; }
            pool.deallocate(p, sz);
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int i = 0; i < kThreads; i++)
        threads.emplace_back(worker, i);
    for (auto& t : threads) t.join();

    EXPECT_EQ(errors.load(), 0u);
}

// =============================================================================
// Test 2: Mixed hold-and-release pattern (simulates real workload)
// =============================================================================

TEST(PoolNUMA, HoldAndRelease) {
    QuantMemoryResource pool;
    pool.warmup(64 * 1024 * 1024);

    constexpr int kThreads = 4;
    constexpr size_t kHoldCount = 1000;   // objects held per thread
    constexpr size_t kCycles = 50;         // release-reallocate cycles

    auto worker = [&](int tid) {
        (void)tid;
        size_t sz = 64;
        std::vector<void*> held(kHoldCount);

        // Phase 1: allocate kHoldCount objects
        for (size_t i = 0; i < kHoldCount; i++) {
            held[i] = pool.allocate(sz);
            EXPECT_NE(held[i], nullptr);
        }

        // Phase 2: cycle — deallocate then reallocate all held objects
        for (size_t c = 0; c < kCycles; c++) {
            for (size_t i = 0; i < kHoldCount; i++) {
                pool.deallocate(held[i], sz);
                held[i] = pool.allocate(sz);
                EXPECT_NE(held[i], nullptr);
            }
        }

        // Phase 3: final deallocate
        for (void* p : held) {
            pool.deallocate(p, sz);
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int i = 0; i < kThreads; i++)
        threads.emplace_back(worker, i);
    for (auto& t : threads) t.join();
}

// =============================================================================
// Test 3: Per-NUMA throughput scaling (1–8 threads)
// =============================================================================

TEST(PoolNUMA, ThroughputScaling) {
    QuantMemoryResource pool;
    pool.warmup(64 * 1024 * 1024);

    constexpr size_t N = 500'000;
    size_t sizes[] = {32, 64, 64, 128, 64, 32, 256, 64};  // mixed workload

    for (int t = 1; t <= 8; t++) {
        auto worker = [&](int /*tid*/) {
            size_t per_thread = N / t;
            for (size_t i = 0; i < per_thread; i++) {
                size_t sz = sizes[i % 8];
                void* p = pool.allocate(sz);
                pool.deallocate(p, sz);
            }
        };

        auto t0 = now_ns();
        std::vector<std::thread> threads;
        threads.reserve(t);
        for (int i = 0; i < t; i++)
            threads.emplace_back(worker, i);
        for (auto& th : threads) th.join();
        auto t1 = now_ns();

        double ns_per = (t1 - t0) / (double)N;
        printf("  [%d threads] %.1f ns/op \u2192 %.0f Mops/s total, %.0f/thread\n",
               t, ns_per, 1000.0 / ns_per, 1000.0 / ns_per * t);
        fflush(stdout);
    }
}
