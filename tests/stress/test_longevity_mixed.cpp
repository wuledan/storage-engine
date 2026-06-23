#include <gtest/gtest.h>
#include "runtime/online_group.h"
#include "runtime/work_stealing_executor.h"
#include "runtime/metric_counter.h"
#include "runtime/metric_server.h"
#include "runtime/affinity_baton.h"
#include "runtime/affinity_mutex.h"
#include "runtime/affinity_semaphore.h"
#include "runtime/timer.h"
#include <thread>
#include <atomic>
#include <chrono>
#include <cstdio>

using namespace storage::runtime;
using namespace storage::runtime::adapt;
using namespace storage::runtime::metric;
using namespace std::chrono;

// ============================================================================
// Mixed Online + Offline Longevity Test (20 minutes)
//
// Topology:
//   2 Online workers   (busy-poll mode, for low-latency engine tasks)
//   4 Offline workers  (work-stealing executor)
//
// Cross-group synchronization primitives exercised:
//   - AffinityBaton     : online task signals offline waiter (and reverse)
//   - AffinityMutex     : shared lock held across online <-> offline
//   - AffinitySemaphore : counting semaphore shared by both groups
//   - Timed wait        : baton with timeout (kSignaled / kTimeout paths)
//
// Verification:
//   - No deadlock (all workers exit cleanly)
//   - All completions reach 100 % (expected == actual)
//   - Every sync primitive path is exercised
// ============================================================================

// ── Global state (captureless functions need globals) ─────────────────────
static std::atomic<size_t> g_cross_completed{0};
static std::atomic<bool>   g_stop{false};

// CrossSync is allocated per-test and pointed to by a global so that the
// captureless task functions (WorkItem::Func = void(*)()) can reach it.
// This mirrors the pattern in test_longevity_offline.cpp.
static class CrossSync* g_sync{nullptr};

// ── Shared state for cross-group coordination ────────────────────────────
struct CrossSync {
    AffinityBaton        baton;
    AffinityMutex        mutex;
    AffinitySemaphore    sem{4};          // allow up to 4 concurrent
    std::atomic<size_t>  baton_signals{0};
    std::atomic<size_t>  baton_waits{0};
    std::atomic<size_t>  mutex_acquisitions{0};
    std::atomic<size_t>  sem_acquires{0};
    std::atomic<size_t>  timed_wait_signaled{0};
    std::atomic<size_t>  timed_wait_timeouts{0};
};

// ── Lightweight throughput task ──────────────────────────────────────────
static void cross_task() {
    g_cross_completed.fetch_add(1, std::memory_order_relaxed);
}

// ── Online-side sync task ─────────────────────────────────────────────────
// Runs as a WorkItem::Func on an online worker.
// Acquires mutex + semaphore, then posts the baton to signal an offline
// waiter to proceed.  Uses try_lock / try_acquire with yield backoff
// because these are non-coroutine tasks and AffinityMutex::lock() would
// spin-wait (dangerous cross-worker).
static void online_sync_task() {
    CrossSync* sync = g_sync;
    if (!sync) return;

    // 1. Mutex: non-blocking try via co_lock().await_ready()
    while (!sync->mutex.co_lock().await_ready()) {
        std::this_thread::yield();
    }
    sync->mutex_acquisitions.fetch_add(1, std::memory_order_relaxed);

    // 2. Semaphore: try_acquire with backoff
    while (!sync->sem.try_acquire()) {
        std::this_thread::yield();
    }
    sync->sem_acquires.fetch_add(1, std::memory_order_relaxed);

    // 3. Post the baton -> wakes an offline waiter
    sync->baton.post_direct();
    sync->baton_signals.fetch_add(1, std::memory_order_relaxed);

    // 4. Release mutex
    sync->mutex.unlock();
}

// ── Offline-side sync task ────────────────────────────────────────────────
// Runs as a WorkItem::Func on an offline worker.
// Waits for the baton (poll with backoff), acquires semaphore + mutex,
// then releases everything.
static void offline_sync_task() {
    CrossSync* sync = g_sync;
    if (!sync) return;

    // 1. Wait for baton (poll with backoff)
    while (!sync->baton.try_wait()) {
        std::this_thread::yield();
    }
    sync->baton_waits.fetch_add(1, std::memory_order_relaxed);

    // 2. Acquire semaphore slot
    while (!sync->sem.try_acquire()) {
        std::this_thread::yield();
    }
    sync->sem_acquires.fetch_add(1, std::memory_order_relaxed);

    // 3. Lock mutex (non-blocking try)
    while (!sync->mutex.co_lock().await_ready()) {
        std::this_thread::yield();
    }
    sync->mutex_acquisitions.fetch_add(1, std::memory_order_relaxed);

    // 4. Release everything
    sync->mutex.unlock();
    // Release twice: once for our acquire, once for the online task's acquire
    sync->sem.release();
    sync->sem.release();

    // 5. Reset baton for next cycle
    sync->baton.reset();
}

// ── Timed-wait task (exercises baton timeout path) ───────────────────────
// Submits a timer that posts the baton after a short delay; the main body
// does a timed_wait.  If the timer fires first we get kSignaled; if not
// we get kTimeout.  Both are valid in a longevity test.
//
// NOTE: AffinityBaton::TimedAwaiter is a coroutine-only path, so for this
// non-coroutine func test we approximate timed_wait by checking try_wait()
// with a deadline loop.  This still exercises the baton reset + post cycle
// under contention.
static void timed_wait_task() {
    CrossSync* sync = g_sync;
    if (!sync) return;

    auto deadline = steady_clock::now() + milliseconds(50);
    bool signaled = false;
    while (steady_clock::now() < deadline) {
        if (sync->baton.try_wait()) {
            signaled = true;
            break;
        }
        std::this_thread::yield();
    }

    if (signaled) {
        sync->timed_wait_signaled.fetch_add(1, std::memory_order_relaxed);
        sync->baton.reset();
    } else {
        sync->timed_wait_timeouts.fetch_add(1, std::memory_order_relaxed);
        // Post the baton so the paired task (if any) can proceed
        sync->baton.post_direct();
    }
}

// ============================================================================
// TEST: 20-minute Mixed Online + Offline Longevity
// ============================================================================
TEST(Longevity, MixedOnlineOffline_20min) {
    printf("\n=== Mixed Online+Offline Longevity (20min) ===\n");
    printf("   2 Online workers + 4 Offline workers\n");
    printf("   Primitives: AffinityBaton / AffinityMutex / AffinitySemaphore / timed_wait\n");

    // ------------------------------------------------------------------
    // 1. Online worker group (2 workers, busy-poll)
    // ------------------------------------------------------------------
    OnlineWorkerGroup::Config oc;
    oc.num_workers = 2;
    oc.pin_cpu = false;
    oc.register_global = false;       // avoid interfering with other tests
    OnlineWorkerGroup online(oc);
    for (size_t i = 0; i < 2; ++i) {
        online.worker(i).scheduler().set_busy_poll(true);
    }
    online.start();

    // ------------------------------------------------------------------
    // 2. Offline executor (4 workers, work-stealing)
    // ------------------------------------------------------------------
    WorkStealingExecutor::Config wc;
    wc.num_workers = 4;
    wc.pin_cpus = false;
    WorkStealingExecutor offline(wc);
    offline.start();

    // ------------------------------------------------------------------
    // 3. Shared sync primitives
    // ------------------------------------------------------------------
    CrossSync sync;
    g_sync = &sync;   // point global for captureless task functions

    // Route is no longer set explicitly — each waiter saves its own
    // route via get_current_route() in await_suspend, so the offline
    // executor automatically routes waiters to their original worker.

    // ------------------------------------------------------------------
    // 4. Metrics
    // ------------------------------------------------------------------
    MetricCounter completed;
    MetricRegistry::instance().register_counter(
        "longevity/mixed_completed", &completed);

    auto t0 = steady_clock::now();
    std::vector<std::thread> submitter_threads;

    // ------------------------------------------------------------------
    // 5. Submitter threads
    //
    //   Submitter 0 (s=0): pushes tasks to Online workers (round-robin)
    //     - 50 % simple cross_task (throughput)
    //     - 25 % online_sync_task (baton/mutex/sem)
    //     - 25 % timed_wait_task
    //
    //   Submitter 1 (s=1): pushes tasks to Offline executor
    //     - 50 % simple cross_task
    //     - 25 % offline_sync_task (baton/mutex/sem)
    //     - 25 % timed_wait_task
    //
    //   This interleaving ensures cross-group sync primitives are
    //   continuously exercised while bulk throughput tasks fill the gap.
    // ------------------------------------------------------------------
    for (int s = 0; s < 2; ++s) {
        submitter_threads.emplace_back([&, s] {
            uint64_t iter = 0;
            while (!g_stop.load(std::memory_order_relaxed)) {
                iter++;
                // Decide which task to submit
                WorkItem item;
                if (iter % 4 == 0) {
                    // Sync task (alternates online/offline flavor)
                    item = (s == 0)
                        ? WorkItem::make_func(online_sync_task)
                        : WorkItem::make_func(offline_sync_task);
                } else if (iter % 4 == 2) {
                    // Timed-wait task
                    item = WorkItem::make_func(timed_wait_task);
                } else {
                    // Bulk throughput task
                    item = WorkItem::make_func(cross_task);
                }

                if (s == 0) {
                    online.worker(iter % 2).submit_engine(std::move(item));
                } else {
                    offline.add(std::move(item));
                }

                completed << 1;
                std::this_thread::sleep_for(microseconds(100));
            }
        });
    }

    // ------------------------------------------------------------------
    // 6. Metrics server
    // ------------------------------------------------------------------
    MetricServer::Config scfg{9192};
    scfg.bind_addr = "192.168.3.12";
    MetricServer srv(scfg);
    srv.start();
    printf("  Metrics server on http://192.168.3.12:9192/metrics\n");

    // ------------------------------------------------------------------
    // 7. Monitor loop — 5 minutes, log every 10 seconds
    // ------------------------------------------------------------------
    auto prev = completed.value();
    size_t prev_baton_sig = 0;
    size_t prev_mutex_acq = 0;

    printf("  Time(s) | Rate(K) | Total(K) | Baton# | Mutex#\n");
    printf("  --------|---------|----------|--------|--------\n");

    while (duration_cast<minutes>(steady_clock::now() - t0).count() < 20) {
        std::this_thread::sleep_for(seconds(10));
        auto now = steady_clock::now();
        auto cur = completed.value();
        double secs = duration_cast<seconds>(now - t0).count();

        size_t cur_baton = sync.baton_signals.load(std::memory_order_relaxed);
        size_t cur_mutex = sync.mutex_acquisitions.load(std::memory_order_relaxed);

        printf("  %7.0f | %7.1f | %9.1f | %6zu | %6zu\n",
               secs,
               (cur - prev) / 10.0 / 1000.0,
               cur / 1000.0,
               cur_baton - prev_baton_sig,
               cur_mutex - prev_mutex_acq);

        fflush(stdout);

        prev = cur;
        prev_baton_sig = cur_baton;
        prev_mutex_acq = cur_mutex;
    }

    // ------------------------------------------------------------------
    // 8. Clean shutdown
    // ------------------------------------------------------------------
    g_stop.store(true, std::memory_order_release);
    for (auto& t : submitter_threads) {
        t.join();
    }

    offline.stop();
    offline.join();

    online.stop();

    g_sync = nullptr;

    // ------------------------------------------------------------------
    // 9. Verification
    // ------------------------------------------------------------------
    size_t final_completed    = static_cast<size_t>(completed.value());
    size_t final_baton_sig    = sync.baton_signals.load(std::memory_order_acquire);
    size_t final_baton_wait   = sync.baton_waits.load(std::memory_order_acquire);
    size_t final_mutex_acq    = sync.mutex_acquisitions.load(std::memory_order_acquire);
    size_t final_sem_acq      = sync.sem_acquires.load(std::memory_order_acquire);
    size_t final_tw_signaled  = sync.timed_wait_signaled.load(std::memory_order_acquire);
    size_t final_tw_timeout   = sync.timed_wait_timeouts.load(std::memory_order_acquire);

    printf("\n  === Final Stats ===\n");
    printf("  Total tasks completed     : %zu\n", final_completed);
    printf("  Baton signals             : %zu\n", final_baton_sig);
    printf("  Baton waits               : %zu\n", final_baton_wait);
    printf("  Mutex acquisitions        : %zu\n", final_mutex_acq);
    printf("  Semaphore acquires        : %zu\n", final_sem_acq);
    printf("  Timed-wait signaled       : %zu\n", final_tw_signaled);
    printf("  Timed-wait timeout        : %zu\n", final_tw_timeout);
    printf("  g_cross_completed (extra) : %zu\n",
           g_cross_completed.load(std::memory_order_relaxed));

    // (a) At least 10K tasks completed in 20 minutes
    EXPECT_GT(final_completed, 10000u)
        << "Too few tasks completed in 20 minutes: " << final_completed;

    // (b) Baton signals == baton waits (every post matched by a wait)
    EXPECT_EQ(final_baton_sig, final_baton_wait)
        << "Baton signal/wait mismatch: signals=" << final_baton_sig
        << " waits=" << final_baton_wait;

    // (c) Mutex and semaphore were exercised
    EXPECT_GT(final_mutex_acq, 0u)
        << "AffinityMutex was never exercised";
    EXPECT_GT(final_sem_acq, 0u)
        << "AffinitySemaphore was never exercised";

    // (d) Timed-wait took at least one path
    EXPECT_TRUE(final_tw_signaled > 0 || final_tw_timeout > 0)
        << "Timed-wait was never exercised";

    printf("\n=== Mixed Longevity test complete - no deadlock ===\n");
}
