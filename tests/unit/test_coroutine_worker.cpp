#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>
#include "runtime/coro_task.h"
#include "runtime/online_worker.h"
#include "runtime/affinity_baton.h"

using namespace storage::runtime;

// ============================================================================
// 测试1: Worker start/stop 生命周期（协程环境）
// ============================================================================
TEST(CoroutineWorkerTest, StartStopWithCoroutine) {
    Worker::Config cfg;
    cfg.cpu_id = 0; // 不绑定 CPU
    OnlineWorker w(cfg);
    w.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    w.stop();
    w.join();
    SUCCEED();
}

// ============================================================================
// 测试2: submit_engine 任务在协程环境中执行
// ============================================================================
namespace {
    std::atomic<int> g_worker_count{0};
}

static void inc_worker_count() {
    g_worker_count.fetch_add(1);
}

TEST(CoroutineWorkerTest, SubmitEngineExecutes) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    OnlineWorker w(cfg);
    g_worker_count = 0;
    w.start();
    for (int i = 0; i < 100; ++i) {
        w.submit_engine(WorkItem::make_func(inc_worker_count));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    w.stop();
    w.join();
    EXPECT_GE(g_worker_count.load(), 100);
}

// ============================================================================
// 测试3: enqueue_affine 可被外部线程调用
// ============================================================================
TEST(CoroutineWorkerTest, EnqueueAffineFromExternalThread) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    OnlineWorker w(cfg);
    w.start();
    // 投递一个 noop coroutine 验证不 crash
    w.enqueue_affine(std::noop_coroutine());
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    w.stop();
    w.join();
    SUCCEED();
}

// ============================================================================
// 测试4: get_current_worker_id 在 worker 线程上返回正确值
// ============================================================================
namespace {
    std::atomic<size_t> g_captured_worker_id{SIZE_MAX};
}

static void capture_worker_id() {
    g_captured_worker_id.store(
        adapt::detail::get_current_worker_id(),
        std::memory_order_relaxed);
}

TEST(CoroutineWorkerTest, CurrentWorkerIdSet) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    OnlineWorker w(cfg);
    g_captured_worker_id = SIZE_MAX;

    w.start();
    w.submit_engine(WorkItem::make_func(capture_worker_id));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    w.stop();
    w.join();
    EXPECT_NE(g_captured_worker_id.load(), SIZE_MAX);
}
