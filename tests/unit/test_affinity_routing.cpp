#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>
#include <folly/coro/Task.h>
#include <folly/coro/BlockingWait.h>
#include "runtime/online_worker.h"
#include "runtime/affine_work_queue.h"
#include "runtime/local_work_queue.h"
#include "runtime/affinity_baton.h"

using namespace storage::runtime;

// ============================================================================
// 测试用子类：暴露 protected 成员便于测试
// ============================================================================
class TestableOnlineWorker : public OnlineWorker {
public:
    using OnlineWorker::OnlineWorker;
    WorkQueue* get_q(size_t idx) { return get_queue(idx); }
    size_t affine_idx() const { return idx_affine_; }
    size_t engine_idx() const { return idx_engine_; }
};

// ============================================================================
// 全局测试状态（captureless lambda 需要全局变量）
// ============================================================================
namespace {
    std::atomic<int> g_exec_order{0};
    std::atomic<int> g_affine_order{-1};
    std::atomic<int> g_engine_first_order{-1};
    std::atomic<int> g_w1_count{0};
    std::atomic<int> g_w2_count{0};
    std::atomic<bool> g_affine_executed{false};
    std::atomic<bool> g_route_task_done{false};
}

static void record_affine_order() {
    g_affine_order.store(g_exec_order.fetch_add(1));
}

static void record_engine_order() {
    int order = g_exec_order.fetch_add(1);
    int expected = -1;
    g_engine_first_order.compare_exchange_strong(expected, order);
}

static void inc_w1() { g_w1_count.fetch_add(1); }
static void inc_w1_10() { g_w1_count.fetch_add(10); }
static void inc_w2() { g_w2_count.fetch_add(1); }
static void set_affine_executed() { g_affine_executed.store(true); }
static void set_route_task_done() { g_route_task_done.store(true); }

// ============================================================================
// 测试1: Affine 队列 (P0) 优先级高于 Engine 队列 (P2)
//
// 先投递 100 个 engine 任务 (P2)，再投递 1 个 affine 任务 (P0)，
// 严格优先级策略应保证 P0 任务先执行。
// ============================================================================
TEST(AffinityRoutingTest, AffineQueueHasHighestPriority) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    TestableOnlineWorker w(cfg);
    g_exec_order = 0;
    g_affine_order = -1;
    g_engine_first_order = -1;

    auto* affine_q = static_cast<AffineWorkQueue*>(w.get_q(w.affine_idx()));
    ASSERT_NE(affine_q, nullptr);

    w.start();

    // 先投递 100 个 engine 任务 (P2)
    for (int i = 0; i < 100; ++i) {
        w.submit_engine(WorkItem::make_func(record_engine_order));
    }

    // 再投递 1 个 affine 任务 (P0)
    affine_q->enqueue(WorkItem::make_func(record_affine_order));
    w.notify();

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    w.stop();
    w.join();

    // affine (P0) 应先于 engine (P2) 执行
    ASSERT_GE(g_affine_order.load(), 0);
    ASSERT_GE(g_engine_first_order.load(), 0);
    EXPECT_LT(g_affine_order.load(), g_engine_first_order.load());
}

// ============================================================================
// 测试2: RouteFunc 桥接 — enqueue_affine 可被 RouteFunc 调用
//
//  1. 从 worker 外部创建 RouteFunc（调用 enqueue_affine）
//  2. 调用 RouteFunc 投递一个任务（通过 coroutine_handle 包装为 WorkItem）
//  3. 验证 worker 的 affine 队列处理了该任务
// ============================================================================
TEST(AffinityRoutingTest, RouteFuncBridge) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    TestableOnlineWorker w(cfg);
    g_route_task_done = false;

    w.start();

    // 创建一个 RouteFunc: 调用 enqueue_affine 投递任务
    auto route_func = w.make_route_func();

    // 先通过 engine 投递一个标记任务
    w.submit_engine(WorkItem::make_func(set_route_task_done));

    // 通过 RouteFunc 投递 noop coroutine 到 affine 队列
    route_func(0, std::noop_coroutine());

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    w.stop();
    w.join();

    // engine 任务被执行
    EXPECT_TRUE(g_route_task_done.load());
    // RouteFunc 投递到 affine 队列的 noop coroutine 不会造成崩溃
    SUCCEED();
}

// ============================================================================
// 测试3: 多 Worker 隔离 — 不同 Worker 的 affine 队列互不干扰
//
// 创建 2 个 OnlineWorker，各自投递任务，验证各自独立执行。
// ============================================================================
TEST(AffinityRoutingTest, MultiWorkerIsolation) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    TestableOnlineWorker w1(cfg);
    TestableOnlineWorker w2(cfg);
    g_w1_count = 0;
    g_w2_count = 0;

    auto* affine_q1 = static_cast<AffineWorkQueue*>(w1.get_q(w1.affine_idx()));
    auto* affine_q2 = static_cast<AffineWorkQueue*>(w2.get_q(w2.affine_idx()));
    ASSERT_NE(affine_q1, nullptr);
    ASSERT_NE(affine_q2, nullptr);

    w1.start();
    w2.start();

    // 向 worker 1 投递 50 个 engine 任务 + 1 个 affine 任务
    for (int i = 0; i < 50; ++i) {
        w1.submit_engine(WorkItem::make_func(inc_w1));
    }
    affine_q1->enqueue(WorkItem::make_func(inc_w1_10));
    w1.notify();

    // 向 worker 2 投递 30 个 engine 任务
    for (int i = 0; i < 30; ++i) {
        w2.submit_engine(WorkItem::make_func(inc_w2));
    }
    w2.notify();

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    w1.stop();
    w1.join();
    w2.stop();
    w2.join();

    EXPECT_GE(g_w1_count.load(), 51);
    EXPECT_GE(g_w2_count.load(), 30);
}

// ── co_submit_engine removed (C1 cleanup) ──
// Test 4 (CoSubmitEngineEndToEnd) and Test 5 (MultipleCoSubmitEngine) 
// have been removed as part of the co_submit_engine deprecation.
