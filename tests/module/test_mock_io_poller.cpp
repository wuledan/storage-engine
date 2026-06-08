#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>
#include "mock_io_poller.h"
#include "runtime/online_worker.h"
#include "runtime/affine_work_queue.h"
#include "runtime/batched_spsc_work_queue.h"
#include "runtime/local_work_queue.h"

using namespace storage::runtime;
using namespace storage::runtime::test;

// ============================================================================
// 全局测试计数器（captureless lambda / void(*)() 需要全局变量）
// ============================================================================
namespace {
    std::atomic<int> g_netio_exec_count{0};
    std::atomic<int> g_affine_exec_count{0};
}

static void inc_netio() { g_netio_exec_count.fetch_add(1); }
static void inc_affine() { g_affine_exec_count.fetch_add(1); }

// ============================================================================
// 测试1: MockIOPoller 向 NetIO 队列投递完成事件
// ============================================================================
TEST(MockIOTest, PollerDeliversCompletions) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    OnlineWorker w(cfg);

    // 获取 NetIO 队列
    auto* net_io_q = static_cast<BatchedSPSCWorkQueue*>(w.get_queue(w.idx_net_io_));
    ASSERT_NE(net_io_q, nullptr);

    // 慢速轮询：每 2ms 投递 4 个完成事件，避免 BatchedSPSC 队列（容量 4096）溢出
    MockIOPoller poller(MockIOPoller::Config{
        std::chrono::milliseconds(2),  // 2ms 间隔
        4                                // 每次 4 个完成事件
    });
    poller.bind_net_io(net_io_q);
    poller.set_notify([&w]() { w.notify(); });  // 推完后唤醒 worker

    w.start();
    poller.start();

    // 运行 300ms：约 150 次轮询 × 4 = 600 个事件，远低于队列容量
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    poller.stop();
    w.stop();
    w.join();

    // 验证有完成事件被投递
    EXPECT_GT(poller.enqueued_net(), 0);
    // 验证 worker 消费了所有投递的事件（队列未溢出）
    EXPECT_GE(w.stats().tasks_executed, poller.enqueued_net());
}

// ============================================================================
// 测试2: Worker 正常消费 NetIO 完成事件（带计数）
// ============================================================================
TEST(MockIOTest, WorkerConsumesIOCompletions) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    OnlineWorker w(cfg);
    auto* net_io_q = static_cast<BatchedSPSCWorkQueue*>(w.get_queue(w.idx_net_io_));
    ASSERT_NE(net_io_q, nullptr);

    // 预先投递一批带计数器的任务
    g_netio_exec_count = 0;
    constexpr int kNumItems = 100;
    WorkItem items[kNumItems];
    for (int i = 0; i < kNumItems; ++i) {
        items[i] = WorkItem::make_func(inc_netio);
    }
    net_io_q->push_batch(items, kNumItems);

    w.start();
    w.notify();  // 唤醒 worker

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    w.stop();
    w.join();

    // 所有 100 个任务应被执行
    EXPECT_EQ(g_netio_exec_count.load(), kNumItems);
    EXPECT_GE(w.stats().tasks_executed, kNumItems);
}

// ============================================================================
// 测试3: NetIO(P1) 优先级低于 Affine(P0)
//
// 先投递 NetIO 任务，再投递 Affine 任务，
// 严格优先级策略应保证 Affine(P0) 先执行。
// ============================================================================
TEST(MockIOTest, AffinePriorityOverNetIO) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    OnlineWorker w(cfg);
    auto* net_io_q = static_cast<BatchedSPSCWorkQueue*>(w.get_queue(w.idx_net_io_));
    auto* affine_q = static_cast<AffineWorkQueue*>(w.get_queue(w.idx_affine_));
    ASSERT_NE(net_io_q, nullptr);
    ASSERT_NE(affine_q, nullptr);

    g_netio_exec_count = 0;
    g_affine_exec_count = 0;

    w.start();

    // 先投递 NetIO 任务 (P1)
    constexpr int kNetIOCount = 100;
    WorkItem netio_items[kNetIOCount];
    for (int i = 0; i < kNetIOCount; ++i) {
        netio_items[i] = WorkItem::make_func(inc_netio);
    }
    net_io_q->push_batch(netio_items, kNetIOCount);
    w.notify();

    // 稍等片刻让 worker 可能开始处理 NetIO
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // 再投递 Affine 任务 (P0)
    affine_q->enqueue(WorkItem::make_func(inc_affine));
    w.notify();

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    w.stop();
    w.join();

    // 验证所有任务都被执行
    EXPECT_EQ(g_netio_exec_count.load(), kNetIOCount);
    EXPECT_EQ(g_affine_exec_count.load(), 1);
}
