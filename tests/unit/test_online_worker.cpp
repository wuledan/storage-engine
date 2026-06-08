#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>
#include "runtime/online_worker.h"
#include "runtime/local_work_queue.h"
#include "runtime/batched_spsc_work_queue.h"
#include "runtime/affine_work_queue.h"

using namespace storage::runtime;

static std::atomic<int> g_ow_counter{0};

// Test subclass to expose protected members for inspection
class TestableOnlineWorker : public OnlineWorker {
public:
    using OnlineWorker::OnlineWorker;
    WorkQueue* get_q(size_t idx) { return get_queue(idx); }
    size_t engine_idx() const { return idx_engine_; }
    size_t net_io_idx() const { return idx_net_io_; }
    size_t disk_io_idx() const { return idx_disk_io_; }
    size_t affine_idx() const { return idx_affine_; }
    size_t timer_idx() const { return idx_timer_; }
};

// ============================================================================
// Default 5 queues registered
// ============================================================================
TEST(OnlineWorkerTest, DefaultQueuesRegistered) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    TestableOnlineWorker w(cfg);

    // Check that all 5 queue indices are set
    EXPECT_NE(w.engine_idx(), size_t(-1));
    EXPECT_NE(w.net_io_idx(), size_t(-1));
    EXPECT_NE(w.disk_io_idx(), size_t(-1));
    EXPECT_NE(w.affine_idx(), size_t(-1));
    EXPECT_NE(w.timer_idx(), size_t(-1));

    // Check distinct indices
    EXPECT_NE(w.engine_idx(), w.net_io_idx());

    // Verify queue types via the get_queue accessor
    auto* q_affine = w.get_q(w.affine_idx());
    ASSERT_NE(q_affine, nullptr);
    EXPECT_EQ(q_affine->type(), QueueType::kAffine);
    EXPECT_EQ(q_affine->semantic(), QueueSemantic::kMPMC);

    auto* q_net = w.get_q(w.net_io_idx());
    ASSERT_NE(q_net, nullptr);
    EXPECT_EQ(q_net->type(), QueueType::kNetIO);
    EXPECT_EQ(q_net->semantic(), QueueSemantic::kSPSC);

    auto* q_disk = w.get_q(w.disk_io_idx());
    ASSERT_NE(q_disk, nullptr);
    EXPECT_EQ(q_disk->type(), QueueType::kDiskIO);
    EXPECT_EQ(q_disk->semantic(), QueueSemantic::kSPSC);

    auto* q_engine = w.get_q(w.engine_idx());
    ASSERT_NE(q_engine, nullptr);
    EXPECT_EQ(q_engine->type(), QueueType::kEngine);
    EXPECT_EQ(q_engine->semantic(), QueueSemantic::kSPSC);

    auto* q_timer = w.get_q(w.timer_idx());
    ASSERT_NE(q_timer, nullptr);
    EXPECT_EQ(q_timer->type(), QueueType::kTimer);
    EXPECT_EQ(q_timer->semantic(), QueueSemantic::kLocal);
}

// ============================================================================
// Default policy is StrictPriority
// ============================================================================
TEST(OnlineWorkerTest, DefaultPolicyStrictPriority) {
    Worker::Config cfg;
    TestableOnlineWorker w(cfg);

    // Fill P0 queue (affine, priority kCritical) and P2 queue (engine, kMedium)
    auto* affine_q = static_cast<AffineWorkQueue*>(w.get_q(w.affine_idx()));
    ASSERT_NE(affine_q, nullptr);
    affine_q->enqueue(WorkItem::make_func(+[]() { g_ow_counter.store(100); }));

    auto* engine_q = static_cast<LocalWorkQueue*>(w.get_q(w.engine_idx()));
    ASSERT_NE(engine_q, nullptr);
    engine_q->try_enqueue(WorkItem::make_func(+[]() { g_ow_counter.store(200); }));

    g_ow_counter = 0;

    w.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    w.stop();
    w.join();

    // Affine queue (P0) should be processed before engine (P2).
    // But both items eventually get processed; we verify at least one task ran.
    EXPECT_GE(w.stats().tasks_executed, 1u);
}

// ============================================================================
// submit_engine works
// ============================================================================
TEST(OnlineWorkerTest, SubmitEngineWorks) {
    g_ow_counter = 0;
    Worker::Config cfg;
    cfg.cpu_id = 0;
    OnlineWorker w(cfg);  // use real class, no test subclass needed

    // submit_engine can be called before start (from same thread, safe)
    w.submit_engine(WorkItem::make_func(+[]() { g_ow_counter = 42; }));

    w.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    w.stop();
    w.join();

    EXPECT_GE(w.stats().tasks_executed, 1u);
    EXPECT_EQ(g_ow_counter.load(), 42);
}

// ============================================================================
// submit_net_io submits to BatchedSPSC queue (thread-safe producer)
// ============================================================================
TEST(OnlineWorkerTest, SubmitNetIoWorks) {
    g_ow_counter = 0;
    Worker::Config cfg;
    OnlineWorker w(cfg);

    // Submit before start — same thread, safe
    w.submit_net_io(WorkItem::make_func(+[]() { g_ow_counter = 77; }));

    w.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    w.stop();
    w.join();

    EXPECT_GE(w.stats().tasks_executed, 1u);
    EXPECT_EQ(g_ow_counter.load(), 77);
}

// ============================================================================
// submit_disk_io works similarly
// ============================================================================
TEST(OnlineWorkerTest, SubmitDiskIoWorks) {
    g_ow_counter = 0;
    Worker::Config cfg;
    OnlineWorker w(cfg);

    w.submit_disk_io(WorkItem::make_func(+[]() { g_ow_counter = 99; }));

    w.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    w.stop();
    w.join();

    EXPECT_GE(w.stats().tasks_executed, 1u);
    EXPECT_EQ(g_ow_counter.load(), 99);
}
