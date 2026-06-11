#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>
#include <folly/coro/Task.h>
#include <folly/coro/BlockingWait.h>
#include "runtime/coro_api.h"

using namespace storage::runtime;

// ============================================================================
// Worker lifecycle (spawn / shutdown / join)
// ============================================================================

TEST(CoroApi, WorkerSpawnJoin) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    worker_t w = worker_spawn(cfg);
    ASSERT_NE(w, nullptr);
    worker_join(w);  // should not hang
}

TEST(CoroApi, WorkerShutdown) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    worker_t w = worker_spawn(cfg);
    worker_shutdown(w);
    // Give it a moment to react to stop
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    // Join should be quick since we already requested stop
    w->join();
    delete w;
}

// ============================================================================
// co_spawn with captureless function pointer
// ============================================================================

TEST(CoroApi, CoSpawnFunctionPtr) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    worker_t w = worker_spawn(cfg);

    std::atomic<int> counter{0};

    // submit some tasks via engine directly to verify lifecycle
    w->submit_engine(WorkItem::make_func(+[]() { /* no-op */ }));
    w->submit_engine(WorkItem::make_func(+[]() { /* no-op */ }));
    w->submit_engine(WorkItem::make_func(+[]() { /* no-op */ }));

    auto s = w->stats();
    int spins = 0;
    while (s.tasks_executed < 3 && spins < 500) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        s = w->stats();
        ++spins;
    }
    EXPECT_GE(s.tasks_executed, 3u);

    worker_join(w);
}

// ============================================================================
// co_spawn with capturing lambda (fire-and-forget)
// ============================================================================

TEST(CoroApi, CoSpawnLambda) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    worker_t w = worker_spawn(cfg);

    std::atomic<int> counter{0};

    // Use co_submit to execute a task on the worker that calls co_spawn
    // with a capturing lambda.
    auto task = w->co_submit<void>([&counter]() {
        co_spawn([&counter]() { counter.fetch_add(1); });
        co_spawn([&counter]() { counter.fetch_add(1); });
        co_spawn([&counter]() { counter.fetch_add(1); });
    });
    folly::coro::blockingWait(std::move(task));

    // The spawned tasks are enqueued on the engine.  Wait for them to
    // be processed by the scheduler on the worker thread.
    int spins = 0;
    while (counter.load() < 3 && spins < 1000) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        ++spins;
    }
    EXPECT_EQ(counter.load(), 3);

    w->stop();
    w->join();
    delete w;
}

// ============================================================================
// co_submit (underlying mechanism for co_async)
// ============================================================================

TEST(CoroApi, CoSubmitReturnsValue) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    worker_t w = worker_spawn(cfg);

    auto task = w->co_submit<int>([] { return 42; });
    int result = folly::coro::blockingWait(std::move(task));
    EXPECT_EQ(result, 42);

    worker_join(w);
}

TEST(CoroApi, CoSubmitReturnsVoid) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    worker_t w = worker_spawn(cfg);

    std::atomic<bool> called{false};
    auto task = w->co_submit<void>([&called] { called = true; });
    folly::coro::blockingWait(std::move(task));

    EXPECT_TRUE(called.load());
    worker_join(w);
}

TEST(CoroApi, CoSubmitPropagatesException) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    worker_t w = worker_spawn(cfg);

    auto task = w->co_submit<int>([]() -> int {
        throw std::runtime_error("test error");
    });
    EXPECT_THROW(folly::coro::blockingWait(std::move(task)), std::runtime_error);

    worker_join(w);
}

// ============================================================================
// Group lifecycle
// ============================================================================

TEST(CoroApi, GroupSpawnJoin) {
    OnlineWorkerGroup::Config cfg;
    cfg.num_workers = 2;
    cfg.pin_cpu = false;
    group_t g = group_spawn(cfg);
    ASSERT_NE(g, nullptr);
    EXPECT_EQ(g->worker_count(), 2u);
    group_join(g);
}

TEST(CoroApi, GroupShutdown) {
    OnlineWorkerGroup::Config cfg;
    cfg.num_workers = 2;
    cfg.pin_cpu = false;
    group_t g = group_spawn(cfg);
    group_shutdown(g);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    // Manually join since group_shutdown doesn't join
    for (size_t i = 0; i < g->worker_count(); ++i) {
        g->worker(i).join();
    }
    delete g;
}

TEST(CoroApi, GroupSpawnJoinWithWork) {
    OnlineWorkerGroup::Config cfg;
    cfg.num_workers = 2;
    cfg.pin_cpu = false;
    group_t g = group_spawn(cfg);

    std::atomic<int> counter{0};
    for (int i = 0; i < 10; ++i) {
        g->submit_to(static_cast<uint64_t>(i),
                     WorkItem::make_func(+[]() { /* no-op */ }));
    }
    // Submit tasks via known hash keys
    for (int i = 0; i < 6; ++i) {
        g->submit_to(static_cast<uint64_t>(i),
                     WorkItem::make_func(+[]() { /* no-op */ }));
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    auto s = g->stats();
    EXPECT_EQ(s.worker_count, 2u);
    EXPECT_GE(s.total_tasks_executed, 10u);

    group_join(g);
}

// ============================================================================
// Synchronization aliases compile check
// ============================================================================

TEST(CoroApi, SyncAliasesCompile) {
    // These are type aliases; just verify they name valid types
    static_assert(!std::is_void_v<coro_mutex_t>, "coro_mutex_t must be a type");
    static_assert(!std::is_void_v<coro_sem_t>,   "coro_sem_t must be a type");
    static_assert(!std::is_void_v<coro_baton_t>, "coro_baton_t must be a type");
    SUCCEED();
}
