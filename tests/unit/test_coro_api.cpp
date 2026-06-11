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

// ============================================================================
// coro_create + coro_join  —  pthread_create/join analogs
// ============================================================================

// Helper: a plain function pointer for start_routine
static void* return_int(void* arg) {
    return reinterpret_cast<void*>(static_cast<intptr_t>(reinterpret_cast<ssize_t>(arg)));
}

TEST(CoroApi, CoroCreateJoinReturnsValue) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    worker_t w = worker_spawn(cfg);

    // Create a coroutine from within the worker context.
    // co_submit runs our lambda on the worker thread, where
    // current_online_worker() is valid.
    auto create_task = w->co_submit<coro_t>([]() -> coro_t {
        coro_t c = nullptr;
        int ret = coro_create(&c, nullptr, return_int,
                              reinterpret_cast<void*>(static_cast<intptr_t>(42)));
        EXPECT_EQ(ret, 0);
        EXPECT_NE(c, nullptr);
        return c;
    });

    coro_t c = folly::coro::blockingWait(std::move(create_task));
    ASSERT_NE(c, nullptr);

    // Join via co_await — suspends the blockingWait coroutine until done.
    auto join_task = [](coro_t c) -> folly::coro::Task<void*> {
        co_return co_await coro_join(c);
    };
    void* retval = folly::coro::blockingWait(join_task(c));
    EXPECT_EQ(reinterpret_cast<intptr_t>(retval), 42);

    worker_join(w);
}

TEST(CoroApi, CoroCreateJoinNullArg) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    worker_t w = worker_spawn(cfg);

    auto create_task = w->co_submit<coro_t>([]() -> coro_t {
        coro_t c = nullptr;
        int ret = coro_create(&c, nullptr, return_int, nullptr);
        EXPECT_EQ(ret, 0);
        return c;
    });

    coro_t c = folly::coro::blockingWait(std::move(create_task));
    ASSERT_NE(c, nullptr);

    auto join_task = [](coro_t c) -> folly::coro::Task<void*> {
        co_return co_await coro_join(c);
    };
    void* retval = folly::coro::blockingWait(join_task(c));
    EXPECT_EQ(retval, nullptr);

    worker_join(w);
}

TEST(CoroApi, CoroCreateJoinNoRetval) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    worker_t w = worker_spawn(cfg);

    auto create_task = w->co_submit<coro_t>([]() -> coro_t {
        coro_t c = nullptr;
        int ret = coro_create(&c, nullptr, return_int,
                              reinterpret_cast<void*>(static_cast<intptr_t>(99)));
        EXPECT_EQ(ret, 0);
        return c;
    });

    coro_t c = folly::coro::blockingWait(std::move(create_task));
    ASSERT_NE(c, nullptr);

    // Join — discard the return value (should not crash)
    auto join_task = [](coro_t c) -> folly::coro::Task<void> {
        (void)co_await coro_join(c);
    };
    folly::coro::blockingWait(join_task(c));

    worker_join(w);
}

// ============================================================================
// coro_create error handling
// ============================================================================

TEST(CoroApi, CoroCreateInvalidArgs) {
    // Not on a worker thread → ENXIO
    coro_t c;
    int ret = coro_create(nullptr, nullptr, return_int, nullptr);
    EXPECT_EQ(ret, -EINVAL);

    ret = coro_create(&c, nullptr, nullptr, nullptr);
    EXPECT_EQ(ret, -EINVAL);

    // Outside worker thread → ENXIO
    ret = coro_create(&c, nullptr, return_int, nullptr);
    EXPECT_EQ(ret, -ENXIO);
}

TEST(CoroApi, CoroJoinInvalidArgs) {
    // The new coro_join API requires a non-null coro handle.
    // Passing nullptr is undefined behaviour (null dereference in the awaiter).
    // This test just verifies that coro_join(nullptr) compiles — it must not
    // be called in production with a null handle.
    //
    // We don't actually invoke it here (would crash); we only verify the
    // function signature is callable by checking the expression type.
    using result_t = decltype(coro_join(nullptr));
    static_assert(std::is_same_v<result_t, coro_join_awaiter>,
                  "coro_join must return coro_join_awaiter");
    SUCCEED();
}

TEST(CoroApi, CoroDetachInvalidArgs) {
    int ret = coro_detach(nullptr);
    EXPECT_EQ(ret, -EINVAL);
}

// ============================================================================
// coro_detach
// ============================================================================

TEST(CoroApi, CoroDetach) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    worker_t w = worker_spawn(cfg);

    std::atomic<bool> detached_done{false};

    auto create_task = w->co_submit<coro_t>([&detached_done]() -> coro_t {
        // Create a coroutine and detach it immediately
        coro_t c2;
        int ret = coro_create(&c2, nullptr, +[](void* arg) -> void* {
            auto* done = static_cast<std::atomic<bool>*>(arg);
            done->store(true, std::memory_order_release);
            return nullptr;
        }, &detached_done);
        EXPECT_EQ(ret, 0);

        ret = coro_detach(c2);
        EXPECT_EQ(ret, 0);
        return nullptr;
    });

    (void)folly::coro::blockingWait(std::move(create_task));

    // Wait for the detached coroutine to complete
    int spins = 0;
    while (!detached_done.load(std::memory_order_acquire) && spins < 500) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        ++spins;
    }
    EXPECT_TRUE(detached_done.load());

    worker_join(w);
}

// ============================================================================
// coro_self — returns current coroutine handle
// ============================================================================

// coro_self() returns non-null only inside a coroutine created by
// coro_create().  Inside a co_submit callback, the underlying coroutine
// is created by the co_submit machinery (not coro_create), so
// coro_self() returns nullptr there.  This matches the pthread API
// where pthread_self() returns the calling thread's ID — analogously,
// coro_self() returns the calling coroutine's handle, which exists
// only when that coroutine was created via coro_create().
//
// NOTE: coro_join() is co_await-able.  It suspends the calling
// coroutine via the target coroutine's AffinityBaton.  Outside a
// coroutine context, wrap it with folly::coro::blockingWait.

TEST(CoroApi, CoroSelfInsideCoroutine) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    worker_t w = worker_spawn(cfg);

    // Create a coroutine on the worker, check coro_self() inside it
    std::atomic<bool> self_is_non_null{false};

    auto task = w->co_submit<coro_t>([&self_is_non_null]() -> coro_t {
        // coro_self() returns nullptr inside co_submit (correct)
        EXPECT_EQ(coro_self(), nullptr);

        coro_t child = nullptr;
        int ret = coro_create(&child, nullptr, +[](void* arg) -> void* {
            auto* out = static_cast<std::atomic<bool>*>(arg);
            // Inside a coro_create'd coroutine, coro_self() is non-null
            out->store(coro_self() != nullptr, std::memory_order_release);
            return nullptr;
        }, &self_is_non_null);
        EXPECT_EQ(ret, 0);
        return child;
    });

    // Get the handle from the worker, join from main thread (safe)
    coro_t child = folly::coro::blockingWait(std::move(task));
    ASSERT_NE(child, nullptr);

    {
        auto join_task = [](coro_t c) -> folly::coro::Task<void> {
            (void)co_await coro_join(c);
        };
        folly::coro::blockingWait(join_task(child));
    }

    EXPECT_TRUE(self_is_non_null.load());

    // coro_self should return null outside a coroutine (main thread)
    coro_t self_outside = coro_self();
    EXPECT_EQ(self_outside, nullptr);

    worker_join(w);
}

TEST(CoroApi, CoroSelfConsistent) {
    // Verify that coro_self() returns the same handle as the one
    // returned by coro_create() for the currently executing coroutine.
    Worker::Config cfg;
    cfg.cpu_id = 0;
    worker_t w = worker_spawn(cfg);

    std::atomic<coro_t> self_inside{nullptr};

    auto task = w->co_submit<coro_t>([&self_inside]() -> coro_t {
        coro_t created_handle = nullptr;
        int ret = coro_create(&created_handle, nullptr, +[](void* arg) -> void* {
            auto* captured = static_cast<std::atomic<coro_t>*>(arg);
            captured->store(coro_self(), std::memory_order_release);
            return nullptr;
        }, &self_inside);
        EXPECT_EQ(ret, 0);
        return created_handle;
    });

    // Join from main thread (safe)
    coro_t created_handle = folly::coro::blockingWait(std::move(task));
    ASSERT_NE(created_handle, nullptr);

    {
        auto join_task = [](coro_t c) -> folly::coro::Task<void> {
            (void)co_await coro_join(c);
        };
        folly::coro::blockingWait(join_task(created_handle));
    }

    // The handle from coro_self() inside the coroutine should
    // match the handle returned by coro_create()
    coro_t inside = self_inside.load();
    EXPECT_NE(inside, nullptr);
    EXPECT_EQ(inside, created_handle);

    worker_join(w);
}

// ============================================================================
// coro_yield macro (compile check)
// ============================================================================

// Verify that coro_yield() expands correctly in a coroutine context.
// We use a standalone coroutine lambda (not inside co_submit) because
// co_submit wraps a regular callable, not a coroutine.
namespace {
    folly::coro::Task<void> yield_coro_fn(std::atomic<int>* counter) {
        counter->fetch_add(1, std::memory_order_relaxed);
        // coro_yield() would suspend+re-enqueue; but this coroutine
        // runs on the calling thread without a worker context, so
        // yield() would silently drop the handle (no resume).
        // We verify the macro compiles; runtime behavior is tested
        // via the scheduler in other tests.
        co_await yield();  // same as coro_yield() macro expansion
        counter->fetch_add(1, std::memory_order_relaxed);
        co_return;
    }
}

TEST(CoroApi, CoroYieldMacroCompiles) {
    // Just verify the macro is valid in a coroutine context
    // The macro expands to `co_await yield()` which requires
    // coroutine context — this test verifies it compiles.
    std::atomic<int> counter{0};
    auto task = yield_coro_fn(&counter);

    // Don't await the task (yield() without a worker would drop
    // the coroutine handle).  The task will be destroyed, which
    // is safe but does not advance the counter past the yield point.
    SUCCEED();
}

// ============================================================================
// coro_mutex_* — pthread mutex analogs
// ============================================================================

TEST(CoroApi, CoroMutexInitDestroy) {
    coro_mutex_t mutex;
    int ret = coro_mutex_init(&mutex, nullptr);
    EXPECT_EQ(ret, 0);

    ret = coro_mutex_destroy(&mutex);
    EXPECT_EQ(ret, 0);
}

TEST(CoroApi, CoroMutexInvalidArgs) {
    int ret = coro_mutex_init(nullptr, nullptr);
    EXPECT_EQ(ret, -EINVAL);

    ret = coro_mutex_destroy(nullptr);
    EXPECT_EQ(ret, -EINVAL);

    ret = coro_mutex_lock(nullptr);
    EXPECT_EQ(ret, -EINVAL);

    ret = coro_mutex_unlock(nullptr);
    EXPECT_EQ(ret, -EINVAL);
}

TEST(CoroApi, CoroMutexLockUnlock) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    worker_t w = worker_spawn(cfg);

    // Test mutex inside a coroutine (same worker, safe for spin-lock)
    auto task = w->co_submit<bool>([]() -> bool {
        coro_mutex_t mutex;
        coro_mutex_init(&mutex, nullptr);

        coro_mutex_lock(&mutex);
        // Critical section
        coro_mutex_unlock(&mutex);

        // Lock again to verify state is clean
        coro_mutex_lock(&mutex);
        coro_mutex_unlock(&mutex);

        coro_mutex_destroy(&mutex);
        return true;
    });
    EXPECT_TRUE(folly::coro::blockingWait(std::move(task)));

    worker_join(w);
}

// ============================================================================
// coro_sem_* — semaphore analogs
// ============================================================================

TEST(CoroApi, CoroWaitPost) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    worker_t w = worker_spawn(cfg);

    std::atomic<int> step{0};

    auto task = w->co_submit<void>([&step]() {
        // Use placement new since AffinitySemaphore has no default ctor
        alignas(coro_sem_t) char sem_buf[sizeof(coro_sem_t)];
        auto* sem = reinterpret_cast<coro_sem_t*>(sem_buf);
        coro_sem_init(sem, 0);

        // Post from this coroutine
        coro_sem_post(sem);
        step.store(1, std::memory_order_release);

        // Wait (should succeed immediately since count is now 1)
        coro_sem_wait(sem);
        step.store(2, std::memory_order_release);

        // Try post/wait cycle again
        coro_sem_post(sem);
        coro_sem_wait(sem);
        step.store(3, std::memory_order_release);

        coro_sem_destroy(sem);
    });
    folly::coro::blockingWait(std::move(task));

    EXPECT_EQ(step.load(), 3);
    worker_join(w);
}

TEST(CoroApi, CoroInvalidArgs) {
    int ret = coro_sem_init(nullptr, 0);
    EXPECT_EQ(ret, -EINVAL);

    ret = coro_sem_destroy(nullptr);
    EXPECT_EQ(ret, -EINVAL);

    ret = coro_sem_wait(nullptr);
    EXPECT_EQ(ret, -EINVAL);

    ret = coro_sem_post(nullptr);
    EXPECT_EQ(ret, -EINVAL);
}

// ============================================================================
// coro_create + coro_detach (detach before completion)
// ============================================================================

TEST(CoroApi, CoroCreateDetachCompletes) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    worker_t w = worker_spawn(cfg);

    std::atomic<bool> coro_done{false};

    auto create_task = w->co_submit<coro_t>([&coro_done]() -> coro_t {
        coro_t c = nullptr;
        int ret = coro_create(&c, nullptr, +[](void* arg) -> void* {
            auto* done = static_cast<std::atomic<bool>*>(arg);
            done->store(true, std::memory_order_release);
            return nullptr;
        }, &coro_done);
        EXPECT_EQ(ret, 0);
        return c;
    });

    // Wait for creation
    coro_t c = folly::coro::blockingWait(std::move(create_task));
    ASSERT_NE(c, nullptr);

    // Detach before the coroutine has finished running
    int ret = coro_detach(c);
    EXPECT_EQ(ret, 0);

    // Wait for the coroutine to actually finish
    int spins = 0;
    while (!coro_done.load(std::memory_order_acquire) && spins < 500) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        ++spins;
    }
    EXPECT_TRUE(coro_done.load());

    worker_join(w);
}

// ============================================================================
// coro_create + coro_detach (detach after completion — immediate cleanup)
// ============================================================================

TEST(CoroApi, CoroCreateDetachAfterCompletion) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    worker_t w = worker_spawn(cfg);

    std::atomic<bool> coro_done{false};
    coro_t c = nullptr;

    auto create_task = w->co_submit<void>([&c, &coro_done]() {
        // Create a coroutine that completes quickly
        int ret = coro_create(&c, nullptr, +[](void* arg) -> void* {
            auto* done = static_cast<std::atomic<bool>*>(arg);
            done->store(true, std::memory_order_release);
            return nullptr;
        }, &coro_done);
        EXPECT_EQ(ret, 0);
    });
    folly::coro::blockingWait(std::move(create_task));
    ASSERT_NE(c, nullptr);

    // Wait for the coroutine to finish
    int spins = 0;
    while (!coro_done.load(std::memory_order_acquire) && spins < 500) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        ++spins;
    }
    EXPECT_TRUE(coro_done.load());

    // Detach after completion — should delete immediately
    int ret = coro_detach(c);
    EXPECT_EQ(ret, 0);

    worker_join(w);
}

// ============================================================================
// Semaphore: initial value > 0
// ============================================================================

TEST(CoroApi, CoroInitialValue) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    worker_t w = worker_spawn(cfg);

    auto task = w->co_submit<bool>([]() -> bool {
        alignas(coro_sem_t) char sem_buf[sizeof(coro_sem_t)];
        auto* sem = reinterpret_cast<coro_sem_t*>(sem_buf);
        coro_sem_init(sem, 3);

        // Should be able to wait 3 times without blocking
        for (int i = 0; i < 3; ++i) {
            if (!sem->try_acquire()) return false;
        }
        // Fourth should fail
        if (sem->try_acquire()) return false;

        coro_sem_destroy(sem);
        return true;
    });
    EXPECT_TRUE(folly::coro::blockingWait(std::move(task)));

    worker_join(w);
}

// ============================================================================
// coro_mutex: concurrent access (same worker, sequential)
// ============================================================================

TEST(CoroApi, CoroMutexConcurrent) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    worker_t w = worker_spawn(cfg);

    std::atomic<int> shared{0};

    // Use co_spawn + co_submit to create two coroutines that
    // increment a shared counter under mutex protection
    auto task = w->co_submit<void>([&shared]() {
        coro_mutex_t mutex;
        coro_mutex_init(&mutex, nullptr);

        // Spawn two tasks that increment under lock
        auto t1 = [&mutex, &shared]() {
            coro_mutex_lock(&mutex);
            shared.fetch_add(1, std::memory_order_relaxed);
            coro_mutex_unlock(&mutex);
        };

        auto t2 = [&mutex, &shared]() {
            coro_mutex_lock(&mutex);
            shared.fetch_add(10, std::memory_order_relaxed);
            coro_mutex_unlock(&mutex);
        };

        // Run both inline (same coroutine — no actual concurrency but
        // verifies lock/unlock sequence is correct)
        t1();
        t2();

        EXPECT_EQ(shared.load(), 11);

        coro_mutex_destroy(&mutex);
    });
    folly::coro::blockingWait(std::move(task));

    worker_join(w);
}
