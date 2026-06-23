#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>
#include "runtime/coro_task.h"
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
    adapt::blockingWait(std::move(task));

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
    int result = adapt::blockingWait(std::move(task));
    EXPECT_EQ(result, 42);

    worker_join(w);
}

TEST(CoroApi, CoSubmitReturnsVoid) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    worker_t w = worker_spawn(cfg);

    std::atomic<bool> called{false};
    auto task = w->co_submit<void>([&called] { called = true; });
    adapt::blockingWait(std::move(task));

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
    EXPECT_THROW(adapt::blockingWait(std::move(task)), std::runtime_error);

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

    coro_t c = adapt::blockingWait(std::move(create_task));
    ASSERT_NE(c, nullptr);

    // Join via co_await — suspends the blockingWait coroutine until done.
    auto join_task = [](coro_t c) -> adapt::Task<void*> {
        co_return co_await coro_join(c);
    };
    void* retval = adapt::blockingWait(join_task(c));
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

    coro_t c = adapt::blockingWait(std::move(create_task));
    ASSERT_NE(c, nullptr);

    auto join_task = [](coro_t c) -> adapt::Task<void*> {
        co_return co_await coro_join(c);
    };
    void* retval = adapt::blockingWait(join_task(c));
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

    coro_t c = adapt::blockingWait(std::move(create_task));
    ASSERT_NE(c, nullptr);

    // Join — discard the return value (should not crash)
    auto join_task = [](coro_t c) -> adapt::Task<void> {
        (void)co_await coro_join(c);
    };
    adapt::blockingWait(join_task(c));

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

    (void)adapt::blockingWait(std::move(create_task));

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
// coroutine context, wrap it with adapt::blockingWait.

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
    coro_t child = adapt::blockingWait(std::move(task));
    ASSERT_NE(child, nullptr);

    {
        auto join_task = [](coro_t c) -> adapt::Task<void> {
            (void)co_await coro_join(c);
        };
        adapt::blockingWait(join_task(child));
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
    coro_t created_handle = adapt::blockingWait(std::move(task));
    ASSERT_NE(created_handle, nullptr);

    {
        auto join_task = [](coro_t c) -> adapt::Task<void> {
            (void)co_await coro_join(c);
        };
        adapt::blockingWait(join_task(created_handle));
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
    adapt::Task<void> yield_coro_fn(std::atomic<int>* counter) {
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

    // coro_mutex_lock() has been removed — use co_await mutex.co_lock()
    ret = coro_mutex_unlock(nullptr);
    EXPECT_EQ(ret, -EINVAL);
}

TEST(CoroApi, CoroMutexLockUnlock) {
    adapt::blockingWait([]() -> adapt::Task<void> {
        coro_mutex_t mutex;
        coro_mutex_init(&mutex, nullptr);

        co_await mutex.co_lock();
        // Critical section
        mutex.unlock();

        // Lock again to verify state is clean
        co_await mutex.co_lock();
        mutex.unlock();

        coro_mutex_destroy(&mutex);
        co_return;
    }());
}

// ============================================================================
// coro_sem_* — semaphore analogs
// ============================================================================

TEST(CoroApi, CoroWaitPost) {
    adapt::blockingWait([]() -> adapt::Task<void> {
        // Use placement new since AffinitySemaphore has no default ctor
        alignas(coro_sem_t) char sem_buf[sizeof(coro_sem_t)];
        auto* sem = reinterpret_cast<coro_sem_t*>(sem_buf);
        coro_sem_init(sem, 0);

        // Post from this coroutine
        coro_sem_post(sem);

        // Wait (should succeed immediately since count is now 1)
        co_await sem->acquire();

        // Try post/wait cycle again
        coro_sem_post(sem);
        co_await sem->acquire();

        coro_sem_destroy(sem);
        co_return;
    }());
}

TEST(CoroApi, CoroInvalidArgs) {
    int ret = coro_sem_init(nullptr, 0);
    EXPECT_EQ(ret, -EINVAL);

    ret = coro_sem_destroy(nullptr);
    EXPECT_EQ(ret, -EINVAL);

    // coro_sem_wait() has been removed — use co_await sem->acquire()
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
    coro_t c = adapt::blockingWait(std::move(create_task));
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
    adapt::blockingWait(std::move(create_task));
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
    EXPECT_TRUE(adapt::blockingWait(std::move(task)));

    worker_join(w);
}

// ============================================================================
// coro_mutex: concurrent access (same worker, sequential)
// ============================================================================

TEST(CoroApi, CoroMutexConcurrent) {
    std::atomic<int> shared{0};

    adapt::blockingWait([&shared]() -> adapt::Task<void> {
        coro_mutex_t mutex;
        coro_mutex_init(&mutex, nullptr);

        // Spawn two tasks that increment under lock (coroutine lambdas)
        auto t1 = [&mutex, &shared]() -> adapt::Task<void> {
            co_await mutex.co_lock();
            shared.fetch_add(1, std::memory_order_relaxed);
            mutex.unlock();
        };

        auto t2 = [&mutex, &shared]() -> adapt::Task<void> {
            co_await mutex.co_lock();
            shared.fetch_add(10, std::memory_order_relaxed);
            mutex.unlock();
        };

        // Run both sequentially (same coroutine — no actual concurrency but
        // verifies lock/unlock sequence is correct)
        co_await t1();
        co_await t2();

        EXPECT_EQ(shared.load(), 11);

        coro_mutex_destroy(&mutex);
        co_return;
    }());
}

// ============================================================================
// coro_thread — std::thread-like RAII wrapper
// ============================================================================

// Helper: a callable that stores a value into an atomic<int>
struct StoreValue {
    std::atomic<int>* target;
    int value;
    void* operator()() const {
        target->store(value, std::memory_order_release);
        return nullptr;
    }
};

TEST(CoroThread, DefaultConstructed) {
    coro_thread t;
    EXPECT_FALSE(t.joinable());
    EXPECT_EQ(t.get_id(), nullptr);
    EXPECT_EQ(t.native_handle(), nullptr);
}

TEST(CoroThread, SpawnAndJoin) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    worker_t w = worker_spawn(cfg);
    ASSERT_NE(w, nullptr);

    std::atomic<int> val{0};

    // Create coro_thread from within the worker context.
    // The coroutine is submitted to the engine queue.  After
    // co_submit returns we wait for it to complete.
    auto create = w->co_submit<void>([&val]() {
        coro_thread thr(StoreValue{&val, 42});
        EXPECT_TRUE(thr.joinable());
        // thr destructor called here → detaches the handle
    });
    adapt::blockingWait(std::move(create));

    // Wait for the detached coroutine to complete
    int spins = 0;
    while (val.load(std::memory_order_acquire) != 42 && spins < 1000) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        ++spins;
    }
    EXPECT_EQ(val.load(), 42);

    worker_join(w);
}

TEST(CoroThread, SpawnAndJoinWithArgs) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    worker_t w = worker_spawn(cfg);
    ASSERT_NE(w, nullptr);

    std::atomic<int> val{0};

    // coro_thread with additional args forwarded to the callable
    auto create = w->co_submit<void>([&val]() {
        auto fn = [](std::atomic<int>* t, int v) -> void* {
            t->store(v, std::memory_order_release);
            return nullptr;
        };
        coro_thread thr(fn, &val, 77);
        EXPECT_TRUE(thr.joinable());
    });
    adapt::blockingWait(std::move(create));

    int spins = 0;
    while (val.load(std::memory_order_acquire) != 77 && spins < 1000) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        ++spins;
    }
    EXPECT_EQ(val.load(), 77);

    worker_join(w);
}

TEST(CoroThread, MoveSemantics) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    worker_t w = worker_spawn(cfg);
    ASSERT_NE(w, nullptr);

    std::atomic<int> val{0};

    auto create = w->co_submit<void>([&val]() {
        coro_thread t1;
        EXPECT_FALSE(t1.joinable());

        coro_thread t2(StoreValue{&val, 99});
        EXPECT_TRUE(t2.joinable());

        // Move-assign
        t1 = std::move(t2);
        EXPECT_TRUE(t1.joinable());
        EXPECT_FALSE(t2.joinable());
        EXPECT_EQ(t2.get_id(), nullptr);

        // Move-construct
        coro_thread t3(std::move(t1));
        EXPECT_TRUE(t3.joinable());
        EXPECT_FALSE(t1.joinable());
        EXPECT_EQ(t1.get_id(), nullptr);
    });
    adapt::blockingWait(std::move(create));

    int spins = 0;
    while (val.load(std::memory_order_acquire) != 99 && spins < 1000) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        ++spins;
    }
    EXPECT_EQ(val.load(), 99);

    worker_join(w);
}

TEST(CoroThread, Detach) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    worker_t w = worker_spawn(cfg);
    ASSERT_NE(w, nullptr);

    std::atomic<int> val{0};

    auto create = w->co_submit<void>([&val]() {
        coro_thread thr(StoreValue{&val, 7});
        thr.detach();
        EXPECT_FALSE(thr.joinable());
    });
    adapt::blockingWait(std::move(create));

    int spins = 0;
    while (val.load(std::memory_order_acquire) != 7 && spins < 1000) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        ++spins;
    }
    EXPECT_EQ(val.load(), 7);

    worker_join(w);
}

TEST(CoroThread, DestructorDetaches) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    worker_t w = worker_spawn(cfg);
    ASSERT_NE(w, nullptr);

    std::atomic<int> val{0};

    // coro_thread goes out of scope (destructor detaches)
    {
        auto create = w->co_submit<void>([&val]() {
            coro_thread thr(StoreValue{&val, 100});
            EXPECT_TRUE(thr.joinable());
            // destructor called when lambda returns
        });
        adapt::blockingWait(std::move(create));
    }

    int spins = 0;
    while (val.load(std::memory_order_acquire) != 100 && spins < 1000) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        ++spins;
    }
    EXPECT_EQ(val.load(), 100);

    worker_join(w);
}

TEST(CoroThread, NativeHandle) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    worker_t w = worker_spawn(cfg);
    ASSERT_NE(w, nullptr);

    auto create = w->co_submit<coro_t>([]() -> coro_t {
        coro_thread thr(+[]() -> void* { return nullptr; });
        coro_t h = thr.native_handle();
        EXPECT_NE(h, nullptr);
        EXPECT_EQ(thr.get_id(), h);
        return h;
    });

    // Just verify we got a non-null handle
    coro_t handle = adapt::blockingWait(std::move(create));
    EXPECT_NE(handle, nullptr);

    worker_join(w);
}

// ============================================================================
// this_coro namespace
// ============================================================================

TEST(CoroThread, ThisCoroGetIdOutsideCoroutine) {
    // Outside any coroutine context, get_id() returns nullptr.
    coro_t id = this_coro::get_id();
    EXPECT_EQ(id, nullptr);
}

TEST(CoroThread, ThisCoroGetIdInsideCoroutine) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    worker_t w = worker_spawn(cfg);
    ASSERT_NE(w, nullptr);

    std::atomic<coro_t> id_inside{nullptr};

    // Create a coroutine via coro_thread and get the handle from inside
    auto create = w->co_submit<void>([&id_inside]() {
        coro_thread thr(+[](void* arg) -> void* {
            auto* out = static_cast<std::atomic<coro_t>*>(arg);
            out->store(this_coro::get_id(), std::memory_order_release);
            return nullptr;
        }, &id_inside);
        // thr detaches on destruction
    });
    adapt::blockingWait(std::move(create));

    int spins = 0;
    while (id_inside.load(std::memory_order_acquire) == nullptr && spins < 1000) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        ++spins;
    }

    // The handle returned by this_coro::get_id() from inside the
    // coro_thread-created coroutine should be non-null.
    EXPECT_NE(id_inside.load(), nullptr);

    worker_join(w);
}

// ============================================================================
// when_all — spawn N tasks, wait for all, collect results
// ============================================================================

// ═══════════════════════════════════════════════════════════════
// WhenAll tests
//
// These tests use run_busy() mode (busy_poll = true) so the engine
// queue is drained every tick alongside the timer queue — the
// persistent timer coroutine on P1 would otherwise starve P2.
// The coroutine handle is submitted via submit_engine() and
// the test spins on an atomic result.
//
// WhenAllOne   — single callable, single result
// WhenAllTwo   — two callables, tuple of results
// WhenAllThree — three callables, tuple of results
// WhenAllEmpty — zero-arg sanity check (no worker needed)
// ═══════════════════════════════════════════════════════════════

TEST(CoroApi, WhenAllOne) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    auto* w = new OnlineWorker(cfg);
    w->scheduler().set_busy_poll(true);
    w->start();

    std::atomic<int> result{0};

    auto coro_fn = [&result]() -> adapt::Task<void> {
        auto [v] = co_await when_all([] { return 42; });
        result.store(v, std::memory_order_release);
        co_return;
    };

    auto wtc = coro_fn();
    w->submit_engine(WorkItem::make_coro(wtc.release()));

    int spins = 0;
    while (result.load(std::memory_order_acquire) != 42 && spins < 2000) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        ++spins;
    }
    EXPECT_EQ(result.load(), 42);

    w->stop();
    w->join();
    delete w;
}

TEST(CoroApi, WhenAllTwo) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    auto* w = new OnlineWorker(cfg);
    w->scheduler().set_busy_poll(true);
    w->start();

    std::atomic<int> sum{0};

    auto coro_fn = [&sum]() -> adapt::Task<void> {
        auto [a, b] = co_await when_all(
            [] { return 10; },
            [] { return 32; }
        );
        sum.store(a + b, std::memory_order_release);
        co_return;
    };

    auto wtc = coro_fn();
    w->submit_engine(WorkItem::make_coro(wtc.release()));

    int spins = 0;
    while (sum.load(std::memory_order_acquire) != 42 && spins < 2000) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        ++spins;
    }
    EXPECT_EQ(sum.load(), 42);

    w->stop();
    w->join();
    delete w;
}

TEST(CoroApi, WhenAllThree) {
    Worker::Config cfg;
    cfg.cpu_id = 0;
    auto* w = new OnlineWorker(cfg);
    w->scheduler().set_busy_poll(true);
    w->start();

    std::atomic<int> result{0};

    auto coro_fn = [&result]() -> adapt::Task<void> {
        auto [a, b, c] = co_await when_all(
            [] { return 1; },
            [] { return 2; },
            [] { return 3; }
        );
        result.store(a + b + c, std::memory_order_release);
        co_return;
    };

    auto wtc = coro_fn();
    w->submit_engine(WorkItem::make_coro(wtc.release()));

    int spins = 0;
    while (result.load(std::memory_order_acquire) != 6 && spins < 2000) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        ++spins;
    }
    EXPECT_EQ(result.load(), 6);

    w->stop();
    w->join();
    delete w;
}

TEST(CoroApi, WhenAllEmpty) {
    // when_all() with no arguments should return immediately
    // and produce an empty tuple.
    bool ok = false;
    auto test_fn = [&]() -> adapt::Task<void> {
        auto t = co_await when_all();
        static_assert(std::tuple_size_v<decltype(t)> == 0,
                      "empty when_all must produce empty tuple");
        ok = true;
    };
    // blockingWait runs the coroutine on the calling thread.
    // when_all(N=0) does not need a worker context (no tasks to spawn).
    adapt::blockingWait(test_fn());
    EXPECT_TRUE(ok);
}
