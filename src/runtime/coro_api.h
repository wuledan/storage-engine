#pragma once
// Coroutine API — pthread-aligned interface for thread-to-coroutine migration.
// Single include for worker lifecycle, coroutine spawning, and synchronization.
#include "coro_primitives.h"
#include "online_worker.h"
#include "online_group.h"
#include "offline_group.h"
#include <memory>
#include <type_traits>

namespace storage::runtime {

// ═══════════════════════════════════════════════════════
// Worker lifecycle (pthread_create/join analog)
// ═══════════════════════════════════════════════════════

using worker_t = OnlineWorker*;

inline worker_t worker_spawn(const Worker::Config& cfg) {
    auto* w = new OnlineWorker(cfg);
    w->start();
    return w;
}

inline void worker_shutdown(worker_t w) {
    if (w) w->stop();  // request_stop only, non-blocking
}

inline void worker_join(worker_t w) {
    if (!w) return;
    w->stop();
    w->join();
    delete w;
}

// ═══════════════════════════════════════════════════════
// Group lifecycle
// ═══════════════════════════════════════════════════════

using group_t = OnlineWorkerGroup*;

inline group_t group_spawn(const OnlineWorkerGroup::Config& cfg) {
    auto* g = new OnlineWorkerGroup(cfg);
    g->start();
    return g;
}

inline void group_shutdown(group_t g) {
    if (!g) return;
    // Iterate workers and request stop (non-blocking)
    for (size_t i = 0; i < g->worker_count(); ++i) {
        g->worker(i).stop();
    }
}

inline void group_join(group_t g) {
    if (!g) return;
    g->stop();   // already includes join() per worker
    delete g;
}

// ═══════════════════════════════════════════════════════
// Coroutine spawning
// ═══════════════════════════════════════════════════════

// ── co_spawn for captureless function pointers ──
// Submits directly to the engine queue for immediate scheduling.
inline void co_spawn(void(*fn)()) {
    auto* w = current_online_worker();
    if (w) w->submit_engine(WorkItem::make_func(fn));
}

// ── co_spawn for capturing lambdas / callables ──
// Wraps the callable in a coroutine with initial_suspend = always,
// so it is safely submitted to the engine queue without ever running
// on the submitting thread. The callable is stored as a coroutine
// function parameter (in the coroutine frame), avoiding dangling captures.
//
// Must be called from within a worker context (current_online_worker() != nullptr).
template<typename F>
void co_spawn(F&& func) {
    auto* w = current_online_worker();
    if (!w) return;

    // Local coroutine type: initial_suspend = always (submitted via handle),
    // final_suspend = never (frame auto-destroyed after body runs).
    struct SpawnTask {
        struct promise_type {
            SpawnTask get_return_object() {
                return SpawnTask{
                    std::coroutine_handle<promise_type>::from_promise(*this)};
            }
            std::suspend_always initial_suspend() noexcept { return {}; }
            std::suspend_never  final_suspend()   noexcept { return {}; }
            void return_void()   noexcept {}
            void unhandled_exception() noexcept { std::terminate(); }
        };
        std::coroutine_handle<promise_type> handle;
    };

    // Generic lambda (no captures) — the callable, `fn`, is a coroutine
    // function parameter stored in the coroutine frame (survives suspends).
    // The coroutine body does not run until the scheduler resumes it.
    auto task = [](auto fn) -> SpawnTask {
        fn();
        co_return;
    }(std::forward<F>(func));

    // Submit the handle to the engine queue.  The queue now "owns" the
    // coroutine; we release our handle reference.
    w->submit_engine(WorkItem::make_coro(task.handle));
    task.handle = {};
}

// ── co_async: submit + get result ──
// Wraps OnlineWorker::co_submit<R> for the current worker.
// Usage:
//   int v = co_await co_async<int>([] { return 42; });
//   co_await co_async<void>([&] { do_work(); });
//
template<typename R, typename F>
folly::coro::Task<R> co_async(F&& func) {
    auto* w = current_online_worker();
    if (!w) {
        // Not on a worker thread — return default / void.
        if constexpr (std::is_void_v<R>) {
            co_return;
        } else {
            co_return R{};
        }
    }
    co_return co_await w->co_submit<R>(std::forward<F>(func));
}

// ═══════════════════════════════════════════════════════
// Synchronization aliases
// ═══════════════════════════════════════════════════════

using coro_mutex_t = adapt::AffinityMutex;
using coro_sem_t   = adapt::AffinitySemaphore;
using coro_baton_t = adapt::AffinityBaton;

// ═══════════════════════════════════════════════════════
// Yield
// ═══════════════════════════════════════════════════════

// Use: co_yield();  (macro, so it works like pthread_yield())
#define co_yield()  co_await yield()

}  // namespace storage::runtime
