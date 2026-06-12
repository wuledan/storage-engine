#pragma once
// Coroutine API — pthread-aligned interface.
// Workers are transparent. All submission uses TLS (current worker).
//
// API design mirrors pthread exactly:
//   coro_create    → pthread_create
//   coro_join      → pthread_join
//   coro_detach    → pthread_detach
//   coro_self      → pthread_self
//   coro_yield     → pthread_yield (macro, expands to co_await yield())
//   coro_mutex_*   → pthread_mutex_*
//   coro_sem_*     → sem_*
//
// All functions return 0 on success, -errno on failure.
//
// Worker and Group lifecycle are also provided:
//   worker_spawn / worker_shutdown / worker_join
//   group_spawn  / group_shutdown  / group_join
//
// Legacy convenience APIs (kept for backward compatibility):
//   co_spawn(fn) / co_spawn(lambda) — fire-and-forget submission
//   co_async<R>(func)               — submit + co_await result
//   co_yield()                      — macro for co_await yield()

#include "coro_primitives.h"
#include "online_worker.h"
#include "online_group.h"
#include "work_stealing_executor.h"
#include "metric_counter.h"
#include <memory>
#include <atomic>
#include <type_traits>
#include <functional>  // std::invoke, std::invoke_result_t
#include <tuple>
#include <utility>     // std::index_sequence, std::index_sequence_for
#include <cstring>     // strerror_r
#include <cerrno>

namespace storage::runtime {

namespace {
metric::MetricCounter g_coro_created_ctr;
metric::MetricCounter g_coro_destroyed_ctr;
}

// ═══════════════════════════════════════════════════════
// Thread-local: current coroutine handle (for coro_self)
// ═══════════════════════════════════════════════════════

namespace detail {
    inline thread_local const void* tls_current_coro = nullptr;
}

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
    for (size_t i = 0; i < g->worker_count(); ++i) {
        g->worker(i).stop();
    }
}

inline void group_join(group_t g) {
    if (!g) return;
    g->stop();
    delete g;
}

// ═══════════════════════════════════════════════════════
// Opaque types for the pthread-aligned coroutine API
// ═══════════════════════════════════════════════════════

using coro_t = void*;                          // opaque handle (points to coro_state)
using coro_attr_t = struct coro_attr_st {};    // reserved for future use
using coro_mutex_t = adapt::AffinityMutex;
using coro_mutexattr_t = struct coro_mutexattr_st {};  // reserved
using coro_sem_t = adapt::AffinitySemaphore;

// ═══════════════════════════════════════════════════════
// Internal: coroutine state (for join / detach / self)
// ═══════════════════════════════════════════════════════

struct coro_state {
    adapt::AffinityBaton baton;          // signaled when routine completes
    void* retval{nullptr};               // return value from start_routine
    bool detached{false};                // if true, auto-delete on completion
    std::atomic<bool> done{false};       // completion flag (release/acquire)

    static coro_state* from_handle(coro_t h) noexcept {
        return static_cast<coro_state*>(h);
    }
};

// ═══════════════════════════════════════════════════════
// coro_create  —  like pthread_create
//
// Creates a coroutine that executes start_routine(arg) on the current
// worker.  Returns an opaque handle via `coro`.  The handle can be used
// with coro_join() or coro_detach().
//
// Must be called from within a worker thread context
// (current_online_worker() != nullptr).
//
// The coroutine is submitted to the engine queue and scheduled normally.
// ═══════════════════════════════════════════════════════

inline int coro_create(coro_t *coro, const coro_attr_t * /*attr*/,
                        void *(*start_routine)(void*), void *arg) {
    if (!coro || !start_routine) return -EINVAL;

    auto* w = current_online_worker();
    if (!w) return -ENXIO;  // not on a worker thread

    auto* state = new coro_state;
    auto route = w->make_route_func();

    // ── Engine coroutine type ──
    //   initial_suspend = always  : submitted via handle, never runs inline
    //   final_suspend   = never   : coroutine frame destroyed after co_return
    struct CoroTask {
        struct promise_type {
            CoroTask get_return_object() {
                return CoroTask{
                    std::coroutine_handle<promise_type>::from_promise(*this)};
            }
            std::suspend_always initial_suspend() noexcept { return {}; }
            std::suspend_never  final_suspend()   noexcept { return {}; }
            void return_void()   noexcept {}
            void unhandled_exception() noexcept { std::terminate(); }
        };
        std::coroutine_handle<promise_type> handle;
    };

    // Generic lambda (no captures) — all state lives as coroutine function
    // parameters in the coroutine frame, surviving suspends.
    // This avoids dangling-capture issues.
    auto task = [](void* (*fn)(void*), void* a,
                    coro_state* s, adapt::RouteFunc rt) -> CoroTask {
        // Track this coroutine for coro_self()
        detail::tls_current_coro = s;
        s->retval = fn(a);
        detail::tls_current_coro = nullptr;

        // Signal completion — must happen before the post so that any
        // concurrent coro_join awaiter sees baton.ready() == true.
        s->done.store(true, std::memory_order_release);
        s->baton.post(rt);

        // Auto-cleanup if detached before completion
        if (s->detached) {
            g_coro_destroyed_ctr << 1;
            delete s;
        }
        co_return;
    }(start_routine, arg, state, std::move(route));

    // Submit to engine queue — the queue now "owns" the handle
    w->submit_engine(WorkItem::make_coro(task.handle));
    task.handle = {};

    *coro = static_cast<coro_t>(state);
    g_coro_created_ctr << 1;
    return 0;
}

// ═══════════════════════════════════════════════════════
// coro_join  —  like pthread_join
//
// Returns a co_await-able awaiter.  The calling coroutine suspends until
// the target coroutine completes, then receives the return value.
//
// Usage:  void* result = co_await coro_join(my_coro);
//
// The awaiter delegates to AffinityBaton's waiter infrastructure:
// the WaiterNode is embedded inside coro_join_awaiter (which lives in
// the awaiting coroutine's frame), so it remains valid for the entire
// suspend period.
//
// SAFETY: coro must be non-null (obtained from a prior successful call
// to coro_create).  Passing nullptr is undefined behaviour.
// ═══════════════════════════════════

struct coro_join_awaiter {
    coro_state* state;
    adapt::AffinityBaton::Awaiter baton_awaiter;

    coro_join_awaiter(coro_state* s)
        : state(s)
        , baton_awaiter{s->baton, adapt::AffinityBaton::WaiterNode{}}
    {}

    bool await_ready() noexcept {
        return state->baton.ready();
    }

    void await_suspend(std::coroutine_handle<> h) noexcept {
        baton_awaiter.await_suspend(h);
    }

    void* await_resume() noexcept {
        void* ret = state->retval;
        g_coro_destroyed_ctr << 1;
        delete state;
        return ret;
    }
};

inline coro_join_awaiter coro_join(coro_t coro) noexcept {
    return coro_join_awaiter{coro_state::from_handle(coro)};
}

// ═══════════════════════════════════════════════════════
// coro_detach  —  like pthread_detach
//
// Marks the coroutine as detached.  When it completes, internal state
// is freed automatically.  Cannot be joined after detach.
// ═══════════════════════════════════════════════════════

inline int coro_detach(coro_t coro) {
    if (!coro) return -EINVAL;
    auto* state = coro_state::from_handle(coro);
    state->detached = true;
    // If already finished, clean up now
    if (state->done.load(std::memory_order_acquire)) {
        g_coro_destroyed_ctr << 1;
        delete state;
    }
    return 0;
}

// ═══════════════════════════════════════════════════════
// coro_self  —  like pthread_self
//
// Returns the handle of the currently executing coroutine.
// Returns nullptr if called outside a coroutine context.
// ═══════════════════════════════════════════════════════

inline coro_t coro_self() {
    return const_cast<coro_t>(detail::tls_current_coro);
}

// ═══════════════════════════════════════════════════════
// coro_yield  —  like pthread_yield (macro)
//
// Usage:  coro_yield();
//
// Must appear inside a coroutine function (co_await context).
// ═══════════════════════════════════════════════════════

#define coro_yield()  co_await yield()

// ═══════════════════════════════════════════════════════
// Mutex  —  pthread_mutex_t analogs
//
/// These are blocking wrappers.  In coroutine context, prefer using
// co_await mutex.co_lock() directly for true cooperative semantics.
// ═══════════════════════════════════════════════════════

inline int coro_mutex_init(coro_mutex_t *mutex,
                            const coro_mutexattr_t * /*attr*/) {
    if (!mutex) return -EINVAL;
    // Trivially constructible — placement new to ensure zero-initialized
    // state (atomic<uintptr_t>{0}) even if caller reuses memory.
    new (mutex) coro_mutex_t();
    return 0;
}

inline int coro_mutex_destroy(coro_mutex_t *mutex) {
    if (!mutex) return -EINVAL;
    mutex->~coro_mutex_t();
    return 0;
}

inline int coro_mutex_lock(coro_mutex_t *mutex) {
    if (!mutex) return -EINVAL;
    // Blocking spin-lock — only safe when caller is on the same worker.
    // AffinityMutex::lock() spin-waits internally.
    mutex->lock();
    return 0;
}

inline int coro_mutex_unlock(coro_mutex_t *mutex) {
    if (!mutex) return -EINVAL;
    mutex->unlock();
    return 0;
}

// ═══════════════════════════════════════════════════════
// Semaphore  —  sem_t analogs
//
// These are blocking wrappers.  In coroutine context, prefer using
// co_await sem.acquire() directly.
// ═══════════════════════════════════════════════════════

inline int coro_sem_init(coro_sem_t *sem, unsigned int value) {
    if (!sem) return -EINVAL;
    new (sem) coro_sem_t(value);
    return 0;
}

inline int coro_sem_destroy(coro_sem_t *sem) {
    if (!sem) return -EINVAL;
    sem->~coro_sem_t();
    return 0;
}

inline int coro_sem_wait(coro_sem_t *sem) {
    if (!sem) return -EINVAL;
    // Blocking spin-wait on try_acquire()
    while (!sem->try_acquire()) {
        __builtin_ia32_pause();
    }
    return 0;
}

inline int coro_sem_post(coro_sem_t *sem) {
    if (!sem) return -EINVAL;
    sem->release();
    return 0;
}

// ═══════════════════════════════════════════════════════
// Legacy convenience APIs (kept for backward compatibility)
// ═══════════════════════════════════════════════════════

// ── co_spawn for captureless function pointers ──
inline void co_spawn(void(*fn)()) {
    auto* w = current_online_worker();
    if (w) w->submit_engine(WorkItem::make_func(fn));
}

// ── co_spawn for capturing lambdas / callables ──
template<typename F>
void co_spawn(F&& func) {
    auto* w = current_online_worker();
    if (!w) return;

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

    auto task = [](auto fn) -> SpawnTask {
        fn();
        co_return;
    }(std::forward<F>(func));

    w->submit_engine(WorkItem::make_coro(task.handle));
    task.handle = {};
}

// ── co_async: submit + get result ──
template<typename R, typename F>
folly::coro::Task<R> co_async(F&& func) {
    auto* w = current_online_worker();
    if (!w) {
        if constexpr (std::is_void_v<R>) {
            co_return;
        } else {
            co_return R{};
        }
    }
    co_return co_await w->co_submit<R>(std::forward<F>(func));
}

// ═══════════════════════════════════════════════════════
// Synchronization type aliases (backward compat)
// ═══════════════════════════════════════════════════════

using coro_baton_t = adapt::AffinityBaton;

// ═══════════════════════════════════════════════════════════
// coro_thread — std::thread-like RAII wrapper around
//               coro_create / coro_join / coro_detach
//
// Mirrors std::thread's interface.  Must be constructed from
// within a worker context (current_online_worker() != nullptr)
// because coro_create requires it.
// ═══════════════════════════════════════════════════════════

class coro_thread {
public:
    using id = coro_t;

    // ── Default constructor: not joinable ──
    coro_thread() noexcept = default;

    // ── Move constructor ──
    coro_thread(coro_thread&& other) noexcept
        : handle_(other.handle_) {
        other.handle_ = nullptr;
    }

    // ── Move assignment ──
    coro_thread& operator=(coro_thread&& other) noexcept {
        if (joinable()) detach();
        handle_ = other.handle_;
        other.handle_ = nullptr;
        return *this;
    }

    // ── Spawning constructor: creates a coroutine that
    //     executes fn(args...).  Requires worker context.
    // ──
    template<typename F, typename... Args>
    explicit coro_thread(F&& fn, Args&&... args) {
        // Build a void*() callable that wraps fn(args...)
        auto wrapper = [fn = std::forward<F>(fn),
                        ... args = std::forward<Args>(args)]() -> void* {
            if constexpr (std::is_void_v<std::invoke_result_t<F, Args...>>) {
                std::invoke(fn, args...);
                return nullptr;
            } else {
                using R = std::invoke_result_t<F, Args...>;
                auto* result = new R(std::invoke(fn, args...));
                return static_cast<void*>(result);
            }
        };

        using wrapper_t = std::decay_t<decltype(wrapper)>;
        auto* heap_wrapper = new wrapper_t(std::move(wrapper));

        // Captureless lambda — decays to void* (*)(void*)
        auto start_fn = [](void* ctx) -> void* {
            auto* w = static_cast<wrapper_t*>(ctx);
            void* ret = (*w)();
            delete w;
            return ret;
        };

        int rc = coro_create(&handle_, nullptr, start_fn, heap_wrapper);
        if (rc != 0) {
            delete heap_wrapper;
            handle_ = nullptr;
        }
    }

    // ── Destructor: detach if still joinable ──
    ~coro_thread() {
        if (joinable()) detach();
    }

    coro_thread(const coro_thread&) = delete;

    // ── Observers ──
    bool joinable() const noexcept { return handle_ != nullptr; }
    id get_id() const noexcept { return handle_; }
    coro_t native_handle() noexcept { return handle_; }

    // ── Join: returns a co_await-able awaiter.
    //     Usage:   void* result = co_await thr.join();
    // ──
    auto join() {
        auto h = handle_;
        handle_ = nullptr;
        return coro_join(h);
    }

    // ── Detach: mark for auto-cleanup on completion ──
    void detach() {
        if (handle_) {
            coro_detach(handle_);
            handle_ = nullptr;
        }
    }

    // ── Static ──
    static id current_id() { return coro_self(); }

private:
    coro_t handle_{nullptr};
};

// ── this_coro namespace (like std::this_thread) ──
namespace this_coro {

    /// Yield the current coroutine (equivalent to co_await yield()).
    /// Must be called from within a coroutine context.
    inline auto yield() { return storage::runtime::yield(); }

    /// Return the handle of the current coroutine (or nullptr if none).
    inline coro_t get_id() { return coro_self(); }

} // namespace this_coro

// ═══════════════════════════════════════════════════════════════
// when_all — spawn N callables and co_await all completions
//
// Returns a co_await-able object that suspends until all spawned
// tasks have completed, then yields a std::tuple of results.
//
// All callables must return non-void values.  Each callable is
// wrapped in a coroutine (SpawnTask) and submitted to the current
// executor's high-priority queue:
//   - OnlineWorker::enqueue_affine()  (P0, drained every tick)
//   - WorkStealingExecutor::add()     (WSE worker)
//
// The last task to finish posts the AffinityBaton, which resumes
// the original caller on the correct worker thread (affinity).
//
// NOTE: For OnlineWorker we use enqueue_affine (affine queue, P0)
// instead of submit_engine (engine queue, P2) because the persistent
// timer coroutine created in the OnlineWorker constructor keeps the
// timer queue (P1) constantly non-empty, starving the P2 engine queue.
// The affine queue is drained unconditionally in drain_p0() on every
// scheduler iteration, guaranteeing forward progress.
//
// Usage:
//   auto [r1, r2] = co_await when_all(
//       [] { return 1; },
//       [] { return 2; }
//   );
//
//   auto [a, b, c] = co_await when_all(taskA, taskB, taskC);
// ═══════════════════════════════════════════════════════════════

template<typename... Fs>
auto when_all(Fs&&... funcs) {
    using namespace adapt;
    constexpr size_t N = sizeof...(Fs);

    // Result tuple — all callables must return non-void values.
    // For N == 0 this is std::tuple<>.
    using ResultTuple = std::tuple<std::invoke_result_t<std::decay_t<Fs>>...>;

    struct SharedState {
        std::atomic<size_t> remaining{N};
        AffinityBaton baton;
        ResultTuple results;
    };

    auto state = std::make_shared<SharedState>();

    // When N == 0 there are no tasks to spawn; post the baton
    // immediately so the awaiter returns without suspending.
    if constexpr (N == 0) {
        state->baton.post_direct();
    }

    // ── Spawn tasks (only for N > 0) ──
    if constexpr (N > 0) {

    // Build a RouteFunc from the current executor context.
    RouteFunc route = []() -> RouteFunc {
        if (auto* ow = current_online_worker()) {
            return ow->make_route_func();
        } else if (auto* exec = WorkStealingExecutor::current_executor()) {
            return exec->make_route_func();
        }
        return RouteFunc{};
    }();

    // ── SpawnTask — local coroutine type for wrapping capturing lambdas ──
    //   initial_suspend = always  : submitted via handle, never runs inline
    //   final_suspend   = never   : coroutine frame auto-destroyed after body
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

    // ── spawn_one: spawn a single task at compile-time index I ──
    auto spawn_one = [&]<size_t I, typename F>(F&& func) {
        // Inner coroutine lambda — no captures, all state as function
        // parameters (stored in the coroutine frame).
        auto task = [](auto f, std::shared_ptr<SharedState> s,
                       RouteFunc rt) -> SpawnTask {
            // Execute the callable and store result at index I
            if constexpr (std::is_void_v<
                              std::invoke_result_t<std::decay_t<decltype(f)>>>) {
                f();
            } else {
                std::get<I>(s->results) = f();
            }
            // Decrement the counter; if this was the last task, post
            // the baton to resume the original awaiter.
            if (s->remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                s->baton.post(std::move(rt));
            }
            co_return;
        }(std::forward<F>(func), state, route);

        // Submit the coroutine handle to the current executor.
        // NOTE: we use enqueue_affine (P0) for OnlineWorker because the
        // engine queue is starved by the persistent timer coroutine (P1)
        // created in the OnlineWorker constructor.  The affine queue is
        // drained unconditionally in drain_p0() on every scheduler tick.
        if (auto* ow = current_online_worker()) {
            ow->enqueue_affine(task.handle);
        } else if (auto* exec = WorkStealingExecutor::current_executor()) {
            exec->add(WorkItem::make_coro(task.handle));
        }
        // Release ownership — executor owns the handle now.
        task.handle = {};
    };

    // Fold over all callables with their positional indices
    [&]<size_t... Is>(std::index_sequence<Is...>) {
        (spawn_one.template operator()<Is>(std::forward<Fs>(funcs)), ...);
    }(std::index_sequence_for<Fs...>{});

    }  // if constexpr (N > 0)

    // ── Awaiter returned for co_await ──
    //
    // Embeds AffinityBaton::Awaiter so the WaiterNode lives on the
    // awaiting coroutine's frame (stable for the entire suspend).
    struct WhenAllAwaiter {
        std::shared_ptr<SharedState> state;
        AffinityBaton::Awaiter baton_awaiter;

        WhenAllAwaiter(std::shared_ptr<SharedState> s)
            : state(std::move(s))
            , baton_awaiter{state->baton, AffinityBaton::WaiterNode{}}
        {}

        WhenAllAwaiter(const WhenAllAwaiter&) = delete;
        WhenAllAwaiter& operator=(const WhenAllAwaiter&) = delete;
        WhenAllAwaiter(WhenAllAwaiter&&) = default;
        WhenAllAwaiter& operator=(WhenAllAwaiter&&) = delete;

        bool await_ready() noexcept {
            return baton_awaiter.await_ready();
        }

        void await_suspend(std::coroutine_handle<> h) noexcept {
            baton_awaiter.await_suspend(h);
        }

        ResultTuple await_resume() noexcept {
            return std::move(state->results);
        }
    };

    return WhenAllAwaiter{std::move(state)};
}

inline void register_coro_metrics() {
    using namespace metric;
    MetricRegistry::instance().register_counter("coro/created", &g_coro_created_ctr);
    MetricRegistry::instance().register_counter("coro/destroyed", &g_coro_destroyed_ctr);
}

}  // namespace storage::runtime
