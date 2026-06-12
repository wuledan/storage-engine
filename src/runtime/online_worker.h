#pragma once
#include "coro_primitives.h"
#include "dispatch_types.h"
#include "local_work_queue.h"
#include "affine_work_queue.h"
#include "ring_work_queue.h"
#include "io/io_backend.h"
#include "io/io_engine.h"
#include "timer.h"
#include <functional>
#include <optional>
#include <type_traits>
#include <folly/coro/Task.h>

namespace storage::runtime {

// 任务分发模式
enum class TaskDispatchMode : uint8_t {
    kIndirect,  // 通过 MPMC/Affine 队列间接分发（默认）
    kDirect,    // 直接写入 Local 队列
};

class OnlineWorker : public Worker {
public:
    explicit OnlineWorker(const Worker::Config& cfg);

    // 向特定队列投递任务
    void submit_engine(WorkItem item);
    void submit_net_io(WorkItem item);
    void submit_disk_io(WorkItem item);

    // Submit a callable and get a Task that completes with the result.
    // Usage:
    //   auto result = co_await worker.co_submit<int>([] { return 42; });
    //   auto task = worker.co_submit<void>([] { do_work(); });
    //
    // The callable runs on the worker's thread.  The caller's coroutine
    // suspends until the callable completes, then resumes with the result.
    template <typename R, typename F>
    folly::coro::Task<R> co_submit(F&& func);

    // 向 affine queue 投递协程句柄
    void enqueue_affine(std::coroutine_handle<> h) override;

    // 创建 RouteFunc，捕获 this 用于 enqueue_affine 路由
    adapt::RouteFunc make_route_func();

    // 热替换 engine 队列（用于 dispatch 策略切换）
    // 排空旧队列任务，迁移到新队列，更新 dispatch_mode_
    bool swap_engine_queue(std::unique_ptr<WorkQueue> new_queue) {
        auto* old_q = get_queue(idx_engine_);
        if (!old_q) return false;

        // 排空旧队列
        WorkItem item;
        while (old_q->try_dequeue(item)) {
            if (auto* lq = dynamic_cast<LocalWorkQueue*>(new_queue.get())) {
                lq->try_enqueue(std::move(item));
            } else if (auto* rq = dynamic_cast<RingWorkQueue*>(new_queue.get())) {
                rq->enqueue(std::move(item));
            } else if (auto* aq = dynamic_cast<AffineWorkQueue*>(new_queue.get())) {
                aq->enqueue(std::move(item));
            }
        }

        // 替换
        bool ok = scheduler().replace_queue(idx_engine_, std::move(new_queue));
        if (ok) {
            // 根据新队列语义自动设置 dispatch 模式
            auto* q = get_queue(idx_engine_);
            if (q && q->semantic() == QueueSemantic::kLocal) {
                dispatch_mode_ = TaskDispatchMode::kDirect;
            } else {
                dispatch_mode_ = TaskDispatchMode::kIndirect;
            }
        }
        return ok;
    }

    TaskDispatchMode dispatch_mode() const { return dispatch_mode_; }

    // ── IO Backend ──
    void init_io_backend(const io::IOBackendConfig& cfg);
    io::IIOBackend* io_backend() { return io_backend_.get(); }

    // 协程友好的 IO API
    folly::coro::Task<io::IOCompletion> co_read(
        int fd, uint64_t offset, void* buf, size_t len);
    folly::coro::Task<io::IOCompletion> co_write(
        int fd, uint64_t offset, const void* buf, size_t len);

    // 队列索引（在构造函数中注册，公开供外部轮询器和测试使用）
    size_t idx_engine_{0};
    size_t idx_net_io_{0};
    size_t idx_disk_io_{0};
    size_t idx_affine_{0};
    size_t idx_timer_{0};

    size_t default_queue_idx() const noexcept override { return idx_engine_; }

    WorkerTimerState& timer_state() noexcept { return timer_state_; }

    std::unique_ptr<io::IIOBackend> io_backend_;

private:
    TaskDispatchMode dispatch_mode_{TaskDispatchMode::kDirect};
    WorkerTimerState timer_state_;
};

// ============================================================================
// Template implementation: co_submit
// ============================================================================
// Must be in the header because it's a template.  We use VoidTag as a
// placeholder for void returns — std::optional<void> is ill-formed.
//
// Design: wraps the user's callable in a coroutine with
//   initial_suspend = always  → doesn't start until the scheduler resumes it
//   final_suspend   = never   → coroutine frame is destroyed after body runs
//
// The user's callable, state, and route are passed as *function parameters*
// to a generic lambda (no captures).  Coroutine function parameters live in
// the coroutine frame, so they survive suspends — unlike lambda captures
// which would dangle once the closure goes out of scope.
//
template <typename R, typename F>
folly::coro::Task<R> OnlineWorker::co_submit(F&& func) {
    struct VoidTag {};
    using ResultType = std::conditional_t<std::is_void_v<R>, VoidTag, R>;

    struct SharedState {
        std::optional<ResultType> result;
        std::exception_ptr exception;
        adapt::AffinityBaton baton;
    };

    auto state = std::make_shared<SharedState>();
    auto route = make_route_func();

    // ── Engine-work coroutine type ──
    //   initial_suspend = always  : submitted via handle to engine queue
    //   final_suspend   = never   : frame auto-destroyed after co_return;
    //                               the handle is valid only while the
    //                               coroutine body runs (inside execute()),
    //                               and the queue does not need it afterwards.
    struct EngineWork {
        struct promise_type {
            EngineWork get_return_object() {
                return EngineWork{
                    std::coroutine_handle<promise_type>::from_promise(*this)};
            }
            std::suspend_always initial_suspend() noexcept { return {}; }
            std::suspend_never  final_suspend()   noexcept { return {}; }
            void return_void()   noexcept {}
            void unhandled_exception() noexcept { std::terminate(); }
        };
        std::coroutine_handle<promise_type> handle;
    };

    // Generic lambda (no captures) — the callable, state, and route are
    // stored as coroutine function parameters in the coroutine frame.
    // This avoids the dangling-capture problem.
    auto make_engine_work = [](auto callable,
                               std::shared_ptr<SharedState> st,
                               adapt::RouteFunc rt) -> EngineWork {
        try {
            if constexpr (std::is_void_v<R>) {
                callable();
                st->result.emplace(VoidTag{});
            } else {
                st->result.emplace(callable());
            }
        } catch (...) {
            st->exception = std::current_exception();
        }
        st->baton.post(rt);
        co_return;
    };

    // Build the coroutine — body does NOT run yet (initial_suspend == always).
    // Pass copies so the shared state / route survive for the co_submit body.
    EngineWork ew = make_engine_work(
        std::forward<F>(func), state, route);

    // Submit the handle to the engine queue.
    submit_engine(WorkItem::make_coro(ew.handle));

    // Release ownership — the queue now "owns" the handle; final_suspend /
    // never will auto-destroy the frame after the body runs.
    ew.handle = {};

    // Wait for completion.
    co_await state->baton;

    if (state->exception) {
        std::rethrow_exception(state->exception);
    }

    if constexpr (std::is_void_v<R>) {
        co_return;
    } else {
        co_return std::move(*state->result);
    }
}

}  // namespace storage::runtime
