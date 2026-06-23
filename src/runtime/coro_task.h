// coro_task.h — Lightweight Task<R> replacing folly::coro::Task
//
// Design:
//   initial_suspend = always  → caller owns the handle; must schedule via resume()
//   final_suspend   = always  → frame survives co_return; continuation resumed or
//                                frame self-destructs for fire-and-forget via release()
//
// Two usage patterns:
//   1. Awaitable (co_await Task<R>): the awaiting coroutine is stored as
//      continuation_ and resumed from final_suspend. Task destructor destroys
//      the frame after the awaiter reads the result.
//
//   2. Fire-and-forget (Task<void> released via release()): the handle is
//      submitted to a work queue. On completion, the frame self-destructs
//      in final_suspend (released_ == true).
//
//   3. Blocking run (blockingRun / blockingWait): the handle is released,
//      the released_ flag is cleared, the coroutine runs synchronously,
//      the result is read, then the frame is explicitly destroyed.

#pragma once

#include <atomic>
#include <coroutine>
#include <exception>
#include <optional>
#include <thread>
#include <type_traits>
#include <utility>

namespace storage::runtime::adapt {

// ============================================================
// Task<R> — minimal coroutine task (replaces folly::coro::Task)
// ============================================================
//
// Usage:
//   Task<int> foo() { co_return 42; }
//   Task<void> bar() { do_work(); co_return; }
//
// co_await support:
//   int result = co_await foo();
//
// Fire-and-forget (release handle to queue):
//   auto task = some_coro();
//   queue.enqueue(WorkItem::make_coro(task.release()));
//
// Blocking run (replacement for blockingWait):
//   int result = std::move(task).blockingRun();

template<typename R>
class Task;

namespace detail {

struct TaskPromiseBase {
    std::coroutine_handle<> continuation_{nullptr};  // awaiting coroutine
    std::exception_ptr exception_{};
    bool released_{false};  // true if release() was called → self-destruct on complete
    std::atomic<bool> completed_{false};  // set to true in final_suspend

    std::suspend_always initial_suspend() noexcept { return {}; }
    void unhandled_exception() noexcept {
        exception_ = std::current_exception();
    }
};

template<typename R>
struct TaskPromise : TaskPromiseBase {
    std::optional<R> result_;

    Task<R> get_return_object();

    void return_value(R value) noexcept {
        result_.emplace(std::move(value));
    }

    // final_suspend custom awaiter:
    //   - If continuation_ is set: resume the awaiter (frame stays alive for ~Task)
    //   - Else if released_: self-destruct the frame (fire-and-forget)
    //   - Else: do nothing (frame stays alive for blockingRun)
    auto final_suspend() noexcept {
        using PromiseT = TaskPromise<R>;
        struct FinalAwaiter {
            bool await_ready() noexcept { return false; }
            void await_suspend(std::coroutine_handle<PromiseT> h) noexcept {
                auto& promise = h.promise();
                promise.completed_.store(true, std::memory_order_release);
                if (promise.released_) {
                    h.destroy();
                } else if (promise.continuation_) {
                    auto cont = promise.continuation_;
                    promise.continuation_ = nullptr;
                    cont.resume();
                }
            }
            void await_resume() noexcept {}
        };
        return FinalAwaiter{};
    }
};

template<>
struct TaskPromise<void> : TaskPromiseBase {
    Task<void> get_return_object();

    void return_void() noexcept {}

    // Same final_suspend logic as TaskPromise<R>
    auto final_suspend() noexcept {
        using PromiseT = TaskPromise<void>;
        struct FinalAwaiter {
            bool await_ready() noexcept { return false; }
            void await_suspend(std::coroutine_handle<PromiseT> h) noexcept {
                auto& promise = h.promise();
                promise.completed_.store(true, std::memory_order_release);
                if (promise.released_) {
                    h.destroy();
                } else if (promise.continuation_) {
                    auto cont = promise.continuation_;
                    promise.continuation_ = nullptr;
                    cont.resume();
                }
            }
            void await_resume() noexcept {}
        };
        return FinalAwaiter{};
    }
};

}  // namespace detail

template<typename R>
class Task {
public:
    using promise_type = detail::TaskPromise<R>;

    Task() noexcept : handle_(nullptr) {}

    explicit Task(std::coroutine_handle<promise_type> h) noexcept : handle_(h) {}

    Task(Task&& other) noexcept : handle_(other.handle_) {
        other.handle_ = nullptr;
    }

    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            if (handle_) handle_.destroy();
            handle_ = other.handle_;
            other.handle_ = nullptr;
        }
        return *this;
    }

    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    ~Task() {
        if (handle_) handle_.destroy();
    }

    // Access the coroutine handle for manual scheduling (fire-and-forget).
    // Marks the promise as released_ so final_suspend self-destructs the frame.
    std::coroutine_handle<promise_type> release() noexcept {
        auto h = handle_;
        handle_ = nullptr;
        if (h) {
            h.promise().released_ = true;
        }
        return h;
    }

    // ── co_await support ──
    bool await_ready() const noexcept { return !handle_; }

    void await_suspend(std::coroutine_handle<> h) noexcept {
        handle_.promise().continuation_ = h;
        handle_.resume();
    }

    R await_resume() {
        if (handle_.promise().exception_) {
            std::rethrow_exception(handle_.promise().exception_);
        }
        if constexpr (!std::is_void_v<R>) {
            return std::move(*handle_.promise().result_);
        }
    }

    // ── Synchronous blocking run (replacement for blockingWait) ──
    // Runs the coroutine to completion on the current thread and returns
    // the result (or rethrows the exception). The coroutine frame is
    // destroyed after the result is extracted.
    R blockingRun() && {
        auto h = release();      // released_ = true (prevents ~Task from destroying)
        h.promise().released_ = false;  // We'll manually destroy after reading result
        h.resume();              // Start the coroutine

        // Wait for completion — the coroutine may suspend on a baton
        // (e.g., in co_submit) and be resumed by a worker thread.
        // Do NOT call h.resume() here — the coroutine will be resumed
        // by the baton poster.  Just spin-wait on the completed_ flag
        // which is set in final_suspend.
        while (!h.promise().completed_.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }

        // Coroutine has reached final_suspend — result is ready
        auto& promise = h.promise();
        if (promise.exception_) {
            auto e = std::move(promise.exception_);
            h.destroy();
            std::rethrow_exception(e);
        }
        if constexpr (!std::is_void_v<R>) {
            R result = std::move(*promise.result_);
            h.destroy();
            return result;
        } else {
            h.destroy();
        }
    }

private:
    std::coroutine_handle<promise_type> handle_;
};

// ── blockingWait free function (replaces folly::coro::blockingWait) ──
template<typename R>
inline R blockingWait(Task<R> task) {
    return std::move(task).blockingRun();
}

// ── promise_type::get_return_object implementations ──

namespace detail {

template<typename R>
Task<R> TaskPromise<R>::get_return_object() {
    return Task<R>{std::coroutine_handle<TaskPromise<R>>::from_promise(*this)};
}

inline Task<void> TaskPromise<void>::get_return_object() {
    return Task<void>{std::coroutine_handle<TaskPromise<void>>::from_promise(*this)};
}

}  // namespace detail

}  // namespace storage::runtime::adapt
