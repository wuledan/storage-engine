# Task\<R\> — Lightweight Coroutine Task

## 1. 为什么自建

在项目早期，协程任务基于 `folly::coro::Task`。随着单 Worker 架构的定型，`folly::coro::Task` 的 `ViaIfAsync` 路由机制成为多余的负担：

| folly::coro::Task | 本项目的 Task\<R\> |
|-------------------|-------------------|
| 通过 `ViaIfAsync` 支持跨 Executor 调度 | 单 Worker 内所有协程在同一线程运行，无需 Executor 路由 |
| 依赖 folly 庞大的基础设施 (SemiFuture, Executor, ManualExecutor) | 零外部依赖，仅 `<coroutine>` 标准库 |
| final_suspend 行为通过模版参数可配置 | `final_suspend = always` 固定，逻辑内聚在 FinalAwaiter |
| blockingWait 依赖 `ManualExecutor` + 事件循环 | `blockingRun()` 自旋 + yield，不依赖任何 Executor |
| 协程帧生命周期由 shared_ptr<Promise> 管理 | 裸 `coroutine_handle` + `released_` 标志位，零堆分配 |

**核心矛盾**：`ViaIfAsync` 设计的目的是让协程在 `co_await` 时透明地切换到另一个 Executor；而在单 Worker 中，所有协程始终在同一个线程上运行，这个机制纯属开销。自建 `Task<R>` 去掉了 Executor 抽象层，将调度权完全交给上层的 `Scheduler`。

---

## 2. 设计

### 2.1 Promise 基类 — `TaskPromiseBase`

```cpp
// src/runtime/coro_task.h 第 56-66 行
struct TaskPromiseBase {
    std::coroutine_handle<> continuation_{nullptr};  // 等待链：co_await 的调用方
    std::exception_ptr exception_{};
    bool released_{false};          // release() 后 final_suspend 自销毁
    std::atomic<bool> completed_{false};  // blockingRun 等待标志

    std::suspend_always initial_suspend() noexcept { return {}; }
    void unhandled_exception() noexcept {
        exception_ = std::current_exception();
    }
};
```

### 2.2 `initial_suspend = always` — 创建不启动

`initial_suspend` 返回 `std::suspend_always`，意味着协程在 `co_return` / `co_await` 之前**不会自动开始执行**。调用者获得一个挂起的协程句柄，必须显式 resume（通过 `co_await`、`release()` + 入队调度、或 `blockingRun()`）才能启动执行。

```cpp
// src/runtime/coro_task.h 第 62 行
std::suspend_always initial_suspend() noexcept { return {}; }
```

这个设计给调度器完全的控制权：协程在哪个队列、何时执行，都由 `Scheduler` 决定。

### 2.3 `final_suspend` — 自定义 FinalAwaiter

`final_suspend` 返回一个自定义的 `FinalAwaiter`，它在 `await_suspend` 中做出三路决策：

```cpp
// src/runtime/coro_task.h 第 82-102 行
auto final_suspend() noexcept {
    using PromiseT = TaskPromise<R>;
    struct FinalAwaiter {
        bool await_ready() noexcept { return false; }
        std::coroutine_handle<> await_suspend(std::coroutine_handle<PromiseT> h) noexcept {
            auto& promise = h.promise();
            promise.completed_.store(true, std::memory_order_release);
            if (promise.released_) {
                h.destroy();                           // ① fire-and-forget: 自销毁
                return std::noop_coroutine();
            } else if (promise.continuation_) {
                auto cont = promise.continuation_;
                promise.continuation_ = nullptr;
                return cont;                           // ② co_await: symmetric transfer 回 caller
            }
            return std::noop_coroutine();              // ③ blockingRun: 什么都不做, 等待阻塞线程销毁
        }
        void await_resume() noexcept {}
    };
    return FinalAwaiter{};
}
```

- **① fire-and-forget**（`released_ == true`）：协程帧立即销毁，不恢复任何人
- **② co_await**（`continuation_` 非空）：通过 `return cont` 实现 symmetric transfer，恢复调用方协程
- **③ blockingRun**（两者均不满足）：仅设置 `completed_` 标志，等待阻塞线程通过 `h.destroy()` 销毁

### 2.4 `continuation_` — 等待链

当 `co_await task` 被使用时，`Task::await_suspend` 将当前正在挂起的协程句柄存入 `continuation_`，然后将子协程的句柄返回给运行时：

```cpp
// src/runtime/coro_task.h 第 178-183 行
std::coroutine_handle<> await_suspend(std::coroutine_handle<> h) noexcept {
    handle_.promise().continuation_ = h;
    return handle_;
}
```

这样运行时先挂起调用方，再 resume 子协程。子协程完成后，`final_suspend` 通过 symmetric transfer 无缝跳回调用方。

### 2.5 `released_` — Fire-and-forget 标志

`release()` 方法设置 `released_ = true` 并清空 Task 内部的 `handle_`（防止 `~Task` 重复销毁），返回裸句柄供调度器入队：

```cpp
// src/runtime/coro_task.h 第 168-175 行
std::coroutine_handle<promise_type> release() noexcept {
    auto h = handle_;
    handle_ = nullptr;
    if (h) {
        h.promise().released_ = true;
    }
    return h;
}
```

典型使用场景：

```cpp
// 创建持久协程（如 timer_coro_fn），入队到定时器队列
auto coro = timer_coro_fn(this);
timer_handle_ = coro.release();
queue.enqueue(WorkItem::make_coro(timer_handle_));
```

### 2.6 `completed_` — blockingRun 等待标志

当协程到达 `final_suspend` 时，`completed_` 被设置为 `true`。`blockingRun()` 在一个 spin + yield 循环中等待此标志变为 `true`，然后读取结果并销毁帧。

```cpp
// src/runtime/coro_task.h 第 198-226 行
R blockingRun() && {
    auto h = release();
    h.promise().released_ = false;
    h.resume();
    while (!h.promise().completed_.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    // 读取结果...
    h.destroy();
}
```

---

## 3. 三种使用模式

### 3.1 `co_await` — 协程组合

```cpp
Task<int> compute() { co_return 42; }
Task<void> runner() {
    int result = co_await compute();  // 挂起 runner, 启动 compute
    assert(result == 42);             // compute 完成, runner 继续
}
```

**生命周期**：
1. `runner()` 创建 `compute()` 的 Task
2. `co_await compute()` 调用 `Task::await_suspend`：`continuation_ = runner 的句柄`，返回 `compute` 的句柄
3. 运行时恢复 `compute`，`runner` 挂起
4. `compute` 执行 `co_return 42`，进入 `final_suspend`
5. FinalAwaiter：`continuation_` 非空 → symmetric transfer 回 `runner`
6. `runner` 从 `await_resume()` 获得结果
7. `compute` 的 Task 析构，`~Task` 销毁 `compute` 的协程帧

### 3.2 `release()` — Fire-and-forget

```cpp
Task<void> background_work() {
    do_work();
    co_return;
}

auto task = background_work();
queue.enqueue(WorkItem::make_coro(task.release()));
// task.handle_ 已为 null, ~Task 不做任何事
```

**生命周期**：
1. `release()` 设置 `released_ = true`，清空 `handle_`
2. 调度器从队列取出 WorkItem，调用 `coro.resume()` 启动协程
3. 协程执行，完成时进入 `final_suspend`
4. FinalAwaiter：`released_ == true` → `h.destroy()` 自销毁
5. Task 析构时 `handle_` 为 null，无操作

### 3.3 `blockingRun()` — 同步等待

```cpp
int result = std::move(task).blockingRun();
```

**生命周期**：
1. `release()` 设置 `released_ = true`，清空 `handle_`
2. 立即设置 `released_ = false`（防止 final_suspend 自销毁）
3. `h.resume()` 启动协程
4. spin + yield 等待 `completed_` 标志
5. final_suspend 设置 `completed_ = true`，返回 `noop_coroutine()`
6. 阻塞线程退出 spin 循环，读取结果
7. `h.destroy()` 手动销毁帧

---

## 4. Symmetric Transfer

### 问题：栈累积

在修复之前（commit `baf8b79`），`Task::await_suspend` 和 `FinalAwaiter::await_suspend` 都返回 `void`，意味着它们内部调用 `handle.resume()` / `cont.resume()` 是**内联递归**：

```cpp
// 修复前：递归 resume → 栈累积
// Task::await_suspend
void await_suspend(std::coroutine_handle<> h) noexcept {
    handle_.promise().continuation_ = h;
    handle_.resume();  // ← 在当前栈帧上恢复子协程
}

// FinalAwaiter::await_suspend
void await_suspend(std::coroutine_handle<PromiseT> h) noexcept {
    if (promise.continuation_) {
        auto cont = promise.continuation_;
        promise.continuation_ = nullptr;
        cont.resume();  // ← 子协程完成后递归恢复调用方
    }
}
```

当深度嵌套 `co_await` 时（例如 `A co_await B co_await C co_await D`），调用栈会这样累积：

```
co_await D  →  resume D  →  D completes  →  resume C  →  C completes  →  resume B  →  ...
```

每层嵌套都在栈上新增一帧，深度为 N 时栈深度为 O(N)。对于长期运行的递归协程或深度组合，这会导致**栈溢出**。

### 修复：Symmetric Transfer

C++20 标准为 `await_suspend` 提供了另一种签名：

```cpp
// 返回值类型为 coroutine_handle<>
std::coroutine_handle<> await_suspend(std::coroutine_handle<> h) noexcept;
```

当返回一个非 `noop_coroutine()` 的句柄时，运行时**立即恢复该句柄**，但**不增加调用栈深度**——它通过 `tail-call` 语义实现：

```
co_await D  →  return D's handle  →  runtime resumes D  →  D completes
  → final_suspend returns C's handle  →  runtime resumes C (same frame, new coro)
  → C completes  →  final_suspend returns B's handle  →  ...
```

修复后（commit `2c1ec49`）：

```cpp
// Task::await_suspend
std::coroutine_handle<> await_suspend(std::coroutine_handle<> h) noexcept {
    handle_.promise().continuation_ = h;
    return handle_;  // ← symmetric transfer: 运行时恢复子协程
}

// FinalAwaiter::await_suspend
std::coroutine_handle<> await_suspend(std::coroutine_handle<PromiseT> h) noexcept {
    if (promise.continuation_) {
        auto cont = promise.continuation_;
        promise.continuation_ = nullptr;
        return cont;  // ← symmetric transfer: 运行时恢复调用方
    }
    return std::noop_coroutine();
}
```

**效果对比**（N 层 `co_await` 深度）：

| 方式 | 栈深度 | 1000 层嵌套 | 100000 层嵌套 |
|------|--------|------------|--------------|
| 内联 resume | O(N) | ~1MB 栈 | 栈溢出 ❌ |
| Symmetric transfer | O(1) | 稳定 | 稳定 ✅ |

**为什么必须做**：本项目使用协程持久化 IO 路径（如 `timer_coro_fn` 永不退出的循环），以及深度业务链路（请求 → 认证 → 配额 → IO → 响应），N 很容易达到数百到数千。没有 symmetric transfer，系统在高负载下会频繁栈溢出。

---

## 5. 与 folly::coro::Task 对比

| 特性 | folly::coro::Task\<R\> | 本项目的 Task\<R\> |
|------|----------------------|-------------------|
| **依赖** | folly 全量（Executor, Future, 等） | 仅 `<coroutine>` 标准库 |
| **initial_suspend** | 可配置（always / never） | always 固定 |
| **final_suspend** | 模版参数可配置 | 自定义 FinalAwaiter，三路分支 |
| **Executor 路由** | ViaIfAsync，运行时多态 | 无。调度由 Scheduler 负责 |
| **co_await** | 返回 void，内联 resume | 返回 coroutine_handle<>，symmetric transfer |
| **blockingWait** | ManualExecutor + 事件循环 | blockingRun() spin + yield |
| **协程帧管理** | shared_ptr<Promise> | 裸句柄 + released_ / completed_ 标志 |
| **Fire-and-forget** | via SemiFuture.detach() | release() + 入队 |
| **堆分配** | 每次 co_await 可能堆分配 | 零堆分配，IOState 嵌入协程帧 |
| **跨 Worker 路由** | ViaIfAsync 原生支持 | 依赖 AffinityBaton::post(route_func) |
| **代码量** | ~2000 行（含基础设施） | ~250 行（一个头文件） |

---

## 6. `blockingRun` 双重释放 Bug 修复

### 背景

`blockingRun()` 用于从非协程上下文同步等待一个协程完成。它替代了 `folly::coro::blockingWait`。

### Bug 场景

在初始实现中（commit `baf8b79`），`blockingRun()` 的流程是：

```
release()  →  released_ = true, handle_ = null
released_ = false        ← 通知 final_suspend 不要自销毁
h.resume()               ← 启动协程
spin-wait completed_
h.destroy()              ← 手动销毁
```

问题出现在**同步完成路径**：如果协程在 `h.resume()` 后**立即同步完成**（内部无反挂起点，如纯计算协程），则：

1. `h.resume()` 启动协程
2. 协程执行完毕，进入 `final_suspend`
3. FinalAwaiter 看到 `released_ = false` + `continuation_ = null` → 仅设置 `completed_ = true`，返回 `noop_coroutine()`
4. 协程帧存活
5. `blockingRun` 进入 spin 循环，检查到 `completed_` 为 true 后退出
6. 读取结果，`h.destroy()` 销毁帧 → ✅ 正确

**但是**：在初始版本中 `FinalAwaiter::await_suspend` 返回 `void`，而且内部调用 `cont.resume()` 是内联递归。当 `blockingRun` 的 `h.resume()` 内部发生了内联 resume 时，控制流变复杂。如果 `blockingRun` 被用于一个已经 `co_await` 过的 Task：

1. 用户 A `co_await task` → `continuation_` 设为 A 的句柄
2. 用户 B 调用 `std::move(task).blockingRun()` 
3. `release()` 设置 `released_ = true`，清空 `handle_`
4. `released_ = false`
5. `h.resume()` 启动
6. final_suspend 看到 `continuation_` 非空，resume 了 A → A 读取了结果并销毁了自己的副本
7. blockingRun 完成后再次 `h.destroy()` → **双重释放！**

### 修复

修复（commit `2c1ec49`）的核心是：

```
- void await_suspend(...)  →  std::coroutine_handle<> await_suspend(...)
```

这不仅解决了栈累积问题（见第 4 节），也间接修复了 `blockingRun` 的双重释放：

1. `co_await` 后 `Task` 的 `handle_` 在 `~Task` 中销毁
2. `blockingRun()` 内部 `release()` 清空了 `handle_`，因此 `~Task` 不再销毁它
3. Symmetric transfer 确保了协程间切换是 tail-call，不会引入意外的内联递归
4. `blockingRun()` 的 spin 循环在稳定的内存序下等待 `completed_`，消除了竞态

**结论**：`blockingRun()` 的正确性依赖于 `release()` / `released_` / `completed_` 三者的精确配合，而 symmetric transfer 的引入是保证整个过程不出现栈溢出和双重释放的前置条件。
