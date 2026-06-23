# 同步原语设计 — AffinityBaton / AffinityMutex / AffinitySemaphore

> 面向需要修改同步原语的后端开发者。
> 源码位置：`src/runtime/affinity_baton.h`、`src/runtime/affinity_mutex.h`、`src/runtime/affinity_semaphore.h`
> 实现补齐：`src/runtime/worker.cc`（`adapt` 命名空间段）

---

## 目录

1. [RouteFunc — 轻量级路由上下文](#1-routefunc--轻量级路由上下文)
2. [AffinityBaton — 协程信号](#2-affinitybaton--协程信号)
3. [AffinityMutex — 协程互斥锁](#3-affinitymutex--协程互斥锁)
4. [AffinitySemaphore — 计数信号量](#4-affinitysemaphore--计数信号量)
5. [Symmetric Transfer — 必修课](#5-symmetric-transfer--必修课)

---

## 1. RouteFunc — 轻量级路由上下文

### 为什么是 16B（fn\* + ctx\*）而不是 `std::function`

```cpp
// src/runtime/affinity_baton.h:25-34
struct RouteFunc {
    void (*fn)(void* ctx, size_t worker_id, std::coroutine_handle<> h) = nullptr;
    void* ctx = nullptr;

    void operator()(size_t worker_id, std::coroutine_handle<> h) const {
        if (fn) fn(ctx, worker_id, h);
    }

    explicit operator bool() const noexcept { return fn != nullptr; }
};
```

`std::function<void(size_t, std::coroutine_handle<>)>` 在 Itanium C++ ABI 上最小为 32B（含 vptr + 堆分配用于捕获超过 16B 的可调用对象），且调用路径经过虚函数分派（小对象优化失败则间接堆分配）。`RouteFunc` 使用裸函数指针 + `void*` 上下文，恰好 16 字节，嵌入对象本身零堆开销。

| 维度 | `std::function` | `RouteFunc` |
|------|-----------------|-------------|
| 大小 | 32B+（小对象优化） | 16B（结构体） |
| 堆分配 | 捕获超过 16B 时触发 | 从不 |
| 调用开销 | indirect call 通过虚函数表 | 直接函数指针调用 |
| 拷贝 | 引用计数或深拷贝 | 平凡拷贝（memcpy） |

### 嵌入 Waiter/WaiterNode 的语义

每个 `WaiterNode`（或 `AffinityMutex::Waiter`）**自己保存自己的 `RouteFunc`**。这个设计决策的关键含义：

```cpp
// affinity_baton.h:119-124
struct WaiterNode {
    std::coroutine_handle<> handle;
    size_t worker_id;     // waiter's worker_id (SIZE_MAX = external thread)
    RouteFunc route;      // how to resume this waiter on its worker
    WaiterNode* next;
};
```

`post()` 遍历链表时，**每个 waiter 用自己当初保存的 route** 恢复自己。这使 `post()` 完全不知道 route 的具体含义——它不必知道当前是 OnlineWorker 还是 WSE 上下文。各个 waiter 甚至可以在不同 executor 注册（尽管实践中不会发生）。

### `get_current_route()` — 自动感知上下文

```cpp
// src/runtime/worker.cc:77-84
RouteFunc get_current_route() {
    if (auto* ow = storage::runtime::current_online_worker()) {
        return ow->make_route_func();
    } else if (auto* exec = storage::runtime::WorkStealingExecutor::current_executor()) {
        return exec->make_route_func();
    }
    return RouteFunc{};
}
```

三层检查链：

| 上下文 | 检测方式 | 返回的 RouteFunc |
|--------|---------|-----------------|
| **OnlineWorker** | `current_online_worker()` 非空 | `OnlineWorker::make_route_func()` → 将 coroutine 入队 worker 的 affine 队列 |
| **WorkStealingExecutor** | `current_executor()` 非空 | `WSE::make_route_func()` → `add_to_worker(id, handle)` 入队目标 worker 的 local deque |
| **外部线程 / none** | 两者均为空 | `RouteFunc{}` → `operator bool() == false`，`post()` 回退到 `handle.resume()` 内联恢复 |

`OnlineWorker::make_route_func()` 的实现（`worker.h:62-69`）：

```cpp
virtual adapt::RouteFunc make_route_func() {
    return adapt::RouteFunc{
        [](void* ctx, size_t /*worker_id*/, std::coroutine_handle<> h) {
            static_cast<Worker*>(ctx)->enqueue_affine(h);
        },
        this
    };
}
```

`ctx` 指向 `Worker` 实例本身，这样 lambda 无需堆捕获，且路由逻辑完全解耦。

---

## 2. AffinityBaton — 协程信号

### 设计概览

`affinity_baton.h:55-226`，单 `std::atomic<WaiterNode*>` 编码两种状态：

```
waiters_ 指针低 bit 用法:
  bit 0 == 0 → 未 posted，指针指向 WaiterNode 链表头
  bit 0 == 1 → 已 posted（kPostedBit = 1），忽略高位的"指针"
```

这种设计合并了"状态"和"等待者链表"为单个原子字，消除了 `await_suspend` 中的"先检查状态再 CAS 入队"的竞态窗口。

### `await_suspend` — 对称转移与原子入队

```cpp
// affinity_baton.h:136-157
std::coroutine_handle<> await_suspend(std::coroutine_handle<> handle) noexcept {
    node.handle = handle;
    node.worker_id = current_worker_id();
    node.route = get_current_route();
    node.next = nullptr;

    auto* old = baton.waiters_.load(std::memory_order_acquire);

    do {
        if (reinterpret_cast<uintptr_t>(old) & kPostedBit) {
            // Already posted — don't suspend
            return handle;  // symmetric transfer: resume immediately
        }
        node.next = clear_posted(old);
    } while (!baton.waiters_.compare_exchange_weak(
        old, &node,
        std::memory_order_release,
        std::memory_order_acquire));
    return std::noop_coroutine();  // suspend
}
```

关键路径：

1. 加载 `waiters_` 快照
2. 如果 posted bit 已置位 → **不挂起**，返回 `handle` 立即恢复
3. 否则 CAS 将当前节点入栈为新的链表头
4. CAS 成功 → 返回 `std::noop_coroutine()` 表示挂起
5. CAS 失败 → 重试（`old` 由 CAS 自动更新）

**单 CAS 原子性**：检查和入队是一个原子操作，不存在"检查发现未 posted 但 post() 已发生"的竞态。

### `post()` — exchange drain + 每个 waiter 用自己的 route

```cpp
// src/runtime/worker.cc:44-49
void AffinityBaton::post() {
    auto* old = waiters_.exchange(
        reinterpret_cast<WaiterNode*>(kPostedBit),
        std::memory_order_acq_rel);
    resume_chain(clear_posted(old));
}
```

```cpp
// src/runtime/worker.cc:58-72
void AffinityBaton::resume_chain(WaiterNode* waiters) {
    while (waiters) {
        auto* next = waiters->next;
        auto handle = waiters->handle;
        auto worker_id = waiters->worker_id;

        if (waiters->route && worker_id != SIZE_MAX) {
            waiters->route(worker_id, handle);
        } else {
            handle.resume();
        }

        waiters = next;
    }
}
```

- `exchange(kPostedBit)` 一次性将整个链表摘下并置 posted bit
- 遍历时的 **每个 waiter 用自己的 `route`** 恢复自己
- 如果 route 无效或 `worker_id == SIZE_MAX`（外部线程），回退到 `handle.resume()` 直接在当前线程恢复

### `post_direct()` — 直接恢复（析构/测试用）

```cpp
// src/runtime/worker.cc:51-56
void AffinityBaton::post_direct() noexcept {
    auto* old = waiters_.exchange(
        reinterpret_cast<WaiterNode*>(kPostedBit),
        std::memory_order_acq_rel);
    resume_chain(clear_posted(old));
}
```

当前实现与 `post()` 相同。区别在语义上：`post_direct()` 承诺**不在当前执行上下文做路由**。在未来版本中，`post_direct()` 可改为直接 `handle.resume()` 而不经过 route，用于析构时快速清理。

**析构时调用**（`affinity_baton.h:59-69`）：如果 baton 在仍有 waiter 时析构，析构函数 `exchange` 摘链并逐个 `resume`，使等待协程能继续执行并最终发现 baton 已销毁。

### `TimedAwaiter` — 定时 + baton 双路

```cpp
// affinity_baton.h:164-189
struct TimedAwaiter {
    AffinityBaton& baton;
    Duration timeout;
    WaiterNode node;
    TimedWaitState state;   // embedded in coroutine frame — zero heap alloc
    WaitResult result{WaitResult::kTimeout};

    std::coroutine_handle<> await_suspend(std::coroutine_handle<> h) noexcept;
    // ...
};
```

`await_suspend` 实现（`worker.cc:131-190`）分三步：

1. **前置检查**：如果 `timeout <= 0` 或没有定时器上下文，立即返回 `handle`（超时），不做任何入队
2. **注册 baton**：与普通 `Awaiter` 相同的 CAS 入队逻辑
3. **注册定时器**：创建 `TimerNode`，`on_expire` 回调设置 `timed_out = true` 并调用 `baton.post()`

```cpp
tn.on_expire = [state_ptr, &baton = this->baton]() {
    state_ptr->timed_out.store(true, std::memory_order_release);
    baton.post();  // Uses each waiter's own saved route
};
```

**双路唤醒**：
- **信号到达**：`baton.post()` → 恢复协程 → `await_resume()` 检查 `timed_out` → 返回 `WaitResult::kSignaled`
- **定时器到期**：回调设置 `timed_out` → `baton.post()` → 恢复协程 → `await_resume()` 返回 `WaitResult::kTimeout`

注意定时器回调不直接 resume 协程——它调用 `baton.post()`，由 baton 的路由机制确保在原 worker 线程上恢复。这是设计关键：**定时器回调线程不知道目标协程的 worker 归属**，而 baton 的每个 `WaiterNode` 已经保存了 route 信息。

---

## 3. AffinityMutex — 协程互斥锁

### 设计概览

`affinity_mutex.h:36-163`。单 `std::atomic<uintptr_t> state_` 编码三种状态：

```
state_ == 0                          → 未锁定，无等待者
state_ == kLockedFlag (1)            → 已锁定，无等待者
state_ & ~kLockedFlag                → 已锁定，有等待者（低位为锁标志，高位为 Waiter* 指针）
```

这种编码将锁状态和等待者栈合并为一个原子字，`co_lock` 和 `unlock` 之间不存在竞态窗口。

### `co_lock()` — CAS 获取，失败则 CAS 入栈

```cpp
// affinity_mutex.h:68-133
struct LockAwaiter {
    AffinityMutex& mutex;
    Waiter node;

    bool await_ready() const noexcept {
        uintptr_t expected = 0;
        return mutex.state_.compare_exchange_strong(
            expected, kLockedFlag,
            std::memory_order_acquire,
            std::memory_order_relaxed);
    }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<> handle) noexcept {
        node.handle = handle;
        node.worker_id = current_worker_id();
        node.route = get_current_route();
        node.next = nullptr;

        auto* waiter_ptr = &node;
        auto old_state = mutex.state_.load(std::memory_order_acquire);

        do {
            // 双重检查：锁已释放则直接获取
            if (old_state == 0) {
                if (mutex.state_.compare_exchange_strong(
                        old_state, kLockedFlag,
                        std::memory_order_acquire,
                        std::memory_order_relaxed)) {
                    return handle;  // 获取锁成功，不挂起
                }
                continue;
            }

            // 推入等待者栈
            auto* old_waiters = reinterpret_cast<Waiter*>(
                old_state & ~kLockedFlag);
            waiter_ptr->next = old_waiters;

            uintptr_t new_state = reinterpret_cast<uintptr_t>(waiter_ptr)
                                  | kLockedFlag;

            if (mutex.state_.compare_exchange_weak(
                    old_state, new_state,
                    std::memory_order_release,
                    std::memory_order_acquire)) {
                return std::noop_coroutine();  // 挂起
            }
        } while (true);
    }
};
```

`await_ready` 尝试 **一次** CAS 0→1 获取锁：
- 成功 → 不挂起，直接持有锁
- 失败 → 进入 `await_suspend`

`await_suspend` 的 CAS 循环做两件事：
1. **双重检查**（`old_state == 0`）：锁可能在 `await_ready` 之后被释放，此时直接 CAS 获取
2. **入栈**：将当前 waiter 推入等待者栈，编码 `(waiter_ptr | kLockedFlag)`

### `unlock()` — CAS 出栈 + route 唤醒

```cpp
// src/runtime/worker.cc:93-123
void AffinityMutex::unlock() {
    auto old_state = state_.load(std::memory_order_relaxed);
    Waiter* waiters;

    do {
        waiters = extract_waiters(old_state);
        uintptr_t new_state;
        if (waiters) {
            auto* next = waiters->next;
            new_state = next
                ? (reinterpret_cast<uintptr_t>(next) | kLockedFlag)
                : 0;
        } else {
            new_state = 0;
        }
        if (state_.compare_exchange_weak(
                old_state, new_state,
                std::memory_order_release,
                std::memory_order_relaxed)) {
            break;
        }
    } while (true);

    if (waiters) {
        if (waiters->route && waiters->worker_id != SIZE_MAX) {
            waiters->route(waiters->worker_id, waiters->handle);
        } else {
            waiters->handle.resume();
        }
    }
}
```

- CAS 循环：将 state 从 `(waiter_stack | kLockedFlag)` 变为 `(waiter_stack->next | kLockedFlag)` 或 `0`（无更多等待者）
- 出栈的 waiter 使用其自己的 `route` 恢复（同 baton 的设计）
- **注意**：永远不会有 `kLockedFlag` 置位但 `waiters` 非空且 state == kLockedFlag 的情况被错误释放，因为编码保证了 `state_` 要么是 0/1，要么是 `(valid_ptr | 1)`——指针总是 2 字节对齐的

### `co_scoped_lock()` — RAII Guard

```cpp
// affinity_mutex.h:196-203
struct ScopedLockAwaiter {
    AffinityMutex& mtx;
    AffinityMutex::LockAwaiter inner;

    bool await_ready() const noexcept { return inner.await_ready(); }
    std::coroutine_handle<> await_suspend(std::coroutine_handle<> h) noexcept { return inner.await_suspend(h); }
    AffinityScopedLock await_resume() noexcept { return AffinityScopedLock(mtx); }
};
```

`co_await mutex.co_scoped_lock()` 返回 `AffinityScopedLock`（`affinity_mutex.h:169-193`），一个 movable-only 的 RAII guard，析构时调用 `unlock()`。

```cpp
struct AffinityScopedLock {
    AffinityMutex* mtx{nullptr};
    ~AffinityScopedLock() { if (mtx) mtx->unlock(); }
    void unlock() noexcept { if (mtx) { mtx->unlock(); mtx = nullptr; } }
};
```

### 为什么删除了 `lock()` / `try_lock()`

早期版本提供 `lock()` 用于 blocking 场景，在单 Worker 模型下自旋等待：

```cpp
void lock() {
    while (!try_lock()) {
        __builtin_ia32_pause();
    }
}
```

**问题**：单 Worker 模型下，如果锁被当前 Worker 的另一个协程持有，`lock()` 在 `while(pause())` 中自旋，但 Worker 线程被当前协程占用——持有锁的协程永远不会被调度到，形成**活锁/死锁**。

删除后：
- **协程场景**：必须使用 `co_lock()` / `co_scoped_lock()`，让出执行权
- **阻塞场景**：不需要——整个系统设计为非阻塞 + 协程驱动

---

## 4. AffinitySemaphore — 计数信号量

### 设计概览

`affinity_semaphore.h:28-130`。双原子字设计：

```cpp
// affinity_semaphore.h:67-68
std::atomic<AffinityBaton::WaiterNode*> waiters_{nullptr};
std::atomic<size_t> count_;
```

| 成员 | 类型 | 用途 |
|------|------|------|
| `count_` | `std::atomic<size_t>` | 可用许可数 |
| `waiters_` | `std::atomic<WaiterNode*>` | 等待者链表头（CAS 保护） |

注意 `AffinitySemaphore` 复用了 `AffinityBaton::WaiterNode`，而不是定义自己的节点类型。这确保了所有同步原语使用同一套 Waiter 结构。

### `acquire()` — 计数减一，为零时入栈

```cpp
// affinity_semaphore.h:41-51
struct AcquireAwaiter {
    AffinitySemaphore& sem;
    AffinityBaton::WaiterNode node;  // embedded — no heap alloc

    bool await_ready() const noexcept {
        return sem.try_acquire();
    }
    // ...
};
```

```cpp
// affinity_semaphore.h:84-105
inline std::coroutine_handle<> AffinitySemaphore::AcquireAwaiter::await_suspend(
    std::coroutine_handle<> h) noexcept {
    node.handle = h;
    node.worker_id = sem.current_worker_id();
    node.route = get_current_route();
    node.next = nullptr;

    auto* old = sem.waiters_.load(std::memory_order_acquire);
    do {
        // Re-check count — might have been released between await_ready and now
        if (sem.try_acquire()) {
            return h;  // symmetric transfer — don't suspend
        }
        node.next = old;
    } while (!sem.waiters_.compare_exchange_weak(
        old, &node, std::memory_order_release, std::memory_order_acquire));
    return std::noop_coroutine();
}
```

与 `AffinityBaton` 相同的模式：
1. `await_ready` 调用 `try_acquire()` CAS 减 `count_`
2. 如果 count 为 0 → 失败 → 进入 `await_suspend`
3. `await_suspend` 中再次 `try_acquire()` 检查（防止丢失 release）
4. CAS 入栈等待者链表

### `release()` — 计数加一，出栈唤醒

```cpp
// affinity_semaphore.h:107-128
inline void AffinitySemaphore::release() noexcept {
    count_.fetch_add(1, std::memory_order_release);

    // Dequeue one waiter and resume
    auto* old = waiters_.load(std::memory_order_acquire);
    while (old) {
        if (waiters_.compare_exchange_weak(old, old->next,
                std::memory_order_acquire, std::memory_order_relaxed)) {
            auto* node = old;
            if (node->route && node->worker_id != SIZE_MAX) {
                node->route(node->worker_id, node->handle);
            } else {
                node->handle.resume();
            }
            return;
        }
    }
}
```

- `fetch_add(1)` 释放一个许可
- CAS 取出链表头的一个 waiter
- 使用 waiter 自己的 route 恢复（与 baton/mutex 一致）
- 每次 `release()` 只唤醒**一个**等待者（公平信号量语义）

### WaiterNode 嵌入 AcquireAwaiter — 零堆分配

```cpp
struct AcquireAwaiter {
    AffinitySemaphore& sem;
    AffinityBaton::WaiterNode node;  // ← 直接嵌入，不 new/delete
    // ...
};
```

`WaiterNode` 是 `AcquireAwaiter` 的值成员，而 `AcquireAwaiter` 在 `co_await` 时位于协程 frame 上。这意味着：
- **零堆分配**：`acquire()` 返回的 `AcquireAwaiter` 在调用栈上构造，最终位于协程 frame 中
- **零释放**：协程完成后 frame 自动销毁，`WaiterNode` 也随之销毁
- **安全**：Baton/Mutex 中的节点同理

---

## 5. Symmetric Transfer — 必修课

### 什么是 Symmetric Transfer

`await_suspend` 的返回值决定协程的恢复方式：

```cpp
// 选项 A：在当前线程恢复 handle（会导致栈累积）
void await_suspend(std::coroutine_handle<> h) {
    h.resume();  // ← 危险！
}

// 选项 B：对称转移 — 让 C++20 协程框架负责切换
std::coroutine_handle<> await_suspend(std::coroutine_handle<> h) {
    if (can_skip_suspend) return h;         // 立即恢复当前协程
    return std::noop_coroutine();           // 挂起（实际什么都不做）
}
```

| 返回值 | 含义 |
|--------|------|
| `return handle` | 对称转移：立即恢复该 handle 的协程，当前协程挂起 |
| `return std::noop_coroutine()` | 挂起当前协程（不做额外恢复） |
| `return void` | **已废弃**：等价于 `handle.resume()` + 挂起（危险） |
| `return bool` | `true` = 挂起，`false` = 不挂起（无转移） |

### 为什么这是必修课

**问题**：在 `await_suspend` 内直接调用 `handle.resume()` 会在当前协程的栈上嵌套调用目标协程。如果 A→B→C→A 形成了 resume 链，每次 resume 都在栈上累积一帧。对于长时间运行的服务器，这可能导致：

- **栈溢出**：深度的 resume 链（如回调链、post 链、级联唤醒）
- **难以调试**：调用栈中包含无意义的协程帧
- **破坏 tail-call 优化**：本可优化的尾调用被嵌套调用取代

**修复**：使用 return handle 让 C++20 协程框架负责尾调用优化。

### 代码示例：正确 vs 错误

```cpp
// ── 错误：栈累积 ──
// 在 await_suspend 中直接 resume，当 waiter 链很长时深度嵌套
std::coroutine_handle<> await_suspend(std::coroutine_handle<> h) noexcept {
    // ...
    while (waiters) {         // ← 在 baton::post 中调用 resume_chain
        waiters->handle.resume();  // ← 在当前协程栈上恢复
        waiters = waiters->next;
    }
    return std::noop_coroutine();
}
```

```cpp
// ── 正确：对称转移 ──
// 每个 await_suspend 要么转移要么挂起，绝不嵌套 resume
std::coroutine_handle<> await_suspend(std::coroutine_handle<> h) noexcept {
    if (/* 条件满足，无需挂起 */) {
        return h;  // ← 对称转移：立即恢复 h，当前协程挂起
    }
    // CAS 入队...
    return std::noop_coroutine();  // ← 挂起
}
```

### 本项目中的对称转移模式

所有三个同步原语都严格遵循对称转移：

| 原语 | 文件 | 不挂起时返回 | 挂起时返回 |
|------|------|-------------|-----------|
| `AffinityBaton::Awaiter` | `affinity_baton.h:149` | `return handle` | `return std::noop_coroutine()` |
| `AffinityBaton::TimedAwaiter` | `worker.cc:140,150,159` | `return handle` | `return std::noop_coroutine()` |
| `AffinityMutex::LockAwaiter` | `affinity_mutex.h:102` | `return handle` | `return std::noop_coroutine()` |
| `AffinitySemaphore::AcquireAwaiter` | `affinity_semaphore.h:99` | `return handle` | `return std::noop_coroutine()` |

关键案例——`AffinityMutex` 的 `await_suspend` 双重检查：

```cpp
// affinity_mutex.h:96-103
if (old_state == 0) {  // 锁已释放
    if (mutex.state_.compare_exchange_strong(
            old_state, kLockedFlag,
            std::memory_order_acquire,
            std::memory_order_relaxed)) {
        return handle;  // ← 对称转移：不挂起，立即持锁恢复
    }
    continue;  // CAS 失败，重试
}
```

如果这里使用 `handle.resume(); return std::noop_coroutine();`，将会：
1. 在当前协程的栈帧上调用 `handle.resume()`
2. 目标协程执行完毕后返回到 `await_suspend`
3. 返回 `noop_coroutine()` 挂起当前协程（已 resume 完毕？）

使用 `return handle` 后，行为变为：
1. 返回 `handle` 给协程框架
2. 框架将当前协程挂起，并立即恢复 `handle`
3. 这是**真正的尾调用**——没有额外的栈帧

### 一个常见的错误模式

```cpp
// 错误：先把 waiter 入队再检查 post 状态
auto* old = baton.waiters_.load(std::memory_order_acquire);
node.next = old;
if (baton.waiters_.compare_exchange_weak(old, &node, ...)) {
    // 已入队，但 post() 可能在 CAS 之前执行过
    // node 已经不在链上（被 post 摘走了），但我们也挂了
    return std::noop_coroutine();  // ← 永远没人唤醒我们
}
```

正确的做法（如 `AffinityBaton::await_suspend`）：
1. 先读取 `waiters_` 快照
2. 如果 posted → 对称转移回去
3. 否则 CAS 入队，在 CAS 循环中确保一致

```cpp
do {
    if (reinterpret_cast<uintptr_t>(old) & kPostedBit) {
        return handle;  // posted 已置位，不挂起
    }
    node.next = clear_posted(old);
} while (!baton.waiters_.compare_exchange_weak(old, &node, ...));
return std::noop_coroutine();
```
