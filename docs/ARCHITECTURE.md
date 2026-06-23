# Storage Engine Runtime 架构概览

> 面向新加入团队的开发者。本文解释 Runtime 框架的整体设计理念、线程模型、协程模型、同步原语原则、内存管理、路由机制以及外部依赖策略。

---

## 目录

1. [设计哲学](#1-设计哲学)
2. [线程模型](#2-线程模型)
3. [协程模型](#3-协程模型)
4. [同步原语原则](#4-同步原语原则)
5. [内存原则](#5-内存原则)
6. [路由自动感知](#6-路由自动感知)
7. [依赖策略](#7-依赖策略)

---

## 1. 设计哲学

### Worker 线程独占，禁止阻塞

每个 Worker 线程独占一个物理核（通过 hwloc CPU pinning），所有资源（内存池、IO 队列、定时器）均为该线程私有。**任何形式的阻塞都不允许**——既不能有 `std::mutex::lock()`，也不能有 spin-lock 或 `std::this_thread::sleep_for`。

```
禁止清单（在 Worker 线程上）:
  ❌ std::mutex::lock() / std::lock_guard<std::mutex>
  ❌ std::atomic_flag test_and_spin
  ❌ std::this_thread::sleep_for / sleep_until
  ❌ pthread_mutex_lock / pthread_cond_wait

允许清单:
  ✅ co_await mutex.co_lock()          // 协程挂起而非线程阻塞
  ✅ co_await baton                     // 协程挂起
  ✅ co_await sem.acquire()             // 协程挂起
  ✅ co_await yield()                   // 协程让出，重新入队
  ✅ co_await co_sleep_for(dur)         // 协程睡眠（通过定时器唤醒）
```

**原理**：一个 Worker 线程是单线程的事件循环（`Scheduler::run_busy()` 或 `Scheduler::run()`），它轮询多个优先级队列并执行任务。如果某任务阻塞了线程，整个 Worker 上的所有协程都无法继续——包括 IO poll 协程、定时器协程、以及其他业务协程。

```cpp
// src/runtime/worker.cc:268
void Worker::worker_loop() {
    tls_worker_id = id_;
    tls_current_worker = this;
    adapt::detail::get_current_worker_id = []() -> size_t { return tls_worker_id; };
    on_worker_start();
    scheduler_.run();     // <--- 主循环，返回时 Worker 退出
    tls_current_worker = nullptr;
    tls_worker_id = SIZE_MAX;
}
```

### 协程驱动所有异步操作

所有 IO、定时器、跨线程通信均通过协程挂起/恢复完成。不存在回调注册 + 回调执行的传统模式——IO 完成回调由 `co_read`/`co_write` 的 `co_await` 隐式处理。

```cpp
// src/runtime/online_worker.h:91
adapt::Task<io::IOCompletion> co_read(int fd, uint64_t offset, void* buf, size_t len);
adapt::Task<io::IOCompletion> co_write(int fd, uint64_t offset, const void* buf, size_t len);
```

使用方式:
```cpp
auto comp = co_await worker.co_read(fd, offset, buf, len);
// 恢复后直接处理结果
```

---

## 2. 线程模型

Runtime 提供两种执行上下文，分别对应不同负载类型。

### 2.1 OnlineWorker — 单线程 busy_poll

`OnlineWorker` 是一个**专用线程**，运行 `Scheduler::run_busy()` 主循环，以固定优先级顺序轮询所有队列。

```
OnlineWorker::worker_loop()
  └─ Scheduler::run_busy()    // never sleeps, always polls
       ├─ P0: affine queue    (drain_p0)  — 跨线程协程恢复、baton.post 唤醒
       ├─ P1: disk IO queue                — IO poll coroutine, 定时器 coroutine
       ├─ P2: engine queue                 — 业务逻辑（读写请求、引擎操作）
       ├─ P3: net IO queue                 — 网络收发完成
       └─ P4: timer queue                  — 定时器到期唤醒
```

实现在 `src/runtime/scheduler.cc:166`:

```cpp
void Scheduler::run_busy() {
    while (running_.load(std::memory_order_acquire)) {
        // P0: drain affine queue
        drain_p0(p0_batch, kMaxBatchSize);
        // P1: disk IO coroutine
        drain disk_io_idx_ queue;
        // P2/P3/P4: engine, net_io, timer
        for (size_t qi = 0; qi < queues_.size(); ++qi) {
            if (qi == affine_idx_ || qi == disk_io_idx_) continue;
            drain queue[qi];
        }
        // P0 again (新产生的 P0 任务)
        drain_p0(p0_batch, kMaxBatchSize);
    }
}
```

队列在 `OnlineWorker` 构造时注册（`src/runtime/online_worker.cc:84`）:

```cpp
OnlineWorker::OnlineWorker(const Worker::Config& cfg) : Worker(cfg) {
    idx_affine_  = add_queue(std::make_unique<RingWorkQueue>(..., Priority::kCritical, "affine", 65536));
    idx_net_io_  = add_queue(std::make_unique<BatchedSPSCWorkQueue>(..., Priority::kHigh, "net_io"));
    idx_disk_io_ = add_queue(std::make_unique<BatchedSPSCWorkQueue>(..., Priority::kHigh, "disk_io"));
    idx_engine_  = add_queue(std::make_unique<RingWorkQueue>(..., Priority::kMedium, "engine", 65536));
    idx_timer_   = add_queue(std::make_unique<AffineWorkQueue>(..., Priority::kHigh, "timer"));
    set_policy(make_policy(PolicyConfig{"strict_priority"}));
}
```

### 2.2 WorkStealingExecutor — 多 Worker、NUMA-aware、work-stealing

`WorkStealingExecutor` 管理一组 Worker 线程，每个 Worker 维护自己的 `RingWorkQueue`（local_deque）。空闲时，Worker 会从同 NUMA 节点的 peer 窃取任务。

```
WorkStealingExecutor::worker_loop(ws)
  ┌─ 1. Drain yield_queue (本 Worker yield 回来的协程)
  ├─ 2. Pop from own local_deque (MPMC ring, 使用 try_dequeue_mc CAS)
  ├─ 3. Pull from global_entry_ (外部提交的全局 MPMC ring)
  ├─ 4. Steal from NUMA peers (同 NUMA 节点的 local_deque)
  └─ 5. Park (adaptive idle: spin → yield → cv wait)
```

实现在 `src/runtime/work_stealing_executor.cc:261`:

```cpp
void WorkStealingExecutor::worker_loop(WorkerState& ws) {
    while (ws.running.load(std::memory_order_acquire)) {
        // 1. yield queue
        if (ws.yield_queue->try_dequeue(item)) { ... continue; }
        // 2. local deque (MC — 多消费者模式，允许被窃取)
        if (ws.local_deque->try_dequeue_mc(item)) { ... continue; }
        // 2.5 global entry
        if (global_entry_->try_dequeue_mc(item)) { ... continue; }
        // 3. steal from NUMA peers
        for (size_t i = 0; i < attempts; ++i) {
            victim.local_deque->try_dequeue_mc(item) → stolen
        }
        // 4. park
        ws.park.enter_idle();
    }
}
```

**NUMA 感知**：通过 hwloc 自动发现拓扑，只在同 NUMA 节点内窃取（`src/runtime/work_stealing_executor.cc:21`）。Worker 数量自动设置为每个物理核一个 Worker。

### 2.3 不创建用户线程

所有执行上下文由 Runtime 管理。用户代码不应直接创建 `std::thread`。通过 `coro_create`（pthread 风格 API）或 `co_submit` 将工作提交到 Runtime 队列。

```cpp
// src/runtime/coro_api.h — pthread-aligned coroutine API
// coro_create / coro_join / coro_detach / coro_yield
inline int coro_create(coro_t *coro, const coro_attr_t *attr,
                        void *(*start_routine)(void*), void *arg);
```

---

## 3. 协程模型

### Task\<R\> — 自建协程任务类型

使用 C++20 标准协程，自建 `Task<R>`（定义在 `src/runtime/coro_task.h`），替代 `folly::coro::Task`。

关键设计:
- **`initial_suspend` = `always`**: 创建后不立即执行，需通过 `release()` 提交到队列或 `co_await` 启动
- **`final_suspend` = `always`**: 保证协程帧在 `co_return` 后存活，直到结果被读取
- **Symmetric transfer**: 所有 `await_suspend` 返回 `std::coroutine_handle<>`，编译器保证 tail-call，消除栈累积

```cpp
// src/runtime/coro_task.h:51 — Task<R> 核心设计
template<typename R>
class Task {
    struct promise_type : TaskPromiseBase {
        std::optional<R> result_;
        auto final_suspend() noexcept {
            struct FinalAwaiter {
                std::coroutine_handle<> await_suspend(std::coroutine_handle<PromiseT> h) noexcept {
                    // symmetric transfer: 返回 continuation_，编译器 tail-call
                    if (promise.continuation_) return promise.continuation_;
                    if (promise.released_) { h.destroy(); return std::noop_coroutine(); }
                    return std::noop_coroutine();
                }
            };
            return FinalAwaiter{};
        }
    };
};
```

三种使用模式:

#### 模式 1: co_await（链式调用）
```cpp
Task<int> compute() { co_return 42; }
Task<void> use() {
    int v = co_await compute();   // 挂起当前协程，compute 执行后恢复
}
```

#### 模式 2: Fire-and-forget（通过 release() 提交到队列）
```cpp
auto task = some_coro();
queue.enqueue(WorkItem::make_coro(task.release()));
// task 析构不销毁帧 — final_suspend 中自销毁
```

#### 模式 3: blockingRun（同步等待，用于测试）
```cpp
int result = std::move(task).blockingRun();
// 内部 spin-wait 直到 completed_ flag 置位
```

> **关于 Symmetric transfer**:
>
> C++20 协程中，`await_suspend` 返回 `void` 时，编译器在当前栈帧上手递手恢复协程，可能产生深层嵌套导致栈溢出。返回 `std::coroutine_handle<>` 时，编译器生成 tail-call（`co_await` 的最终 `suspend` 也是如此），消除了栈累积。
>
> 本项目中**所有** `await_suspend` 均返回 `std::coroutine_handle<>`，无一例外。
>
> 示例（`src/runtime/affinity_baton.h:136`）:
> ```cpp
> std::coroutine_handle<> await_suspend(std::coroutine_handle<> handle) noexcept {
>     // ...
>     if (already_posted) return handle;     // symmetric transfer: 立即恢复
>     /* enqueue */;
>     return std::noop_coroutine();           // 挂起（不恢复任何协程）
> }
> ```

---

## 4. 同步原语原则

**只允许 `co_await` 系列同步原语**，禁止阻塞式 API。

| 原语 | 所在文件 | 用途 | 禁止替代项 |
|------|---------|------|-----------|
| `AffinityBaton` | `src/runtime/affinity_baton.h` | 协程信号量（一次唤醒多个等待者） | `std::promise` + `std::future` |
| `AffinityMutex` | `src/runtime/affinity_mutex.h` | 协程互斥锁 | `std::mutex::lock()` |
| `AffinitySemaphore` | `src/runtime/affinity_semaphore.h` | 协程计数信号量 | `sem_wait()` |
| `co_await yield()` | `src/runtime/yield_awaiter.h` | 协程让出当前队列 | `std::this_thread::yield()` |
| `co_await co_sleep_for()` | `src/runtime/timer.h` | 协程睡眠 | `std::this_thread::sleep_for()` |

**核心设计模式**: 所有 `await_suspend` 中保存 `RouteFunc` + `worker_id`，在 `post()`/`unlock()`/`release()` 时将协程句柄路由回原始 Worker 的 affine 队列（P0）。

```cpp
// src/runtime/affinity_baton.h:136 — await_suspend 保存路由信息
std::coroutine_handle<> await_suspend(std::coroutine_handle<> handle) noexcept {
    node.handle = handle;
    node.worker_id = current_worker_id();     // 记录当前 Worker
    node.route = get_current_route();          // 记录路由函数
    node.next = nullptr;
    // CAS 入队...
    return std::noop_coroutine();
}

// src/runtime/worker.cc:44 — post() 时路由恢复
void AffinityBaton::post() {
    auto* old = waiters_.exchange(reinterpret_cast<WaiterNode*>(kPostedBit), ...);
    resume_chain(clear_posted(old));
}

void AffinityBaton::resume_chain(WaiterNode* waiters) {
    while (waiters) {
        if (waiters->route && worker_id != SIZE_MAX) {
            waiters->route(worker_id, handle);   // 路由回原 Worker
        } else {
            handle.resume();                     // 无路由时直接恢复
        }
        waiters = next;
    }
}
```

### 为什么不能有 `std::mutex::lock()`

`std::mutex::lock()` 在当前 OS 线程上阻塞。如果 Worker 线程调用了它：
1. Worker 线程被 OS 挂起
2. Scheduler 无法轮询任何队列
3. 所有 IO 操作停止（IO poll coroutine 也停了）
4. 定时器无法触发
5. 解锁线程可能无法运行（如果它需要这个 Worker 的资源）

**结论**：一个 `std::mutex::lock()` 可能导致整个 Worker 死锁。所有同步必须通过协程挂起实现，这样 Scheduler 可以继续轮询其他队列。

---

## 5. 内存原则

### Per-Worker MemoryPool，无原子争用

每个 Worker 拥有独立的 `MemoryPool`（`src/runtime/memory_pool.h`），单线程访问，**无任何锁或原子操作**在热路径上。

```cpp
// src/runtime/worker.h:94
adapt::MemoryPool& memory_resource() { return *mem_pool_; }
```

### 三级分层结构

```
Allocation 请求
  │
  ├─ 1. TLS cache (ThreadLocalCache)
  │     非原子 size_t count_，内嵌 LocalFreeList 数组
  │     无 std::atomic，无锁总线开销
  │
  ├─ 2. Central per-size-class free list (SizeClassFreeList)
  │     CAS push/pop，无 std::mutex
  │     MPMCRing return_ring 做 batch-transfer
  │
  └─ 3. Bump allocation from pre-allocated 1MB blocks
        从 OS 预分配的大块内存中 bump 分配
```

```cpp
// src/runtime/memory_pool.cc:232 — ThreadLocalCache（无原子）
class ThreadLocalCache {
    struct LocalFreeList {
        SizeClassFreeList::Node* head{nullptr};
        size_t count{0};    // 普通 size_t，TLS 无竞争
    };
    LocalFreeList free_lists_[kNumSizeClasses];  // 内联数组
};
```

与 `std::pmr::memory_resource` 不同，本项目 `MemoryPool` 不继承 PMR 接口，直接暴露 `allocate`/`deallocate`。

**Size classes**: 8 / 16 / 32 / 64 / 128 / 256 字节（`src/runtime/memory_pool.cc:16`）。超大对象（>4KB）直接走 `::operator new`。

**TLS cache 原子消除效果**：将 `size_t count_` 从 `std::atomic` 改为普通变量后，round-trip 从 80ns 降到 65ns（`// TODO: verify with current benchmark`）。

### 无锁化中心 freelist

SizeClassFreeList 使用 CAS push/pop，CentralFreeList 使用 `exchange` drain（`src/runtime/memory_pool.cc:127`、`:167`）。per-size-class 的 MPMCRing 用于 TLS 到中央的 batch-transfer，减少 CAS 争用。

---

## 6. 路由自动感知

### RouteFunc — 16 字节零分配可调用对象

`RouteFunc` 是一个 16 字节的结构体，包含函数指针和 context 指针，替代 `std::function`。

```cpp
// src/runtime/affinity_baton.h:25
struct RouteFunc {
    void (*fn)(void* ctx, size_t worker_id, std::coroutine_handle<> h) = nullptr;
    void* ctx = nullptr;

    void operator()(size_t worker_id, std::coroutine_handle<> h) const {
        if (fn) fn(ctx, worker_id, h);
    }
};
```

与 `std::function` 对比:
| 特性 | `std::function` | `RouteFunc` |
|------|----------------|-------------|
| 大小 | ≥ 32 字节（含 SBO） | 16 字节 |
| 堆分配 | 当 callable > SBO 时 | 从不 |
| 热路径开销 | 虚函数调用 | 直接函数指针调用 |

### get_current_route() — 自动感知上下文

`get_current_route()` 通过 TLS 检测当前执行上下文，自动返回正确的路由函数：

```cpp
// src/runtime/worker.cc:77
RouteFunc get_current_route() {
    if (auto* ow = storage::runtime::current_online_worker()) {
        return ow->make_route_func();           // OnlineWorker: enqueue_affine
    } else if (auto* exec = WorkStealingExecutor::current_executor()) {
        return exec->make_route_func();         // WSE: add_to_worker
    }
    return RouteFunc{};                         // 非 Worker 上下文：空路由
}
```

OnlineWorker 的路由直接将协程句柄投递到 affine 队列（P0）：

```cpp
// src/runtime/online_worker.cc:172
adapt::RouteFunc OnlineWorker::make_route_func() {
    return adapt::RouteFunc{
        [](void* ctx, size_t /*worker_id*/, std::coroutine_handle<> h) {
            static_cast<OnlineWorker*>(ctx)->enqueue_affine(h);
        },
        this
    };
}
```

WSE 的路由将协程句柄投递到指定 Worker 的 local_deque：

```cpp
// src/runtime/work_stealing_executor.cc:252
adapt::RouteFunc WorkStealingExecutor::make_route_func() {
    return adapt::RouteFunc{
        [](void* ctx, size_t worker_id, std::coroutine_handle<> h) {
            static_cast<WorkStealingExecutor*>(ctx)->add_to_worker(worker_id, h);
        },
        this
    };
}
```

**自动感知原理**：所有的 `await_suspend`（`AffinityBaton`、`AffinityMutex`、`AffinitySemaphore`）都在挂起时调用 `get_current_route()` 将路由信息保存在 WaiterNode 中。当 `post()`/`unlock()`/`release()` 发生时，使用保存的路由将协程句柄投递回原始 Worker。

### TLS 变量体系

| 变量 | 设置时机 | 用途 |
|------|---------|------|
| `tls_worker_id` | Worker 线程启动 | 标识当前 Worker |
| `tls_current_worker` | `worker_loop()` 入口 | `current_worker()` / `current_online_worker()` |
| `tls_source_queue_idx` | Scheduler 每次 `execute()` 前 | `yield()` 路由回源队列 |
| `tls_source_queue` | Scheduler 每次 `execute()` 前 | 同上，直接指针 |
| `tls_worker_id_` (WSE) | WSE `worker_loop()` 入口 | `WorkStealingExecutor::current_worker_id()` |
| `tl_executor_` (WSE) | WSE `worker_loop()` 入口 | `WorkStealingExecutor::current_executor()` |

---

## 7. 依赖策略

### 零 folly 依赖

项目初始设计参考了 `folly::coro`，但最终全部自建实现，无任何 folly 依赖。

| 原 folly 组件 | 替代实现 | 所在文件 |
|--------------|---------|---------|
| `folly::coro::Task` | `adapt::Task<R>` | `src/runtime/coro_task.h` |
| `folly::coro::Baton` | `adapt::AffinityBaton` | `src/runtime/affinity_baton.h` |
| `folly::coro::Mutex` | `adapt::AffinityMutex` | `src/runtime/affinity_mutex.h` |
| `folly::coro::blockingWait` | `Task<R>::blockingRun()` | `src/runtime/coro_task.h` |
| `folly::Executor` | `RouteFunc` + `enqueue_affine` | `src/runtime/affinity_baton.h` |
| `folly::ViaIfAsync` | 不需要（单 Worker 架构无需跨 Executor 路由决策） | — |

**自建 MPMC Ring**（`src/runtime/mpmc_ring.h`）：基于 DPDK 23.11 `rte_ring` 算法的 C++ 模板移植。支持单生产者/多生产者入队，单消费者/多消费者出队。

```cpp
// src/runtime/mpmc_ring.h:14 — DPDK 风格 MPMC Ring
template<typename T>
class alignas(64) MPMCRing {
    // prod_ 和 cons_ 各占独立 cacheline，避免 false sharing
    struct alignas(kCachelineSize) {
        std::atomic<size_t> head{0}, tail{0};
    } prod_, cons_;
};
```

**自建 Chase-Lev Deque**（`src/runtime/chase_lev_deque.h`）："Dynamic Circular Work-Stealing Deque" — Chase & Lev, SPAA 2005。

### 外部依赖清单

| 依赖 | 用途 | 源码位置 | 备注 |
|------|------|---------|------|
| **hwloc** | NUMA 拓扑发现 + CPU pinning | `src/runtime/work_stealing_executor.cc`, `src/runtime/worker.cc` | 编译时链接 |
| **liburing** (可选) | io_uring 后端 | `src/io/io_uring_backend.cc` | 编译期可选 |
| **SPDK** (可选) | SPDK 用户态 NVMe 驱动 | `src/io/spdk_backend.cc` | 编译期可选 |
| **libaio** (可选) | Linux AIO 后端 | `src/io/libaio_backend.cc` | 编译期可选 |

---

## 附录：关键文件索引

| 文件 | 内容 |
|------|------|
| `src/runtime/worker.h` / `.cc` | Worker 基类，线程启动/停止，worker_loop |
| `src/runtime/online_worker.h` / `.cc` | OnlineWorker：队列注册、IO 协程、定时器协程、co_submit |
| `src/runtime/work_stealing_executor.h` / `.cc` | WorkStealingExecutor：多 Worker 管理、work-stealing loop |
| `src/runtime/scheduler.h` / `.cc` | Scheduler：队列管理、run_busy/run、drain_p0 |
| `src/runtime/coro_task.h` | Task\<R\>：自建协程任务 |
| `src/runtime/affinity_baton.h` | AffinityBaton：协程信号量 + 线程亲和路由 |
| `src/runtime/affinity_mutex.h` | AffinityMutex：协程互斥锁 |
| `src/runtime/affinity_semaphore.h` | AffinitySemaphore：协程计数信号量 |
| `src/runtime/yield_awaiter.h` | yield() / yield_to() 协程让出 |
| `src/runtime/mpmc_ring.h` | DPDK 风格 MPMC 无锁 Ring |
| `src/runtime/chase_lev_deque.h` | Chase-Lev 无锁工作窃取双端队列 |
| `src/runtime/memory_pool.h` / `.cc` | 三级分层内存池（TLS → CAS center → bump） |
| `src/runtime/coro_api.h` | pthread 风格协程 API + when_all |
| `src/runtime/timer.h` / `.cc` | 定时器（Online per-worker + Offline global） |
| `src/runtime/worker_registry.h` / `.cc` | Worker 注册表：Online/Offline 统一查找 |
| `src/runtime/universal_route.h` | 统一路由：通过 WorkerRegistry 路由到任意 Worker |
| `src/runtime/adaptive_idle.h` / `.cc` | 自适应空闲（spin → yield → park） |
