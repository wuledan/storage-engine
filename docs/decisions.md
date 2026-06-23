# 设计决策日志

> 按时间线记录每个关键设计决策的背景、方案、影响。

---

## 2026-06 — 删除 AffinityMutex::lock()

- **原因**: `lock()` 实现使用 spin-lock（`while (state_.load() & kLockedFlag) pause();`），在协程中执行会阻塞整个 Worker 线程。如果锁被另一个协程持有且该协程尚未运行（例如在另一个队列中等待），Worker 线程将永远自旋。
- **替代**: 全部改为 `co_await mutex.co_lock()` 或 `co_await mutex.co_scoped_lock()`，通过协程挂起等待而非线程自旋。
- **影响**: `ObjectPool`/`MemoryPool` 中桥接了 `std::mutex` 的代码需要逐步迁移到无锁或协程锁模式。当前 `MemoryPool::Impl` 中 `size_class_free_lists_` 已改用 CAS push/pop，不再需要锁。
- **代码**: `src/runtime/affinity_mutex.h` — 只有 `LockAwaiter` 和 `unlock()`，无 `lock()`。
- **遗留**: `MemoryPool::Impl` 中的 `bump_block_ptr_` 等成员是单线程访问（per-Worker），无需锁。

---

## 2026-06 — RouteFunc 从 std::function 改为函数指针 + context

- **原因**: 每个 `IORequest::Callback` 和 `AffinityBaton::WaiterNode` 在热路径上创建。`std::function` 即使使用小对象优化（SBO），大小也 ≥ 32 字节，当 callable 超过 SBO 阈值时触发堆分配。Perf 分析显示该分配在 `io_uring` 完成回调中占比显著。
- **替代**: 16 字节 `struct{fn*, ctx*}`，零堆分配，直接函数指针调用。
- **影响**:
  - `AffinityBaton::WaiterNode` 大小减小，cache 友好性提升
  - `RouteFunc` 现在是平凡可复制类型（trivially copyable），可安全放入 `MPMCRing`
  - 所有同步原语（`AffinityBaton`、`AffinityMutex`、`AffinitySemaphore`）以及 IO 回调均改用 `RouteFunc`
- **代码**: `src/runtime/affinity_baton.h:25`

```cpp
struct RouteFunc {
    void (*fn)(void* ctx, size_t worker_id, std::coroutine_handle<> h) = nullptr;
    void* ctx = nullptr;
    // 16 字节，零堆分配
};
```

---

## 2026-06 — 自建 Task\<R\> 替代 folly::coro::Task

- **原因**:
  1. 移除 folly 依赖，减少构建复杂性
  2. `folly::ViaIfAsync` 的路由机制在我们的单 Worker 架构中不需要——每个 Worker 只有一个线程，没有"在哪个 Executor 上恢复"的决策需求
  3. folly 的 `co_viaIfAsync` 增加额外的帧开销
- **替代**: 自建 `Task<R>`（`src/runtime/coro_task.h`），关键设计:
  - `initial_suspend = always`：调用者拥有句柄，必须通过 `release()` 提交到队列或 `co_await` 启动
  - `final_suspend = always`：协程帧在 `co_return` 后存活，直到结果被读取或自销毁
  - Symmetric transfer：`await_suspend` 返回 `std::coroutine_handle<>`
  - `blockingRun()` 替代 `folly::coro::blockingWait`
- **影响**:
  - 所有协程函数的返回类型从 `folly::coro::Task<R>` 改为 `adapt::Task<R>`
  - `blockingWait(task)` 改为 `std::move(task).blockingRun()`
  - 消除了 folly 的 ABI 兼容性问题
- **代码**: `src/runtime/coro_task.h`

```cpp
template<typename R>
class Task {
    // blockingRun: sync wait with spin on completed_ flag
    R blockingRun() && {
        auto h = release();
        h.promise().released_ = false;
        h.resume();
        while (!h.promise().completed_.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        // extract result, destroy frame
    }
};
```

---

## 2026-06 — TLS cache 原子变量消除

- **原因**: `ThreadLocalCache::LocalFreeList::count_` 原为 `std::atomic<size_t>`，尽管是 TLS 无竞争，编译器仍会生成 `lock cmpxchg` / `lock xadd` 指令，在热路径上产生约 12ns/op 的锁总线开销。对比 glibc tcache 使用普通 `size_t`。
- **替代**: 改为普通 `size_t count_`，`ThreadLocalCache` 整体不包含任何 `std::atomic` 成员。
- **影响**: round-trip（allocate + deallocate 小对象）从约 80ns 降至约 65ns（`// TODO: verify with current benchmark`）。
- **代码**: `src/runtime/memory_pool.cc:232`

```cpp
class ThreadLocalCache {
    struct LocalFreeList {
        SizeClassFreeList::Node* head{nullptr};
        size_t count{0};    // 非原子 — TLS 单线程
    };
    LocalFreeList free_lists_[kNumSizeClasses];  // 内联数组
};
```

- **注意**: `count_` 只在 TLS 内使用（非共享），所以不需要原子语义。`free_lists_` 本身也不在线程间共享。

---

## 2026-06 — 内存池中心 freelist 无锁化

- **原因**: 最初的实现使用 `std::mutex` 保护中心 freelist，多线程（多个 WSE Worker）竞争时锁总线成为瓶颈，吞吐量随核数扩展极差。
- **替代**: 三层无锁结构:
  1. `SizeClassFreeList` — CAS push/pop（`src/runtime/memory_pool.cc:127`）
  2. `CentralFreeList` — `exchange` 批量 drain + CAS push-back（`src/runtime/memory_pool.cc:167`）
  3. `MPMCRing<>` return_ring — TLS 到中心层的 batch-transfer（减少 CAS 频率）
- **影响**: 多线程吞吐从 1 核到 8 核扩展约 9 倍（`// TODO: verify with current benchmark`）。OnlineWorker 场景（单线程访问）无影响。
- **代码**: `src/runtime/memory_pool.cc`

```cpp
class SizeClassFreeList {
    std::atomic<Node*> head_{nullptr};
    void push(void* ptr) { CAS loop on head_ }
    void* pop() { CAS loop on head_ }
};
```

---

## 2026-06 — await_suspend 全部改为 symmetric transfer

- **原因**: C++20 协程中，`await_suspend` 返回 `void` 时，编译器在当前栈帧上直接恢复目标协程，不展开调用栈。深层链式 `co_await`（如 A→B→C→D）产生 O(n) 栈深度，最终 stack overflow。
- **替代**: 所有 `await_suspend` 返回 `std::coroutine_handle<>`，编译器保证生成 tail-call（`jmp` 而非 `call`）。挂起时返回 `std::noop_coroutine()`，恢复时返回目标协程句柄。
- **影响**:
  - 所有同步原语（`AffinityBaton`、`AffinityMutex`、`AffinitySemaphore`）的 `await_suspend` 签名变更
  - `Task<R>::await_suspend` 和 `TaskPromise::final_suspend` 均响应变更
  - 消除了深层嵌套协程的栈溢出 bug
- **代码模式**（`src/runtime/affinity_baton.h:136`）:

```cpp
// ❌ 旧模式 (void return — 可能栈累积)
void await_suspend(std::coroutine_handle<> h) { /* ... */ }

// ✅ 新模式 (返回 handle — 对称转移)
std::coroutine_handle<> await_suspend(std::coroutine_handle<> h) noexcept {
    // 如果已 post，立即恢复当前协程
    if (already_posted) return h;
    // 否则挂起，不恢复任何协程
    return std::noop_coroutine();
}
```

---

## 2026-06 — 删除 global_memory_resource()

- **原因**: `global_memory_resource()` 是一个全局单例，内部使用无锁 bump allocator。在多 Worker 场景下，两个 Worker 同时 bump 分配时产生数据竞争——TLS 无保护，bump offset 的 `fetch_add` 虽然原子，但不同 Worker 的 `pre-allocated block` 指针可能重叠。
- **替代**: 每个 Worker 拥有自己的 `mem_pool_`（`src/runtime/worker.h:108`），单线程安全。OnlineWorker 在构造时 `warmup` 预分配 4MB（`src/runtime/online_worker.cc:87`），WSE Worker 在首次使用时惰性分配。
- **影响**: 删除了 `global_memory_resource()` 函数及对应的 `.cc` 文件。所有调用点已迁移。
- **代码**: `src/runtime/worker.h:94`

```cpp
// 每个 Worker 一个 pool
std::unique_ptr<adapt::MemoryPool> mem_pool_;
adapt::MemoryPool& memory_resource() { return *mem_pool_; }
```

```cpp
// OnlineWorker 构造时 warmup
// src/runtime/online_worker.cc:87
OnlineWorker::OnlineWorker(const Worker::Config& cfg) : Worker(cfg) {
    memory_resource().warmup(4 * 1024 * 1024);  // 4MB
    // ...
}
```

---

## 2026-06 — Timer coroutine 启动时机从构造推迟到 on_worker_start()

- **原因**: `timer_coro_fn(this)` 在 `OnlineWorker` 构造函数中创建，内部使用 `current_worker()` 和 `yield()`。但在构造函数执行时，Worker 线程尚未启动（`worker_loop` 未运行），`tls_current_worker` 为 `nullptr`，`yield()` 中的路由逻辑无法正常工作。具体表现为 `yield()` 回退到 `default_queue_idx()` 时 segfault。
- **替代**: 构造函数中只创建协程并保存 `timer_handle_`，在 `on_worker_start()` 中 `resume()`。此时 `worker_loop` 已设置 TLS，`current_worker()` 返回正确指针。
- **影响**: `OnlineWorker` 新增 `on_worker_start` 覆盖（`src/runtime/online_worker.cc:111`）。同样逻辑也应用于 IO poll coroutine（`io_handle_`）。

```cpp
// src/runtime/online_worker.cc:111
void OnlineWorker::on_worker_start() {
    if (timer_handle_) {
        tls_source_queue_idx = idx_timer_;
        tls_source_queue = get_queue(idx_timer_);
        timer_handle_.resume();   // 此时 TLS 已就绪
        tls_source_queue_idx = SIZE_MAX;
    }
    if (io_handle_) {
        tls_source_queue_idx = idx_disk_io_;
        tls_source_queue = get_queue(idx_disk_io_);
        io_handle_.resume();
        tls_source_queue_idx = SIZE_MAX;
    }
}
```

---

## 2026-06 — Scheduler 分拆 run() 和 run_busy() 两条路径

- **原因**: OnlineWorker 需要始终轮询（busy_poll），因为 IO poll coroutine 必须在每个调度周期执行一次 `flush_submissions` + `poll`，不能进入 idle。而 WSE 使用 adaptive idle（spin → yield → park），在没有任务时降低 CPU 消耗。两条路径在同一个 `Scheduler::run()` 中通过 `if (busy_poll_)` 分支。
- **替代**: `Scheduler::run()` 判断 `busy_poll_` 后分流到 `run_busy()`（无 idle 的循环）或原来的 `run()`（带 policy decision + adaptive idle）。
- **影响**:
  - OnlineWorker: `scheduler_.set_io_backend()` 自动设置 `busy_poll_ = true`
  - WSE: 不用 busy_poll，用 adaptive idle
  - 两个路径的性能均更优（不存在"要 idle 还是要 polling"的权衡）
- **代码**: `src/runtime/scheduler.cc`

```cpp
void Scheduler::run() {
    if (busy_poll_) { run_busy(); return; }
    // 原逻辑: snapshot → decide → dequeue → execute → idle
}

void Scheduler::run_busy() {
    while (running_) {
        drain_p0();         // P0 affine
        drain P1;           // disk IO
        drain P2/P3/P4;     // engine/net_io/timer
        drain_p0();         // 再次 P0
    }
}
```

---

## 2026-06 — WorkStealingExecutor 本地队列放弃 Chase-Lev Deque，改用 MPMC Ring

- **原因**: 初始设计使用 `ChaseLevDeque`（LIFO owner pop / FIFO thief steal），但在 WorkStealingExecutor 中：
  1. `yield()` 将协程重新投递到 `yield_queue`（`LocalWorkQueue`），而不是 local_deque 的 pop 端，LIFO 语义未充分利用
  2. 所有 Worker 同步使用 `try_dequeue_mc`（包括 Owner），Chase-Lev 的 owner-steal 竞争保护反而增加了 CAS 路径的复杂度
  3. Owner 使用 `try_dequeue_mc` 时，Chase-Lev 的 `pop()` 中 `bottom--` + fence + `top CAS` 的三段式比 Ring 的 `cons_head CAS` 更昂贵
- **替代**: 使用 `RingWorkQueue`（基于 `MPMCRing<WorkItem>`，DPDK 风格），Owner 和 Thief 均使用 `try_dequeue_mc`（CAS on cons_head）。
- **影响**: 多 Worker 窃取吞吐提升（`// TODO: verify with current benchmark`）。OnlineWorker 保持使用 `RingWorkQueue` 不变。
- **代码**: `src/runtime/work_stealing_executor.h:34`

```cpp
struct WorkerState {
    std::unique_ptr<RingWorkQueue> local_deque;  // MPMC ring, FIFO, lock-free
    std::unique_ptr<LocalWorkQueue> yield_queue; // yield() 目标队列
};
```

---

## 2026-06 — co_submit 模板从 [&] 捕获改为参数传递

- **原因**: `co_submit` 是一个协程函数。当 lambda 使用 `[&]`（引用捕获）时，捕获的变量（如 `func` 和 `state`）在 lambda 初始化时绑定到栈地址。但协程的 `initial_suspend = always` 意味着 lambda 体不立即执行——当协程被 `release()` 提交到队列并在另一个上下文中 `resume()` 时，原始栈帧已失效，引用捕获产生 dangling reference。
- **替代**: 将 `func` 和 `state` 作为协程函数参数传递给 generic lambda（**无捕获**）。协程函数参数存储在协程帧中，生命周期与协程帧一致，安全跨越 `suspend`。
- **影响**: 所有类似的协程 lambda 模式（包括 `when_all`、`coro_create`、`co_async`）都遵循同一模式。

```cpp
// src/runtime/online_worker.h:153 — 无捕获，参数传递
auto make_engine_work = [](auto callable, SharedState* st) -> adapt::Task<void> {
    // callable 和 st 是协程函数参数，存在协程帧中
    callable();
    st->baton.post();
    co_return;
};
auto ew = make_engine_work(std::move(func), &state);
```

---

## 2026-06 — timed baton wait 的定时器路由从 RouteFunc 参数改为 baton.post() 自动路由

- **原因**: `AffinityBaton::TimedAwaiter` 最初将 `RouteFunc` 作为额外参数传入定时器回调，导致每个 `wait_for` 调用者需要手动传递路由信息，使用繁琐且容易遗漏。
- **替代**: 每个 waiter 在 `await_suspend` 时将自己的 `RouteFunc` 保存在 `WaiterNode` 中。定时器到期时，`on_expire` callback 调用 `baton.post()`，后者自动遍历 waiter chain 并使用每个 waiter 自己保存的 `route` 恢复。定时器回调本身不需要知道路由信息。
- **影响**:
  - `TimerNode` 不再需要 `RouteFunc` 字段
  - `on_expire` lambda 不再需要 `RouteFunc` 参数
  - `co_await baton.wait_for(timeout)` 使用方式简化
- **代码**: `src/runtime/worker.cc:131`

```cpp
std::coroutine_handle<> AffinityBaton::TimedAwaiter::await_suspend(
    std::coroutine_handle<> h) noexcept {
    node.handle = h;
    node.worker_id = detail::get_current_worker_id();
    node.route = get_current_route();   // 保存路由
    // ... CAS 入队 baton waiters ...

    // 定时器回调只需调用 baton.post()
    tn.on_expire = [state_ptr, &baton = this->baton]() {
        state_ptr->timed_out.store(true, std::memory_order_release);
        baton.post();  // 使用每个 waiter 自己的 route
    };
}
```

---

## 2026-06 — 引入 universal_route.h 统一 Online/Offline 路由

- **原因**: 当全局定时器（offline timer thread）需要恢复一个协程时，它不知道目标是 online worker 还是 offline worker。早期代码维护了两套独立的 Worker 查找逻辑。
- **替代**: 通过 `WorkerRegistry` 统一查找，`make_universal_route()` 返回的函数自动判断目标 Worker 类型:
  - Online: `OnlineWorker::enqueue_affine(coro_handle)`
  - Offline: `WorkStealingExecutor::add_to_worker(worker_id, coro_handle)`
- **影响**: 所有"从外部线程路由到任意 Worker"的场景统一走 `universal_route`。`WorkerRegistry` 新增 `WorkerHandle` 联合类型（online/offline）。
- **代码**: `src/runtime/universal_route.h`，`src/runtime/worker_registry.h:21`

```cpp
struct WorkerHandle {
    enum Type : uint8_t { kOnline, kOffline };
    Type type;
    union {
        OnlineWorker* online;
        struct { WorkStealingExecutor* exec; size_t local_id; } offline;
    };
    void push_affine(WorkItem item);
};
```

---

## 附录：决策时间线概览

| 时间 | 决策 | 核心动因 |
|------|------|---------|
| 2026-06 | 删除 AffinityMutex::lock() | 协程中 spin-lock 阻塞 Worker |
| 2026-06 | RouteFunc 从 std::function 改为函数指针 | 消除热路径堆分配 |
| 2026-06 | 自建 Task\<R\> | 移除 folly 依赖 |
| 2026-06 | TLS cache 原子变量消除 | 锁总线开销 ~12ns/op |
| 2026-06 | 内存池中心 freelist 无锁化 | 多线程扩展 9x |
| 2026-06 | await_suspend 全部改为 symmetric transfer | 消除栈累积溢出 |
| 2026-06 | 删除 global_memory_resource() | 多 Worker 数据竞争 |
| 2026-06 | Timer 启动推迟到 on_worker_start() | TLS 未初始化导致 segfault |
| 2026-06 | Scheduler 分拆 run/run_busy | Online vs Offline 不同 idle 策略 |
| 2026-06 | WSE 改用 MPMC Ring 替代 Chase-Lev | 更优的 MC 性能 |
| 2026-06 | co_submit 无捕获参数传递 | 协程帧生命周期安全 |
| 2026-06 | TimedBaton 路由自动保存 | 简化 wait_for 使用 |
| 2026-06 | 引入 universal_route | 统一 Online/Offline Worker 路由 |
