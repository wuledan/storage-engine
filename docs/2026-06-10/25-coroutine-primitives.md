# 协程同步原语实现方案

## 统一入口

`#include "runtime/coro_primitives.h"` 一次引入所有原语：

```cpp
#include "runtime/coro_primitives.h"
// → yield_awaiter.h, affinity_baton.h, affinity_mutex.h,
//   affinity_semaphore.h, work_item.h, worker.h
```

---

## 1. yield() / yield_to() — 协程挂起原语

### 接口
```cpp
co_await yield();              // 回到来源队列（Scheduler 执行前设 tls_source_queue_idx）
co_await yield_to(queue_idx);  // 显式指定队列
```

### 路由决策
```
yield() 优先级:
  1. tls_source_queue_idx != SIZE_MAX  → 回到该索引队列
  2. tls_source_queue != nullptr       → 使用该 WorkQueue* 直接入队
  3. current_worker()->default_queue_idx() → Online: engine(P2), Offline: 0
```

### 实现 (yield_awaiter.h)
```cpp
struct yield_awaiter {
    size_t queue_idx;
    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) noexcept {
        auto* w = current_worker();
        WorkQueue* q = nullptr;
        if (tls_source_queue) {
            q = tls_source_queue;
        } else if (w) {
            size_t idx = (tls_source_queue_idx != SIZE_MAX)
                ? tls_source_queue_idx : w->default_queue_idx();
            q = w->get_queue(idx);
        }
        if (q) q->enqueue(WorkItem::make_coro(h));
    }
    void await_resume() const noexcept {}
};
```

### Online vs Offline
| 场景 | Scheduler | tls_source_queue | yield() 行为 |
|------|-----------|-----------------|-------------|
| Online (drain_p0) | 设 tls_source_queue_idx = affine_idx_ | — | 回 P0 队列 |
| Online (drain_all) | 设 tls_source_queue_idx = qi | — | 回原队列 |
| Offline (WSE) | — | 设 tls_source_queue = ws.yield_queue | 回 worker 的 LocalWorkQueue |

### 缺陷修复记录
**Bug**: WorkStealingExecutor 未设 `tls_source_queue`，`current_worker()` 返回 null → yield() 静默丢弃协程 handle。
**修复**: WSE 每 worker 创建 `LocalWorkQueue` 作为 yield 目标，`worker_loop` 中每次 execute 前设 `tls_source_queue`，循环顶部 drain yield 队列。

---

## 2. AffinityBaton — 协程信号

### 接口
```cpp
AffinityBaton baton;
co_await baton;              // 挂起直到 post()
baton.ready();               // 查询是否已 post
baton.post(route_func);      // 通过 RouteFunc 路由到原 worker
```

### 路由机制
```
await_suspend: 记录 worker_id = detail::get_current_worker_id()
post(RouteFunc): 遍历 waiter 链，调 route(worker_id, handle)
  → Online:  enqueue_affine(handle) — 到 P0 队列
  → Offline: add_to_worker(id, handle) — 到目标 worker 的 local_deque
```

### 兼容性
| 场景 | get_current_worker_id | RouteFunc | 正确性 |
|------|----------------------|-----------|--------|
| Online | Worker::worker_loop() 设 | OnlineWorker::make_route_func() | ✅ |
| Offline | WSE::worker_loop() 设 | WSE::make_route_func() | ✅ |

---

## 3. AffinityMutex — 协程互斥锁

### 接口
```cpp
AffinityMutex mtx;
co_await mtx.co_lock();                  // 协程获取锁
auto guard = co_await mtx.co_scoped_lock(); // RAII
mtx.try_lock();                          // 非阻塞
mtx.unlock();                            // 释放+路由唤醒下一个

// std 兼容（仅单 worker 安全）
std::lock_guard<AffinityMutex> guard(mtx);
std::unique_lock<AffinityMutex> lock(mtx, std::defer_lock);
```

### `lock()` 安全性
`lock()` 使用 `__builtin_ia32_pause()` 自旋等待。**仅限持有者与调用者在同一 worker 线程**。
Online 场景安全（持有者在同线程必定释放），Offline 场景必须用 `co_await co_lock()`。

### 路由
`unlock()` 检查 `route_` 是否设置：
- 已设置且 waiter.worker_id ≠ SIZE_MAX → `route_(worker_id, handle)` 跨 worker 路由
- 未设置 → `handle.resume()` 内联恢复

---

## 4. AffinitySemaphore — 计数信号量

### 接口
```cpp
AffinitySemaphore sem(4);         // 最大 4 并发
co_await sem.acquire();           // 获取许可（count>0直接拿, =0挂起）
bool ok = sem.try_acquire();      // 非阻塞
sem.release();                    // 释放许可+唤醒一个等待者
size_t n = sem.available();       // 当前可用数
sem.set_route(route_func);        // 设跨 worker 路由
```

### 实现要点
- `acquire()`: CAS 递减 count，count=0 时堆分配 WaiterNode 入 CAS 保护链表
- `release()`: count++，出队一个 waiter
- WaiterNode 记录 `worker_id`（通过 `detail::get_current_worker_id()`）
- 支持 `RouteFunc`：设置后 `release()` 通过 `route_(worker_id, handle)` 路由回原 worker

### 缺陷修复记录
**Bug**: `release()` 直接 `handle.resume()` 内联恢复，忽略 `worker_id`。Offline 场景下恢复在错误线程。
**修复**: 添加 `RouteFunc route_` 成员和 `set_route()`，`release()` 检查 `route_` 是否设置，已设置则路由。

### 兼容性
| 场景 | 需要 set_route | 说明 |
|------|---------------|------|
| Online (单 worker) | 否 | 内联 resume 正确，所有协程同线程 |
| Offline (多 worker) | 是 | 必须设 RouteFunc，否则破坏亲和性 |

---

## 5. 线程身份与 TLS

| 变量 | 设置者 | 用途 |
|------|--------|------|
| `tls_worker_id` | Worker::worker_loop, WSE::worker_loop | worker 唯一编号 |
| `tls_current_worker` | Worker::worker_loop (仅 Online) | current_worker() 返回值 |
| `tls_source_queue_idx` | Scheduler::drain_p0/drain_all | yield() 目标队列索引 |
| `tls_source_queue` | WSE::worker_loop, Scheduler | yield() 目标 WorkQueue* |
| `detail::get_current_worker_id` | Worker::worker_loop, WSE::worker_loop | AffinityBaton/Mutex/Semaphore 记录等待 worker |

---

## 6. 跨 Worker 亲和性保证

| 原语 | 挂起记录 | 恢复路由 | 完整路径 |
|------|---------|---------|---------|
| Baton | `await_suspend`: worker_id = get_current_worker_id() | `post(route)`: route(worker_id, handle) → add_to_worker | ✅ |
| Mutex | `co_lock`: worker_id = get_current_worker_id() | `unlock()`: route_(worker_id, handle) → add_to_worker | ✅ |
| Semaphore | `acquire`: worker_id = get_current_worker_id() | `release()`: route_(worker_id, handle) → add_to_worker | ✅ |
| yield | — | tls_source_queue → enqueue(WorkItem::make_coro) → 本地 drain | ✅ |
