# 协程线程模型

## 一、概述

storage-engine 构建了一套**无阻塞的用户态协程库**，让多线程程序零成本迁移到协程模型。核心特征：

- **零阻塞** — 调度器、IO 收割、同步原语全链路无 syscall、无 `std::mutex`、无 `folly::coro::blockingWait`
- **简易接入** — `#include "runtime/coro_api.h"`，API 对齐 pthread (`coro_start`/`coro_join`/`coro_detach`)
- **双调度模型** — Online (RTC 优先级队列) + Offline (工作偷取)

## 二、双调度模型

### Online — 优先级队列 (每个 Worker 独立)

```
Scheduler (busy_poll, ~328ns/轮):
  drain_p0()     → P0: baton.post 唤醒的生产者
  drain disk_io  → P1: IO 收割协程 (peek CQE → callback → baton.post)
  drain engine   → P2: 业务协程
  drain net_io   → P1: 网络 IO
  drain timer    → P4: 定时器
```

- 5 级队列，严格优先级
- IO 收割在 P1，新请求在 P2——先收割再接收，保证已提交 IO 的低延迟

### Offline — 工作偷取 (多 Worker 共享)

```
Worker 主循环:
  1. local_deque.pop()         ← 自己 LIFO
  2. NUMA peer.steal()         ← FIFO, CAS
  3. park                      ← 三级回退 (spin→yield→cv_wait)

外部提交: add(item) → 轮询选 worker → local_deque.push()
```

- Chase-Lev 偷取队列，NUMA 感知
- 无全局队列、无锁竞争

## 三、协程调度原语

| 原语 | 作用 | 用法 |
|------|------|------|
| `yield()` | 挂起→回到原优先级队列 | `co_await yield()` |
| `yield_to(idx)` | 挂起→指定队列 | `co_await yield_to(QueueType::kDiskIO)` |
| `AffinityBaton` | 协程间信号 | `co_await baton; baton.post(route)` |
| `AffinityMutex` | 互斥锁 | `co_await mtx.co_lock()` |
| `AffinitySemaphore` | 计数信号量 | `co_await sem.acquire(); sem.release()` |
| `coro_thread` | pthread 等价 | `coro_thread thr(fn); co_await thr.join()` |
| `co_sleep_for(dur)` | 定时挂起 | `co_await co_sleep_for(10ms)` |

## 四、常驻协程

两个系统协程在每个 Worker 启动时自动创建：

### IO 收割协程 (P1, IOCoro)
```cpp
while (true) {
    backend->poll();           // peek CQE
    fire callbacks;            // → baton.post → enqueue P0
    co_await yield();          // 挂起, 下轮继续
}
```

### 定时器协程 (P4, TimerCoro)
```cpp
while (true) {
    expire due timers;         // 到期 timer → 入队原优先级队列
    co_await yield();          // 挂起, 下轮继续
}
```

## 五、IO 模型

```
提交:  fill SQE → flush (推进 SQ tail, 66ns, 零 syscall)
       → SQPOLL 内核线程拉取 (CPU2, idle=0, 始终自旋)
完成:  NVMe 中断 → CQE 写入 ring → P1 协程 peek_cqe → callback → baton → 恢复
```

- io_uring: SQPOLL + idle=0, flush=66ns
- libaio: 内联 io_submit
- SPDK: 用户态 NVMe 驱动 (代码就绪)

## 六、迁移指南

| pthread | 协程框架 |
|---------|---------|
| `pthread_create` | `coro_thread thr(fn, args)` |
| `pthread_join` | `co_await thr.join()` |
| `pthread_detach` | `thr.detach()` |
| `pthread_yield` | `co_await this_coro::yield()` |
| `pthread_mutex_lock` | `co_await mtx.co_lock()` |
| `pthread_self` | `this_coro::get_id()` |
| `sem_wait` | `co_await sem.acquire()` |
| `sleep` | `co_await co_sleep_for(10ms)` |

## 七、禁止使用

| 禁止 | 替代 |
|------|------|
| `folly::coro::blockingWait` | `co_await` |
| `std::mutex` | `AffinityMutex` |
| `folly::coro::Task` (跨 worker) | `co_submit<R>` |

## 八、性能锚点

- 调度周期: 328ns
- Baton 往返: 47ns
- IO flush (SQPOLL): 66ns
- 协程挂起/恢复: < 200ns
- QD=1 IO P50: 17-28μs (盘状态波动)
