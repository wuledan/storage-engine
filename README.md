# Storage Engine

高性能单节点存储引擎，目标：最少核心 → 百万级 IOPS + 微秒尾延迟。

## Runtime — 用户态协程线程库

C++20 无阻塞协程运行时，对齐 pthread 语义，零成本迁移多线程代码。

### 只需一个头文件

```cpp
#include "runtime/coro_api.h"
using namespace storage::runtime;

// 像用线程一样用协程
coro_thread thr([] { do_work(); });
co_await thr.join();

// 互斥、信号量、睡眠 —— 全是协程原语
coro_mutex_t mtx;
co_await mtx.co_lock();

coro_sem_t sem(4);
co_await sem.acquire();

co_await co_sleep_for(100ms);
co_await yield();
```

### 挂起原理

所有 `co_await` 走 C++20 三步协议：`await_ready()` → `await_suspend(handle)` → `await_resume()`。handle 存入队列/链表/堆，由 Scheduler 或 timer 恢复。

| 原语 | 挂起方式 | 恢复时机 |
|------|---------|---------|
| `yield()` | `WorkItem` 入调度队列 | Scheduler 轮询到该队列 |
| `Baton` | CAS 入侵入式等待链表 | `post()` → RouteFunc → 入队 |
| `co_sleep_for` | 入 timer 小顶堆 | 到期 → pop → 入原优先级队列 |
| `Mutex` | CAS 入等待链表 | `unlock()` → 出队 → resume |
| `Semaphore` | 堆分配 WaiterNode | `release()` → 出队 → resume |

### 双调度模型

**Online** — 每 Worker 独立优先级队列：
```
drain_p0 (affine) → P1 (IO poll) → P2 (engine) → P4 (timer)
```

**Offline** — Chase-Lev 工作偷取：
```
affine_queue → local_deque(LIFO) → steal peer(FIFO) → park
```

### pthread 迁移对照

| pthread | 本框架 |
|---------|--------|
| `pthread_create` | `coro_thread thr(fn, args)` |
| `pthread_join` | `co_await thr.join()` |
| `pthread_detach` | `thr.detach()` |
| `pthread_mutex_lock` | `co_await mtx.co_lock()` |
| `sem_wait/sem_post` | `co_await sem.acquire()` / `sem.release()` |
| `sleep` | `co_await co_sleep_for(10ms)` |
| `pthread_yield` | `co_await yield()` |
| `pthread_self` | `this_coro::get_id()` |

### 禁止

`folly::coro::blockingWait`, `std::mutex`, `std::condition_variable` — 破坏协作调度模型。

### 性能锚点

- 调度周期: 328ns
- Baton 往返: 47ns  
- IO flush (SQPOLL): 66ns
- 协程挂起/恢复: <200ns

### IO 后端

io_uring (SQPOLL+idle=0) / libaio / SPDK (代码就绪)。QD=1 P50 17-28μs，与 fio 阻塞模型持平。

### 构建

```bash
cmake -B build && cmake --build build && ctest --test-dir build
```

### 文档

- [协程使用指南](docs/2026-06-10/26-coroutine-usage.md)
- [线程模型概述](docs/2026-06-10/28-threading-model.md)
- [IO 性能分析](docs/2026-06-10/24-io-perf-analysis.md)
- [亲和性队列架构](docs/2026-06-10/29-affine-architecture.md)
- [IO 优化与定时器](docs/2026-06-10/27-optimization-design.md)
