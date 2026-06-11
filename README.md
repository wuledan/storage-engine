# Storage Engine

高性能存储引擎，构建可插拔的多引擎存储资源池。

## 设计哲学

在低延迟时代，**尽量抛弃一切多余动作**——同步开销、系统调用开销、不必要的数据拷贝。以此为出发点，构建基础、可高度配置的 Runtime 框架。

## 架构

```
RPC Layer (DPDK/RDMA) → Consistency Layer → Pluggable Engine → IO Backend (io_uring/libaio/SPDK)
                                    ↕
                          Execution Runtime (C++20 Coroutines)
                          ├── Online: P0→P1→P2 priority scheduler
                          └── Offline: NUMA-aware work-stealing executor
```

### 核心模块

| 模块 | 状态 | 说明 |
|------|------|------|
| **Runtime** | ✔ | Online (priority scheduler) + Offline (work-stealing) |
| **IO Adapter** | ✔ | io_uring (SQPOLL) / libaio / SPDK 可插拔后端 |
| **Coroutine Primitives** | ✔ | yield, baton, mutex, semaphore, coro_thread |
| **SPDK Backend** | ✔ | 用户态 NVMe 驱动 (代码就绪，待环境) |
| **Perf & Trace** | ⏳ | 三级计数 + 直方图 |
| **RPC Layer** | ⏳ | DPDK TCP / RDMA |
| **Pluggable Engine** | ⏳ | LSM / B+Tree |

## 快速开始

### 迁移线程代码

```cpp
#include "runtime/coro_api.h"
using namespace storage::runtime;

// std::thread → coro_thread
coro_thread thr([] { do_work(); });
co_await thr.join();

// std::mutex → AffinityMutex
coro_mutex_t mtx;
co_await mtx.co_lock();

// sem_t → AffinitySemaphore
coro_sem_t sem(4);
co_await sem.acquire();

// std::this_thread::yield() → this_coro::yield()
co_await this_coro::yield();
```

### 协程原语 (coro_primitives.h)

| 原语 | 用途 | 用法 |
|------|------|------|
| `yield()` | 挂起→回来源队列 | `co_await yield()` |
| `yield_to(idx)` | 挂起→指定队列 | `co_await yield_to(QueueType::kDiskIO)` |
| `AffinityBaton` | 协程间信号 | `co_await baton` / `baton.post(route)` |
| `AffinityMutex` | 互斥锁 | `co_await mtx.co_lock()` / `std::lock_guard` |
| `AffinitySemaphore` | 计数信号量 | `co_await sem.acquire()` / `sem.release()` |
| `coro_thread` | std::thread 等价 | `coro_thread thr(fn)` / `co_await thr.join()` |
| `co_submit<R>(fn)` | 提交+返回结果 | `auto r = co_await w.co_submit<int>(fn)` |

### Online vs Offline

| 特性 | Online | Offline |
|------|--------|---------|
| 调度 | P0→P1→P2 优先级队列 | Chase-Lev 工作偷取 |
| 提交 | `submit_engine()` 到当前 worker | `group.submit()` 到全局队列 |
| 亲和性 | AffinityBaton → enqueue_affine(P0) | AffinityBaton → add_to_worker(id, handle) |
| IO | 支持 io_uring/libaio/SPDK | 纯 CPU 任务 |
| 创建 | `worker_spawn(cfg)` | `OfflineWorkerGroup(cfg)` |

## Performance (NVMe QD=1, Solidigm P41 Plus 1TB)

| Backend | P50 | RIOP | 模型 |
|---------|-----|------|------|
| io_uring (SQPOLL) | 17.9μs | 29.2K | 非阻塞, flush=66ns |
| libaio | 17.8μs | 33.3K | 内联 submit |
| fio io_uring | 47μs | 18.7K | 阻塞 submit_and_wait |

- P1 IO 收割协程: Scheduler 周期 328ns (drain_p0 41ns + drain_all 289ns)
- SQPOLL 内核线程: idle=0 紧密自旋, CPU2 绑定
- QD=4 SLC cache 命中: 10-17μs P50
- 详细分析: [docs/2026-06-10/24-io-perf-analysis.md](docs/2026-06-10/24-io-perf-analysis.md)
- 原语设计: [docs/2026-06-10/25-coroutine-primitives.md](docs/2026-06-10/25-coroutine-primitives.md)

## Benchmarking

```bash
# 框架多 QD 矩阵
./scripts/bench.sh

# fio 对照
./scripts/fio_compare.sh /mnt/nvme_test/fio_test 1G io_uring
```

## 构建

```bash
cmake -B build && cmake --build build
ctest --test-dir build
```

要求: GCC 13+, CMake 3.20+, folly, hwloc, GTest, liburing, libaio
