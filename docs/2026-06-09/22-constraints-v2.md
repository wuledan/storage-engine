# 系统架构与编码约束

> 日期：2026-06-09
> 版本：经 2026-06-08/09 讨论修正

## 1. 线程模型

### 1.1 Shared-Nothing Per-Worker
- 每个 Worker 独占一个 OS 线程，绑定一个物理 CPU core (hwloc)
- Worker 持有独立的 io_uring/libaio 实例、内存池、队列组
- 线程间仅通过 MPMC 队列（RingWorkQueue）通信

### 1.2 Worker 生命周期
- `Worker::start()` → 创建 OS 线程 → hwloc CPU+NUMA 绑定 → worker_loop
- `worker_loop()` → `on_worker_start()` → `scheduler_.run()` (纯 while 循环)
- 禁止使用 `folly::coro::blockingWait`

## 2. 协程模型

### 2.1 协程创建
- 协程使用 `suspend_always` 作为 initial_suspend
- 创建后不立即执行，通过 `enqueue_affine(handle)` 投递到 P0 队列
- Scheduler::drain_p0 单次 `h.resume()` 驱动执行
- `h.resume()` 从 initial_suspend 连续执行到下一个 co_await 才挂起

### 2.2 协程同步
- **仅使用 AffinityBaton**：`co_await baton` → IO 完成 → `baton.post(route)` → `enqueue_affine` → P0 → resume
- 禁止使用 `folly::coro::Task`、`folly::coro::BlockingWait`
- 禁止 `std::mutex`——同步走 AffinityBaton/AffinityMutex/AffinitySharedMutex
- AffinityBaton 的 Awaiter 在协程帧上，WaiterNode 栈分配

### 2.3 协程间通信
- 外部线程提交任务：`submit_engine()` → MPMC engine 队列 (P2)
- 协程恢复：`baton.post(route)` → `enqueue_affine` → P0 队列
- IO poll 协程：`submit_disk_io` → P1 队列，Reschedule 自循环

## 3. Scheduler 设计

### 3.1 主循环结构
```
while (running) {
    IO poll: flush + poll CQEs + callbacks  → P0
    drain_p0: resume coroutines
    snapshot + decide
    dequeue P1/P2 + execute
    drain_p0: resume new P0 items
}
```

### 3.2 核心约束
- **禁止在 Scheduler 主循环中硬编码业务分支**
- 所有业务逻辑（IO、网络、定时器、引擎）以协程任务形式接入
- IO backend 活跃时禁止进入 idle 休眠（`if (io_backend_) { continue; }`）

### 3.3 多优先级队列
| 优先级 | 队列 | 用途 |
|--------|------|------|
| P0 (Critical) | Affine (RingWorkQueue) | Baton 唤醒的协程恢复 |
| P1 (High) | NetIO (BatchedSPSC), DiskIO (BatchedSPSC), Timer (RingWorkQueue) | 网络/磁盘 IO 完成 |
| P2 (Medium) | Engine (RingWorkQueue) | 引擎任务、外部提交 |

## 4. IO 模型

### 4.1 提交模型
- **生产者驱动 flush**：产生 IO 的协程累积够 QD 后自行调 `flush_submissions()`
- `submit()` 只填 SQE，不调 `io_uring_submit`
- 单请求 `submit()` 直通，批量请求用 `submit_batch()`
- IO poll 协程仅收割 CQE，不调 flush_submissions

### 4.2 QD 感知
- 生产者一次产出 QD 个 IO 请求，共享一个 baton + pending 计数器
- 最后一个 IO 完成时 `baton.post(route)` → 生产协程恢复 → 下一批
- min_batch_depth=0（无缓冲），由生产者自行控制批量

### 4.3 IO 后端
- 支持 io_uring / libaio / SPDK (骨架)
- io_uring: 自适应提交阈值已移除（统一由生产者/Scheduler flush）
- libaio: io_submit 直连内核

## 5. 队列实现

### 5.1 队列选型
| 场景 | 实现 | 原因 |
|------|------|------|
| P0 affine (MPMC) | RingWorkQueue (rte_ring) | 多生产者 + 单消费者，零分配 |
| P2 engine (MPMC) | RingWorkQueue (rte_ring) | 外部多线程提交 |
| P1 net_io (SPSC) | BatchedSPSCWorkQueue | IO poller 单生产者 |
| P1 disk_io (SPSC) | BatchedSPSCWorkQueue | IO poll 协程单生产者 |

### 5.2 禁止
- 禁止 `folly::UMPMCQueue`（调用层级深，unbounded）
- 禁止在热路径上使用 `std::mutex` 保护的队列

## 6. 性能约束

### 6.1 任务级性能预期
- 每个开发任务必须明确 P50/P99/吞吐预期
- Review 时实际偏离 > 2x → 不通过

### 6.2 基准
| 指标 | 当前值 | 预期 |
|------|--------|------|
| 纯队列调度 P50 | 683 ns | < 1 μs |
| 框架开销占 IO 延迟 | < 3% | < 5% |
| Scheduler 单轮迭代 | ~500ns | < 1 μs |

## 7. 编码规范

### 7.1 禁止
- `std::mutex` (用 AffinityMutex)
- `folly::coro::Task` (用 SimpleCoro + suspend_always)
- `folly::coro::blockingWait` (用纯 while 循环)
- `std::function` 在热路径上 (用函数指针或 RouteFunc)

### 7.2 要求
- 错误码：StorageError 枚举 + 模块分段
- 日志：结构化，含 thread_id/trace_id
- 内存：关键路径预分配，对象池复用
- 测试：单测 + 模块测试 + 压力测试 + Benchmark
