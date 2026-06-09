# 2026-06-09 讨论总结

## 1. Scheduler 架构演进

### 问题
`folly::coro::blockingWait(scheduler_.run())` 在 Worker 线程上阻塞执行。当 Scheduler 进入 idle 时，Worker 线程被阻塞。

### 解决
- 移除 `blockingWait`，Scheduler::run() 改为 `void`（非 coroutine）
- Worker::worker_loop() 直接调用 `scheduler_.run()` — 纯 while 循环
- 对齐 `quant::WorkStealingExecutor` 模式

### 规则
**Scheduler 主循环禁止硬编码业务分支。所有业务以协程任务形式接入多优先级队列。**

## 2. IO 提交模型

### 问题
`submit()` 中每 4 个 SQE 调用一次 `io_uring_submit`，非真正批量提交。

### 最终模型
- **生产者自己 flush_submissions**：协程产生 QD 个 IO → 自行调 flush
- **IO poll 协程只收割 CQE**：通过 submit_disk_io 进 P1，Reschedule 模式循环
- **IO poll 在 Scheduler 循环顶部**：同轮收割 → callback → baton.post → drain_p0 立即恢复

### 规则
**生产者驱动 flush，IO poll 协程只收割。队列优先级: P0(affine) > P1(disk_io/net_io) > P2(engine)。**

## 3. 队列实现

### 问题
`folly::UMPMCQueue` 调用层级深（>10 层），无界队列在热路径上有 malloc 开销。

### 解决
- 移植 DPDK rte_ring 算法（CAS prod/cons head/tail, cacheline 对齐）
- MPMCRing<T>：有界 MPMC 环形队列，零分配
- RingWorkQueue：包装为 WorkQueue 接口
- P0 affine + P2 engine 队列使用 RingWorkQueue

### 规则
**热路径队列使用有界零分配的无锁队列。SPSC 场景用 BatchedSPSCWorkQueue。**

## 4. 协程执行模型

### 问题
- `suspend_never` → 协程体在创建线程立即执行 → worker_id 错误
- `suspend_always` + 双 resume → 对重入队协程错误
- `folly::coro::Task` 与裸 `handle.resume()` 不兼容

### 最终模型
- `suspend_always` init + 单 resume
- `h.resume()` 一次执行从 initial_suspend 到下一个 co_await
- `co_await baton` → baton.post → enqueue_affine → drain_p0 resume
- 全协程同步用 AffinityBaton，不用 folly::coro::Task

### 规则
**协程创建用 suspend_always，Scheduler 单 resume 驱动。禁止 folly::coro::Task 和 blockingWait。同步仅用 AffinityBaton。**

## 5. 清理

- 移除 `co_submit_engine` + `g_engine_task_mutex`（folly::coro 依赖）
- 移除 `buffer_mutex_`（min_batch_depth=0 时不需要）
- 移除 scheduler.cc 临时 inline IO poll
- OnlineWorker 中零 `std::mutex`

### 规则
**系统零 std::mutex。所有同步通过 AffinityBaton/Mutex/SharedMutex 实现。**

## 6. Benchmark 方法

### 问题
IOPS 使用 wall clock 计算，包含 Scheduler 空闲轮询时间。

### 解决
IOPS 改为延迟推导：`1 / avg_latency × QD`。P50 精确反映单次 IO 延迟。

## TODO

- [ ] 大页内存配置 + 测试
- [ ] 企业级 NVMe 对比验证
- [ ] io_uring SQPOLL 零 syscall 提交
- [ ] IO poll 协程恢复为独立 P1 协程（当前临时 inline 在 Scheduler 中）
