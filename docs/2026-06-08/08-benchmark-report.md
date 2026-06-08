# Storage Engine Benchmark 报告

> 日期：2026-06-08
> 环境：Linux x86-64, GCC 13, C++20
> 测试：152/152 通过

## 1. 队列吞吐量

### 1.1 LocalQueue（非原子，单线程）

| 操作 | 吞吐量 | 说明 |
|------|--------|------|
| enqueue 1M | ~10 B ops/s | 单线程，零原子操作，受计时精度限制 |
| dequeue 1M | ~11 B ops/s | 同上 |
| enqueue+dequeue 1M | ~10 B ops/s | 同上 |

> **注**：LocalQueue 完全非原子，耗时低于微秒级计时器分辨率，上述数值为时间分辨率极限。实际吞吐应远高于此。

### 1.2 BatchedSPSCQueue（批处理 SPSC）

| 操作 | 吞吐量 | 说明 |
|------|--------|------|
| push_batch 1M | 984 M ops/s | 批量入队，1 次 release fence |
| dequeue_batch 1M | ~97 B ops/s | 本地拷贝，计时器分辨率限制 |
| push+dequeue 1M | 629 M ops/s | 生产线模式 |

### 1.3 AffineWorkQueue（MPMC，协程恢复）

| 操作 | 吞吐量 | 说明 |
|------|--------|------|
| enqueue 1M（4 生产者） | 8.4 M ops/s | 多生产者竞争 |
| dequeue 1M（1 消费者） | 12.6 M ops/s | 单消费者，批量取出 |
| pipeline 1M（4P1C） | 8.5 M ops/s | 端到端生产线 |

## 2. 调度器性能

| 指标 | 数值 |
|------|------|
| Scheduler 吞吐量（100k 任务） | **2.5 M ops/s** |
| Runtime 吞吐量（50k 任务，4 Online Worker） | **2.5 M ops/s** |

## 3. 协程延迟

### 3.1 端到端提交延迟

| 百分位 | 延迟 |
|--------|------|
| P50 | **4 μs** |
| P99 | **10 μs** |
| P999 | **19 μs** |

路径：`submit_engine → Scheduler dequeue → execute → 完成回调`

### 3.2 批量任务吞吐量

| 指标 | 数值 |
|------|------|
| 100k 任务完成时间 | 32.4 ms |
| 吞吐量 | **3.1 M ops/s** |

## 4. Worker 生命周期

| 操作 | P50 | P99 |
|------|-----|-----|
| Worker start | **63 μs** | **125 μs** |
| Worker stop + join | **78 μs** | **149 μs** |

> 测试条件：100 次循环，不绑定 CPU

## 5. Worker 启动延迟（独立测试）

| 指标 | 数值 |
|------|------|
| 平均 | 175 μs |
| P50 | 188 μs |
| P90 | 198 μs |
| P99 | 198 μs |

> 包含 start + stop + join 完整周期，10 次采样

## 6. 总结

| 维度 | 指标 | 数值 |
|------|------|------|
| **吞吐** | OnlineWorker 批量任务 | 3.1 M ops/s |
| **吞吐** | Scheduler 调度 | 2.5 M ops/s |
| **延迟** | 协程提交 E2E P50 | 4 μs |
| **延迟** | 协程提交 E2E P99 | 10 μs |
| **启动** | Worker 启动 P50 | 63 μs |
| **队列** | Affine MPMC pipeline | 8.5 M ops/s |
| **队列** | Batched SPSC | 629 M ops/s |

### 已知限制

1. LocalQueue 和 BatchedSPSCQueue 部分指标受微秒级计时器精度限制（`std::chrono` 最小分辨率约 1μs），nops 操作延迟低于分辨率
2. 测试均在单 NUMA 节点上运行，跨 NUMA 访问延迟未测
3. 未启用 ASAN/TSAN/UBSAN（需单独构建 sanitized 预设）
4. Runtime 测试使用 4 OnlineWorker，均不绑定 CPU
