# Storage Engine Benchmark 报告

> 日期：2026-06-08
> 环境：Linux x86-64, GCC 13, C++20, 3GHz
> 测试：152/152 通过
> 计时：`__builtin_ia32_rdtsc` (纳秒精度)，转换公式 `ns = rdtsc / 3.0`

## 1. 队列吞吐量

### 1.1 LocalQueue（非原子，单线程）

| 操作 | 吞吐量 | 说明 |
|------|--------|------|
| enqueue 1M | ~10 B ops/s | 零原子操作，受计时器分辨率限制 |
| dequeue 1M | ~11 B ops/s | 同上 |
| enqueue+dequeue 1M | ~10 B ops/s | 同上 |

> 实际吞吐远高于测量值（单次操作 < 1ns），`std::chrono` 微秒分辨率不足以精确测量。

### 1.2 BatchedSPSCQueue（批量 SPSC，1 次 release fence per batch）

| 操作 | 吞吐量 |
|------|--------|
| push_batch 1M | 984 M ops/s |
| dequeue_batch 1M | ~97 B ops/s |
| push+dequeue 1M | 629 M ops/s |

### 1.3 AffineWorkQueue（MPMC，协程恢复）

| 操作 | 吞吐量 |
|------|--------|
| enqueue 1M（4 生产者） | 8.4 M ops/s |
| dequeue 1M（1 消费者） | 12.6 M ops/s |
| pipeline 1M（4P1C） | 8.5 M ops/s |

## 2. 调度器性能

| 指标 | 数值 |
|------|------|
| Scheduler 吞吐量（100k 任务） | **2.5 M ops/s** |
| Runtime 吞吐量（50k 任务，4 Worker） | **2.5 M ops/s** |
| OnlineWorker 批量吞吐（100k 任务） | **3.1 M ops/s** |

## 3. 协程调度延迟（rdtsc 纳秒精度）

### 3.1 纯队列调度延迟（Worker 内部，无跨线程）

路径：`submit_engine → Scheduler dequeue → execute`

| 百分位 | 延迟 |
|--------|------|
| P50 | **1,530 ns** |
| P99 | **3,673 ns** |
| P999 | **5,817 ns** |
| P9999 | **11,883 ns** |

### 3.2 排队延迟（perf counter，P0 affine queue）

| 指标 | 延迟 |
|------|------|
| avg_wait | 2,878 ns |
| P50_wait | **683 ns** |
| P99_wait | **1,365 ns** |
| max_wait | 139,746 ns |

### 3.3 跨线程往返延迟

| 场景 | P50 | P99 | P999 |
|------|-----|-----|------|
| Worker **ACTIVE** | **982 ns** | 1,021 ns | 7,589 ns |
| Worker **IDLE** (PARK) | **16,437 ns** | 22,135 ns | 24,648 ns |

### 3.4 延迟分解

```
Active 跨线程 982ns:
  ├─ 队列延迟 (P50):       683 ns  (69%)   Scheduler poll → dequeue
  └─ 线程间通知 + spin:    299 ns  (31%)   eventfd write/read + _mm_pause

Idle 跨线程 16.4μs:
  ├─ 队列延迟:             683 ns   (4%)
  ├─ 线程间通知:           299 ns   (2%)
  └─ futex PARK 唤醒:    ~15.4 μs  (94%)   condition_variable wait/wake
```

**核心协程恢复路径（AffinityBaton）零 futex，零系统调用**。PARK 仅在 Worker 所有队列为空时触发，属于空闲路径。

## 4. Worker 生命周期

| 操作 | P50 | P99 |
|------|-----|-----|
| Worker start | **63 μs** | **125 μs** |
| Worker stop + join | **78 μs** | **149 μs** |

> 100 次循环，不绑定 CPU

## 5. 总结

| 维度 | 指标 | 数值 |
|------|------|------|
| **吞吐** | OnlineWorker 批量 | 3.1 M ops/s |
| **吞吐** | Scheduler 调度 | 2.5 M ops/s |
| **吞吐** | Affine MPMC pipeline | 8.5 M ops/s |
| **吞吐** | Batched SPSC | 629 M ops/s |
| **延迟** | 纯队列调度 P50 | 683 ns |
| **延迟** | 协程恢复 (active) P50 | 982 ns |
| **延迟** | 协程恢复 (idle) P50 | 16.4 μs |
| **启动** | Worker 启动 P50 | 63 μs |

### 已知限制

1. LocalQueue/BatchedSPSC dequeue 受 `std::chrono` 微秒分辨率限制
2. 单 NUMA 节点，跨 NUMA 未测
3. 未启用 ASAN/TSAN/UBSAN
4. Worker 未绑定 CPU
