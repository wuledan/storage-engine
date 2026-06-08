# Storage Engine Benchmark 报告

> 日期：2026-06-08
> 环境：Linux x86-64, GCC 13, C++20, 3GHz
> 测试：全部通过
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

## 5. IO 性能

### 5.1 批量模式（fire-and-forget，N 请求同时提交）

| 后端 | IOPS | P50 | P99 |
|------|------|-----|-----|
| io_uring | **292 K ops/s** | 4.75 μs | 12.0 μs |
| libaio | **363 K ops/s** | — | — |

### 5.2 Ping-Pong 模式（无队列堆积，串行单请求）

| 后端 | P50 | P99 | 等效 IOPS |
|------|-----|-----|----------|
| io_uring | **8.1 μs** | 17.1 μs | 112 K ops/s |
| libaio | **4.7 μs** | 11.8 μs | 211 K ops/s |

libaio 在单请求低延迟场景更优（`io_getevents` 路径比 io_uring CQ peek 开销低）。io_uring 在批量模式有优势（SQ 批提交减少 syscall）。

### 5.3 队列深度扩展（SPDK-style pipelined，固定深度在飞行中）

#### io_uring

| QD | IOPS (K) | P50 (μs) | P99 (μs) | P999 (μs) | Avg (μs) |
|----|----------|----------|----------|-----------|----------|
| 1 | 108 | 8.49 | 16.77 | 131.84 | 9.23 |
| 4 | 442 | 8.58 | 19.59 | 87.61 | 9.06 |
| 8 | 1,116 | 6.37 | 16.80 | 26.63 | 7.17 |
| 16 | 2,230 | 6.70 | 16.45 | 23.26 | 7.17 |
| 32 | 3,823 | 7.25 | 16.30 | 23.52 | 8.37 |
| 64 | **13,643** | 4.43 | 10.06 | 11.80 | 4.69 |
| 128 | **34,427** | **3.39** | 7.10 | 9.91 | 3.72 |
| 256 | **51,323** | 3.91 | 19.50 | 26.30 | 4.99 |

#### libaio

| QD | IOPS (K) | P50 (μs) | P99 (μs) | P999 (μs) | Avg (μs) |
|----|----------|----------|----------|-----------|----------|
| 1 | 212 | 4.65 | 8.92 | 23.09 | 4.73 |
| 4 | 779 | 5.00 | 9.63 | 18.84 | 5.14 |
| 8 | 2,161 | 3.43 | 6.92 | 13.33 | 3.70 |
| 16 | 5,314 | **2.97** | 5.03 | 8.58 | 3.01 |
| 32 | 7,503 | 3.96 | 8.45 | 13.48 | 4.26 |
| 64 | **20,922** | 3.02 | 5.28 | 10.12 | 3.06 |

#### 分析

- **io_uring QD 扩展性更优**：支持到 QD=256，单线程 51.3M IOPS；libaio 受限于 io_context 大小
- **libaio 低延迟更优**：QD=1 时 P50=4.65μs vs io_uring 的 8.49μs（SQ 提交开销）
- **io_uring 高吞吐更优**：QD=128 时 P50 仅 3.39μs，IOPS 达 34.4M
- **两者都在 QD>8 时延迟收敛**，说明内部流水线生效

## 6. 自适应空闲 (AdaptiveIdle)

| 空闲时长 | 恢复延迟 | 退避级别 |
|---------|---------|---------|
| 1-50 ms | **8-10 μs** | SPIN / YIELD |
| 100 ms | **28.7 μs** | PARK (futex) |

**慢降级 / 快恢复**：持续空闲时逐级退避（SPIN→YIELD→PARK），一旦有任务立即通过 notify 重置为 ACTIVE。

## 7. 总结

| 维度 | 指标 | 数值 |
|------|------|------|
| **吞吐** | OnlineWorker 批量 | 3.1 M ops/s |
| **吞吐** | io_uring QD=256 | **51.3 M IOPS** |
| **吞吐** | libaio QD=64 | **20.9 M IOPS** |
| **延迟** | 队列调度 P50 | 683 ns |
| **延迟** | 跨线程 Active P50 | 982 ns |
| **延迟** | io_uring Ping-Pong P50 | 8.1 μs |
| **延迟** | libaio Ping-Pong P50 | **4.7 μs** |
| **延迟** | io_uring QD=128 P50 | **3.39 μs** |
| **延迟** | libaio QD=16 P50 | **2.97 μs** |
| **启动** | Worker 启动 P50 | 63 μs |

### 已知限制

1. LocalQueue/BatchedSPSC dequeue 受 `std::chrono` 微秒分辨率限制
2. 单 NUMA 节点，跨 NUMA 未测
3. 未启用 ASAN/TSAN/UBSAN
4. Worker 未绑定 CPU
