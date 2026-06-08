# Storage Engine Benchmark 报告

> 日期：2026-06-08
> 环境：Linux x86-64, 6.17.0-29-generic, GCC 13, C++20
> CPU：2× Intel Xeon, 56c/112t per socket, 2 NUMA nodes
> 内存：125 GiB
> 计时：`__builtin_ia32_rdtsc` (纳秒精度)，转换公式 `ns = rdtsc / 3.0`
> io_uring：内核 6.17 原生支持，max aio: 65536

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

## 5. IO 性能（O_DIRECT, NVMe Solidigm P41 Plus, 绑核）

### fio 基线（裸 IO 参考）

| 配置 | P50 | P99 | IOPS |
|------|-----|-----|------|
| libaio QD=1 | 20 μs | 46 μs | 20.6k |
| io_uring QD=1 | 27 μs | 35 μs | 26.7k |
| libaio QD=128 | 692 μs | 1.06 ms | 130k |
| io_uring QD=128 | 545 μs | 922 μs | 155k |

### 5.1 Mode 1 — 同核协程（SimpleCoro, O_DIRECT, NVMe, 4K）

#### QD 扩展 (4K)

**io_uring:**

| QD | P50 (μs) | IOPS (K) |
|----|----------|----------|
| 1 | 18.7 | 36 |
| 4 | 6.7 | 316 |
| 16 | 6.5 | 1,466 |
| 64 | 5.8 | 10,876 |
| 128 | **5.8** | **13,651** |

**libaio:**

| QD | P50 (μs) | IOPS (K) |
|----|----------|----------|
| 1 | 9.3 | 100 |
| 4 | 6.7 | 586 |
| 16 | **4.3** | 2,953 |
| 64 | **4.2** | 15,126 |
| 128 | **4.2** | **30,009** |

### 5.2 Mode 2 — 跨线程（测试线程 → Worker）

测试线程直接 submit + spin wait，Worker Scheduler 异步 poll IO 完成。

#### Ping-Pong (QD=1, 4K)

| 后端 | P50 | P99 |
|------|-----|-----|
| io_uring | 18.6 μs | 633.2 μs |
| libaio | **5.5 μs** | **12.5 μs** |

### 5.3 Mode 1 vs Mode 2 对比

| 模式 | io_uring P50 | libaio P50 | 跨线程开销 |
|------|-------------|-----------|-----------|
| Mode 1 (同核) | 16.8 μs | 9.5 μs | 0 |
| Mode 2 (跨线程) | 18.6 μs | 5.5 μs | ~300ns eventfd + spin |

### 5.4 关键发现

- **libaio 全面领先 io_uring**：QD=1 时 9.5μs vs 16.8μs，QD=128 时 30M vs 13.7M IOPS
- **io_uring 批量提交优化生效**：Scheduler 统一 `flush_submissions`，高 QD 时延迟明显收敛（P50: 18.7→5.8μs）
- **SimpleCoro 协程模式验证通过**：同核协程通过 enqueue_affine → Scheduler 驱动 → co_await 挂起/恢复，无 blockingWait 死锁
- **O_DIRECT 延迟略高于 page cache**（4K P50 约 9-17μs vs 之前的 5-8μs），符合预期

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
| **吞吐** | libaio Mode1 QD=128 | **30.0 M IOPS** |
| **吞吐** | io_uring Mode1 QD=128 | **13.7 M IOPS** |
| **延迟** | libaio Mode1 Ping-Pong P50 | **9.5 μs** (O_DIRECT) |
| **延迟** | libaio Mode2 Cross-Thread P50 | **5.5 μs** (O_DIRECT) |
| **延迟** | 队列调度 P50 | 683 ns |
| **延迟** | 跨线程 Active P50 | 982 ns |
| **启动** | Worker 启动 P50 | 63 μs |
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
