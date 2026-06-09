# IO 批量提交模型：完整分析

> 日期：2026-06-09

## 1. 用户需求逐条核对

### 需求 1：任务包含 QD 深度数据 → 一次 submit

```
外部提交者: {req₀, req₁, ..., req₁₂₇}  →  submit_batch(batch)
IO Backend: 填充 128 个 SQE → io_uring_submit 1 次
```

**当前实现**：❌ 每个 `submit(req)` 单独填 1 个 SQE，经 `flush_submissions` 合并提交，但 batch 语义不保证——128 个可能跨多轮 Scheduler。

**应该**：`submit_batch(requests)` 一口气填完并标记为同一批，Scheduler 见到一批就一次性提交。

### 需求 2：不足 QD → 缓冲 ≤ 2-3 轮

```
submit_batch(50 requests), QD=128
  → buffer 50, 标记 deadline = iteration + 3
  → 3 轮内如果凑齐 128 → flush
  → 3 轮到期仍不足 → 强制 flush
```

**当前**：❌ 无缓冲概念。每个 `submit` 直接填 SQE 进 ring。

### 需求 3：缓冲 + 新请求 > QD → 先 flush 旧

```
buffer = 50,  新 submit_batch(100)
  → 50 + 100 = 150 > 128
  → 先 flush 旧 50 (1 次 io_uring_submit)
  → 新 100 进 buffer (100 < 128, 走需求 2 逻辑)
```

### 需求 4（补充）：P0 级 IO 立即 flush

```
buffer = 50 (P2 优先级)
  → P0 协程: submit_batch(1) ← 高优
  → 立即 flush buffer(50) + P0(1) 或至少 P0 立即走
```

### 需求 5（补充）：多提交者公平性

```
协程 A: submit_batch(30)
协程 B: submit_batch(30)
  → 合并到 buffer = 60
  → Scheduler flush: 60 一起提交 (1 次 io_uring_submit)
```

## 2. API 设计

```cpp
class IOBatchingBackend : public IIOBackend {
public:
    // 单请求（兼容当前 API，内部走 batch 逻辑）
    void submit(IORequest req) override;

    // 批量提交
    // 保证：batch 内所有请求在同一次 io_uring_submit 中提交
    void submit_batch(std::vector<IORequest> requests);

    // Scheduler 每轮调用：决定是否 flush
    void flush_pending();

private:
    struct PendingBuffer {
        std::vector<IORequest> requests;
        uint64_t submit_count{0};     // 已填 SQE 数
        uint64_t age_iterations{0};
        Priority priority{Priority::kMedium};
    };
    std::vector<PendingBuffer> buffers_;

    static constexpr uint64_t kMaxBufferAge = 3;
};
```

## 3. 调度决策（含老化）

```
Scheduler::flush_pending():
  for each buffer:
    effective = buffer.priority - age_weight * buffer.age
  
  ┌─ 有 effective ≤ 0 (P0/老化足够)？ → flush 全部
  ├─ total_pending >= QD？ → flush 全部
  ├─ oldest_buffer.age >= 3？ → flush 该 buffer
  └─ 否则 → 继续缓冲
```

## 4. 与现有架构的集成

```
Scheduler::run() 每轮:
  1. drain_p0()
  2. flush_pending()            ← 批量提交决策 + io_uring_submit
  3. drain CQEs until EAGAIN    ← 收割完成
  4. drain_p0()                 ← IO 完成产生的 P0
  5. snapshot + decide
  6. dequeue P1/P2
  7. execute tasks
  8. drain_p0()
```

### co_read/co_write 路径

```
Engine 协程: co_await io_backend->co_read(fd, offset, buf, len)
  → 创建 IORequest + AffinityBaton
  → submit(req)  → 进入 buffer
  → co_await baton → 挂起
  
Scheduler: flush_pending() → io_uring_submit → poll → CQE → callback
  → baton.post(route) → enqueue_affine → drain_p0 → resume
  
Engine 协程: 恢复 → 拿到 IOCompletion
```

单请求 `submit()` 走 buffer 逻辑：
- 如果 buffer 空且就这一个请求 → 标记 deadline=0，下一轮立即 flush
- 如果有其他 buffer → 合并

## 5. 测试场景

| 测试 | 内容 |
|------|------|
| BatchExactQD | submit_batch(128), QD=128, 预期 1 次 io_uring_submit |
| BatchPartialFlush | submit_batch(30), QD=128, 等 3 轮, 预期强制 flush |
| BatchOverflow | buffer 30 + submit_batch(100), 预期先 flush 30 再缓冲 100 |
| P0Preempt | buffer 50(P2) + submit_batch(1, P0), 预期 P0 立即 flush |
| MixedPriority | 多个协程提交不同优先级 batch, 验证合并+flush 排序 |
| SingleRequest | submit(req)×1, QD=任何值, 预期 ≤ 1 轮内提交 |

## 6. 预期效果

| 场景 | 当前 (per-req submit) | 批量提交 | 改善 |
|------|----------------------|---------|------|
| QD=128 IOPS | ~121K | 预期 > 140K | +16% |
| QD=1 延迟 | ~17μs | ~17μs | 持平 (单请求 deadline=0) |
| io_uring syscall 数 | ~32/QD=128 | 1 | -97% |
| 多提交者公平性 | 先到先填 ring | buffer 合并 | 更公平 |

## 7. 待确认 → 已确认

1. **单请求缓冲**：单请求统一走 buffer，但 deadline=1 轮（即下一轮立刻 flush）。避免单请求被部分填充的 buffer 无限阻塞。
2. **加权**：buffer 合并时取 `max(priorities)`。调度决策时引入 **优先级老化**：buffer 每等一轮，effective_priority 上升。低优先级 buffer 等够久后强制 flush，防止饥饿。

```
effective_priority = base_priority - age_weight * buffer_age_iterations
// priority 数值越小越高 (0=P0, 3=P3)
// 老化使 effective 递减，最终触发 flush
```
3. **跨 Worker 场景**：可能存在。Worker A 的协程恢复后需要向 Worker B 的 io_uring 提交 IO（例如数据跨 shard）。`submit_batch` 的目标 io_uring 实例与调用方 Worker 解耦。

## 8. 最终实现计划

| Task | 内容 | 工时 |
|------|------|------|
| B1 | `IOBatchingBackend` 类：buffer 管理 + deadline + 加权合并 | 3h |
| B2 | `flush_pending()` 决策逻辑集成到 Scheduler | 2h |
| B3 | `submit_batch()` API + 单请求 `submit()` 走 buffer(dl=1) | 2h |
| B4 | 测试：6 个场景 (BatchExactQD/BatchPartialFlush/BatchOverflow/P0Preempt/MixedPriority/SingleRequest) | 3h |
| B5 | 跨 Worker 场景桩（占位，后续 IO 层完善） | 1h |
| **总计** | **5 任务** | **11h** |

## 9. libaio vs io_uring 差异根因分析

### 数据

| QD | io_uring IOPS | libaio IOPS | gap |
|----|:----------:|:---------:|:---:|
| 1 | 1.1K | 1.2K | 1.1x |
| 4 | 36.7K | 60.0K | 1.6x |
| 16 | 78.7K | 102.3K | 1.3x |
| 64 | 142.2K | 293.3K | 2.1x |
| 128 | 164.3K | 316.1K | 1.9x |
| 256 | 177.7K | 335.6K | 1.9x |

### 逐层解剖

**QD=1 gap (1.4x in latency: 40.6 vs 29.1us)**

纯内核路径差异：
- `io_uring_enter` 每次都要同步 SQ/CQ ring 状态 (head/tail 原子读写 + 内存屏障)
- `io_submit` 没有 ring 管理开销，直接入队 AIO 链表
- 差距 11.5us 中约 8-10us 是内核 ring 管理的额外开销

**QD=256 gap (1.9x in IOPS: 178K vs 336K)**

锁定在 CQ ring 的共享内存争用。256 个 IO 同时完成后，内核连续写入 256 个 CQE 到 ring：

内核 (IRQ 上下文) 每个完成 IO 执行:
  cqe->user_data = idx
  cqe->res = nbytes  
  smp_wmb()              ← 写屏障，确保 cqe 对用户态可见
  ring->cq.tail++         ← 原子写，使 CPU cache line 失效

用户态 (Scheduler) 每个 CQE 执行:
  peek_cqe → head = ring->cq.head (原子读)
  process callback
  io_uring_cqe_seen → ring->cq.head++ (原子写，cache line 失效)

每次 ring->cq.tail++ 和 ring->cq.head++ 都是原子写操作，导致该 cache line 在所有 CPU 核心之间反复失效和重新加载。
256 个 CQE = 256 次内核原子写 + 256 次用户态原子读/写 = 512 次 cache line 乒乓。

libaio 没有这个问题：
- 完成事件挂在内核链表上（纯内核内存，无共享）
- io_getevents 一次性 syscall 复制到用户态 buffer（单次内存拷贝）
- 无 cache line 争用

### 结论

差距的根本原因是 **io_uring CQ ring 在高并发完成时的 cache 一致性开销**，不是 Scheduler 间隙。
fio 中差距较小（155K vs 130K，1.2x）是因为 fio 的紧循环让 poll 和 kernel CQE 写入几乎同步发生，减少了并发写入的积压。
我们的多任务 Scheduler 在 poll 间隙处理了 P0/P1/P2，给了内核积压更多 CQE 的时间，返回后面对的是爆发性的 CQE 写入流 → 更严重的 cache 乒乓。libaio 不受此影响。
