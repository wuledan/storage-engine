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

## 3. 调度决策

```
Scheduler::flush_pending():
  ┌─ 有 P0 请求？ → 立即 flush 全部
  ├─ total_pending >= QD？ → flush 全部
  ├─ oldest_buffer.age >= 3？ → flush 该 buffer
  └─ 否则 → 继续缓冲，不提交
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

## 7. 待确认

1. `submit_batch` 是否需要支持跨多个 Scheduler 的 io_uring 实例（per-worker 隔离）？
2. buffer 合并时，priority 取最高还是平均？
3. 单请求 `submit()` 在 buffer 为空时是否直接 fill SQE（跳过缓冲）以减少延迟？抑或统一走 buffer 以保证语义一致？
